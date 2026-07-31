//! Local hot tier — "demote, don't drop" (P1 of
//! `docs/plans/2026-07-31-local-hot-tier.md`).
//!
//! A sealed MemBuffer bucket whose rows are already committed to Delta is
//! written to local disk as an **uncompressed Arrow IPC file** instead of
//! simply evaporating, and served back as a third scan leg (mem ∪ hot ∪
//! delta) via zero-copy mmap. That converts the recent-window read from
//! "open 50-325 R2 parquet files" into a page-cache hit with near-zero
//! decode CPU.
//!
//! Invariants:
//! - **A file here is a cache, never a durability boundary.** Only
//!   post-commit data is demoted; losing the whole directory costs latency,
//!   not rows.
//! - **Uncompressed IPC only** — buffer compression breaks the zero-copy
//!   mmap → `Buffer` → `FileDecoder` path (arrow-rs PR #6986).
//! - **Immutable files.** A demotion always writes a NEW file (tmp + fsync +
//!   rename); nothing is ever rewritten in place, so an mmap held by an
//!   in-flight query stays valid across GC (unlink only drops the link).
//! - **Name-filtered GC over our own root** (lesson of ba8820e: never a
//!   generic recursive deleter). Only `*.arrow` files matching our own
//!   `{bucket}_{min}_{end}_{seq}.arrow` convention are ever unlinked.
//!
//! Every failure path is best-effort: a bad write is counted and dropped, a
//! torn/unreadable file is treated as ABSENT so the window falls through to
//! the Delta leg. Nothing here may fail a query.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use arrow::{array::RecordBatch, buffer::Buffer, datatypes::SchemaRef};
use arrow_ipc::{Block, convert::fb_to_schema, reader::FileDecoder, reader::read_footer_length, root_as_footer, writer::FileWriter, writer::IpcWriteOptions};
use dashmap::DashMap;
use datafusion::logical_expr::Expr;
use tracing::{debug, info, warn};

use crate::mem_buffer::{TableKey, compile_filter_conjunction, filter_snapshot, overlaps};

const EXT: &str = "arrow";
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";
/// magic + minimal footer + trailer; anything shorter is definitionally torn.
const MIN_FILE_LEN: usize = 6 + 10;
/// The tier's three ceilings. Every one of them is an operator knob
/// (`TIMEFUSION_HOT_TIER_{MAX_DISK_GB,LEG_BUDGET_MB,MEMO_MB}`) because each
/// bounds a resource this box has already been killed by: the WAL/data volume,
/// query heap, and pinned mappings.
#[derive(Clone, Copy, Debug)]
pub struct HotTierLimits {
    /// Directory cap; over it GC unlinks oldest-first.
    pub max_disk_bytes: u64,
    /// Post-filter Arrow bytes ONE scan may materialize from demoted files.
    pub leg_budget_bytes: u64,
    /// Bytes of file mappings the decode memo may pin, LRU-evicted.
    pub memo_bytes: u64,
}

impl Default for HotTierLimits {
    fn default() -> Self {
        Self { max_disk_bytes: 64 << 30, leg_budget_bytes: 512 << 20, memo_bytes: 1 << 30 }
    }
}

/// Demotions a table must accumulate before its first conviction — one unlucky
/// DML must not cost a healthy table its tier.
const PROBE_DEMOTES: u64 = 4;
/// After the first conviction the probe is a single file: the cooldown only
/// buys anything if re-testing is cheap.
const REPROBE_DEMOTES: u64 = 1;
/// Share of a probe's files that must have been invalidated WITHOUT ever being
/// decoded by a query to convict. Not 100%: GC may legitimately have reaped one
/// of them first.
const WASTE_PCT: u64 = 75;
/// How long a convicted table stops demoting. Long relative to the flush
/// interval (so the wasted-write rate drops from one file per flush to one per
/// cooldown), short enough that a table whose enrichment stopped is back inside
/// the hot window.
const SUPPRESSION_COOLDOWN: Duration = Duration::from_secs(30 * 60);
/// Per-table suppression rows exposed in `timefusion_stats`; the count is
/// always exact, only the enumeration is capped.
const MAX_SUPPRESSED_ROWS: usize = 32;

/// Per-table demotion payoff, the input to adaptive suppression.
///
/// A demoted file is an immutable pre-DML snapshot, so any UPDATE/DELETE on the
/// table drops every file it holds. Under a workload that rewrites the whole
/// table continuously (monoscope's enrichment jobs on `otel_logs_and_spans`)
/// that is forever: the tier writes IPC, burns NVMe and page cache, and serves
/// nothing — invisibly, because files/bytes/writes all look healthy. So each
/// table is judged on its own files and stops demoting when they don't pay off.
#[derive(Default)]
struct DemotionHealth {
    /// Files written in the current probe window...
    demoted: AtomicU64,
    /// ...and how many of them a DML dropped before any query decoded them.
    wasted: AtomicU64,
    /// Micros at which the current suppression lifts; 0 = demoting.
    until: AtomicI64,
    episodes: AtomicU64,
}

impl DemotionHealth {
    /// True = skip this demotion. Lifting the cooldown resets the window, so
    /// the re-probe is judged only on what the table does from here — a table
    /// that stopped being mutated recovers with no restart and no config.
    fn suppressed(&self) -> bool {
        match self.until.load(Relaxed) {
            0 => false,
            until if crate::clock::now_micros() < until => true,
            _ => {
                self.until.store(0, Relaxed);
                self.demoted.store(0, Relaxed);
                self.wasted.store(0, Relaxed);
                false
            }
        }
    }
}

/// Must a scan reaching `lookback` micros into the past skip the hot leg
/// entirely? (`None` = no lower time bound, i.e. infinitely deep.)
///
/// The leg is materialized EAGERLY at plan time into a `MemorySourceConfig` —
/// outside every DataFusion memory pool and outside `GatedScanExec`, which
/// wraps only the Delta plan. A 7d/14d scan's range covers the whole hot
/// window, so consulting the tier would pull all of it into heap to shave a
/// handful of files off a scan already dominated by thousands. Past the tier's
/// own window it is by definition a fraction of the answer, so that window is
/// the threshold; `retention_micros = 0` (tier off) rejects everything.
pub fn skip_for_lookback(lookback: Option<i64>, retention_micros: i64) -> bool {
    lookback.is_none_or(|d| d > retention_micros)
}

/// One demoted bucket file. `[min_ts, end_ts)` is the file's ACTUAL row range,
/// not its bucket window, and is HALF-OPEN — the same convention as
/// `MemBuffer::get_bucket_ranges`, so both feed the Delta exclusion unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotBucketMeta {
    pub bucket_id: i64,
    pub seq: u64,
    pub min_ts: i64,
    pub end_ts: i64,
    pub bytes: u64,
    pub path: PathBuf,
}

impl HotBucketMeta {
    fn range(&self) -> (i64, i64) {
        (self.min_ts, self.end_ts)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HotTierStats {
    pub tables: usize,
    pub files: usize,
    pub bytes: u64,
    pub writes: u64,
    pub write_failures: u64,
    /// Files served to a scan / failed to decode (torn, unlinked mid-query,
    /// foreign format). Misses fall through to Delta.
    pub read_hits: u64,
    pub read_misses: u64,
    /// Files a scan skipped because MemBuffer still owned their window
    /// (expected), or because their schema no longer matches the table's (NOT
    /// expected — a drifted file contributes zero rows while the tier still
    /// looks healthy).
    pub mem_skipped: u64,
    pub schema_drift: u64,
    pub gc_deleted: u64,
    pub gc_bytes_freed: u64,
    pub invalidated: u64,
    /// DML statements that invalidated a derived time window vs. those that
    /// had to drop the whole table because no bound could be derived. A
    /// fallback rate near 100% means range invalidation is not working and the
    /// tier is back to losing its files to every enrichment statement.
    pub invalidations_ranged: u64,
    pub invalidations_full: u64,
    /// Tables currently NOT demoting because their files were being
    /// invalidated before any query read them, and the cumulative number of
    /// times that verdict has been reached.
    pub suppressed_tables: usize,
    pub suppressions: u64,
    /// Decode memo: entries and the mapping bytes they pin (bounded by
    /// `HotTierLimits::memo_bytes`), plus LRU evictions. Sustained eviction
    /// means the working set exceeds the memo — a re-decode per scan, not a
    /// correctness problem.
    pub memo_files: usize,
    pub memo_bytes: u64,
    pub memo_evicted: u64,
    /// Scans that hit `HotTierLimits::leg_budget_bytes` and stopped adding hot
    /// files. Non-zero means those windows were served from Delta instead;
    /// sustained non-zero means the budget is the effective tier size.
    pub leg_budget_stops: u64,
    /// The suppressed tables and their remaining cooldown in seconds, capped at
    /// [`MAX_SUPPRESSED_ROWS`].
    pub suppressed: Vec<(TableKey, i64)>,
}

/// One memoized decode: the batches, the mapping bytes it pins, and its LRU
/// stamp (a monotonic counter, not a clock — cheaper and monotone under test
/// time control).
struct MemoEntry {
    batches: Arc<Vec<RecordBatch>>,
    bytes: u64,
    used: AtomicU64,
}

#[derive(Default)]
pub struct HotTier {
    root: PathBuf,
    /// `None` = tier off: nothing is demoted and GC sweeps the directory
    /// clean, so a disabled tier can never strand a previous run's files.
    retention: Option<Duration>,
    limits: HotTierLimits,
    /// (project, table) → its files, ordered by (bucket_id, seq). A bucket can
    /// own several files: late arrivals into an already-drained bucket flush
    /// again and demote a second, disjoint row set.
    index: DashMap<TableKey, BTreeMap<(i64, u64), HotBucketMeta>>,
    /// Decoded batches, memoized per file. Files are immutable and the batches
    /// are zero-copy views over the mmap, so an entry costs `ArrayData` structs
    /// plus the mapping — not row data. Dropped when the file is, when
    /// `limits.memo_bytes` is exceeded (LRU), or on `rescan`.
    decoded: DashMap<PathBuf, MemoEntry>,
    /// Bytes of mapping currently pinned by `decoded`, and the LRU stamp
    /// source. `memo_bytes` is only ever mutated through `memo_insert` /
    /// `memo_forget`, which is what keeps it in step with the map.
    memo_bytes: AtomicU64,
    memo_clock: AtomicU64,
    memo_evicted: AtomicU64,
    leg_budget_stops: AtomicU64,
    health: DashMap<TableKey, Arc<DemotionHealth>>,
    seq: AtomicU64,
    suppressions: AtomicU64,
    writes: AtomicU64,
    write_failures: AtomicU64,
    read_hits: AtomicU64,
    read_misses: AtomicU64,
    mem_skipped: AtomicU64,
    schema_drift: AtomicU64,
    gc_deleted: AtomicU64,
    gc_bytes_freed: AtomicU64,
    invalidated: AtomicU64,
    invalidations_ranged: AtomicU64,
    invalidations_full: AtomicU64,
}

/// Path components must round-trip through the filesystem exactly, because
/// `rescan` reconstructs the (project, table) key FROM the directory names.
/// A lossy sanitization would let project `a/b` and project `a_b` share a
/// directory — a cross-tenant read. So unsafe names are simply not demoted.
fn safe_component(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s != "." && s != ".." && s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn parse_meta(path: PathBuf, bytes: u64) -> Option<HotBucketMeta> {
    if path.extension()?.to_str()? != EXT {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let mut parts = stem.split('_');
    let (bucket_id, min_ts, end_ts, seq) = (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    Some(HotBucketMeta { bucket_id: bucket_id.parse().ok()?, min_ts: min_ts.parse().ok()?, end_ts: end_ts.parse().ok()?, seq: seq.parse().ok()?, bytes, path })
}

impl HotTier {
    /// Open (creating if needed) a hot tier at `root` and rebuild its index
    /// from whatever survived the last process — restart warmth. `retention`
    /// and `limits` are the tier's entire policy; it enforces them itself, so
    /// callers only ever say `gc(now)`.
    pub fn open(root: PathBuf, retention: Option<Duration>, limits: HotTierLimits) -> Arc<Self> {
        let tier = Arc::new(Self { root, retention, limits, ..Default::default() });
        if let Err(e) = fs::create_dir_all(&tier.root) {
            warn!("hot tier disabled for this process — cannot create {:?}: {e}", tier.root);
        }
        tier.rescan();
        if retention.is_none() {
            // Disabled, but still responsible for its own directory: without
            // this a previous run's files sit there forever, unbounded and
            // invisible (the disk-leak failure mode of the orphaned spill dirs
            // and the never-evicted tantivy cache).
            tier.gc(0);
        }
        tier
    }

    /// Demote one committed bucket, whose rows span the INCLUSIVE `[min_ts,
    /// max_ts]` MemBuffer tracks. Best-effort: a failure is counted and
    /// logged, never propagated (the rows are already durable in Delta).
    pub fn demote(&self, project_id: &str, table_name: &str, bucket_id: i64, batches: &[RecordBatch], min_ts: i64, max_ts: i64) {
        if self.retention.is_none() || min_ts > max_ts || batches.iter().all(|b| b.num_rows() == 0) {
            return;
        }
        let key: TableKey = (Arc::from(project_id), Arc::from(table_name));
        let health = self.health(&key);
        if health.suppressed() {
            return;
        }
        match self.write_bucket(project_id, table_name, bucket_id, batches, min_ts, max_ts + 1) {
            Ok(Some(meta)) => {
                self.writes.fetch_add(1, Relaxed);
                health.demoted.fetch_add(1, Relaxed);
                self.index.entry(key.clone()).or_default().insert((meta.bucket_id, meta.seq), meta);
                self.judge(&key, &health);
            }
            Ok(None) => {}
            Err(e) => {
                self.write_failures.fetch_add(1, Relaxed);
                warn!("hot tier demotion failed for {project_id}.{table_name} bucket {bucket_id} (rows are in Delta; read falls through): {e:#}");
            }
        }
    }

    fn health(&self, key: &TableKey) -> Arc<DemotionHealth> {
        self.health.entry(key.clone()).or_default().clone()
    }

    /// Convict a table whose probe window was mostly wasted files, and start
    /// its cooldown. Only ever called at the transition, so the log fires once
    /// per episode, not once per bucket.
    fn judge(&self, key: &TableKey, h: &DemotionHealth) {
        let (demoted, wasted, episodes) = (h.demoted.load(Relaxed), h.wasted.load(Relaxed), h.episodes.load(Relaxed));
        let sample = if episodes == 0 { PROBE_DEMOTES } else { REPROBE_DEMOTES };
        if h.until.load(Relaxed) != 0 || demoted < sample || wasted * 100 < demoted * WASTE_PCT {
            return;
        }
        h.until.store(crate::clock::now_micros() + SUPPRESSION_COOLDOWN.as_micros() as i64, Relaxed);
        h.episodes.fetch_add(1, Relaxed);
        self.suppressions.fetch_add(1, Relaxed);
        info!(
            "hot tier suppressing demotion of {}.{} for {}s: {wasted}/{demoted} demoted file(s) were invalidated by DML before any query read them (episode {})",
            key.0,
            key.1,
            SUPPRESSION_COOLDOWN.as_secs(),
            episodes + 1
        );
    }

    fn write_bucket(
        &self, project_id: &str, table_name: &str, bucket_id: i64, batches: &[RecordBatch], min_ts: i64, end_ts: i64,
    ) -> anyhow::Result<Option<HotBucketMeta>> {
        if !safe_component(project_id) || !safe_component(table_name) {
            debug!("hot tier skipping {project_id}.{table_name}: name is not a safe path component");
            return Ok(None);
        }
        let Some(schema) = batches.first().map(|b| b.schema()) else { return Ok(None) };
        let dir = self.root.join(project_id).join(table_name);
        fs::create_dir_all(&dir)?;
        let seq = self.seq.fetch_add(1, Relaxed);
        let path = dir.join(format!("{bucket_id}_{min_ts}_{end_ts}_{seq}.{EXT}"));
        crate::wal::write_atomic_with(&path, true, |f| {
            // Default options: 64-byte alignment, V5, NO compression — the
            // alignment is what keeps the mmap read zero-copy, the absence of
            // compression is what makes it possible at all.
            let mut w = FileWriter::try_new_with_options(f, schema.as_ref(), IpcWriteOptions::default()).map_err(std::io::Error::other)?;
            batches.iter().try_for_each(|b| w.write(b)).map_err(std::io::Error::other)?;
            w.finish().map_err(std::io::Error::other)
        })?;
        Ok(Some(HotBucketMeta { bucket_id, seq, min_ts, end_ts, bytes: fs::metadata(&path)?.len(), path }))
    }

    /// Files whose row range overlaps `query_range` — a CLOSED `[lo, hi]`
    /// window, as query filters report it; `None` = everything. Oldest bucket
    /// first.
    pub fn buckets_in_range(&self, project_id: &str, table_name: &str, query_range: Option<(i64, i64)>) -> Vec<HotBucketMeta> {
        let key: TableKey = (Arc::from(project_id), Arc::from(table_name));
        let Some(entry) = self.index.get(&key) else { return Vec::new() };
        entry.values().filter(|m| query_range.is_none_or(|(lo, hi)| overlaps(m.range(), (lo, hi.saturating_add(1))))).cloned().collect()
    }

    /// The scan's hot leg: one batch partition per demoted file, plus the
    /// timestamp ranges those files are authoritative for.
    ///
    /// Coverage contract — every window is served by exactly ONE tier:
    /// - a file overlapping a live MemBuffer range is SKIPPED (MemBuffer holds
    ///   the fresher copy of that window);
    /// - a file we serve contributes its range, which the caller excludes from
    ///   the Delta leg exactly like `mem_ranges`;
    /// - a file we could NOT use (torn, unlinked, schema drift) contributes
    ///   NOTHING — no rows and no exclusion — so its window falls through to
    ///   Delta intact.
    ///
    /// - once `HotTierLimits::leg_budget_bytes` is reached NOTHING further is
    ///   added — neither rows nor ranges — so the remaining windows fall
    ///   through to Delta intact.
    ///
    /// Filters are applied here for the same reason the MemBuffer leg applies
    /// them: pushdown may be reported `Exact`, so no `FilterExec` is
    /// guaranteed above the scan.
    ///
    /// DEDUP CONTRACT: the batches served here are the RAW drained bucket, not
    /// the `dedup_batches(...)` output the flush wrote to Delta, so a hot file
    /// may hold key duplicates Delta does not. That is sound because the two
    /// cases are exhaustive: with no `dedup_keys` the flush's dedup is itself a
    /// pass-through (raw == committed), and with `dedup_keys` a non-empty hot
    /// leg forces `ProjectRoutingTable::scan` down the union path, which never
    /// sets `skip_dedup` — the two branches that do are guarded on
    /// `hot_partitions.is_empty() && hot_ranges.is_empty()`. Granting
    /// `skip_dedup` on a plan carrying a hot leg would break this.
    #[allow(clippy::too_many_arguments)]
    pub async fn query_partitioned(
        self: &Arc<Self>, project_id: &str, table_name: &str, query_range: Option<(i64, i64)>, mem_ranges: &[(i64, i64)], filters: &[Expr], schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> (Vec<Vec<RecordBatch>>, Vec<(i64, i64)>) {
        let metas = self.buckets_in_range(project_id, table_name, query_range);
        if metas.is_empty() {
            return Default::default();
        }
        let (tier, mem_ranges, filters, schema, projection) = (self.clone(), mem_ranges.to_vec(), filters.to_vec(), schema.clone(), projection.cloned());
        // open + mmap + decode + page faults are blocking syscalls; a planner
        // future must not run them on its async worker.
        tokio::task::spawn_blocking(move || tier.read_leg(&metas, &mem_ranges, &filters, &schema, projection.as_deref())).await.unwrap_or_default()
    }

    fn read_leg(
        &self, metas: &[HotBucketMeta], mem_ranges: &[(i64, i64)], filters: &[Expr], schema: &SchemaRef, projection: Option<&[usize]>,
    ) -> (Vec<Vec<RecordBatch>>, Vec<(i64, i64)>) {
        let cols = plan_columns(projection, filters, schema);
        let filter_schema = cols.as_ref().map_or_else(|| schema.clone(), |c| c.schema.clone());
        let pred = compile_filter_conjunction(filters, &filter_schema).ok().flatten();
        let (mut partitions, mut ranges, mut used) = (Vec::new(), Vec::new(), 0u64);
        for meta in metas {
            if mem_ranges.iter().any(|r| overlaps(meta.range(), *r)) {
                self.mem_skipped.fetch_add(1, Relaxed);
                continue;
            }
            let Some(batches) = self.read_file(&meta.path) else { continue };
            // A file that decodes to NOTHING would pass the schema check below
            // (`first()` is None), push its range, and contribute no rows — the
            // one shape that silently excludes a window from Delta and serves
            // it from nowhere. `demote` rejects all-empty input so this is
            // unreachable today; the coverage contract must not depend on that.
            if batches.is_empty() {
                self.read_misses.fetch_add(1, Relaxed);
                continue;
            }
            // One schema per IPC file by construction, so one compare per file.
            // Drift (a column added since the demotion) breaks the
            // `MemorySourceConfig` contract — treat the file as absent.
            if batches.first().is_some_and(|b| b.schema().fields() != schema.fields()) {
                self.schema_drift.fetch_add(1, Relaxed);
                continue;
            }
            // Project BEFORE filtering: `filter_record_batch` heap-copies every
            // column it is handed, so an unprojected filter would materialize
            // `body`/`attributes` for the whole hot window on a 3-column query.
            let projected = match &cols {
                Some(c) => batches.iter().map(|b| b.project(&c.needed)).collect::<Result<Vec<_>, _>>(),
                None => Ok(batches.as_ref().clone()),
            };
            // A failure here must not leave the range excluded from Delta.
            let Ok(projected) = projected else { continue };
            let filtered = filter_snapshot(projected, &pred);
            // Cut the predicate-only columns back off (positions are `0..n` by
            // construction in `plan_columns`, so this cannot fail).
            let filtered: Vec<RecordBatch> = match cols.as_ref().and_then(|c| c.output.as_ref()) {
                Some(out) => filtered.iter().flat_map(|b| b.project(out)).collect(),
                None => filtered,
            };
            // The hot leg is materialized at PLAN time into a
            // `MemorySourceConfig` — no memory pool, no `GatedScanExec`, no
            // spill. `limits.leg_budget_bytes` is therefore the only thing
            // bounding it, and it is charged on the post-filter bytes actually
            // retained (`filter_record_batch` heap-copies, so these are real
            // allocations, unlike the zero-copy `batches` above).
            //
            // Range and partition are pushed TOGETHER, after the check: a file
            // whose rows we refuse to hold must also not be excluded from the
            // Delta leg, or its window is served by nobody.
            let bytes: u64 = filtered.iter().map(|b| b.get_array_memory_size() as u64).sum();
            if used + bytes > self.limits.leg_budget_bytes {
                self.leg_budget_stops.fetch_add(1, Relaxed);
                debug!("hot leg stopped at {used} bytes (budget {}); {:?} and older windows fall through to Delta", self.limits.leg_budget_bytes, meta.path);
                break;
            }
            used += bytes;
            ranges.push(meta.range());
            if !filtered.is_empty() {
                partitions.push(filtered);
            }
        }
        (partitions, ranges)
    }

    /// Zero-copy read of one demoted file, memoized. `None` = treat as absent
    /// (torn, unlinked under us, or written by something that isn't us) — the
    /// caller must then let the Delta leg serve that window.
    pub fn read_file(&self, path: &Path) -> Option<Arc<Vec<RecordBatch>>> {
        if let Some(hit) = self.decoded.get(path) {
            hit.used.store(self.memo_clock.fetch_add(1, Relaxed), Relaxed);
            self.read_hits.fetch_add(1, Relaxed);
            return Some(hit.batches.clone());
        }
        match decode_file(path, false) {
            Ok(batches) => {
                self.read_hits.fetch_add(1, Relaxed);
                let batches = Arc::new(batches);
                self.memo_insert(path, batches.clone());
                Some(batches)
            }
            Err(e) => {
                self.read_misses.fetch_add(1, Relaxed);
                debug!("hot tier file {path:?} unreadable, falling through to Delta: {e:#}");
                None
            }
        }
    }

    /// Memoize one decode and enforce the memo's byte ceiling. The charge is
    /// the file's length: an entry's cost is dominated by the mmap it pins for
    /// its whole lifetime (clean, reclaimable pages — pressure, not RSS), and
    /// that mapping is exactly the file. An entry that can't be sized isn't
    /// memoized at all, so `memo_bytes` can never understate the map.
    fn memo_insert(&self, path: &Path, batches: Arc<Vec<RecordBatch>>) {
        let Ok(bytes) = fs::metadata(path).map(|m| m.len()) else { return };
        let used = AtomicU64::new(self.memo_clock.fetch_add(1, Relaxed));
        if let Some(old) = self.decoded.insert(path.to_path_buf(), MemoEntry { batches, bytes, used }) {
            self.memo_bytes.fetch_sub(old.bytes, Relaxed);
        }
        self.memo_bytes.fetch_add(bytes, Relaxed);
        // The entry just inserted carries the newest stamp, so it is evicted
        // last — a memo smaller than one file degrades to "no memo", never to
        // an evict-what-we-just-read spin.
        while self.memo_bytes.load(Relaxed) > self.limits.memo_bytes {
            let Some(lru) = self.decoded.iter().min_by_key(|e| e.used.load(Relaxed)).map(|e| e.key().clone()) else { break };
            if self.memo_forget(&lru) {
                self.memo_evicted.fetch_add(1, Relaxed);
            } else {
                break; // raced with another evictor; it owns the accounting
            }
        }
    }

    /// Drop one memo entry, returning whether THIS call removed it (the caller
    /// counts evictions, and only the remover may charge the bytes back).
    fn memo_forget(&self, path: &Path) -> bool {
        match self.decoded.remove(path) {
            Some((_, e)) => {
                self.memo_bytes.fetch_sub(e.bytes, Relaxed);
                true
            }
            None => false,
        }
    }

    /// Drop every file for a (project, table) — the FALLBACK scope, for a DML
    /// whose touched rows can't be bounded in time. Demoted rows are pre-DML
    /// copies and the hot leg is ordered ahead of Delta in the union, so a
    /// stale file would shadow the corrected Delta row. Prefer
    /// [`Self::invalidate_range`] whenever the statement carries time bounds.
    ///
    /// ORDERING INVARIANT (load-bearing, and the reason WAL replay needs no
    /// invalidation of its own): callers must invalidate BEFORE appending the
    /// DML to the WAL, and `rescan` must run BEFORE `recover_from_wal`. Then a
    /// replayable DML entry can only ever exist once the files are already
    /// gone. Move either and stale rows come back — silently.
    pub fn invalidate(&self, project_id: &str, table_name: &str) {
        self.invalidations_full.fetch_add(1, Relaxed);
        let key: TableKey = (Arc::from(project_id), Arc::from(table_name));
        let paths = self.index.remove(&key).map(|(_, m)| m.into_values().map(|m| m.path).collect()).unwrap_or_default();
        self.drop_files(&key, paths);
    }

    /// Range-scoped [`Self::invalidate`]: drop only the files whose half-open
    /// row range overlaps the half-open `[lo, hi)` window the statement can
    /// touch. Files provably disjoint from it hold no row the DML rewrote, so
    /// they stay authoritative.
    ///
    /// Callers MUST fall back to the table-wide [`Self::invalidate`] whenever
    /// the window can't be derived from the statement — over-invalidating is a
    /// cache miss, under-invalidating resurrects a stale row.
    pub fn invalidate_range(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) {
        self.invalidations_ranged.fetch_add(1, Relaxed);
        let key: TableKey = (Arc::from(project_id), Arc::from(table_name));
        let paths = {
            let Some(mut entry) = self.index.get_mut(&key) else { return };
            let mut doomed = Vec::new();
            entry.retain(|_, m| {
                if overlaps(m.range(), (lo, hi)) {
                    doomed.push(m.path.clone());
                    return false;
                }
                true
            });
            doomed
        };
        self.index.remove_if(&key, |_, v| v.is_empty());
        self.drop_files(&key, paths);
    }

    /// Unlink an invalidated set, durably, and charge it to the table's
    /// demotion payoff. Shared by both invalidation scopes.
    fn drop_files(&self, key: &TableKey, paths: Vec<PathBuf>) {
        if paths.is_empty() {
            return;
        }
        self.invalidated.fetch_add(paths.len() as u64, Relaxed);
        // A file with no memoized decode was never read by any query: it was
        // written and thrown away, pure cost. Must be counted before `unlink`,
        // which drops those entries.
        let wasted = paths.iter().filter(|p| !self.decoded.contains_key(*p)).count() as u64;
        debug!("hot tier invalidated {} file(s) ({wasted} never read) for {}.{} after DML", paths.len(), key.0, key.1);
        self.unlink(&paths);
        // Make the unlinks durable. A SIGKILL/OOM-kill is safe without this
        // (the unlink is already visible), but a POWER LOSS can lose it, and
        // then boot `rescan` resurrects a pre-DML file whose stale rows shadow
        // the corrected Delta rows for the whole retention window. Cheap: one
        // fsync per table dir per DML, off the query path.
        for dir in paths.iter().filter_map(|p| p.parent()).collect::<std::collections::BTreeSet<_>>() {
            let _ = fs::File::open(dir).and_then(|d| d.sync_all());
        }
        let health = self.health(key);
        health.wasted.fetch_add(wasted, Relaxed);
        self.judge(key, &health);
    }

    /// Forget + unlink. Held mmaps stay valid across the unlink, so in-flight
    /// queries are unaffected.
    fn unlink(&self, paths: &[PathBuf]) {
        for p in paths {
            self.memo_forget(p);
            let _ = fs::remove_file(p);
        }
    }

    /// Rebuild the index from disk (boot warmth). Walks only our own two-level
    /// layout and only accepts files matching our naming convention; anything
    /// else is left strictly alone.
    pub fn rescan(&self) {
        self.index.clear();
        self.decoded.clear();
        self.memo_bytes.store(0, Relaxed);
        let (mut files, mut max_seq) = (0usize, 0u64);
        let Ok(projects) = fs::read_dir(&self.root) else { return };
        for project in projects.flatten() {
            let Some(pid) = project.file_name().to_str().map(Arc::<str>::from) else { continue };
            let Ok(tables) = fs::read_dir(project.path()) else { continue };
            for table in tables.flatten() {
                let Some(tname) = table.file_name().to_str().map(Arc::<str>::from) else { continue };
                let Ok(entries) = fs::read_dir(table.path()) else { continue };
                for e in entries.flatten() {
                    let path = e.path();
                    // A `*.arrow.tmp` is ours and, at boot, definitionally dead
                    // (a demotion that never reached its rename).
                    if path.extension().is_some_and(|x| x == "tmp") {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    let Some(meta) = parse_meta(path, e.metadata().map(|m| m.len()).unwrap_or(0)) else { continue };
                    max_seq = max_seq.max(meta.seq + 1);
                    files += 1;
                    self.index.entry((pid.clone(), tname.clone())).or_default().insert((meta.bucket_id, meta.seq), meta);
                }
            }
        }
        self.seq.fetch_max(max_seq, Relaxed);
        if files > 0 {
            info!("hot tier rescan: {files} file(s) across {} table(s) in {:?}", self.index.len(), self.root);
        }
    }

    /// Unlink files whose newest row is older than the retention window (event
    /// time — the tier's membership rule is data recency), then oldest-first
    /// until under the disk cap. Both reduce to ONE age cutoff, so the sweep is
    /// a single `retain`; a disabled tier has no window at all, which makes the
    /// cutoff infinite and sweeps the directory clean.
    pub fn gc(&self, now_micros: i64) {
        let mut cutoff = self.retention.map_or(i64::MAX, |r| now_micros - r.as_micros() as i64);
        // The full file list is only worth building when actually over the cap.
        let total: u64 = self.index.iter().map(|e| e.values().filter(|m| m.end_ts > cutoff).map(|m| m.bytes).sum::<u64>()).sum();
        if total > self.limits.max_disk_bytes {
            let mut live: Vec<(i64, u64)> =
                self.index.iter().flat_map(|e| e.values().filter(|m| m.end_ts > cutoff).map(|m| (m.end_ts, m.bytes)).collect::<Vec<_>>()).collect();
            live.sort_unstable();
            let mut excess = total - self.limits.max_disk_bytes;
            for (end_ts, bytes) in live {
                if excess == 0 {
                    break;
                }
                excess = excess.saturating_sub(bytes);
                cutoff = end_ts;
            }
        }
        // Collect under the shard guards but unlink AFTER dropping them — N
        // unlink syscalls inside a write guard block every concurrent query's
        // range lookup for the duration.
        let (mut doomed, mut freed) = (Vec::new(), 0u64);
        for mut entry in self.index.iter_mut() {
            entry.value_mut().retain(|_, m| {
                if m.end_ts <= cutoff {
                    doomed.push(m.path.clone());
                    freed += m.bytes;
                    return false;
                }
                true
            });
        }
        self.index.retain(|_, v| !v.is_empty());
        if !doomed.is_empty() {
            self.gc_deleted.fetch_add(doomed.len() as u64, Relaxed);
            self.gc_bytes_freed.fetch_add(freed, Relaxed);
            debug!("hot tier GC: unlinked {} file(s), freed {freed} bytes", doomed.len());
            self.unlink(&doomed);
        }
    }

    pub fn stats(&self) -> HotTierStats {
        let (files, bytes) = self.index.iter().fold((0usize, 0u64), |(f, b), e| (f + e.len(), b + e.values().map(|m| m.bytes).sum::<u64>()));
        let now = crate::clock::now_micros();
        let mut suppressed: Vec<_> =
            self.health.iter().filter_map(|e| (e.until.load(Relaxed) > now).then(|| (e.key().clone(), (e.until.load(Relaxed) - now) / 1_000_000))).collect();
        suppressed.sort_unstable();
        let suppressed_tables = suppressed.len();
        suppressed.truncate(MAX_SUPPRESSED_ROWS);
        HotTierStats {
            suppressed_tables,
            suppressed,
            suppressions: self.suppressions.load(Relaxed),
            memo_files: self.decoded.len(),
            memo_bytes: self.memo_bytes.load(Relaxed),
            memo_evicted: self.memo_evicted.load(Relaxed),
            leg_budget_stops: self.leg_budget_stops.load(Relaxed),
            tables: self.index.len(),
            files,
            bytes,
            writes: self.writes.load(Relaxed),
            write_failures: self.write_failures.load(Relaxed),
            read_hits: self.read_hits.load(Relaxed),
            read_misses: self.read_misses.load(Relaxed),
            mem_skipped: self.mem_skipped.load(Relaxed),
            schema_drift: self.schema_drift.load(Relaxed),
            gc_deleted: self.gc_deleted.load(Relaxed),
            gc_bytes_freed: self.gc_bytes_freed.load(Relaxed),
            invalidated: self.invalidated.load(Relaxed),
            invalidations_ranged: self.invalidations_ranged.load(Relaxed),
            invalidations_full: self.invalidations_full.load(Relaxed),
        }
    }
}

/// The columns one query needs materialized out of a hot file.
struct LegColumns {
    /// Requested projection first, then any extra column the predicate reads.
    needed: Vec<usize>,
    /// `needed`'s schema — what the predicate is compiled against.
    schema: SchemaRef,
    /// Cut back to the requested columns after filtering; `None` when the
    /// predicate needed nothing extra.
    output: Option<Vec<usize>>,
}

/// `None` = no projection was pushed (or a column didn't resolve): keep every
/// column and filter full-width, exactly like the MemBuffer leg.
fn plan_columns(projection: Option<&[usize]>, filters: &[Expr], schema: &SchemaRef) -> Option<LegColumns> {
    let requested = projection?;
    let mut needed = requested.to_vec();
    for f in filters {
        for c in f.column_refs() {
            let i = schema.index_of(&c.name).ok()?;
            if !needed.contains(&i) {
                needed.push(i);
            }
        }
    }
    let output = (needed.len() > requested.len()).then(|| (0..requested.len()).collect());
    let schema = Arc::new(schema.project(&needed).ok()?);
    Some(LegColumns { needed, schema, output })
}

/// mmap + validate + decode. `require_alignment` is the arrow-rs knob that
/// turns a silent realigning COPY into an error — production reads leave it
/// off (correctness over strictness), tests turn it on to prove the
/// zero-copy assumption actually holds for files we write.
fn decode_file(path: &Path, require_alignment: bool) -> anyhow::Result<Vec<RecordBatch>> {
    let file = fs::File::open(path)?;
    // SAFETY: hot-tier files are immutable once renamed into place — we never
    // rewrite one, and GC only unlinks (the mapping survives the unlink), so
    // the mapped bytes cannot change under us.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let buffer = Buffer::from(bytes::Bytes::from_owner(mmap));
    let len = buffer.len();
    anyhow::ensure!(len >= MIN_FILE_LEN && &buffer[..6] == ARROW_MAGIC && &buffer[len - 6..] == ARROW_MAGIC, "missing ARROW1 magic (torn or foreign file)");
    let trailer_start = len - 10;
    let footer_len = read_footer_length(buffer[trailer_start..].try_into()?)?;
    anyhow::ensure!(footer_len <= trailer_start, "footer length {footer_len} exceeds file");
    let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).map_err(|e| anyhow::anyhow!("unparseable IPC footer: {e}"))?;
    let schema = fb_to_schema(footer.schema().ok_or_else(|| anyhow::anyhow!("IPC footer without schema"))?);
    let mut decoder = FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(require_alignment);

    let slice = |block: &Block| -> anyhow::Result<Buffer> {
        let (offset, block_len) = (block.offset() as usize, block.bodyLength() as usize + block.metaDataLength() as usize);
        anyhow::ensure!(offset.checked_add(block_len).is_some_and(|end| end <= len), "IPC block out of bounds (torn file)");
        Ok(buffer.slice_with_length(offset, block_len))
    };
    for block in footer.dictionaries().iter().flatten() {
        let data = slice(block)?;
        decoder.read_dictionary(block, &data)?;
    }
    let mut out = Vec::new();
    for block in footer.recordBatches().iter().flatten() {
        let data = slice(block)?;
        if let Some(batch) = decoder.read_record_batch(block, &data)? {
            out.push(batch);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use serial_test::serial;

    use super::*;

    fn batch(n: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())), Arc::new(StringArray::from((0..n).map(|i| format!("row{i}")).collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false), Field::new("name", DataType::Utf8, false)]))
    }

    fn limits(max_disk_bytes: u64) -> HotTierLimits {
        HotTierLimits { max_disk_bytes, leg_budget_bytes: u64::MAX, memo_bytes: u64::MAX }
    }

    fn open(dir: &tempfile::TempDir) -> Arc<HotTier> {
        HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX))
    }

    /// The whole design rests on mmap'd IPC decoding WITHOUT a realigning
    /// copy — `with_require_alignment(true)` makes a copy an error, so this
    /// asserts the 64-byte writer alignment survives the mmap round trip.
    #[test]
    fn roundtrip_is_zero_copy_and_lossless() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        let b = batch(64);
        tier.demote("p1", "otel_logs_and_spans", 7, std::slice::from_ref(&b), 100, 200);

        let metas = tier.buckets_in_range("p1", "otel_logs_and_spans", Some((150, 300)));
        assert_eq!(metas.len(), 1);
        // Stored half-open: the inclusive max 200 becomes end 201.
        assert_eq!((metas[0].bucket_id, metas[0].min_ts, metas[0].end_ts), (7, 100, 201));
        assert_eq!(decode_file(&metas[0].path, true).unwrap(), vec![b], "mmap'd IPC must decode zero-copy and byte-identical");
        // Out-of-range windows must not select it.
        assert!(tier.buckets_in_range("p1", "otel_logs_and_spans", Some((201, 300))).is_empty());
        assert!(tier.buckets_in_range("other", "otel_logs_and_spans", None).is_empty());
    }

    /// The hot leg projects to the requested columns BEFORE filtering, keeps a
    /// predicate-only column alive across the filter, and hands back exactly
    /// the requested shape plus the range it covered.
    #[tokio::test]
    async fn leg_projects_before_filtering_and_reports_its_range() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(8)], 10, 20);
        let filters = vec![datafusion::prelude::col("ts").gt(datafusion::prelude::lit(5i64))];
        // Project only `name`; `ts` is predicate-only and must not leak out.
        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[], &filters, &schema(), Some(&vec![1])).await;
        assert_eq!(ranges, vec![(10, 21)]);
        let batches: Vec<_> = parts.into_iter().flatten().collect();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "only ts=6,7 match");

        // A file MemBuffer still owns is skipped whole — no rows, no range, so
        // the window stays with the tier that has the fresher copy.
        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[(15, 25)], &filters, &schema(), None).await;
        assert!(parts.is_empty() && ranges.is_empty());
        assert_eq!(tier.stats().mem_skipped, 1);
    }

    #[test]
    fn torn_file_is_absent_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(32)], 10, 20);
        let path = tier.buckets_in_range("p1", "t", None)[0].path.clone();

        let full = fs::read(&path).unwrap();
        fs::write(&path, &full[..full.len() / 2]).unwrap();
        assert!(tier.read_file(&path).is_none(), "truncated file must read as absent");

        fs::write(&path, b"not an arrow file at all").unwrap();
        assert!(tier.read_file(&path).is_none());
        assert!(tier.read_file(Path::new("/definitely/not/here.arrow")).is_none());
        assert_eq!(tier.stats().read_misses, 3);
    }

    #[test]
    fn rescan_rebuilds_index_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(8)], 10, 20);
        tier.demote("p1", "t", 2, &[batch(8)], 30, 40);
        drop(tier);

        // Simulated restart: fresh instance over the same root.
        let restarted = open(&dir);
        let metas = restarted.buckets_in_range("p1", "t", None);
        assert_eq!(metas.iter().map(|m| (m.bucket_id, m.min_ts, m.end_ts)).collect::<Vec<_>>(), vec![(1, 10, 21), (2, 30, 41)]);
        assert_eq!(*restarted.read_file(&metas[0].path).unwrap(), vec![batch(8)]);
        // A post-restart demotion must not collide with a recovered seq.
        restarted.demote("p1", "t", 2, &[batch(8)], 41, 42);
        assert_eq!(restarted.buckets_in_range("p1", "t", None).len(), 3);
    }

    #[test]
    fn gc_expires_by_age_then_caps_disk_and_never_touches_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(1)), limits(u64::MAX));
        let now = 10_000_000i64;
        tier.demote("p1", "t", 1, &[batch(8)], 1, 2); // ancient
        tier.demote("p1", "t", 2, &[batch(8)], now - 1000, now); // fresh
        // Regression guard for ba8820e: a generic recursive deleter ate the WAL
        // quarantine dir. GC must only ever unlink our own `*.arrow` files.
        let foreign = dir.path().join("p1").join("t").join("do_not_delete.bin");
        fs::File::create(&foreign).unwrap().write_all(b"payload").unwrap();

        tier.gc(now);
        assert_eq!(tier.buckets_in_range("p1", "t", None).iter().map(|m| m.bucket_id).collect::<Vec<_>>(), vec![2], "only the aged-out file is unlinked");
        assert!(foreign.exists(), "GC must never delete a non-.arrow file under its own root");

        // Disk cap: 0 bytes allowed ⇒ everything goes, oldest first.
        let capped = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(0));
        capped.gc(now);
        assert!(capped.buckets_in_range("p1", "t", None).is_empty());
        assert!(foreign.exists());
        assert_eq!(capped.stats().files, 0);
    }

    /// A disabled tier is still responsible for its own directory: `open`
    /// sweeps whatever a previously-enabled run left, or the files leak
    /// forever, unbounded and invisible.
    #[test]
    fn disabled_tier_sweeps_instead_of_leaking() {
        let dir = tempfile::tempdir().unwrap();
        open(&dir).demote("p1", "t", 1, &[batch(8)], 10, 20);
        let foreign = dir.path().join("p1").join("t").join("do_not_delete.bin");
        fs::File::create(&foreign).unwrap().write_all(b"payload").unwrap();

        let off = HotTier::open(dir.path().to_path_buf(), None, limits(u64::MAX));
        assert_eq!(off.stats().files, 0, "a disabled tier must not strand the previous run's files");
        assert!(foreign.exists());
        off.demote("p1", "t", 2, &[batch(8)], 10, 20);
        assert_eq!(off.stats().files, 0, "and must not demote either");
    }

    #[test]
    fn invalidate_drops_every_file_for_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(8)], 10, 20);
        tier.demote("p2", "t", 1, &[batch(8)], 10, 20);
        tier.invalidate("p1", "t");
        assert!(tier.buckets_in_range("p1", "t", None).is_empty());
        assert_eq!(tier.buckets_in_range("p2", "t", None).len(), 1, "invalidation is per (project, table)");
    }

    /// A range-scoped invalidation drops only what the DML window overlaps, and
    /// the half-open convention decides the boundary cases: a file ENDING at
    /// the window's start, or STARTING at its end, shares no microsecond with
    /// it. Getting either wrong resurrects rows the DML corrected — silently,
    /// since the hot leg shadows Delta.
    #[test]
    fn invalidate_range_unlinks_only_overlapping_files_and_pins_the_half_open_edges() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        // Ranges stored half-open: demote(min, max) → [min, max+1).
        tier.demote("p1", "t", 1, &[batch(4)], 100, 199); // [100, 200) — abuts window start
        tier.demote("p1", "t", 2, &[batch(4)], 250, 350); // [250, 351) — overlaps
        tier.demote("p1", "t", 3, &[batch(4)], 400, 500); // [400, 501) — starts at window end
        tier.demote("p1", "t", 4, &[batch(4)], 600, 700); // far outside

        tier.invalidate_range("p1", "t", 200, 400);
        assert_eq!(
            tier.buckets_in_range("p1", "t", None).iter().map(|m| m.bucket_id).collect::<Vec<_>>(),
            vec![1, 3, 4],
            "only the overlapping file goes; end_ts == lo and min_ts == hi are disjoint"
        );
        let s = tier.stats();
        assert_eq!((s.invalidated, s.invalidations_ranged, s.invalidations_full), (1, 1, 0));

        // A window that covers everything behaves exactly like the full scope.
        tier.invalidate_range("p1", "t", i64::MIN, i64::MAX);
        assert!(tier.buckets_in_range("p1", "t", None).is_empty());
        assert_eq!(tier.stats().invalidated, 4);
    }

    /// One-sided bounds are the common enrichment shape (`timestamp >= X` with
    /// no upper): the missing side is infinite, and the bound we DO have must
    /// still scope the invalidation.
    #[test]
    fn one_sided_window_still_scopes_the_invalidation() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(4)], 100, 199);
        tier.demote("p1", "t", 2, &[batch(4)], 300, 400);

        tier.invalidate_range("p1", "t", 250, i64::MAX);
        assert_eq!(tier.buckets_in_range("p1", "t", None).iter().map(|m| m.bucket_id).collect::<Vec<_>>(), vec![1]);
        tier.invalidate_range("p1", "t", i64::MIN, 150);
        assert!(tier.buckets_in_range("p1", "t", None).is_empty());
    }

    /// The wasted/judge accounting is charged by the range scope too — but only
    /// for the files it actually dropped, so a table whose DML windows miss its
    /// demoted files is never convicted.
    #[test]
    fn range_invalidation_charges_waste_only_for_files_it_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", i, &[batch(4)], 10, 20);
            // A DML window nowhere near the demoted rows: nothing dropped,
            // nothing wasted.
            tier.invalidate_range("p1", "t", 10_000, 20_000);
        }
        let s = tier.stats();
        assert_eq!((s.files, s.suppressions, s.invalidated), (PROBE_DEMOTES as usize, 0, 0));

        // An overlapping window IS waste, and convicts exactly as the full
        // scope does (the probe window is already full of never-read files).
        tier.invalidate_range("p1", "t", 15, 25);
        let s = tier.stats();
        assert_eq!((s.files, s.invalidated, s.suppressions), (0, PROBE_DEMOTES, 1));
    }

    /// Continuous whole-table enrichment (monoscope writes `hashes` over all of
    /// `otel_logs_and_spans`) invalidates every demoted file before a query can
    /// read it. The tier must notice and stop paying for those writes.
    #[test]
    fn wasted_demotions_suppress_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", i, &[batch(4)], 10, 20);
            tier.invalidate("p1", "t");
        }
        let s = tier.stats();
        assert_eq!((s.suppressions, s.suppressed_tables, s.writes), (1, 1, PROBE_DEMOTES));
        assert_eq!(s.suppressed[0].0, (Arc::from("p1"), Arc::from("t")));

        for i in 0..10 {
            tier.demote("p1", "t", 100 + i, &[batch(4)], 10, 20);
        }
        let s = tier.stats();
        assert_eq!((s.files, s.writes), (0, PROBE_DEMOTES), "a suppressed table writes nothing");
        // Another table is judged entirely on its own files.
        tier.demote("p2", "t", 1, &[batch(4)], 10, 20);
        assert_eq!(tier.stats().files, 1);
    }

    /// A table nobody mutates is never suppressed, however much it demotes.
    #[test]
    fn untouched_table_is_never_suppressed() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..50i64 {
            tier.demote("p1", "t", i, &[batch(2)], i * 100, i * 100 + 50);
        }
        let s = tier.stats();
        assert_eq!((s.files, s.suppressions, s.suppressed_tables), (50, 0, 0));
    }

    /// Suppression is a cooldown, not a death sentence: a table whose
    /// enrichment stops recovers on its own, with no restart and no config.
    #[serial]
    #[test]
    fn suppression_lifts_after_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        let t0 = crate::clock::set_micros(crate::clock::now_micros());
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", i, &[batch(4)], 10, 20);
            tier.invalidate("p1", "t");
        }
        assert_eq!(tier.stats().suppressions, 1);
        tier.demote("p1", "t", 10, &[batch(4)], 10, 20);
        assert_eq!(tier.stats().files, 0, "still inside the cooldown");

        crate::clock::set_micros(t0 + SUPPRESSION_COOLDOWN.as_micros() as i64 + 1);
        tier.demote("p1", "t", 11, &[batch(4)], 10, 20);
        let s = tier.stats();
        assert_eq!((s.files, s.suppressed_tables), (1, 0), "cooldown elapsed → demotion resumes");
        // ...and if the workload hasn't changed, one wasted probe file is
        // enough to re-convict (REPROBE_DEMOTES), so the tier spends ~one file
        // per cooldown instead of one per flush.
        tier.invalidate("p1", "t");
        assert_eq!(tier.stats().suppressions, 2);
        crate::clock::unfreeze();
    }

    /// Suppression only gates WRITES: files already on disk keep serving.
    #[tokio::test]
    async fn suppression_does_not_affect_reads() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..PROBE_DEMOTES as i64 - 1 {
            tier.demote("p1", "t", i, &[batch(4)], 10, 20);
            tier.invalidate("p1", "t");
        }
        // Convicted by this write (3 wasted of 4), but the file itself stays.
        tier.demote("p1", "t", 3, &[batch(4)], 10, 20);
        let s = tier.stats();
        assert_eq!((s.suppressed_tables, s.files), (1, 1));

        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert_eq!(ranges, vec![(10, 21)]);
        assert_eq!(parts.into_iter().flatten().map(|b| b.num_rows()).sum::<usize>(), 4);
        assert_eq!(tier.stats().read_hits, 1);
    }

    /// The leg is planned eagerly into an unpooled, ungated `MemorySourceConfig`,
    /// so a deep scan must never consult it at all — a 14d dashboard would
    /// otherwise materialize the ENTIRE hot window before execution starts.
    #[test]
    fn deep_scans_skip_the_hot_leg_entirely() {
        let hour = 3_600_000_000i64;
        let six_h = 6 * hour;
        assert!(!skip_for_lookback(Some(hour), six_h), "a 1h dashboard is exactly what the tier is for");
        assert!(!skip_for_lookback(Some(3 * hour), six_h), "3h too");
        assert!(!skip_for_lookback(Some(six_h), six_h), "the window's own edge is still served");
        assert!(skip_for_lookback(Some(six_h + 1), six_h), "one micro past the window and the tier is a fraction of the answer");
        assert!(skip_for_lookback(Some(14 * 24 * hour), six_h), "14d");
        assert!(skip_for_lookback(None, six_h), "no lower bound = infinitely deep");
        // Tier off (retention 0): nothing is ever consulted, however shallow.
        assert!(skip_for_lookback(Some(1), 0) && skip_for_lookback(None, 0));
    }

    /// The per-scan byte budget, and the invariant that makes it SAFE: a file
    /// the budget refuses contributes neither rows NOR its exclusion range. If
    /// the two ever diverge, that window is excluded from the Delta leg and
    /// served by nobody — silent row loss.
    #[tokio::test]
    async fn leg_budget_stops_rows_and_ranges_together() {
        let dir = tempfile::tempdir().unwrap();
        // Budget for ~2 files: measure one file's post-filter footprint first.
        let probe = tempfile::tempdir().unwrap();
        let t0 = HotTier::open(probe.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX));
        t0.demote("p1", "t", 0, &[batch(64)], 0, 10);
        let (parts, _) = t0.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        let one: u64 = parts.into_iter().flatten().map(|b| b.get_array_memory_size() as u64).sum();

        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { leg_budget_bytes: one * 2, ..limits(u64::MAX) });
        for i in 0..5i64 {
            tier.demote("p1", "t", i, &[batch(64)], i * 100, i * 100 + 10);
        }
        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert_eq!(parts.len(), 2, "the budget admits two files' worth and stops");
        assert_eq!(ranges, vec![(0, 11), (100, 111)], "exactly the admitted files' ranges are excluded from Delta — no more");
        assert_eq!(parts.len(), ranges.len(), "one range per admitted partition, always");
        assert!(parts.iter().flatten().map(|b| b.get_array_memory_size() as u64).sum::<u64>() <= one * 2, "retained bytes stay under budget");
        assert_eq!(tier.stats().leg_budget_stops, 1);

        // Budget edge: a budget smaller than ONE file admits nothing at all —
        // and, critically, excludes nothing, so every window falls to Delta.
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { leg_budget_bytes: 1, ..limits(u64::MAX) });
        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert!(parts.is_empty() && ranges.is_empty(), "no rows AND no exclusion — the whole window must fall through to Delta");
    }

    /// The decode memo pins one mmap per entry for the entry's lifetime, so an
    /// unbounded memo means one wide scan pins the whole tier. It must be
    /// capped in bytes and evict least-recently-used.
    #[test]
    fn memo_is_byte_capped_and_evicts_lru() {
        let dir = tempfile::tempdir().unwrap();
        let big = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX));
        for i in 0..3i64 {
            big.demote("p1", "t", i, &[batch(64)], i * 100, i * 100 + 10);
        }
        let metas = big.buckets_in_range("p1", "t", None);
        let file_bytes = metas[0].bytes;

        // Cap at two files. Read 0, 1, 2 — the first read must be the victim.
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { memo_bytes: file_bytes * 2, ..limits(u64::MAX) });
        let metas = tier.buckets_in_range("p1", "t", None);
        for m in &metas {
            assert!(tier.read_file(&m.path).is_some());
        }
        let s = tier.stats();
        assert_eq!((s.memo_files, s.memo_evicted), (2, 1), "the memo holds exactly its cap, evicting one");
        assert!(s.memo_bytes <= file_bytes * 2 && s.memo_bytes > 0, "memo_bytes tracks the map: {s:?}");
        assert!(!tier.decoded.contains_key(&metas[0].path), "the least-recently-used entry is the one dropped");
        assert!(tier.decoded.contains_key(&metas[2].path));
        // Touching 1 makes 2 the LRU; the next insert must then evict 2.
        tier.read_file(&metas[1].path).unwrap();
        tier.read_file(&metas[0].path).unwrap();
        assert!(!tier.decoded.contains_key(&metas[2].path), "recency, not insertion order, decides the victim");
        assert_eq!(tier.stats().memo_files, 2);

        // Unlinking an entry gives its bytes back — the accounting can't drift.
        tier.unlink(&metas.iter().map(|m| m.path.clone()).collect::<Vec<_>>());
        let s = tier.stats();
        assert_eq!((s.memo_files, s.memo_bytes), (0, 0));
    }

    /// A file decoding to ZERO batches would pass the schema check (`first()`
    /// is None), push its range, and contribute no rows — excluding that window
    /// from Delta while serving nothing. `demote` rejects all-empty input, so
    /// this is unreachable today; the coverage contract must not rest on that.
    #[tokio::test]
    async fn zero_batch_file_contributes_no_range() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", 1, &[batch(8)], 10, 20);
        let path = tier.buckets_in_range("p1", "t", None)[0].path.clone();

        // A well-formed IPC file carrying the schema and no record batches.
        let f = fs::File::create(&path).unwrap();
        FileWriter::try_new_with_options(f, schema().as_ref(), IpcWriteOptions::default()).unwrap().finish().unwrap();
        assert_eq!(tier.read_file(&path).unwrap().len(), 0, "it decodes fine — it is just empty");

        let (parts, ranges) = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert!(parts.is_empty(), "no rows");
        assert!(ranges.is_empty(), "and NO exclusion range: the window must fall through to Delta, not vanish");
        assert_eq!(tier.stats().read_misses, 1, "counted as a miss, like any other unusable file");
    }

    /// A project/table name that can't round-trip through a path component is
    /// silently NOT demoted — a lossy mapping could collide two tenants in one
    /// directory, and rescan reconstructs the key from the directory name.
    #[test]
    fn unsafe_names_are_not_demoted() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("../escape", "t", 1, &[batch(8)], 10, 20);
        tier.demote("p1", "sub/dir", 1, &[batch(8)], 10, 20);
        assert_eq!(tier.stats().files, 0);
        assert_eq!(tier.stats().write_failures, 0, "an unsafe name is a skip, not a failure");
    }
}
