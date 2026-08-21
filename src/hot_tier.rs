//! Local cache of committed MemBuffer buckets.
//!
//! Files are immutable, uncompressed Arrow IPC written only after Delta commits.
//! DataFusion decodes them at execution time inside the query memory pool. Disk
//! pressure evicts the oldest files; there is no time-based retention.
//!
//! This tier is never a durability boundary. Writes are best effort, and an
//! unreadable file is absent so reads fall through to Delta. GC only removes
//! cache-named `*.arrow` files beneath its own root.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, SchemaRef, TimeUnit},
};
use arrow_ipc::{writer::FileWriter, writer::IpcWriteOptions};
use dashmap::{DashMap, DashSet};
use datafusion::{error::Result as DFResult, logical_expr::Expr, physical_expr::PhysicalExpr, physical_plan::ExecutionPlan};
use tracing::{debug, info, warn};

use crate::write::mem_buffer::{TableKey, compile_filter_conjunction, overlaps, table_key};

const EXT: &str = "arrow";
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";
/// magic + minimal footer + trailer; anything shorter is definitionally torn.
const MIN_FILE_LEN: usize = 6 + 10;
/// The tier's only ceiling — an operator knob
/// (`TIMEFUSION_HOT_TIER_MAX_DISK_GB`) because it bounds the WAL/data volume
/// this box has already been killed by. Query heap needs no knob of its own
/// since `HotTier::scan` streams inside the query's memory pool.
#[derive(Clone, Copy, Debug)]
pub struct HotTierLimits {
    /// Directory cap; over it GC unlinks oldest-first.
    pub max_disk_bytes: u64,
    /// Merge-on-demote: a versioned bucket that already has demoted files is
    /// rewritten (old stack + new drain, keep-greatest) into ONE covering file
    /// whose gate is the newest stamp. Kill switch falls back to STACKED files
    /// (coverage still stands, gate still advances via `plan_leg`) — never to
    /// the old coverage-voiding behaviour.
    pub merge_demote: bool,
}

impl Default for HotTierLimits {
    fn default() -> Self {
        Self { max_disk_bytes: 64 << 30, merge_demote: true }
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
            until if crate::support::now_micros() < until => true,
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
/// entirely? Only an UNBOUNDED scan must: with no lower time bound the query
/// reaches all of history, of which the tier holds a vanishing fraction, and it
/// would still pay to list every file the tier has.
///
/// This used to also reject anything deeper than 2x the retention window,
/// because the leg was materialized EAGERLY at plan time — a 7d/14d scan pulled
/// the whole tier into un-pooled heap to shave a handful of files off a scan
/// dominated by thousands. `HotTier::scan` streams now (2026-08-14), so a deep
/// scan costs only the metadata walk (footer validation is memoized per file),
/// and every window the tier covers is one that does NOT go to R2. Serving that
/// is the entire point of the tier.
///
/// Keying the gate on the tier's MEASURED span was tried and is strictly worse:
/// the span is ~0 while the tier is young, so a freshly-started process refuses
/// to use its own tier — and the tier can never become useful, because nothing
/// reads it. A depth proxy must not be able to deadlock the thing it guards.
pub fn skip_for_lookback(lookback: Option<i64>) -> bool {
    lookback.is_none()
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
    /// This file's rows are ordered by the table's declared `sorting_columns`.
    /// Recorded per FILE, in the name (an `s` on the seq component), because
    /// the read side turns it into a `try_with_sort_information` claim: a file
    /// left by an older binary, or one whose batches would not sort, must be
    /// able to say so rather than be assumed ordered. Any unsorted file in a
    /// leg retracts the claim for the whole leg.
    pub sorted: bool,
    /// Greatest `dedup_tiebreak` (version stamp) among the file's rows, on a
    /// `version_append` table. It is what lets the file keep excluding its
    /// window from Delta while merge-on-read keeps appending newer versions
    /// INTO that window: everything at or below this stamp is already in this
    /// file, so only strictly-greater stamps need reading from Delta (see
    /// `version_gate`). `None` = unknown (file demoted before the table opted
    /// in, or by an older binary) — the gate then admits every stamped row,
    /// which is conservative, never wrong.
    pub max_stamp: Option<i64>,
    /// True only when this file holds EVERY row in `[min_ts, end_ts]` at or
    /// below `max_stamp` — the precondition the Delta-scan range exclusion
    /// silently assumed and nothing enforced (prod 2026-08-07: a window read
    /// 4322 rows against a true 11349, because the file claimed its whole span
    /// while holding only the rows one drain happened to carry).
    ///
    /// Set by the writer when a FULL bucket drain produced the file. Absent on
    /// files from an older binary, which is the safe default: no claim, so the
    /// window falls through to Delta. Necessary but NOT sufficient — a bucket
    /// demoted more than once holds its span across several files, and
    /// `plan_leg` grants the window only when EVERY file of the bucket is
    /// served and every one carries this mark (merge-on-demote collapses such
    /// stacks back to one file when it can).
    pub covers_window: bool,
    pub path: PathBuf,
}

impl HotBucketMeta {
    fn range(&self) -> (i64, i64) {
        (self.min_ts, self.end_ts)
    }
}

/// What the hot tier contributes to one scan.
#[derive(Debug, Default)]
pub struct HotLeg {
    /// The leg's scan — projected, filtered, and streamed from disk at execute
    /// time. `None` when the tier contributes nothing. The plan carries its own
    /// ordering claim, so that claim cannot desynchronise from the rows it
    /// describes (the lesson `wrap_result` records for leg identity).
    pub plan: Option<Arc<dyn ExecutionPlan>>,
    /// Windows the served files are authoritative for; the caller excludes them
    /// from the Delta leg.
    pub ranges: Vec<(i64, i64)>,
    /// Merge-on-read gate. Under `version_append` an UPDATE appends a NEW
    /// version carrying the row's ORIGINAL timestamp, so it lands in Delta
    /// *inside* one of `ranges` and a plain range exclusion would hide it. The
    /// caller therefore weakens each exclusion with `OR stamp > gate`: at-or
    /// below the gate the hot files already hold the newest version, above it
    /// only Delta does. `None` = the table does not append versions, so no row
    /// in an excluded window can be newer than the file that owns it and the
    /// exclusion stands unweakened.
    pub version_gate: Option<i64>,
}

impl HotLeg {
    pub fn is_empty(&self) -> bool {
        self.plan.is_none() && self.ranges.is_empty()
    }

    /// Run the leg and collect its rows. Only for callers that genuinely need
    /// BATCHES rather than a plan — the count-pushdown overlay, which merges hot
    /// rows into a cached winner set. The query path must use `plan` and let
    /// DataFusion stream it. A failure yields no rows, which for that overlay
    /// means falling back to the Delta base: slower, never wrong.
    pub async fn collect(&self) -> Vec<RecordBatch> {
        let Some(plan) = self.plan.clone() else { return Vec::new() };
        let ctx = datafusion::prelude::SessionContext::new();
        datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap_or_else(|e| {
            warn!("hot leg could not be collected ({e}); the overlay falls back to the Delta base");
            Vec::new()
        })
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
    /// Windows served WITHOUT claiming a Delta exclusion because coverage was
    /// not proven. These are the reads that would previously have lost rows.
    pub unproven_windows: u64,
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
    /// The suppressed tables and their remaining cooldown in seconds, capped at
    /// [`MAX_SUPPRESSED_ROWS`].
    pub suppressed: Vec<(TableKey, i64)>,
}

#[derive(Default)]
pub struct HotTier {
    root: PathBuf,
    /// `None` = tier off: nothing is demoted and GC sweeps the directory
    /// clean, so a disabled tier can never strand a previous run's files.
    /// The tier's off switch. There is no time retention: what the tier holds
    /// is decided by `limits.max_disk_bytes` and oldest-first GC.
    enabled: bool,
    limits: HotTierLimits,
    /// (project, table) → its files, ordered by (bucket_id, seq). A bucket can
    /// own several files: late arrivals into an already-drained bucket flush
    /// again and demote a second, disjoint row set.
    index: DashMap<TableKey, BTreeMap<(i64, u64), HotBucketMeta>>,
    /// Paths replaced by a merge-on-demote rewrite, awaiting a DEFERRED unlink
    /// in `gc`. Unlinking inline would race a scan that planned against the old
    /// metas and opens files at EXECUTE time (GC's own unlinks target files old
    /// enough to be out of every live plan; a just-merged bucket is exactly the
    /// opposite). Index removal is immediate, so no new plan serves them.
    retired: parking_lot::Mutex<Vec<PathBuf>>,
    /// Buckets whose demotion was SKIPPED, so no file of theirs may ever claim
    /// a window.
    ///
    /// `covers_window` says "this file holds every row the bucket carried",
    /// which is a claim about ONE incarnation — a bucket re-forms when late
    /// rows arrive after a full drain. `plan_leg` catches multiple incarnations
    /// by counting a bucket's files, but a skipped demote leaves NO file, so
    /// the count cannot see it: incarnation A skipped and B demoted looks
    /// exactly like a single fully-covered drain, and the survivor excludes
    /// from Delta a window whose earlier rows it never received.
    ///
    /// Durable, because the omission is: the rows are in Delta and the file is
    /// not, and a restart must not turn that back into a claim.
    uncovered: DashSet<(TableKey, i64)>,
    /// Files served to a scan since boot — the signal for whether demotion paid
    /// off before DML invalidated a file.
    read_files: DashSet<PathBuf>,
    /// Paths whose footer has already been validated. Hot files are IMMUTABLE
    /// once renamed into place, so a pass is permanent and the tail read is
    /// worth doing exactly once. Without this the plan-time probe reopens every
    /// candidate file on EVERY scan — tolerable at ~50 files, not once the tier
    /// is bounded only by a 600GB disk and a deep scan can span thousands.
    /// Entries are dropped with the file (`unlink`) and on `rescan`.
    validated: DashSet<PathBuf>,
    health: DashMap<TableKey, Arc<DemotionHealth>>,
    seq: AtomicU64,
    suppressions: AtomicU64,
    writes: AtomicU64,
    write_failures: AtomicU64,
    read_hits: AtomicU64,
    read_misses: AtomicU64,
    mem_skipped: AtomicU64,
    unproven_windows: AtomicU64,
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

/// Readable entries of one directory; an unreadable directory is simply empty
/// (the tier is a cache — a walk failure costs warmth, never correctness).
fn dir_entries(dir: impl AsRef<Path>) -> impl Iterator<Item = fs::DirEntry> {
    fs::read_dir(dir).into_iter().flatten().flatten()
}

/// A directory entry's name (as a `TableKey` half) and path, if it is UTF-8.
fn named(d: fs::DirEntry) -> Option<(Arc<str>, PathBuf)> {
    Some((Arc::<str>::from(d.file_name().to_str()?), d.path()))
}

/// `(end_ts, bytes)` of the files a GC pass would keep on age alone.
fn live_files(files: &BTreeMap<(i64, u64), HotBucketMeta>, cutoff: i64) -> impl Iterator<Item = (i64, u64)> + '_ {
    files.values().filter(move |m| m.end_ts > cutoff).map(|m| (m.end_ts, m.bytes))
}

/// `retain` that hands back what it removed — the shape both invalidation and
/// GC need (drop from the index, then unlink the files off the lock).
fn drop_unless(files: &mut BTreeMap<(i64, u64), HotBucketMeta>, keep: impl Fn(&HotBucketMeta) -> bool) -> Vec<HotBucketMeta> {
    let mut dropped = Vec::new();
    files.retain(|_, m| {
        keep(m) || {
            dropped.push(m.clone());
            false
        }
    });
    dropped
}

/// [`drop_unless`], widened so a bucket is always dropped whole.
///
/// The files of a twice-drained bucket are only JOINTLY authoritative for its
/// span, which `plan_leg` enforces by refusing the claim whenever a bucket has
/// more than one file. Deleting one of them from the index defeats that gate:
/// the survivor then looks like a single-file bucket and reclaims the whole
/// span, so its deleted sibling's rows are missing from the hot leg *and*
/// excluded from the Delta leg that still holds them — a query that silently
/// under-reports.
///
/// Over-deleting is safe in a way under-deleting is not: an extra dropped file
/// costs one fall-through to Delta, which is always correct.
fn drop_unless_bucket_atomic(files: &mut BTreeMap<(i64, u64), HotBucketMeta>, keep: impl Fn(&HotBucketMeta) -> bool) -> Vec<HotBucketMeta> {
    let doomed: std::collections::HashSet<i64> = files.values().filter(|m| !keep(m)).map(|m| m.bucket_id).collect();
    drop_unless(files, |m| !doomed.contains(&m.bucket_id))
}

fn parse_meta(path: PathBuf, bytes: u64) -> Option<HotBucketMeta> {
    if path.extension()?.to_str()? != EXT {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    // The stamp is an OPTIONAL 5th component so a tier written before the
    // version gate existed still rescans (as `max_stamp: None`), and so a
    // rollback to a binary that rejects it degrades to "file absent" rather
    // than to a mis-parsed range.
    let (bucket_id, min_ts, end_ts, seq, stamp) = match stem.split('_').collect::<Vec<_>>()[..] {
        [bucket_id, min_ts, end_ts, seq] => (bucket_id, min_ts, end_ts, seq, None),
        [bucket_id, min_ts, end_ts, seq, stamp] => (bucket_id, min_ts, end_ts, seq, Some(stamp)),
        _ => return None,
    };
    // A trailing `s` on the seq component marks a SORTED file. Its absence is
    // what a pre-sort binary's files (and unsortable batches) look like, so the
    // default is the safe one: no ordering claim.
    // Markers ride on the seq component, order `<n>[s][c]`. Absence is what a
    // pre-marker binary's files look like, so both defaults are the safe ones:
    // no ordering claim, and no coverage claim.
    let covers_window = seq.ends_with('c');
    let seq = seq.trim_end_matches('c');
    let sorted = seq.ends_with('s');
    Some(HotBucketMeta {
        bucket_id: bucket_id.parse().ok()?,
        min_ts: min_ts.parse().ok()?,
        end_ts: end_ts.parse().ok()?,
        seq: seq.trim_end_matches('s').parse().ok()?,
        max_stamp: stamp.map(str::parse).transpose().ok()?,
        sorted,
        covers_window,
        bytes,
        path,
    })
}

/// Greatest non-null value of `column` across `batches`, as micros. `None` when
/// the column is absent, not a microsecond timestamp, or entirely null — every
/// one of which means "this file cannot vouch for any stamp", which the gate
/// reads as "admit everything".
fn max_stamp_of(batches: &[RecordBatch], column: &str) -> Option<i64> {
    use arrow::{array::AsArray, datatypes::TimestampMicrosecondType};
    batches
        .iter()
        .filter_map(|b| {
            let col = b.column_by_name(column)?;
            matches!(col.data_type(), DataType::Timestamp(TimeUnit::Microsecond, _))
                .then(|| datafusion::arrow::compute::max(col.as_primitive::<TimestampMicrosecondType>()))
                .flatten()
        })
        .max()
}

/// One MemBuffer bucket handed to the tier. A parameter struct because the
/// five fields travel together through `demote` → `write_bucket` and are
/// trivially transposable positionally (three `i64`s in a row).
pub struct Bucket<'a> {
    pub bucket_id: i64,
    pub batches: &'a [RecordBatch],
    pub min_ts: i64,
    /// Inclusive at `demote`, exclusive at `write_bucket`.
    pub max_ts: i64,
    pub covers_window: bool,
}

impl HotTier {
    /// Open (creating if needed) a hot tier at `root` and rebuild its index
    /// from whatever survived the last process — restart warmth. `retention`
    /// and `limits` are the tier's entire policy; it enforces them itself, so
    /// callers only ever say `gc(now)`.
    pub fn open(root: PathBuf, enabled: bool, limits: HotTierLimits) -> Arc<Self> {
        let tier = Self::open_lazy(root, enabled, limits);
        tier.finish_open();
        tier
    }

    /// Construct the cache without walking its directory. Until
    /// [`Self::finish_open`] completes, missing index entries simply fall
    /// through to Delta, so application readiness need not wait on a large
    /// derived-cache scan.
    pub fn open_lazy(root: PathBuf, enabled: bool, limits: HotTierLimits) -> Arc<Self> {
        let tier = Arc::new(Self { root, enabled, limits, ..Default::default() });
        if let Err(e) = fs::create_dir_all(&tier.root) {
            warn!("hot tier disabled for this process — cannot create {:?}: {e}", tier.root);
        }
        tier
    }

    /// Complete a lazy open by rebuilding the derived index and enforcing the
    /// disabled-tier cleanup policy. Safe to run while queries are served:
    /// the index maps are concurrent and an entry not loaded yet falls through
    /// to the authoritative Delta leg.
    pub fn finish_open(&self) {
        self.rescan();
        self.load_uncovered();
        if !self.enabled {
            // Disabled, but still responsible for its own directory: without
            // this a previous run's files sit there forever, unbounded and
            // invisible (the disk-leak failure mode of the orphaned spill dirs
            // and the never-evicted tantivy cache).
            let paths: Vec<PathBuf> = self.index.iter().flat_map(|e| e.value().values().map(|m| m.path.clone()).collect::<Vec<_>>()).collect();
            self.index.clear();
            self.gc_deleted.fetch_add(paths.len() as u64, Relaxed);
            self.unlink(&paths);
        }
    }

    /// Demote one committed bucket, whose rows span the INCLUSIVE `[min_ts,
    /// max_ts]` MemBuffer tracks. Best-effort: a failure is counted and
    /// logged, never propagated (the rows are already durable in Delta).
    pub fn demote(&self, project_id: &str, table_name: &str, bucket: Bucket<'_>) {
        let Bucket { bucket_id, batches, min_ts, max_ts, covers_window } = bucket;
        if !self.enabled || min_ts > max_ts || batches.iter().all(|b| b.num_rows() == 0) {
            return;
        }
        let key = table_key(project_id, table_name);
        let health = self.health(&key);
        if health.suppressed() {
            return;
        }
        // Merge-on-demote: a versioned bucket that already has demoted files is
        // rewritten (existing stack + this drain, keep-greatest per dedup key)
        // into ONE file, so the per-key version fan-out leaves the tier at
        // write time and the gate advances to this drain's stamp. Soundness:
        // - ANY decode failure aborts the whole merge and falls back to a plain
        //   stacked write. A torn base treated as absent would otherwise mint a
        //   `covers_window` file MISSING the base's rows — rows served from
        //   nowhere, the one way this cache could break correctness.
        // - The merged file claims coverage only if EVERY constituent did and
        //   the bucket was never `uncovered` (a skipped demote is a gap no
        //   merge can recover).
        // - Old paths are retired to a DEFERRED unlink (see `retired`); index
        //   removal is immediate. `SortByDedup`-style collapse is safe for the
        //   same reason as compaction's: everything dropped loses to a kept
        //   survivor of the same key.
        let stack: Vec<HotBucketMeta> = self.index.get(&key).map(|e| e.values().filter(|m| m.bucket_id == bucket_id).cloned().collect()).unwrap_or_default();
        let schema_def = crate::schema::get_schema(table_name).filter(|s| s.version_append && !s.dedup_keys.is_empty());
        let merged: Option<(Vec<RecordBatch>, i64, i64, bool)> =
            (self.limits.merge_demote && !stack.is_empty()).then_some(()).and(schema_def).and_then(|schema| {
                let old: anyhow::Result<Vec<RecordBatch>> = stack.iter().try_fold(Vec::new(), |mut acc, m| {
                    let f = fs::File::open(&m.path)?;
                    for b in arrow_ipc::reader::FileReader::try_new(std::io::BufReader::new(f), None)? {
                        acc.push(b?);
                    }
                    Ok(acc)
                });
                let mut all = match old {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("hot tier merge-on-demote aborted for {project_id}.{table_name} bucket {bucket_id} (stacked write instead): {e:#}");
                        return None;
                    }
                };
                all.extend_from_slice(batches);
                let collapsed = crate::write::mem_buffer::dedup_batches(all, &schema.dedup_keys, schema.dedup_tiebreak.as_deref(), None).ok()?;
                let merged_min = stack.iter().map(|m| m.min_ts).min().unwrap_or(min_ts).min(min_ts);
                let merged_end = stack.iter().map(|m| m.end_ts).max().unwrap_or(0).max(max_ts + 1);
                let covers = covers_window && stack.iter().all(|m| m.covers_window) && !self.uncovered.contains(&(key.clone(), bucket_id));
                Some((collapsed, merged_min, merged_end, covers))
            });
        let write = match &merged {
            Some((collapsed, merged_min, merged_end, covers)) => {
                Bucket { bucket_id, batches: collapsed, min_ts: *merged_min, max_ts: *merged_end - 1, covers_window: *covers }
            }
            None => Bucket { bucket_id, batches, min_ts, max_ts, covers_window },
        };
        match self.write_bucket(project_id, table_name, Bucket { max_ts: write.max_ts + 1, ..write }) {
            Ok(Some(meta)) => {
                self.writes.fetch_add(1, Relaxed);
                health.demoted.fetch_add(1, Relaxed);
                let mut entry = self.index.entry(key.clone()).or_default();
                entry.insert((meta.bucket_id, meta.seq), meta);
                if merged.is_some() {
                    for m in &stack {
                        entry.remove(&(m.bucket_id, m.seq));
                    }
                    drop(entry);
                    self.retired.lock().extend(stack.iter().map(|m| m.path.clone()));
                } else {
                    drop(entry);
                }
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
        h.until.store(crate::support::now_micros() + SUPPRESSION_COOLDOWN.as_micros() as i64, Relaxed);
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

    /// `max_ts` is the EXCLUSIVE end here — `demote` converts its inclusive
    /// bound before handing over.
    fn write_bucket(&self, project_id: &str, table_name: &str, bucket: Bucket<'_>) -> anyhow::Result<Option<HotBucketMeta>> {
        let Bucket { bucket_id, batches, min_ts, max_ts: end_ts, covers_window } = bucket;
        if !safe_component(project_id) || !safe_component(table_name) {
            debug!("hot tier skipping {project_id}.{table_name}: name is not a safe path component");
            return Ok(None);
        }
        // Sort ONCE, here. These are the MemBuffer bucket's own batches
        // (`demote_drained` hands over `FlushableBucket.batches`), so this is
        // the single point where the rows are frozen and can be ordered for
        // every later reader of the tier — off the insert path, off the query
        // path, and off the flush's critical path (demotion already runs in a
        // spawn_blocking behind its own permit).
        //
        // Best-effort: an unsortable batch set is still worth demoting, it just
        // does not carry the `s` marker and so contributes no ordering claim.
        let owned = crate::schema::get_schema(table_name)
            .filter(|s| !s.sorting_columns.is_empty())
            .and_then(|s| crate::write::mem_buffer::sort_partition(s, batches.to_vec()));
        let (batches, sorted) = owned.as_deref().map_or((batches, false), |b| (b, true));
        let Some(schema) = batches.first().map(|b| b.schema()) else { return Ok(None) };
        let dir = self.root.join(project_id).join(table_name);
        fs::create_dir_all(&dir)?;
        let seq = self.seq.fetch_add(1, Relaxed);
        // The stamp rides in the FILENAME so `rescan` recovers the gate after a
        // restart without reopening every file.
        let max_stamp =
            crate::schema::get_schema(table_name).filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone()).and_then(|c| max_stamp_of(batches, &c));
        let stamp_suffix = max_stamp.map(|s| format!("_{s}")).unwrap_or_default();
        let sort_marker = if sorted { "s" } else { "" };
        let covers_marker = if covers_window { "c" } else { "" };
        let path = dir.join(format!("{bucket_id}_{min_ts}_{end_ts}_{seq}{sort_marker}{covers_marker}{stamp_suffix}.{EXT}"));
        crate::write::wal::write_atomic_with(&path, true, |f| {
            // UNCOMPRESSED, deliberately (2026-08-14). LZ4 bought hours per GB
            // on a 128GB cap, but the box has 1.1TB free, so the cap was the
            // wrong constraint to optimise against. Uncompressed restores the
            // module's founding property: batches are zero-copy views over the
            // file rather than freshly allocated decompressions, which turns the
            // tier's read heap from ANON (only the OOM killer can reclaim it)
            // into page cache (the kernel just drops it) — and this box's
            // failure mode is anon reaching the memcg limit. Delta stays zstd;
            // the tier's edge over it was never the codec, it is locality.
            let options = IpcWriteOptions::default();
            let mut w = FileWriter::try_new_with_options(f, schema.as_ref(), options).map_err(std::io::Error::other)?;
            batches.iter().try_for_each(|b| w.write(b)).map_err(std::io::Error::other)?;
            w.finish().map_err(std::io::Error::other)
        })?;
        Ok(Some(HotBucketMeta { bucket_id, seq, min_ts, end_ts, max_stamp, sorted, covers_window, bytes: fs::metadata(&path)?.len(), path }))
    }

    /// Files whose row range overlaps `query_range` — a CLOSED `[lo, hi]`
    /// window, as query filters report it; `None` = everything. Oldest bucket
    /// first.
    pub fn buckets_in_range(&self, project_id: &str, table_name: &str, query_range: Option<(i64, i64)>) -> Vec<HotBucketMeta> {
        let key = table_key(project_id, table_name);
        let Some(entry) = self.index.get(&key) else { return Vec::new() };
        // The single choke point every reader goes through, so retracting a
        // skipped bucket's claim here covers all of them. The file still SERVES
        // its rows; it just may not tell Delta to skip that window.
        entry
            .values()
            .filter(|m| query_range.is_none_or(|(lo, hi)| overlaps(m.range(), (lo, hi.saturating_add(1)))))
            .map(|m| {
                let mut m = m.clone();
                m.covers_window &= !self.uncovered.contains(&(key.clone(), m.bucket_id));
                m
            })
            .collect()
    }

    /// Record that a bucket's demotion was skipped, so no file of that bucket
    /// may claim a window again. See the `uncovered` field.
    ///
    /// Best-effort persistence: losing the marker on a crash costs correctness,
    /// so the write happens before the in-memory insert is relied on, but a
    /// write failure disables the tier's claims for the table rather than
    /// silently forgetting.
    pub fn note_demote_skipped(&self, project_id: &str, table_name: &str, bucket_ids: &[i64]) {
        if !self.enabled || bucket_ids.is_empty() {
            return;
        }
        let key = table_key(project_id, table_name);
        let mut line = String::new();
        for id in bucket_ids {
            self.uncovered.insert((key.clone(), *id));
            line.push_str(&format!("{project_id}\t{table_name}\t{id}\n"));
        }
        if let Err(e) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.uncovered_path())
            .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()))
        {
            warn!(
                "hot tier could not persist skipped-demote markers for {project_id}/{table_name} ({e}); those windows stay unclaimable this run but a restart would forget"
            );
        }
    }

    fn uncovered_path(&self) -> PathBuf {
        self.root.join(".uncovered")
    }

    /// Reload the skipped-demote markers written by earlier runs.
    fn load_uncovered(&self) {
        let Ok(text) = std::fs::read_to_string(self.uncovered_path()) else { return };
        for line in text.lines() {
            let mut parts = line.split('\t');
            if let (Some(project), Some(table), Some(Ok(id))) = (parts.next(), parts.next(), parts.next().map(str::parse::<i64>)) {
                self.uncovered.insert((table_key(project, table), id));
            }
        }
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
    /// `HotLeg::is_empty`. Granting `skip_dedup` on a plan carrying a hot leg
    /// would break this.
    ///
    /// The leg as a PLAN rather than as decoded rows: DataFusion's own
    /// `ArrowSource` streams each file at EXECUTE time, a batch at a time,
    /// inside the query's memory pool.
    ///
    /// This replaces the eager walk (`read_leg`), which decoded every file
    /// while PLANNING. Prod 2026-08-14 measured `hot_plan_us_avg` at **954 ms**
    /// on 98% of scans — serial across files, overlapping nothing, outside every
    /// memory pool, with `leg_budget_bytes` silently truncating wide legs back
    /// to R2 (`leg_budget_stops_total` 35). Streaming removes all four at once:
    /// decode overlaps execution, file groups run in parallel, batches are
    /// pooled and spillable, and there is no admission bound left to truncate.
    ///
    /// Any failure builds NO leg at all rather than a partial one: `ranges`
    /// excludes windows from Delta on the promise that this leg serves them, so
    /// half a leg is worse than none. Returning empty falls the whole window
    /// through to Delta — slower, never wrong.
    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        self: &Arc<Self>, project_id: &str, table_name: &str, query_range: Option<(i64, i64)>, mem_ranges: &[(i64, i64)], filters: &[Expr], schema: &SchemaRef,
        projection: Option<&Vec<usize>>,
    ) -> HotLeg {
        let metas = self.buckets_in_range(project_id, table_name, query_range);
        if metas.is_empty() {
            return HotLeg::default();
        }
        let declared = crate::schema::get_schema(table_name);
        let versioned = declared.as_ref().is_some_and(|s| s.version_append);
        let orders = declared.is_some_and(|s| !s.sorting_columns.is_empty());
        let plan = plan_leg(&metas, mem_ranges, versioned, orders, &|p| self.validate(p));
        self.mem_skipped.fetch_add((metas.len() - plan.served.len()) as u64, Relaxed);
        if plan.served.is_empty() {
            return HotLeg::default();
        }
        match self.leg_exec(&metas, &plan, filters, schema, projection, table_name) {
            Ok(exec) => {
                self.read_hits.fetch_add(plan.served.len() as u64, Relaxed);
                for &i in &plan.served {
                    self.read_files.insert(metas[i].path.clone());
                }
                self.unproven_windows.fetch_add((plan.served.len() - plan.ranges.len()) as u64, Relaxed);
                HotLeg { plan: Some(exec), ranges: plan.ranges, version_gate: plan.version_gate }
            }
            Err(e) => {
                self.read_misses.fetch_add(1, Relaxed);
                warn!("hot leg for {project_id}/{table_name} could not be planned ({e}); the whole window falls through to Delta");
                HotLeg::default()
            }
        }
    }

    /// `footer_is_readable`, memoized. Hot files are immutable once renamed
    /// into place, so a file that validated once always will; the tail read is
    /// worth doing exactly once per file rather than once per scan.
    fn validate(&self, path: &Path) -> bool {
        self.validated.contains(path) || (footer_is_readable(path) && self.validated.insert(path.to_path_buf()))
    }

    /// Build the leg's physical plan: scan → filter → cut predicate-only
    /// columns. The scan carries the projection, so unused column buffers are
    /// never decoded at all.
    fn leg_exec(
        &self, metas: &[HotBucketMeta], plan: &LegPlan, filters: &[Expr], schema: &SchemaRef, projection: Option<&Vec<usize>>, table_name: &str,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        use datafusion::{
            datasource::physical_plan::ArrowSource,
            error::DataFusionError,
            execution::object_store::ObjectStoreUrl,
            physical_expr::expressions::Column as PhysicalColumn,
            physical_plan::{filter::FilterExec, projection::ProjectionExec},
        };
        use datafusion_datasource::{PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder, source::DataSourceExec};
        let cols = plan_columns(projection.map(|p| p.as_slice()), filters, schema);
        let out_schema = cols.as_ref().map_or_else(|| schema.clone(), |c| c.schema.clone());
        // One file per group => one partition per file, so files decode in
        // parallel. DataFusion may split a group further by record-batch block
        // offset (`ArrowSource::repartitioned`), giving parallelism WITHIN a
        // file too — something the eager walk could never do.
        let groups = plan
            .served
            .iter()
            .map(|&i| {
                let m = &metas[i];
                let path = object_store::path::Path::from_absolute_path(&m.path).map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(FileGroup::new(vec![PartitionedFile::new(path.as_ref().to_string(), m.bytes)]))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let mut cfg = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(ArrowSource::new_file_source(schema.clone())))
            .with_file_groups(groups)
            .with_projection_indices(cols.as_ref().map(|c| c.needed.clone()))?;
        // The ordering is declared against the leg's own output schema, and only
        // when every served file attested to it (`LegPlan::sorted`).
        if let Some(ordering) = crate::database::table_ordering(table_name, &out_schema, plan.sorted) {
            cfg = cfg.with_output_ordering(vec![ordering]);
        }
        let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(cfg.build());
        // Same predicate the eager path applied post-decode — now a plan node,
        // so it runs per batch and can terminate a scan early.
        let exec = match compile_filter_conjunction(filters, &out_schema).ok().flatten() {
            Some(pred) => Arc::new(FilterExec::try_new(pred, exec)?) as Arc<dyn ExecutionPlan>,
            None => exec,
        };
        // Cut the predicate-only columns back off (positions are `0..n` by
        // construction in `plan_columns`).
        match cols.as_ref().and_then(|c| c.output.as_ref()) {
            Some(out) => {
                let exprs = out
                    .iter()
                    .map(|&i| {
                        let f = out_schema.field(i);
                        (Arc::new(PhysicalColumn::new(f.name(), i)) as Arc<dyn PhysicalExpr>, f.name().clone())
                    })
                    .collect::<Vec<_>>();
                Ok(Arc::new(ProjectionExec::try_new(exprs, exec)?))
            }
            None => Ok(exec),
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
        let key = table_key(project_id, table_name);
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
        let key = table_key(project_id, table_name);
        let paths = {
            let Some(mut entry) = self.index.get_mut(&key) else { return };
            drop_unless_bucket_atomic(entry.value_mut(), |m| !overlaps(m.range(), (lo, hi))).into_iter().map(|m| m.path).collect()
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
        // A file never served by any query was written and thrown away, pure
        // cost. Must be counted before `unlink`, which drops the read marker.
        let wasted = paths.iter().filter(|p| !self.read_files.contains(*p)).count() as u64;
        debug!("hot tier invalidated {} file(s) ({wasted} never read) for {}.{} after DML", paths.len(), key.0, key.1);
        self.unlink(&paths);
        // Make the unlinks durable. A SIGKILL/OOM-kill is safe without this
        // (the unlink is already visible), but a POWER LOSS can lose it, and
        // then boot `rescan` resurrects a pre-DML file whose stale rows shadow
        // the corrected Delta rows for the whole retention window. Cheap: one
        // fsync per table dir per DML, off the query path.
        paths.iter().filter_map(|p| p.parent()).collect::<BTreeSet<_>>().into_iter().for_each(|dir| {
            let _ = fs::File::open(dir).and_then(|d| d.sync_all());
        });
        let health = self.health(key);
        health.wasted.fetch_add(wasted, Relaxed);
        self.judge(key, &health);
    }

    /// Forget + unlink. Held mmaps stay valid across the unlink, so in-flight
    /// queries are unaffected.
    fn unlink(&self, paths: &[PathBuf]) {
        for p in paths {
            self.read_files.remove(p);
            self.validated.remove(p);
            let _ = fs::remove_file(p);
        }
    }

    /// Rebuild the index from disk (boot warmth). Walks only our own two-level
    /// layout and only accepts files matching our naming convention; anything
    /// else is left strictly alone.
    pub fn rescan(&self) {
        self.index.clear();
        self.read_files.clear();
        self.validated.clear();
        let files = dir_entries(&self.root)
            .filter_map(named)
            .flat_map(|(pid, dir)| dir_entries(dir).filter_map(named).map(move |(tname, dir)| ((pid.clone(), tname), dir)))
            .flat_map(|(key, dir)| {
                dir_entries(dir).filter_map(move |e| {
                    let path = e.path();
                    // A `*.arrow.tmp` is ours and, at boot, definitionally dead
                    // (a demotion that never reached its rename).
                    if path.extension().is_some_and(|x| x == "tmp") {
                        let _ = fs::remove_file(&path);
                        return None;
                    }
                    Some((key.clone(), parse_meta(path, e.metadata().map(|m| m.len()).unwrap_or(0))?))
                })
            })
            .fold(0usize, |n, (key, meta)| {
                self.seq.fetch_max(meta.seq + 1, Relaxed);
                self.index.entry(key).or_default().insert((meta.bucket_id, meta.seq), meta);
                n + 1
            });
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
        // No age bound: the tier keeps whatever fits, and the disk cap alone
        // decides what falls off (oldest-first). `now_micros` is still taken so
        // callers keep a deterministic settle point under test clocks.
        let _ = now_micros;
        let age_cutoff = i64::MIN;
        // The full file list is only worth building when actually over the cap.
        let total: u64 = self.index.iter().map(|e| live_files(e.value(), age_cutoff).map(|(_, bytes)| bytes).sum::<u64>()).sum();
        let cutoff = match total.checked_sub(self.limits.max_disk_bytes).filter(|excess| *excess > 0) {
            // Raise the cutoff over the oldest files until the excess is covered.
            Some(excess) => {
                let mut sorted: Vec<(i64, u64)> = self.index.iter().flat_map(|e| live_files(e.value(), age_cutoff).collect::<Vec<_>>()).collect();
                sorted.sort_unstable();
                sorted
                    .into_iter()
                    .scan(excess, |left, (end_ts, bytes)| {
                        (*left > 0).then(|| {
                            *left = left.saturating_sub(bytes);
                            end_ts
                        })
                    })
                    .last()
                    .unwrap_or(age_cutoff)
            }
            None => age_cutoff,
        };
        // Merge-retired paths: their index entries are long gone, so any scan
        // still holding them planned before the merge — by the next gc tick it
        // has finished (or mmap'd the bytes, which survive the unlink).
        let retired: Vec<PathBuf> = std::mem::take(&mut *self.retired.lock());
        if !retired.is_empty() {
            self.gc_deleted.fetch_add(retired.len() as u64, Relaxed);
            self.unlink(&retired);
        }
        // Collect under the shard guards but unlink AFTER dropping them — N
        // unlink syscalls inside a write guard block every concurrent query's
        // range lookup for the duration.
        let dropped: Vec<HotBucketMeta> = self.index.iter_mut().flat_map(|mut e| drop_unless_bucket_atomic(e.value_mut(), |m| m.end_ts > cutoff)).collect();
        self.index.retain(|_, v| !v.is_empty());
        if !dropped.is_empty() {
            let freed: u64 = dropped.iter().map(|m| m.bytes).sum();
            self.gc_deleted.fetch_add(dropped.len() as u64, Relaxed);
            self.gc_bytes_freed.fetch_add(freed, Relaxed);
            debug!("hot tier GC: unlinked {} file(s), freed {freed} bytes", dropped.len());
            self.unlink(&dropped.into_iter().map(|m| m.path).collect::<Vec<_>>());
        }
    }

    pub fn stats(&self) -> HotTierStats {
        let (files, bytes) = self.index.iter().fold((0usize, 0u64), |(f, b), e| (f + e.len(), b + e.values().map(|m| m.bytes).sum::<u64>()));
        let now = crate::support::now_micros();
        let mut suppressed: Vec<_> = self
            .health
            .iter()
            .filter_map(|e| Some(e.until.load(Relaxed) - now).filter(|left| *left > 0).map(|left| (e.key().clone(), left / 1_000_000)))
            .collect();
        suppressed.sort_unstable();
        let suppressed_tables = suppressed.len();
        suppressed.truncate(MAX_SUPPRESSED_ROWS);
        HotTierStats {
            suppressed_tables,
            suppressed,
            suppressions: self.suppressions.load(Relaxed),
            tables: self.index.len(),
            files,
            bytes,
            writes: self.writes.load(Relaxed),
            write_failures: self.write_failures.load(Relaxed),
            read_hits: self.read_hits.load(Relaxed),
            read_misses: self.read_misses.load(Relaxed),
            mem_skipped: self.mem_skipped.load(Relaxed),
            unproven_windows: self.unproven_windows.load(Relaxed),
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
    let extra: Vec<usize> = filters.iter().flat_map(|f| f.column_refs()).map(|c| schema.index_of(&c.name).ok()).collect::<Option<Vec<_>>>()?;
    // Requested columns keep their positions; predicate-only ones follow, once.
    let needed: Vec<usize> = requested.iter().copied().chain(extra).fold(Vec::new(), |mut acc, i| {
        if !acc.contains(&i) {
            acc.push(i);
        }
        acc
    });
    let output = (needed.len() > requested.len()).then(|| (0..requested.len()).collect());
    let schema = Arc::new(schema.project(&needed).ok()?);
    Some(LegColumns { needed, schema, output })
}

/// Cheap plan-time validation: the file opens, carries both ARROW1 magics, a
/// parseable footer, and at least one record batch. Reads only the tail — not
/// one batch is decoded.
///
/// This is what keeps the module's "a torn or unreadable file is ABSENT, never
/// a query failure" promise now that decoding happens at execute time. A file
/// rejected here loses its rows AND its exclusion together, so its window falls
/// through to Delta intact. Left to `ArrowSource`, that same file would instead
/// fail the query mid-execution with its window ALREADY excluded — the one
/// shape that turns a cache miss into an outage.
///
/// Zero record batches counts as unusable for the same reason: a file serving
/// no rows must not exclude a window that Delta could serve.
fn footer_is_readable(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    use arrow_ipc::{reader::read_footer_length, root_as_footer};
    (|| -> anyhow::Result<bool> {
        let mut f = fs::File::open(path)?;
        let len = f.metadata()?.len();
        anyhow::ensure!(len >= MIN_FILE_LEN as u64, "shorter than an empty IPC file");
        let mut magic = [0u8; 6];
        f.read_exact(&mut magic)?;
        anyhow::ensure!(&magic == ARROW_MAGIC, "missing leading ARROW1");
        let mut trailer = [0u8; 10];
        f.seek(SeekFrom::End(-10))?;
        f.read_exact(&mut trailer)?;
        anyhow::ensure!(&trailer[4..] == ARROW_MAGIC, "missing trailing ARROW1");
        let footer_len = read_footer_length(trailer)?;
        anyhow::ensure!(footer_len as u64 + 10 <= len, "footer length exceeds file");
        let mut buf = vec![0u8; footer_len];
        f.seek(SeekFrom::End(-10 - footer_len as i64))?;
        f.read_exact(&mut buf)?;
        let footer = root_as_footer(&buf).map_err(|e| anyhow::anyhow!("unparseable IPC footer: {e}"))?;
        Ok(footer.recordBatches().is_some_and(|b| !b.is_empty()))
    })()
    .unwrap_or_else(|e| {
        debug!("hot tier file {path:?} unusable at plan time, falling through to Delta: {e:#}");
        false
    })
}

/// Everything a leg may CLAIM about a set of files, derived from METADATA
/// ALONE — not one batch is decoded. `read_leg` recomputes the same claims
/// while materializing; this is the form a lazy (`ArrowSource`) leg needs,
/// where the claims must be fixed before a single row is read.
///
/// It claims a SUPERSET of what `read_leg` serves, because it cannot know which
/// files a predicate empties. That difference is safe: the excluded window's
/// Delta copy is filtered by the SAME predicate, so a window empty here is empty
/// there and nothing is lost. Serving extra files likewise only LOWERS
/// `version_gate` (admitting more stamped Delta rows, which `DedupExec`
/// collapses) and only RETRACTS `sorted` (forcing a sort that was already
/// correct) — both the conservative direction.
///
/// **The soundness precondition is that every file in `served` is actually
/// read.** `ranges` excludes a window from Delta on the promise that this leg
/// serves it; a caller that keeps the claim but drops the rows serves that
/// window from NOWHERE. So this is only usable by a leg with no admission
/// bound — which is exactly why the lazy `ArrowSource` leg can use it and the
/// eager path cannot: `leg_budget_bytes` stops that walk mid-list, and
/// `leg_budget_stops_rows_and_ranges_together` pins that its rows and ranges
/// must therefore fall away together. Streaming removes the budget, and with it
/// the only unsafe divergence.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LegPlan {
    /// Indices into `metas` of the files to serve, in `metas` order.
    pub served: Vec<usize>,
    /// Windows the served files are authoritative for; the caller excludes them
    /// from the Delta leg.
    pub ranges: Vec<(i64, i64)>,
    /// Merge-on-read gate — see [`HotLeg::version_gate`].
    pub version_gate: Option<i64>,
    /// Every served file is ordered by the table's declared `sorting_columns`.
    pub sorted: bool,
}

/// `orders` = the table declares sorting columns; `versioned` = it appends
/// versions. Both come from the declared schema, not from the files.
pub(crate) fn plan_leg(metas: &[HotBucketMeta], mem_ranges: &[(i64, i64)], versioned: bool, orders: bool, usable: &dyn Fn(&Path) -> bool) -> LegPlan {
    // Counted over ALL metas, not just served ones: a bucket demoted more than
    // once holds PART of its span in each file, so the set is authoritative
    // only when every member of it is present AND serving. A sibling MemBuffer
    // still owns is exactly such a partial file — dropping it from the count
    // would let the rest claim a window they only partly hold.
    let mut files_per_bucket: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for m in metas {
        *files_per_bucket.entry(m.bucket_id).or_default() += 1;
    }
    // A file MemBuffer still owns, or one that will not read, is skipped whole
    // — no rows and no exclusion, so the window stays with the tier that can
    // actually serve it.
    let served: Vec<usize> =
        metas.iter().enumerate().filter(|(_, m)| !mem_ranges.iter().any(|r| overlaps(m.range(), *r)) && usable(&m.path)).map(|(i, _)| i).collect();

    // Which buckets the leg holds ENTIRELY: every file of the bucket is being
    // served, and every one of them claims to hold its whole drain.
    //
    // This used to demand a single file per bucket, which handed every
    // twice-drained bucket's window back to Delta. Prod 2026-08-17:
    // `unproven_windows_total` 2286 against `read_hits_total` 3163, and a
    // one-hour scan read 410K rows to answer 160K because the Delta leg
    // re-fetched from object storage what was already on local disk.
    //
    // Sound only because both ways a file set can be incomplete are now
    // closed: deletion is bucket-atomic (`drop_unless_bucket_atomic`) and a
    // skipped demote permanently retracts its bucket's claim (`uncovered`).
    // Without those, "every file present" did not mean "every file ever
    // written", and this would exclude windows the tier never held.
    let mut served_per_bucket: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    let mut marked_per_bucket: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for &i in &served {
        *served_per_bucket.entry(metas[i].bucket_id).or_default() += 1;
        *marked_per_bucket.entry(metas[i].bucket_id).or_default() += usize::from(metas[i].covers_window);
    }
    let whole = |id: i64| {
        let total = files_per_bucket.get(&id).copied().unwrap_or(0);
        total != 0 && served_per_bucket.get(&id).copied().unwrap_or(0) == total && marked_per_bucket.get(&id).copied().unwrap_or(0) == total
    };

    let mut plan = LegPlan { sorted: orders, ..Default::default() };
    // The gate guards only the RANGES, and ranges come only from whole
    // buckets. A whole stack is complete up to its NEWEST file's stamp:
    // version stamps are per-table monotone (`next_stamp` is a hybrid clock),
    // so every row that arrived between two drains carries a stamp in
    // (s_prev, s_last] and sits in the later file — the union is complete at
    // the stack MAX. The old per-file MIN pinned a stack's gate to its OLDEST
    // drain, re-admitting from Delta the very versions the stack already
    // holds (with monoscope enriching every row, that was ~every row, on
    // every recent-window scan, forever). One file with an unknown stamp
    // makes its whole stack unable to vouch (i64::MIN — admit everything).
    // Files of non-whole buckets claim no ranges and must not drag the gate.
    // One gate guards all ranges, so it is the MIN over whole-bucket gates.
    let mut stack_gate: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for &i in &served {
        let m = &metas[i];
        plan.served.push(i);
        // One unmarked file retracts the claim for the whole leg:
        // `try_with_sort_information` declares ONE ordering for the source.
        plan.sorted &= m.sorted;
        if whole(m.bucket_id) {
            plan.ranges.push(m.range());
            if versioned {
                let g = stack_gate.entry(m.bucket_id).or_insert(i64::MIN + 1);
                *g = match m.max_stamp {
                    Some(s) if *g != i64::MIN => (*g).max(s),
                    _ => i64::MIN,
                };
            }
        }
    }
    if versioned && let Some(g) = stack_gate.values().min() {
        plan.version_gate = Some(*g);
    }
    plan
}

/// mmap + validate + decode. `require_alignment` is the arrow-rs knob that
/// turns a silent realigning COPY into an error — production reads leave it
/// off (correctness over strictness), tests turn it on to prove the
/// zero-copy assumption actually holds for files we write.
#[cfg(test)]
fn decode_file(path: &Path, require_alignment: bool, projection: Option<&[usize]>) -> anyhow::Result<Vec<RecordBatch>> {
    use arrow::buffer::Buffer;
    use arrow_ipc::{Block, convert::fb_to_schema, reader::FileDecoder, reader::read_footer_length, root_as_footer};

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
    let decoder = FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(require_alignment);
    let mut decoder = match projection {
        Some(projection) => decoder.with_projection(projection.to_vec()),
        None => decoder,
    };

    let slice = |block: &Block| -> anyhow::Result<Buffer> {
        let (offset, block_len) = (block.offset() as usize, block.bodyLength() as usize + block.metaDataLength() as usize);
        anyhow::ensure!(offset.checked_add(block_len).is_some_and(|end| end <= len), "IPC block out of bounds (torn file)");
        Ok(buffer.slice_with_length(offset, block_len))
    };
    footer.dictionaries().iter().flatten().try_for_each(|b| anyhow::Ok(decoder.read_dictionary(b, &slice(b)?)?))?;
    footer.recordBatches().iter().flatten().map(|b| anyhow::Ok(decoder.read_record_batch(b, &slice(b)?)?)).filter_map(Result::transpose).collect()
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
        HotTierLimits { max_disk_bytes, merge_demote: true }
    }

    /// Run a leg's plan the way a query does. Tests assert on ROWS SERVED, not
    /// on batches a plan-time decode happened to materialize — the whole point
    /// of the streaming leg is that nothing is decoded until this runs.
    async fn served(leg: &HotLeg) -> Vec<RecordBatch> {
        let Some(plan) = leg.plan.clone() else { return Vec::new() };
        let ctx = datafusion::prelude::SessionContext::new();
        datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap()
    }

    fn open(dir: &tempfile::TempDir) -> Arc<HotTier> {
        HotTier::open(dir.path().to_path_buf(), true, limits(u64::MAX))
    }

    /// PROD REGRESSION 2026-07-31: demoted batches carried `timestamp` as
    /// NULLABLE while the table declares it NOT NULL, and the leg's strict
    /// field-equality check discarded 100% of reads — `schema_drift_total`
    /// tracked `read_hits_total` exactly (75/75) while files/bytes/writes all
    /// looked healthy, so the tier burned disk and RAM serving zero rows.
    ///
    /// Pinned here at the level it now manifests: the streaming leg has no
    /// post-decode re-stamp to get right, so what must hold is simply that a
    /// nullability-widened file still SERVES ITS ROWS under the strict table
    /// schema. (The root cause is fixed upstream in
    /// `mem_buffer::align_nullability`; this is tolerance for files written by
    /// an older binary.)
    #[tokio::test]
    async fn nullability_widened_file_still_serves_its_rows() {
        let nullable = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true), Field::new("name", DataType::Utf8, false)]));
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        let b = RecordBatch::try_new(nullable, vec![Arc::new(Int64Array::from(vec![1i64, 2])), Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap();
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[b], min_ts: 1, max_ts: 2, covers_window: true });

        // Scanned with the STRICT table schema, as a query would.
        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "nullability alone must not cost the tier its rows");
        assert_eq!(leg.ranges, vec![(1, 3)]);
    }

    /// New hot files are buffer-compressed and remain lossless. Repetitive
    /// payload makes the capacity property deterministic: the file must be
    /// materially smaller than its decoded Arrow representation.
    #[test]
    fn roundtrip_is_uncompressed_zero_copy_and_lossless() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        let b =
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![100i64; 4096])), Arc::new(StringArray::from(vec!["repeated payload"; 4096]))])
                .unwrap();
        tier.demote("p1", "otel_logs_and_spans", Bucket { bucket_id: 7, batches: std::slice::from_ref(&b), min_ts: 100, max_ts: 200, covers_window: true });

        let metas = tier.buckets_in_range("p1", "otel_logs_and_spans", Some((150, 300)));
        assert_eq!(metas.len(), 1);
        // Stored half-open: the inclusive max 200 becomes end 201.
        assert_eq!((metas[0].bucket_id, metas[0].min_ts, metas[0].end_ts), (7, 100, 201));
        // UNCOMPRESSED: the file is the size of the data, not a fraction of it.
        // That is the trade — disk (which this box has) for decode CPU and anon
        // heap (which it does not).
        assert!(metas[0].bytes > b.get_array_memory_size() as u64 / 2, "hot files must be uncompressed: {} bytes", metas[0].bytes);
        // `require_alignment` makes arrow-rs ERROR rather than silently
        // realign-and-copy, so this passing is the zero-copy property itself:
        // batches are views over the file, which is why the tier's read memory
        // is reclaimable page cache instead of anon heap. LZ4 had silently
        // forfeited this while the module doc still claimed it.
        assert_eq!(decode_file(&metas[0].path, true, None).unwrap(), vec![b], "uncompressed IPC must decode ZERO-COPY and byte-identical");
        // Out-of-range windows must not select it.
        assert!(tier.buckets_in_range("p1", "otel_logs_and_spans", Some((201, 300))).is_empty());
        assert!(tier.buckets_in_range("other", "otel_logs_and_spans", None).is_empty());
    }

    fn meta(bucket_id: i64, min_ts: i64, end_ts: i64, covers_window: bool, sorted: bool, max_stamp: Option<i64>) -> HotBucketMeta {
        HotBucketMeta { bucket_id, seq: 0, min_ts, end_ts, bytes: 1, sorted, max_stamp, covers_window, path: PathBuf::from(format!("b{bucket_id}-{min_ts}")) }
    }

    /// Every claim the leg makes is a function of METADATA, so a lazy leg can
    /// fix them before decoding a row. Each rule is checked by flipping exactly
    /// one input away from a fully-claiming baseline.
    #[test]
    fn plan_leg_derives_every_claim_from_metadata_alone() {
        let full = vec![meta(1, 0, 10, true, true, Some(7)), meta(2, 20, 30, true, true, Some(9))];
        let p = plan_leg(&full, &[], true, true, &|_| true);
        assert_eq!(p, LegPlan { served: vec![0, 1], ranges: vec![(0, 10), (20, 30)], version_gate: Some(7), sorted: true }, "baseline claims everything");

        // A table that declares no sorting columns cannot claim an ordering
        // however its files are marked; one unsorted file retracts the claim
        // for the whole leg, because the source declares ONE ordering.
        assert!(!plan_leg(&full, &[], true, false, &|_| true).sorted, "no declared sorting columns => no claim");
        let mixed = vec![full[0].clone(), meta(2, 20, 30, true, false, Some(9))];
        assert!(!plan_leg(&mixed, &[], true, true, &|_| true).sorted, "one unsorted file retracts the leg's ordering");
        assert_eq!(plan_leg(&mixed, &[], true, true, &|_| true).ranges, p.ranges, "...but it still serves and still excludes");

        // The gate is the MIN over served files, and an unknown stamp drops it
        // to i64::MIN — admitting every stamped Delta row, never hiding one.
        assert_eq!(plan_leg(&full, &[], false, true, &|_| true).version_gate, None, "a table that appends no versions needs no gate");
        let unstamped = vec![full[0].clone(), meta(2, 20, 30, true, true, None)];
        assert_eq!(plan_leg(&unstamped, &[], true, true, &|_| true).version_gate, Some(i64::MIN), "an unknown stamp admits everything");

        // `covers_window` is necessary but not sufficient: a bucket demoted
        // more than once holds only PART of its span in each file.
        let partial = vec![meta(1, 0, 10, false, true, Some(7)), full[1].clone()];
        assert_eq!(plan_leg(&partial, &[], true, true, &|_| true).ranges, vec![(20, 30)], "an incomplete drain claims no window");
        // A bucket's files are JOINTLY authoritative. With every one of them
        // present, served and marked, the set holds the whole drain and the
        // span is excluded — the tier serves those rows, so Delta must not.
        let split = vec![meta(1, 0, 10, true, true, Some(7)), meta(1, 0, 10, true, true, Some(8)), full[1].clone()];
        let p = plan_leg(&split, &[], true, true, &|_| true);
        assert_eq!(p.served, vec![0, 1, 2], "both halves serve their rows");
        assert!(p.ranges.contains(&(0, 10)) && p.ranges.contains(&(20, 30)), "a COMPLETE twice-drained bucket claims its span, got {:?}", p.ranges);

        // ...but one unmarked member retracts the whole bucket's claim: that
        // file does not even hold its own drain, so the set cannot hold the span.
        let half_marked = vec![meta(1, 0, 10, true, true, Some(7)), meta(1, 0, 10, false, true, Some(8)), full[1].clone()];
        let p = plan_leg(&half_marked, &[], true, true, &|_| true);
        assert_eq!(p.ranges, vec![(20, 30)], "one unmarked member retracts its bucket's claim");
        assert_eq!(p.served, vec![0, 1, 2], "...though both still SERVE their rows");
    }

    /// A file MemBuffer still owns contributes nothing AND excludes nothing —
    /// but it still counts against its bucket, or the sibling that survives
    /// would claim a window the two of them only jointly cover.
    #[test]
    fn plan_leg_skips_files_membuffer_still_owns_without_forfeiting_their_bucket() {
        let metas = vec![meta(1, 0, 10, true, true, Some(7)), meta(2, 20, 30, true, true, Some(9))];
        let p = plan_leg(&metas, &[(5, 15)], true, true, &|_| true);
        assert_eq!(p.served, vec![1], "the overlapped file is skipped whole");
        assert_eq!(p.ranges, vec![(20, 30)], "and excludes nothing, so that window falls through to Delta");
        assert_eq!(p.version_gate, Some(9), "a skipped file does not constrain the gate");

        // Both halves of a twice-demoted bucket, one of them mem-owned.
        let split = vec![meta(1, 0, 10, true, true, Some(7)), meta(1, 10, 20, true, true, Some(8))];
        let p = plan_leg(&split, &[(12, 14)], true, true, &|_| true);
        assert_eq!(p.served, vec![0], "the mem-owned half is skipped");
        assert!(p.ranges.is_empty(), "the survivor must NOT claim the bucket's span — it holds half of it");
    }

    /// A bucket whose demotion was SKIPPED may never claim a window again,
    /// even from a later incarnation's file — and that must survive a restart.
    ///
    /// `covers_window` is a claim about ONE incarnation; a bucket re-forms when
    /// late rows arrive after a full drain. `plan_leg` catches multiple
    /// incarnations by counting a bucket's files, but a skipped demote leaves
    /// NO file to count. Incarnation A skipped and B demoted is therefore
    /// indistinguishable from a single fully-covered drain, and the survivor
    /// excludes from Delta a window whose earlier rows it never received.
    ///
    /// Prod 2026-08-17 had `demote_skipped_total` = 20 with the queue-limit
    /// path logging "a permanent coverage hole served from Delta" — which is
    /// only true if the tier stops claiming those windows.
    #[tokio::test]
    async fn a_skipped_demote_permanently_retracts_that_buckets_claim() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        // Incarnation A never reaches the tier (queue full).
        tier.note_demote_skipped("p1", "t", &[7]);
        // Incarnation B drains cleanly and claims to hold "every row".
        tier.demote("p1", "t", Bucket { bucket_id: 7, batches: &[batch(8)], min_ts: 0, max_ts: 20, covers_window: true });

        let metas = tier.buckets_in_range("p1", "t", None);
        assert_eq!(metas.len(), 1, "only the second incarnation has a file");
        let plan = plan_leg(&metas, &[], false, false, &footer_is_readable);
        assert_eq!(plan.served, vec![0], "the file still SERVES its rows");
        assert!(plan.ranges.is_empty(), "but it must not exclude a window whose earlier rows the tier never received");

        // The omission is durable, so the retraction must be too.
        let reopened = HotTier::open(dir.path().to_path_buf(), true, limits(u64::MAX));
        let metas = reopened.buckets_in_range("p1", "t", None);
        assert_eq!(metas.len(), 1, "the file is still there after a restart");
        assert!(
            plan_leg(&metas, &[], false, false, &footer_is_readable).ranges.is_empty(),
            "a restart must not turn a skipped demote back into a coverage claim"
        );
    }

    /// Deleting ONE file of a twice-demoted bucket must not restore the
    /// survivor's claim over the span they only jointly covered.
    ///
    /// `plan_leg` withholds the claim while both halves are present
    /// (`files_per_bucket == 1` is the gate), but GC and ranged invalidation
    /// delete files from the INDEX — so the survivor is left looking like a
    /// single-file bucket and reclaims the whole span. Its deleted sibling's
    /// rows are then absent from the hot leg AND excluded from the Delta leg
    /// that still holds them, so a query silently under-reports.
    ///
    /// Deletion is therefore bucket-atomic: dropping any file of a bucket drops
    /// the bucket. Over-deleting only costs a fall-through to Delta, which is
    /// always correct; under-deleting loses rows.
    #[tokio::test]
    async fn deleting_one_half_of_a_bucket_must_not_restore_the_others_claim() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        // One bucket, drained twice, with OVERLAPPING spans — late rows landing
        // inside a window an earlier drain already reported on.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 0, max_ts: 20, covers_window: true });
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 30, covers_window: true });
        let metas = tier.buckets_in_range("p1", "t", None);
        assert_eq!(metas.len(), 2, "the bucket must hold both drains");
        // Both present, both served, both marked: the set is complete, so it
        // legitimately claims. That is the state the deletion below destroys.
        assert!(!plan_leg(&metas, &[], false, false, &footer_is_readable).ranges.is_empty(), "a complete bucket claims its span");

        // Invalidate a window that touches only the FIRST drain.
        tier.invalidate_range("p1", "t", 0, 5);
        let after = tier.buckets_in_range("p1", "t", None);
        assert!(
            plan_leg(&after, &[], false, false, &footer_is_readable).ranges.is_empty(),
            "after one half is deleted the survivor must still claim nothing; it holds only part of the span, and the rest now lives only in Delta"
        );
    }

    /// The claims `plan_leg` makes from metadata are the ones the leg actually
    /// honours: every file it says it serves hands over its rows, and every
    /// window it excludes is one of those files'.
    #[tokio::test]
    async fn plan_leg_claims_match_the_rows_the_leg_serves() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..4i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(8)], min_ts: i * 100, max_ts: i * 100 + 10, covers_window: true });
        }
        let metas = tier.buckets_in_range("p1", "t", None);
        let plan = plan_leg(&metas, &[], false, false, &footer_is_readable);
        assert_eq!((plan.served.len(), plan.ranges.len()), (4, 4), "four proven single-file buckets");

        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert_eq!(leg.ranges, plan.ranges, "the leg claims exactly what the metadata plan did");
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 32, "and serves every row it claimed");

        // A predicate no row satisfies still excludes: the claim rests on the
        // files COVERING those windows, and the same predicate empties Delta's
        // copies identically. See `LegPlan`.
        let never = vec![datafusion::prelude::col("ts").lt(datafusion::prelude::lit(i64::MIN))];
        let leg = tier.scan("p1", "t", None, &[], &never, &schema(), None);
        assert_eq!(leg.ranges.len(), 4);
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    }

    /// The hot leg projects to the requested columns BEFORE filtering, keeps a
    /// predicate-only column alive across the filter, and hands back exactly
    /// the requested shape plus the range it covered.
    #[tokio::test]
    async fn leg_projects_before_filtering_and_reports_its_range() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        let filters = vec![datafusion::prelude::col("ts").gt(datafusion::prelude::lit(5i64))];
        // Project only `name`; `ts` is predicate-only and must not leak out.
        let leg = tier.scan("p1", "t", None, &[], &filters, &schema(), Some(&vec![1]));
        assert_eq!(leg.ranges, vec![(10, 21)]);
        let batches = served(&leg).await;
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "only ts=6,7 match");

        // A file MemBuffer still owns is skipped whole — no rows, no range, so
        // the window stays with the tier that has the fresher copy.
        let leg = tier.scan("p1", "t", None, &[(15, 25)], &filters, &schema(), None);
        assert!(leg.is_empty());
        assert_eq!(tier.stats().mem_skipped, 1);
    }

    /// Merge-on-read version gate. A `version_append` table's demoted file
    /// records the greatest stamp it holds, survives a restart carrying it (the
    /// stamp rides in the filename), and the leg reports the MINIMUM across the
    /// files it served — the conservative bound the caller weakens its Delta
    /// exclusion with. A file that cannot vouch for a stamp drops the gate to
    /// `i64::MIN`, which admits every stamped row rather than hiding one.
    #[test]
    fn stacked_bucket_gate_advances_to_the_stack_maximum() {
        let meta = |bucket_id: i64, seq: u64, min_ts: i64, end_ts: i64, stamp: Option<i64>, covers: bool| HotBucketMeta {
            bucket_id,
            seq,
            min_ts,
            end_ts,
            max_stamp: stamp,
            sorted: true,
            covers_window: covers,
            bytes: 1,
            path: PathBuf::from(format!("{seq}.arrow")),
        };
        // Bucket 1 demoted twice: base complete at stamp 100, late drain at 200.
        // Bucket 2 has one file MemBuffer still owns and one served — not whole,
        // so its low stamp must not drag the gate down.
        let metas =
            vec![meta(1, 0, 0, 10, Some(100), true), meta(1, 1, 0, 10, Some(200), true), meta(2, 2, 20, 30, Some(5), true), meta(2, 3, 31, 40, Some(5), true)];
        let plan = plan_leg(&metas, &[(20, 30)], true, true, &|_| true);
        assert_eq!(plan.served, vec![0, 1, 3]);
        assert!(plan.ranges.contains(&(0, 10)));
        assert_eq!(
            plan.version_gate,
            Some(200),
            "a whole stack is complete up to its NEWEST drain; the per-file MIN pinned the gate to the oldest and re-admitted every enriched row from Delta"
        );
        // One unknown stamp inside a whole stack collapses that stack to
        // admit-everything — and with it the global gate.
        let metas = vec![meta(1, 0, 0, 10, Some(100), true), meta(1, 1, 0, 10, None, true)];
        assert_eq!(plan_leg(&metas, &[], true, true, &|_| true).version_gate, Some(i64::MIN));
    }

    #[test]
    #[serial]
    fn merge_on_demote_collapses_a_bucket_stack_into_one_covering_file() {
        use arrow::{
            array::TimestampMicrosecondArray,
            datatypes::{Field, Schema, TimeUnit},
        };
        let s = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ]));
        let rows = |ids: Vec<(&str, i64, i64)>| {
            RecordBatch::try_new(
                s.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(ids.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>()).with_timezone("UTC")),
                    Arc::new(StringArray::from(ids.iter().map(|(id, _, _)| *id).collect::<Vec<_>>())),
                    Arc::new(TimestampMicrosecondArray::from(ids.iter().map(|(_, _, v)| *v).collect::<Vec<_>>()).with_timezone("UTC")),
                ],
            )
            .unwrap()
        };
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        // Base drain: A@v100, B@v100. Late drain: A@v200 (the enrichment).
        tier.demote(
            "p1",
            "mor_versioned",
            Bucket { bucket_id: 1, batches: &[rows(vec![("a", 1000, 100), ("b", 1001, 100)])], min_ts: 1000, max_ts: 1001, covers_window: true },
        );
        tier.demote("p1", "mor_versioned", Bucket { bucket_id: 1, batches: &[rows(vec![("a", 1000, 200)])], min_ts: 1000, max_ts: 1000, covers_window: true });

        let metas = tier.buckets_in_range("p1", "mor_versioned", None);
        assert_eq!(metas.len(), 1, "merge-on-demote must leave ONE file per bucket, not a stack");
        let m = &metas[0];
        assert!(m.covers_window, "a merge of covering drains still covers");
        assert_eq!(m.max_stamp, Some(200), "the merged file vouches up to the newest drain");
        let rows_read: usize = decode_file(&m.path, false, None).unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows_read, 2, "A collapsed to its v200 winner, B retained");
        // The replaced paths are retired, not gone — gc sweeps them.
        assert_eq!(tier.retired.lock().len(), 1);
        tier.gc(crate::support::now_micros());
        assert!(tier.retired.lock().is_empty());
    }

    #[tokio::test]
    async fn version_gate_is_the_minimum_stamp_and_survives_restart() {
        use arrow::{
            array::TimestampMicrosecondArray,
            datatypes::{Field, Schema, TimeUnit},
        };

        let versioned = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ]));
        let rows = |stamps: Vec<i64>| {
            let ts: Vec<i64> = (0..stamps.len() as i64).collect();
            RecordBatch::try_new(
                versioned.clone(),
                vec![Arc::new(Int64Array::from(ts)), Arc::new(TimestampMicrosecondArray::from(stamps).with_timezone("UTC"))],
            )
            .unwrap()
        };

        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        // `mor_versioned` is the shipped `version_append` schema; the gate keys
        // off the registry, not off the batch, exactly as the write path does.
        tier.demote("p1", "mor_versioned", Bucket { bucket_id: 1, batches: &[rows(vec![100, 400])], min_ts: 0, max_ts: 1, covers_window: true });
        tier.demote("p1", "mor_versioned", Bucket { bucket_id: 2, batches: &[rows(vec![200, 300])], min_ts: 2, max_ts: 3, covers_window: true });

        let leg = tier.scan("p1", "mor_versioned", None, &[], &[], &versioned, None);
        assert_eq!(leg.version_gate, Some(300), "the gate is the least per-file maximum, so no file's newer rows are excluded");
        assert_eq!(leg.ranges.len(), 2);

        // A file whose rows predate the stamp (all NULL — the state of every row
        // written before a table opted in) cannot vouch for one; the gate must
        // collapse to "admit everything", never to the other files' higher
        // bound.
        let unstamped = RecordBatch::try_new(
            versioned.clone(),
            vec![Arc::new(Int64Array::from(vec![0i64, 1])), Arc::new(TimestampMicrosecondArray::from(vec![None, None]).with_timezone("UTC"))],
        )
        .unwrap();
        tier.demote("p1", "mor_versioned", Bucket { bucket_id: 3, batches: &[unstamped], min_ts: 4, max_ts: 5, covers_window: true });
        let leg = tier.scan("p1", "mor_versioned", None, &[], &[], &versioned, None);
        assert_eq!(leg.version_gate, Some(i64::MIN), "an unstamped file must not let the gate hide newer Delta rows");

        // A table that does not append versions gets no gate at all — its
        // exclusion stands unweakened, byte-for-byte the pre-merge-on-read plan.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert_eq!(leg.version_gate, None);

        // Restart: `rescan` recovers the stamp from the filename.
        let reopened = HotTier::open(dir.path().to_path_buf(), true, limits(u64::MAX));
        let metas = reopened.buckets_in_range("p1", "mor_versioned", None);
        let stamps: Vec<_> = metas.iter().map(|m| m.max_stamp).collect();
        assert_eq!(stamps, vec![Some(400), Some(300), None], "stamps must survive a restart via the filename");
    }

    #[test]
    fn torn_file_is_absent_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(32)], min_ts: 10, max_ts: 20, covers_window: true });
        let path = tier.buckets_in_range("p1", "t", None)[0].path.clone();

        // Each unusable shape must cost the file BOTH its rows and its
        // exclusion, so the window falls through to Delta. If the probe let one
        // through, `ArrowSource` would fail the query at execute time with the
        // window already excluded — a cache miss turned into an outage.
        let full = fs::read(&path).unwrap();
        for corrupt in [&full[..full.len() / 2], b"not an arrow file at all".as_slice(), b"".as_slice()] {
            fs::write(&path, corrupt).unwrap();
            assert!(!footer_is_readable(&path), "corrupt file must probe as unusable");
            let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
            assert!(leg.is_empty(), "no rows AND no exclusion — the whole window falls through to Delta");
        }
        assert!(!footer_is_readable(Path::new("/definitely/not/here.arrow")));
    }

    #[test]
    fn rescan_rebuilds_index_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: 30, max_ts: 40, covers_window: true });
        drop(tier);

        // Simulated restart: fresh instance over the same root.
        let restarted = open(&dir);
        let metas = restarted.buckets_in_range("p1", "t", None);
        assert_eq!(metas.iter().map(|m| (m.bucket_id, m.min_ts, m.end_ts)).collect::<Vec<_>>(), vec![(1, 10, 21), (2, 30, 41)]);
        assert_eq!(decode_file(&metas[0].path, false, None).unwrap(), vec![batch(8)], "recovered files still decode");
        // A post-restart demotion must not collide with a recovered seq.
        restarted.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: 41, max_ts: 42, covers_window: true });
        assert_eq!(restarted.buckets_in_range("p1", "t", None).len(), 3);
    }

    /// GC has exactly one policy since 2026-08-14: stay under the disk cap by
    /// unlinking OLDEST-FIRST. There is no age expiry — the tier keeps whatever
    /// fits, so buying disk buys history. And it only ever unlinks its own
    /// `*.arrow` files (regression guard for ba8820e, where a generic recursive
    /// deleter ate the WAL quarantine dir).
    #[test]
    fn gc_caps_disk_oldest_first_and_never_touches_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let tier = HotTier::open(dir.path().to_path_buf(), true, limits(u64::MAX));
        let now = 10_000_000i64;
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 1, max_ts: 2, covers_window: true }); // ancient
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: now - 1000, max_ts: now, covers_window: true }); // fresh
        let foreign = dir.path().join("p1").join("t").join("do_not_delete.bin");
        fs::File::create(&foreign).unwrap().write_all(b"payload").unwrap();

        tier.gc(now);
        assert_eq!(
            tier.buckets_in_range("p1", "t", None).iter().map(|m| m.bucket_id).collect::<Vec<_>>(),
            vec![1, 2],
            "under the cap NOTHING is dropped, however old — age is no longer a policy"
        );
        assert!(foreign.exists(), "GC must never delete a non-.arrow file under its own root");

        // Disk cap: 0 bytes allowed => everything goes.
        let capped = HotTier::open(dir.path().to_path_buf(), true, limits(0));
        capped.gc(now);
        assert!(capped.buckets_in_range("p1", "t", None).is_empty());
        assert!(foreign.exists());
        assert_eq!(capped.stats().files, 0);
    }

    /// The safety property behind raising `d_hot_tier_retention_hours` to 72:
    /// the DISK CAP, not the hour count, is the real window. Prod 2026-08-09
    /// sat at 79.6 GB of a 128 GB cap because 24h bound first, leaving a
    /// quarter of the disk unused while every window that left the tier cost
    /// 17-31 s instead of 0.3-1.0 s. Raising retention is only safe because GC
    /// still bounds bytes oldest-first, which is what this pins.
    #[test]
    fn a_long_retention_is_still_bounded_by_the_disk_cap_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let now = 10_000_000i64;
        // A retention window so wide nothing can age out of it.
        let tier = HotTier::open(dir.path().to_path_buf(), true, limits(u64::MAX));
        for (id, end) in [(1i64, now - 3000), (2, now - 2000), (3, now - 1000)] {
            tier.demote("p1", "t", Bucket { bucket_id: id, batches: &[batch(8)], min_ts: end - 10, max_ts: end, covers_window: true });
        }
        tier.gc(now);
        assert_eq!(tier.stats().files, 3, "nothing ages out under a wide window — the hour count alone would keep everything");

        // Same wide window, but a cap that admits roughly one file: age keeps
        // all three, so the cap must be what evicts, and it must take the
        // OLDEST first.
        let one_file_bytes = tier.stats().bytes / 3;
        let capped = HotTier::open(dir.path().to_path_buf(), true, limits(one_file_bytes));
        capped.gc(now);
        let kept: Vec<i64> = capped.buckets_in_range("p1", "t", None).iter().map(|m| m.bucket_id).collect();
        assert!(capped.stats().bytes <= one_file_bytes, "the cap binds regardless of retention: {} bytes over {one_file_bytes}", capped.stats().bytes);
        assert!(!kept.contains(&1), "the OLDEST file must go first, so a longer window degrades to fewer hours, never to unbounded disk: kept {kept:?}");
        assert!(kept.contains(&3), "and the newest survives — the hours nearest now are the ones dashboards ask for");
    }

    /// A disabled tier is still responsible for its own directory: `open`
    /// sweeps whatever a previously-enabled run left, or the files leak
    /// forever, unbounded and invisible.
    #[test]
    fn disabled_tier_sweeps_instead_of_leaking() {
        let dir = tempfile::tempdir().unwrap();
        open(&dir).demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        let foreign = dir.path().join("p1").join("t").join("do_not_delete.bin");
        fs::File::create(&foreign).unwrap().write_all(b"payload").unwrap();

        let off = HotTier::open(dir.path().to_path_buf(), false, limits(u64::MAX));
        assert_eq!(off.stats().files, 0, "a disabled tier must not strand the previous run's files");
        assert!(foreign.exists());
        off.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(off.stats().files, 0, "and must not demote either");
    }

    #[test]
    fn invalidate_drops_every_file_for_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        tier.demote("p2", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
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
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 100, max_ts: 199, covers_window: true }); // [100, 200) — abuts window start
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(4)], min_ts: 250, max_ts: 350, covers_window: true }); // [250, 351) — overlaps
        tier.demote("p1", "t", Bucket { bucket_id: 3, batches: &[batch(4)], min_ts: 400, max_ts: 500, covers_window: true }); // [400, 501) — starts at window end
        tier.demote("p1", "t", Bucket { bucket_id: 4, batches: &[batch(4)], min_ts: 600, max_ts: 700, covers_window: true }); // far outside

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
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 100, max_ts: 199, covers_window: true });
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(4)], min_ts: 300, max_ts: 400, covers_window: true });

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
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
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
    #[serial]
    #[test]
    fn wasted_demotions_suppress_the_table() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
            tier.invalidate("p1", "t");
        }
        let s = tier.stats();
        assert_eq!((s.suppressions, s.suppressed_tables, s.writes), (1, 1, PROBE_DEMOTES));
        assert_eq!(s.suppressed[0].0, (Arc::from("p1"), Arc::from("t")));

        for i in 0..10 {
            tier.demote("p1", "t", Bucket { bucket_id: 100 + i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        }
        let s = tier.stats();
        assert_eq!((s.files, s.writes), (0, PROBE_DEMOTES), "a suppressed table writes nothing");
        // Another table is judged entirely on its own files.
        tier.demote("p2", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(tier.stats().files, 1);
    }

    /// A table nobody mutates is never suppressed, however much it demotes.
    #[test]
    fn untouched_table_is_never_suppressed() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..50i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(2)], min_ts: i * 100, max_ts: i * 100 + 50, covers_window: true });
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
        let t0 = crate::support::set_micros(crate::support::now_micros());
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
            tier.invalidate("p1", "t");
        }
        assert_eq!(tier.stats().suppressions, 1);
        tier.demote("p1", "t", Bucket { bucket_id: 10, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(tier.stats().files, 0, "still inside the cooldown");

        crate::support::set_micros(t0 + SUPPRESSION_COOLDOWN.as_micros() as i64 + 1);
        tier.demote("p1", "t", Bucket { bucket_id: 11, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        let s = tier.stats();
        assert_eq!((s.files, s.suppressed_tables), (1, 0), "cooldown elapsed → demotion resumes");
        // ...and if the workload hasn't changed, one wasted probe file is
        // enough to re-convict (REPROBE_DEMOTES), so the tier spends ~one file
        // per cooldown instead of one per flush.
        tier.invalidate("p1", "t");
        assert_eq!(tier.stats().suppressions, 2);
        crate::support::unfreeze();
    }

    /// Suppression only gates WRITES: files already on disk keep serving.
    #[serial]
    #[tokio::test]
    async fn suppression_does_not_affect_reads() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..PROBE_DEMOTES as i64 - 1 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
            tier.invalidate("p1", "t");
        }
        // Convicted by this write (3 wasted of 4), but the file itself stays.
        tier.demote("p1", "t", Bucket { bucket_id: 3, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        let s = tier.stats();
        assert_eq!((s.suppressed_tables, s.files), (1, 1));

        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert_eq!(leg.ranges, vec![(10, 21)]);
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        assert_eq!(tier.stats().read_hits, 1);
    }

    /// Only an unbounded scan skips the tier. Everything with a lower bound
    /// consults it, however deep — the tier's contribution is a window that does
    /// not go to R2, and since the leg streams there is no heap left to protect.
    #[test]
    fn only_an_unbounded_scan_skips_the_hot_leg() {
        let hour = 3_600_000_000i64;
        assert!(!skip_for_lookback(Some(hour)), "a 1h dashboard is exactly what the tier is for");
        assert!(!skip_for_lookback(Some(6 * hour)));
        // A 14d scan still consults it: whatever the tier holds of those 14 days
        // is served locally instead of from R2. The old gate rejected this only
        // because the leg was materialized eagerly into un-pooled heap.
        assert!(!skip_for_lookback(Some(14 * 24 * hour)), "deep scans still take whatever the tier has");
        assert!(skip_for_lookback(None), "no lower bound = all of history; the tier is a rounding error");
    }

    /// A file decoding to ZERO batches would pass the schema check (`first()`
    /// is None), push its range, and contribute no rows — excluding that window
    /// from Delta while serving nothing. `demote` rejects all-empty input, so
    /// this is unreachable today; the coverage contract must not rest on that.
    #[tokio::test]
    async fn zero_batch_file_contributes_no_range() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        let path = tier.buckets_in_range("p1", "t", None)[0].path.clone();

        // A well-formed IPC file carrying the schema and no record batches.
        let f = fs::File::create(&path).unwrap();
        FileWriter::try_new_with_options(f, schema().as_ref(), IpcWriteOptions::default()).unwrap().finish().unwrap();
        // `footer_is_readable` rejects it at plan time, so it costs the file
        // both its rows and its exclusion — exactly as the eager decode did.
        assert!(!footer_is_readable(&path), "a file with no record batches is unusable");
        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert!(leg.is_empty(), "no rows AND no exclusion: the window falls through to Delta, not into a hole");
    }

    /// PROD 2026-08-07 — the hole shape. A file whose rows the predicate ALL
    /// filters out used to push its range anyway (`ranges.push` was
    /// unconditional; only `partitions.push` was guarded), excluding that whole
    /// time window from the Delta leg while contributing nothing to serve it.
    /// Customers saw it as recurring multi-minute gaps in their dashboards; the
    /// bad plan's predicate literally excluded 09:15:05-09:19:58 that day.
    ///
    /// A time-range exclusion may only be claimed by a file that actually hands
    /// over rows — it vouches for the rows it holds, never for a window.
    #[tokio::test]
    async fn file_filtered_to_nothing_contributes_no_range() {
        use datafusion::prelude::{col, lit};
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });

        // Matches no row in the file, so the leg serves nothing...
        let never = col("ts").eq(lit(i64::MIN));
        let leg = tier.scan("p1", "t", None, &[], &[never], &schema(), None);
        assert!(served(&leg).await.is_empty(), "no rows survive the predicate");
        // ...but it STILL claims its window, and that is sound. A claim requires
        // `covers_window` on a single-file bucket, i.e. the file holds every row
        // in the span, so Delta's copies of those rows are duplicates of the
        // ones just filtered away — the same predicate empties them too.
        assert_eq!(leg.ranges, vec![(10, 21)], "a PROVEN window may be excluded even when the predicate empties it");

        // Control: a matching predicate claims the range for the original
        // reason — or the Delta leg would double-count the rows just served.
        let all = col("ts").gt_eq(lit(i64::MIN));
        let leg = tier.scan("p1", "t", None, &[], &[all], &schema(), None);
        assert!(!served(&leg).await.is_empty() && leg.ranges.len() == 1, "serving rows ⇒ claim the range");
    }

    /// PROD 2026-08-07 — the hole. A file claimed a Delta exclusion over its
    /// whole `[min_ts, end_ts]` while holding only the rows one drain carried.
    /// Delta legitimately holds other rows in that span (a later drain of the
    /// same bucket, late arrivals), and the exclusion hid them: project
    /// 00000000, 09:14-09:21 read 4322 rows against a true 11349.
    ///
    /// A range may only be claimed with coverage PROVEN — and one complete
    /// drain is not proof when the bucket was demoted more than once.
    #[tokio::test]
    async fn range_is_claimed_only_when_coverage_is_proven() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);

        // One full drain of a bucket: proven, so it may exclude its window.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(tier.scan("p1", "t", None, &[], &[], &schema(), None).ranges.len(), 1, "a proven, single-file bucket claims its window");

        // A SECOND drain of the SAME bucket. Each file holds only PART of the
        // span, but the two are jointly authoritative, and the tier is serving
        // both — so each claims the window it actually holds. This is only
        // sound because a file can no longer go missing behind the leg's back:
        // deletion is bucket-atomic and a skipped demote retracts the bucket.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 21, max_ts: 30, covers_window: true });
        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        assert_eq!(leg.ranges.len(), 2, "a COMPLETE twice-drained bucket claims both halves it holds");
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 8, "and both files still SERVE their rows");

        // A file from an older binary carries no claim and must not invent one.
        tier.demote("p2", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: false });
        let leg = tier.scan("p2", "t", None, &[], &[], &schema(), None);
        assert!(leg.ranges.is_empty(), "unproven coverage ⇒ no exclusion; the window falls through to Delta");
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 4, "rows are still served");
        assert!(tier.stats().unproven_windows >= 1, "the unmarked file is still an unproven window");
    }

    /// A project/table name that can't round-trip through a path component is
    /// silently NOT demoted — a lossy mapping could collide two tenants in one
    /// directory, and rescan reconstructs the key from the directory name.
    #[test]
    fn unsafe_names_are_not_demoted() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        tier.demote("../escape", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        tier.demote("p1", "sub/dir", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(tier.stats().files, 0);
        assert_eq!(tier.stats().write_failures, 0, "an unsafe name is a skip, not a failure");
    }

    /// The leg must be a PLAN, not rows. This is the whole change: the eager
    /// leg decoded every file while planning (prod 2026-08-14: `hot_plan_us_avg`
    /// 954 ms on 98% of scans, held outside every memory pool). Planning must
    /// now touch no batch, and each file must land in its own partition so they
    /// decode in parallel instead of one after another.
    #[tokio::test]
    async fn planning_builds_a_partitioned_plan_without_decoding() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..3i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(64)], min_ts: i * 100, max_ts: i * 100 + 10, covers_window: true });
        }
        let leg = tier.scan("p1", "t", None, &[], &[], &schema(), None);
        let plan = leg.plan.clone().expect("three usable files must produce a leg");
        assert_eq!(plan.properties().output_partitioning().partition_count(), 3, "one partition per file, so the files decode concurrently");

        // The rows exist only once the plan runs.
        assert_eq!(served(&leg).await.iter().map(|b| b.num_rows()).sum::<usize>(), 192);
    }
}
