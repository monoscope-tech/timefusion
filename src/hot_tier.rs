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
    buffer::Buffer,
    datatypes::{DataType, SchemaRef, TimeUnit},
};
use arrow_ipc::{
    Block, CompressionType, convert::fb_to_schema, reader::FileDecoder, reader::read_footer_length, root_as_footer, writer::FileWriter, writer::IpcWriteOptions,
};
use dashmap::{DashMap, DashSet};
use datafusion::{
    error::Result as DFResult,
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    logical_expr::Expr,
    physical_expr::PhysicalExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream},
};
use tracing::{debug, info, warn};

use crate::mem_buffer::{TableKey, compile_filter_conjunction, filter_snapshot, overlaps, table_key};

/// Reconcile a demoted file's schema with the table's, tolerating ONLY a
/// nullability difference: names, order and data types must match exactly.
/// Returns `None` when they genuinely drift, or when a column the table
/// declares NOT NULL actually contains nulls (re-stamping there would hand
/// DataFusion a batch that lies about its own data).
///
/// Cheap: metadata-only when the schemas already agree, otherwise one
/// `null_count()` per tightened column plus a `RecordBatch` rebuild that
/// re-uses the same `ArrayData` (no row copy).
fn align_nullability(batches: &[RecordBatch], schema: &SchemaRef) -> Option<Vec<RecordBatch>> {
    let first = batches.first()?;
    let have = first.schema();
    if have.fields() == schema.fields() {
        return Some(batches.to_vec());
    }
    let compatible = have.fields().len() == schema.fields().len()
        && have.fields().iter().zip(schema.fields()).all(|(h, w)| h.name() == w.name() && h.data_type() == w.data_type());
    if !compatible {
        return None;
    }
    // Only columns the table tightens need checking; widening is always safe.
    let tightened: Vec<usize> =
        schema.fields().iter().enumerate().filter_map(|(i, w)| (!w.is_nullable() && have.field(i).is_nullable()).then_some(i)).collect();
    batches
        .iter()
        .map(|b| tightened.iter().all(|&i| b.column(i).null_count() == 0).then(|| RecordBatch::try_new(schema.clone(), b.columns().to_vec()).ok())?)
        .collect()
}

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
/// handful of files off a scan already dominated by thousands.
///
/// The threshold is a MULTIPLE of the retention window, not the window itself.
/// At exactly one window a query for the tier's own span sits right on the
/// line: a dashboard asking for 6h against 6h of retention computes its
/// lookback from a `now()` sampled before the scan runs, so it lands a few
/// micros OVER and skips the tier — the tier is at its most useful for exactly
/// the query that cannot use it. The heap it could waste is already bounded
/// precisely and independently by `limits.leg_budget_bytes`, charged on the
/// post-filter bytes actually retained, so this test only has to reject scans
/// so deep the tier is a rounding error in the answer. (Same mistake, same
/// fix, as the wide-scan gate: do not let a depth proxy stand in for a work
/// bound that already exists.)
///
/// `retention_micros = 0` (tier off) still rejects everything.
pub fn skip_for_lookback(lookback: Option<i64>, retention_micros: i64) -> bool {
    lookback.is_none_or(|d| d > retention_micros.saturating_mul(LOOKBACK_WINDOWS))
}

/// How many retention windows deep a scan may reach and still consult the tier.
/// 2 keeps the tier's own span (and a little slack for clock skew and a
/// half-open bound) comfortably inside, while still rejecting the multi-day
/// scans the eager materialization was never meant to serve.
const LOOKBACK_WINDOWS: i64 = 2;

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
    /// demoted more than once has several files each covering part of the
    /// span, so `read_leg` additionally requires the bucket to have exactly one.
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
    /// One batch partition per served file.
    pub partitions: Vec<Vec<RecordBatch>>,
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
    /// Every served partition is ordered by the table's declared
    /// `sorting_columns`, so the caller may declare that ordering on the
    /// `MemorySourceConfig`. True exactly when the table declares sorting
    /// columns: `write_bucket` sorts every file it writes and refuses to write
    /// one it could not sort, and projection/filtering preserve row order.
    pub sorted: bool,
    /// Post-filter bytes the served partitions hold — the heap this leg keeps
    /// alive for the life of the plan. `HotLegPooledExec` charges exactly this
    /// to the query's memory pool.
    pub bytes: u64,
}

impl HotLeg {
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty() && self.ranges.is_empty()
    }
}

/// Charges the hot leg's materialized bytes to the query's memory pool.
///
/// The leg is built eagerly at plan time (`read_leg`) into plain
/// `RecordBatch`es that live until the plan drops, invisible to every
/// DataFusion pool — N concurrent scans could otherwise stack N ×
/// `leg_budget_bytes` of unaccounted heap. The first `execute` call reserves
/// the leg's post-filter size from the query pool; the reservation is held by
/// the plan node itself, so it frees exactly when the batches do. The charge
/// happens at execute rather than plan time only because planning has no pool
/// in scope — the allocation it accounts for already exists either way, so a
/// failed `try_grow` fails the query (the sound direction: the leg's ranges
/// were already excluded from the Delta leg, serving without it would drop
/// windows).
#[derive(Debug)]
pub struct HotLegPooledExec {
    inner: Arc<dyn ExecutionPlan>,
    bytes: u64,
    reservation: std::sync::Mutex<Option<MemoryReservation>>,
}

impl HotLegPooledExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, bytes: u64) -> Self {
        Self { inner, bytes, reservation: std::sync::Mutex::new(None) }
    }
}

impl DisplayAs for HotLegPooledExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "HotLegPooledExec: bytes={}", self.bytes)
    }
}

impl ExecutionPlan for HotLegPooledExec {
    fn name(&self) -> &'static str {
        "HotLegPooledExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0), self.bytes)))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let mut guard = self.reservation.lock().unwrap();
        if guard.is_none() {
            let r = MemoryConsumer::new("HotTierLeg").register(context.memory_pool());
            r.try_grow(self.bytes as usize)?;
            *guard = Some(r);
        }
        drop(guard);
        self.inner.execute(partition, context)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<datafusion::common::Statistics>> {
        self.inner.partition_statistics(partition)
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
    /// Files whose rows were ALL filtered out. They must contribute no
    /// exclusion either; nonzero means windows are falling through to Delta
    /// (correct) rather than vanishing (the 2026-08-07 hole shape).
    pub empty_after_filter: u64,
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
    /// Files successfully served since boot. Projection-specific decodes are
    /// intentionally not in `decoded`, so memo presence is no longer a sound
    /// proxy for whether demotion paid off before DML invalidated a file.
    read_files: DashSet<PathBuf>,
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
    empty_after_filter: AtomicU64,
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
    pub fn open(root: PathBuf, retention: Option<Duration>, limits: HotTierLimits) -> Arc<Self> {
        let tier = Self::open_lazy(root, retention, limits);
        tier.finish_open();
        tier
    }

    /// Construct the cache without walking its directory. Until
    /// [`Self::finish_open`] completes, missing index entries simply fall
    /// through to Delta, so application readiness need not wait on a large
    /// derived-cache scan.
    pub fn open_lazy(root: PathBuf, retention: Option<Duration>, limits: HotTierLimits) -> Arc<Self> {
        let tier = Arc::new(Self { root, retention, limits, ..Default::default() });
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
        if self.retention.is_none() {
            // Disabled, but still responsible for its own directory: without
            // this a previous run's files sit there forever, unbounded and
            // invisible (the disk-leak failure mode of the orphaned spill dirs
            // and the never-evicted tantivy cache).
            self.gc(0);
        }
    }

    /// Demote one committed bucket, whose rows span the INCLUSIVE `[min_ts,
    /// max_ts]` MemBuffer tracks. Best-effort: a failure is counted and
    /// logged, never propagated (the rows are already durable in Delta).
    pub fn demote(&self, project_id: &str, table_name: &str, bucket: Bucket<'_>) {
        let Bucket { bucket_id, batches, min_ts, max_ts, covers_window } = bucket;
        if self.retention.is_none() || min_ts > max_ts || batches.iter().all(|b| b.num_rows() == 0) {
            return;
        }
        let key = table_key(project_id, table_name);
        let health = self.health(&key);
        if health.suppressed() {
            return;
        }
        match self.write_bucket(project_id, table_name, Bucket { bucket_id, batches, min_ts, max_ts: max_ts + 1, covers_window }) {
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
        let owned = crate::schema_loader::get_schema(table_name)
            .filter(|s| !s.sorting_columns.is_empty())
            .and_then(|s| crate::mem_buffer::sort_partition(s, batches.to_vec()));
        let (batches, sorted) = owned.as_deref().map_or((batches, false), |b| (b, true));
        let Some(schema) = batches.first().map(|b| b.schema()) else { return Ok(None) };
        let dir = self.root.join(project_id).join(table_name);
        fs::create_dir_all(&dir)?;
        let seq = self.seq.fetch_add(1, Relaxed);
        // The stamp rides in the FILENAME so `rescan` recovers the gate after a
        // restart without reopening every file.
        let max_stamp = crate::schema_loader::get_schema(table_name)
            .filter(|s| s.version_append)
            .and_then(|s| s.dedup_tiebreak.clone())
            .and_then(|c| max_stamp_of(batches, &c));
        let stamp_suffix = max_stamp.map(|s| format!("_{s}")).unwrap_or_default();
        let sort_marker = if sorted { "s" } else { "" };
        let covers_marker = if covers_window { "c" } else { "" };
        let path = dir.join(format!("{bucket_id}_{min_ts}_{end_ts}_{seq}{sort_marker}{covers_marker}{stamp_suffix}.{EXT}"));
        crate::wal::write_atomic_with(&path, true, |f| {
            // Compress each IPC buffer independently. FileDecoder projection
            // skips unrequested buffers before decompression, so a narrow query
            // pays only for its columns while LZ4 lets the 24h tier fit under
            // the fixed disk ceiling. The previous uncompressed representation
            // saturated 128GB after only part of the desired window.
            let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME)).map_err(std::io::Error::other)?;
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
        let Some(entry) = self.index.get(&table_key(project_id, table_name)) else { return Vec::new() };
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
    ) -> HotLeg {
        let metas = self.buckets_in_range(project_id, table_name, query_range);
        if metas.is_empty() {
            return Default::default();
        }
        let declared = crate::schema_loader::get_schema(table_name);
        let versioned = declared.as_ref().is_some_and(|s| s.version_append);
        let orders = declared.is_some_and(|s| !s.sorting_columns.is_empty());
        let (tier, mem_ranges, filters, schema, projection) = (self.clone(), mem_ranges.to_vec(), filters.to_vec(), schema.clone(), projection.cloned());
        // open + mmap + decode + page faults are blocking syscalls; a planner
        // future must not run them on its async worker.
        tokio::task::spawn_blocking(move || tier.read_leg(&metas, &mem_ranges, &filters, &schema, projection.as_deref(), versioned, orders))
            .await
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    fn read_leg(
        &self, metas: &[HotBucketMeta], mem_ranges: &[(i64, i64)], filters: &[Expr], schema: &SchemaRef, projection: Option<&[usize]>, versioned: bool,
        orders: bool,
    ) -> HotLeg {
        let cols = plan_columns(projection, filters, schema);
        let filter_schema = cols.as_ref().map_or_else(|| schema.clone(), |c| c.schema.clone());
        let pred = compile_filter_conjunction(filters, &filter_schema).ok().flatten();
        // Imperative: `used` is a running total whose overflow must stop the
        // walk, and each step is a counted/logged side effect.
        // `sorted` is an AND across the files actually SERVED: projection and
        // filtering preserve row order, so a leg of sorted files is sorted, but
        // one unmarked file (older binary, unsortable batches) retracts the
        // claim for all of them — `try_with_sort_information` declares one
        // ordering for the whole source.
        // A bucket demoted more than once has several files, each holding part
        // of the span while `min_ts..end_ts` spans it — so no single one may
        // claim the window even if its own drain was complete.
        let mut files_per_bucket: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
        for m in metas {
            *files_per_bucket.entry(m.bucket_id).or_default() += 1;
        }
        let (mut partitions, mut ranges, mut used, mut gate, mut sorted) = (Vec::new(), Vec::new(), 0u64, None::<i64>, orders);
        for meta in metas {
            let Some(filtered) = self.materialize(meta, mem_ranges, cols.as_ref(), &pred, schema) else { continue };
            // The hot leg is materialized at PLAN time into a
            // `MemorySourceConfig` — no `GatedScanExec`, no spill, and the
            // query pool only learns of it at execute time via
            // `HotLegPooledExec`. `limits.leg_budget_bytes` is therefore what
            // bounds ONE scan's materialization, charged on the post-filter
            // bytes actually retained (`filter_record_batch` heap-copies, so
            // these are real allocations, unlike the zero-copy decode they
            // came from).
            //
            // Range and partition are pushed TOGETHER, after the check: a file
            // whose rows we refuse to hold must also not be excluded from the
            // Delta leg, or its window is served by nobody.
            // Contributing NOTHING must also mean excluding nothing. `ranges`
            // excludes a whole TIME WINDOW from the Delta leg, but a file only
            // ever vouches for the rows it actually hands over; when the
            // predicate retains none, the exclusion would still hide every Delta
            // row in that window and the leg would serve no replacement — the
            // window is then served by nobody. Skipping is safe in the other
            // direction: the same predicate filters the Delta copy identically,
            // so falling through cannot duplicate rows.
            if filtered.is_empty() {
                self.empty_after_filter.fetch_add(1, Relaxed);
                continue;
            }
            let bytes: u64 = filtered.iter().map(|b| b.get_array_memory_size() as u64).sum();
            if used + bytes > self.limits.leg_budget_bytes {
                self.leg_budget_stops.fetch_add(1, Relaxed);
                debug!("hot leg stopped at {used} bytes (budget {}); {:?} and older windows fall through to Delta", self.limits.leg_budget_bytes, meta.path);
                break;
            }
            used += bytes;
            sorted &= meta.sorted;
            // Serve the rows either way; only the EXCLUSION needs proof. An
            // unproven window is read from both legs and DedupExec collapses
            // the overlap — slower, never wrong.
            if meta.covers_window && files_per_bucket.get(&meta.bucket_id) == Some(&1) {
                ranges.push(meta.range());
            } else {
                self.unproven_windows.fetch_add(1, Relaxed);
            }
            // The gate travels with the range it guards: a file that cannot
            // vouch for a stamp (`None`) drops it to `i64::MIN`, which admits
            // every stamped Delta row in the excluded windows — the safe
            // direction, since an over-admitted row is a duplicate `DedupExec`
            // collapses while an under-admitted one is a stale read.
            if versioned {
                gate = Some(gate.unwrap_or(i64::MAX).min(meta.max_stamp.unwrap_or(i64::MIN)));
            }
            partitions.push(filtered);
        }
        HotLeg { partitions, ranges, version_gate: gate, sorted, bytes: used }
    }

    /// One file's contribution to the leg, projected and filtered. `None` =
    /// unusable, which by the coverage contract means NO rows AND no exclusion
    /// range, so that window falls through to Delta intact.
    fn materialize(
        &self, meta: &HotBucketMeta, mem_ranges: &[(i64, i64)], cols: Option<&LegColumns>, pred: &Option<Arc<dyn PhysicalExpr>>, schema: &SchemaRef,
    ) -> Option<Vec<RecordBatch>> {
        if mem_ranges.iter().any(|r| overlaps(meta.range(), *r)) {
            self.mem_skipped.fetch_add(1, Relaxed);
            return None;
        }
        // Decode only the columns this query needs. Projecting an already-
        // decoded full batch is too late: Arrow IPC decoding touches every
        // column buffer first, page-faulting tens of GB from the mmap for a
        // four-column count query. Projection at the decoder keeps the unused
        // body/attribute buffers cold.
        let batches = match cols {
            Some(c) => self.read_file_projected(&meta.path, &c.needed)?,
            None => self.read_file(&meta.path)?,
        };
        // A file that decodes to NOTHING would pass the schema check below
        // (`first()` is None), report its range and contribute no rows — the one
        // shape that silently excludes a window from Delta and serves it from
        // nowhere. `demote` rejects all-empty input so this is unreachable
        // today; the coverage contract must not depend on that.
        if batches.is_empty() {
            self.read_misses.fetch_add(1, Relaxed);
            return None;
        }
        // One schema per IPC file by construction, so one compare per file.
        // Drift (a column added since the demotion) breaks the
        // `MemorySourceConfig` contract — treat the file as absent.
        //
        // Nullability alone is NOT drift. Prod 2026-07-31: demoted batches
        // carried `timestamp` nullable while the table declares it NOT NULL, so
        // strict field equality rejected 100% of reads — the tier served zero
        // rows while files/bytes/writes all looked healthy (caught only by
        // `schema_drift_total` == `read_hits_total`). Re-stamp instead, once the
        // data proves it satisfies the stricter schema.
        //
        // The root cause was upstream — MemBuffer pinned whatever nullability
        // the first batch to arrive happened to carry — and is fixed at the
        // source in `mem_buffer::align_nullability`. This stays as tolerance for
        // files demoted by an older binary.
        let expected_schema = cols.map_or_else(|| schema.clone(), |c| c.schema.clone());
        let Some(batches) = align_nullability(batches.as_ref(), &expected_schema) else {
            self.schema_drift.fetch_add(1, Relaxed);
            return None;
        };
        let filtered = filter_snapshot(batches, pred);
        // Cut the predicate-only columns back off (positions are `0..n` by
        // construction in `plan_columns`, so this cannot fail).
        Some(match cols.and_then(|c| c.output.as_ref()) {
            Some(out) => filtered.iter().flat_map(|b| b.project(out)).collect(),
            None => filtered,
        })
    }

    /// Zero-copy read of one demoted file, memoized. `None` = treat as absent
    /// (torn, unlinked under us, or written by something that isn't us) — the
    /// caller must then let the Delta leg serve that window.
    pub fn read_file(&self, path: &Path) -> Option<Arc<Vec<RecordBatch>>> {
        if let Some(hit) = self.decoded.get(path) {
            hit.used.store(self.memo_clock.fetch_add(1, Relaxed), Relaxed);
            self.read_hits.fetch_add(1, Relaxed);
            self.read_files.insert(path.to_path_buf());
            return Some(hit.batches.clone());
        }
        match decode_file(path, false, None) {
            Ok(batches) => {
                self.read_hits.fetch_add(1, Relaxed);
                self.read_files.insert(path.to_path_buf());
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

    /// Projection-aware IPC decode for query legs. Projection-specific batches
    /// are deliberately not stored in the full-file memo: mixing projections
    /// under a path-only key is incorrect, while decoding the narrow mmap view
    /// is cheap and avoids faulting unused column pages into memory.
    fn read_file_projected(&self, path: &Path, projection: &[usize]) -> Option<Arc<Vec<RecordBatch>>> {
        match decode_file(path, false, Some(projection)) {
            Ok(batches) => {
                self.read_hits.fetch_add(1, Relaxed);
                self.read_files.insert(path.to_path_buf());
                Some(Arc::new(batches))
            }
            Err(e) => {
                self.read_misses.fetch_add(1, Relaxed);
                debug!("hot tier file {path:?} projected decode failed, falling through to Delta: {e:#}");
                None
            }
        }
    }

    /// Memoize one full-schema decode and enforce the memo's byte ceiling.
    /// Charge the larger of file bytes and decoded Arrow bytes: legacy files
    /// are zero-copy mmap views (file size dominates), while compressed files
    /// own decompressed buffers (Arrow size dominates). Under-counting the
    /// latter would turn LZ4's disk win into invisible heap growth.
    fn memo_insert(&self, path: &Path, batches: Arc<Vec<RecordBatch>>) {
        let Ok(file_bytes) = fs::metadata(path).map(|m| m.len()) else { return };
        let decoded_bytes = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
        let bytes = file_bytes.max(decoded_bytes);
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
            drop_unless(entry.value_mut(), |m| !overlaps(m.range(), (lo, hi))).into_iter().map(|m| m.path).collect()
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
            self.memo_forget(p);
            self.read_files.remove(p);
            let _ = fs::remove_file(p);
        }
    }

    /// Rebuild the index from disk (boot warmth). Walks only our own two-level
    /// layout and only accepts files matching our naming convention; anything
    /// else is left strictly alone.
    pub fn rescan(&self) {
        self.index.clear();
        self.decoded.clear();
        self.read_files.clear();
        self.memo_bytes.store(0, Relaxed);
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
        let age_cutoff = self.retention.map_or(i64::MAX, |r| now_micros - r.as_micros() as i64);
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
        // Collect under the shard guards but unlink AFTER dropping them — N
        // unlink syscalls inside a write guard block every concurrent query's
        // range lookup for the duration.
        let dropped: Vec<HotBucketMeta> = self.index.iter_mut().flat_map(|mut e| drop_unless(e.value_mut(), |m| m.end_ts > cutoff)).collect();
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
        let now = crate::clock::now_micros();
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
            empty_after_filter: self.empty_after_filter.load(Relaxed),
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
pub(crate) fn plan_leg(metas: &[HotBucketMeta], mem_ranges: &[(i64, i64)], versioned: bool, orders: bool) -> LegPlan {
    // Counted over ALL metas, not just served ones: a bucket demoted more than
    // once has several files each holding PART of the span while `min_ts..
    // end_ts` spans it, so no single one may claim the window. A sibling that
    // MemBuffer still owns is exactly such a partial file — dropping it from the
    // count would let the survivor claim a window it only partly holds.
    let mut files_per_bucket: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
    for m in metas {
        *files_per_bucket.entry(m.bucket_id).or_default() += 1;
    }
    metas
        .iter()
        .enumerate()
        // A file MemBuffer still owns is skipped whole — no rows and no
        // exclusion, so the window stays with the tier holding the fresher copy.
        .filter(|(_, m)| !mem_ranges.iter().any(|r| overlaps(m.range(), *r)))
        .fold(LegPlan { sorted: orders, ..Default::default() }, |mut plan, (i, m)| {
            plan.served.push(i);
            // One unmarked file retracts the claim for the whole leg:
            // `try_with_sort_information` declares ONE ordering for the source.
            plan.sorted &= m.sorted;
            // The gate travels with the ranges it guards, so it is the MIN over
            // every served file: an unknown stamp drops it to `i64::MIN`, which
            // admits every stamped Delta row — the safe direction.
            if versioned {
                plan.version_gate = Some(plan.version_gate.unwrap_or(i64::MAX).min(m.max_stamp.unwrap_or(i64::MIN)));
            }
            if m.covers_window && files_per_bucket.get(&m.bucket_id) == Some(&1) {
                plan.ranges.push(m.range());
            }
            plan
        })
}

/// mmap + validate + decode. `require_alignment` is the arrow-rs knob that
/// turns a silent realigning COPY into an error — production reads leave it
/// off (correctness over strictness), tests turn it on to prove the
/// zero-copy assumption actually holds for files we write.
fn decode_file(path: &Path, require_alignment: bool, projection: Option<&[usize]>) -> anyhow::Result<Vec<RecordBatch>> {
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
        HotTierLimits { max_disk_bytes, leg_budget_bytes: u64::MAX, memo_bytes: u64::MAX }
    }

    fn open(dir: &tempfile::TempDir) -> Arc<HotTier> {
        HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX))
    }

    /// PROD REGRESSION 2026-07-31: demoted batches carried `timestamp` as
    /// NULLABLE while the table declares it NOT NULL, and the leg's strict
    /// field-equality check discarded 100% of reads — `schema_drift_total`
    /// tracked `read_hits_total` exactly (75/75) while files/bytes/writes all
    /// looked healthy, so the tier burned disk and RAM serving zero rows.
    /// Nullability alone must NOT count as drift.
    #[test]
    fn nullability_difference_is_not_drift_but_real_nulls_are() {
        let nullable = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, true), Field::new("name", DataType::Utf8, false)]));
        let strict = schema(); // ts NOT NULL

        // Widened-on-disk, no actual nulls => re-stamped to the strict schema.
        let b = RecordBatch::try_new(nullable.clone(), vec![Arc::new(Int64Array::from(vec![1i64, 2])), Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap();
        let out = align_nullability(&[b], &strict).expect("nullability alone is not drift");
        assert_eq!(out[0].schema().fields(), strict.fields(), "batch is re-stamped to the table schema");
        assert_eq!(out[0].num_rows(), 2, "no rows lost");

        // A real NULL in a NOT NULL column must still be rejected — re-stamping
        // there would hand DataFusion a batch that lies about its own data.
        let with_null =
            RecordBatch::try_new(nullable, vec![Arc::new(Int64Array::from(vec![Some(1i64), None])), Arc::new(StringArray::from(vec!["a", "b"]))]).unwrap();
        assert!(align_nullability(&[with_null], &strict).is_none(), "a genuine null in a NOT NULL column is drift");

        // A genuinely different column set is still drift.
        let other = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false)]));
        assert!(align_nullability(&[batch(2)], &other).is_none(), "column-count drift still rejected");
    }

    /// New hot files are buffer-compressed and remain lossless. Repetitive
    /// payload makes the capacity property deterministic: the file must be
    /// materially smaller than its decoded Arrow representation.
    #[test]
    fn roundtrip_is_compressed_aligned_and_lossless() {
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
        assert!(metas[0].bytes < b.get_array_memory_size() as u64 / 2, "LZ4 hot file should fit far more history under the disk cap");
        assert_eq!(decode_file(&metas[0].path, true, None).unwrap(), vec![b], "compressed IPC must decode aligned and byte-identical");
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
        let p = plan_leg(&full, &[], true, true);
        assert_eq!(p, LegPlan { served: vec![0, 1], ranges: vec![(0, 10), (20, 30)], version_gate: Some(7), sorted: true }, "baseline claims everything");

        // A table that declares no sorting columns cannot claim an ordering
        // however its files are marked; one unsorted file retracts the claim
        // for the whole leg, because the source declares ONE ordering.
        assert!(!plan_leg(&full, &[], true, false).sorted, "no declared sorting columns => no claim");
        let mixed = vec![full[0].clone(), meta(2, 20, 30, true, false, Some(9))];
        assert!(!plan_leg(&mixed, &[], true, true).sorted, "one unsorted file retracts the leg's ordering");
        assert_eq!(plan_leg(&mixed, &[], true, true).ranges, p.ranges, "...but it still serves and still excludes");

        // The gate is the MIN over served files, and an unknown stamp drops it
        // to i64::MIN — admitting every stamped Delta row, never hiding one.
        assert_eq!(plan_leg(&full, &[], false, true).version_gate, None, "a table that appends no versions needs no gate");
        let unstamped = vec![full[0].clone(), meta(2, 20, 30, true, true, None)];
        assert_eq!(plan_leg(&unstamped, &[], true, true).version_gate, Some(i64::MIN), "an unknown stamp admits everything");

        // `covers_window` is necessary but not sufficient: a bucket demoted
        // more than once holds only PART of its span in each file.
        let partial = vec![meta(1, 0, 10, false, true, Some(7)), full[1].clone()];
        assert_eq!(plan_leg(&partial, &[], true, true).ranges, vec![(20, 30)], "an incomplete drain claims no window");
        let split = vec![meta(1, 0, 10, true, true, Some(7)), meta(1, 0, 10, true, true, Some(8)), full[1].clone()];
        let p = plan_leg(&split, &[], true, true);
        assert_eq!(p.ranges, vec![(20, 30)], "two files for one bucket: neither may claim the span");
        assert_eq!(p.served, vec![0, 1, 2], "...though both still SERVE their rows");
    }

    /// A file MemBuffer still owns contributes nothing AND excludes nothing —
    /// but it still counts against its bucket, or the sibling that survives
    /// would claim a window the two of them only jointly cover.
    #[test]
    fn plan_leg_skips_files_membuffer_still_owns_without_forfeiting_their_bucket() {
        let metas = vec![meta(1, 0, 10, true, true, Some(7)), meta(2, 20, 30, true, true, Some(9))];
        let p = plan_leg(&metas, &[(5, 15)], true, true);
        assert_eq!(p.served, vec![1], "the overlapped file is skipped whole");
        assert_eq!(p.ranges, vec![(20, 30)], "and excludes nothing, so that window falls through to Delta");
        assert_eq!(p.version_gate, Some(9), "a skipped file does not constrain the gate");

        // Both halves of a twice-demoted bucket, one of them mem-owned.
        let split = vec![meta(1, 0, 10, true, true, Some(7)), meta(1, 10, 20, true, true, Some(8))];
        let p = plan_leg(&split, &[(12, 14)], true, true);
        assert_eq!(p.served, vec![0], "the mem-owned half is skipped");
        assert!(p.ranges.is_empty(), "the survivor must NOT claim the bucket's span — it holds half of it");
    }

    /// The contract that lets the eager path be replaced. Unfiltered, the two
    /// agree exactly. Under a predicate that empties every file they diverge —
    /// `plan_leg` still excludes, because it cannot see emptiness — and that
    /// divergence is only sound once the rows are streamed rather than admitted
    /// under a budget. See `LegPlan`.
    #[tokio::test]
    async fn plan_leg_agrees_with_the_eager_path_except_where_only_streaming_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let tier = open(&dir);
        for i in 0..4i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(8)], min_ts: i * 100, max_ts: i * 100 + 10, covers_window: true });
        }
        let metas = tier.buckets_in_range("p1", "t", None);
        let eager = tier.read_leg(&metas, &[], &[], &schema(), None, false, false);
        let plan = plan_leg(&metas, &[], false, false);
        assert_eq!(plan.ranges, eager.ranges, "unfiltered, the metadata claims are exactly the eager ones");
        assert_eq!(plan.served.len(), eager.partitions.len());
        assert_eq!((plan.version_gate, plan.sorted), (eager.version_gate, eager.sorted));

        // A predicate no row satisfies: the eager path sees the emptiness and
        // withholds every exclusion; `plan_leg` cannot, and excludes all four.
        // That is SAFE because the same predicate empties the Delta copy of
        // those windows identically — an excluded window with no rows on either
        // side loses nothing.
        let never = vec![datafusion::prelude::col("ts").lt(datafusion::prelude::lit(i64::MIN))];
        let eager = tier.read_leg(&metas, &[], &never, &schema(), None, false, false);
        assert!(eager.ranges.is_empty() && eager.partitions.is_empty(), "the eager path serves and excludes nothing");
        assert_eq!(plan_leg(&metas, &[], false, false).ranges.len(), 4, "the lazy plan still excludes — the predicate empties Delta identically");
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
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &filters, &schema(), Some(&vec![1])).await;
        assert_eq!(ranges, vec![(10, 21)]);
        let batches: Vec<_> = parts.into_iter().flatten().collect();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "only ts=6,7 match");

        // A file MemBuffer still owns is skipped whole — no rows, no range, so
        // the window stays with the tier that has the fresher copy.
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[(15, 25)], &filters, &schema(), None).await;
        assert!(parts.is_empty() && ranges.is_empty());
        assert_eq!(tier.stats().mem_skipped, 1);
    }

    /// Merge-on-read version gate. A `version_append` table's demoted file
    /// records the greatest stamp it holds, survives a restart carrying it (the
    /// stamp rides in the filename), and the leg reports the MINIMUM across the
    /// files it served — the conservative bound the caller weakens its Delta
    /// exclusion with. A file that cannot vouch for a stamp drops the gate to
    /// `i64::MIN`, which admits every stamped row rather than hiding one.
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

        let leg = tier.query_partitioned("p1", "mor_versioned", None, &[], &[], &versioned, None).await;
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
        let leg = tier.query_partitioned("p1", "mor_versioned", None, &[], &[], &versioned, None).await;
        assert_eq!(leg.version_gate, Some(i64::MIN), "an unstamped file must not let the gate hide newer Delta rows");

        // A table that does not append versions gets no gate at all — its
        // exclusion stands unweakened, byte-for-byte the pre-merge-on-read plan.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        let leg = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert_eq!(leg.version_gate, None);

        // Restart: `rescan` recovers the stamp from the filename.
        let reopened = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX));
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
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: 30, max_ts: 40, covers_window: true });
        drop(tier);

        // Simulated restart: fresh instance over the same root.
        let restarted = open(&dir);
        let metas = restarted.buckets_in_range("p1", "t", None);
        assert_eq!(metas.iter().map(|m| (m.bucket_id, m.min_ts, m.end_ts)).collect::<Vec<_>>(), vec![(1, 10, 21), (2, 30, 41)]);
        assert_eq!(*restarted.read_file(&metas[0].path).unwrap(), vec![batch(8)]);
        // A post-restart demotion must not collide with a recovered seq.
        restarted.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: 41, max_ts: 42, covers_window: true });
        assert_eq!(restarted.buckets_in_range("p1", "t", None).len(), 3);
    }

    #[test]
    fn gc_expires_by_age_then_caps_disk_and_never_touches_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(1)), limits(u64::MAX));
        let now = 10_000_000i64;
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 1, max_ts: 2, covers_window: true }); // ancient
        tier.demote("p1", "t", Bucket { bucket_id: 2, batches: &[batch(8)], min_ts: now - 1000, max_ts: now, covers_window: true }); // fresh
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
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(365 * 24 * 3600)), limits(u64::MAX));
        for (id, end) in [(1i64, now - 3000), (2, now - 2000), (3, now - 1000)] {
            tier.demote("p1", "t", Bucket { bucket_id: id, batches: &[batch(8)], min_ts: end - 10, max_ts: end, covers_window: true });
        }
        tier.gc(now);
        assert_eq!(tier.stats().files, 3, "nothing ages out under a wide window — the hour count alone would keep everything");

        // Same wide window, but a cap that admits roughly one file: age keeps
        // all three, so the cap must be what evicts, and it must take the
        // OLDEST first.
        let one_file_bytes = tier.stats().bytes / 3;
        let capped = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(365 * 24 * 3600)), limits(one_file_bytes));
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

        let off = HotTier::open(dir.path().to_path_buf(), None, limits(u64::MAX));
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
        let t0 = crate::clock::set_micros(crate::clock::now_micros());
        for i in 0..PROBE_DEMOTES as i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
            tier.invalidate("p1", "t");
        }
        assert_eq!(tier.stats().suppressions, 1);
        tier.demote("p1", "t", Bucket { bucket_id: 10, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
        assert_eq!(tier.stats().files, 0, "still inside the cooldown");

        crate::clock::set_micros(t0 + SUPPRESSION_COOLDOWN.as_micros() as i64 + 1);
        tier.demote("p1", "t", Bucket { bucket_id: 11, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: true });
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

        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
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
        // THE case this threshold exists for: a dashboard asking for the tier's
        // own span computes its lookback from a `now()` sampled before the scan,
        // so it lands just OVER one window. At a 1x threshold that query — the
        // one the tier is most useful for — skipped the tier entirely.
        assert!(!skip_for_lookback(Some(six_h + 1), six_h), "a hair past the window is still the tier's own span, not a deep scan");
        assert!(!skip_for_lookback(Some(2 * six_h), six_h), "slack up to the multiple; leg_budget_bytes bounds the heap, not this");
        assert!(skip_for_lookback(Some(2 * six_h + 1), six_h), "past the multiple the tier is a fraction of the answer");
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
        t0.demote("p1", "t", Bucket { bucket_id: 0, batches: &[batch(64)], min_ts: 0, max_ts: 10, covers_window: true });
        let HotLeg { partitions: parts, .. } = t0.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        let one: u64 = parts.into_iter().flatten().map(|b| b.get_array_memory_size() as u64).sum();

        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { leg_budget_bytes: one * 2, ..limits(u64::MAX) });
        for i in 0..5i64 {
            tier.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(64)], min_ts: i * 100, max_ts: i * 100 + 10, covers_window: true });
        }
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert_eq!(parts.len(), 2, "the budget admits two files' worth and stops");
        assert_eq!(ranges, vec![(0, 11), (100, 111)], "exactly the admitted files' ranges are excluded from Delta — no more");
        assert_eq!(parts.len(), ranges.len(), "one range per admitted partition, always");
        assert!(parts.iter().flatten().map(|b| b.get_array_memory_size() as u64).sum::<u64>() <= one * 2, "retained bytes stay under budget");
        assert_eq!(tier.stats().leg_budget_stops, 1);

        // Budget edge: a budget smaller than ONE file admits nothing at all —
        // and, critically, excludes nothing, so every window falls to Delta.
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { leg_budget_bytes: 1, ..limits(u64::MAX) });
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert!(parts.is_empty() && ranges.is_empty(), "no rows AND no exclusion — the whole window must fall through to Delta");
    }

    /// The decode memo owns either mmap views or decompressed Arrow buffers, so
    /// an unbounded memo can retain the whole tier. It must charge the larger
    /// representation, cap bytes, and evict least-recently-used.
    #[test]
    fn memo_is_byte_capped_and_evicts_lru() {
        let dir = tempfile::tempdir().unwrap();
        let big = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), limits(u64::MAX));
        for i in 0..3i64 {
            big.demote("p1", "t", Bucket { bucket_id: i, batches: &[batch(64)], min_ts: i * 100, max_ts: i * 100 + 10, covers_window: true });
        }
        let metas = big.buckets_in_range("p1", "t", None);
        let file_bytes = metas[0].bytes;
        let decoded = decode_file(&metas[0].path, false, None).unwrap();
        let decoded_bytes = decoded.iter().map(|b| b.get_array_memory_size() as u64).sum::<u64>();
        let entry_bytes = file_bytes.max(decoded_bytes);

        // Cap at two files. Read 0, 1, 2 — the first read must be the victim.
        let tier = HotTier::open(dir.path().to_path_buf(), Some(Duration::from_secs(3600)), HotTierLimits { memo_bytes: entry_bytes * 2, ..limits(u64::MAX) });
        let metas = tier.buckets_in_range("p1", "t", None);
        for m in &metas {
            assert!(tier.read_file(&m.path).is_some());
        }
        let s = tier.stats();
        assert_eq!((s.memo_files, s.memo_evicted), (2, 1), "the memo holds exactly its cap, evicting one");
        assert_eq!(s.memo_bytes, entry_bytes * 2, "memo_bytes tracks decoded ownership: {s:?}");
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
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(8)], min_ts: 10, max_ts: 20, covers_window: true });
        let path = tier.buckets_in_range("p1", "t", None)[0].path.clone();

        // A well-formed IPC file carrying the schema and no record batches.
        let f = fs::File::create(&path).unwrap();
        FileWriter::try_new_with_options(f, schema().as_ref(), IpcWriteOptions::default()).unwrap().finish().unwrap();
        assert_eq!(tier.read_file(&path).unwrap().len(), 0, "it decodes fine — it is just empty");

        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert!(parts.is_empty(), "no rows");
        assert!(ranges.is_empty(), "and NO exclusion range: the window must fall through to Delta, not vanish");
        assert_eq!(tier.stats().read_misses, 1, "counted as a miss, like any other unusable file");
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

        // Matches no row in the file, so the leg retains nothing.
        let never = col("ts").eq(lit(i64::MIN));
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[never], &schema(), None).await;
        assert!(parts.is_empty(), "no rows survive the predicate");
        assert!(ranges.is_empty(), "and NO exclusion range: the window must fall through to Delta, not vanish");
        assert_eq!(tier.stats().empty_after_filter, 1);

        // Control: a predicate that DOES match still claims its range, or the
        // Delta leg would double-count the rows the hot tier just served.
        let all = col("ts").gt_eq(lit(i64::MIN));
        let HotLeg { partitions: parts, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[all], &schema(), None).await;
        assert!(!parts.is_empty() && ranges.len() == 1, "serving rows ⇒ claim the range");
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
        let HotLeg { ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert_eq!(ranges.len(), 1, "a proven, single-file bucket claims its window");

        // A SECOND drain of the SAME bucket: each file now holds only part of
        // the span, so neither may claim it — even though both were full drains.
        tier.demote("p1", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 21, max_ts: 30, covers_window: true });
        let HotLeg { partitions, ranges, .. } = tier.query_partitioned("p1", "t", None, &[], &[], &schema(), None).await;
        assert!(ranges.is_empty(), "multi-file bucket must claim NOTHING: neither file covers the span");
        assert_eq!(partitions.len(), 2, "but both files still SERVE their rows");

        // A file from an older binary carries no claim and must not invent one.
        tier.demote("p2", "t", Bucket { bucket_id: 1, batches: &[batch(4)], min_ts: 10, max_ts: 20, covers_window: false });
        let HotLeg { partitions, ranges, .. } = tier.query_partitioned("p2", "t", None, &[], &[], &schema(), None).await;
        assert!(ranges.is_empty(), "unproven coverage ⇒ no exclusion; the window falls through to Delta");
        assert_eq!(partitions.len(), 1, "rows are still served");
        assert!(tier.stats().unproven_windows >= 3);
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

    /// The leg's plan-time heap must be visible to the query pool: a leg
    /// larger than the pool fails its query with ResourcesExhausted instead of
    /// stacking unaccounted bytes toward the cgroup kill, and the charge is
    /// made ONCE across partitions.
    #[tokio::test]
    async fn pooled_leg_charges_the_query_pool_once_and_oversize_fails() {
        use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
        use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};

        let source = MemorySourceConfig::try_new(&[vec![batch(4)], vec![batch(4)]], schema(), None).unwrap();
        let inner = Arc::new(DataSourceExec::new(Arc::new(source)));
        let ctx = |pool_bytes| {
            let env = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_bytes))).build_arc().unwrap();
            Arc::new(TaskContext::default().with_runtime(env))
        };

        let fits = HotLegPooledExec::new(inner.clone(), 512);
        let ctx_fits = ctx(1024);
        fits.execute(0, ctx_fits.clone()).unwrap();
        fits.execute(1, ctx_fits.clone()).unwrap();
        assert_eq!(ctx_fits.memory_pool().reserved(), 512, "one charge for the whole leg, not per partition");

        let oversize = HotLegPooledExec::new(inner, 2048);
        let err = match oversize.execute(0, ctx(1024)) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("oversized leg must fail its query"),
        };
        assert!(err.contains("Resources exhausted"), "got: {err}");
    }
}
