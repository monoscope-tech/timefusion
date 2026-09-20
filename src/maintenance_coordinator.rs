//! Durable, byte-bounded work units shared by background maintenance.
//!
//! Work is journaled before it can be selected, and a worker only receives a
//! unit whose decoded-byte reservation fits the configured ceiling.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::support::lock;

pub const NORMAL_SLICE_MICROS: i64 = 10 * 60 * 1_000_000;
pub const DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
/// Widths `coarsen_sealed_slices` fuses sealed units to, widest first. Each
/// divides the one above, so an aligned unit at any width sits inside exactly
/// one bucket at every coarser width.
pub const COARSEN_WIDTHS: [i64; 3] = [DAY_MICROS, 6 * 60 * 60 * 1_000_000, 60 * 60 * 1_000_000];
/// What one `coarsen_sealed_slices` pass actually did, per stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CoarsenReport {
    /// Units dropped because a wider live unit already covers them.
    pub subsumed: usize,
    /// Units replaced by a wider fused unit.
    pub fused: usize,
    /// Sealed units eligible to fuse, before any group was rejected.
    pub candidates: usize,
    /// Candidates whose bucket a Running / equal-or-wider / superseded unit held.
    pub blocked: usize,
    /// Candidates in a group whose priced estimate exceeded MAX_DECODED_BYTES.
    pub over_budget: usize,
    /// Buckets that fit ONLY because members sharing a file set were charged once.
    pub priced_by_footprint: usize,
}

impl CoarsenReport {
    pub const fn total(self) -> usize {
        self.subsumed + self.fused
    }
}

/// Is this operation fully re-derivable from a storage scan, and therefore not
/// worth persisting? Repair is deliberately NOT here: its units stage output
/// before committing, so the durable record is what a resume works against.
pub const fn is_derived_operation(operation: Operation) -> bool {
    matches!(operation, Operation::HotPacking | Operation::SealedConsolidation)
}

/// Widths a unit can be SUBSUMED at, finest first — the mint width plus every
/// fusion width. Each divides the next.
pub const SUBSUME_WIDTHS: [i64; 4] = [NORMAL_SLICE_MICROS, 60 * 60 * 1_000_000, 6 * 60 * 60 * 1_000_000, DAY_MICROS];
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const DERIVED_SLICE_MICROS: i64 = 60 * 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;

/// Frontier lag above which `claim_next` stops reserving a share for sealed
/// work. One `NORMAL_SLICE_MICROS` — a frontier a whole slice behind is not
/// keeping up.
pub const FRONTIER_LAG_BUDGET_SECS: u64 = 600;

/// The longest per-unit idle window any operation gets. `COORDINATOR_LOOP_TIMEOUT`
/// is derived from this so the outer guard cannot become the real deadline.
pub const MAX_OPERATION_DEADLINE_SECS: u64 = operation_deadline_secs(Operation::Repair);

/// Per-operation IDLE window (fires only after this long with no rows written);
/// also bounds retry backoff so oversized units cannot monopolize a worker.
/// Repair gets an hour because its blocking `ORDER BY` emits no row until the
/// whole input is downloaded, decoded and spilled.
pub const fn operation_deadline_secs(operation: Operation) -> u64 {
    match operation {
        Operation::Repair => 60 * 60,
        Operation::Dedup | Operation::HotPacking | Operation::SealedConsolidation | Operation::BaseRollup | Operation::DerivedRollup => 15 * 60,
    }
}

/// Whether a failure means "this did not fit" rather than "this went wrong".
/// Matched on the message, not the type: these errors arrive type-erased across
/// the delta-rs and `anyhow` boundaries.
pub fn is_capacity_failure(message: &str) -> bool {
    message.contains("Resources exhausted") || message.contains("Not enough memory to continue external sort") || message.contains("resource_admission")
}

/// Whether a failure is a DETERMINISTIC PLAN error — the SQL could not be built
/// at all, so every retry and every CHILD of a bisection fails identically.
/// Opposite verdict to [`is_capacity_failure`]: splitting cannot help, only
/// multiply the units failing.
pub fn is_schema_failure(message: &str) -> bool {
    message.contains("Schema error") || message.contains("SchemaError") || message.contains("No field named")
}
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const INVALIDATION_DEADLINE_BUCKET_MICROS: i64 = 30 * 1_000_000;
pub const LIVE_FRONTIER_WINDOW_MICROS: i64 = DAY_MICROS;
const PRIORITY_BUCKET_MICROS: i64 = 60 * 1_000_000;
/// File-count band for hygiene benefit ranking. Coarse ON PURPOSE: `claim_next`
/// matches the winning rank tuple EXACTLY, so a continuous key would make one
/// cell the sole winner of every claim and defeat the per-project rotation in
/// `fair_cursors`.
const BENEFIT_BUCKET_FILES: u32 = 32;
pub const TAG_SOURCE: &str = "timefusion.source";
pub const TAG_PROJECT: &str = "timefusion.project";
pub const TAG_SLICE_START: &str = "timefusion.slice_start_micros";
pub const TAG_SLICE_END: &str = "timefusion.slice_end_micros";
pub const TAG_SOURCE_FINGERPRINT: &str = "timefusion.source_fingerprint";
/// The INPUT FILE SET this cell was aggregated from, deletion vectors included —
/// the no-op-rebuild proof. Unlike [`TAG_SOURCE_FINGERPRINT`], which hashes
/// paths alone, it sees a deletion vector superseding an `Add` under the same
/// path. Absent on older cells, which declines the skip.
pub const TAG_CONTENT_FINGERPRINT: &str = "timefusion.content_fingerprint";
/// How many rows the SOURCE DATE PARTITION held when this slice was built.
/// Read coverage is refused unless every slice covering a date still agrees
/// with the partition's present count. A row count, not a fingerprint, because
/// row counts survive compaction.
pub const TAG_SOURCE_ROWS: &str = "timefusion.source_rows";

/// The same count taken BELOW the slice's own end, so rows arriving past the
/// build's bound cannot perturb it.
///
/// `TAG_SOURCE_ROWS` is the whole partition's `num_records`, which any ingest
/// anywhere in the day moves — and 96.8% of measured rollup staleness is exactly
/// that (`rollup_stale_grew` 3,769,781 vs `rollup_stale_shrank` 124,695 over 12h).
/// `partition_stats_bounded` excludes any file whose `max_ts` reaches the bound, so
/// a file written past it is absent from BOTH this witness and the live count and
/// the two still agree.
///
/// WRITTEN ONLY. Nothing reads it yet: it exists so the data accrues before the
/// read side is flipped, and so a slice built now is verifiable under the new rule
/// when it is. A witness is only ever comparable to one recorded the same way, so
/// this is a separate tag rather than a redefinition of the old one.
pub const TAG_SOURCE_ROWS_BELOW: &str = "timefusion.source_rows_below";
pub const TAG_GENERATION: &str = "timefusion.generation";
/// Which declared measures this slice's files actually MATERIALIZED, comma
/// separated — NOT what the spec declares. A measure added after a slice was
/// written null-fills on scan and merges skip the nulls, so the read path
/// refuses a cell that cannot prove the measure a query needs.
pub const TAG_MEASURES: &str = "timefusion.measures";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_COMPACT_BYTES: u64 = 64 * 1024 * 1024;
static THROUGHPUT_SAMPLE: std::sync::OnceLock<Mutex<(i64, u64)>> = std::sync::OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize, strum::EnumCount, strum::IntoStaticStr)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Dedup,
    BaseRollup,
    DerivedRollup,
    HotPacking,
    SealedConsolidation,
    Repair,
}

/// The operation mix a maintenance worker rotates through, shared by the server
/// loop and the journal-replay simulator so the two cannot drift apart.
/// BALANCED interleaves dependent publication with dedup; COVERAGE_SHORT gives
/// the rollup chain the slots while contiguity is below goal. Every operation
/// must keep at least one slot.
pub const CYCLE_BALANCED: [Operation; 10] = [
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::Repair,
];
pub const CYCLE_COVERAGE_SHORT: [Operation; 10] = [
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::BaseRollup,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Repair,
];

pub fn operation_cycle(coverage_short: bool) -> &'static [Operation; 10] {
    if coverage_short { &CYCLE_COVERAGE_SHORT } else { &CYCLE_BALANCED }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TimeSlice {
    pub start_micros: i64,
    pub end_micros: i64,
}

impl TimeSlice {
    pub fn new(start_micros: i64, end_micros: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(start_micros < end_micros, "maintenance slice must be non-empty");
        Ok(Self { start_micros, end_micros })
    }

    pub const fn width(self) -> i64 {
        self.end_micros - self.start_micros
    }

    /// Half-open intersection.
    pub const fn overlaps(self, start_micros: i64, end_micros: i64) -> bool {
        end_micros > self.start_micros && start_micros < self.end_micros
    }

    pub fn normal_units(start_micros: i64, end_micros: i64) -> anyhow::Result<Vec<Self>> {
        Self::fixed_units(start_micros, end_micros, NORMAL_SLICE_MICROS)
    }

    fn fixed_units(start_micros: i64, end_micros: i64, width_micros: i64) -> anyhow::Result<Vec<Self>> {
        let whole = Self::new(start_micros, end_micros)?;
        anyhow::ensure!(width_micros > 0, "maintenance slice width must be positive");
        Ok(std::iter::successors(Some(whole.start_micros), |start| Some(start.saturating_add(width_micros)).filter(|next| *next < whole.end_micros))
            .map(|start| Self { start_micros: start, end_micros: start.saturating_add(width_micros).min(whole.end_micros) })
            .collect())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TaskKey {
    pub physical_table: String,
    pub source: String,
    pub project_id: String,
    pub slice: TimeSlice,
    pub operation: Operation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Retry,
    Complete,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MaintenanceTask {
    pub key: TaskKey,
    pub state: TaskState,
    pub deadline_micros: i64,
    pub estimated_decoded_bytes: u64,
    pub hash_shard: u32,
    pub hash_shards: u32,
    pub attempts: u32,
    pub created_unix_ms: u64,
    #[serde(default)]
    pub retry_reason: Option<String>,
    #[serde(default)]
    pub publication: Option<Publication>,
    /// The base tier this derived unit aggregates is ALREADY PRESENT, proven
    /// from real rollup coverage. Only ever set from positive coverage evidence,
    /// which is why it overrides rather than supplements the journal's
    /// requirement of COMPLETE `BaseRollup` tasks (which a historical day no
    /// longer has, leaving the unit unclaimable forever).
    #[serde(default)]
    pub base_tier_present: bool,
    /// What this unit's slice actually READS, measured when its estimate was taken.
    #[serde(default)]
    pub input: Option<InputFootprint>,
    /// What the parent MEASURED when it split into this unit, so the next
    /// preflight can tell whether halving the width actually bought anything.
    /// Children are priced by TIME SHARE, so a split always "fits" on paper;
    /// without this feedback a lineage bisects all the way to `MIN_SLICE_MICROS`.
    #[serde(default)]
    pub parent_measured_bytes: Option<u64>,
    /// Byte estimate from this claim's input preflight, before time-share modelling.
    #[serde(default)]
    pub preflight_decoded_bytes: Option<u64>,
    /// Scheduling weight inherited from the backfill unit this task was split
    /// out of; `None` means "weigh me by my own width". Sealed ordering ranks
    /// wide units first because width proxies backfill provenance, and splitting
    /// would otherwise break that proxy.
    #[serde(default)]
    pub backfill_priority_micros: Option<i64>,
}

impl MaintenanceTask {
    /// Width for SCHEDULING only, never for planning or execution.
    pub fn scheduling_width(&self) -> i64 {
        self.backfill_priority_micros.unwrap_or_else(|| self.key.slice.width())
    }

    /// A freshly minted Pending unit with no history.
    fn pending(key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64) -> Self {
        Self {
            key,
            state: TaskState::Pending,
            deadline_micros,
            estimated_decoded_bytes,
            hash_shard: 0,
            hash_shards: 1,
            attempts: 0,
            created_unix_ms,
            retry_reason: None,
            publication: None,
            base_tier_present: false,
            input: None,
            parent_measured_bytes: None,
            preflight_decoded_bytes: None,
            backfill_priority_micros: None,
        }
    }
}

/// The file set behind a unit's byte estimate, so a fusion group can be priced
/// by DISTINCT file set instead of by summing prorated members. Prorating is
/// right for ONE unit and wrong the moment `coarsen_to_width` sums siblings:
/// pruning is row-group granular, so children reading the same files are one
/// scan. Partial overlap counts twice — the safe direction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct InputFootprint {
    /// Hash of the live file paths the slice overlaps.
    pub fp: u64,
    /// Decoded, projected bytes of that whole set — one scan, unprorated.
    pub whole_file_bytes: u64,
    /// How many files. For file hygiene this IS the benefit: a consolidation
    /// removes them and leaves one. Zero means "unknown", which orders last.
    #[serde(default)]
    pub files: u32,
}

impl InputFootprint {
    /// Fingerprint a selected file set. Order-independent, since a snapshot's
    /// file order is not stable.
    ///
    /// FROZEN HASH: persisted in the task journal, so changing it makes every
    /// in-flight unit's footprint stop matching its own journal entry.
    pub fn new<I: IntoIterator<Item = S>, S: AsRef<str>>(paths: I, whole_file_bytes: u64) -> Self {
        use std::hash::{Hash, Hasher};
        let (fp, files) = paths.into_iter().fold((0u64, 0u32), |(acc, count), path| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            path.as_ref().hash(&mut hasher);
            (acc ^ hasher.finish(), count.saturating_add(1))
        });
        Self { fp, whole_file_bytes, files }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Publication {
    pub source_fingerprint: u64,
    pub generation: String,
    pub rows: u64,
    /// The source DATE partition's `num_records` sum when this slice was built,
    /// mirroring [`TAG_SOURCE_ROWS`]. `None` cannot be verified by the read
    /// path, so such slices read raw until the coordinator republishes.
    #[serde(default)]
    pub source_rows: Option<u64>,
    /// Mirrors [`TAG_SOURCE_ROWS_BELOW`]: the same count over only the files
    /// wholly below the slice's end. `default` so journals written before the
    /// field deserialize as `None` and fall back to the whole-partition compare.
    #[serde(default)]
    pub source_rows_below: Option<u64>,
}

/// What one fusion bucket would cost to scan, accumulated member by member.
/// Members naming the same [`InputFootprint`] are charged ONCE — they re-read
/// the same row groups; members with no footprint keep the summed price.
#[derive(Default)]
struct GroupPrice {
    /// Distinct footprints, each charged its unprorated whole-file cost.
    distinct: HashMap<u64, InputFootprint>,
    /// Members with nothing better to say than their prorated share.
    unpriced_bytes: u64,
    unpriced_members: usize,
    /// Every member's own estimate, summed — reported so a pass can tell whether
    /// footprint pricing changed anything.
    summed_bytes: u64,
    /// How many units this bucket holds, which is what `over_budget` reports.
    members: usize,
    /// The OLDEST member's creation time; the fused unit inherits its members'
    /// work and must inherit their age with it. `Option`, not a bare `u64`: a
    /// `Default` of 0 would min-fold to the epoch and make every fused unit
    /// permanently the oldest thing in the queue.
    oldest: Option<u64>,
}

impl GroupPrice {
    fn add(&mut self, task: &MaintenanceTask) {
        self.summed_bytes = self.summed_bytes.saturating_add(task.estimated_decoded_bytes);
        self.members += 1;
        self.oldest = Some(self.oldest.unwrap_or(u64::MAX).min(task.created_unix_ms));
        if let Some(input) = task.input {
            self.distinct.insert(input.fp, input);
        } else {
            self.unpriced_bytes = self.unpriced_bytes.saturating_add(task.estimated_decoded_bytes);
            self.unpriced_members += 1;
        }
    }

    /// Each distinct footprint charged its unprorated whole-file cost, once.
    fn priced(&self) -> u64 {
        self.distinct.values().fold(0, |total, input| total.saturating_add(input.whole_file_bytes))
    }

    fn bytes(&self) -> u64 {
        self.priced().saturating_add(self.unpriced_bytes)
    }

    /// Bound the price by what the partition can actually decode to. Applied to
    /// `unpriced_bytes` only — that is the term that double-counts, since
    /// footprint-priced members are already charged once each.
    fn cap_at(&mut self, ceiling: u64) {
        self.unpriced_bytes = self.unpriced_bytes.min(ceiling.saturating_sub(self.priced().min(ceiling)));
    }

    fn unanimous_input(&self) -> Option<InputFootprint> {
        use itertools::Itertools;
        self.distinct.values().exactly_one().ok().copied().filter(|_| self.unpriced_members == 0)
    }
}

/// A child must shed at least this much of what its parent measured for the
/// next bisection to be worth minting units for. Bisection halves the WIDTH, so
/// the model expects ~50%; anything above this is the row-group floor.
const SPLIT_MUST_SHED_NUMERATOR: u64 = 3;
const SPLIT_MUST_SHED_DENOMINATOR: u64 = 4;

/// Whether halving the width bought enough to justify halving it again. `None`
/// (no parent evidence) always splits. Two-sided on purpose: a child preflight
/// LARGER than its parent also splits, establishing a new baseline.
fn split_sheds_enough(parent_measured_bytes: Option<u64>, observed_bytes: u64) -> bool {
    split_sheds_enough_at(parent_measured_bytes, observed_bytes, SPLIT_MUST_SHED_NUMERATOR, SPLIT_MUST_SHED_DENOMINATOR)
}

/// The shed test at an arbitrary ratio, so the simulator's threshold sweep runs
/// this function rather than a copy of it.
pub fn split_sheds_enough_at(parent_measured_bytes: Option<u64>, observed_bytes: u64, numerator: u64, denominator: u64) -> bool {
    let Some(parent) = parent_measured_bytes else { return true };
    observed_bytes > parent || observed_bytes.saturating_mul(denominator) < parent.saturating_mul(numerator)
}

/// Split a unit that does not fit, **one level per call**. A unit already at the
/// bisection floor is divided by a stable hash of the complete dedup key;
/// callers must apply `hash(key) % hash_shards == hash_shard` before
/// deduplication. One level, not a subtree, so every width gets re-measured.
pub fn byte_bounded_units(task: &MaintenanceTask, observed_or_estimated_bytes: u64) -> Vec<MaintenanceTask> {
    if observed_or_estimated_bytes <= MAX_DECODED_BYTES {
        return vec![MaintenanceTask { estimated_decoded_bytes: observed_or_estimated_bytes, ..task.clone() }];
    }
    if let Some(children) = bisect_time_unit(task, observed_or_estimated_bytes) {
        return children.into();
    }

    let shards_u64 = observed_or_estimated_bytes.div_ceil(MAX_DECODED_BYTES).max(2);
    let shards = u32::try_from(shards_u64).unwrap_or(u32::MAX);
    let per_shard = observed_or_estimated_bytes.div_ceil(u64::from(shards));
    (0..shards).map(|hash_shard| MaintenanceTask { hash_shard, hash_shards: shards, estimated_decoded_bytes: per_shard, ..task.clone() }).collect()
}

/// Bisect time independently of whether a byte estimate exceeds the budget.
fn bisect_time_unit(task: &MaintenanceTask, observed_or_estimated_bytes: u64) -> Option<[MaintenanceTask; 2]> {
    // Bisection stops at the width where a slice stops shedding FILES. A dedup
    // unit's cost is its whole PARTITION, so halving time below a slice sheds
    // nothing; below the floor, shard by KEY instead.
    let bisect_floor = if task.key.operation == Operation::Dedup { NORMAL_SLICE_MICROS } else { MIN_SLICE_MICROS };
    let (start, end, width) = (task.key.slice.start_micros, task.key.slice.end_micros, task.key.slice.width());
    let midpoint = (start.saturating_add(width / 2) / MIN_SLICE_MICROS) * MIN_SLICE_MICROS;
    if width <= bisect_floor || midpoint <= start || midpoint >= end {
        return None;
    }
    let left_bytes = ((u128::from(observed_or_estimated_bytes) * u128::try_from(midpoint - start).unwrap_or(0)) / u128::try_from(width).unwrap_or(1)) as u64;
    let child = |slice, estimated_decoded_bytes| MaintenanceTask { key: TaskKey { slice, ..task.key.clone() }, estimated_decoded_bytes, ..task.clone() };
    Some([
        child(TimeSlice { start_micros: start, end_micros: midpoint }, left_bytes),
        child(TimeSlice { start_micros: midpoint, end_micros: end }, observed_or_estimated_bytes.saturating_sub(left_bytes)),
    ])
}

#[derive(Clone, Copy)]
enum SplitTrigger {
    Preflight(u64),
    RepeatedFailure,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Snapshot {
    version: u32,
    tasks: Vec<MaintenanceTask>,
    source_cursors: BTreeMap<String, u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum JournalRecord {
    // Boxed for variant-size parity: a task is ~330 bytes against a tombstone's
    // ~60, and every record in a checkpoint stream pays the larger. `Box<T>`
    // serializes transparently, so the wire format is unchanged.
    Task(Box<MaintenanceTask>),
    SourceCursor {
        source: String,
        delta_version: u64,
    },
    /// This task no longer exists. Without a tombstone the WAL is upsert-only,
    /// so a removal would be undone by the next restart.
    Removed(TaskKey),
}

/// Crash-safe task journal. `checkpoint` uses fsync + atomic rename, so a failed
/// completion checkpoint causes redundant work, never missing work.
#[derive(Debug)]
pub struct TaskJournal {
    path: PathBuf,
    wal_path: PathBuf,
    snapshot: Snapshot,
    /// Stable indices into `snapshot.tasks`. Tasks are never removed, so point
    /// updates and WAL replay stay O(1) even with a production-sized backlog.
    task_indices: HashMap<TaskKey, usize>,
    /// Keys that MIGHT be claimable, so `claim_next` does not walk the dead.
    ///
    /// Prod 2026-09-20 held 93,326 Complete tasks against 857 Pending, and
    /// `claim_next` filtered all 94,349 of them on every pass — three passes, 219
    /// claims/sec, 4.7 ms each, the journal mutex busy 41% of wall-clock. The
    /// completed ones are retained on purpose (`dependencies_complete` proves base
    /// coverage from them) but they have no business in the CLAIM scan.
    ///
    /// Deliberately PERMISSIVE: an entry that is no longer claimable is filtered
    /// out as before and costs one lookup, whereas a MISSING entry would silently
    /// strand a unit forever. So every transition into a claimable state inserts,
    /// and removal is lazy. `reconcile_claimable` re-derives the set and counts
    /// divergence, so a missed insert is loud rather than invisible.
    claimable: std::collections::BTreeSet<TaskKey>,
    dirty_tasks: HashSet<TaskKey>,
    /// Keys removed since the last write, pending a `Removed` tombstone.
    removed_tasks: HashSet<TaskKey>,
    dirty_cursors: HashSet<String>,
    /// When the gauges were last recomputed, so `checkpoint` does not rescan the
    /// whole task list on every claim and completion.
    stats_published_at: Option<std::time::Instant>,
    fair_cursors: HashMap<Operation, String>,
    /// `(source, project_id, date)` whose BASE tier is already built, read from
    /// real rollup coverage. `dependencies_complete` consults this instead of
    /// requiring COMPLETE `BaseRollup` TASKS, which a historical day does not
    /// have. Keyed by DAY so it cannot miss a task whatever slice it covers.
    /// Runtime only, never journalled — a restart costs one planner pass.
    base_tier_ready: HashSet<(String, String, String)>,
    /// `(source, project_id, physical_table, date)` where the tier is MISSING.
    /// `scheduling_class` ranks a hole ahead of a re-derive; otherwise sealed
    /// rollup work is strictly newest-first and the claim never walks back far
    /// enough to reach an old hole. Runtime only.
    tier_holes: HashSet<(String, String, String, String)>,
    /// `(source, project, tier table, date)` for partitions still holding tier
    /// files with NO identity tags — ranked like `tier_holes`, because such a
    /// file cannot be certified or retired until something republishes the
    /// partition. Kept separate because the two are published by different
    /// passes and a wholesale replace by either would erase the other's evidence.
    untagged_cells: HashSet<(String, String, String, String)>,
    /// Rotates so a fixed share of claims is reserved for sealed work. Runtime only.
    claim_tick: u64,
    /// Last observed `eligible_watermark_lag_seconds`; read by `claim_next` to
    /// decide whether the sealed reservation can still be afforded. Atomic
    /// because `publish_statistics` takes `&self`; runtime only.
    frontier_lag_secs: std::sync::atomic::AtomicU64,
    /// Boundary micros of COMPLETED Dedup slices, per project then source, so
    /// `rank` can prefer the pending slice that EXTENDS a completed run.
    /// Runtime only. Nested maps (not a tuple key) so the lookup in `rank`
    /// borrows `&str`.
    dedup_complete_edges: HashMap<String, HashMap<String, HashSet<i64>>>,
}

/// Ensures a claimed task cannot remain stuck in `Running` when its worker
/// returns early through an unexpected error. Expected retry, split, and
/// completion paths change the state before this guard is dropped.
pub struct TaskLease {
    journal: Arc<Mutex<TaskJournal>>,
    key: TaskKey,
    /// Teardown signal. The unstarted release is scoped to SHUTDOWN and nothing
    /// else: a unit that errors through `?` also reaches `Drop` with no recorded
    /// failure, and refunding its attempt would retry it with no backoff at all.
    shutdown: tokio_util::sync::CancellationToken,
    started_micros: i64,
    /// Why the unit is about to fail, when the failing path knows. Errors leave
    /// a run function through `?` and reach [`Drop`] carrying nothing, so
    /// without this `abandon_running` would treat a deterministic plan error
    /// like a timeout and bisect it. Mutex, not `RefCell`, to stay `Send`.
    failure: Mutex<Option<String>>,
}

impl TaskLease {
    pub fn new(journal: Arc<Mutex<TaskJournal>>, key: TaskKey, shutdown: tokio_util::sync::CancellationToken) -> Self {
        Self { journal, key, shutdown, started_micros: crate::support::now_micros(), failure: Mutex::new(None) }
    }

    /// Annotate the error on its way out: `lease.note_failure(error)?`.
    pub fn note_failure<E: std::fmt::Display>(&self, error: E) -> E {
        *lock(&self.failure) = Some(error.to_string());
        error
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        let mut journal = lock(&self.journal);
        // Emitted from the RAII lease rather than `complete()` so no early
        // return can skip it. `Running` here means the unit died without
        // recording anything and is abandoned below.
        let outcome = journal.state(&self.key);
        let ran_micros = crate::support::now_micros().saturating_sub(self.started_micros);
        tracing::info!(
            operation = ?self.key.operation, table = %self.key.physical_table, project_id = %self.key.project_id,
            slice_start = self.key.slice.start_micros, slice_end = self.key.slice.end_micros,
            outcome = ?outcome, ran_secs = ran_micros / 1_000_000,
            // What the unit READ, not what it changed.
            input_files = journal.input_files(&self.key),
            event = "maintenance_task_finished"
        );
        if outcome == Some(TaskState::Running) {
            let failure = self.failure.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            // A unit that never started is not a worker failure. `mark_running`
            // already charged it an attempt and `abandon_running` would charge a
            // `worker_error` and a backoff on top — then SPLIT it at two attempts
            // and quarantine it at `QUARANTINE_ATTEMPTS`. The database restarts a
            // couple of times a day, so without this a unit merely UNLUCKY with
            // deploys is punished as if it had repeatedly failed.
            // SHUTDOWN ONLY, and only with nothing written. A unit that errors
            // through `?` reaches here with no recorded failure too, so without the
            // teardown check this would refund its attempt and re-run it with no
            // backoff — a hot loop. `None` progress means we are outside a unit
            // scope, read as "it ran": never guess a unit into a free retry.
            let did_no_work = crate::database::maintain::current_unit_progress() == Some(0);
            if failure.is_none() && did_no_work && self.shutdown.is_cancelled() {
                journal.release_unstarted(&self.key);
                if let Err(error) = journal.checkpoint() {
                    tracing::error!(error = %error, task = ?self.key, "failed to checkpoint unstarted maintenance task release");
                }
                return;
            }
            journal.abandon_running(&self.key, crate::support::now_micros(), failure.as_deref());
            if let Err(error) = journal.checkpoint() {
                tracing::error!(error = %error, task = ?self.key, "failed to checkpoint maintenance task lease recovery");
            }
        }
    }
}

pub struct Invalidation<'a> {
    pub source_table: &'a str,
    pub rollup_table: &'a str,
    pub source: &'a str,
    pub project_id: &'a str,
    pub start_micros: i64,
    pub end_micros: i64,
    pub observed_at_micros: i64,
    pub derived: bool,
    /// False ONLY when every commit behind this invalidation is a self-authored
    /// DV-dedup wave: such a commit adds no rows, so re-minting Dedup from it
    /// would upsert already-Complete slices back to Pending forever. Every other
    /// caller passes true — fail toward minting.
    pub mint_dedup: bool,
    /// False on the SAME condition as `mint_dedup=false`: a DV-dedup wave masks
    /// losers in place, so a rebuild would produce byte-identical aggregates.
    pub mint_rollup: bool,
}

/// Insert or replace a task by key, keeping `indices` in step with `tasks`.
/// A free function so WAL replay in [`TaskJournal::load`], which runs before
/// there is a `Self`, shares one definition with every other insert site.
fn insert_task(tasks: &mut Vec<MaintenanceTask>, indices: &mut HashMap<TaskKey, usize>, task: MaintenanceTask) {
    match indices.get(&task.key).copied() {
        Some(index) => tasks[index] = task,
        None => {
            indices.insert(task.key.clone(), tasks.len());
            tasks.push(task);
        }
    }
}

impl TaskJournal {
    // Markers are versioned: bump the suffix to re-run a one-shot migration.
    const BOOTSTRAP_BACKLOG_MIGRATION: &'static str = "__maintenance_bootstrap_backlog_v3";
    const BOOTSTRAP_BACKLOG_LIMIT: usize = 100_000;
    const COARSE_BACKFILL_MIGRATION: &'static str = "__maintenance_coarse_backfill_v2";
    const STALE_ESTIMATE_MIGRATION: &'static str = "__maintenance_stale_estimate_v2";
    /// Bump to re-run the orphaned-coverage repair after a future spec edit. The
    /// cursor is persisted BEFORE the caller does the work so a crash cannot
    /// loop; bumping is the intended recovery, since re-enqueueing is idempotent.
    const ORPHAN_REPAIR_MIGRATION: &'static str = "__maintenance_orphan_repair_v2";

    /// Re-enqueue of a measured list of damaged (project, date) cells. Its
    /// cursor counts a CONSUMED PREFIX — a one-shot cursor would force the whole
    /// list into one planner pass, which truncates it and drops the tail
    /// permanently.
    pub const DAMAGE_REPAIR_MIGRATION: &'static str = "__maintenance_damage_repair_v2";
    /// See [`TaskJournal::reset_repair_attempts`].
    const REPAIR_SINGLE_PASS_MIGRATION: &'static str = "__maintenance_repair_single_pass_v1";

    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = crate::write::wal::meta_path(data_dir, "maintenance_tasks.json");
        let wal_path = crate::write::wal::meta_path(data_dir, "maintenance_tasks.wal");
        let mut snapshot = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes)?,
            Err(error) if error.kind() == ErrorKind::NotFound => Snapshot { version: JOURNAL_VERSION, ..Snapshot::default() },
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(snapshot.version == JOURNAL_VERSION, "unsupported maintenance task journal version {}", snapshot.version);
        // Every record ends in a newline. Ignore only a torn final record; all
        // earlier records were fsynced before the caller acknowledged the
        // invalidation or publication that produced them.
        let mut task_indices = snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect::<HashMap<_, _>>();
        if let Ok(bytes) = fs::read(&wal_path) {
            for line in bytes.split_inclusive(|byte| *byte == b'\n').take_while(|line| line.ends_with(b"\n")) {
                let record = serde_json::from_slice::<JournalRecord>(&line[..line.len() - 1])?;
                match record {
                    JournalRecord::Task(task) => insert_task(&mut snapshot.tasks, &mut task_indices, *task),
                    JournalRecord::SourceCursor { source, delta_version } => {
                        snapshot.source_cursors.entry(source).and_modify(|cursor| *cursor = (*cursor).max(delta_version)).or_insert(delta_version);
                    }
                    // `swap_remove` keeps this O(1); snapshot ORDER carries no
                    // meaning, since every consumer sorts or filters.
                    JournalRecord::Removed(key) => {
                        if let Some(index) = task_indices.remove(&key) {
                            snapshot.tasks.swap_remove(index);
                            if let Some(moved) = snapshot.tasks.get(index) {
                                task_indices.insert(moved.key.clone(), index);
                            }
                        }
                    }
                }
            }
        }
        let mut journal = Self {
            path,
            wal_path,
            snapshot,
            task_indices,
            dirty_tasks: HashSet::new(),
            removed_tasks: HashSet::new(),
            dirty_cursors: HashSet::new(),
            stats_published_at: None,
            fair_cursors: HashMap::new(),
            base_tier_ready: HashSet::new(),
            tier_holes: HashSet::new(),
            untagged_cells: HashSet::new(),
            claimable: std::collections::BTreeSet::new(),
            claim_tick: 0,
            frontier_lag_secs: std::sync::atomic::AtomicU64::new(0),
            dedup_complete_edges: HashMap::new(),
        };
        journal.rebuild_dedup_edges();
        journal.rebuild_claimable();
        Ok(journal)
    }

    /// Has this one-shot migration already consumed its cursor?
    fn migration_done(&self, marker: &str) -> bool {
        self.snapshot.source_cursors.get(marker).copied().unwrap_or_default() >= 1
    }

    /// Consume a one-shot migration's cursor.
    fn mark_migration_done(&mut self, marker: &str) {
        self.snapshot.source_cursors.insert(marker.to_owned(), 1);
        self.dirty_cursors.insert(marker.to_owned());
    }

    /// Run a one-shot migration whose cursor is unspent, consuming the cursor
    /// only after `migrate` has run. `None` from `migrate` leaves it UNSPENT.
    fn run_once(&mut self, marker: &str, migrate: impl FnOnce(&mut Self) -> Option<usize>) -> Option<usize> {
        if self.migration_done(marker) {
            return None;
        }
        let done = migrate(self)?;
        self.mark_migration_done(marker);
        Some(done)
    }

    /// Apply `edit` to every task `select` accepts, marking each dirty and
    /// returning how many changed.
    fn edit_tasks(&mut self, select: impl Fn(&MaintenanceTask) -> bool, mut edit: impl FnMut(&mut MaintenanceTask)) -> usize {
        let dirty = &mut self.dirty_tasks;
        self.snapshot.tasks.iter_mut().filter(|task| select(task)).fold(0usize, |changed, task| {
            edit(task);
            dirty.insert(task.key.clone());
            changed + 1
        })
    }

    /// Rebuild the completed-dedup boundary index from the snapshot. Called
    /// once at load; `note_dedup_edges` maintains it incrementally after that.
    fn rebuild_dedup_edges(&mut self) {
        self.dedup_complete_edges.clear();
        let edges = &mut self.dedup_complete_edges;
        self.snapshot
            .tasks
            .iter()
            .filter(|task| task.key.operation == Operation::Dedup && task.state == TaskState::Complete)
            .for_each(|task| Self::insert_dedup_edges(edges, &task.key));
    }

    /// Record a completed Dedup slice's boundaries so `rank` can prefer the
    /// pending slice that EXTENDS a completed run. Insert-only and advisory: a
    /// stale edge is only an ordering preference, never a correctness input.
    fn note_dedup_edges(&mut self, key: &TaskKey) {
        if key.operation == Operation::Dedup {
            Self::insert_dedup_edges(&mut self.dedup_complete_edges, key);
        }
    }

    fn insert_dedup_edges(map: &mut HashMap<String, HashMap<String, HashSet<i64>>>, key: &TaskKey) {
        let edges = map.entry(key.project_id.clone()).or_default().entry(key.source.clone()).or_default();
        edges.extend([key.slice.start_micros, key.slice.end_micros]);
    }

    /// Supersede unpublished sub-hour `DerivedRollup` fragments and replace them
    /// with one aligned hour task. Completed publications are not rewritten.
    pub fn migrate_derived_slices(&mut self) -> usize {
        let mut replacements: HashMap<TaskKey, (i64, u64, u64)> = HashMap::new();
        let migrated = self.edit_tasks(
            |task| {
                // A SPLIT CHILD is not a legacy fragment: collapsing one back to its
                // hour erases the bisection ladder and re-enqueues the parent key,
                // which `enqueue_inner` resurrects to Pending — an endless loop.
                // `parent_measured_bytes` is set only by `split_time_task`, so it is
                // the exact discriminator.
                //
                // NARROWER than an hour, not merely "not an hour": the replacement
                // key below is the single hour containing the slice START, so `!=`
                // would collapse a day-wide unit to hour 00 and drop the other 23.
                task.key.operation == Operation::DerivedRollup
                    && task.key.slice.width() < DERIVED_SLICE_MICROS
                    && task.parent_measured_bytes.is_none()
                    && !matches!(task.state, TaskState::Complete | TaskState::Superseded)
            },
            |task| {
                let start = task.key.slice.start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
                let key = TaskKey { slice: TimeSlice { start_micros: start, end_micros: start.saturating_add(DERIVED_SLICE_MICROS) }, ..task.key.clone() };
                replacements
                    .entry(key)
                    .and_modify(|(deadline, estimate, created)| {
                        *deadline = (*deadline).min(task.deadline_micros);
                        *estimate = estimate.saturating_add(task.estimated_decoded_bytes);
                        *created = (*created).min(task.created_unix_ms);
                    })
                    .or_insert((task.deadline_micros, task.estimated_decoded_bytes, task.created_unix_ms));
                task.state = TaskState::Superseded;
                task.retry_reason = Some("migrated_to_aligned_hour_slice".to_owned());
            },
        );
        for (key, (deadline, estimate, created)) in replacements {
            self.enqueue(key, deadline, estimate, created);
        }
        migrated
    }

    /// Remove the one-time global backlog produced by the original cursor
    /// bootstrap. Dropping unfinished entries is correctness-safe: those slices
    /// stay uncovered and reads fall back to raw data until a planner re-enqueues.
    pub fn migrate_bootstrap_backlog(&mut self) -> Option<usize> {
        self.migrate_bootstrap_backlog_with_limit(Self::BOOTSTRAP_BACKLOG_LIMIT)
    }

    fn migrate_bootstrap_backlog_with_limit(&mut self, limit: usize) -> Option<usize> {
        self.run_once(Self::BOOTSTRAP_BACKLOG_MIGRATION, |journal| {
            Some(if journal.snapshot.tasks.len() > limit { journal.retain_tasks(|task| task.state == TaskState::Complete) } else { 0 })
        })
    }

    /// One-shot: forget every stored byte estimate. Zero is what a freshly
    /// minted unit carries — the claim-time preflight computes the real estimate
    /// and splits if it must.
    pub fn clear_stale_estimates(&mut self) -> Option<usize> {
        self.run_once(Self::STALE_ESTIMATE_MIGRATION, |journal| {
            let cleared =
                journal.edit_tasks(|task| task.state != TaskState::Complete && task.estimated_decoded_bytes != 0, |task| task.estimated_decoded_bytes = 0);
            journal.dirty_tasks.clear();
            Some(cleared)
        })
    }

    /// One-shot: forget the attempt history of every Repair unit. `attempts >= 2`
    /// QUARANTINES a unit and floors its retry backoff at an hour, so stale
    /// history alone would stall the queue for a week.
    pub fn reset_repair_attempts(&mut self) -> Option<usize> {
        self.run_once(Self::REPAIR_SINGLE_PASS_MIGRATION, |journal| {
            let reset = journal.edit_tasks(
                |task| task.key.operation == Operation::Repair && task.state != TaskState::Complete,
                |task| {
                    task.attempts = 0;
                    task.retry_reason = None;
                    task.deadline_micros = 0;
                },
            );
            // A journal with no repair queue must not spend the one-shot cursor,
            // or a boot that merely preceded the queue consumes the migration.
            (reset > 0).then(|| {
                journal.dirty_tasks.clear();
                reset
            })
        })
    }

    /// Drop queued work for a rollup tier that is no longer DECLARED — otherwise
    /// tasks queued against a removed or renamed spec stay claimable forever.
    ///
    /// Conservative on purpose, since a false positive deletes live work: only
    /// `_rollup_` table names, only non-Complete tasks, and NOTHING at all when
    /// `declared` is empty (an unloaded registry must not retire the queue).
    pub fn retire_undeclared_tiers(&mut self, declared: &HashSet<String>) -> usize {
        if declared.is_empty() {
            return 0;
        }
        self.retain_tasks(|task| {
            !(task.key.physical_table.contains("_rollup_") && task.state != TaskState::Complete && !declared.contains(&task.key.physical_table))
        })
    }

    /// How much of `migration`'s ordered repair list `source` has CONSUMED.
    /// A prefix index survives restarts and lets every pass force only what it
    /// can fit. See [`Self::DAMAGE_REPAIR_MIGRATION`].
    pub fn repair_cursor(&self, migration: &str, source: &str) -> usize {
        usize::try_from(self.snapshot.source_cursors.get(&format!("{migration}:{source}")).copied().unwrap_or_default()).unwrap_or(usize::MAX)
    }

    /// Record that `source` has consumed `consumed` entries of `migration`'s list.
    /// Monotonic: callers must only ever move the cursor forward, and must call
    /// this AFTER the enqueue's own checkpoint or work can be lost.
    pub fn advance_repair_cursor(&mut self, migration: &str, source: &str, consumed: usize) -> anyhow::Result<()> {
        let key = format!("{migration}:{source}");
        let consumed = u64::try_from(consumed).unwrap_or(u64::MAX);
        if self.snapshot.source_cursors.get(&key).copied().unwrap_or_default() >= consumed {
            return Ok(());
        }
        self.snapshot.source_cursors.insert(key.clone(), consumed);
        self.dirty_cursors.insert(key);
        self.checkpoint()
    }

    /// Claim the one-shot orphaned-coverage repair; `false` if it already ran. The caller does the
    /// work; this owns only the once-ness, and marks the migration done IMMEDIATELY so a restart
    /// mid-repair cannot re-force every cell. Keyed PER SOURCE, or the first source processed
    /// consumes it for all. Returns `bool`, not a cursor: this repair has no prefix to resume from.
    pub fn repair_orphaned_coverage_once(&mut self, source: &str) -> bool {
        let key = format!("{}:{source}", Self::ORPHAN_REPAIR_MIGRATION);
        if self.migration_done(&key) {
            return false;
        }
        self.mark_migration_done(&key);
        // The claim is replayable from the log, so a failed checkpoint costs a redo, not the claim.
        if let Err(e) = self.checkpoint() {
            tracing::warn!(source, "orphan-repair claim not checkpointed; it will be replayed from the log: {e:#}");
        }
        true
    }

    pub fn migrate_fine_grained_backfill(&mut self, now_micros: i64) -> Option<usize> {
        self.run_once(Self::COARSE_BACKFILL_MIGRATION, |journal| {
            Some(journal.retain_tasks(|task| {
                task.state == TaskState::Complete
                    || !matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking)
                    || is_live_frontier(task.key.slice, now_micros)
                    // A day-sized unit is what replaces these; anything already that
                    // wide came from the coarse planner and must survive.
                    || task.key.slice.width() >= DAY_MICROS
            }))
        })
    }

    /// Collapse a sealed day's leftover ten-minute units into one day unit, as a
    /// cascade over `COARSEN_WIDTHS`: a span lands at the widest width whose
    /// estimate fits the decode budget.
    ///
    /// Anti-loop guard: a span already covered by a non-complete unit at least
    /// that wide is skipped, otherwise a split parent's children would fuse back
    /// into a unit that splits again forever. The guard is per-width.
    pub fn coarsen_sealed_slices(&mut self, now_micros: i64) -> usize {
        self.coarsen_sealed_slices_reporting(now_micros).total()
    }

    /// `coarsen_sealed_slices`, with the per-stage breakdown of why a pass was
    /// small (blocked vs over-budget vs few candidates).
    pub fn coarsen_sealed_slices_reporting(&mut self, now_micros: i64) -> CoarsenReport {
        self.coarsen_sealed_slices_capped(now_micros, &|_, _, _| None)
    }

    /// `coarsen_sealed_slices_reporting`, with a ceiling on what a partition can
    /// possibly decode to.
    ///
    /// The fit test sums children's WHOLE-FILE `estimated_decoded_bytes`, which
    /// double-count when children re-read one file set. `partition_bytes(project,
    /// source, date)` caps the fused estimate at what the partition actually
    /// holds; `None` keeps the plain summed behaviour, for callers without
    /// storage access.
    pub fn coarsen_sealed_slices_capped(&mut self, now_micros: i64, partition_bytes: &dyn Fn(&str, &str, &str) -> Option<u64>) -> CoarsenReport {
        // SUBSUME before fusing: fusion cannot touch a bucket a wider pending
        // unit already covers, so on its own it leaves exactly the redundancy it
        // exists to remove.
        let subsumed = self.subsume_covered_units(now_micros);
        let mut report = CoarsenReport { subsumed, ..Default::default() };
        for &width in COARSEN_WIDTHS.iter() {
            let stage = self.coarsen_to_width_reporting(width, now_micros, partition_bytes);
            report.fused += stage.fused;
            report.candidates += stage.candidates;
            report.blocked += stage.blocked;
            report.over_budget += stage.over_budget;
            report.priced_by_footprint += stage.priced_by_footprint;
        }
        report
    }

    /// Drop sealed units wholly covered by a WIDER non-complete unit for the
    /// same (table, source, project, operation) — a rollup or dedup unit rebuilds
    /// its entire slice, so the narrow units are the same work listed twice.
    ///
    /// Only NON-COMPLETE covering units subsume: a narrower unit inside a
    /// completed span is a later invalidation that must run.
    fn subsume_covered_units(&mut self, now_micros: i64) -> usize {
        type Group = (String, String, String, Operation);
        let group_of =
            |task: &MaintenanceTask| -> Group { (task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation) };
        // For each subsume width, the buckets wholly inside some non-complete
        // unit STRICTLY wider than it.
        let mut covered: [HashSet<(Group, i64)>; SUBSUME_WIDTHS.len()] = Default::default();
        // Pending/Retry/Running only. Excluding Superseded is load-bearing:
        // `split_time_task` supersedes an oversized unit and replaces it with
        // children tiling its range, so a superseded parent subsuming its own
        // children would delete exactly the work the split just created.
        for task in self.snapshot.tasks.iter().filter(|task| matches!(task.state, TaskState::Pending | TaskState::Retry | TaskState::Running)) {
            let group = group_of(task);
            let (start, end) = (task.key.slice.start_micros, task.key.slice.end_micros);
            for (index, &width) in SUBSUME_WIDTHS.iter().enumerate().filter(|&(_, &width)| task.key.slice.width() > width) {
                let buckets = std::iter::successors(Some(start.div_euclid(width) * width), |bucket| Some(bucket.saturating_add(width)))
                    .take_while(|bucket| bucket.saturating_add(width) <= end)
                    .filter(|bucket| *bucket >= start);
                covered[index].extend(buckets.map(|bucket| (group.clone(), bucket)));
            }
        }
        // Damage repairs are exempt, as in fusion: the unit is sized to one
        // file's uncovered span and a wider unit does NOT replace it.
        let damaged = self.untagged_cells.clone();
        let is_damage = |task: &MaintenanceTask| Self::cell_of(task).is_some_and(|cell| damaged.contains(&cell));
        self.retain_tasks(|task| {
            // A covering task does not carry this retry's failure history or deadline.
            if task.state != TaskState::Pending || is_live_frontier(task.key.slice, now_micros) || is_damage(task) {
                return true;
            }
            // The NARROWEST ladder width this unit fits inside, so every unit
            // recorded against it is strictly wider. Taking the widest that fits
            // lets an unaligned unit find the entry its OWN expansion wrote and
            // subsume itself, leaving the cell with nothing queued.
            let Some(index) = SUBSUME_WIDTHS.iter().position(|&width| width >= task.key.slice.width()) else { return true };
            let width = SUBSUME_WIDTHS[index];
            let bucket = task.key.slice.start_micros.div_euclid(width) * width;
            // An unaligned unit can straddle two buckets; one bucket covering it
            // is the claim being made, so check it rather than assume alignment.
            if bucket > task.key.slice.start_micros || task.key.slice.end_micros > bucket.saturating_add(width) {
                return true;
            }
            !covered[index].contains(&(group_of(task), bucket))
        })
    }

    /// One pass of `coarsen_sealed_slices` at a single width: fuses every
    /// strictly-narrower sealed unit in a bucket into one unit of `width`, when
    /// the bucket's estimate fits `MAX_DECODED_BYTES` and nothing at least that
    /// wide already covers it.
    fn coarsen_to_width_reporting(&mut self, width: i64, now_micros: i64, partition_bytes: &dyn Fn(&str, &str, &str) -> Option<u64>) -> CoarsenReport {
        let bucket_of = |start: i64| start.div_euclid(width) * width;
        // DAMAGE REPAIR IS NEVER COARSENED: a repair unit is sized to one file's
        // uncovered span, so fusing it destroys the only work that can close
        // that hole. Cloned, not borrowed: this pass mutates `self` further down.
        let untagged_cells = self.untagged_cells.clone();
        let coarsenable = |task: &MaintenanceTask| {
            matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking)
                && task.state == TaskState::Pending
                && !is_live_frontier(task.key.slice, now_micros)
                && task.key.slice.width() < width
                && !Self::cell_of(task).is_some_and(|cell| untagged_cells.contains(&cell))
        };
        // Running and Retry units block every overlapping bucket: a retry owns a
        // deadline and failure history that fusing would reset. Pending units
        // block buckets already covered at this width or wider.
        let group_of = |task: &MaintenanceTask, bucket: i64| {
            (task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation, bucket)
        };
        let own_group = |task: &MaintenanceTask| group_of(task, bucket_of(task.key.slice.start_micros));
        let blocks_bucket = |task: &MaintenanceTask| match task.state {
            TaskState::Running | TaskState::Retry => true,
            TaskState::Pending => task.key.slice.width() >= width,
            // Superseded must NOT block, or superseded ancestors would block
            // every fusion width while none may subsume, stranding their
            // descendants forever. The budget test below is the anti-loop guard.
            TaskState::Superseded | TaskState::Complete => false,
        };
        let blocked: HashSet<_> = self
            .snapshot
            .tasks
            .iter()
            .filter(|task| blocks_bucket(task))
            .flat_map(|task| {
                std::iter::successors(Some(bucket_of(task.key.slice.start_micros)), |bucket| Some(bucket.saturating_add(width)))
                    .take_while(|bucket| *bucket < task.key.slice.end_micros)
                    .map(move |bucket| group_of(task, bucket))
            })
            .collect();

        let mut report = CoarsenReport::default();
        let mut groups: HashMap<(String, String, String, Operation, i64), GroupPrice> = HashMap::new();
        for task in self.snapshot.tasks.iter().filter(|task| coarsenable(task)) {
            report.candidates += 1;
            let group = own_group(task);
            if blocked.contains(&group) {
                report.blocked += 1;
                continue;
            }
            groups.entry(group).or_default().add(task);
        }
        // Only fuse a span that will actually FIT: a unit that cannot finish
        // inside its deadline is strictly worse than the slices it replaced.
        // Then bound the price by what the partition can hold, which only ever
        // removes double-counting from members priced by a plain sum.
        let priced_by_partition: HashSet<_> = groups
            .iter_mut()
            .filter_map(|(group, price)| {
                let (_, source, project_id, op, bucket) = group;
                let date = chrono::DateTime::from_timestamp_micros(*bucket)?.date_naive().to_string();
                price.cap_at(partition_bytes(project_id, source, &date)?);
                // Dedup ONLY: a fused over-budget unit is safe only where the
                // runner honours `hash_shard`, which the rollup lanes do not.
                (*op == Operation::Dedup).then(|| group.clone())
            })
            .collect();
        groups.retain(|group, price| {
            // A group priced against its PARTITION may exceed the decode budget
            // and still be worth fusing: its members each pay that cost anyway.
            // The budget is then enforced at claim time, where the preflight
            // measures the fused unit and `byte_bounded_units` shards it BY KEY.
            let fits = price.bytes() <= MAX_DECODED_BYTES || priced_by_partition.contains(group);
            report.priced_by_footprint += usize::from(fits && price.summed_bytes > MAX_DECODED_BYTES);
            if !fits {
                report.over_budget += price.members;
            }
            fits
        });
        if groups.is_empty() {
            return report;
        }
        report.fused = self.retain_tasks(|task| !coarsenable(task) || !groups.contains_key(&own_group(task)));
        for ((physical_table, source, project_id, operation, bucket), price) in groups {
            let Ok(slice) = TimeSlice::new(bucket, bucket.saturating_add(width)) else { continue };
            let oldest_member = price.oldest.unwrap_or_else(|| u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default());
            self.upsert(MaintenanceTask {
                // Only when every member agreed: a fused unit over several file
                // sets reads their union, which no scalar here can state.
                input: price.unanimous_input(),
                // Inherit the OLDEST member's age, not `now` — `scheduling_class`
                // escalates on wait time, so a fresh stamp would keep the fused
                // unit permanently outranked.
                ..MaintenanceTask::pending(TaskKey { physical_table, source, project_id, slice, operation }, now_micros, price.bytes(), oldest_member)
            });
        }
        report
    }

    /// Work of `operation` still waiting to run — every state `claim_next` can
    /// select from.
    fn queued(&self, operation: Operation) -> impl Iterator<Item = &MaintenanceTask> {
        self.snapshot.tasks.iter().filter(move |task| task.key.operation == operation && matches!(task.state, TaskState::Pending | TaskState::Retry))
    }

    /// Why queued work for `operation` is not being claimed: `(pending, sealed,
    /// unproven, quarantined, not_yet_due)`. `unproven` counts
    /// `!base_tier_present` rather than calling `dependencies_complete`, which
    /// would make this census O(n^2) under the journal lock.
    pub fn claimability_census(&self, operation: Operation, now_micros: i64) -> (usize, usize, usize, usize, usize) {
        self.queued(operation).fold((0, 0, 0, 0, 0), |(pending, sealed, unproven, quarantined, not_due), task| {
            (
                pending + 1,
                sealed + usize::from(!is_frontier_task(task, now_micros)),
                unproven + usize::from(!task.base_tier_present),
                quarantined + usize::from(Self::is_quarantined(task)),
                not_due + usize::from(task.deadline_micros > now_micros),
            )
        })
    }

    /// The full claim order for one task: `(class, damaged, starved, hole,
    /// width, benefit, recency)`. Smaller wins.
    ///
    /// `hole_rank` orders WITHIN a class: a cell whose tier output is missing
    /// outranks one merely being re-derived, because newest-first is right for
    /// freshness and wrong for contiguity. DAMAGE leads its class, ahead of
    /// `starved`, and orders by NEITHER width nor recency — damage units all
    /// tie, so the per-project cursor in `claim_next` rotates across damaged
    /// cells instead of draining one to exhaustion.
    fn rank(&self, task: &MaintenanceTask, now_micros: i64) -> Rank {
        let (class, starved, width, benefit, order) = scheduling_class(task, now_micros);
        let hole = self.hole_rank(task);
        let (width, order) = if hole == 0 { (0, 0) } else { (width, order) };
        // Dedup contiguity: a slice adjacent to a COMPLETED dedup slice ranks ahead of one
        // seeding a new island. Must stay AFTER `starved` and out of `hole`, which outranks it.
        let adjacent = class != 0
            && task.key.operation == Operation::Dedup
            && crate::config::try_config().is_none_or(|cfg| cfg.maintenance.timefusion_dedup_contiguity_rank)
            && self
                .dedup_complete_edges
                .get(&task.key.project_id)
                .and_then(|by_source| by_source.get(&task.key.source))
                .is_some_and(|edges| edges.contains(&task.key.slice.start_micros) || edges.contains(&task.key.slice.end_micros));
        (class, u8::from(hole > 0), starved, hole, u8::from(!adjacent), width, benefit, order)
    }

    /// File-count spread of an operation's claimable cells, and how many of them the
    /// `benefit` term cannot tell apart. `None` when nothing is claimable.
    pub fn hygiene_debt_spread(&self, operation: Operation, now_micros: i64) -> Option<String> {
        use itertools::Itertools;
        let files = self
            .queued(operation)
            .filter(|task| task.deadline_micros <= now_micros && !Self::is_quarantined(task))
            .map(|task| task.input.map_or(0, |input| input.files))
            .sorted_unstable()
            .collect_vec();
        if files.is_empty() {
            return None;
        }
        let at = |q: f64| files[((files.len() - 1) as f64 * q) as usize];
        let tied = files.iter().filter(|f| **f < BENEFIT_BUCKET_FILES).count();
        // Sorted, so equal buckets are adjacent and `dedup` counts the distinct ones.
        let buckets = files.iter().map(|f| f / BENEFIT_BUCKET_FILES).dedup().count();
        let (cells, p50, p90, max) = (files.len(), at(0.5), at(0.9), at(1.0));
        Some(format!("cells={cells} files_p50={p50} files_p90={p90} files_max={max} tied_at_zero={tied} distinct_buckets={buckets}"))
    }

    /// The queued unit holding the most DEBT (`input.files`) that is not being claimed, and
    /// why — including which unit outranks it. `None` when it already wins its own claims.
    pub fn most_indebted_unclaimed(&self, operation: Operation, now_micros: i64) -> Option<String> {
        let eligible = || self.queued(operation);
        let date_of = |task: &MaintenanceTask| task_date(task).unwrap_or_else(|| "?".to_owned());
        let worst = eligible().max_by_key(|task| task.input.map_or(0, |input| input.files))?;
        let files = worst.input.map_or(0, |input| input.files);
        // The reasons that live on the task itself; anything else means ordering.
        let reason = match self.refusal_reason(worst, now_micros) {
            Some(reason) => reason.to_owned(),
            None => {
                let winner = eligible().filter(|task| self.refusal_reason(task, now_micros).is_none()).min_by_key(|task| self.rank(task, now_micros))?;
                if winner.key == worst.key {
                    return None;
                }
                format!("outranked_by:{:.8}:{}", winner.key.project_id, date_of(winner))
            }
        };
        Some(format!("{reason}:{:.8}:{}:files={files}", worst.key.project_id, date_of(worst)))
    }

    /// The first SEALED task of `operation` that `claim_next` would refuse, and why — as
    /// `(project, date, reason)`. Bounded to `LIMIT` because `dependencies_complete` is a scan.
    pub fn first_refused_sealed(&self, operation: Operation, now_micros: i64) -> Option<(String, String, &'static str)> {
        const LIMIT: usize = 64;
        use itertools::Itertools;
        // A claimable one means eligibility is fine and the refusal is in ordering.
        self.queued(operation)
            .filter(|task| !is_frontier_task(task, now_micros))
            .take(LIMIT)
            .find_or_first(|task| self.refusal_reason(task, now_micros).is_none())
            .map(|task| {
                let why = self.refusal_reason(task, now_micros).unwrap_or("CLAIMABLE");
                (task.key.project_id.clone(), task_date(task).unwrap_or_else(|| "?".to_owned()), why)
            })
    }

    /// Publish which `(source, project, date)` have their BASE tier built. Replaces the set
    /// wholesale: coverage can go backwards, and a stale "ready" derives from a missing tier.
    pub fn set_base_tier_ready(&mut self, ready: HashSet<(String, String, String)>) {
        self.base_tier_ready = ready;
    }

    pub fn base_tier_ready_len(&self) -> usize {
        self.base_tier_ready.len()
    }

    /// Which sources contributed to the published coverage and holes.
    pub fn tier_hole_sources(&self) -> HashSet<String> {
        self.tier_holes.iter().map(|(source, ..)| source.clone()).collect()
    }

    /// Publish which `(source, project, tier table, date)` are MISSING, so `claim_next` can
    /// rank holes ahead of re-derives. Replaced wholesale, like `base_tier_ready`.
    pub fn set_tier_holes(&mut self, holes: HashSet<(String, String, String, String)>) {
        self.tier_holes = holes;
    }

    /// Replace the untagged set for ONE `(source, tier table)`, leaving every
    /// other producer's cells alone — see the field.
    pub fn set_untagged_cells(&mut self, source: &str, table: &str, cells: impl IntoIterator<Item = (String, String)>) {
        self.untagged_cells.retain(|(cell_source, _, cell_table, _)| cell_source != source || cell_table != table);
        self.untagged_cells.extend(cells.into_iter().map(|(project, date)| (source.to_owned(), project, table.to_owned(), date)));
    }

    pub fn untagged_cells_len(&self) -> usize {
        self.untagged_cells.len()
    }

    /// Seed the set from the sidecar at boot so the damage rank is live from the first claim.
    /// Additive, and safe if stale: a stale entry costs one mis-ranked claim, never correctness.
    pub fn restore_untagged_cells(&mut self, cells: impl IntoIterator<Item = (String, String, String, String)>) {
        self.untagged_cells.extend(cells);
    }

    /// Every cell currently ranked as damaged, for persisting.
    pub fn untagged_cells(&self) -> impl Iterator<Item = &(String, String, String, String)> {
        self.untagged_cells.iter()
    }

    /// Forget one cell, for a publish that has just left the partition clean. The only other
    /// producer runs once at startup, so without this a converged cell out-ranks real work
    /// until the next restart.
    pub fn clear_untagged_cell(&mut self, source: &str, table: &str, project: &str, date: &str) -> bool {
        self.untagged_cells.remove(&(source.to_owned(), project.to_owned(), table.to_owned(), date.to_owned()))
    }

    /// How badly this cell needs the work: 0 repairs DAMAGE, 1 fills a missing day, 2
    /// re-derives a day that already has output. Smaller runs first — damage leads because a
    /// repair unit is narrow by construction and the width tiebreak would otherwise bury it.
    fn hole_rank(&self, task: &MaintenanceTask) -> u8 {
        let cell = matches!(task.key.operation, Operation::BaseRollup | Operation::DerivedRollup).then(|| Self::cell_of(task)).flatten();
        cell.map_or(2, |cell| {
            if self.untagged_cells.contains(&cell) {
                0
            } else if self.tier_holes.contains(&cell) {
                1
            } else {
                2
            }
        })
    }

    /// Two units' identity apart from their time slices.
    fn same_cell(a: &TaskKey, b: &TaskKey) -> bool {
        a.physical_table == b.physical_table && a.source == b.source && a.project_id == b.project_id && a.operation == b.operation
    }

    /// The cell a unit sits in, keyed exactly as `untagged_cells` and `tier_holes` key theirs.
    fn cell_of(task: &MaintenanceTask) -> Option<(String, String, String, String)> {
        task_date(task).map(|date| (task.key.source.clone(), task.key.project_id.clone(), task.key.physical_table.clone(), date))
    }

    /// Why `claim_next` would refuse this task, in the order it applies the reasons that live
    /// on the task itself. `None` means the refusal is upstream — ordering, not eligibility.
    fn refusal_reason(&self, task: &MaintenanceTask, now_micros: i64) -> Option<&'static str> {
        (task.deadline_micros > now_micros)
            .then_some("not_due")
            .or_else(|| Self::is_quarantined(task).then_some("quarantined"))
            .or_else(|| (!self.dependencies_complete(task)).then_some("dependencies"))
    }

    /// The task `key` names, if the journal still holds it.
    /// Re-derive the claimable set from scratch. Cheap relative to a claim and
    /// the only thing that bounds the permissive set's growth.
    fn rebuild_claimable(&mut self) {
        self.claimable =
            self.snapshot.tasks.iter().filter(|task| matches!(task.state, TaskState::Pending | TaskState::Retry)).map(|task| task.key.clone()).collect();
    }

    /// Tasks of `operation` worth considering for a claim.
    ///
    /// Walks the claimable INDEX, not the whole journal. Prod held 93,326
    /// Complete tasks against 857 Pending, and filtering all of them on three
    /// passes per claim cost 4.7 ms at 219 claims/sec — the journal mutex busy
    /// 41% of wall-clock, with a worst single claim of 3.3 s.
    fn claim_candidates(&self, operation: Operation) -> impl Iterator<Item = &MaintenanceTask> {
        self.claimable.iter().filter_map(move |key| self.task(key)).filter(move |task| task.key.operation == operation)
    }

    /// Record that `key` changed: it needs persisting, and it MIGHT now be
    /// claimable.
    ///
    /// Every state transition already passed through `dirty_tasks`, so routing
    /// both through one call is what makes the claimable set a sound superset —
    /// a task cannot become claimable without being mutated, and a mutation
    /// cannot skip this. Entries that are not (or no longer) claimable are
    /// filtered on the way past and pruned lazily, so the set errs toward
    /// holding too much rather than too little.
    fn mark_dirty(&mut self, key: TaskKey) {
        if self.task(&key).is_some_and(|task| matches!(task.state, TaskState::Pending | TaskState::Retry)) {
            self.claimable.insert(key.clone());
        }
        self.dirty_tasks.insert(key);
    }

    fn task(&self, key: &TaskKey) -> Option<&MaintenanceTask> {
        self.snapshot.tasks.get(*self.task_indices.get(key)?)
    }

    /// Mutable form of [`Self::task`]. Callers own their own `dirty_tasks` bookkeeping.
    fn task_mut(&mut self, key: &TaskKey) -> Option<&mut MaintenanceTask> {
        let index = *self.task_indices.get(key)?;
        self.snapshot.tasks.get_mut(index)
    }

    pub fn prove_base_tier_for_day(&mut self, key: &TaskKey, day_start: i64, day_end: i64) -> usize {
        // Only work that can still run: proving a completed task is a no-op that would
        // make the returned count read as progress.
        self.edit_tasks(
            |task| {
                matches!(task.state, TaskState::Pending | TaskState::Retry)
                    && !task.base_tier_present
                    && Self::same_cell(&task.key, key)
                    && task.key.slice.start_micros >= day_start
                    && task.key.slice.end_micros <= day_end
            },
            |task| task.base_tier_present = true,
        )
    }

    /// Remember what a unit reads, measured by the claim-time preflight. Must be recorded on
    /// EVERY claim, not only when the preflight splits: `abandon_running` bisects from a key
    /// alone, so without a footprint on the parent the children carry none.
    pub fn record_input(&mut self, key: &TaskKey, input: InputFootprint) -> bool {
        let Some(task) = self.task_mut(key) else { return false };
        if task.input == Some(input) {
            return false;
        }
        task.input = Some(input);
        task.preflight_decoded_bytes = None;
        self.mark_dirty(key.clone());
        true
    }

    /// Retain the current input estimate even when the unit fits and is dispatched.
    pub fn record_preflight(&mut self, key: &TaskKey, input: Option<InputFootprint>, decoded_bytes: u64) -> bool {
        let input_changed = input.is_some_and(|input| self.record_input(key, input));
        let Some(task) = self.task_mut(key) else { return false };
        if task.preflight_decoded_bytes == Some(decoded_bytes) {
            return input_changed;
        }
        task.preflight_decoded_bytes = Some(decoded_bytes);
        self.mark_dirty(key.clone());
        true
    }

    pub fn upsert(&mut self, task: MaintenanceTask) {
        // A key removed earlier in this write window and re-created now must drop its tombstone.
        self.removed_tasks.remove(&task.key);
        let key = task.key.clone();
        if task.state == TaskState::Complete {
            self.note_dedup_edges(&task.key);
        }
        insert_task(&mut self.snapshot.tasks, &mut self.task_indices, task);
        // AFTER the insert: `mark_dirty` reads the task's state to decide whether it
        // belongs in the claim index, and before the insert there is nothing to read.
        self.mark_dirty(key);
    }

    pub fn enqueue(&mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64) {
        self.enqueue_with_base_tier(key, deadline_micros, estimated_decoded_bytes, created_unix_ms, false);
    }

    /// `base_tier_present` records that the tier this unit aggregates already exists; only
    /// `plan_rollup_backfill` can prove it. Returns whether the queue accepted the unit —
    /// `false` means one of the two structural vetoes in `enqueue_inner` fired.
    pub fn enqueue_with_base_tier(
        &mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64, base_tier_present: bool,
    ) -> bool {
        self.enqueue_inner(key, deadline_micros, estimated_decoded_bytes, created_unix_ms, base_tier_present, None)
    }

    /// Queue a unit the planner has already MEASURED, carrying its footprint so
    /// `scheduling_class` can rank a never-claimed hygiene unit by `input.files`.
    ///
    /// Deliberately NOT `upsert`: that resets `state`/`attempts` and bypasses the
    /// Superseded/live-descendant veto below, resurrecting a parent beside its children.
    pub fn enqueue_planned(&mut self, task: &MaintenanceTask) {
        self.enqueue_inner(task.key.clone(), task.deadline_micros, task.estimated_decoded_bytes, task.created_unix_ms, task.base_tier_present, task.input);
    }

    /// Footprint precedence: `None` NEVER erases an existing one (that would break the bisect
    /// ladder); the claim-time measurement wins while Running or quarantined; between claims
    /// the PLANNER's is the fresher observation.
    ///
    /// Returns `true` when the unit is queued afterwards; only the two structural vetoes
    /// below return `false`.
    fn enqueue_inner(
        &mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64, base_tier_present: bool,
        input: Option<InputFootprint>,
    ) -> bool {
        // Same rule as `upsert`: a key removed earlier in this write window and enqueued
        // again is CREATED, not removed (`coarsen_to_width` does both in one pass).
        self.removed_tasks.remove(&key);
        // Re-pend an existing entry. `attempts` restarts only for a superseded parent, which
        // is fresh debt rather than a retry of the same unit.
        let repend = |task: &mut MaintenanceTask, deadline: i64, reset_attempts: bool| {
            task.state = TaskState::Pending;
            task.deadline_micros = deadline;
            task.estimated_decoded_bytes = estimated_decoded_bytes;
            task.attempts = if reset_attempts { 0 } else { task.attempts };
            task.retry_reason = None;
            task.publication = None;
            task.base_tier_present |= base_tier_present;
            task.input = input.or(task.input);
        };
        if let Some(index) = self.task_indices.get(&key).copied() {
            // A superseded parent must not be resurrected beside its children — its wider
            // slice would outrank them forever. Only with no live descendant is this fresh
            // debt, and then it is a NEW unit: attempts start over.
            if self.snapshot.tasks[index].state == TaskState::Superseded {
                let parent = &self.snapshot.tasks[index].key;
                let live_descendant = self.snapshot.tasks.iter().any(|task| {
                    task.key != *parent
                        && Self::same_cell(&task.key, parent)
                        && task.key.slice.start_micros >= parent.slice.start_micros
                        && task.key.slice.end_micros <= parent.slice.end_micros
                        && matches!(task.state, TaskState::Pending | TaskState::Retry | TaskState::Running)
                });
                if live_descendant {
                    return false;
                }
                repend(&mut self.snapshot.tasks[index], deadline_micros, true);
                self.mark_dirty(key);
                return true;
            }
            // `abandon_running`'s verdict must outlive a planner tick: re-minting would clear
            // both the deadline floor and the quarantine-routing reason every tick.
            if self.snapshot.tasks[index].state == TaskState::Retry
                && matches!(self.snapshot.tasks[index].retry_reason.as_deref(), Some(Self::WORKER_FAILURE_REASON | Self::SCHEMA_FAILURE_REASON))
            {
                return false;
            }
            let task = &mut self.snapshot.tasks[index];
            if task.state != TaskState::Running {
                let new_deadline = task.deadline_micros.min(deadline_micros);
                let changed = task.state != TaskState::Pending
                    || task.deadline_micros != new_deadline
                    || task.estimated_decoded_bytes != estimated_decoded_bytes
                    || task.retry_reason.is_some()
                    || task.publication.is_some()
                    // Latching, never clearing: an enqueue that cannot prove presence is
                    // silence, not evidence of absence.
                    || (base_tier_present && !task.base_tier_present)
                    || (input.is_some() && input != task.input);
                if changed {
                    repend(task, new_deadline, false);
                    self.mark_dirty(key);
                }
            }
            return true;
        }
        let enqueued = key.clone();
        insert_task(
            &mut self.snapshot.tasks,
            &mut self.task_indices,
            MaintenanceTask { base_tier_present, input, ..MaintenanceTask::pending(key, deadline_micros, estimated_decoded_bytes, created_unix_ms) },
        );
        // AFTER the insert, as in `upsert`: `mark_dirty` reads state to decide
        // whether the key belongs in the claim index, and there is nothing to read
        // until the task exists.
        self.mark_dirty(enqueued);
        true
    }

    /// Drop every task the predicate rejects, recording a tombstone for each. THE way to
    /// remove tasks — a bare `snapshot.tasks.retain` records no tombstone, so the removal
    /// lives only in memory and comes back on the next journal load.
    fn retain_tasks(&mut self, mut keep: impl FnMut(&MaintenanceTask) -> bool) -> usize {
        let before = self.snapshot.tasks.len();
        let removed = &mut self.removed_tasks;
        let dirty = &mut self.dirty_tasks;
        self.snapshot.tasks.retain(|task| {
            if keep(task) {
                return true;
            }
            // A key that was pending a write and is now gone must not also be upserted.
            dirty.remove(&task.key);
            removed.insert(task.key.clone());
            false
        });
        let dropped = before - self.snapshot.tasks.len();
        if dropped != 0 {
            self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
        }
        dropped
    }

    /// Record all maintenance consequences of a source mutation. Repeated
    /// invalidations are idempotent by `TaskKey`; an already-complete slice is
    /// made pending again and its quiet-period deadline moves forward.
    pub fn invalidate(&mut self, invalidation: Invalidation<'_>) -> anyhow::Result<()> {
        let Invalidation { start_micros, end_micros, derived, .. } = invalidation;
        let normal_slices = TimeSlice::normal_units(start_micros, end_micros)?;
        let rollup_slices = if derived {
            let aligned_start = start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            let aligned_end = end_micros.saturating_add(DERIVED_SLICE_MICROS - 1).div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            TimeSlice::fixed_units(aligned_start, aligned_end, DERIVED_SLICE_MICROS)?
        } else {
            normal_slices.clone()
        };
        self.invalidate_slices(invalidation, &normal_slices, &rollup_slices)
    }

    /// A history gap has no trustworthy per-hour change bounds. Requeue the
    /// complete range as coarse work; ordinary capacity splitting still applies.
    pub(crate) fn invalidate_coarse(&mut self, invalidation: Invalidation<'_>) -> anyhow::Result<()> {
        let slice = TimeSlice::new(invalidation.start_micros, invalidation.end_micros)?;
        self.invalidate_slices(invalidation, &[slice], &[slice])
    }

    fn invalidate_slices(&mut self, invalidation: Invalidation<'_>, normal_slices: &[TimeSlice], rollup_slices: &[TimeSlice]) -> anyhow::Result<()> {
        let Invalidation { source_table, rollup_table, source, project_id, observed_at_micros, derived, mint_dedup, mint_rollup, .. } = invalidation;
        // Round up, never down: a bucket may delay eligibility but must never publish before
        // the full quiet period.
        let deadline = observed_at_micros.saturating_add(FINALIZATION_DELAY_MICROS);
        let deadline_micros = deadline
            .saturating_add(INVALIDATION_DEADLINE_BUCKET_MICROS - 1)
            .div_euclid(INVALIDATION_DEADLINE_BUCKET_MICROS)
            .saturating_mul(INVALIDATION_DEADLINE_BUCKET_MICROS);
        let created_unix_ms = u64::try_from(observed_at_micros.div_euclid(1_000)).unwrap_or_default();
        // HotPacking is deliberately NOT minted here: file hygiene is planned by DEBT, not by
        // the calendar — see `plan_compaction_debt`.
        for (operation, slices) in [(Operation::Dedup, normal_slices), (if derived { Operation::DerivedRollup } else { Operation::BaseRollup }, rollup_slices)]
        {
            // False only for a DV-dedup-only wave, which changed no logical content.
            if operation == Operation::Dedup && !mint_dedup {
                crate::observability::maintenance_stats().dedup_remint_skipped.fetch_add(slices.len() as u64, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            if matches!(operation, Operation::BaseRollup | Operation::DerivedRollup) && !mint_rollup {
                crate::observability::maintenance_stats().rollup_remint_skipped.fetch_add(slices.len() as u64, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            for &slice in slices {
                let key = TaskKey {
                    physical_table: match operation {
                        Operation::Dedup | Operation::HotPacking => source_table,
                        _ => rollup_table,
                    }
                    .to_owned(),
                    source: source.to_owned(),
                    project_id: project_id.to_owned(),
                    slice,
                    operation,
                };
                if let Some(index) = self.task_indices.get(&key).copied() {
                    let task = &mut self.snapshot.tasks[index];
                    let new_deadline = task.deadline_micros.max(deadline_micros);
                    let changed =
                        task.state != TaskState::Pending || task.deadline_micros != new_deadline || task.retry_reason.is_some() || task.publication.is_some();
                    if changed {
                        task.state = TaskState::Pending;
                        task.deadline_micros = new_deadline;
                        task.retry_reason = None;
                        task.publication = None;
                        self.mark_dirty(key);
                    }
                } else {
                    let task = MaintenanceTask::pending(key.clone(), deadline_micros, 0, created_unix_ms);
                    insert_task(&mut self.snapshot.tasks, &mut self.task_indices, task);
                    self.mark_dirty(key);
                }
            }
        }
        Ok(())
    }

    pub fn mark_running(&mut self, key: &TaskKey) -> bool {
        let Some(task) = self.task_mut(key) else { return false };
        if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
            return false;
        }
        task.state = TaskState::Running;
        task.preflight_decoded_bytes = None;
        task.attempts = task.attempts.saturating_add(1);
        true
    }

    /// Attempts after which a unit has PROVEN it does not fit its deadline: one timeout is a
    /// blip, two is the slice itself.
    pub const QUARANTINE_ATTEMPTS: u32 = 2;

    /// The `retry_reason` `abandon_running` writes when a WORKER gave a unit back.
    pub const WORKER_FAILURE_REASON: &'static str = "worker_error";

    /// The `retry_reason` a unit parked on a DETERMINISTIC plan error carries — a state
    /// token, never the error text, because three places match it exactly.
    pub const SCHEMA_FAILURE_REASON: &'static str = "schema_error";
    /// How long a schema-parked unit waits: what fixes it is a rebuild or a deploy, not
    /// another attempt.
    const SCHEMA_PARK_MICROS: i64 = 3_600 * 1_000_000;

    /// Park a unit whose SQL cannot be planned. Never bisected: the children would name the
    /// same missing column and fail identically.
    fn park_schema_failure(&mut self, key: &TaskKey, now_micros: i64, error: &str) {
        crate::observability::maintenance_stats().maintenance_schema_parked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(?key, %error, event = "maintenance_task_schema_parked", "parked a maintenance unit on a deterministic plan error instead of bisecting it");
        self.retry(key, Self::SCHEMA_FAILURE_REASON.to_owned(), now_micros.saturating_add(Self::SCHEMA_PARK_MICROS));
    }

    /// Has this unit PROVEN it cannot fit its deadline?
    ///
    /// Attempts alone are NOT proof — an attempt is counted before cost is known — so the
    /// worker's own verdict is required too. A schema-parked unit qualifies immediately.
    pub fn is_quarantined(task: &MaintenanceTask) -> bool {
        task.retry_reason.as_deref() == Some(Self::SCHEMA_FAILURE_REASON)
            || (task.attempts >= Self::QUARANTINE_ATTEMPTS && task.retry_reason.as_deref() == Some(Self::WORKER_FAILURE_REASON))
    }

    /// Claim one unit. `allow_quarantined` admits units that have already timed out
    /// [`Self::QUARANTINE_ATTEMPTS`] times; the caller must gate it on a small occupancy
    /// permit so proven-unfittable work cannot hold the whole pool.
    pub fn claim_next(&mut self, operation: Operation, now_micros: i64, allow_quarantined: bool) -> Option<MaintenanceTask> {
        self.claim_tick = self.claim_tick.wrapping_add(1);
        // Class is strict priority and ingest regenerates frontier work continuously, so one
        // claim in two is RESERVED for sealed work — one in four while the frontier is behind
        // its lag budget. DerivedRollup always takes the sealed turn.
        let sealed_turn = operation == Operation::DerivedRollup
            || if self.frontier_lag_secs.load(std::sync::atomic::Ordering::Relaxed) > FRONTIER_LAG_BUDGET_SECS {
                self.claim_tick.is_multiple_of(4)
            } else {
                self.claim_tick.is_multiple_of(2)
            };
        let claimable = |task: &MaintenanceTask| task.key.operation == operation && Self::task_can_be_claimed(task, now_micros, allow_quarantined);
        // One claim in four is RESERVED for work inside the window dashboards read, chosen
        // WITHOUT reference to `starved` (any starved task outranks any non-starved one).
        // Residue 3 is load-bearing: ODD, so it never collides with a sealed turn (multiples
        // of 2 or 4), and it does not fire on a fresh journal's first claim.
        let window_turn = self.claim_tick % 4 == 3;
        let rank = |journal: &Self, task: &MaintenanceTask| -> Rank { journal.rank(task, now_micros) };
        // One claim in eight for the band NOTHING else serves: older than the query window,
        // younger than the starvation horizon. Residue 5 is odd (never a sealed turn) and
        // `5 % 4 == 1` (never a window turn).
        let horizon_turn = self.claim_tick % 8 == 5;
        /// `(sealed_only, window_only, horizon_only)` — one reservation's filter.
        type Reservation = (bool, bool, bool);
        // ONE pass for the reservation AND its fallback, not two.
        //
        // The turn always falls back to the normal order, so the old form scanned
        // the candidate set twice before the selection pass scanned it a third
        // time. Tracking both minima in a single walk makes that 3 passes -> 2,
        // and the per-task work that actually costs — `rank` and
        // `dependencies_complete` — is now evaluated once per task instead of once
        // per pass. The selection pass cannot fold in: it needs the winning class,
        // which is not known until the walk finishes.
        let best_class = |journal: &Self, primary: Reservation, fallback: Reservation| -> Option<Rank> {
            let (mut best_primary, mut best_fallback): (Option<Rank>, Option<Rank>) = (None, None);
            for task in journal.claim_candidates(operation) {
                if !Self::task_can_be_claimed(task, now_micros, allow_quarantined) {
                    continue;
                }
                let waited = now_micros.saturating_sub(task.key.slice.end_micros);
                let admits = |(sealed_only, window_only, horizon_only): Reservation| {
                    !(sealed_only && is_frontier_task(task, now_micros))
                        && !(window_only && waited > QUERY_WINDOW_MICROS)
                        && (!horizon_only || (QUERY_WINDOW_MICROS..=STARVATION_HORIZON_MICROS).contains(&waited))
                };
                let (in_primary, in_fallback) = (admits(primary), admits(fallback));
                if !in_primary && !in_fallback {
                    continue;
                }
                let candidate = rank(journal, task);
                let improves_primary = in_primary && best_primary.is_none_or(|best| candidate < best);
                let improves_fallback = in_fallback && best_fallback.is_none_or(|best| candidate < best);
                // Same laziness as before: only pay for the dependency scan when this
                // task would actually win something.
                if (improves_primary || improves_fallback) && journal.dependencies_complete(task) {
                    if improves_primary {
                        best_primary = Some(candidate);
                    }
                    if improves_fallback {
                        best_fallback = Some(candidate);
                    }
                }
            }
            best_primary.or(best_fallback)
        };
        // Every reservation falls back to the normal order, so a quiet lane never idles a worker.
        const NORMAL: Reservation = (false, false, false);
        let class = if window_turn {
            best_class(self, (false, true, false), (sealed_turn, false, false))
        } else if horizon_turn {
            best_class(self, (true, false, true), NORMAL)
        } else if sealed_turn {
            best_class(self, (true, false, false), NORMAL)
        } else {
            best_class(self, NORMAL, NORMAL)
        }?;
        let cursor = self.fair_cursors.get(&operation).map(String::as_str).unwrap_or("");
        let beats = |task: &MaintenanceTask, current: Option<&MaintenanceTask>| {
            current.is_none_or(|current| {
                (&task.key.project_id, task.deadline_micros, &task.key) < (&current.key.project_id, current.deadline_micros, &current.key)
            })
        };
        let mut fallback: Option<&MaintenanceTask> = None;
        let mut next: Option<&MaintenanceTask> = None;
        // One pass on purpose: `dependencies_complete` is itself a scan, so a two-pass
        // `min_by_key` form would double the hot claim path's cost.
        for task in self
            .claim_candidates(operation)
            .filter(|task| Self::task_can_be_claimed(task, now_micros, allow_quarantined) && rank(self, task) == class && self.dependencies_complete(task))
        {
            if beats(task, fallback) {
                fallback = Some(task);
            }
            if task.key.project_id.as_str() > cursor && beats(task, next) {
                next = Some(task);
            }
        }
        let key = next.or(fallback)?.key.clone();
        self.fair_cursors.insert(operation, key.project_id.clone());
        self.mark_running(&key);
        self.task(&key).cloned()
    }

    /// Claim exactly the requested task without changing unrelated queue entries.
    /// Manual selection changes ordering, not eligibility or dependency proofs.
    pub fn claim_exact(&mut self, key: &TaskKey, now_micros: i64, allow_quarantined: bool) -> Option<MaintenanceTask> {
        let task = self.task(key)?;
        if !Self::task_can_be_claimed(task, now_micros, allow_quarantined) || !self.dependencies_complete(task) {
            return None;
        }
        self.mark_running(key);
        self.task(key).cloned()
    }

    fn task_can_be_claimed(task: &MaintenanceTask, now_micros: i64, allow_quarantined: bool) -> bool {
        matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros && (allow_quarantined || !Self::is_quarantined(task))
    }

    fn dependencies_complete(&self, task: &MaintenanceTask) -> bool {
        // Only a DERIVED unit has a dependency at all.
        let required = matches!(task.key.operation, Operation::DerivedRollup).then_some(Operation::BaseRollup);
        // Either witness of a present base tier short-circuits; the day-keyed set is the
        // reliable one, since the per-task flag must land on exactly the right `TaskKey`.
        if task.base_tier_present {
            return true;
        }
        if let Some(date) = task_date(task)
            && self.base_tier_ready.contains(&(task.key.source.clone(), task.key.project_id.clone(), date))
        {
            return true;
        }
        // Contiguous coverage of the slice by COMPLETE units of the required operation: the
        // `scan` walks sorted intervals and stops dead at the first gap.
        required.is_none_or(|required| {
            use itertools::Itertools;
            self.snapshot
                .tasks
                .iter()
                .filter(|candidate| {
                    candidate.key.source == task.key.source
                        && candidate.key.project_id == task.key.project_id
                        && candidate.key.operation == required
                        && candidate.state == TaskState::Complete
                        && task.key.slice.overlaps(candidate.key.slice.start_micros, candidate.key.slice.end_micros)
                })
                .map(|candidate| candidate.key.slice)
                .sorted_unstable()
                .scan(task.key.slice.start_micros, |covered, interval| {
                    (interval.start_micros <= *covered).then(|| {
                        *covered = (*covered).max(interval.end_micros);
                        *covered
                    })
                })
                .any(|covered| covered >= task.key.slice.end_micros)
        })
    }

    pub fn attempts(&self, key: &TaskKey) -> u32 {
        self.task(key).map_or(0, |task| task.attempts)
    }

    pub fn retry(&mut self, key: &TaskKey, reason: String, not_before_micros: i64) -> bool {
        let Some(task) = self.task_mut(key) else { return false };
        task.state = TaskState::Retry;
        tracing::debug!(?key, %reason, attempts = task.attempts, not_before_micros, "maintenance task retry");
        crate::observability::set_maintenance_retry_reason(&reason);
        crate::observability::count_maintenance_retry(&format!("{:?}", key.operation), &reason);
        task.retry_reason = Some(reason);
        task.deadline_micros = not_before_micros;
        self.mark_dirty(key.clone());
        true
    }

    pub fn complete(&mut self, key: &TaskKey) -> bool {
        self.finish(key, None)
    }

    pub fn publish(&mut self, key: &TaskKey, publication: Publication) -> bool {
        self.finish(key, Some(publication))
    }

    /// Mark a unit Complete. `None` leaves any existing publication untouched.
    fn finish(&mut self, key: &TaskKey, publication: Option<Publication>) -> bool {
        let Some(task) = self.task_mut(key) else { return false };
        task.state = TaskState::Complete;
        task.retry_reason = None;
        if let Some(publication) = publication {
            task.publication = Some(publication);
        }
        self.mark_dirty(key.clone());
        self.note_dedup_edges(key);
        true
    }

    /// A worker gave the task back without finishing it — an error, or a deadline it could
    /// not meet. Once is a blip: back off and retry whole. Twice means the slice does not fit
    /// its deadline, so bisect TIME (byte splitting does not cover this). Only positive
    /// evidence of a DETERMINISTIC plan error in `failure` suppresses the bisect.
    /// Give a claimed unit back WITHOUT charging it an attempt.
    ///
    /// `mark_running` increments `attempts` at claim time, so a unit released
    /// before doing any work must give that increment back or a restart looks
    /// exactly like a failed run. Returns false when the unit is not Running,
    /// which is the racy-but-harmless case of a concurrent state change.
    pub fn release_unstarted(&mut self, key: &TaskKey) -> bool {
        let Some(task) = self.task_mut(key) else { return false };
        if task.state != TaskState::Running {
            return false;
        }
        task.state = TaskState::Pending;
        task.attempts = task.attempts.saturating_sub(1);
        task.retry_reason = None;
        self.mark_dirty(key.clone());
        crate::observability::maintenance_stats().maintenance_unstarted_releases.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    pub fn abandon_running(&mut self, key: &TaskKey, now_micros: i64, failure: Option<&str>) {
        if let Some(error) = failure.filter(|failure| is_schema_failure(failure)) {
            self.park_schema_failure(key, now_micros, error);
            return;
        }
        let attempts = self.task(key).map_or(1, |task| task.attempts);
        if attempts >= 2 && self.split_task(key, SplitTrigger::RepeatedFailure, None) {
            return;
        }
        // Floored at this operation's OWN deadline, otherwise an unsplittable unit burns the
        // full deadline, waits out a backoff capped at 256s, and burns it again forever.
        let backoff_micros = exponential_backoff_micros(attempts);
        // Only after a REPEAT: the FairSpillPool can squeeze out a correctly sized unit that
        // would succeed untouched next pass.
        let delay_micros = if attempts >= 2 {
            let floor_micros = i64::try_from(operation_deadline_secs(key.operation).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            backoff_micros.max(floor_micros)
        } else {
            backoff_micros
        };
        self.retry(key, Self::WORKER_FAILURE_REASON.to_owned(), now_micros.saturating_add(delay_micros));
    }

    /// Retry, or split when the input did not FIT — here what overran is BYTES, which is what
    /// `byte_bounded_units` divides. One failure is tolerated (the pool is a FairSpillPool,
    /// so unrelated work can squeeze a unit out); only a repeat says it is too big.
    pub fn retry_or_split(&mut self, key: &TaskKey, reason: String, when_micros: i64, attempts: u32) {
        // A missing field is the opposite verdict: unplannable at any width.
        if is_schema_failure(&reason) {
            self.park_schema_failure(key, crate::support::now_micros(), &reason);
            return;
        }
        let repeated_capacity = attempts >= 2 && is_capacity_failure(&reason);
        if repeated_capacity && self.split_task(key, SplitTrigger::RepeatedFailure, None) {
            return;
        }
        // Split refused on a REPEATED capacity failure: the caller's delay is tuned for
        // transient contention, so use the exponential backoff instead of hot-looping.
        let escalated = crate::support::now_micros().saturating_add(exponential_backoff_micros(attempts));
        self.retry(key, reason, if repeated_capacity { when_micros.max(escalated) } else { when_micros });
    }

    pub fn split_time_task(&mut self, key: &TaskKey, observed_bytes: u64, input: Option<InputFootprint>) -> bool {
        self.split_task(key, SplitTrigger::Preflight(observed_bytes), input)
    }

    fn split_task(&mut self, key: &TaskKey, trigger: SplitTrigger, input: Option<InputFootprint>) -> bool {
        // A Repair unit's cost is the FILE it rewrites; time-bisection cannot shrink a file
        // set, so every child would fight over the same file.
        if key.operation == Operation::Repair {
            crate::observability::maintenance_stats().split_declined_at_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let mut parent = self.snapshot.tasks[index].clone();
        // Children of a split read ALL of the parent's files, and `coarsen_to_width` needs
        // the stamp to refuse them later.
        parent.input = input.or(parent.input);
        // Bisecting halves BYTES only while the slice is wide enough to drop whole files;
        // below that the modelled cost keeps falling while the real one does not, so splits
        // "fit" on paper forever. Decline once a child costs most of its parent and let it
        // RUN — the runner hash-shards internally at any width.
        let observed_bytes = match trigger {
            SplitTrigger::Preflight(bytes) => Some(bytes),
            SplitTrigger::RepeatedFailure => parent.preflight_decoded_bytes,
        };
        if observed_bytes.is_some_and(|bytes| !split_sheds_enough(parent.parent_measured_bytes, bytes)) {
            crate::observability::maintenance_stats().split_declined_at_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        let cost = observed_bytes.unwrap_or(parent.estimated_decoded_bytes);
        let children = match trigger {
            SplitTrigger::Preflight(_) => byte_bounded_units(&parent, cost),
            SplitTrigger::RepeatedFailure => bisect_time_unit(&parent, cost).map(Vec::from).unwrap_or_default(),
        };
        if children.len() <= 1 || children.iter().any(|child| child.hash_shards > 1) {
            crate::observability::maintenance_stats().split_declined_no_width.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Superseded;
        task.retry_reason = Some("split_into_smaller_slices".to_owned());
        self.mark_dirty(key.clone());
        for mut child in children {
            // What the PARENT measured; the child's modelled share is not trustworthy.
            child.parent_measured_bytes = observed_bytes;
            child.preflight_decoded_bytes = None;
            child.state = TaskState::Pending;
            child.attempts = 0;
            child.retry_reason = None;
            child.publication = None;
            // A split narrows the WORK, not the priority: without this the children rank by
            // their own narrow width and fall behind every day-wide unit.
            child.backfill_priority_micros = Some(parent.scheduling_width());
            self.upsert(child);
        }
        true
    }

    /// A process may die after selecting work. Running is not a durable lease;
    /// requeue it at boot so recovery produces redundant work, never a hole.
    pub fn requeue_running(&mut self, now_micros: i64) -> usize {
        self.edit_tasks(
            |task| task.state == TaskState::Running,
            |task| {
                task.state = TaskState::Retry;
                task.deadline_micros = now_micros;
                task.retry_reason = Some("coordinator_restart".to_owned());
            },
        )
    }

    pub fn tasks(&self) -> impl Iterator<Item = &MaintenanceTask> {
        self.snapshot.tasks.iter()
    }

    pub fn state(&self, key: &TaskKey) -> Option<TaskState> {
        self.task(key).map(|task| task.state)
    }

    /// Files this unit's footprint says it reads. `None` for a unit that has never been
    /// claimed, since `record_input` writes the footprint at claim time.
    pub fn input_files(&self, key: &TaskKey) -> Option<u32> {
        self.task(key).and_then(|task| task.input).map(|input| input.files)
    }

    pub fn rollup_slice_complete(&self, source: &str, project_id: &str, target: &str, slice: TimeSlice) -> bool {
        self.snapshot.tasks.iter().any(|task| {
            task.key.source == source
                && task.key.project_id == project_id
                && task.key.physical_table == target
                && task.key.slice == slice
                && matches!(task.key.operation, Operation::BaseRollup | Operation::DerivedRollup)
                && task.state == TaskState::Complete
        })
    }

    pub fn published_rollups(&self, source: &str, target: &str) -> Vec<(TaskKey, Publication)> {
        self.snapshot
            .tasks
            .iter()
            .filter(|task| task.state == TaskState::Complete && task.key.source == source && task.key.physical_table == target)
            .filter_map(|task| task.publication.clone().map(|publication| (task.key.clone(), publication)))
            .collect()
    }

    /// Rows this project has PUBLISHED into `target` over slices overlapping `[start, end)`.
    /// Use instead of day-level `source_rows`, which is keyed on (project, date) and so reads
    /// a genuinely empty hour of a busy day as non-empty.
    pub fn published_rows_overlapping(&self, project_id: &str, target: &str, start: i64, end: i64) -> u64 {
        self.snapshot
            .tasks
            .iter()
            .filter(|task| task.key.project_id == project_id && task.key.physical_table == target && task.key.slice.overlaps(start, end))
            .filter_map(|task| task.publication.as_ref().map(|publication| publication.rows))
            .sum()
    }

    /// Reopen the COMPLETE derived cells built over a base range that has just been
    /// republished, returning how many. A derived cell's witness is the RAW partition, so a
    /// rebuilt base otherwise leaves it serving an aggregate of a base that no longer exists.
    ///
    /// Deliberately NOT `invalidate`: that would mint `Dedup` work, which a rollup rebuild
    /// says nothing about. Mints no task, so it cannot loop.
    pub fn reopen_derived_over(&mut self, project_id: &str, rollup_table: &str, start_micros: i64, end_micros: i64) -> usize {
        self.edit_tasks(
            |task| {
                task.state == TaskState::Complete
                    && task.key.operation == Operation::DerivedRollup
                    && task.key.project_id == project_id
                    && task.key.physical_table == rollup_table
                    && task.key.slice.overlaps(start_micros, end_micros)
            },
            |task| {
                task.state = TaskState::Pending;
                task.retry_reason = None;
                // `Publication` is what coverage is recovered from at boot; leaving it would
                // have the next process re-adopt the cell being replaced.
                task.publication = None;
            },
        )
    }

    pub fn source_cursor(&self, source: &str) -> Option<u64> {
        self.snapshot.source_cursors.get(source).copied()
    }

    pub fn set_source_cursor(&mut self, source: String, delta_version: u64) {
        let cursor = self.snapshot.source_cursors.entry(source.clone()).or_default();
        if delta_version > *cursor {
            *cursor = delta_version;
            self.dirty_cursors.insert(source);
        }
    }

    /// Append the pending journal records and fsync them. Called synchronously from tasks on
    /// the shared runtime, so the blocking `fsync` must go through
    /// `without_blocking_the_worker`; the mutex stays held across it for durability ordering.
    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.wal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !self.dirty_tasks.is_empty() || !self.dirty_cursors.is_empty() || !self.removed_tasks.is_empty() {
            let Self { wal_path, dirty_tasks, dirty_cursors, removed_tasks, task_indices, snapshot, .. } = self;
            let (task_indices, snapshot) = (&*task_indices, &*snapshot);
            let mut wal = OpenOptions::new().create(true).append(true).open(&*wal_path)?;
            let durable = |key: &TaskKey| !is_derived_operation(key.operation);
            let records = dirty_tasks
                .drain()
                .filter(durable)
                .filter_map(|key| task_indices.get(&key).map(|&index| JournalRecord::Task(Box::new(snapshot.tasks[index].clone()))))
                .chain(dirty_cursors.drain().filter_map(|source| {
                    (snapshot.source_cursors.get(&source).copied()).map(|delta_version| JournalRecord::SourceCursor { source, delta_version })
                }))
                // AFTER the upserts, so a key removed and re-created in the same window keeps
                // the re-creation.
                .chain(removed_tasks.drain().filter(durable).map(JournalRecord::Removed))
                .try_fold(Vec::new(), |mut records, record| {
                    serde_json::to_writer(&mut records, &record)?;
                    records.push(b'\n');
                    anyhow::Ok(records)
                })?;
            crate::support::without_blocking_the_worker(|| {
                wal.write_all(&records)?;
                wal.sync_all()
            })?;
        }
        if fs::metadata(&self.wal_path).is_ok_and(|metadata| metadata.len() >= JOURNAL_COMPACT_BYTES) {
            self.compact()?;
        }
        self.publish_statistics_throttled();
        Ok(())
    }

    /// How often `checkpoint` may recompute the maintenance gauges.
    const STATS_PUBLISH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    /// `publish_statistics`, but at most once per [`Self::STATS_PUBLISH_INTERVAL`] — it is a
    /// full scan of `snapshot.tasks` under the global journal mutex, costed by journal
    /// HISTORY rather than load. Call `publish_statistics` directly for an exact read now.
    fn publish_statistics_throttled(&mut self) {
        let now = std::time::Instant::now();
        if self.stats_published_at.is_some_and(|last| now.duration_since(last) < Self::STATS_PUBLISH_INTERVAL) {
            return;
        }
        self.stats_published_at = Some(now);
        self.publish_statistics();
    }

    /// Drop FINISHED tasks whose slice is past the abandonment horizon, which otherwise
    /// accumulate forever and are re-serialized by every `compact`. Pending work past the
    /// horizon stays — `beyond_horizon_tasks` is how the abandoned debt is sized.
    ///
    /// Goes through `retain_tasks` so each drop leaves a tombstone and survives a restart.
    pub fn prune_retired_history(&mut self, now_micros: i64) -> usize {
        // The claimable set is permissive — it keeps whatever was mutated — so it
        // is re-derived on this same minute-scale sweep. Without it the set drifts
        // toward the journal's full size and the claim scan slowly gets its cost
        // back.
        self.rebuild_claimable();
        crate::observability::maintenance_stats().claimable_tasks.store(self.claimable.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let dropped = self.retain_tasks(|task| {
            !matches!(task.state, TaskState::Complete | TaskState::Superseded)
                || now_micros.saturating_sub(task.key.slice.end_micros) <= STARVATION_HORIZON_MICROS
        });
        if dropped != 0 {
            crate::observability::maintenance_stats().journal_retired_tasks_pruned.fetch_add(dropped as u64, std::sync::atomic::Ordering::Relaxed);
        }
        dropped
    }

    pub fn compact(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Derived work is left out of the authoritative snapshot: a reload starts with none
        // of it and `plan_compaction_debt` re-derives it.
        let durable = Snapshot {
            version: self.snapshot.version,
            tasks: self.snapshot.tasks.iter().filter(|task| !is_derived_operation(task.key.operation)).cloned().collect(),
            source_cursors: self.snapshot.source_cursors.clone(),
        };
        // Serialize AND write off the worker: the `to_vec` over every live task is as costly
        // as the fsync, and both hold the journal mutex.
        crate::support::without_blocking_the_worker(|| -> anyhow::Result<()> {
            let bytes = serde_json::to_vec(&durable)?;
            crate::write::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
            let wal = OpenOptions::new().create(true).write(true).truncate(true).open(&self.wal_path)?;
            wal.sync_all()?;
            Ok(())
        })?;
        self.dirty_tasks.clear();
        self.removed_tasks.clear();
        self.dirty_cursors.clear();
        Ok(())
    }

    pub fn publish_statistics(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        stats.journal_stats_publishes.fetch_add(1, Relaxed);
        let mut counts = [0u64; 5];
        let mut backlog_bytes = 0u64;
        let mut sealed_debt_bytes = 0u64;
        let mut oldest_created = u64::MAX;
        let mut beyond_horizon = 0u64;
        let mut latest_frontier_rollup: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
        let mut per_operation = [0u64; <Operation as strum::EnumCount>::COUNT];
        let (mut eligible_base_rollup, mut eligible_sealed) = (0u64, 0u64);
        let now_micros = crate::support::now_micros();
        for task in &self.snapshot.tasks {
            counts[task.state as usize] = counts[task.state as usize].saturating_add(1);
            if task.state.is_active() {
                backlog_bytes = backlog_bytes.saturating_add(task.estimated_decoded_bytes);
                // The age gauge covers only work the scheduler still intends to do; past the
                // horizon a task is abandoned and counted separately.
                if now_micros.saturating_sub(task.key.slice.end_micros) > STARVATION_HORIZON_MICROS {
                    beyond_horizon = beyond_horizon.saturating_add(1);
                } else {
                    oldest_created = oldest_created.min(task.created_unix_ms);
                }
                if task.key.operation == Operation::SealedConsolidation {
                    sealed_debt_bytes = sealed_debt_bytes.saturating_add(task.estimated_decoded_bytes);
                }
                // Per-operation split, plus what is claimable now: when coverage stalls, is
                // the work absent, ineligible, or out-competed?
                per_operation[task.key.operation as usize] = per_operation[task.key.operation as usize].saturating_add(1);
                if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
                    if task.key.operation == Operation::BaseRollup {
                        eligible_base_rollup = eligible_base_rollup.saturating_add(1);
                    }
                    if !is_frontier_task(task, now_micros) {
                        eligible_sealed = eligible_sealed.saturating_add(1);
                    }
                }
            }
            track_latest_frontier_rollup(&mut latest_frontier_rollup, task, now_micros);
        }
        stats.pending_dedup.store(per_operation[Operation::Dedup as usize], Relaxed);
        stats.pending_base_rollup.store(per_operation[Operation::BaseRollup as usize], Relaxed);
        stats.pending_derived_rollup.store(per_operation[Operation::DerivedRollup as usize], Relaxed);
        stats.pending_hot_packing.store(per_operation[Operation::HotPacking as usize], Relaxed);
        stats.pending_sealed_consolidation.store(per_operation[Operation::SealedConsolidation as usize], Relaxed);
        stats.pending_repair.store(per_operation[Operation::Repair as usize], Relaxed);
        stats.eligible_base_rollup.store(eligible_base_rollup, Relaxed);
        stats.eligible_sealed_total.store(eligible_sealed, Relaxed);
        stats.maintenance_tasks_pending.store(counts[0], Relaxed);
        stats.maintenance_tasks_running.store(counts[1], Relaxed);
        stats.maintenance_tasks_retry.store(counts[2], Relaxed);
        stats.maintenance_tasks_complete.store(counts[TaskState::Complete as usize].saturating_add(counts[TaskState::Superseded as usize]), Relaxed);
        stats.maintenance_backlog_bytes.store(backlog_bytes, Relaxed);
        stats.sealed_compaction_debt_bytes.store(sealed_debt_bytes, Relaxed);
        let eligible_lag_secs = frontier_lag_secs(latest_frontier_rollup.values().copied(), now_micros);
        stats.maintenance_eligible_watermark_lag_secs.store(eligible_lag_secs, Relaxed);
        self.frontier_lag_secs.store(eligible_lag_secs, Relaxed);
        stats
            .maintenance_raw_tail_duration_secs
            .store(u64::try_from(FINALIZATION_DELAY_MICROS / 1_000_000).unwrap_or_default().saturating_add(eligible_lag_secs), Relaxed);
        let processed = stats.maintenance_processed_bytes.load(Relaxed);
        let mut sample = lock(THROUGHPUT_SAMPLE.get_or_init(|| Mutex::new((now_micros, processed))));
        let elapsed = now_micros.saturating_sub(sample.0);
        if elapsed >= 1_000_000 {
            let rate = processed.saturating_sub(sample.1).saturating_mul(1_000_000) / u64::try_from(elapsed).unwrap_or(u64::MAX).max(1);
            stats.maintenance_processed_bytes_per_sec.store(rate, Relaxed);
            *sample = (now_micros, processed);
        }
        let now = u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default();
        let oldest_age_secs = if oldest_created != u64::MAX { now.saturating_sub(oldest_created) / 1_000 } else { 0 };
        stats.maintenance_oldest_task_age_secs.store(oldest_age_secs, Relaxed);
        stats.maintenance_beyond_horizon_tasks.store(beyond_horizon, Relaxed);
    }
}

impl TaskState {
    /// Work the scheduler still intends to do. `Superseded` is terminal too: it is
    /// what a split leaves on a parent, and it is never claimable.
    pub(crate) fn is_active(self) -> bool {
        !matches!(self, Self::Complete | Self::Superseded)
    }
}

/// The claim-order tuple `claim_next` minimises: see `TaskJournal::rank`.
type Rank = (u8, u8, u8, u8, u8, i64, i64, i64);

// How long past SEALING a partition may carry debt before it is overdue. Raising this does
// NOT protect the query window — `starved` beats non-starved outright, so a higher threshold
// evicts the window from the privileged lane. Bound the claim share instead (see `claim_next`).
const STARVATION_MICROS: i64 = 3 * DAY_MICROS;
/// The window dashboards read, and therefore the window maintenance has to keep clean.
const QUERY_WINDOW_MICROS: i64 = 14 * DAY_MICROS;
/// Upper bound on starvation escalation: without it every sealed partition older than a day
/// qualifies, and a flag everything sets sorts nothing. Beyond it a task gains one step per
/// further day (the graded term in `scheduling_class`) rather than hitting a hard cut-off,
/// which would make the far tail unreachable.
pub(crate) const STARVATION_HORIZON_MICROS: i64 = 31 * DAY_MICROS;

fn scheduling_class(task: &MaintenanceTask, now_micros: i64) -> (u8, u8, i64, i64, i64) {
    if is_frontier_task(task, now_micros) {
        // Smaller tuples run first, so negating makes the newest minute the most urgent while
        // keeping all projects in that minute deadline-equivalent.
        (0, 0, 0, 0, -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
    } else {
        // Newest slice first, but WIDTH outranks recency: a day-sized unit is the only kind
        // that advances the horizon, and a freshly sealed day carries ~144 ten-minute units
        // per project per tier.
        //
        // A SEALED task's age is how long its DATA has been sealed, NOT how long its record
        // has existed — records are re-created constantly, so a record birthday makes the
        // threshold unreachable and starves coarsened output.
        let waited = now_micros.saturating_sub(task.key.slice.end_micros);
        // The horizon is a SLOPE, not a cliff: below the floor is worst, the whole
        // [floor, horizon] band ties, and each further DAY past it is one step better.
        // Graded in DAYS because `claim_next` matches the winning tuple EXACTLY — a
        // continuous key would make one unit the sole winner and defeat `fair_cursors`.
        let starved = if waited < STARVATION_MICROS {
            u8::MAX
        } else {
            (u8::MAX - 1).saturating_sub(u8::try_from(waited.saturating_sub(STARVATION_HORIZON_MICROS).max(0) / DAY_MICROS).unwrap_or(u8::MAX))
        };
        // Starved work drains OLDEST-first; fresh work stays newest-first. Both halves are
        // needed: escalating a task but still ordering it by recency changes nothing.
        let recency = task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS);
        // File hygiene ranks by BENEFIT, not date: every hygiene unit is day-wide, so
        // `-width` is constant among them. Zero files orders LAST — "benefit unknown".
        let benefit = match task.key.operation {
            // Bucketed for the same reason recency is: an exact tuple match plus a raw file
            // count would make one cell the sole winner of every claim.
            Operation::SealedConsolidation | Operation::HotPacking | Operation::Repair => {
                -i64::from(task.input.map_or(0, |input| input.files) / BENEFIT_BUCKET_FILES)
            }
            _ => 0,
        };
        // Keyed on the AGE, not on `starved`: the graded term is 254 in-band, not 0, so
        // testing the rank value here would flip the whole backlog to newest-first. Width
        // goes through `scheduling_width` so splitting does not demote a unit's children.
        (1, starved, -task.scheduling_width(), benefit, if waited >= STARVATION_MICROS { recency } else { -recency })
    }
}

/// Does this task make its (project, date, tier) cell ineligible for re-planning?
///
/// Only work that will actually RUN may veto: `Superseded` is never claimable, so letting it
/// block would mark every split day "already queued" permanently. Only the operations a
/// backfill enqueues count — unrelated file debt must not veto.
pub fn blocks_rollup_backfill(task: &MaintenanceTask) -> bool {
    matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup) && task.state.is_active()
}

/// The 2^attempts-seconds backoff both failure paths must agree on, capped at 256s.
fn exponential_backoff_micros(attempts: u32) -> i64 {
    i64::try_from((1u64 << attempts.min(8)).saturating_mul(1_000_000)).unwrap_or(i64::MAX)
}

fn is_live_frontier(slice: TimeSlice, now_micros: i64) -> bool {
    slice.end_micros >= now_micros.saturating_sub(LIVE_FRONTIER_WINDOW_MICROS) && slice.start_micros <= now_micros
}

/// Whether a TASK is live-frontier work — not the same question as whether its slice is
/// inside the frontier window. `SealedConsolidation` never is, whatever its slice says, since
/// `is_live_frontier` stays true for 24 h after a slice ENDS and class is STRICT priority.
fn is_frontier_task(task: &MaintenanceTask, now_micros: i64) -> bool {
    task.key.operation != Operation::SealedConsolidation && is_live_frontier(task.key.slice, now_micros)
}

/// A task's slice as a calendar date, `None` if the start timestamp is out of `chrono`'s range.
fn task_date(task: &MaintenanceTask) -> Option<String> {
    chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map(|time| time.date_naive().to_string())
}

fn track_latest_frontier_rollup<'a>(latest: &mut HashMap<(&'a str, &'a str, &'a str), &'a MaintenanceTask>, task: &'a MaintenanceTask, now_micros: i64) {
    if task.key.operation != Operation::BaseRollup || !is_live_frontier(task.key.slice, now_micros) {
        return;
    }
    let stream = (task.key.source.as_str(), task.key.project_id.as_str(), task.key.physical_table.as_str());
    let rank = |task: &MaintenanceTask| (task.key.slice.end_micros, task.state.is_active(), task.deadline_micros);
    if latest.get(&stream).is_none_or(|current| rank(task) > rank(current)) {
        latest.insert(stream, task);
    }
}

fn frontier_lag_secs<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> u64 {
    tasks
        .into_iter()
        .filter(|task| task.state.is_active() && task.deadline_micros <= now_micros)
        .map(|task| u64::try_from(now_micros.saturating_sub(task.deadline_micros).div_euclid(1_000_000)).unwrap_or_default())
        .max()
        .unwrap_or_default()
}

/// Frontier lag over the latest base-rollup task of each stream.
#[cfg(test)]
fn live_frontier_lag_secs<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> u64 {
    let mut latest: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
    for task in tasks {
        track_latest_frontier_rollup(&mut latest, task, now_micros);
    }
    frontier_lag_secs(latest.values().copied(), now_micros)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Resources {
    pub cpu: u32,
    pub decoded_bytes: u64,
    pub object_reads: u32,
    pub object_writes: u32,
}

/// Apply one integer method fieldwise; the field list lives here, not in every op.
macro_rules! resources_zip {
    ($op:ident, $a:expr, $b:expr) => {{
        let (a, b) = ($a, $b);
        Resources {
            cpu: a.cpu.$op(b.cpu),
            decoded_bytes: a.decoded_bytes.$op(b.decoded_bytes),
            object_reads: a.object_reads.$op(b.object_reads),
            object_writes: a.object_writes.$op(b.object_writes),
        }
    }};
}

impl Resources {
    fn fits(self, available: Self) -> bool {
        self.cpu <= available.cpu
            && self.decoded_bytes <= available.decoded_bytes
            && self.object_reads <= available.object_reads
            && self.object_writes <= available.object_writes
    }

    /// Fieldwise subtraction, `None` if any field would underflow — which is
    /// exactly "the request does not fit".
    fn checked_sub(self, request: Self) -> Option<Self> {
        request.fits(self).then(|| resources_zip!(saturating_sub, self, request))
    }

    fn saturating_add(self, released: Self) -> Self {
        resources_zip!(saturating_add, self, released)
    }
}

#[derive(Debug)]
struct AdmissionState {
    capacity: Resources,
    available: Resources,
    /// The static reservation the adaptive ceiling falls back to under load.
    cpu_base: u32,
}

impl AdmissionState {
    fn used(&self) -> Resources {
        resources_zip!(saturating_sub, self.capacity, self.available)
    }
}

/// Non-queuing multi-resource admission. Workers that cannot reserve every
/// resource return to the durable queue instead of sleeping on a semaphore.
#[derive(Clone, Debug)]
pub struct AdmissionController(Arc<Mutex<AdmissionState>>);

/// The largest single unit a pool at this occupancy will admit: the size cap
/// scales linearly with the free fraction, so a busy pool admits only small work.
fn occupancy_scaled_ceiling(available: u64, capacity: u64) -> u64 {
    /// A busy pool must still admit work this small, or hygiene starves.
    const FLOOR: u64 = MAX_DECODED_BYTES / 16;
    /// Free fraction at which the FULL cap is granted, as a divisor: 2 = half
    /// free. It must not be 1: no working pool is ever fully free, so a unit
    /// priced at exactly `MAX_DECODED_BYTES` — which is what the splitter
    /// produces — would then be refused forever.
    const FULL_CAP_AT_FREE_FRACTION: u128 = 2;
    let scaled = (u128::from(MAX_DECODED_BYTES) * FULL_CAP_AT_FREE_FRACTION * u128::from(available) / u128::from(capacity.max(1))) as u64;
    scaled.clamp(FLOOR, MAX_DECODED_BYTES)
}

/// Which lane is asking. Rollups get a reserved share because they lose every
/// race otherwise: compaction and dedup arrive continuously while rollups queue
/// behind them, and prod 2026-09-19 ran 1,690 eligible base-rollup units with
/// `rollup_hits_full_total` at ZERO — not one query served from a full rollup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionLane {
    Rollup,
    Other,
}

/// CPU slots reserved for rollups, which no other lane may take.
///
/// Measured on prod 2026-09-20, hours after the reservation shipped at 2: base
/// rollups ran 6 units per 5 min while `pending_base_rollup` sat at 2198 and
/// GREW (1690 -> 2198). Compaction arrives continuously and saturates the rest
/// of the ceiling, so "reserved" was also the ceiling rollups ever got — 72
/// units/hour, a 30-HOUR drain, slower than new dirty partitions appear.
///
/// Eight leaves other lanes 16 of the 24-slot ceiling, still well above the 10
/// they shared in total before any of this, and takes the drain to ~7 hours.
pub(crate) const ROLLUP_RESERVED_CPU_MAX: u32 = 8;

/// The reservation is a THIRD of the ceiling, capped at `ROLLUP_RESERVED_CPU_MAX`.
/// A flat 8 would leave a small box (ceiling 4) just one slot for every other
/// lane — the reservation has to scale with what there is to reserve from.
pub(crate) fn rollup_reserved_cpu(ceiling: u32) -> u32 {
    (ceiling / 3).min(ROLLUP_RESERVED_CPU_MAX)
}

/// CPU slots admitted at the runtime's current scheduling lag.
///
/// `cores/3` was a static stand-in for "leave CPU for the query path". It is
/// MEASURED now: scheduling lag is how late a 500ms timer actually woke, so it
/// reports runtime starvation directly rather than guessing at it. An idle box
/// gets the full ceiling; a starved one falls back to exactly the static
/// reservation and never below it, so this can only ever admit more work than
/// the constant did, never less.
#[cfg(test)]
pub fn lag_scaled_cpu_ceiling_for_test(base: u32, capacity: u32, lag_ms: u64) -> u32 {
    lag_scaled_cpu_ceiling_inner(base, capacity, lag_ms)
}

fn lag_scaled_cpu_ceiling(base: u32, capacity: u32) -> u32 {
    lag_scaled_cpu_ceiling_inner(base, capacity, crate::observability::runtime_lag_ms().0)
}

fn lag_scaled_cpu_ceiling_inner(base: u32, capacity: u32, lag: u64) -> u32 {
    /// At or under this lag the runtime is not starved and the full ceiling applies.
    const FULL_AT_LAG_MS: u64 = 25;
    /// At or over it, fall back to the static reservation.
    const BASE_AT_LAG_MS: u64 = 250;
    if capacity <= base {
        return base;
    }
    if lag <= FULL_AT_LAG_MS {
        return capacity;
    }
    if lag >= BASE_AT_LAG_MS {
        return base;
    }
    let span = u128::from(BASE_AT_LAG_MS - FULL_AT_LAG_MS);
    let headroom = u128::from(BASE_AT_LAG_MS - lag);
    base + (u128::from(capacity - base) * headroom / span) as u32
}

impl AdmissionController {
    /// `cpu` is the STATIC reservation (the old fixed ceiling) and `cpu_max` the
    /// most an unstarved runtime will admit; the live ceiling moves between them.
    pub fn new(cpu: u32, cgroup_memory_bytes: u64, object_reads: u32, object_writes: u32) -> Self {
        Self::with_cpu_ceiling(cpu, cpu, cgroup_memory_bytes, object_reads, object_writes)
    }

    pub fn with_cpu_ceiling(cpu_base: u32, cpu_max: u32, cgroup_memory_bytes: u64, object_reads: u32, object_writes: u32) -> Self {
        // At most 75% is trackable maintenance decode. The remainder is an
        // unconditional foreground/untracked-allocation reserve.
        let decoded_bytes = cgroup_memory_bytes.saturating_mul(3) / 4;
        let capacity = Resources { cpu: cpu_max.max(cpu_base), decoded_bytes, object_reads, object_writes };
        Self(Arc::new(Mutex::new(AdmissionState { capacity, available: capacity, cpu_base })))
    }

    pub fn try_acquire(&self, request: Resources) -> Option<AdmissionPermit> {
        self.try_acquire_for(request, AdmissionLane::Other)
    }

    pub fn try_acquire_for(&self, request: Resources, lane: AdmissionLane) -> Option<AdmissionPermit> {
        if request.decoded_bytes > MAX_DECODED_BYTES {
            return None;
        }
        let mut state = lock(&self.0);
        if request.decoded_bytes > occupancy_scaled_ceiling(state.available.decoded_bytes, state.capacity.decoded_bytes) {
            return None;
        }
        // CPU is the dimension that actually binds — prod sat at 10 of 10 tokens
        // with ~2,500 units eligible, 3 of 4 rewrite permits idle and the box at
        // half its CPU limit. Scale it by measured starvation, and hold back a
        // slice so rollups cannot be crowded out by continuous compaction work.
        let ceiling = lag_scaled_cpu_ceiling(state.cpu_base, state.capacity.cpu);
        let ceiling = match lane {
            AdmissionLane::Rollup => ceiling,
            AdmissionLane::Other => ceiling.saturating_sub(rollup_reserved_cpu(ceiling)).max(1),
        };
        if state.used().cpu.saturating_add(request.cpu) > ceiling {
            return None;
        }
        // `checked_sub` is `fits` plus the subtraction, so it is the whole gate.
        state.available = state.available.checked_sub(request)?;
        Self::publish_utilization(&state);
        Some(AdmissionPermit { controller: self.clone(), resources: request })
    }

    pub fn utilization(&self) -> Resources {
        lock(&self.0).used()
    }

    fn publish_utilization(state: &AdmissionState) {
        use std::sync::atomic::Ordering::Relaxed;
        let used = state.used();
        let stats = crate::observability::maintenance_stats();
        stats.maintenance_cpu_tokens_used.store(u64::from(used.cpu), Relaxed);
        stats.maintenance_decoded_bytes_used.store(used.decoded_bytes, Relaxed);
        stats.maintenance_object_read_tokens_used.store(u64::from(used.object_reads), Relaxed);
        stats.maintenance_object_write_tokens_used.store(u64::from(used.object_writes), Relaxed);
    }
}

#[must_use]
pub struct AdmissionPermit {
    controller: AdmissionController,
    resources: Resources,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = lock(&self.controller.0);
        state.available = state.available.saturating_add(self.resources);
        debug_assert!(state.available.fits(state.capacity));
        AdmissionController::publish_utilization(&state);
    }
}

#[cfg(test)]
mod tests {
    use tap::Tap;

    use super::*;

    /// The DataFusion `SchemaError` arrives type-erased, so its text is the contract.
    #[test]
    fn schema_failures_are_recognised_from_prod_text() {
        assert!(is_schema_failure("Schema error: No field named duration_digest. Valid fields are __maintenance_slice_input_0.project_id, ..."));
        assert!(is_schema_failure("Error during planning: SchemaError(FieldNotFound { field: Column { name: \"duration_digest\" } })"));
        for benign in ["dedup: Object at location ... not found", "compaction: transaction failed: version 2667 already exists", "resource_admission"] {
            assert!(!is_schema_failure(benign), "must not park a unit that would succeed on a retry: {benign}");
        }
    }

    /// The claim scan walks an INDEX, so the index must never miss a claimable
    /// unit — a miss strands that unit forever, silently. The set is permissive on
    /// purpose (stale entries are filtered on the way past), so this asserts the
    /// direction that actually matters: everything claimable is present.
    #[test]
    fn every_claimable_task_is_in_the_claim_index() {
        let (_dir, mut journal) = new_journal();
        let now = 1_000_000_000;

        // One unit per state the journal can put a task in.
        let pending = upserted(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup));
        let retried = upserted(&mut journal, task("q", 0, DAY_MICROS, Operation::BaseRollup));
        assert!(journal.retry(&retried, "because".to_owned(), now));
        let running = running_unit(&mut journal, task("r", 0, DAY_MICROS, Operation::BaseRollup), 1);
        let done = upserted(&mut journal, task("s", 0, DAY_MICROS, Operation::BaseRollup));
        assert!(journal.complete(&done));

        let derived: std::collections::BTreeSet<_> =
            journal.tasks().filter(|t| matches!(t.state, TaskState::Pending | TaskState::Retry)).map(|t| t.key.clone()).collect();
        assert!(derived.contains(&pending) && derived.contains(&retried), "fixture must produce both claimable states");

        for key in &derived {
            assert!(journal.claimable.contains(key), "claimable unit missing from the index: {key:?}");
        }
        // A unit that was NEVER claimable must never have been added.
        assert!(!journal.claimable.contains(&running), "a unit upserted as Running was never claimable");
        // `done` WAS claimable before it completed, so a stale entry is expected and
        // correct: the set is permissive and sheds on rebuild, because holding too
        // much costs a lookup while holding too little strands a unit.
        assert!(journal.claimable.contains(&done), "the permissive set keeps a completed unit until the next rebuild");
        journal.rebuild_claimable();
        assert!(!journal.claimable.contains(&done), "and the rebuild sheds it");
        for key in &derived {
            assert!(journal.claimable.contains(key), "a rebuild must not drop a claimable unit: {key:?}");
        }
    }

    /// Re-deriving must not change what can be claimed — it only sheds the stale
    /// entries the permissive path accumulates.
    #[test]
    fn rebuilding_the_claim_index_preserves_every_claimable_unit() {
        let (_dir, mut journal) = new_journal();
        let a = upserted(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup));
        let b = upserted(&mut journal, task("q", 0, DAY_MICROS, Operation::Dedup));
        let done = upserted(&mut journal, task("s", 0, DAY_MICROS, Operation::BaseRollup));
        assert!(journal.complete(&done));

        journal.rebuild_claimable();

        assert!(journal.claimable.contains(&a) && journal.claimable.contains(&b), "claimable units must survive a rebuild");
        assert!(!journal.claimable.contains(&done), "a rebuild must shed completed units");
    }

    /// The database restarts a couple of times a day. A unit the process shut
    /// down on BEFORE it ran must come back untouched: `mark_running` already
    /// charged it an attempt, and charging a second one via `abandon_running`
    /// would SPLIT it at two (`SplitTrigger::RepeatedFailure`) and quarantine it
    /// at `QUARANTINE_ATTEMPTS` — punishing a unit that was merely unlucky with
    /// deploys as though it had failed repeatedly.
    #[test]
    fn a_unit_released_before_it_ran_is_not_charged_an_attempt() {
        let (_dir, mut journal) = new_journal();
        let key = running_unit(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup), 1);

        assert!(journal.release_unstarted(&key), "a Running unit must be releasable");

        let unit = journal.tasks().find(|candidate| candidate.key == key).expect("still queued");
        assert_eq!(unit.state, TaskState::Pending, "it must go back to the queue, not to Retry");
        assert_eq!(unit.attempts, 0, "the claim's attempt must be given back, got {}", unit.attempts);
        assert!(unit.retry_reason.is_none(), "a release is not a failure and must not carry a retry reason");
    }

    /// The release is scoped to TEARDOWN. A unit that errored through `?` reaches
    /// `Drop` with no recorded failure and may also have written nothing, so
    /// without the shutdown check it would be refunded its attempt and re-run
    /// immediately — no backoff, a hot loop on a unit that is failing.
    #[test]
    fn a_unit_that_failed_without_shutdown_still_backs_off() {
        let (_dir, mut journal) = new_journal();
        let key = running_unit(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup), 1);
        let now = 1_000_000_000;

        // What `Drop` does when the process is NOT tearing down.
        journal.abandon_running(&key, now, None);

        let unit = journal.tasks().find(|candidate| candidate.key == key).expect("present");
        assert_eq!(unit.state, TaskState::Retry, "a failure must go to Retry, not back to the queue");
        assert!(unit.deadline_micros > now, "and must carry a backoff, got {}", unit.deadline_micros);
    }

    /// Releasing is only correct for a unit that is actually claimed; anything
    /// else means a concurrent state change won the race and must be left alone.
    #[test]
    fn releasing_a_unit_that_is_not_running_changes_nothing() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup));

        assert!(!journal.release_unstarted(&key), "a Pending unit is not ours to release");
        assert_eq!(journal.tasks().find(|c| c.key == key).expect("present").attempts, 0);
    }

    /// A missing column cannot be halved away: every CHILD of a bisection names
    /// it too and fails identically, so both entry points must park instead —
    /// and the park has to survive the 60s planner re-mint.
    #[test_case::test_case(true ; "the worker hands back a plan error")]
    #[test_case::test_case(false ; "a fast-fail retry reason carries one")]
    fn a_deterministic_plan_error_parks_instead_of_shredding_the_slice(via_abandon: bool) {
        const ERROR: &str = "Schema error: No field named duration_digest.";
        let (_dir, mut journal) = new_journal();
        // Day-wide: splittable, so nothing but the guard stops the bisection.
        let key = running_unit(&mut journal, task("p", 0, DAY_MICROS, Operation::BaseRollup), 2);

        let now = 1_000_000_000;
        if via_abandon {
            journal.abandon_running(&key, now, Some(ERROR));
        } else {
            journal.retry_or_split(&key, format!("resolve_input: {ERROR}"), now, 2);
        }

        assert_eq!(journal.tasks().count(), 1, "a deterministic failure must not be bisected into children that fail identically");
        let parked = journal.tasks().find(|candidate| candidate.key == key).expect("requeued").clone();
        assert_eq!(parked.state, TaskState::Retry);
        assert_eq!(parked.retry_reason.as_deref(), Some(TaskJournal::SCHEMA_FAILURE_REASON), "the operator must see a park, not a backoff");
        assert!(TaskJournal::is_quarantined(&parked), "a parked unit is rationed to the quarantine permit, not the whole pool");
        assert!(parked.deadline_micros >= now + 3_600_000_000, "a park waits for a deploy or a rebuild, not for the next tick");

        journal.enqueue(key.clone(), now + 60_000_000, 1_000, 0);
        let after = journal.tasks().find(|candidate| candidate.key == key).expect("still queued");
        assert_eq!(after.deadline_micros, parked.deadline_micros, "the planner tick must not hand an unplannable unit straight back");
        assert_eq!(after.retry_reason.as_deref(), Some(TaskJournal::SCHEMA_FAILURE_REASON));
    }

    /// Abandoning an UNSPLITTABLE unit waits the greater of its exponential
    /// backoff and the operation's own deadline — but only once it has failed
    /// more than once, so a one-off squeeze-out is not penalised. The floor
    /// raises the backoff, it never replaces it.
    #[test_case::test_case(Operation::Repair, 1 => 2 * 1_000_000i64 ; "a_first_abandonment_is_not_floored")]
    #[test_case::test_case(Operation::Repair, 5 => with |delay: i64| assert!(delay >= floor_micros(Operation::Repair), "a repair unit that burned {}s must wait at least that long again, waited {}s", floor_micros(Operation::Repair) / 1_000_000, delay / 1_000_000) ; "a_unit_that_burned_its_deadline_waits_at_least_that_long_again")]
    #[test_case::test_case(Operation::Dedup, 8 => (256 * 1_000_000i64).max(floor_micros(Operation::Dedup)) ; "the_deadline_floor_does_not_cap_exponential_backoff")]
    fn abandoning_an_unsplittable_unit_waits_the_greater_of_backoff_and_deadline(operation: Operation, attempts: u32) -> i64 {
        let (_dir, mut journal) = new_journal();
        // A single-slice unit: `byte_bounded_units` cannot divide it, so the
        // split path declines and the backoff is what bounds the waste.
        let key = running_unit(&mut journal, task("p", 0, 1, operation), attempts);

        let now = 1_000_000_000;
        journal.abandon_running(&key, now, None);
        requeued_deadline(&journal, &key) - now
    }

    /// A fast-fail retry (resource admission) repeats identically at the same
    /// size, never claiming a worker, so neither `abandon_running`'s split nor
    /// its floor fires. A repeated admission failure must bisect instead.
    #[test]
    fn a_repeated_admission_failure_splits_instead_of_hot_looping() {
        let (_dir, mut journal) = new_journal();
        // NOT Repair: its cost is a whole file, so it declines to split by
        // construction (see `a_repair_unit_is_never_bisected_...`). The
        // hot-loop invariant this test guards belongs to every other operation.
        let key = upserted(
            &mut journal,
            task("p", 0, DAY_MICROS, Operation::SealedConsolidation).tap_mut(|unit| {
                unit.attempts = 3;
                unit.estimated_decoded_bytes = 1_100_000_000_000;
            }),
        );

        journal.retry_or_split(&key, "resource_admission".into(), 1_000_000, 3);

        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "an unadmittable unit must split, not requeue whole");
        let children: Vec<_> = journal.tasks().filter(|t| t.key != key && t.state == TaskState::Pending).collect();
        assert_eq!(children.len(), 2, "one bisection: two half-day children");
        assert!(children.iter().all(|t| t.attempts == 0));
    }

    /// A unit whose own estimate equals its parent's MEASUREMENT shed nothing,
    /// so bisecting again cannot help and must be declined — otherwise the
    /// lineage bisects all the way to `MIN_SLICE_MICROS`.
    #[test_case::test_case(true ; "timeout")]
    #[test_case::test_case(false ; "capacity failure")]
    fn a_lineage_that_did_not_shed_is_not_split_again(via_timeout: bool) {
        let (_dir, mut journal) = new_journal();
        const MEASURED: u64 = 8 * 1024 * 1024 * 1024;
        let key = upserted(
            &mut journal,
            task("whale", 0, DAY_MICROS, Operation::BaseRollup).tap_mut(|unit| {
                unit.attempts = 3;
                unit.parent_measured_bytes = Some(MEASURED);
                unit.estimated_decoded_bytes = MEASURED;
            }),
        );

        journal.record_preflight(&key, None, MEASURED);
        if via_timeout {
            journal.abandon_running(&key, 1_000_000, None);
        } else {
            journal.retry_or_split(&key, "resource_admission".into(), 1_000_000, 3);
        }

        assert_eq!(
            journal.state(&key),
            Some(TaskState::Retry),
            "a unit that shed NOTHING against its parent must be retried, not bisected again — \
             splitting it only mints children that each pay the same scan"
        );
        assert_eq!(journal.tasks().filter(|t| t.key != key).count(), 0, "and it must mint no children");
    }

    /// When the split is REFUSED (already at minimum width), a repeated
    /// capacity failure must not keep the caller's transient-tuned delay: a 1s
    /// admission retry on an unfittable unit is a hot loop that increments
    /// attempts every second forever. The delay escalates with the evidence.
    #[test]
    fn a_split_refused_capacity_retry_escalates_its_delay() {
        let (_dir, mut journal) = new_journal();
        // Single MIN_SLICE unit: byte_bounded_units would hash-shard, so the
        // split declines.
        let key = upserted(
            &mut journal,
            task("p", 0, MIN_SLICE_MICROS, Operation::Repair).tap_mut(|unit| {
                unit.attempts = 6;
                unit.estimated_decoded_bytes = 1_100_000_000_000;
            }),
        );

        let now = crate::support::now_micros();
        journal.retry_or_split(&key, "resource_admission".into(), now + 1_000_000, 6);

        let not_before = requeued_deadline(&journal, &key);
        assert!(
            not_before >= now + (1 << 6) * 1_000_000,
            "sixth identical capacity failure must wait 2^6s, not the 1s admission delay (waited {}s)",
            (not_before - now) / 1_000_000
        );
    }

    /// The planner re-derives file debt every 60s and enqueues the same day-wide
    /// key while a partition stays out of policy. That re-mint must not resurrect
    /// a parent `split_time_task` superseded, or its live children never start.
    #[test]
    fn a_replanned_day_does_not_resurrect_a_superseded_parent() {
        let (_dir, mut journal) = new_journal();
        let key = running_unit(&mut journal, task("p", 0, DAY_MICROS, Operation::SealedConsolidation), 2);

        journal.abandon_running(&key, 0, None);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "two failures split a day-wide unit");
        let children: Vec<_> = journal.tasks().filter(|child| child.key != key).map(|child| child.key.clone()).collect();
        assert_eq!(children.len(), 2, "bisection leaves two live children");

        // The 60s planner tick re-mints the day key while children are live.
        journal.enqueue(key.clone(), 60_000_000, 1_000, 0);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "the children carry the work; the parent must stay down");
        assert!(!journal.mark_running(&key), "a superseded parent is not claimable");

        // Once every child is done, new debt on the day is new work: recreate fresh.
        for child in &children {
            journal.complete(child);
        }
        journal.enqueue(key.clone(), 120_000_000, 1_000, 0);
        let revived = journal.tasks().find(|candidate| candidate.key == key).expect("revived");
        assert_eq!(revived.state, TaskState::Pending, "a childless superseded key revives for fresh debt");
        assert_eq!(revived.attempts, 0, "revival is a new unit, not attempt 205 of the old one");
    }

    /// The same 60s re-mint must not erase what `abandon_running` recorded: its
    /// deadline floor is the only bound on a doomed unit's duty cycle, and its
    /// `worker_error` reason is what routes the unit through the small quarantine
    /// permit.
    #[test]
    fn a_replanned_debt_does_not_erase_worker_failure_backoff() {
        let (_dir, mut journal) = new_journal();
        // Single-slice: unsplittable, so abandonment falls to the backoff floor.
        let key = running_unit(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::Repair), 5);

        let now = 1_000_000_000;
        journal.abandon_running(&key, now, None);
        let floored = requeued_deadline(&journal, &key);

        journal.enqueue(key.clone(), now + 60_000_000, 1_000, 0);
        let after = journal.tasks().find(|candidate| candidate.key == key).expect("still queued");
        assert_eq!(after.deadline_micros, floored, "a re-noticed debt must not cancel the abandonment backoff");
        assert_eq!(after.retry_reason.as_deref(), Some(TaskJournal::WORKER_FAILURE_REASON), "the quarantine tag survives the planner tick");
    }

    /// Splitting a backfill unit must narrow the WORK, not the priority.
    ///
    /// Sealed ordering ranks wide units first because width PROXIES backfill
    /// provenance: a day-sized unit comes from the backfill planner and is the
    /// only kind that advances the horizon, while a ten-minute one is what the
    /// live path mints by the hundred. `split_time_task` breaks that proxy, so a
    /// child must keep its parent's width term or rank below every day-wide unit
    /// in history. Asserted on the width term alone, at one seal time, so the
    /// starvation slope does not confound it.
    #[test]
    fn a_split_backfill_child_keeps_its_parents_scheduling_width() {
        let now = 60 * DAY_MICROS;
        let (start, end) = (now - 10 * DAY_MICROS, now - 9 * DAY_MICROS);
        let (_dir, mut journal) = new_journal();

        let key = upserted(&mut journal, task("split", start, end, Operation::BaseRollup));
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None), "the day unit splits");

        // The same day, same seal time, never split — what the children must not
        // be demoted below.
        let peer = task("peer", start, end, Operation::BaseRollup);
        let peer_width = scheduling_class(&peer, now).2;

        let child = journal.tasks().find(|task| task.key != key && task.key.project_id == "split").expect("a split child");
        assert!(child.key.slice.width() < end - start, "the child really is narrower: {}", child.key.slice.width());
        assert_eq!(scheduling_class(child, now).2, peer_width, "a split child must rank at its PARENT's width, not its own {}", child.key.slice.width());
    }

    /// The query-window reservation must not preempt the sealed one (any phase
    /// that is a multiple of 2 or 4 steals sealed turns) and must not change what
    /// the first claims hand out.
    #[test]
    fn the_window_reservation_composes_with_the_sealed_one() {
        // Sealed fires on multiples of 2, or of 4 while the frontier is behind.
        for tick in 1u64..=64 {
            let window_turn = tick % 4 == 3;
            if window_turn {
                assert!(!tick.is_multiple_of(2), "tick {tick} would steal a sealed turn");
                assert!(!tick.is_multiple_of(4), "tick {tick} would steal a frontier-behind sealed turn");
            }
        }
        // `claim_tick` is incremented BEFORE use, so the first claim sees 1.
        assert!(![1u64, 2].into_iter().any(|first| first % 4 == 3), "the reservation must not take the first claims");
        // And it is bounded: one claim in four, so three in four still drain the
        // backlog in the existing order.
        assert_eq!((1u64..=64).filter(|tick| tick % 4 == 3).count(), 16, "one claim in four, no more");
    }

    /// Months-old history OUTRANKS the dates dashboards read: `starved` is
    /// `u8::MAX` when NOT starved, so unstarved recent days lose to everything in
    /// the starved lane. Raising `STARVATION_MICROS` makes it worse, not better;
    /// this pins the shape a future fix has to move.
    #[test]
    fn months_old_history_outranks_the_dates_dashboards_read() {
        let now = 400 * DAY_MICROS;
        let rank = |days_ago: i64| {
            let end = now - days_ago * DAY_MICROS;
            scheduling_class(&task("p", end - DAY_MICROS, end, Operation::Dedup), now)
        };
        let (day2, day4, day10, day90) = (rank(2), rank(4), rank(10), rank(90));

        // Only the first three days of a 14-day window escape the starved lane.
        assert_eq!(day2.1, u8::MAX, "day 2 is inside the 3d floor, so it is NOT starved");
        assert!(day4.1 < u8::MAX, "day 4 already IS starved");
        assert!(day10.1 < u8::MAX, "and so is day 10");

        // `starved` is compared before every other term, and smaller wins — so
        // the further past the horizon, the better the rank.
        assert!(day90 < day4, "months-old history outranks a date the dashboard reads");
        assert!(day90 < day10, "and outranks the middle of the window");
        // And the unstarved days lose to ALL of it: u8::MAX is the worst value,
        // which is why raising the threshold cannot be the fix.
        assert!(day4 < day2, "an in-window starved date still outranks an unstarved newer one");
    }

    /// The contiguity term: a pending Dedup slice that EXTENDS a completed run
    /// outranks one that would seed a new island — certification grants a
    /// (project, date) only when the merged intervals cover the WHOLE day.
    #[test]
    fn a_dedup_slice_extending_a_completed_run_outranks_an_island_seed() {
        const TEN_MIN: i64 = 10 * 60 * 1_000_000;
        let (_dir, mut journal) = new_journal();
        let now = 400 * DAY_MICROS;
        // Sealed and inside the [3d, 31d] starved band, where the whole band
        // TIES on `starved`.
        let day = now - 10 * DAY_MICROS;
        journal.upsert(task("p", day + 2 * TEN_MIN, day + 3 * TEN_MIN, Operation::Dedup).tap_mut(|done| done.state = TaskState::Complete));

        // The island seed carries the OLDER end; the extender shares a boundary
        // with the completed run.
        let seed = task("p", day, day + TEN_MIN, Operation::Dedup);
        let extend = task("p", day + 3 * TEN_MIN, day + 4 * TEN_MIN, Operation::Dedup);
        assert!(journal.rank(&extend, now) < journal.rank(&seed, now), "the run-extending slice must outrank the island seed");

        // With no adjacent candidate in play the established order is
        // untouched: oldest-first still decides among non-adjacent slices.
        let later_island = task("p", day + 5 * TEN_MIN, day + 6 * TEN_MIN, Operation::Dedup);
        assert!(journal.rank(&seed, now) < journal.rank(&later_island, now), "non-adjacent slices keep the oldest-first order");

        // Another project's completed run must not vouch for this one: same
        // slice as `extend`, different project, and the adjacency slot reads 1.
        let other_project = task("q", day + 3 * TEN_MIN, day + 4 * TEN_MIN, Operation::Dedup);
        assert_eq!(journal.rank(&other_project, now).4, 1, "adjacency is per (project, source)");
        assert_eq!(journal.rank(&extend, now).4, 0, "the extender is adjacent in its own project");
    }

    fn task(project: &str, start: i64, end: i64, operation: Operation) -> MaintenanceTask {
        task_in("table", project, start, end, operation)
    }

    /// `task`, with the physical table named — a few assertions turn on the tier.
    fn task_in(table: &str, project: &str, start: i64, end: i64, operation: Operation) -> MaintenanceTask {
        let key = TaskKey {
            physical_table: table.into(),
            source: "source".into(),
            project_id: project.into(),
            slice: TimeSlice::new(start, end).expect("valid slice"),
            operation,
        };
        MaintenanceTask::pending(key, 0, 0, 0)
    }

    /// A fresh journal over a temp dir; the dir is returned because several tests
    /// reload the journal from it.
    fn new_journal() -> (tempfile::TempDir, TaskJournal) {
        let dir = tempfile::tempdir().expect("temp dir");
        let journal = TaskJournal::load(dir.path()).expect("journal");
        (dir, journal)
    }

    /// Upserts a unit and hands back its key — the clone-then-upsert every
    /// fixture below repeats. Field overrides ride in on `tap_mut`.
    fn upserted(journal: &mut TaskJournal, unit: MaintenanceTask) -> TaskKey {
        let key = unit.key.clone();
        journal.upsert(unit);
        key
    }

    /// Upserts a unit that has already failed `attempts` times and is Running.
    fn running_unit(journal: &mut TaskJournal, unit: MaintenanceTask, attempts: u32) -> TaskKey {
        upserted(journal, unit.tap_mut(|unit| unit.attempts = attempts).tap_mut(|unit| unit.state = TaskState::Running))
    }

    fn requeued_deadline(journal: &TaskJournal, key: &TaskKey) -> i64 {
        journal.tasks().find(|candidate| candidate.key == *key).map(|candidate| candidate.deadline_micros).expect("requeued")
    }

    /// The journal WAL's size on disk — what the idempotence tests compare.
    fn wal_len(journal: &TaskJournal) -> u64 {
        fs::metadata(&journal.wal_path).expect("wal").len()
    }

    /// One invalidation of `[start, end)` on project "p", as ingest emits it.
    fn invalidation(rollup_table: &'static str, start_micros: i64, end_micros: i64, observed_at_micros: i64, derived: bool) -> Invalidation<'static> {
        Invalidation {
            source_table: "source",
            rollup_table,
            source: "source",
            project_id: "p",
            start_micros,
            end_micros,
            observed_at_micros,
            derived,
            mint_dedup: true,
            mint_rollup: true,
        }
    }

    /// The operation's own deadline, in micros — the floor a burned unit waits.
    fn floor_micros(operation: Operation) -> i64 {
        i64::try_from(operation_deadline_secs(operation) * 1_000_000).expect("fits")
    }

    /// A 12h child of a day-wide parent that measured 100x the budget: the model
    /// promised half the bytes, the next measurement comes back with 96% of them.
    fn floored_child(journal: &mut TaskJournal) -> TaskKey {
        upserted(journal, task("whale", 0, DAY_MICROS / 2, Operation::BaseRollup).tap_mut(|unit| unit.parent_measured_bytes = Some(100 * MAX_DECODED_BYTES)))
    }

    /// The reference clock for the ordering cases: ten days in.
    const ORDER_NOW: i64 = 10 * 24 * 60 * 60 * 1_000_000;

    /// (slice start, slice end, operation, deadline) — all offsets from `ORDER_NOW`.
    type Ranked = (i64, i64, Operation, Option<i64>);

    /// (project, slice start, slice end, deadline, completed) — offsets from `ORDER_NOW`.
    type Stream = (&'static str, i64, i64, i64, bool);

    /// `checkpoint` must not rescan the whole journal every time it is called:
    /// `publish_statistics` is O(tasks) and runs under the global journal mutex.
    /// Asserted on the scan COUNT, not a duration, to avoid a timing flake.
    #[test]
    fn checkpoint_does_not_rescan_the_journal_on_every_call() {
        use std::sync::atomic::Ordering::Relaxed;
        let (_dir, mut journal) = new_journal();
        let publishes = || crate::observability::maintenance_stats().journal_stats_publishes.load(Relaxed);

        // The first checkpoint must publish: a process that never checkpointed
        // twice would otherwise export nothing at all.
        journal.upsert(task("p", 0, 1, Operation::BaseRollup));
        journal.checkpoint().expect("checkpoint");
        let after_first = publishes();

        for i in 1..40i64 {
            journal.upsert(task("p", i, i + 1, Operation::BaseRollup));
            journal.checkpoint().expect("checkpoint");
        }
        assert_eq!(publishes(), after_first, "39 further checkpoints inside one second must not rescan the journal 39 more times");

        // Throttled, not disabled — the gauges still describe the journal.
        journal.publish_statistics();
        assert_eq!(publishes(), after_first + 1);
        assert_eq!(
            crate::observability::maintenance_stats().pending_base_rollup.load(Relaxed),
            40,
            "an explicit publish must still report the true pending count"
        );
    }

    /// The hygiene pass must shed FINISHED work past the abandonment horizon,
    /// keep everything else, and make the drop survive a restart. Three things
    /// must NOT be pruned: pending and retrying work past the horizon (that is
    /// the abandoned-debt gauge), and finished work inside it.
    #[test]
    fn hygiene_sheds_finished_work_past_the_horizon_and_nothing_else() {
        let (dir, mut journal) = new_journal();
        let now = 400 * DAY_MICROS;
        let at = |days_ago: i64| {
            let end = now - days_ago * DAY_MICROS;
            (end - DAY_MICROS, end)
        };
        let mut add = |name: &str, days_ago: i64, state: TaskState| {
            let (start, end) = at(days_ago);
            journal.upsert(task(name, start, end, Operation::BaseRollup).tap_mut(|t| t.state = state));
        };
        add("old-complete", 40, TaskState::Complete);
        add("old-superseded", 40, TaskState::Superseded);
        add("old-pending", 40, TaskState::Pending);
        add("old-retry", 40, TaskState::Retry);
        add("fresh-complete", 2, TaskState::Complete);
        add("fresh-pending", 2, TaskState::Pending);
        // Persist FIRST, or the durability assertion at the end is vacuous: an
        // unpersisted task cannot resurrect, so the tombstones would never be
        // what kept it gone.
        journal.checkpoint().expect("persist the whole set before pruning any of it");

        assert_eq!(journal.prune_retired_history(now), 2, "exactly the two finished-and-past-the-horizon tasks");
        let survivors: Vec<&str> = journal.tasks().map(|t| t.key.project_id.as_str()).collect();
        assert!(!survivors.contains(&"old-complete") && !survivors.contains(&"old-superseded"));
        assert!(
            survivors.contains(&"old-pending") && survivors.contains(&"old-retry"),
            "abandoned work past the horizon is the debt gauge and must survive: {survivors:?}"
        );
        assert!(survivors.contains(&"fresh-complete") && survivors.contains(&"fresh-pending"), "work inside the horizon must survive: {survivors:?}");

        // The index must still find what is left, or every later lookup silently
        // misses — the failure a bare `retain` on the vector would have caused.
        for (name, days_ago, state) in [("old-pending", 40, TaskState::Pending), ("fresh-complete", 2, TaskState::Complete)] {
            let (start, end) = at(days_ago);
            assert_eq!(journal.state(&task(name, start, end, Operation::BaseRollup).key), Some(state), "{name} must still be indexed");
        }
        assert_eq!(journal.prune_retired_history(now), 0, "a second pass has nothing left to drop");

        // The drop must be DURABLE on the cheap append: going through
        // `retain_tasks` is what leaves the `Removed` tombstone, without which
        // the next restart undoes the prune.
        journal.checkpoint().expect("checkpoint the tombstones");
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let names: Vec<&str> = reloaded.tasks().map(|t| t.key.project_id.as_str()).collect();
        assert!(!names.contains(&"old-complete") && !names.contains(&"old-superseded"), "pruned tasks must not resurrect after a restart: {names:?}");
        assert_eq!(names.len(), 4, "and nothing else may vanish with them: {names:?}");
    }

    /// Only outstanding ROLLUP work may veto a rollup backfill — unrelated file
    /// debt on the same day must not disqualify it.
    #[test]
    fn only_outstanding_rollup_work_blocks_a_backfill() {
        for operation in [Operation::SealedConsolidation, Operation::HotPacking, Operation::Repair] {
            let debt = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(!blocks_rollup_backfill(&debt), "{operation:?} is file debt, not rollup coverage; it must not veto a backfill");
        }
        for operation in [Operation::Dedup, Operation::BaseRollup, Operation::DerivedRollup] {
            let outstanding = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(blocks_rollup_backfill(&outstanding), "{operation:?} is the work a backfill would queue; re-queueing it pushes its deadline out");
            let done = outstanding.clone().tap_mut(|t| t.state = TaskState::Complete);
            assert!(!blocks_rollup_backfill(&done), "a completed {operation:?} leaves the day open to backfill again");
            // A SUPERSEDED parent is what `split_time_task` leaves behind and is
            // never claimable, so it must not veto — its CHILDREN are Pending and
            // still veto on their own.
            let split = outstanding.tap_mut(|t| t.state = TaskState::Superseded);
            assert!(!blocks_rollup_backfill(&split), "a superseded {operation:?} is unclaimable, so it must not veto the backfill that would replace it");
        }
    }

    /// One call halves ONCE: the next level is minted only after its own
    /// preflight has measured it, never priced by time share from this one.
    #[test]
    fn halves_a_whale_once_and_hash_shards_at_the_floor() {
        // A 10-minute slice is Dedup's floor, so the halving property is
        // asserted on an operation that still bisects there.
        let input = task("whale", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let units = byte_bounded_units(&input, 10 * MAX_DECODED_BYTES);
        assert_eq!(units.len(), 2, "one level per measurement, not a subtree");
        assert_eq!(units.iter().map(|unit| unit.estimated_decoded_bytes).sum::<u64>(), 10 * MAX_DECODED_BYTES, "the halves must price the whole parent");
        assert!(units.iter().all(|unit| unit.key.slice.width() == NORMAL_SLICE_MICROS / 2 && unit.hash_shards <= 1));
        assert_eq!(units[0].key.slice.end_micros, units[1].key.slice.start_micros, "and they must tile the parent");

        let minute = task("whale", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let shards = byte_bounded_units(&minute, MAX_DECODED_BYTES * 3);
        assert_eq!(shards.len(), 3);
        assert!(shards.iter().all(|unit| unit.hash_shards == 3));
    }

    /// Bisection prices children by TIME SHARE, so it always "fits" on paper,
    /// but real cost floors out — a slice reads at least one row group of every
    /// file it overlaps. A child that measured what its parent measured IS that
    /// floor, so the split must decline (the runner hash-shards internally at any
    /// width) AND leave the unit runnable, or it does no work at all.
    #[test]
    fn a_child_no_cheaper_than_its_parent_stops_bisecting_and_stays_claimable() {
        let (_dir, mut journal) = new_journal();
        let key = floored_child(&mut journal);
        let now = crate::support::now_micros();

        assert!(
            !journal.split_time_task(&key, 96 * MAX_DECODED_BYTES, None),
            "halving the width bought 4% — the row-group floor dominates, so bisecting again only mints units"
        );
        assert_eq!(journal.state(&key), Some(TaskState::Pending), "a declined split must leave the unit runnable, not superseded");
        let claimed = journal.claim_next(Operation::BaseRollup, now, true);
        assert_eq!(claimed.map(|task| task.key), Some(key), "the declined unit is the one a worker picks up, so the work still happens");
    }

    /// `enqueue` is keyed by `TaskKey`, so N holes escalating to the same
    /// covering slice collapse into ONE pending unit — which is why no batching
    /// layer exists. A change to enqueue's identity rules would reintroduce the
    /// N-rebuild cost silently.
    #[test]
    fn escalations_to_one_covering_slice_collapse_into_a_single_unit() {
        let (_dir, mut journal) = new_journal();
        let day = 3 * DAY_MICROS;
        let covering = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;

        // Five narrow units each escalate to the same covering slice, exactly as
        // `covered_by_wider` does.
        for _ in 0..5 {
            journal.enqueue(covering.clone(), 0, MAX_DECODED_BYTES, 0);
        }

        let covering_units = journal.tasks().filter(|t| t.key == covering).count();
        assert_eq!(covering_units, 1, "the covering rebuild is queued ONCE however many holes escalated to it");
    }

    /// BOTH ways a split can be refused must be counted, or a pinned unit is
    /// invisible. Counters are per-process under nextest, so these are exact.
    #[test]
    fn both_split_declines_are_counted() {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        let (floor0, width0) = (stats.split_declined_at_floor.load(Relaxed), stats.split_declined_no_width.load(Relaxed));

        let (_dir, mut journal) = new_journal();

        // Floor: the unit came back costing nearly what its parent cost.
        let floor_key = floored_child(&mut journal);
        assert!(!journal.split_time_task(&floor_key, 96 * MAX_DECODED_BYTES, None), "floor declines");

        // No width: already at the minimum slice, so bisection yields no children.
        let narrow_key = upserted(&mut journal, task("whale", DAY_MICROS, DAY_MICROS + MIN_SLICE_MICROS, Operation::BaseRollup));
        assert!(!journal.split_time_task(&narrow_key, 96 * MAX_DECODED_BYTES, None), "a minimum-width unit has nothing to split into");

        assert_eq!(stats.split_declined_at_floor.load(Relaxed), floor0 + 1, "the floor decline is counted");
        assert_eq!(stats.split_declined_no_width.load(Relaxed), width0 + 1, "the no-width decline must be counted too — this is the branch that was silent");
    }

    /// The guard above is only as good as the evidence it reads, and that
    /// evidence has to be the parent's MEASUREMENT — the modelled per-child
    /// number is the very thing that cannot be trusted.
    #[test]
    fn a_split_stamps_children_with_what_the_parent_measured() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("whale", 0, DAY_MICROS, Operation::BaseRollup));

        let measured = 4 * MAX_DECODED_BYTES;
        assert!(journal.split_time_task(&key, measured, None), "a day-wide unit with no floor evidence still bisects");

        let children: Vec<_> = journal.tasks().filter(|t| t.state == TaskState::Pending).collect();
        assert!(!children.is_empty(), "the split produced children");
        assert!(
            children.iter().all(|child| child.parent_measured_bytes == Some(measured)),
            "children carry the parent's measurement, not their own modelled share"
        );
    }

    /// `retry_or_split` forces a bisection with a synthetic
    /// `MAX_DECODED_BYTES + 1` — a "does not fit" signal, not a measurement.
    /// Comparing the next REAL measurement against it must not decline every
    /// split in the lineage forever.
    #[test]
    fn a_synthetic_stamp_does_not_freeze_a_lineage() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("whale", 0, DAY_MICROS, Operation::BaseRollup));

        // The retry path's synthetic value, verbatim.
        assert!(journal.split_time_task(&key, MAX_DECODED_BYTES.saturating_add(1), None));
        let child = journal.tasks().find(|t| t.state == TaskState::Pending).expect("a child").clone();
        assert_eq!(child.parent_measured_bytes, Some(MAX_DECODED_BYTES + 1));

        // A real preflight measuring far more than the synthetic seed is not the
        // row-group floor, so the child must still be splittable.
        assert!(
            journal.split_time_task(&child.key, 8 * MAX_DECODED_BYTES, None),
            "a measurement ABOVE the parent's stamp is evidence the stamp was never a measurement"
        );
    }

    #[test]
    fn journal_claims_rotate_projects_instead_of_restarting_at_first() {
        let (_dir, mut journal) = new_journal();
        for input in
            [task("a", 0, 1, Operation::Dedup), task("a", 1, 2, Operation::Dedup), task("b", 0, 1, Operation::Dedup), task("b", 1, 2, Operation::Dedup)]
        {
            journal.upsert(input);
        }
        assert_eq!(journal.claim_next(Operation::Dedup, 0, true).expect("first").key.project_id, "a");
        assert_eq!(journal.claim_next(Operation::Dedup, 0, true).expect("second").key.project_id, "b");
    }

    #[test]
    fn bootstrap_backlog_migration_keeps_publications_and_runs_once() {
        let (dir, mut journal) = new_journal();
        // An existing v1 marker must not suppress the v2 cleanup.
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v1".to_owned(), 1);
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v2".to_owned(), 1);
        journal.upsert(task("pending-a", 0, 1, Operation::Dedup));
        journal.upsert(task("pending-b", 1, 2, Operation::BaseRollup));
        let complete = task("published", 2, 3, Operation::BaseRollup).tap_mut(|t| t.state = TaskState::Complete);
        journal.upsert(complete.clone());

        assert_eq!(journal.migrate_bootstrap_backlog_with_limit(2), Some(2));
        assert_eq!(journal.snapshot.tasks, vec![complete]);
        assert_eq!(journal.migrate_bootstrap_backlog_with_limit(0), None, "migration marker makes cleanup one-shot");
        journal.compact().expect("migration snapshot");
        let reloaded = TaskJournal::load(dir.path()).expect("reloaded journal");
        assert_eq!(reloaded.snapshot.tasks.len(), 1, "removed bootstrap tasks must not resurrect after restart");
        assert_eq!(reloaded.snapshot.tasks[0].key.project_id, "published");
    }

    #[test]
    fn repeated_pending_invalidation_does_not_rewrite_the_wal() {
        let (_dir, mut journal) = new_journal();
        let key = task("customer", 0, 1, Operation::BaseRollup).key;
        journal.enqueue(key.clone(), 10, 512, 1);
        journal.checkpoint().expect("first checkpoint");
        let first_size = wal_len(&journal);

        journal.enqueue(key.clone(), 20, 512, 2);
        journal.checkpoint().expect("idempotent checkpoint");
        assert_eq!(wal_len(&journal), first_size);

        journal.enqueue(key, 5, 512, 3);
        journal.checkpoint().expect("earlier deadline checkpoint");
        assert!(wal_len(&journal) > first_size);
    }

    /// Scheduling order, case by case. Historical debt runs newest SLICE first,
    /// not oldest deadline first: oldest-first spends the whole non-frontier
    /// budget on the data nobody queries, and newest-first does not starve old
    /// debt because the recent window is finite and then the order walks back.
    /// (Overdue work still drains oldest-first — a different question.)
    #[test_case::test_case((-MIN_SLICE_MICROS, 0, Operation::BaseRollup, None), (-ORDER_NOW, -ORDER_NOW + MIN_SLICE_MICROS, Operation::Dedup, None) ; "recent_slices_precede_overdue_historical_work")]
    #[test_case::test_case((-2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, Operation::BaseRollup, None), (-12 * 60 * 60 * 1_000_000, -12 * 60 * 60 * 1_000_000 + MIN_SLICE_MICROS, Operation::BaseRollup, None) ; "newest_eligible_frontier_slice_precedes_older_frontier_debt")]
    #[test_case::test_case((-2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, Operation::BaseRollup, Some(-60 * 1_000_000)), (-ORDER_NOW, -ORDER_NOW + MIN_SLICE_MICROS, Operation::BaseRollup, Some(-1)) ; "recently_mutated_historical_hole_does_not_displace_frontier")]
    #[test_case::test_case((-2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, Operation::BaseRollup, None), (24 * 60 * 60 * 1_000_000, 24 * 60 * 60 * 1_000_000 + MIN_SLICE_MICROS, Operation::BaseRollup, None) ; "future_clock_slice_does_not_displace_live_frontier")]
    #[test_case::test_case((-2 * 24 * 3_600_000_000 + MIN_SLICE_MICROS, -2 * 24 * 3_600_000_000 + 2 * MIN_SLICE_MICROS, Operation::BaseRollup, Some(-60 * 1_000_000)), (-2 * 24 * 3_600_000_000, -2 * 24 * 3_600_000_000 + MIN_SLICE_MICROS, Operation::BaseRollup, Some(-2 * 60 * 1_000_000)) ; "historical_debt_runs_newest_slice_first")]
    fn scheduling_class_prefers(winner: Ranked, loser: Ranked) {
        let now = ORDER_NOW;
        let build = |project: &str, (start, end, operation, deadline): Ranked| {
            let mut unit = task(project, now + start, now + end, operation);
            if let Some(offset) = deadline {
                unit.deadline_micros = now + offset;
            }
            unit
        };
        let (winner, loser) = (build("winner", winner), build("loser", loser));
        assert!(scheduling_class(&winner, now) < scheduling_class(&loser, now), "the winning slice is the one the scheduler must reach first");
    }

    /// Pending work must be reportable per operation AND split by whether it is
    /// claimable right now — `tasks_pending` alone cannot distinguish "none
    /// queued" from "queued but not eligible" from "eligible but out-competed".
    #[test]
    fn pending_work_is_reported_per_operation_and_by_eligibility() {
        use std::sync::atomic::Ordering::Relaxed;
        // publish_statistics reads the real clock, so eligibility has to be
        // expressed against it — an epoch-relative `now` makes every deadline
        // look long past and the eligible/pending distinction vanishes.
        let now = crate::support::now_micros();
        let (_dir, mut journal) = new_journal();
        let key = |op, start: i64, end: i64| task("p", start, end, op).key;
        // One eligible sealed rollup, one sealed rollup not yet due, one repair.
        // Sealed slices: well before now, so they are not live-frontier.
        let old_start = now - 30 * 24 * 60 * 60 * 1_000_000;
        journal.enqueue(key(Operation::BaseRollup, old_start, old_start + MIN_SLICE_MICROS), now - 1, 1, 1);
        journal.enqueue(key(Operation::BaseRollup, old_start + MIN_SLICE_MICROS, old_start + 2 * MIN_SLICE_MICROS), now + 3_600_000_000, 1, 1);
        journal.enqueue(key(Operation::Repair, old_start, old_start + MIN_SLICE_MICROS), now - 1, 1, 1);
        journal.publish_statistics();

        let stats = crate::observability::maintenance_stats();
        assert_eq!(stats.pending_base_rollup.load(Relaxed), 2, "both rollup tasks are pending regardless of eligibility");
        assert_eq!(stats.pending_repair.load(Relaxed), 1);
        assert_eq!(stats.eligible_base_rollup.load(Relaxed), 1, "only the due rollup is claimable now — this is the distinction tasks_pending cannot make");
        assert_eq!(stats.eligible_sealed_total.load(Relaxed), 2, "both due sealed tasks (rollup + repair) are claimable");
    }

    /// `oldest_task_age` reports work the scheduler intends to do; work past
    /// `STARVATION_HORIZON_MICROS` is never scheduled, so it is COUNTED, not
    /// aged. Both halves are asserted together: narrowing the age alone would
    /// look identical to hiding the debt.
    #[test]
    fn the_age_gauge_skips_abandoned_work_and_counts_it_instead() {
        use std::sync::atomic::Ordering::Relaxed;
        let now = crate::support::now_micros();
        let (_dir, mut journal) = new_journal();
        let key = |start: i64| task("p", start, start + DAY_MICROS, Operation::SealedConsolidation).key;
        let stamp = |micros: i64| u64::try_from(micros.div_euclid(1_000)).unwrap_or_default();
        // A hygiene unit aged from a seal time 85 days back...
        journal.enqueue(key(now - 86 * DAY_MICROS), now, 1, stamp(now - 85 * DAY_MICROS));
        // ...beside one inside the window, five days old.
        journal.enqueue(key(now - 6 * DAY_MICROS), now, 1, stamp(now - 5 * DAY_MICROS));
        journal.publish_statistics();

        let stats = crate::observability::maintenance_stats();
        assert_eq!(stats.maintenance_beyond_horizon_tasks.load(Relaxed), 1, "the abandoned unit must be sized, not silently dropped");
        let age_days = stats.maintenance_oldest_task_age_secs.load(Relaxed) / 86_400;
        assert_eq!(age_days, 5, "the gauge must report the oldest unit the scheduler will still escalate, not the 85-day tail");
    }

    /// The coarse-backfill migration must be narrow: fine sealed backfill goes,
    /// everything else stays. Dropping tasks is safe only because
    /// `plan_rollup_backfill` re-derives from rollup COVERAGE; over-reaching
    /// would silently cancel frontier work or separately-planned compaction debt.
    #[test]
    fn coarse_backfill_migration_only_drops_fine_sealed_backfill() {
        let now = crate::support::now_micros();
        let (_dir, mut journal) = new_journal();
        let day = 24 * 60 * 60 * 1_000_000i64;
        let sealed_start = now - 10 * day;
        let key = |op, start: i64, end: i64| task("p", start, end, op).key;
        // Dropped: fine-grained sealed rollup/dedup work.
        journal.enqueue(key(Operation::BaseRollup, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);
        journal.enqueue(key(Operation::Dedup, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);
        // Kept: live frontier, day-sized (already coarse), and compaction debt.
        journal.enqueue(key(Operation::BaseRollup, now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS), now, 1, 1);
        journal.enqueue(key(Operation::BaseRollup, sealed_start, sealed_start + day), now, 1, 1);
        journal.enqueue(key(Operation::SealedConsolidation, sealed_start, sealed_start + day), now, 1, 1);
        journal.enqueue(key(Operation::Repair, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);

        let removed = journal.migrate_fine_grained_backfill(now).expect("migration runs once");
        assert_eq!(removed, 2, "only the two fine-grained sealed backfill tasks should go");
        let kept: Vec<_> = journal.tasks().map(|task| (task.key.operation, task.key.slice.width())).collect();
        assert_eq!(kept.len(), 4, "frontier, day-sized rollup, consolidation and repair must all survive");
        assert!(kept.iter().any(|(op, _)| *op == Operation::SealedConsolidation), "compaction debt is planned elsewhere and must not be cancelled");
        assert!(kept.iter().any(|(op, width)| *op == Operation::BaseRollup && *width == day), "an already-coarse rollup unit must survive");

        // One-shot: a second call must not re-run and drop the coarse re-plan.
        assert!(journal.migrate_fine_grained_backfill(now).is_none(), "migration must be guarded by its cursor");
    }

    /// The comparator for an empty publication must be the base tier's rows in
    /// THIS slice, not the day: `source_rows` is keyed on (project, date) while a
    /// unit is an hour, so a genuinely empty hour of a busy day reads as
    /// non-empty and a guard built on it fires on correct work.
    #[test]
    fn published_rows_are_summed_per_slice_and_per_project() {
        let (_dir, mut journal) = new_journal();
        const HOUR: i64 = 3_600_000_000;
        let key = |project: &str, table: &str, hour: i64| task_in(table, project, hour * HOUR, (hour + 1) * HOUR, Operation::BaseRollup).key;
        let publish = |journal: &mut TaskJournal, project: &str, table: &str, hour: i64, rows: u64| {
            let key = key(project, table, hour);
            journal.enqueue(key.clone(), 0, 1, 1);
            journal.publish(&key, Publication { source_fingerprint: 0, generation: "g".into(), rows, source_rows: Some(1_000_000), source_rows_below: None });
        };
        publish(&mut journal, "p", "base", 1, 10);
        publish(&mut journal, "p", "base", 2, 0); // an hour the base itself left empty
        publish(&mut journal, "p", "base", 3, 7);
        publish(&mut journal, "other", "base", 2, 999); // another tenant
        publish(&mut journal, "p", "elsewhere", 2, 999); // another tier

        let rows = |lo: i64, hi: i64| journal.published_rows_overlapping("p", "base", lo * HOUR, hi * HOUR);
        assert_eq!(rows(1, 2), 10, "one slice");
        assert_eq!(rows(1, 4), 17, "overlapping slices sum, and the empty hour adds nothing");
        assert_eq!(rows(2, 3), 0, "an hour the base left empty is NOT a violation — this is the 9-of-285 case");
        assert_eq!(rows(4, 5), 0, "a slice with no base publication at all");
        assert_eq!(rows(0, 24), 17, "the day never picks up another tenant or another tier");
    }

    /// Sealed work must get a share of claims even while the frontier is busy:
    /// class is strict priority and ingest never stops, so without the
    /// reservation class 1 never runs at all.
    #[test]
    fn sealed_work_gets_claims_while_the_frontier_is_busy() {
        let now = ORDER_NOW;
        let (_dir, mut journal) = new_journal();

        // Plenty of frontier work — class 0 permanently non-empty — plus one
        // sealed day.
        let key = |start: i64, end: i64| task("p", start, end, Operation::BaseRollup).key;
        for k in 1..20 {
            journal.enqueue(key(now - (k + 1) * MIN_SLICE_MICROS, now - k * MIN_SLICE_MICROS), now - 1, 1, 1);
        }
        journal.enqueue(key(0, MIN_SLICE_MICROS), now - 1, 1, 1);

        // Enough sealed slices that the share, not the supply, limits sealed claims.
        for k in 1..20 {
            journal.enqueue(key(k * MIN_SLICE_MICROS, (k + 1) * MIN_SLICE_MICROS), now - 1, 1, 1);
        }

        let sealed_claims = (0..12)
            .filter(|_| {
                let claimed = journal.claim_next(Operation::BaseRollup, now, true).expect("a task is always available");
                !is_live_frontier(claimed.key.slice, now)
            })
            .count();
        // "> 0" would pass at a share too small to ever drain a backlog, so the
        // invariant is "a third or better" — a higher share was tried and OOMed.
        assert!(
            sealed_claims >= 4,
            "sealed work got only {sealed_claims}/12 claims; below a third, a 118k-task sealed backlog never drains and long windows stay unroutable"
        );
    }

    /// Frontier lag is the newest slice of each stream, never a historical hole:
    /// `(project, slice start, slice end, deadline, completed)` offsets from
    /// `ORDER_NOW` in, lag seconds out.
    #[test_case::test_case(vec![("project", -ORDER_NOW, -ORDER_NOW + MIN_SLICE_MICROS, -4 * 60 * 60 * 1_000_000, false)] => 0 ; "live_frontier_lag_ignores_historical_holes")]
    #[test_case::test_case(vec![("project", -3 * MIN_SLICE_MICROS, -2 * MIN_SLICE_MICROS, -10 * 60 * 1_000_000, false), ("project", -2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, -2 * 60 * 1_000_000, false)] => 2 * 60 ; "live_frontier_lag_tracks_only_each_streams_newest_slice")]
    #[test_case::test_case(vec![("project", -3 * MIN_SLICE_MICROS, -2 * MIN_SLICE_MICROS, -10 * 60 * 1_000_000, false), ("project", -2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, -2 * 60 * 1_000_000, true)] => 0 ; "an older hole is not the raw tail once newer coverage landed")]
    #[test_case::test_case(vec![("fast", -2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, -60 * 1_000_000, false), ("slow", -2 * MIN_SLICE_MICROS, -MIN_SLICE_MICROS, -5 * 60 * 1_000_000, false)] => 5 * 60 ; "live_frontier_lag_reports_the_slowest_project")]
    fn live_frontier_lag(streams: Vec<Stream>) -> u64 {
        let now = ORDER_NOW;
        let tasks: Vec<MaintenanceTask> = streams
            .into_iter()
            .map(|(project, start, end, deadline, complete)| {
                let mut unit = task(project, now + start, now + end, Operation::BaseRollup);
                unit.deadline_micros = now + deadline;
                if complete {
                    unit.state = TaskState::Complete;
                }
                unit
            })
            .collect();
        live_frontier_lag_secs(tasks.iter(), now)
    }

    #[test]
    fn derived_invalidations_use_one_aligned_hour() {
        let (_dir, mut journal) = new_journal();
        journal.invalidate(invalidation("derived", NORMAL_SLICE_MICROS, 2 * NORMAL_SLICE_MICROS, 0, true)).expect("invalidate");
        let derived = journal.tasks().find(|task| task.key.operation == Operation::DerivedRollup).expect("derived task");
        assert_eq!(derived.key.slice.width(), DERIVED_SLICE_MICROS);
        assert_eq!(derived.key.slice.start_micros % DERIVED_SLICE_MICROS, 0);
        assert!(journal.tasks().filter(|task| task.key.operation == Operation::Dedup).all(|task| task.key.slice.width() == NORMAL_SLICE_MICROS));
    }

    /// The hour migration must not touch a slice WIDER than an hour: its
    /// replacement key is the single hour containing the slice START, so a
    /// day-wide unit would be re-enqueued as hour 00 and lose the other 23.
    #[test]
    fn the_hour_migration_leaves_a_slice_wider_than_an_hour_alone() {
        let (dir, mut journal) = new_journal();
        const DAY: i64 = 24 * DERIVED_SLICE_MICROS;
        journal.upsert(task("p", 0, DAY, Operation::DerivedRollup));
        // A genuine legacy fragment alongside it, so the migration is not simply inert.
        journal.upsert(task("p", DAY, DAY + NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        journal.checkpoint().expect("checkpoint");

        let mut journal = TaskJournal::load(dir.path()).expect("journal to migrate");
        assert_eq!(journal.migrate_derived_slices(), 1, "only the sub-hour fragment may migrate");
        journal.checkpoint().expect("migration checkpoint");

        // The migration must SURVIVE a restart, so assertions read the reloaded journal.
        let journal = TaskJournal::load(dir.path()).expect("migrated journal");
        assert!(
            journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup
                && task.key.slice.width() == DERIVED_SLICE_MICROS
                && task.key.slice.start_micros == DAY),
            "the fragment is replaced by the aligned hour containing it"
        );
        assert!(
            journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup
                && task.key.slice.width() == NORMAL_SLICE_MICROS
                && task.state == TaskState::Superseded),
            "and the fragment itself is superseded"
        );
        let day_wide = journal
            .tasks()
            .find(|task| task.key.operation == Operation::DerivedRollup && task.key.slice.width() == DAY)
            .expect("the day-wide unit must survive the migration");
        assert_ne!(day_wide.state, TaskState::Superseded, "a day-wide derived unit must not be superseded by the hour migration");
        assert!(
            !journal.tasks().any(|task| task.key.slice.start_micros == 0 && task.key.slice.width() == DERIVED_SLICE_MICROS),
            "the day must not be replaced by hour 00, which drops the other 23 hours"
        );
    }

    /// Rebuilding a BASE slice must reopen the DERIVED cell built over it:
    /// back to Pending, stale publication dropped, and without minting new tasks.
    #[test]
    fn republishing_a_base_slice_reopens_the_derived_cell_over_it() {
        let (_dir, mut journal) = new_journal();
        const HOUR: i64 = DERIVED_SLICE_MICROS;
        let derived = |start: i64, end: i64| task_in("derived", "p", start, end, Operation::DerivedRollup).key;
        let publication = || Publication { source_fingerprint: 7, generation: "g".into(), rows: 5, source_rows: Some(9), source_rows_below: None };
        for (start, end) in [(0, HOUR), (HOUR, 2 * HOUR), (5 * HOUR, 6 * HOUR)] {
            let key = derived(start, end);
            journal.enqueue(key.clone(), 0, 1, 1);
            journal.publish(&key, publication());
            journal.complete(&key);
        }
        let before = journal.tasks().count();

        // A base unit republishes 00:00-02:00 — two of the three derived cells.
        let reopened = journal.reopen_derived_over("p", "derived", 0, 2 * HOUR);
        assert_eq!(reopened, 2, "exactly the derived cells overlapping the republished range");
        assert_eq!(journal.tasks().count(), before, "reopening must not MINT tasks — that is the resurrect loop");

        let state_of = |journal: &TaskJournal, start: i64| {
            journal
                .tasks()
                .find(|task| task.key.slice.start_micros == start && task.key.operation == Operation::DerivedRollup)
                .map(|t| (t.state, t.publication.is_some()))
        };
        assert_eq!(state_of(&journal, 0), Some((TaskState::Pending, false)), "reopened, and its stale publication dropped");
        assert_eq!(state_of(&journal, HOUR), Some((TaskState::Pending, false)), "reopened, and its stale publication dropped");
        assert_eq!(state_of(&journal, 5 * HOUR), Some((TaskState::Complete, true)), "a cell outside the republished range must be untouched");

        assert_eq!(journal.reopen_derived_over("p", "derived", 0, 2 * HOUR), 0, "reopening is idempotent");
    }

    #[test]
    fn journal_round_trips_tasks_and_monotonic_cursors() {
        let (dir, mut journal) = new_journal();
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        journal.set_source_cursor("source".into(), 9);
        journal.set_source_cursor("source".into(), 7);
        journal.checkpoint().expect("checkpoint");
        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        assert_eq!(loaded.tasks().count(), 1);
        assert_eq!(loaded.source_cursor("source"), Some(9));
    }

    #[test]
    fn production_sized_wal_replay_updates_tasks_without_quadratic_scans() {
        const TASKS: i64 = 20_000;
        let (dir, mut journal) = new_journal();
        for index in 0..TASKS {
            journal.upsert(task("large-project", index, index + 1, Operation::Dedup));
        }
        journal.checkpoint().expect("initial backlog checkpoint");
        for index in 0..TASKS {
            let key = task("large-project", index, index + 1, Operation::Dedup).key;
            assert!(journal.retry(&key, "restart_test".to_owned(), index));
        }
        journal.checkpoint().expect("updated backlog checkpoint");

        let loaded = TaskJournal::load(dir.path()).expect("replay large journal");
        assert_eq!(loaded.tasks().count(), usize::try_from(TASKS).expect("positive task count"));
        let last = task("large-project", TASKS - 1, TASKS, Operation::Dedup).key;
        assert_eq!(loaded.state(&last), Some(TaskState::Retry));
    }

    #[test]
    fn empty_rollup_publication_survives_restart() {
        let (dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        assert!(
            journal.publish(&key, Publication { source_fingerprint: 7, generation: "stable".to_owned(), rows: 0, source_rows: None, source_rows_below: None })
        );
        journal.checkpoint().expect("checkpoint");

        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        let publication = loaded.published_rollups("source", "table").into_iter().next().expect("published empty slice").1;
        assert_eq!(publication.rows, 0);
        assert_eq!(publication.source_fingerprint, 7);
    }

    #[test]
    fn dropping_a_running_lease_durably_requeues_the_task() {
        let (dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::Dedup));
        assert!(journal.mark_running(&key));
        journal.checkpoint().expect("running checkpoint");
        let journal = Arc::new(Mutex::new(journal));
        let before_drop = crate::support::now_micros();
        drop(TaskLease::new(Arc::clone(&journal), key.clone(), tokio_util::sync::CancellationToken::new()));

        let journal_guard = journal.lock().expect("lock");
        assert_eq!(journal_guard.state(&key), Some(TaskState::Retry));
        let task = journal_guard.snapshot.tasks.iter().find(|task| task.key == key).expect("requeued task");
        assert_eq!(task.retry_reason.as_deref(), Some("worker_error"));
        assert!(task.deadline_micros >= before_drop + 2_000_000, "first failed attempt must use the same exponential backoff as explicit worker errors");
        drop(journal_guard);
        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        assert_eq!(loaded.state(&key), Some(TaskState::Retry));
    }

    /// A completed unit must NOT be requeued by its lease: `maintenance_task_finished`
    /// is emitted from `Drop` and reports whatever `state` says at that moment.
    #[test]
    fn a_completed_lease_reports_complete_and_is_not_requeued() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::SealedConsolidation));
        assert!(journal.mark_running(&key));
        assert!(journal.complete(&key));
        let journal = Arc::new(Mutex::new(journal));
        drop(TaskLease::new(Arc::clone(&journal), key.clone(), tokio_util::sync::CancellationToken::new()));

        let guard = journal.lock().expect("lock");
        assert_eq!(guard.state(&key), Some(TaskState::Complete), "a completed unit must survive its own lease drop");
        // The debt field is absent on a unit that was never preflighted.
        assert_eq!(guard.input_files(&key), None);
    }

    #[test]
    fn every_crash_boundary_recovers_to_redundant_work_or_published_coverage() {
        let (dir, mut journal) = new_journal();
        // A crash leaves pending work and therefore no coverage claim.
        let key = upserted(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        journal.checkpoint().expect("invalidation checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after invalidation");
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));
        assert!(recovered.published_rollups("source", "table").is_empty());

        // Claims are deliberately transient, so restart sees the last durable
        // Pending state directly; a landed target commit is replaced by the retry.
        assert!(recovered.mark_running(&key));
        recovered.checkpoint().expect("running checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after staging");
        assert_eq!(recovered.requeue_running(100), 0);
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));

        // Coverage checkpoint is the only boundary that makes the slice
        // readable after restart, including an empty output.
        assert!(
            recovered.publish(&key, Publication { source_fingerprint: 9, generation: "g".to_owned(), rows: 0, source_rows: None, source_rows_below: None })
        );
        recovered.checkpoint().expect("coverage checkpoint");
        let recovered = TaskJournal::load(dir.path()).expect("recover publication");
        let published = recovered.published_rollups("source", "table");
        assert_eq!(published.len(), 1);
        assert_eq!((published[0].1.rows, published[0].1.source_fingerprint), (0, 9), "an empty publication survives the restart with its fingerprint intact");
    }

    #[test]
    fn invalidation_is_idempotent_and_extends_the_quiet_period() {
        let (_dir, mut journal) = new_journal();
        // The same slice, re-invalidated; only `observed_at_micros` moves.
        let observed_at = |observed_at_micros| invalidation("rollup", 0, NORMAL_SLICE_MICROS, observed_at_micros, false);
        journal.invalidate(observed_at(10)).expect("invalidate");
        journal.checkpoint().expect("first invalidation checkpoint");
        let first_wal_size = wal_len(&journal);
        journal.invalidate(observed_at(20)).expect("invalidate again");
        journal.checkpoint().expect("same-bucket checkpoint");
        assert_eq!(wal_len(&journal), first_wal_size, "same deadline bucket must not rewrite tasks");
        journal.invalidate(observed_at(INVALIDATION_DEADLINE_BUCKET_MICROS + 1)).expect("invalidate in next bucket");
        // Two, not three: Dedup and the rollup. HotPacking is planned by debt in
        // `plan_compaction_debt`, never minted per slice.
        assert_eq!(journal.tasks().count(), 2);
        assert!(
            journal.tasks().all(|task| task.key.operation != Operation::HotPacking),
            "ingest must not mint file-hygiene work per slice; the debt planner owns it"
        );
        assert!(journal.tasks().all(|task| task.deadline_micros == FINALIZATION_DELAY_MICROS + 2 * INVALIDATION_DEADLINE_BUCKET_MICROS));
    }

    #[test]
    fn running_tasks_are_requeued_after_a_restart() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, MIN_SLICE_MICROS, Operation::Dedup));
        assert!(journal.mark_running(&key));
        assert_eq!(journal.requeue_running(42), 1);
        let task = journal.tasks().next().expect("task");
        assert_eq!(task.state, TaskState::Retry);
        assert_eq!(task.deadline_micros, 42);
    }

    #[test]
    fn claim_is_durable_state_not_an_in_memory_queue_pop() {
        let (_dir, mut journal) = new_journal();
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::Dedup));
        let claimed = journal.claim_next(Operation::Dedup, 0, true).expect("claim");
        assert_eq!(claimed.state, TaskState::Running);
        assert_eq!(claimed.attempts, 1);
        assert!(journal.claim_next(Operation::Dedup, 0, true).is_none());
    }

    /// A derived-rollup key on the 1h tier — the shape `invalidate` mints.
    fn derived_key(project: &str, start: i64, width: i64) -> TaskKey {
        task_in("rollup_1h", project, start, start + width, Operation::DerivedRollup).key
    }

    /// A tier unit as the planner just minted it: named tier, due now, and freshly
    /// created so `starved` cannot decide the order (age would otherwise dominate).
    fn tier_unit(project: &str, table: &str, start: i64, width: i64, now: i64, operation: Operation) -> MaintenanceTask {
        task_in(table, project, start, start + width, operation).tap_mut(|unit| unit.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default())
    }

    /// A sealed rollup unit on the 1m tier, priced trivially so only the
    /// coarsening pass's own rules decide whether it fuses or is subsumed.
    fn coarsenable_unit(project: &str, start: i64, width: i64) -> MaintenanceTask {
        task_in("rollup_1m", project, start, start + width, Operation::BaseRollup).tap_mut(|unit| unit.estimated_decoded_bytes = 1)
    }

    /// `scheduling_class` of a day-wide SealedConsolidation unit whose day
    /// ended `hours_ago`. Day-wide and footprint-less, so width and benefit tie
    /// and only age can order two of these.
    fn sealed_class(project: &str, hours_ago: i64, now: i64) -> (u8, u8, i64, i64, i64) {
        let end = now - hours_ago * 3_600_000_000;
        super::scheduling_class(&task(project, end - DAY_MICROS, end, Operation::SealedConsolidation), now)
    }

    /// A hygiene cell as `plan_compaction_debt` mints one: day-wide, ending at
    /// `end`, carrying the file footprint it was selected on.
    fn hygiene_cell(project: &str, end: i64, files: u32, operation: Operation) -> MaintenanceTask {
        task(project, end - DAY_MICROS, end, operation)
            .tap_mut(|unit| unit.input = Some(InputFootprint::new((0..files).map(|n| format!("{project}/{n}.parquet")), 1)))
    }

    /// Seeds a FRESH journal — rank state (`tier_holes`, untagged cells) is
    /// per-journal — and returns the project whose unit wins the first claim.
    fn claim_winner(now: i64, operation: Operation, seed: impl FnOnce(&mut TaskJournal)) -> String {
        let (_dir, mut journal) = new_journal();
        seed(&mut journal);
        journal.claim_next(operation, now, true).expect("claim").key.project_id
    }

    /// Historical derived work must not lose every claim to the frontier: class is
    /// strict priority and the frontier regenerates continuously, so without a
    /// reservation the historical units never run at all.
    #[test]
    fn a_sealed_derived_unit_is_not_starved_by_the_frontier() {
        let (_dir, mut journal) = new_journal();
        let now = 40 * 24 * 3_600_000_000i64;
        let derived = |project: &str, start: i64, width: i64| {
            task(project, start, start + width, Operation::DerivedRollup).tap_mut(|t| t.base_tier_present = true).tap_mut(|t| t.deadline_micros = 0)
        };
        // A one-hour frontier slice, and a day-wide sealed one ten days back.
        journal.upsert(derived("frontier", now - 3_600_000_000, 3_600_000_000));
        let sealed_start = now - 10 * 24 * 3_600_000_000;
        journal.upsert(derived("sealed", sealed_start, 24 * 3_600_000_000));

        // The frontier is behind, which is when the general reservation shrinks.
        journal.frontier_lag_secs.store(FRONTIER_LAG_BUDGET_SECS + 1, std::sync::atomic::Ordering::Relaxed);
        let claimed = journal.claim_next(Operation::DerivedRollup, now, true).expect("a derived unit is claimable");
        assert_eq!(claimed.key.project_id, "sealed", "historical derived work must win the claim, not today's");
    }

    /// A derived unit whose base TIER already exists must be claimable, even when
    /// no `BaseRollup` journal task records that it was built — through BOTH paths
    /// that carry the proof. `prove_base_tier_for_day` is the one that reaches a
    /// unit `enqueue` can no longer touch: a blocked derived unit stays queued,
    /// which makes its day ineligible for backfill admission.
    #[test_case::test_case(true ; "the backfill planner re-enqueues with the proof")]
    #[test_case::test_case(false ; "an already-queued unit is told its base tier exists")]
    fn a_derived_unit_runs_once_its_base_tier_is_proven(via_enqueue: bool) {
        let (_dir, mut journal) = new_journal();
        let key = derived_key("historical", 0, 3_600_000_000);

        // No completed base task covers this slice, so the unit is refused.
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "without evidence the dependency gate still holds");

        if via_enqueue {
            // The backfill planner reads real tier coverage; that proof must be enough.
            journal.enqueue_with_base_tier(key.clone(), 0, 1, 0, true);
        } else {
            let day = (0, 24 * 3_600_000_000i64);
            assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 1, "the proof lands on an existing task");
            assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 0, "and is idempotent");
        }
        assert_eq!(journal.claim_next(Operation::DerivedRollup, 0, true).expect("proven base tier makes the unit claimable").key.project_id, "historical");
    }

    /// The hygiene benefit band must separate cells at the sizes that exist, while
    /// still TYING comparable ones — `fair_cursors` rotates projects only among
    /// cells that tie, so a band fine enough to separate every count makes one cell
    /// win every claim. A linear band cannot separate p50 from p75; a ratio band would.
    #[test_case::test_case(35, 9 => false ; "a p50 cell must outrank a nearly-empty one")]
    #[test_case::test_case(107, 35 => false ; "p90 must be separable from p50")]
    #[test_case::test_case(200, 210 => true ; "comparable large cells must tie, or rotation dies")]
    #[test_case::test_case(35, 36 => true ; "the band must remain coarse enough to tie neighbours")]
    #[test_case::test_case(35, 56 => true ; "linear banding cannot separate p50 from p75; a ratio band would")]
    fn the_benefit_band_separates_cells_at_the_sizes_that_exist(files: u32, rival_files: u32) -> bool {
        files / BENEFIT_BUCKET_FILES == rival_files / BENEFIT_BUCKET_FILES
    }

    /// Reports how many hygiene cells the ranker cannot separate — the number that
    /// decides whether `BENEFIT_BUCKET_FILES` is too coarse.
    #[test]
    fn the_hygiene_spread_reports_how_many_cells_the_ranker_cannot_separate() {
        let now = 40 * DAY_MICROS;
        let (_dir, mut journal) = new_journal();
        // Three cells under the bucket width and one far above it.
        for (project, files) in [("a", 5usize), ("b", 9), ("c", 60), ("d", 400)] {
            let key = task(project, now - 2 * DAY_MICROS, now - DAY_MICROS, Operation::HotPacking).key;
            journal.enqueue(key.clone(), 0, 0, 0);
            journal.record_input(&key, InputFootprint::new((0..files).map(|i| format!("{project}/{i}.parquet")), 0));
        }
        let spread = journal.hygiene_debt_spread(Operation::HotPacking, now).expect("cells are claimable");
        assert!(spread.contains("cells=4"), "all four cells are claimable: {spread}");
        assert!(spread.contains("files_max=400"), "the large cell is reported: {spread}");
        // 5 and 9 land in band 0; 60 -> 1; 400 -> 12. The metric is the tie count,
        // whatever the band width happens to be.
        assert!(spread.contains("tied_at_zero=2"), "must report how many cells benefit cannot separate: {spread}");
        assert!(spread.contains("distinct_buckets=3"), "4 cells occupy 3 benefit bands: {spread}");
    }

    /// The claimability census must name each reason `claim_next` skips a task —
    /// the skips happen inside filter predicates that otherwise leave no trace.
    #[test]
    fn the_claimability_census_separates_the_reasons_a_task_is_skipped() {
        let (_dir, mut journal) = new_journal();
        const HOUR: i64 = 3_600_000_000;
        let now = 40 * 24 * HOUR;
        let at = |project: &str, start: i64| derived_key(project, start, HOUR);

        // Sealed and dependency-blocked: no completed BaseRollup covers it.
        journal.enqueue(at("blocked", now - 10 * 24 * HOUR), 0, 1, 0);
        // Sealed, proven, but not due yet.
        let later = at("not_due", now - 10 * 24 * HOUR);
        journal.enqueue(later.clone(), now + HOUR, 1, 0);
        journal.prove_base_tier_for_day(&later, now - 10 * 24 * HOUR, now - 9 * 24 * HOUR);
        // Sealed, proven, due, but has burned its attempts.
        let doomed = at("doomed", now - 11 * 24 * HOUR);
        journal.enqueue(doomed.clone(), 0, 1, 0);
        journal.prove_base_tier_for_day(&doomed, now - 11 * 24 * HOUR, now - 10 * 24 * HOUR);
        for _ in 0..TaskJournal::QUARANTINE_ATTEMPTS {
            assert!(journal.mark_running(&doomed));
            journal.retry(&doomed, TaskJournal::WORKER_FAILURE_REASON.to_owned(), 0);
        }

        let (pending, sealed, unproven, quarantined, not_due) = journal.claimability_census(Operation::DerivedRollup, now);
        assert_eq!(pending, 3, "every pending derived task is counted");
        assert_eq!(sealed, 3, "all three are older than the live frontier window");
        assert_eq!(unproven, 1, "only the one without a base-tier proof is dependency-blocked");
        assert_eq!(quarantined, 1, "only the one that burned its attempts is quarantined");
        assert_eq!(not_due, 1, "only the one with a future deadline is not yet due");
    }

    /// Sealed hygiene ranks by how much debt a claim RETIRES, not by date: every
    /// hygiene unit is day-wide, so `-width` ties and recency would otherwise decide.
    #[test]
    fn sealed_hygiene_ranks_by_files_removed_not_by_date() {
        const HOUR: i64 = 3_600_000_000;
        let now = 400 * DAY_MICROS;
        let cell = |project: &str, hours_ago: i64, files: u32, operation| hygiene_cell(project, now - hours_ago * HOUR, files, operation);
        let class = |unit: &MaintenanceTask| super::scheduling_class(unit, now);

        let big_old = cell("a", 60, 200, Operation::SealedConsolidation);
        let small_new = cell("b", 30, 3, Operation::SealedConsolidation);
        assert!(class(&big_old) < class(&small_new), "200 files outrank 3, whichever sealed first");
        // Within a band they TIE, so `fair_cursors` can still rotate projects.
        assert_eq!(
            class(&cell("a", 60, 200, Operation::SealedConsolidation)).3,
            class(&cell("b", 30, 210, Operation::SealedConsolidation)).3,
            "comparable cells must tie"
        );
        // Unknown benefit orders LAST, never first.
        let unknown = cell("c", 60, 0, Operation::SealedConsolidation).tap_mut(|unit| unit.input = None);
        assert!(class(&big_old) < class(&unknown));
        // Benefit is hygiene-only: rollup ordering must still tie, as damage repair relies on.
        assert_eq!(class(&cell("d", 60, 200, Operation::BaseRollup)).3, class(&cell("e", 60, 3, Operation::BaseRollup)).3);
    }

    /// A `SealedConsolidation` unit is never the live frontier: the planner mints it
    /// only for a date it already treats as sealed, so class must not re-promote it
    /// via `is_live_frontier`, which stays true for 24 h after the slice ends.
    #[test]
    fn a_sealed_consolidation_unit_is_never_the_live_frontier() {
        // Late morning, so yesterday's slice ended 11 h ago — inside the 24 h window.
        let now = 400 * DAY_MICROS + 11 * 3_600_000_000;
        let cell = |project: &str, start: i64, files: u32, operation| hygiene_cell(project, start + DAY_MICROS, files, operation);
        let yesterday = cell("small", 399 * DAY_MICROS, 3, Operation::SealedConsolidation);
        let five_days_old = cell("bigdebt", 395 * DAY_MICROS, 238, Operation::SealedConsolidation);

        assert_eq!(super::scheduling_class(&yesterday, now).0, 1, "the planner mints SealedConsolidation only for a date it already treats as sealed");
        assert!(
            super::scheduling_class(&five_days_old, now) < super::scheduling_class(&yesterday, now),
            "238 files of debt must outrank 3 — which class 0 was silently preventing"
        );

        // Today's packing IS frontier work and must stay class 0.
        let today = cell("today", 400 * DAY_MICROS, 9, Operation::HotPacking);
        assert_eq!(super::scheduling_class(&today, now).0, 0, "today's packing is genuinely live-frontier work");
    }

    /// LATENT HAZARD: `benefit = -(input.files / BENEFIT_BUCKET_FILES)` is computed
    /// per UNIT, so cutting a cell into slices makes each slice report a fraction of
    /// the debt and the cell sinks in the ordering. Any change that makes hygiene
    /// units narrower than one (project, date) must fix this first.
    #[test]
    fn splitting_a_cell_would_divide_its_benefit_and_invert_the_ordering() {
        let now = 400 * DAY_MICROS;
        let unit = |project: &str, days_ago: i64, slice: i64, slices: i64, files: u32| {
            let width = DAY_MICROS / slices;
            let end = now - days_ago * DAY_MICROS - slice * width;
            task(project, end - width, end, Operation::SealedConsolidation)
                .tap_mut(|t| t.input = Some(InputFootprint::new((0..files).map(|n| format!("{project}/{days_ago}/{slice}/{n}.parquet")), 1)))
        };

        // One cell per day — how hygiene actually runs.
        let first = claim_winner(now, Operation::SealedConsolidation, |whole| {
            whole.enqueue_planned(&unit("metrics", 6, 0, 1, 261));
            whole.enqueue_planned(&unit("logs", 3, 0, 1, 964));
        });
        assert_eq!(first.as_str(), "logs", "unsplit, the 964-file cell correctly outranks 261");

        // The same debt, split four ways, now loses to a cell holding a quarter as much.
        let (_dir, mut split) = new_journal();
        split.enqueue_planned(&unit("metrics", 6, 0, 1, 261));
        for slice in 0..4 {
            split.enqueue_planned(&unit("logs", 3, slice, 4, 964 / 4));
        }
        assert_eq!(
            split.claim_next(Operation::SealedConsolidation, now, true).expect("a claim").key.project_id.as_str(),
            "metrics",
            "split four ways, a 964-file day is outranked by a 261-file one"
        );

        // `most_indebted_unclaimed` is blinded the same way: it ranks by per-unit files.
        assert!(
            split.most_indebted_unclaimed(Operation::SealedConsolidation, now).is_none(),
            "the instrument cannot see a starved cell whose debt has been divided below its rivals"
        );
    }

    /// `starved` is `0` only for work aged 3-31 days and is compared BEFORE
    /// `benefit`, so a merely young cell loses however much debt it holds.
    #[test]
    fn the_starvation_window_demotes_the_biggest_debt_when_it_is_young() {
        let now = 400 * DAY_MICROS;
        let cell = |project: &str, days_ago: i64, files: u32| hygiene_cell(project, now - days_ago * DAY_MICROS, files, Operation::SealedConsolidation);
        let first = claim_winner(now, Operation::SealedConsolidation, |journal| {
            journal.enqueue_planned(&cell("biggest-but-young", 1, 433));
            journal.enqueue_planned(&cell("smaller-but-aged", 3, 238));
        });
        assert_eq!(first, "smaller-but-aged", "a 238-file cell wins over a 433-file one solely because the bigger one is 1 day old");
    }

    /// The most indebted unclaimed hygiene cell must name what outranks it —
    /// no other instrument reports the winner of the ordering.
    #[test]
    fn the_most_indebted_hygiene_cell_names_what_outranks_it() {
        const HOUR: i64 = 3_600_000_000;
        let now = 400 * DAY_MICROS;
        let (dir, mut journal) = new_journal();
        // Shaped exactly like `plan_compaction_debt`: build the unit with the
        // footprint it selected on, then hand it to `enqueue_planned`.
        let cell = |project: &str, hours_ago: i64, files: u32| hygiene_cell(project, now - hours_ago * HOUR, files, Operation::SealedConsolidation);
        // The biggest debt sealed a day ago, so it is NOT in the starvation band;
        // a much smaller cell has waited five days and is, so it legitimately wins.
        // The smaller cell is enqueued FIRST so insertion order contradicts the
        // answer — otherwise `max_by_key` over zeroes would name it by accident.
        let indebted = cell("bigdebt", 24, 238);
        journal.enqueue_planned(&cell("starved", 120, 10));
        journal.enqueue_planned(&indebted);

        let refusal = journal.most_indebted_unclaimed(Operation::SealedConsolidation, now).expect("the debt is not being claimed");
        assert!(refusal.starts_with("outranked_by:starved"), "it must name the winner, not merely say CLAIMABLE — got {refusal}");
        assert!(refusal.contains("files=238"), "and it must carry the debt that makes it worth reporting — got {refusal}");

        // Eligibility reasons win over the ordering answer. (A fresh journal,
        // because `enqueue` only ever pulls a deadline EARLIER.)
        let mut later = TaskJournal::load(dir.path()).expect("journal");
        later.enqueue_planned(&indebted.clone().tap_mut(|not_due| not_due.deadline_micros = now + DAY_MICROS));
        assert!(
            later.most_indebted_unclaimed(Operation::SealedConsolidation, now).is_some_and(|why| why.starts_with("not_due:")),
            "a future deadline explains it without appealing to ordering"
        );

        // Silence when there is nothing to explain.
        let mut alone = TaskJournal::load(dir.path()).expect("journal");
        alone.enqueue_planned(&indebted);
        assert_eq!(alone.most_indebted_unclaimed(Operation::SealedConsolidation, now), None);
    }

    /// Which footprint wins, and when: the planner re-derives the live file set
    /// periodically, the claim-time preflight measures what a running unit read.
    /// `None` is silence and must never erase either.
    #[test]
    fn a_planned_footprint_heals_a_pending_cell_and_never_erases_a_measured_one() {
        let (_dir, mut journal) = new_journal();
        let mut unit = task("p", 0, DAY_MICROS, Operation::SealedConsolidation);

        // A cell queued before the planner carried a footprint must not need a
        // first claim to become rankable.
        journal.enqueue(unit.key.clone(), 0, 1, 0);
        assert_eq!(journal.input_files(&unit.key), None);
        unit.input = Some(InputFootprint::new(["a", "b", "c"], 1));
        journal.enqueue_planned(&unit);
        assert_eq!(journal.input_files(&unit.key), Some(3), "the next planner tick must supply the count");

        journal.enqueue(unit.key.clone(), 0, 1, 0);
        assert_eq!(journal.input_files(&unit.key), Some(3), "`None` is silence, not evidence of no files");

        // While the unit RUNS, the claim-time measurement is the one that counts:
        // it is what `abandon_running` bisects on.
        let claimed = journal.claim_next(Operation::SealedConsolidation, 0, false).expect("claimable");
        journal.record_input(&claimed.key, InputFootprint::new(["a", "b"], 1));
        journal.enqueue_planned(&unit);
        assert_eq!(journal.input_files(&unit.key), Some(2), "a planner tick must not clobber a running unit's own measurement");
    }

    #[test]
    fn sealed_work_ages_out_of_starvation_without_becoming_oldest_first() {
        let now = 400 * DAY_MICROS;
        // Age comes from the SLICE. Anything sealing within 24 h is the live
        // frontier and is class 0 regardless — these are all older than that.
        let sealed = |project: &str, hours: i64| sealed_class(project, hours, now);

        // Inside the overdue threshold recent sealed days stay newest-first.
        let recent_new = sealed("a", 30);
        let recent_old = sealed("b", 60);
        assert!(recent_new < recent_old, "among days not yet overdue the newest still leads");

        // Past it, a day is backlog and overtakes those recent days.
        let overdue = sealed("c", 10 * 24);
        assert!(overdue < recent_new, "a day overdue past the threshold overtakes newer sealed work");

        // The backlog drains from its OLD end.
        let overdue_older = sealed("d", 30 * 24);
        assert!(overdue_older < overdue, "a backlog drains oldest-first: the older overdue day leads");

        // A day-wide unit still beats a narrow one covering the SAME day, which
        // is what lets certification get the day-wide dedup unit it requires.
        let end = now - 30 * DAY_MICROS;
        let narrow = super::scheduling_class(&task("e", end - NORMAL_SLICE_MICROS, end, Operation::SealedConsolidation), now);
        assert!(overdue_older < narrow, "width breaks the tie: the day-wide unit leads its own day's slices");

        // Past the horizon a day keeps escalating rather than falling off it. The
        // horizon buys the FLAT band beneath it: every day inside the goal window
        // ties on age, so `hole`, `-width` and `benefit` decide there.
        let ancient = sealed("f", 60 * 24);
        assert!(ancient < recent_new, "past STARVATION_HORIZON_MICROS a day still escalates, it does not fall off");

        let outside_goal_window = sealed("h", 40 * 24);
        let inside_goal_window = sealed("i", 25 * 24);
        assert!(outside_goal_window < inside_goal_window, "a day outside the window is behind in the drain, not beneath it");
        // ...and inside the window age is FLAT: 25 days and 10 days tie on `starved`.
        let (_, inside_starved, ..) = inside_goal_window;
        let (_, ten_days_starved, ..) = sealed("j", 10 * 24);
        assert_eq!(inside_starved, ten_days_starved, "the goal window is one band: `hole`/`width`/`benefit` order inside it");

        let frontier = super::scheduling_class(&task("g", now - 600_000_000, now, Operation::BaseRollup), now);
        assert!(frontier < overdue_older, "class still leads: the frontier outranks even overdue sealed work");
    }

    /// Age must keep accruing rank past the horizon instead of falling off a cliff:
    /// `starved` sits ahead of `hole`/`width`/`benefit` in a strict-priority tuple,
    /// so anything it ranks last is never compared on any other term.
    #[test]
    fn age_past_the_starvation_horizon_keeps_accruing_rank() {
        let now = 800 * DAY_MICROS;
        // Day-wide, no footprint: width and benefit tie, so only age can order these.
        let aged = |project: &str, days: i64| sealed_class(project, days * 24, now);

        assert!(aged("ancient", 71) < aged("recent", 10), "a 71-day-old unit must not rank behind a 10-day-old one");
        assert!(aged("settling", 2) > aged("recent", 10), "the 3-day floor holds: work still settling does not jump the queue");

        // Monotonic past the floor: older never ranks worse than younger, and the
        // grade saturates so ancient work ties rather than fanning out forever.
        let ladder: Vec<_> = [4, 10, 31, 32, 71, 300, 400].into_iter().map(|days| aged("p", days)).collect();
        assert!(ladder.windows(2).all(|pair| pair[1] <= pair[0]), "rank must be non-increasing in age: {ladder:?}");
        // Saturation ties the GRADED term; the tuple below it still drains oldest-first.
        let (_, at_300, ..) = aged("p", 300);
        let (_, at_400, ..) = aged("p", 400);
        assert_eq!((at_300, at_400), (0, 0), "the grade saturates instead of running out of `u8`");
    }

    /// A hole must be claimed before a day that already has tier output: sealed
    /// rollup work is otherwise newest-first, which is right for FRESHNESS and
    /// wrong for the contiguity goal.
    #[test]
    fn a_hole_outranks_a_day_that_already_has_tier_output() {
        let now = 40 * DAY_MICROS;
        let at = |project: &str, day: i64| tier_unit(project, "rollup_1h", day * DAY_MICROS, DAY_MICROS, now, Operation::DerivedRollup);
        // Both SEALED and overdue, so the OLDER day leads on age alone. The hole is
        // deliberately on the NEWER day, so `fills_a_hole` must beat that ordering.
        let seed = |journal: &mut TaskJournal| {
            journal.upsert(at("recent", 35));
            journal.upsert(at("oldhole", 20));
            journal.set_base_tier_ready(HashSet::from([
                ("source".to_owned(), "recent".to_owned(), "1970-02-05".to_owned()),
                ("source".to_owned(), "oldhole".to_owned(), "1970-01-21".to_owned()),
            ]));
        };

        // With no hole information, the older overdue day leads on age.
        assert_eq!(claim_winner(now, Operation::DerivedRollup, seed), "oldhole");

        let winner = claim_winner(now, Operation::DerivedRollup, |journal| {
            seed(journal);
            journal.set_tier_holes(HashSet::from([("source".to_owned(), "recent".to_owned(), "rollup_1h".to_owned(), "1970-02-05".to_owned())]));
        });
        assert_eq!(winner, "recent", "a missing day must outrank an OLDER day that already has output — holes rank above backlog age");
    }

    /// A partition still holding UNTAGGED tier files is a hole, whatever else is
    /// live in it: untagged files cannot be certified, so the partition is missing
    /// coverage no matter how much output sits beside them.
    #[test]
    fn a_partition_holding_untagged_files_outranks_a_re_derive() {
        let now = 40 * DAY_MICROS;
        let at = |project: &str, day: i64| tier_unit(project, "rollup_1m", day * DAY_MICROS, DAY_MICROS, now, Operation::BaseRollup);
        // The damaged cell deliberately loses every other tie — newer day, project
        // sorts last — so only the untagged rank can put it first.
        let seed = |journal: &mut TaskJournal| {
            journal.upsert(at("aaa-clean", 20));
            journal.upsert(at("zzz-damaged", 35));
        };
        assert_eq!(claim_winner(now, Operation::BaseRollup, seed), "aaa-clean");

        let winner = claim_winner(now, Operation::BaseRollup, |journal| {
            seed(journal);
            journal.set_untagged_cells("source", "rollup_1m", [("zzz-damaged".to_owned(), "1970-02-05".to_owned())]);
        });
        assert_eq!(winner, "zzz-damaged", "a partition holding unretirable untagged files must outrank a day that is merely being re-derived");
    }

    /// Damage repair leads a missing day, which leads a re-derive. Rank alone is not
    /// enough: a repair unit targets one file's uncovered span, so `-width` would
    /// otherwise put it below every day-wide backfill hole sharing its rank.
    #[test]
    fn damage_outranks_a_missing_day_which_outranks_a_re_derive() {
        let now = 40 * DAY_MICROS;
        // The damaged unit is deliberately the NARROWEST and newest, so only the
        // rank can put it first.
        let seed = |journal: &mut TaskJournal| {
            journal.upsert(tier_unit("damaged", "rollup_1m", 35 * DAY_MICROS, 600_000_000, now, Operation::BaseRollup));
            journal.upsert(tier_unit("missing", "rollup_1m", 20 * DAY_MICROS, DAY_MICROS, now, Operation::BaseRollup));
            journal.set_tier_holes(HashSet::from([("source".to_owned(), "missing".to_owned(), "rollup_1m".to_owned(), "1970-01-21".to_owned())]));
        };

        // Sharing one rank, the day-wide hole wins on width.
        assert_eq!(claim_winner(now, Operation::BaseRollup, seed), "missing");

        let winner = claim_winner(now, Operation::BaseRollup, |journal| {
            seed(journal);
            journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-02-05".to_owned())]);
        });
        assert_eq!(winner, "damaged", "a ten-minute damage repair must outrank a day-wide backfill hole");
    }

    /// Neither fusion nor subsumption may eat a damage repair: a repair unit is
    /// sized to one file's uncovered span, and the wider unit that swallows it is
    /// measured over budget by the preflight and shredded back down, so the hole
    /// ends up with no unit covering it at all.
    ///
    /// `leading_width` picks which half of the pass is exercised: a second narrow
    /// sibling makes the bucket fusible, a day-wide neighbour makes it subsumable.
    #[test_case::test_case(300_000_000, 2 ; "fusion: two narrow sealed units in one bucket are not fused")]
    #[test_case::test_case(DAY_MICROS, 1 ; "subsumption: a wider pending unit does not swallow the repair")]
    fn coarsening_leaves_damage_repairs_alone(leading_width: i64, survivors: usize) {
        let now = 40 * DAY_MICROS;
        let (_dir, mut journal) = new_journal();
        // Same bucket, two cells: the ordinary one is fair game, the damaged one
        // is not.
        for project in ["ordinary", "damaged"] {
            journal.upsert(coarsenable_unit(project, 10 * DAY_MICROS, leading_width));
            journal.upsert(coarsenable_unit(project, 10 * DAY_MICROS + 600_000_000, 300_000_000));
        }
        journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-01-11".to_owned())]);
        journal.coarsen_sealed_slices(now);
        let narrow = |project: &str| journal.tasks().filter(|t| t.key.project_id == project && t.key.slice.width() == 300_000_000).count();
        assert_eq!(narrow("damaged"), survivors, "a damaged cell's repair units must survive coarsening");
        assert_eq!(narrow("ordinary"), 0, "control: an ordinary narrow sealed unit is still fused or subsumed");
    }

    /// Damage ROTATES across cells; one cell's ladder cannot monopolise it. The
    /// selection loop matches the winning rank tuple EXACTLY, so ANY width ordering
    /// makes a single unit win every claim — damage units must all TIE so
    /// `fair_cursors` can rotate across projects.
    #[test]
    fn damage_rotates_across_cells_instead_of_draining_one() {
        let now = 40 * DAY_MICROS;
        let unit = |project: &str, start: i64, width: i64| tier_unit(project, "rollup_1m", start, width, now, Operation::BaseRollup);
        let (_dir, mut journal) = new_journal();
        // A whale ladder of very narrow children, and ONE other cell holding a
        // slightly wider hole.
        for minute in 0..6 {
            journal.upsert(unit("whale", 35 * DAY_MICROS + minute * 60_000_000, 60_000_000));
        }
        journal.upsert(unit("small", 35 * DAY_MICROS, 180_000_000));
        journal.set_untagged_cells("source", "rollup_1m", [("whale".to_owned(), "1970-02-05".to_owned()), ("small".to_owned(), "1970-02-05".to_owned())]);

        let claimed: Vec<String> = (0..6).filter_map(|_| journal.claim_next(Operation::BaseRollup, now, true).map(|task| task.key.project_id)).collect();
        assert!(claimed.iter().any(|project| project == "small"), "one cell's ladder must not monopolise damage repair; claimed {claimed:?}");
    }

    /// A cell restored via `restore_untagged_cells` must rank exactly like a freshly
    /// discovered one — the repair units are durable, their damage rank is not.
    #[test]
    fn a_restored_untagged_cell_ranks_like_a_discovered_one() {
        let now = 40 * DAY_MICROS;
        let (_dir, mut journal) = new_journal();
        journal.upsert(tier_unit("zzz-damaged", "rollup_1m", 35 * DAY_MICROS, 600_000_000, now, Operation::BaseRollup));
        journal.upsert(tier_unit("aaa-clean", "rollup_1m", 20 * DAY_MICROS, DAY_MICROS, now, Operation::BaseRollup));
        // Restored from the sidecar rather than set by a recovery pass.
        journal.restore_untagged_cells([("source".to_owned(), "zzz-damaged".to_owned(), "rollup_1m".to_owned(), "1970-02-05".to_owned())]);
        assert_eq!(journal.untagged_cells().count(), 1, "the restored cell must be readable back for persisting");
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "zzz-damaged",
            "a cell restored from the sidecar must carry the damage rank, or a restart un-prioritises the repair"
        );
    }

    /// Damage outranks starvation: `starved` grades age and is compared BEFORE
    /// `hole_rank`, so a damaged cell whose age loses is otherwise unreachable.
    /// The damaged cell here is a SETTLING one (two days sealed, under
    /// `STARVATION_MICROS`) — the only way age can now rank a cell last.
    #[test]
    fn damage_outranks_work_inside_the_starvation_window() {
        let now = 40 * DAY_MICROS;
        let seed = |journal: &mut TaskJournal| {
            // Two days sealed: under the floor, so it loses to anything in the
            // band — and past LIVE_FRONTIER_WINDOW_MICROS, so still class 1.
            journal.upsert(tier_unit("damaged", "rollup_1m", 37 * DAY_MICROS, DAY_MICROS, now, Operation::BaseRollup));
            journal.upsert(tier_unit("recent", "rollup_1m", 30 * DAY_MICROS, DAY_MICROS, now, Operation::BaseRollup));
        };
        assert_eq!(
            claim_winner(now, Operation::BaseRollup, seed),
            "recent",
            "control: work inside the starvation window leads work still settling under the floor"
        );

        let winner = claim_winner(now, Operation::BaseRollup, |journal| {
            seed(journal);
            journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-02-07".to_owned())]);
        });
        assert_eq!(winner, "damaged", "a damaged cell the age ordering ranks last must still lead, or it is unreachable");
    }

    /// Each (source, tier) owns its own slice of the untagged set:
    /// `recover_rollup_coverage` runs per source AND per tier, so a wholesale
    /// replace would leave the journal holding only whichever tier ran last.
    #[test]
    fn setting_untagged_cells_replaces_only_that_sources_tier() {
        let (_dir, mut journal) = new_journal();
        journal.set_untagged_cells("logs", "logs_1m", [("p".to_owned(), "2026-08-19".to_owned())]);
        journal.set_untagged_cells("metrics", "metrics_1m", [("p".to_owned(), "2026-08-19".to_owned())]);
        assert_eq!(journal.untagged_cells_len(), 2, "a second source must not wipe the first");
        journal.set_untagged_cells("metrics", "metrics_1m", []);
        assert_eq!(journal.untagged_cells_len(), 1, "an emptied tier clears its own cells and only those");
    }

    /// The day-keyed ready set must unblock a derived task of ANY width — a
    /// per-task flag misses, because queued work is not the width the planner
    /// assumes; a day is what the fact is actually about.
    #[test]
    fn the_base_tier_ready_set_unblocks_derived_work_of_any_width() {
        let (_dir, mut journal) = new_journal();
        const HOUR: i64 = 3_600_000_000;
        let at = |start: i64, width: i64| derived_key("p", start, width);
        journal.enqueue(at(0, HOUR), 0, 1, 0);
        journal.enqueue(at(5 * HOUR, HOUR), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: dependency-blocked");

        journal.set_base_tier_ready(HashSet::from([("source".to_owned(), "p".to_owned(), "1970-01-01".to_owned())]));
        assert_eq!(journal.base_tier_ready_len(), 1);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "an hour-wide task is unblocked by a DAY-keyed fact");

        // Wholesale replacement: coverage can go backwards, and a stale "ready"
        // would derive from a tier that is no longer there.
        journal.set_base_tier_ready(HashSet::new());
        let key = at(5 * HOUR, HOUR);
        journal.retry(&key, "requeue".to_owned(), 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "clearing the set re-blocks the work");
    }

    /// The proof must reach the tasks that actually exist, at their own width: a day
    /// can carry hour-wide pending tasks *and* a completed day-wide one, and a proof
    /// aimed at the day-wide key would land on the completed task, which is never claimed.
    #[test]
    fn the_proof_reaches_hour_wide_tasks_under_a_completed_day_unit() {
        let (_dir, mut journal) = new_journal();
        const HOUR: i64 = 3_600_000_000;
        let at = |start: i64, width: i64| derived_key("p", start, width);

        // A COMPLETE day-wide unit, with hour-wide work still pending underneath it.
        let day_unit = at(0, DAY_MICROS);
        journal.enqueue(day_unit.clone(), 0, 1, 0);
        journal.complete(&day_unit);
        journal.enqueue(at(0, HOUR), 0, 1, 0);
        journal.enqueue(at(HOUR, HOUR), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: the hour units are dependency-blocked");

        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY_MICROS), 2, "both pending hour units are proven");
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "an hour unit becomes claimable");

        journal.enqueue(at(DAY_MICROS, HOUR), 0, 1, 0);
        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY_MICROS), 0, "the following day is a different fact and stays unproven");
    }

    /// The proof latches. The frontier re-enqueues the same key without it, and
    /// silence is not evidence that coverage stopped existing.
    #[test]
    fn a_proven_base_tier_is_not_forgotten_by_a_later_enqueue() {
        let (_dir, mut journal) = new_journal();
        let key = derived_key("p", 0, 3_600_000_000);
        journal.enqueue_with_base_tier(key.clone(), 0, 1, 0, true);
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "a later uninformed enqueue must not clear the proof");
    }

    /// Attempts alone must not quarantine; only the worker's own verdict does. An
    /// attempt is counted before anything about cost is known, so a unit handed back
    /// for a cost-unrelated reason accumulates attempts like one that burned its deadline.
    #[test_case::test_case(TaskJournal::QUARANTINE_ATTEMPTS + 3, Some("source_not_flushed") => false ; "a dependency-shaped failure is not evidence about cost")]
    #[test_case::test_case(TaskJournal::QUARANTINE_ATTEMPTS + 3, None => false ; "no recorded reason is not evidence either")]
    #[test_case::test_case(TaskJournal::QUARANTINE_ATTEMPTS + 3, Some(TaskJournal::WORKER_FAILURE_REASON) => true ; "a unit the worker gave back, repeatedly, is quarantined")]
    #[test_case::test_case(1, Some(TaskJournal::WORKER_FAILURE_REASON) => false ; "a single worker failure is still a blip, not proof")]
    fn only_a_worker_verdict_quarantines_a_unit(attempts: u32, retry_reason: Option<&str>) -> bool {
        let mut unit = task("p", 0, MIN_SLICE_MICROS, Operation::DerivedRollup);
        unit.attempts = attempts;
        unit.retry_reason = retry_reason.map(str::to_owned);
        TaskJournal::is_quarantined(&unit)
    }

    /// A unit that has proven it cannot fit its deadline must not crowd out work that
    /// can. Neither the backoff nor `abandon_running`'s bisection bounds this: the
    /// backoff sets how OFTEN a doomed unit runs, not what it costs, and halving a
    /// slice cannot halve a per-file cost — it doubles the number of units paying it.
    #[test]
    fn a_unit_that_cannot_fit_its_deadline_does_not_crowd_out_one_that_can() {
        let (_dir, mut journal) = new_journal();
        // The doomed unit sorts FIRST on every tiebreak in `claim_next`, so a
        // scheduler blind to attempts is guaranteed to pick it.
        let key = upserted(&mut journal, task("a_doomed", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        journal.upsert(task("b_fresh", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        // `mark_running` counts an attempt like `claim_next` does, without also
        // rotating the project-fairness cursor.
        for _ in 0..TaskJournal::QUARANTINE_ATTEMPTS {
            assert!(journal.mark_running(&key));
            journal.retry(&key, TaskJournal::WORKER_FAILURE_REASON.to_owned(), 0);
        }

        assert_eq!(
            journal.claim_next(Operation::BaseRollup, 0, false).expect("fresh work is claimable").key.project_id,
            "b_fresh",
            "a proven-unfittable unit must not be claimed while ordinary work waits"
        );

        // Deprioritised, never abandoned: with a slot it still takes its turn, or a
        // partition whose rollup is genuinely expensive never gains coverage at all.
        assert_eq!(journal.claim_next(Operation::BaseRollup, 0, true).expect("quarantined work still runs").key, key);
    }

    /// Footprint-less shred debris carries WHOLE-FILE estimates, so its sum grows
    /// with the shredding and the fit test refuses hardest where fusing is worth
    /// most. The PARTITION CEILING alone rescues it — no migration and no deletion
    /// of queued work, since fusing preserves the work where deleting would not.
    #[test]
    fn a_footprintless_shred_fuses_once_the_partition_ceiling_is_known() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        // One-minute units, each carrying a whole-file estimate and NO footprint.
        let minutes = 600;
        for slot in 0..minutes {
            let start = day + slot * MIN_SLICE_MICROS;
            journal.upsert(task("p", start, start + MIN_SLICE_MICROS, Operation::BaseRollup).tap_mut(|unit| unit.estimated_decoded_bytes = 4_466_185_462));
        }
        let pending = |journal: &TaskJournal| live_widths(journal, Operation::BaseRollup).len();
        assert_eq!(pending(&journal), minutes as usize, "the shred is queued");

        assert_eq!(journal.coarsen_sealed_slices(now), 0, "summed whole-file estimates refuse to fuse — this is the stuck state");

        // No unit over one partition can decode more than the partition contains.
        let report = journal.coarsen_sealed_slices_capped(now, &|_project, _source, _date| Some(360 * 1024 * 1024));

        assert!(report.fused > 0, "the ceiling makes the fused unit's real cost knowable, so the day collapses: {report:?}");
        assert!(
            pending(&journal) < minutes as usize / 10,
            "600 one-minute units collapse to a handful of wide ones, not to nothing and not to 600: {} left",
            pending(&journal)
        );
        assert!(
            journal.tasks().any(|t| t.key.operation == Operation::BaseRollup && t.key.slice.width() > MIN_SLICE_MICROS),
            "the work SURVIVES as wider units — fusing preserves it where a deleting migration would not"
        );
    }

    /// The stale-estimate migration must run exactly once and free fusion: estimates
    /// predating `slice_share_of_file` measured WHOLE files, so summing a split day's
    /// children prices it at many whole days and no width can ever fit.
    #[test]
    fn clearing_stale_estimates_runs_once_and_lets_a_split_day_fuse_again() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        // 144 children each carrying the WHOLE day's estimate.
        enqueue_run(&mut journal, "p", day, NORMAL_SLICE_MICROS, DAY_MICROS / NORMAL_SLICE_MICROS, MAX_DECODED_BYTES, Operation::BaseRollup);
        assert_eq!(journal.coarsen_sealed_slices(now), 0, "summed stale estimates must refuse to fuse — that is the bug");

        let cleared = journal.clear_stale_estimates().expect("first run migrates");
        assert_eq!(cleared, 144, "every pending unit's estimate is suspect");
        assert!(journal.clear_stale_estimates().is_none(), "the migration must not run twice");

        assert!(journal.coarsen_sealed_slices(now) > 0, "with the stale numbers gone the day must fuse");
        let widths = live_widths(&journal, Operation::BaseRollup);
        assert_eq!(widths, vec![DAY_MICROS], "one day-wide unit should remain, got {widths:?}");
    }

    /// A collapse must SURVIVE a reload: `checkpoint` appends dirty tasks to a WAL
    /// and `JournalRecord::Task` can only upsert — there is no record meaning "this
    /// task is gone" — so a pass that REMOVES tasks must `compact()`, not checkpoint.
    #[test]
    fn a_collapsed_queue_stays_collapsed_across_a_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        let now = 10 * DAY_MICROS;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            enqueue_run(&mut journal, "p", day, NORMAL_SLICE_MICROS, DAY_MICROS / NORMAL_SLICE_MICROS, 16, Operation::BaseRollup);
            journal.checkpoint().expect("persist the fine slices");
            assert!(journal.coarsen_sealed_slices(now) > 0, "the day must collapse");
            journal.compact().expect("a pass that REMOVES must rewrite the snapshot");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let live = live_widths(&reloaded, Operation::BaseRollup).len();
        assert_eq!(live, 1, "the collapse must survive the reload; got {live} units back");
    }

    /// Enqueues `count` contiguous `width`-wide units from `start`, each priced at `bytes`.
    fn enqueue_run(journal: &mut TaskJournal, project: &str, start: i64, width: i64, count: i64, bytes: u64, operation: Operation) {
        for slot in 0..count {
            let at = start + slot * width;
            journal.enqueue(task(project, at, at + width, operation).key, 0, bytes, 0);
        }
    }

    /// Widths of every still-claimable unit for `operation`, in journal order.
    fn live_widths(journal: &TaskJournal, operation: Operation) -> Vec<i64> {
        journal
            .tasks()
            .filter(|t| t.key.operation == operation && matches!(t.state, TaskState::Pending | TaskState::Retry))
            .map(|t| t.key.slice.width())
            .collect()
    }

    /// Widths of `project`'s pending units whose slice starts inside `day`.
    fn pending_widths(journal: &TaskJournal, project: &str, day: i64) -> Vec<i64> {
        journal
            .tasks()
            .filter(|t| {
                t.state == TaskState::Pending
                    && t.key.project_id == project
                    && t.key.slice.start_micros >= day * DAY_MICROS
                    && t.key.slice.start_micros < (day + 1) * DAY_MICROS
            })
            .map(|t| t.key.slice.width())
            .collect()
    }

    /// Claims `ticks` times, re-opening each claimed unit so every tick faces the
    /// same choice, and counts the claims matching `wanted`.
    fn claims_matching(journal: &mut TaskJournal, operation: Operation, now: i64, ticks: usize, wanted: impl Fn(&MaintenanceTask) -> bool) -> usize {
        let mut matched = 0;
        for _ in 0..ticks {
            let Some(claimed) = journal.claim_next(operation, now, true) else { continue };
            matched += usize::from(wanted(&claimed));
            journal.complete(&claimed.key);
            journal.enqueue(claimed.key.clone(), 0, 0, 0);
        }
        matched
    }

    /// Enqueues a unit stamped with the file set it reads, so fusion can price a
    /// group of children as one scan.
    fn enqueue_with_input(journal: &mut TaskJournal, key: &TaskKey, bytes: u64, input: InputFootprint) {
        journal.enqueue(key.clone(), 0, bytes, 0);
        let index = journal.task_indices[key];
        journal.snapshot.tasks[index].input = Some(input);
    }

    /// Derived hygiene work is re-derived from the file scan and never persisted;
    /// durable work (Repair, BaseRollup) always is.
    #[test]
    fn derived_hygiene_is_not_persisted_but_durable_work_is() {
        let (dir, mut journal) = new_journal();
        let day = 3 * DAY_MICROS;
        let of = |operation| task("p", day, day + DAY_MICROS, operation).key;
        for operation in [Operation::HotPacking, Operation::SealedConsolidation, Operation::Repair, Operation::BaseRollup] {
            journal.enqueue(of(operation), 0, 16, 0);
        }
        assert_eq!(journal.tasks().count(), 4, "all four exist in memory");
        journal.checkpoint().expect("persist");
        drop(journal);
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let survived: Vec<_> = reloaded.tasks().map(|t| t.key.operation).collect();
        assert!(survived.contains(&Operation::Repair), "Repair must survive: it stages output before committing");
        assert!(survived.contains(&Operation::BaseRollup), "BaseRollup must survive");
        assert!(!survived.contains(&Operation::HotPacking), "HotPacking is re-derived from the file scan, not reloaded");
        assert!(!survived.contains(&Operation::SealedConsolidation), "SealedConsolidation is re-derived from the file scan");
        assert_eq!(survived.len(), 2, "exactly the durable operations, got {survived:?}");
    }

    /// A removal must survive a reload through `checkpoint` ALONE — without a
    /// tombstone in the WAL, replay resurrects every removed task.
    #[test]
    fn a_removal_survives_a_reload_without_compacting() {
        let (dir, mut journal) = new_journal();
        let day = 3 * DAY_MICROS;
        enqueue_run(&mut journal, "p", day, NORMAL_SLICE_MICROS, 6, 16, Operation::BaseRollup);
        journal.checkpoint().expect("persist the slices");
        let dropped = journal.retain_tasks(|t| t.key.slice.start_micros != day);
        assert_eq!(dropped, 1, "exactly one unit should have been dropped");
        journal.checkpoint().expect("persist the REMOVAL — no compact");
        drop(journal);
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.tasks().count(), 5, "the removal must survive; a tombstone-less WAL replays all 6");
        assert!(!reloaded.tasks().any(|t| t.key.slice.start_micros == day), "the removed unit specifically must be gone, not merely some unit");
    }

    /// Remove-then-recreate replays as CREATED, not as removed: `coarsen_to_width`
    /// drops a bucket's members then writes a fused unit that can reuse a dropped key.
    #[test]
    fn a_task_recreated_after_removal_survives_the_reload() {
        let (dir, mut journal) = new_journal();
        let day = 3 * DAY_MICROS;
        let key = task("p", day, day + NORMAL_SLICE_MICROS, Operation::BaseRollup).key;
        journal.enqueue(key.clone(), 0, 16, 0);
        journal.checkpoint().expect("persist");
        journal.retain_tasks(|t| t.key != key);
        journal.enqueue(key.clone(), 0, 32, 0);
        journal.checkpoint().expect("persist both the removal and the re-creation");
        drop(journal);
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.tasks().count(), 1, "the re-created task must be present exactly once");
        assert_eq!(reloaded.state(&key), Some(TaskState::Pending));
    }

    /// A fused unit inherits its members' AGE, not the moment of fusion: stamped
    /// with `now` it could never escalate past `STARVATION_MICROS`.
    #[test]
    fn a_fused_unit_inherits_the_age_of_the_work_it_replaces() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;
        // Members created five days ago, in milliseconds.
        let old_ms = u64::try_from((now - 5 * DAY_MICROS).div_euclid(1_000)).expect("positive");

        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, old_ms + u64::try_from(slot).unwrap_or(0));
        }
        assert!(journal.coarsen_sealed_slices(now) > 0, "the day must fuse");

        let fused: Vec<_> =
            journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry)).collect();
        assert_eq!(fused.len(), 1, "one day-wide unit should remain");
        assert_eq!(
            fused[0].created_unix_ms, old_ms,
            "the fused unit must carry the OLDEST member's age; stamped with `now` it can never escalate past STARVATION_MICROS"
        );
    }

    /// A day-wide unit must SUBSUME the ten-minute units inside it; `coarsen_to_width`
    /// alone refuses a bucket already covered and so converges to nothing.
    #[test]
    fn a_pending_day_unit_subsumes_the_ten_minute_units_inside_it() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        enqueue_run(&mut journal, "p", day, NORMAL_SLICE_MICROS, DAY_MICROS / NORMAL_SLICE_MICROS, 16, Operation::BaseRollup);
        journal.enqueue(task("p", day, day + DAY_MICROS, Operation::BaseRollup).key, 0, 16, 0);
        assert_eq!(journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count(), 145, "144 slices plus the day unit");

        journal.coarsen_sealed_slices(now);

        let live = live_widths(&journal, Operation::BaseRollup);
        assert_eq!(live, vec![DAY_MICROS], "the day unit must absorb all 144 slices, leaving one unit for the cell; got {live:?}");
    }

    /// A day unit that is not itself queued work subsumes nothing: a SUPERSEDED one
    /// would delete the children the split just created, leaving the cell with no
    /// queued work at all, and a COMPLETE one would silently lose the rebuild a
    /// narrower later invalidation asks for.
    #[test_case::test_case(true ; "a superseded day unit does not subsume the children that replaced it")]
    #[test_case::test_case(false ; "a complete day unit does not subsume a later invalidation")]
    fn a_non_pending_day_unit_does_not_subsume_the_work_under_it(superseded: bool) {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        let parent = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, if superseded { 0 } else { 16 }, 0);
        let expected = if superseded {
            journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
            TaskState::Superseded
        } else {
            journal.complete(&parent);
            let start = day + 5 * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
            TaskState::Complete
        };
        assert_eq!(journal.state(&parent), Some(expected), "the parent must be in that state for this to prove anything");
        let children = journal.tasks().filter(|t| t.state == TaskState::Pending && t.key.operation == Operation::BaseRollup).count();
        assert!(children > 0, "precondition: narrow pending work exists under the parent");

        journal.coarsen_sealed_slices(now);

        assert!(
            journal.tasks().any(|t| matches!(t.state, TaskState::Pending | TaskState::Retry) && t.key.operation == Operation::BaseRollup),
            "the work under a parent that is not queued work must survive"
        );
    }

    /// A sealed day too big to scan as one unit lands at a narrower width, not back
    /// at ten minutes: on an uncompacted partition every file spans the whole day,
    /// so a ten-minute slice reads exactly the files a day unit would.
    #[test]
    fn a_day_over_the_decode_budget_lands_at_a_narrower_width_not_at_ten_minutes() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;

        // One sealed day of ten-minute slices whose summed estimate busts the
        // day budget but leaves each six-hour quarter comfortably inside it.
        let per_slice = MAX_DECODED_BYTES / 96;
        enqueue_run(&mut journal, "p", 3 * DAY_MICROS, NORMAL_SLICE_MICROS, DAY_MICROS / NORMAL_SLICE_MICROS, per_slice, Operation::BaseRollup);
        let minted = journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count();
        assert_eq!(minted, 144, "the live path mints one unit per ten-minute slice");

        journal.coarsen_sealed_slices(now);

        let widths = pending_widths(&journal, "p", 3);
        assert_eq!(widths.len(), 4, "a day over budget must fuse into its four six-hour quarters, got {widths:?}");
        assert!(widths.iter().all(|width| *width == 6 * 60 * 60 * 1_000_000), "expected six-hour units, got {widths:?}");
    }

    /// A sealed day's ten-minute leftovers collapse into one day unit — but never
    /// one that would undo a split, which would loop split/coarsen forever.
    #[test]
    fn a_sealed_days_fine_slices_collapse_but_never_undo_a_split() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;

        // Day 3: ten-minute leftovers, nothing coarse above them.
        enqueue_run(&mut journal, "p", 3 * DAY_MICROS, NORMAL_SLICE_MICROS, 6, 10, Operation::BaseRollup);
        // Day 5: same, but its day unit was already split into these children.
        enqueue_run(&mut journal, "p", 5 * DAY_MICROS, NORMAL_SLICE_MICROS, 6, 10, Operation::BaseRollup);
        let parent = task("p", 5 * DAY_MICROS, 6 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "the parent must be superseded for the guard to be exercised");

        let collapsed = journal.coarsen_sealed_slices(now);
        assert!(collapsed >= 6, "day 3's leftovers must collapse, got {collapsed}");

        let day_five = pending_widths(&journal, "p", 5);
        assert_eq!(pending_widths(&journal, "p", 3), vec![DAY_MICROS], "day 3 is now exactly one day-sized unit");
        assert!(day_five.iter().all(|w| *w < DAY_MICROS), "day 5 already had a day unit that was SPLIT; recreating it would loop forever, got {day_five:?}");
    }

    /// `attempts >= 2` quarantines a unit and floors its backoff at the operation
    /// deadline; the one-shot migration clears that for the repair queue only.
    #[test]
    fn the_repair_migration_unquarantines_the_queue_it_is_for() {
        let (_dir, mut journal) = new_journal();
        let wedged = task("p", 0, DAY_MICROS, Operation::Repair).tap_mut(|unit| {
            unit.attempts = 100;
            unit.state = TaskState::Retry;
            unit.retry_reason = Some("worker_error".to_owned());
            unit.deadline_micros = i64::MAX;
        });
        let repair = upserted(&mut journal, wedged);
        // A neighbour that the migration must not touch.
        let untouched = upserted(&mut journal, task("p", 0, DAY_MICROS, Operation::Dedup).tap_mut(|unit| unit.attempts = 7));

        assert_eq!(journal.reset_repair_attempts(), Some(1), "one repair unit reset");
        let after = journal.tasks().find(|candidate| candidate.key == repair).expect("still queued");
        assert_eq!(after.attempts, 0, "quarantine is keyed on attempts");
        assert_eq!(after.deadline_micros, 0, "and the stamped backoff floor must go with it");
        assert_eq!(journal.attempts(&untouched), 7, "other operations keep their history");
        assert_eq!(journal.reset_repair_attempts(), None, "the migration must not run twice");

        // A journal with no repair queue must not spend the cursor, or a boot
        // that precedes the queue consumes the one shot.
        let (_empty, mut fresh) = new_journal();
        assert_eq!(fresh.reset_repair_attempts(), None, "nothing to forgive, nothing spent");
        fresh.upsert(task("p", 0, DAY_MICROS, Operation::Repair));
        assert_eq!(fresh.reset_repair_attempts(), Some(1), "and the cursor is still available when the queue arrives");
    }

    /// A busy pool admits only small work, so an oversized unit is never admitted
    /// into a position where it would have to be killed — but the ceiling is only
    /// meaningful if it still admits the sizes the splitter actually produces.
    #[test]
    fn the_admission_ceiling_shrinks_as_the_pool_fills() {
        const CAPACITY: u64 = MAX_DECODED_BYTES * 16;
        let ceiling = |free| super::occupancy_scaled_ceiling(free, CAPACITY);
        assert_eq!(ceiling(CAPACITY), MAX_DECODED_BYTES, "an idle pool admits the largest unit");
        assert!(ceiling(CAPACITY / 4) < MAX_DECODED_BYTES, "a three-quarters-full pool admits less");
        assert!(
            ceiling(CAPACITY / 4) >= MAX_DECODED_BYTES / 16,
            "and still admit a small one: a hygiene-sized unit must fit under the ceiling of a busy pool, or the fleet hot-loops on admission"
        );
        assert!(ceiling(0) >= MAX_DECODED_BYTES / 16, "but a full pool must still admit the small hygiene bins, or file counts run away");
        assert!(ceiling(CAPACITY / 2) > ceiling(CAPACITY / 4), "and the ceiling must be monotone in free space");
        // One single unit reserved — the least busy a working pool can be.
        let one_reserved = ceiling(CAPACITY - MAX_DECODED_BYTES);
        assert!(
            one_reserved >= MAX_DECODED_BYTES,
            "a pool with ONE unit reserved refused a max-sized request (ceiling {one_reserved} < {MAX_DECODED_BYTES}); \
             every unit the splitter produces is priced at exactly that, so they can only ever run on a perfectly empty pool"
        );
    }

    /// Fusion must not refuse a group priced against its PARTITION just because
    /// the partition is bigger than one unit's decode budget: the members do not
    /// avoid that cost by staying apart, they each pay it.
    #[test]
    fn a_partition_priced_group_fuses_even_when_the_partition_is_large() {
        // A day shredded into ten-minute slices, each modelled at a fraction of
        // a partition that is far bigger than MAX_DECODED_BYTES.
        let shredded_day = |operation| {
            let (dir, mut journal) = new_journal();
            enqueue_run(&mut journal, "p", DAY_MICROS, NORMAL_SLICE_MICROS, 24, MAX_DECODED_BYTES / 2, operation);
            (dir, journal)
        };
        let (_dir, mut journal) = shredded_day(Operation::Dedup);
        let big_partition = MAX_DECODED_BYTES * 20;

        // The exemption is dedup's alone until the rollup runner honours `hash_shard`.
        let (_rollup_dir, mut rollup) = shredded_day(Operation::BaseRollup);
        assert_eq!(rollup.coarsen_sealed_slices_capped(10 * DAY_MICROS, &|_, _, _| Some(big_partition)).fused, 0, "rollup keeps the old rule");

        // Without storage access the old rule stands — the price is a sum over
        // files that might not overlap.
        let (_blind_dir, mut blind) = shredded_day(Operation::Dedup);
        assert_eq!(blind.coarsen_sealed_slices_capped(10 * DAY_MICROS, &|_, _, _| None).fused, 0, "a sum over unknown files must still be refused");

        let report = journal.coarsen_sealed_slices_capped(10 * DAY_MICROS, &|_, _, _| Some(big_partition));
        assert!(report.fused > 0, "a group known to share one partition must fuse: {report:?}");
        assert_eq!(report.over_budget, 0, "and must not be counted against the decode budget it cannot honour");
    }

    /// Bisecting a dedup unit below the width where it stops shedding FILES
    /// manufactures slivers that each pay the same whole-partition scan.
    #[test]
    fn a_dedup_unit_shards_by_key_instead_of_slivering_time() {
        let over_budget = MAX_DECODED_BYTES * 4;
        let ten_minutes = task("p", 0, NORMAL_SLICE_MICROS, Operation::Dedup);
        let children = byte_bounded_units(&ten_minutes, over_budget);
        assert!(children.iter().all(|child| child.hash_shards > 1), "at the floor, dedup sheds work by KEY, not by halving time again");
        assert!(children.iter().all(|child| child.key.slice.width() == NORMAL_SLICE_MICROS), "and the slice must not narrow further");

        // Above the floor it still bisects — the floor is a floor, not a ban.
        let hour = task("p", 0, 60 * 60 * 1_000_000, Operation::Dedup);
        let halves = byte_bounded_units(&hour, over_budget);
        assert_eq!(halves.len(), 2, "an hour-wide unit still halves");
        assert!(halves.iter().all(|child| child.hash_shards <= 1));

        // Other operations keep the old floor: their cost model is different.
        let rollup = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        assert_eq!(byte_bounded_units(&rollup, over_budget).len(), 2, "only dedup's cost is partition-scoped");
    }

    /// Every operation shares the fleet deadline except Repair, whose ORDER BY
    /// blocks on a whole file.
    #[test]
    fn only_repair_gets_a_window_longer_than_the_fleet_default() {
        for operation in [Operation::Dedup, Operation::HotPacking, Operation::SealedConsolidation, Operation::BaseRollup, Operation::DerivedRollup] {
            assert_eq!(operation_deadline_secs(operation), 15 * 60, "{operation:?} shares the fleet default");
        }
        assert_eq!(operation_deadline_secs(Operation::Repair), 60 * 60, "repair alone is longer: ORDER BY is blocking on a whole file");
        assert!(operation_deadline_secs(Operation::Repair) <= MAX_OPERATION_DEADLINE_SECS);
    }

    /// A Repair unit rewrites ONE whole file, so halving its slice halves nothing:
    /// `coordinator_compaction_files` still hands every child the same `take(1)`.
    #[test]
    fn a_repair_unit_is_never_bisected_because_its_cost_is_a_whole_file() {
        let (_dir, mut journal) = new_journal();
        let repair = task("p", DAY_MICROS, DAY_MICROS + DAY_MICROS, Operation::Repair).key;
        journal.enqueue(repair.clone(), 0, MAX_DECODED_BYTES, 0);
        let before = journal.snapshot.tasks.len();

        // Day-wide, and measured far over budget: every condition a split needs.
        assert!(!journal.split_time_task(&repair, MAX_DECODED_BYTES * 8, None), "repair must decline to split");
        assert_eq!(journal.snapshot.tasks.len(), before, "a declined split must mint no children");
        assert_eq!(journal.state(&repair), Some(TaskState::Pending), "and must not supersede the parent");
    }

    #[test_case::test_case(Operation::Dedup, "worker_error")]
    #[test_case::test_case(Operation::BaseRollup, "schema_error")]
    #[test_case::test_case(Operation::DerivedRollup, "source_not_flushed")]
    fn coarsening_preserves_retry_state(operation: Operation, reason: &str) {
        let now = 10 * DAY_MICROS;
        for deadline in [now - 1, now + 60_000_000] {
            let (dir, mut journal) = new_journal();
            let retry = task("p", DAY_MICROS, DAY_MICROS + NORMAL_SLICE_MICROS, operation).key;
            let neighbour = task("p", DAY_MICROS + NORMAL_SLICE_MICROS, DAY_MICROS + 2 * NORMAL_SLICE_MICROS, operation).key;
            let other = task("q", DAY_MICROS, DAY_MICROS + NORMAL_SLICE_MICROS, operation).key;
            for key in [&retry, &neighbour, &other] {
                journal.enqueue(key.clone(), 0, 1, 0);
            }
            assert!(journal.mark_running(&retry));
            journal.retry(&retry, reason.to_owned(), deadline);
            journal.coarsen_sealed_slices(now);
            journal.checkpoint().expect("checkpoint");
            let retry_state = |journal: &TaskJournal, why: &str| {
                let retained = journal.tasks().find(|t| t.key == retry).expect(why);
                (retained.state, retained.attempts, retained.deadline_micros, retained.retry_reason.clone())
            };
            let expected = (TaskState::Retry, 1, deadline, Some(reason.to_owned()));
            let loaded = TaskJournal::load(dir.path()).expect("reload");
            assert_eq!(retry_state(&loaded, "coarsening must retain the retry"), expected);
            assert!(loaded.tasks().any(|t| t.key.project_id == "q" && t.key.slice.width() > NORMAL_SLICE_MICROS), "unrelated pending work still coarsens");
            assert!(
                !loaded.tasks().any(|t| t.key.project_id == "p"
                    && t.key != retry
                    && t.key.slice.start_micros <= retry.slice.start_micros
                    && t.key.slice.end_micros >= retry.slice.end_micros),
                "coarsening must not bypass the retry through a covering parent"
            );
            // A wider unit queued independently must not erase the retry either.
            let parent = task("p", DAY_MICROS, 2 * DAY_MICROS, operation).key;
            journal.enqueue(parent, 0, 1, 0);
            journal.coarsen_sealed_slices(now);
            assert_eq!(retry_state(&journal, "subsumption must retain the retry"), expected);
        }
    }

    /// Coarsening must not build a day unit that cannot finish: a day over the
    /// decode budget publishes nothing and times out repeatedly, which is strictly
    /// worse than the slices it replaced.
    #[test]
    fn coarsening_skips_a_day_that_would_not_fit_the_decode_budget() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;

        // Day 1: six slices whose combined estimate fits comfortably. Must fuse.
        enqueue_run(&mut journal, "p", DAY_MICROS, NORMAL_SLICE_MICROS, 6, MAX_DECODED_BYTES / 12, Operation::BaseRollup);
        // Day 4: six slices that together blow the budget. Must stay as slices.
        enqueue_run(&mut journal, "q", 4 * DAY_MICROS, NORMAL_SLICE_MICROS, 6, MAX_DECODED_BYTES / 2, Operation::BaseRollup);

        journal.coarsen_sealed_slices(now);

        let over_budget = pending_widths(&journal, "q", 4);
        assert_eq!(pending_widths(&journal, "p", 1), vec![DAY_MICROS], "a day that fits must fuse into one unit");
        assert_eq!(over_budget.len(), 6, "a day over budget must keep its slices — they finish, a too-big day unit never does");
        assert!(over_budget.iter().all(|w| *w < DAY_MICROS));
    }

    /// A day shredded to the one-minute floor must collapse. Each child's estimate
    /// is honest (one row group IS the file), but summing 1,440 children that read
    /// the SAME files charges one scan 1,440 times and refuses every width.
    /// [`InputFootprint`] is what lets fusion charge that file set once.
    #[test]
    fn a_day_shredded_to_the_minute_floor_collapses() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let footprint = InputFootprint::new((0..35).map(|n| format!("part-{n}.parquet")), MAX_DECODED_BYTES / 2);
        // A split stamps its children with what the PARENT read. Exercised on a
        // different day so the halves it leaves cannot subsume the shred below,
        // which would collapse it by covering rather than by fusion.
        let parent = task("p", 3 * DAY_MICROS, 4 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        assert!(journal.split_time_task(&parent, 391 * 1024 * 1024 * 1024, Some(footprint)));
        assert!(journal.tasks().filter(|t| t.state == TaskState::Pending).all(|t| t.input == Some(footprint)), "children inherit what they read");

        // 1,440 contiguous one-minute units over the same file set, nothing wider.
        for minute in 0..1_440 {
            let start = DAY_MICROS + minute * MIN_SLICE_MICROS;
            let key = task("p", start, start + MIN_SLICE_MICROS, Operation::BaseRollup).key;
            enqueue_with_input(&mut journal, &key, 282 * 1024 * 1024, footprint);
        }
        let shredded = |journal: &TaskJournal| pending_widths(journal, "p", 1).len();
        assert!(shredded(&journal) > 1_000, "precondition: the day is shredded to the floor");

        // One pass is enough because every child names the same file set.
        journal.coarsen_sealed_slices(now);
        assert!(shredded(&journal) <= 24, "a contiguous shredded day must collapse; {} units remain", shredded(&journal));
    }

    /// Fusion charges a file set once, and only when the members agree it IS
    /// one set. Two children over different files still sum — a fused unit
    /// would read both, and under-pricing that is how a split/fuse loop starts.
    #[test]
    fn fusion_charges_a_shared_file_set_once_and_disjoint_sets_twice() {
        let now = 10 * DAY_MICROS;
        let half = MAX_DECODED_BYTES / 2 + 1;
        let fuse = |footprints: [InputFootprint; 2]| {
            let (_dir, mut journal) = new_journal();
            for (slot, footprint) in footprints.into_iter().enumerate() {
                let slot = i64::try_from(slot).unwrap_or(0);
                let start = DAY_MICROS + slot * NORMAL_SLICE_MICROS;
                let key = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key;
                enqueue_with_input(&mut journal, &key, half, footprint);
            }
            journal.coarsen_sealed_slices_reporting(now)
        };
        let shared = InputFootprint::new(["a.parquet", "b.parquet"], half);
        // Order-independent: a snapshot lists files in no fixed order, and two
        // units over the same partition must still recognise each other.
        assert_eq!(shared, InputFootprint::new(["b.parquet", "a.parquet"], half));
        assert_eq!(fuse([shared, shared]).fused, 2, "same files, one scan, one charge");
        let disjoint = fuse([shared, InputFootprint::new(["c.parquet"], half)]);
        assert_eq!((disjoint.fused, disjoint.over_budget > 0), (0, true), "different files must still sum");
    }

    /// A shredded day fuses in one pass once its summed estimate is capped by the
    /// real partition size — the sum counts the same files once per child.
    #[test]
    fn a_shredded_day_collapses_once_the_estimate_is_capped_by_its_partition() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        let prod_estimate = 282_280_533u64; // one 60-second slice, whole-file accounting
        enqueue_run(&mut journal, "p", DAY_MICROS, MIN_SLICE_MICROS, 1_440, prod_estimate, Operation::BaseRollup);
        let partition = 386_547_056u64; // 35 files, 0.36 GB — what the day actually holds
        let report = journal.coarsen_sealed_slices_capped(now, &|_, _, _| Some(partition));
        let remaining = pending_widths(&journal, "p", 1).len();
        assert!(
            remaining < 1440,
            "the shredded day must collapse once its estimate is bounded by the partition; {remaining} remain (fused={} over_budget={})",
            report.fused,
            report.over_budget
        );
        assert_eq!(remaining, 1, "and it should land as ONE day-wide unit, got {remaining}");
    }

    /// The partition cap only removes double-counting: a partition whose REAL size is
    /// over budget still cannot be scanned in one unit, so the day keeps its slices.
    #[test]
    fn the_partition_ceiling_does_not_fuse_a_genuinely_oversized_day() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;
        enqueue_run(&mut journal, "p", DAY_MICROS, NORMAL_SLICE_MICROS, 6, MAX_DECODED_BYTES / 2, Operation::BaseRollup);
        // The partition really is 4 GB; the sum was not double-counting here.
        journal.coarsen_sealed_slices_capped(now, &|_, _, _| Some(4 * 1024 * 1024 * 1024));
        let widths = pending_widths(&journal, "p", 1);
        assert_eq!(widths.len(), 6, "a genuinely oversized day must keep its slices, got {widths:?}");
        assert!(widths.iter().all(|w| *w < DAY_MICROS));
    }

    /// Removing a rollup spec must retire its queued work and touch nothing else;
    /// every `_v2` -> `_v3` rename otherwise leaves claimable no-op residue.
    #[test]
    fn removing_a_spec_retires_its_queued_work_and_nothing_else() {
        let (_dir, mut journal) = new_journal();
        let tiered = |table: &str, slot: i64, operation| task_in(table, "p", slot * DAY_MICROS, (slot + 1) * DAY_MICROS, operation).key;
        let gone = tiered("src_rollup_dead_1h_v1", 1, Operation::DerivedRollup);
        let live = tiered("src_rollup_live_1m_v3", 2, Operation::BaseRollup);
        let raw = tiered("src", 3, Operation::Dedup);
        let done = tiered("src_rollup_dead_1h_v1", 4, Operation::DerivedRollup);
        for key in [&gone, &live, &raw, &done] {
            journal.enqueue(key.clone(), 0, 1, 0);
        }
        journal.complete(&done);

        let declared: HashSet<String> = ["src_rollup_live_1m_v3".to_owned()].into_iter().collect();
        assert_eq!(journal.retire_undeclared_tiers(&declared), 1, "exactly the undeclared tier's live work");
        assert_eq!(journal.state(&gone), None, "the undeclared tier's queued unit is gone");
        assert_eq!(journal.state(&live), Some(TaskState::Pending), "a declared tier is untouched");
        assert_eq!(journal.state(&raw), Some(TaskState::Pending), "a non-tier table is never considered");
        assert_eq!(journal.state(&done), Some(TaskState::Complete), "history is untouched");

        // An empty declared set is what an unloaded registry looks like, and it
        // must retire NOTHING — otherwise a startup ordering change silently
        // deletes the whole queue.
        assert_eq!(journal.retire_undeclared_tiers(&HashSet::new()), 0, "an empty registry must never retire anything");
        assert_eq!(journal.state(&live), Some(TaskState::Pending));
    }

    /// The orphan repair must run exactly once per source and persist that claim
    /// BEFORE the caller does the work, so a restart cannot re-force every cell.
    #[test]
    fn the_orphan_repair_claims_itself_once_and_survives_a_reload() {
        let (dir, mut journal) = new_journal();
        assert!(journal.repair_orphaned_coverage_once("otel_logs_and_spans"), "first call claims it");
        assert!(!journal.repair_orphaned_coverage_once("otel_logs_and_spans"), "second call in the same process must not");
        // PER SOURCE: the caller loops over sources, so one global cursor would let
        // whichever source runs first consume the repair the others still need.
        assert!(journal.repair_orphaned_coverage_once("otel_metrics"), "a different source claims independently");
        drop(journal);
        let mut reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert!(!reloaded.repair_orphaned_coverage_once("otel_logs_and_spans"), "a restart must not re-run the repair");
    }

    /// The damage repair's cursor is a consumed-PREFIX index, per source, monotonic,
    /// and it survives a restart; it must not share state with the orphan repair.
    #[test]
    fn the_damage_repair_cursor_is_a_per_source_prefix_that_survives_a_reload() {
        const DAMAGE: &str = TaskJournal::DAMAGE_REPAIR_MIGRATION;
        let (dir, mut journal) = new_journal();
        assert!(journal.repair_orphaned_coverage_once("otel_logs_and_spans"), "orphan repair claims");
        assert_eq!(journal.repair_cursor(DAMAGE, "otel_logs_and_spans"), 0, "the orphan repair's cursor must not consume the damage list");
        journal.advance_repair_cursor(DAMAGE, "otel_logs_and_spans", 24).expect("advance");
        assert_eq!(journal.repair_cursor(DAMAGE, "otel_metrics"), 0, "a different source is consumed independently");
        // Monotonic: `SourceCursor` replay folds with `max()`, and a pass
        // that resolved less than an earlier one must never rewind the list.
        journal.advance_repair_cursor(DAMAGE, "otel_logs_and_spans", 3).expect("advance");
        assert_eq!(journal.repair_cursor(DAMAGE, "otel_logs_and_spans"), 24);
        drop(journal);
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.repair_cursor(DAMAGE, "otel_logs_and_spans"), 24, "a restart resumes at the prefix, it does not start over");
    }

    /// A COMPLETE day unit must not block coarsening, or the queue fills with
    /// sub-day slices that can never collapse — while a SUPERSEDED one must block
    /// it, or the split is undone and the two fight forever.
    #[test]
    fn a_completed_day_unit_does_not_block_coarsening_but_a_superseded_one_does() {
        let (_dir, mut journal) = new_journal();
        let now = 10 * DAY_MICROS;

        // Day 2: a day unit that already ran to completion, plus fresh slices
        // minted by a later invalidation. These MUST collapse.
        let done = task("p", 2 * DAY_MICROS, 3 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(done.clone(), 0, 0, 0);
        journal.complete(&done);
        assert_eq!(journal.state(&done), Some(TaskState::Complete), "precondition");
        enqueue_run(&mut journal, "p", 2 * DAY_MICROS, NORMAL_SLICE_MICROS, 6, 10, Operation::BaseRollup);

        // Day 6: a day unit SPLIT because it was too big. Its children must NOT
        // collapse, or the split is undone and the two fight forever.
        let parent = task("p", 6 * DAY_MICROS, 7 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "precondition");

        journal.coarsen_sealed_slices(now);

        assert_eq!(pending_widths(&journal, "p", 2), vec![DAY_MICROS], "a completed day must re-coarsen to one day unit, not stay as slices");
        assert!(pending_widths(&journal, "p", 6).iter().all(|w| *w < DAY_MICROS), "a SPLIT day's children must stay split — recoarsening them loops forever");
    }

    /// The sealed reservation yields while the frontier is behind (frontier lag is a
    /// per-query cost), and returns on its own once it is not — but never to zero.
    #[test]
    fn the_sealed_reservation_yields_while_the_frontier_is_behind() {
        let now = 10 * DAY_MICROS;
        let claims = |lag: u64| {
            let (_dir, mut journal) = new_journal();
            // One frontier slice (ends at `now`) and one sealed day, both eligible.
            let frontier = task("p", now - 600_000_000, now, Operation::BaseRollup).key.clone();
            let sealed = task("p", now - 5 * DAY_MICROS, now - 4 * DAY_MICROS, Operation::BaseRollup).key.clone();
            journal.enqueue(frontier, 0, 0, 0);
            journal.enqueue(sealed, 0, 0, 0);
            journal.frontier_lag_secs.store(lag, std::sync::atomic::Ordering::Relaxed);
            claims_matching(&mut journal, Operation::BaseRollup, now, 8, |claimed| !is_live_frontier(claimed.key.slice, now))
        };

        assert!(claims(0) > 0, "a keeping-up frontier must still leave sealed work its reserved share");
        assert!(
            claims(FRONTIER_LAG_BUDGET_SECS + 1) < claims(0),
            "past the lag budget the frontier must take a bigger share, because a frontier that never finishes today \
             guarantees tomorrow's yesterday is holed and coverage can never reach thirty contiguous days"
        );
        assert!(
            claims(FRONTIER_LAG_BUDGET_SECS + 1) > 0,
            "but sealed must keep SOME share: withdrawing it entirely stops the backfill that builds 30d coverage"
        );
    }

    /// Work INSIDE the horizon must keep a share against work past it: `starved`
    /// improves every day past `STARVATION_HORIZON_MICROS`, so an ancient unit
    /// otherwise wins every tick. The unit here is 20 days sealed on purpose —
    /// `window_turn` already covers the last 14, so a younger one proves nothing.
    #[test]
    fn work_inside_the_horizon_keeps_a_share_against_work_past_it() {
        let now = 100 * DAY_MICROS;
        let (_dir, mut journal) = new_journal();
        // Ancient: 60 days sealed, so `starved` has gained ~29 steps.
        let ancient = task("p", now - 61 * DAY_MICROS, now - 60 * DAY_MICROS, Operation::Dedup).key.clone();
        // 20 days sealed: past the 14-day query window and inside the horizon —
        // the band with no reservation of its own.
        let inside = task("p", now - 21 * DAY_MICROS, now - 20 * DAY_MICROS, Operation::Dedup).key.clone();
        journal.enqueue(ancient.clone(), 0, 0, 0);
        journal.enqueue(inside.clone(), 0, 0, 0);

        // Every claim is re-opened, so the ancient unit is always available to win again.
        let inside_claims = claims_matching(&mut journal, Operation::Dedup, now, 16, |claimed| claimed.key == inside);
        assert!(
            inside_claims > 0,
            "a unit inside the horizon must get a claim; without a reserved turn the past-horizon unit wins every tick \
             and the band holding most of the queue's bytes never moves"
        );
    }

    /// Among sealed work, a day-sized unit outranks yesterday's ten-minute leftovers
    /// even though those are newer: day units come from the backfill planner and are
    /// the only kind that advances the rollup horizon.
    #[test]
    fn sealed_backfill_units_outrank_yesterdays_fine_slices() {
        let (_dir, mut journal) = new_journal();
        // "Now" well past both, so everything is sealed rather than frontier.
        let now = 10 * DAY_MICROS;

        // Yesterday's fine-grained leftovers: newer, and far more numerous.
        enqueue_run(&mut journal, "p", 8 * DAY_MICROS, NORMAL_SLICE_MICROS, 12, 0, Operation::BaseRollup);
        // An older day, queued whole by the backfill planner.
        let coarse = task("p", 3 * DAY_MICROS, 4 * DAY_MICROS, Operation::BaseRollup);
        journal.enqueue(coarse.key.clone(), 0, 0, 0);

        let claimed = journal.claim_next(Operation::BaseRollup, now, true).expect("a sealed task is claimable");
        assert_eq!(
            claimed.key.slice.width(),
            DAY_MICROS,
            "the horizon-advancing day unit must be claimed before yesterday's ten-minute slices; got a {}s slice starting {}",
            claimed.key.slice.width() / 1_000_000,
            claimed.key.slice.start_micros / DAY_MICROS
        );
    }

    /// Classification is string-based because the DataFusion error arrives
    /// type-erased through delta-rs/anyhow; these strings are the contract. A slice
    /// must not shrink over a fault that has nothing to do with size, and a busy
    /// pool is not an oversized unit: splitting on a transient refusal multiplies
    /// the queue into shards that are each refused in turn.
    #[test_case::test_case("resource_admission" => true ; "a static over-budget estimate still splits")]
    #[test_case::test_case("admission_busy" => false ; "a transient busy pool must back off, not multiply the queue")]
    #[test_case::test_case(
        "dedup: Not enough memory to continue external sort. Consider increasing the memory limit config: \
         'datafusion.runtime.memory_limit', or decreasing the config: 'datafusion.execution.sort_spill_reservation_bytes'."
        => true ; "the sort-OOM wording")]
    #[test_case::test_case("compaction: Resources exhausted: Additional allocation failed for ExternalSorter[1] with top memory consumers" => true ; "pool exhaustion")]
    #[test_case::test_case("dedup: Object at location ... not found" => false ; "a missing object has nothing to do with size")]
    #[test_case::test_case("compaction: transaction failed: version 2667 already exists" => false ; "a commit conflict has nothing to do with size")]
    #[test_case::test_case("source_not_flushed" => false ; "a dependency failure has nothing to do with size")]
    fn capacity_failures_are_recognised_from_prod_text(failure: &str) -> bool {
        is_capacity_failure(failure)
    }

    /// There must be exactly ONE capacity classifier. A local copy that misses the
    /// sort-OOM wording is undetectable by behaviour tests (each classifier is
    /// self-consistent), so it is pinned at the source instead.
    #[test]
    fn no_second_capacity_classifier_exists() {
        for (name, source) in [("database/maintain.rs", include_str!("database/maintain.rs")), ("database/compact.rs", include_str!("database/compact.rs"))] {
            assert!(
                !source.contains(r#"contains("Resources exhausted")"#),
                "{name} classifies pool exhaustion locally — use `maintenance_coordinator::is_capacity_failure`, \
                 or it will miss the sort-OOM wording exactly as it did before 2026-09-03"
            );
        }
    }

    /// A slice too big for the pool — or for its deadline — fails identically every
    /// pass, so back off once (a squeeze may be transient) then shrink it. Byte-based
    /// splitting cannot catch the deadline case: a day-sized slice with modest bytes
    /// still pays one object-store round trip per file.
    #[test_case::test_case(false ; "a_unit_that_cannot_fit_bisects_instead_of_retrying_at_the_same_size")]
    #[test_case::test_case(true ; "a_unit_that_keeps_timing_out_bisects_instead_of_retrying_forever")]
    fn a_repeated_capacity_failure_bisects_instead_of_retrying_at_the_same_size(via_timeout: bool) {
        let (_dir, mut journal) = new_journal();
        let key = task("whale", 0, DAY_MICROS, Operation::Dedup).key;
        journal.enqueue(key.clone(), 0, 0, 0);
        let oom = "dedup: Not enough memory to continue external sort.".to_owned();

        // The first failure retries whole; the second says the slice does not fit.
        for attempt in 1..=2u32 {
            if via_timeout {
                assert!(journal.mark_running(&key));
                journal.abandon_running(&key, 0, None);
            } else {
                journal.retry_or_split(&key, oom.clone(), i64::from(attempt), attempt);
            }
            if attempt == 1 {
                assert_eq!(journal.state(&key), Some(TaskState::Retry), "one squeeze may be someone else's fault; retry it whole");
            }
        }

        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "a repeat says the slice itself does not fit; the parent stays as an audit record");
        let widths: Vec<i64> = journal.tasks().filter(|t| t.state != TaskState::Superseded).map(|t| t.key.slice.width()).collect();
        assert!(!widths.is_empty(), "bisection must leave claimable children behind");
        assert!(widths.iter().all(|w| *w < DAY_MICROS), "bisection must leave SMALLER children; got widths {widths:?} still at the full day");
    }

    #[test_case::test_case(true, 0, None ; "timeout without a byte estimate")]
    #[test_case::test_case(true, MAX_DECODED_BYTES / 2, None ; "timeout below byte budget")]
    #[test_case::test_case(false, 0, None ; "capacity failure without a byte estimate")]
    #[test_case::test_case(false, 2 * MAX_DECODED_BYTES, None ; "capacity failure with an estimate")]
    #[test_case::test_case(true, 0, Some(2 * MAX_DECODED_BYTES) ; "timeout retains actual preflight")]
    #[test_case::test_case(false, 0, Some(MAX_DECODED_BYTES / 2) ; "capacity failure below byte budget retains actual preflight")]
    fn failure_driven_splits_do_not_fabricate_byte_measurements(via_timeout: bool, estimate: u64, preflight: Option<u64>) {
        let (dir, mut journal) = new_journal();
        let mut unit = task("p", 0, DAY_MICROS, Operation::Dedup);
        unit.estimated_decoded_bytes = estimate;
        let key = running_unit(&mut journal, unit, 2);
        if let Some(bytes) = preflight {
            journal.record_preflight(&key, None, bytes);
        }
        if via_timeout {
            journal.abandon_running(&key, 0, None);
        } else {
            journal.retry_or_split(&key, "Not enough memory to continue external sort".to_owned(), 0, 2);
        }
        journal.checkpoint().expect("persist split");
        let journal = TaskJournal::load(dir.path()).expect("reload split");
        assert_eq!(journal.state(&key), Some(TaskState::Superseded));
        let children: Vec<_> = journal.tasks().filter(|task| task.state == TaskState::Pending).collect();
        assert_eq!(children.len(), 2, "a repeated failure still bisects the task");
        assert_eq!(
            children.iter().map(|task| task.estimated_decoded_bytes).sum::<u64>(),
            preflight.unwrap_or(estimate),
            "bisection apportions the available input estimate"
        );
        assert!(
            children.iter().all(|task| task.parent_measured_bytes == preflight && task.preflight_decoded_bytes.is_none()),
            "only actual preflight evidence belongs on children; each child needs its own fresh preflight"
        );
    }

    /// Only capacity failures shrink. A missing object or a commit conflict says
    /// nothing about size, and splitting on it would shred a healthy slice.
    #[test]
    fn an_unrelated_failure_retries_whole_however_often_it_repeats() {
        let (_dir, mut journal) = new_journal();
        let key = task("steady", 0, DAY_MICROS, Operation::Dedup).key;
        journal.enqueue(key.clone(), 0, 0, 0);
        for attempts in 1..6 {
            journal.retry_or_split(&key, "dedup: object not found".to_owned(), i64::from(attempts), attempts);
            assert_eq!(journal.state(&key), Some(TaskState::Retry), "attempt {attempts} must not split");
        }
    }

    /// A derived unit must read the base files that hold its rows by OVERLAP, not
    /// containment: derived units are an hour wide and backfill writes day-wide base
    /// files, which no hour can contain.
    #[test]
    fn a_day_wide_base_file_feeds_every_hour_wide_derived_slice_inside_it() {
        const DAY: i64 = 1_785_628_800_000_000; // 2026-08-02T00:00Z
        const HOUR: i64 = 3_600_000_000;
        let day_file = (DAY, DAY + 86_400_000_000);

        for hour in 0..24 {
            let slice = TimeSlice::new(DAY + hour * HOUR, DAY + (hour + 1) * HOUR).expect("slice");
            assert!(slice.overlaps(day_file.0, day_file.1), "hour {hour} must read the day-wide base file that holds its rows");
            let contained = day_file.0 >= slice.start_micros && day_file.1 <= slice.end_micros;
            assert!(!contained, "containment is what broke: hour {hour} could never select a day-tagged file");
        }

        // Overlap must still EXCLUDE what it should: neighbouring days.
        let slice = TimeSlice::new(DAY, DAY + HOUR).expect("slice");
        assert!(!slice.overlaps(DAY - 86_400_000_000, DAY), "the previous day ends exactly at the boundary and must not be read");
        assert!(!slice.overlaps(DAY + 86_400_000_000, DAY + 2 * 86_400_000_000), "a later day must not be read");
        assert!(slice.overlaps(DAY, DAY + 600_000_000));
    }

    #[test]
    fn replanning_live_debt_reopens_one_idempotent_task() {
        let (_dir, mut journal) = new_journal();
        let key = task("p", 0, MIN_SLICE_MICROS, Operation::SealedConsolidation).key;
        journal.enqueue(key.clone(), 10, 20, 1);
        journal.complete(&key);
        journal.enqueue(key.clone(), 5, 30, 1);
        journal.enqueue(key.clone(), 5, 30, 1);
        let tasks = journal.tasks().collect::<Vec<_>>();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, TaskState::Pending);
        assert_eq!(tasks[0].deadline_micros, 5);
        assert_eq!(tasks[0].estimated_decoded_bytes, 30);
    }

    #[test]
    fn derived_rollup_claim_waits_for_complete_base_hour() {
        let (_dir, mut journal) = new_journal();
        let base_keys: Vec<_> = (0..DERIVED_SLICE_MICROS)
            .step_by(NORMAL_SLICE_MICROS as usize)
            .map(|start| upserted(&mut journal, task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup)))
            .collect();
        journal.upsert(task("p", 0, DERIVED_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none());
        for key in base_keys {
            journal.complete(&key);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some());
    }

    #[test]
    fn oversized_task_is_replaced_by_durable_time_children() {
        let (_dir, mut journal) = new_journal();
        let key = upserted(&mut journal, task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup));
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None));
        assert_eq!(journal.tasks().filter(|task| task.state == TaskState::Pending).count(), 2);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded));
    }

    #[test]
    fn live_frontier_lag_prefers_pending_split_child_over_superseded_parent() {
        let now = 10 * DAY_MICROS;
        let (_dir, mut journal) = new_journal();
        let input = task("p", now - NORMAL_SLICE_MICROS, now, Operation::BaseRollup).tap_mut(|unit| unit.deadline_micros = now - 2 * 60 * 1_000_000);
        let key = upserted(&mut journal, input);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None));
        assert_eq!(live_frontier_lag_secs(journal.tasks(), now), 2 * 60);
    }

    #[test]
    fn completed_children_satisfy_a_larger_derived_dependency() {
        let (_dir, mut journal) = new_journal();
        let base_key = upserted(&mut journal, task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup));
        journal.upsert(task("p", 0, NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.split_time_task(&base_key, 2 * MAX_DECODED_BYTES, None));
        let children = journal
            .tasks()
            .filter(|task| task.key.operation == Operation::BaseRollup && task.state == TaskState::Pending)
            .map(|task| task.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(children.len(), 2);
        for child in children {
            journal.complete(&child);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some());
    }

    #[test]
    fn admission_is_all_or_nothing_and_keeps_memory_headroom() {
        let admission = AdmissionController::new(4, 1_000, 8, 2);
        let request = Resources { cpu: 2, decoded_bytes: 600, object_reads: 4, object_writes: 1 };
        let permit = admission.try_acquire(request).expect("first reservation");
        assert!(admission.try_acquire(request).is_none());
        assert_eq!(admission.utilization(), request);
        drop(permit);
        assert_eq!(admission.utilization(), Resources::default());
        assert!(admission.try_acquire(Resources { decoded_bytes: 751, ..Resources::default() }).is_none());
        assert!(admission.try_acquire(Resources { decoded_bytes: MAX_DECODED_BYTES + 1, ..Resources::default() }).is_none());
    }
}
