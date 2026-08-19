//! Durable, byte-bounded work units shared by background maintenance.
//!
//! This module deliberately contains no scan implementation.  It is the
//! correctness boundary between write/Delta reconciliation and the workers:
//! work is journaled before it can be selected, and a worker can only receive
//! a unit whose decoded-byte reservation fits the configured ceiling.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

pub const NORMAL_SLICE_MICROS: i64 = 10 * 60 * 1_000_000;
pub const DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
/// Widths `coarsen_sealed_slices` fuses sealed units to, widest first. It takes
/// the widest whose summed estimate fits `MAX_DECODED_BYTES`; below the finest,
/// the mint width stands. Each divides the one above, so an aligned unit at any
/// width sits inside exactly one bucket at every coarser width.
pub const COARSEN_WIDTHS: [i64; 3] = [DAY_MICROS, 6 * 60 * 60 * 1_000_000, 60 * 60 * 1_000_000];
/// Widths a unit can be SUBSUMED at, finest first — the mint width plus every
/// fusion width. Each divides the next, so an aligned unit at any of them sits
/// wholly inside one bucket of every coarser one.
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
    /// Candidates in a group whose summed estimate exceeded MAX_DECODED_BYTES.
    pub over_budget: usize,
}

impl CoarsenReport {
    pub const fn total(self) -> usize {
        self.subsumed + self.fused
    }
}

pub const SUBSUME_WIDTHS: [i64; 4] = [NORMAL_SLICE_MICROS, 60 * 60 * 1_000_000, 6 * 60 * 60 * 1_000_000, DAY_MICROS];
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const DERIVED_SLICE_MICROS: i64 = 60 * 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;

/// Frontier lag the sealed reservation is still affordable at.
///
/// Above this, `claim_next` stops reserving a share for sealed work until the
/// live frontier catches up. Ten minutes is one `NORMAL_SLICE_MICROS`: a
/// frontier that is a whole slice behind is not keeping up, and every hybrid
/// query is paying for it through `raw_tail_duration_secs`
/// (`FINALIZATION_DELAY + lag`).
pub const FRONTIER_LAG_BUDGET_SECS: u64 = 600;

/// Whether a failure means "this did not fit" rather than "this went wrong".
///
/// Matched on the message, not the type: these errors originate in DataFusion,
/// cross the delta-rs and `anyhow` boundaries on the way back, and arrive
/// type-erased. The two strings are DataFusion's own — `ResourcesExhausted`'s
/// `Display` and the `ExternalSorter`'s message — and both are asserted against
/// verbatim prod text in `capacity_failures_are_recognised_from_prod_text`.
/// Seconds a unit of this operation is allowed to run before the coordinator
/// abandons it. Defined here, beside `Operation`, because the retry backoff and
/// the deadline have to agree: a unit that ran to its deadline burned that much
/// of a worker, and retrying it sooner than that lets a permanently oversized
/// unit hold a slot almost continuously.
pub const fn operation_deadline_secs(operation: Operation) -> u64 {
    match operation {
        // Dedup used to get 5 minutes because it held a rewrite permit and its
        // memory was bounded by INPUT SIZE, unlike a rollup. #161 removed the
        // hash-sharded passes, so the streaming branch is now a
        // `BoundedWindowAggExec(mode=Sorted)` over a spillable `SortExec` —
        // bounded by one key group, not by input — and that argument no longer
        // holds.
        //
        // 5 minutes is also simply below what this class of work costs. Measured
        // 2026-08-18 with `maintenance_unit_slow`, the comparable file rewrites
        // complete at 320-440s against their 900s deadline (51-64% headroom):
        //
        //     SealedConsolidation  440s, 363s, 335s
        //     HotPacking           320s
        //
        // Dedup over the same window logged 12 timeouts and NOT ONE slow
        // completion — nothing finished between 75s and 300s. Its units are
        // bimodal: quick, or past the deadline. And a minimum-width unit cannot
        // be shrunk to fit: `byte_bounded_units` bisects in time only while the
        // slice is wider than MIN_SLICE_MICROS, below which it emits hash shards,
        // which `split_time_task` refuses. So the unit retries whole, forever, at
        // full cost — the same trap #168 fixed for repair.
        //
        // REVERTED 2026-08-18, ~15 minutes after it shipped. Prod took a 125.1 GB
        // OOM (exit 137, `tokio-rt-worker`) at 11:38 UTC, and the interval
        // between OOMs had shortened from 15 hours to 4.2:
        //
        //     08-17 09:39 -> 16:39   7h
        //     08-18 07:27           15h
        //     08-18 11:38          4.2h   <- with the 900s deadline live
        //
        // Not proven — this OOM signature predates the change and its likeliest
        // driver is a query-side join (see the handover, section 6.4). But the
        // mechanism is precisely what the original 5 minutes was protecting: a
        // dedup unit holds a rewrite permit AND its Arrow decode is OUTSIDE the
        // DataFusion memory pool, so tripling the deadline triples how long that
        // untracked memory is held, times the concurrent units.
        //
        // The argument for raising it was that #161 made the streaming branch a
        // `BoundedWindowAggExec` over a spillable sort. That is true of the
        // STREAMING branch; the collecting branch still materialises. Overriding
        // a comment that named this exact risk, on a partial reading of which
        // branch runs, was the mistake.
        //
        // The measurement that motivated it still stands and is still worth
        // acting on — dedup units are bimodal, and 5 minutes is below what
        // comparable file rewrites (320-440s) cost. The right fix is to make the
        // units FIT, not to let them run longer: `byte_bounded_units` bisects in
        // time only above MIN_SLICE_MICROS and emits hash shards below it, which
        // `split_time_task` refuses outright. Teaching it to accept hash-sharded
        // children would let an oversized unit shrink instead of retrying whole.
        Operation::Dedup => 5 * 60,
        Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair | Operation::BaseRollup | Operation::DerivedRollup => 15 * 60,
    }
}

pub fn is_capacity_failure(message: &str) -> bool {
    message.contains("Resources exhausted") || message.contains("Not enough memory to continue external sort")
}
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const INVALIDATION_DEADLINE_BUCKET_MICROS: i64 = 30 * 1_000_000;
pub const LIVE_FRONTIER_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
const PRIORITY_BUCKET_MICROS: i64 = 60 * 1_000_000;
pub const TAG_SOURCE: &str = "timefusion.source";
pub const TAG_PROJECT: &str = "timefusion.project";
pub const TAG_SLICE_START: &str = "timefusion.slice_start_micros";
pub const TAG_SLICE_END: &str = "timefusion.slice_end_micros";
pub const TAG_SOURCE_FINGERPRINT: &str = "timefusion.source_fingerprint";
pub const TAG_GENERATION: &str = "timefusion.generation";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_COMPACT_BYTES: u64 = 64 * 1024 * 1024;
static THROUGHPUT_SAMPLE: std::sync::OnceLock<Mutex<(i64, u64)>> = std::sync::OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Dedup,
    BaseRollup,
    DerivedRollup,
    HotPacking,
    SealedConsolidation,
    Repair,
}

impl Operation {
    const fn priority(self) -> u8 {
        match self {
            Self::Dedup => 0,
            Self::BaseRollup => 1,
            Self::DerivedRollup => 2,
            Self::HotPacking => 3,
            Self::SealedConsolidation => 4,
            Self::Repair => 5,
        }
    }
}

/// The operation mix a maintenance worker rotates through. One definition for
/// the server loop (`run_coordinator_maintenance_once`) and the journal-replay
/// simulator (`maintenance_sim`) — the sim exists to evaluate changes to this
/// mix, so the two must never be able to drift apart.
///
/// BALANCED interleaves dependent publication with dedup: dedup/base receive
/// three slots each; derived and file work each receive one. `claim_next`
/// still applies deadline, recent-slice, dependency, and project fairness.
///
/// COVERAGE_SHORT gives the rollup chain the slots while
/// `rollup_min_contiguous_days` is below goal: `dependencies_complete` makes
/// BaseRollup depend on NOTHING, so of the balanced cycle six slots in ten go
/// to work that cannot advance the metric governing 14d/30d latency (measured
/// 2026-08-18). Every operation keeps at least one slot — file debt left at
/// zero is how file counts ran to 2-3k and degraded every query (2026-08-01).
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

    /// Half-open intersection. The rollup pipeline asks this in two places —
    /// which base TASKS cover a derived slice, and which base FILES a derived
    /// unit must read — and the two must agree, so they share this.
    pub const fn overlaps(self, start_micros: i64, end_micros: i64) -> bool {
        end_micros > self.start_micros && start_micros < self.end_micros
    }

    pub fn normal_units(start_micros: i64, end_micros: i64) -> anyhow::Result<Vec<Self>> {
        Self::fixed_units(start_micros, end_micros, NORMAL_SLICE_MICROS)
    }

    fn fixed_units(start_micros: i64, end_micros: i64, width_micros: i64) -> anyhow::Result<Vec<Self>> {
        let whole = Self::new(start_micros, end_micros)?;
        anyhow::ensure!(width_micros > 0, "maintenance slice width must be positive");
        let mut result = Vec::new();
        let mut start = whole.start_micros;
        while start < whole.end_micros {
            let end = start.saturating_add(width_micros).min(whole.end_micros);
            result.push(Self { start_micros: start, end_micros: end });
            start = end;
        }
        Ok(result)
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
    /// from real rollup coverage by `plan_rollup_backfill` rather than from
    /// journal bookkeeping.
    ///
    /// `dependencies_complete` otherwise requires COMPLETE `BaseRollup` TASKS
    /// contiguously covering the slice. For a frontier hour that is right. For a
    /// historical day whose 1m tier was built weeks ago — possibly by an older
    /// code path, possibly with its journal records long since collapsed — no
    /// such task exists, so the unit is unclaimable forever and `claim_next`
    /// skips it with no counter and no log.
    ///
    /// Prod 2026-08-18 22:30 UTC is that shape exactly: the 1m base tier is 33
    /// days deep on most projects while the 1h derived tier it feeds sits at
    /// 9-17, `pending_derived_rollup` did not move by ONE task across two 240s
    /// windows with workers free, and all 35 derived units claimed in 20 minutes
    /// were frontier slices whose base had completed minutes earlier.
    ///
    /// Only ever set from positive evidence — the planner computes `missing`
    /// tiers from actual coverage, so a derived tier missing while no base tier
    /// is missing means the base data is there. That is strictly better evidence
    /// than the journal's, which is why this overrides rather than supplements.
    #[serde(default)]
    pub base_tier_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Publication {
    pub source_fingerprint: u64,
    pub generation: String,
    pub rows: u64,
}

/// Split a unit until each estimated reservation fits.  A one-minute whale is
/// divided by a stable hash of the complete dedup key; callers must apply
/// `hash(key) % hash_shards == hash_shard` before deduplication.
pub fn byte_bounded_units(task: &MaintenanceTask, observed_or_estimated_bytes: u64) -> Vec<MaintenanceTask> {
    if observed_or_estimated_bytes <= MAX_DECODED_BYTES {
        let mut task = task.clone();
        task.estimated_decoded_bytes = observed_or_estimated_bytes;
        return vec![task];
    }
    if task.key.slice.width() > MIN_SLICE_MICROS {
        let midpoint = task.key.slice.start_micros.saturating_add(task.key.slice.width() / 2);
        let midpoint = (midpoint / MIN_SLICE_MICROS) * MIN_SLICE_MICROS;
        if midpoint > task.key.slice.start_micros && midpoint < task.key.slice.end_micros {
            let left_bytes = ((u128::from(observed_or_estimated_bytes) * u128::try_from(midpoint - task.key.slice.start_micros).unwrap_or(0))
                / u128::try_from(task.key.slice.width()).unwrap_or(1)) as u64;
            let mut left = task.clone();
            left.key.slice.end_micros = midpoint;
            let mut right = task.clone();
            right.key.slice.start_micros = midpoint;
            let mut units = byte_bounded_units(&left, left_bytes);
            units.extend(byte_bounded_units(&right, observed_or_estimated_bytes.saturating_sub(left_bytes)));
            return units;
        }
    }

    let shards_u64 = observed_or_estimated_bytes.div_ceil(MAX_DECODED_BYTES).max(2);
    let shards = u32::try_from(shards_u64).unwrap_or(u32::MAX);
    let per_shard = observed_or_estimated_bytes.div_ceil(u64::from(shards));
    (0..shards)
        .map(|hash_shard| {
            let mut shard = task.clone();
            shard.hash_shard = hash_shard;
            shard.hash_shards = shards;
            shard.estimated_decoded_bytes = per_shard;
            shard
        })
        .collect()
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
    Task(MaintenanceTask),
    SourceCursor { source: String, delta_version: u64 },
}

/// Crash-safe task journal. `checkpoint` uses the same fsync + atomic rename
/// primitive as WAL metadata; a failed completion checkpoint therefore causes
/// redundant work, never missing work.
#[derive(Debug)]
pub struct TaskJournal {
    path: PathBuf,
    wal_path: PathBuf,
    snapshot: Snapshot,
    /// Stable indices into `snapshot.tasks`. Tasks are never removed, so point
    /// updates and WAL replay stay O(1) even with a production-sized backlog.
    task_indices: HashMap<TaskKey, usize>,
    dirty_tasks: HashSet<TaskKey>,
    dirty_cursors: HashSet<String>,
    fair_cursors: HashMap<Operation, String>,
    /// `(source, project_id, date)` whose BASE tier is already built, as read
    /// from real rollup coverage by `plan_rollup_backfill` every 60s.
    ///
    /// `dependencies_complete` consults this instead of requiring COMPLETE
    /// `BaseRollup` TASKS, which a historical day does not have. Three attempts
    /// to carry the same fact as a per-task flag failed (#184, #186, #195),
    /// because the flag had to land on exactly the right `TaskKey` and the
    /// queued work is not the width the planner assumes: prod 2026-08-19 06:30
    /// measured `derived_unproven=674` out of `derived_pending=674` — the flag
    /// had never been set on ONE pending task.
    ///
    /// A day is the right key because that is what the fact is about, so it
    /// cannot miss a task whatever slice that task covers. Runtime only, never
    /// journalled: the planner rebuilds it from coverage each pass, so a restart
    /// costs one pass and it self-heals if coverage changes underneath it.
    base_tier_ready: HashSet<(String, String, String)>,
    /// `(source, project_id, physical_table, date)` the tier is MISSING, from
    /// the same coverage read that fills `base_tier_ready`.
    ///
    /// `scheduling_class` ranks a hole ahead of a re-derive. Without that,
    /// sealed rollup work is strictly newest-first, and recent days are
    /// continuously re-invalidated by ongoing publication — so the claim never
    /// walks backwards far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f` had 1h-tier dates jumping 2026-07-31 -> 08-14 for a second
    /// day running while day-wide derived units for 08-17 were claimed over and
    /// over. Newest-first is right for freshness and wrong for CONTIGUITY, and
    /// 30d coverage is a contiguity goal.
    ///
    /// Runtime only, rebuilt from coverage every 60s, same as `base_tier_ready`.
    tier_holes: HashSet<(String, String, String, String)>,
    /// Rotates so a fixed share of claims is reserved for sealed work. Runtime
    /// only — never journalled; losing it across a restart costs nothing.
    claim_tick: u64,
    /// Last observed `eligible_watermark_lag_seconds`, republished by
    /// `publish_statistics`. Read by `claim_next` to decide whether the sealed
    /// reservation can still be afforded. Atomic because `publish_statistics`
    /// takes `&self`; runtime only, never journalled.
    frontier_lag_secs: std::sync::atomic::AtomicU64,
}

/// Ensures a claimed task cannot remain stuck in `Running` when its worker
/// returns early through an unexpected error. Expected retry, split, and
/// completion paths change the state before this guard is dropped.
pub struct TaskLease {
    journal: Arc<Mutex<TaskJournal>>,
    key: TaskKey,
}

impl TaskLease {
    pub fn new(journal: Arc<Mutex<TaskJournal>>, key: TaskKey) -> Self {
        Self { journal, key }
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        let mut journal = self.journal.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if journal.state(&self.key) == Some(TaskState::Running) {
            journal.abandon_running(&self.key, crate::support::now_micros());
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
}

impl TaskJournal {
    // v1 removed the original bootstrap expansion, but the then-current
    // reconciliation immediately recreated it before advancing its cursor.
    // v2 removed it in memory but did not force an on-disk snapshot rewrite.
    // v3 runs once with commit-range reconciliation and forced compaction.
    const BOOTSTRAP_BACKLOG_MIGRATION: &'static str = "__maintenance_bootstrap_backlog_v3";
    const BOOTSTRAP_BACKLOG_LIMIT: usize = 100_000;
    const COARSE_BACKFILL_MIGRATION: &'static str = "__maintenance_coarse_backfill_v1";
    const STALE_ESTIMATE_MIGRATION: &'static str = "__maintenance_stale_estimate_v1";

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
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if !line.ends_with(b"\n") {
                    break;
                }
                let record = serde_json::from_slice::<JournalRecord>(&line[..line.len() - 1])?;
                match record {
                    JournalRecord::Task(task) => {
                        if let Some(index) = task_indices.get(&task.key).copied() {
                            snapshot.tasks[index] = task;
                        } else {
                            let index = snapshot.tasks.len();
                            task_indices.insert(task.key.clone(), index);
                            snapshot.tasks.push(task);
                        }
                    }
                    JournalRecord::SourceCursor { source, delta_version } => {
                        snapshot.source_cursors.entry(source).and_modify(|cursor| *cursor = (*cursor).max(delta_version)).or_insert(delta_version);
                    }
                }
            }
        }
        Ok(Self {
            path,
            wal_path,
            snapshot,
            task_indices,
            dirty_tasks: HashSet::new(),
            dirty_cursors: HashSet::new(),
            fair_cursors: HashMap::new(),
            base_tier_ready: HashSet::new(),
            tier_holes: HashSet::new(),
            claim_tick: 0,
            frontier_lag_secs: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Early coordinator builds journaled the one-hour tier in ten-minute
    /// units. Supersede those unpublished fragments and replace them with one
    /// aligned hour task; completed tagged publications remain available for
    /// metadata recovery and are not rewritten by this migration.
    pub fn migrate_derived_slices(&mut self) -> usize {
        let mut replacements: HashMap<TaskKey, (i64, u64, u64)> = HashMap::new();
        let mut migrated = 0usize;
        for task in &mut self.snapshot.tasks {
            if !(task.key.operation == Operation::DerivedRollup
                && task.key.slice.width() != DERIVED_SLICE_MICROS
                && !matches!(task.state, TaskState::Complete | TaskState::Superseded))
            {
                continue;
            }
            let start = task.key.slice.start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            let mut key = task.key.clone();
            key.slice = TimeSlice { start_micros: start, end_micros: start.saturating_add(DERIVED_SLICE_MICROS) };
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
            self.dirty_tasks.insert(task.key.clone());
            migrated = migrated.saturating_add(1);
        }
        for (key, (deadline, estimate, created)) in replacements {
            self.enqueue(key, deadline, estimate, created);
        }
        migrated
    }

    /// Remove the one-time global backlog produced by the original cursor
    /// bootstrap. That implementation expanded every retained partition into
    /// dedup, rollup, and packing work and produced 730k journal entries in
    /// production. Keeping completed publications preserves recoverable
    /// coverage; dropping unfinished entries is correctness-safe because they
    /// remain uncovered and reads use raw data until normal planners or new
    /// invalidations enqueue bounded work.
    pub fn migrate_bootstrap_backlog(&mut self) -> Option<usize> {
        self.migrate_bootstrap_backlog_with_limit(Self::BOOTSTRAP_BACKLOG_LIMIT)
    }

    fn migrate_bootstrap_backlog_with_limit(&mut self, limit: usize) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::BOOTSTRAP_BACKLOG_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut removed = 0;
        if self.snapshot.tasks.len() > limit {
            self.snapshot.tasks.retain(|task| {
                let keep = task.state == TaskState::Complete;
                removed += usize::from(!keep);
                keep
            });
            self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
            self.dirty_tasks.clear();
        }
        self.snapshot.source_cursors.insert(Self::BOOTSTRAP_BACKLOG_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::BOOTSTRAP_BACKLOG_MIGRATION.to_owned());
        Some(removed)
    }

    /// One-shot: drop the fine-grained SEALED backfill tasks so the coarse
    /// planner can re-derive them a day at a time.
    ///
    /// `plan_rollup_backfill` used to enqueue history through `invalidate`,
    /// which expands a day into ~144 ten-minute slices x Dedup/BaseRollup/
    /// HotPacking x each tier — about 450 durable tasks per (project, date).
    /// Prod 2026-08-17 reached 127,536 pending that way and drained ~19/min,
    /// because each unit costs 50-80s of snapshot-refresh and commit regardless
    /// of how little data it covers. That is ~111 hours of pure overhead for
    /// ~600 GB of actual work, and it is why adding concurrency kept not
    /// helping — and why pushing concurrency harder ended in an OOM kill.
    ///
    /// Changing the planner alone does not help, because the already-enqueued
    /// tasks still have to drain. So drop them: they are unpublished backfill
    /// work, and the planner re-derives exactly what is missing from rollup
    /// COVERAGE, so nothing is lost by forgetting the intent.
    ///
    /// Deliberately narrow. Only non-Complete, only sealed slices, and only the
    /// operations the backfill mints. Frontier work is untouched, and
    /// SealedConsolidation/Repair are left alone because `plan_compaction_debt`
    /// already plans those day-sized.
    /// Forget every stored byte estimate once, because they were all measured
    /// with a broken ruler.
    ///
    /// Until `slice_share_of_file` the estimate counted whole files, so a
    /// ten-minute child of a split day carried the WHOLE DAY's estimate — and
    /// `coarsen_to_width` sums its members, so fusing 144 such children summed
    /// 144 whole days and never fit. Prod 2026-08-19, immediately after the
    /// estimate fix deployed:
    ///
    ///     subsumed=0 fused=0 candidates=266530 blocked=24 over_budget=266506
    ///
    /// `blocked` had collapsed from 249,786 to 24 — the superseded trap was
    /// gone — and every single candidate was now refused on a number written
    /// before the fix existed. A correction that only applies to new
    /// measurements cannot repair a durable queue full of old ones.
    ///
    /// Zero is what a freshly minted unit already carries: the claim-time
    /// preflight computes the real estimate and splits if it genuinely must, so
    /// the worst case is one over-sized claim that immediately right-sizes
    /// itself. That is strictly better than a queue that can never fuse.
    pub fn clear_stale_estimates(&mut self) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::STALE_ESTIMATE_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut cleared = 0usize;
        for task in self.snapshot.tasks.iter_mut().filter(|task| task.state != TaskState::Complete && task.estimated_decoded_bytes != 0) {
            task.estimated_decoded_bytes = 0;
            cleared += 1;
        }
        self.dirty_tasks.clear();
        self.snapshot.source_cursors.insert(Self::STALE_ESTIMATE_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::STALE_ESTIMATE_MIGRATION.to_owned());
        Some(cleared)
    }

    pub fn migrate_fine_grained_backfill(&mut self, now_micros: i64) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::COARSE_BACKFILL_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut removed = 0usize;
        self.snapshot.tasks.retain(|task| {
            let coarse_planned = matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking);
            let drop = task.state != TaskState::Complete
                && coarse_planned
                && !is_live_frontier(task.key.slice, now_micros)
                // A day-sized unit is what replaces these; anything already that
                // wide came from the coarse planner and must survive.
                && task.key.slice.width() < 24 * 60 * 60 * 1_000_000;
            removed += usize::from(drop);
            !drop
        });
        if removed != 0 {
            self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
            self.dirty_tasks.clear();
        }
        self.snapshot.source_cursors.insert(Self::COARSE_BACKFILL_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::COARSE_BACKFILL_MIGRATION.to_owned());
        Some(removed)
    }

    /// Collapse a sealed day's leftover ten-minute units into one day unit.
    ///
    /// The live path mints a unit per ten-minute slice per project per tier,
    /// which is right while the day IS the frontier and pure overhead once it
    /// seals: ~144 units where one would do, each paying the same fixed
    /// object-store cost regardless of how little it covers. That is the whole
    /// shape of the backlog — prod 2026-08-17 sat at 18,040 pending with dedup,
    /// base-rollup and hot-packing each around 5,000, draining at roughly the
    /// rate the next midnight refills it. At ten times the projects it does not
    /// merely lag, it diverges.
    ///
    /// One-shot migration `migrate_fine_grained_backfill` did this once for the
    /// historical backlog; this is the recurring form, because every midnight
    /// creates another day of it.
    ///
    /// Fusion is a cascade over `COARSEN_WIDTHS`, not a day-or-nothing choice:
    /// a span lands at the widest width whose estimate fits the decode budget.
    /// Day-or-nothing left an over-budget day holding all 144 of its ten-minute
    /// slices, and on an uncompacted sealed partition each of those re-reads the
    /// whole day regardless — so the fallback for "too big to scan once" was to
    /// scan it 144 times.
    ///
    /// Anti-loop guard: a span already covered by a non-complete unit at least
    /// that wide is skipped. `split_time_task` leaves the parent `Superseded`
    /// when a unit is too big, so without this a whale's day would be split into
    /// children, fused back into a day, split again, forever. The guard is
    /// per-width, so a superseded day blocks only the day — its children still
    /// fuse at six hours, which is the point of the cascade.
    pub fn coarsen_sealed_slices(&mut self, now_micros: i64) -> usize {
        self.coarsen_sealed_slices_reporting(now_micros).total()
    }

    /// `coarsen_sealed_slices`, with the per-stage breakdown.
    ///
    /// The totals alone cannot say why a pass is small, and that is the only
    /// question worth asking of it. Prod 2026-08-19: subsume took `pending_dedup`
    /// from 14,519 to 3,753 in one pass while `pending_base_rollup` moved
    /// 88,104 → 85,287 and the fuse pass settled at ~76 units/tick — at which
    /// rate the base queue needs ~18 hours. Whether that is candidates being
    /// blocked, groups over budget, or simply few candidates is invisible from
    /// a single collapsed count, and every answer implies a different fix.
    pub fn coarsen_sealed_slices_reporting(&mut self, now_micros: i64) -> CoarsenReport {
        // SUBSUME before fusing. Fusion cannot touch a bucket that a wider
        // pending unit already covers — it would duplicate claimed work — so on
        // its own it leaves exactly the redundancy it exists to remove.
        let subsumed = self.subsume_covered_units(now_micros);
        let mut report = CoarsenReport { subsumed, ..Default::default() };
        for &width in COARSEN_WIDTHS.iter() {
            let stage = self.coarsen_to_width_reporting(width, now_micros);
            report.fused += stage.fused;
            report.candidates += stage.candidates;
            report.blocked += stage.blocked;
            report.over_budget += stage.over_budget;
        }
        report
    }

    /// Drop sealed units wholly covered by a WIDER non-complete unit for the
    /// same (table, source, project, operation).
    ///
    /// A rollup or dedup unit rebuilds its entire slice, so a ten-minute unit
    /// sitting inside a queued day-wide unit for the same cell is not work — it
    /// is the same work, listed 144 times. It still costs a full scan when
    /// claimed (a sealed partition's files span the whole day, so nothing
    /// prunes), and it costs `claim_next` a scan of the task set on every tick.
    ///
    /// This is the other half of `coarsen_to_width`, and without it that half
    /// converges to nothing. Fusion refuses any bucket already covered by a
    /// pending unit at least as wide — correctly, since fusing there would
    /// duplicate work — but a pending day-wide unit is *precisely* the condition
    /// under which the narrow units are redundant. So the fuse pass collapsed
    /// only the cells that had no day unit and then went quiet, which is exactly
    /// what prod showed: `maintenance_sealed_slices_coarsened` logged 12, 6, 3
    /// and then nothing for the rest of the process's life while
    /// `pending_base_rollup` sat at 88,100.
    ///
    /// The scale of the redundancy is measured, not assumed. The same process's
    /// backfill census reported `cells_missing=260 cells_wanted=0` — 260 real
    /// (project, date) cells behind 88,100 queued units, about 339 units per
    /// cell, and the planner declining to add more because every cell was
    /// already queued.
    ///
    /// Only NON-COMPLETE covering units subsume. A complete one is not evidence
    /// the span is still queued — it was built once, and a narrower unit inside
    /// it is a later invalidation that must run.
    fn subsume_covered_units(&mut self, now_micros: i64) -> usize {
        type Group = (String, String, String, Operation);
        let group_of =
            |task: &MaintenanceTask| -> Group { (task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation) };
        // For each subsume width, the buckets wholly inside some non-complete
        // unit STRICTLY wider than it. Membership therefore means "contained in
        // a wider live unit", which is the whole predicate.
        let mut covered: [HashSet<(Group, i64)>; SUBSUME_WIDTHS.len()] = Default::default();
        // Pending/Retry/Running only. NOT Superseded, and that exclusion is
        // load-bearing: `split_time_task` supersedes a unit too big to finish
        // and replaces it with children that tile its range, so a superseded
        // parent subsuming its own children would delete exactly the work the
        // split just created and leave the cell with nothing. NOT Complete
        // either — built once is not still queued, and a narrower unit inside a
        // completed span is a later invalidation that must run.
        for task in self.snapshot.tasks.iter().filter(|task| matches!(task.state, TaskState::Pending | TaskState::Retry | TaskState::Running)) {
            let group = group_of(task);
            for (index, &width) in SUBSUME_WIDTHS.iter().enumerate() {
                if task.key.slice.width() <= width {
                    continue;
                }
                let mut bucket = task.key.slice.start_micros.div_euclid(width) * width;
                while bucket.saturating_add(width) <= task.key.slice.end_micros {
                    if bucket >= task.key.slice.start_micros {
                        covered[index].insert((group.clone(), bucket));
                    }
                    bucket = bucket.saturating_add(width);
                }
            }
        }
        let before = self.snapshot.tasks.len();
        self.snapshot.tasks.retain(|task| {
            if !matches!(task.state, TaskState::Pending | TaskState::Retry) || is_live_frontier(task.key.slice, now_micros) {
                return true;
            }
            // The NARROWEST ladder width this unit fits inside, so that every
            // unit recorded against it is strictly wider than this one.
            //
            // Taking the widest that fits instead is subtly wrong and reads as
            // correct: `split_time_task` BISECTS, so it produces 12-hour
            // children, and 12h is not a ladder width. Mapped down to the 6h
            // bucket, a 12-hour unit finds the entry its OWN expansion wrote —
            // a unit subsuming itself, deleting both halves of every split and
            // leaving the cell with nothing queued.
            let Some(index) = SUBSUME_WIDTHS.iter().position(|&width| width >= task.key.slice.width()) else { return true };
            let width = SUBSUME_WIDTHS[index];
            let bucket = task.key.slice.start_micros.div_euclid(width) * width;
            // An unaligned unit can straddle two buckets; one bucket covering it
            // is the claim being made, so check it rather than assume alignment.
            if bucket > task.key.slice.start_micros || task.key.slice.end_micros > bucket.saturating_add(width) {
                return true;
            }
            !covered[index].contains(&(group_of(task), bucket))
        });
        let removed = before - self.snapshot.tasks.len();
        if removed != 0 {
            self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
            self.dirty_tasks.clear();
        }
        removed
    }

    /// One pass of `coarsen_sealed_slices` at a single width.
    ///
    /// Fuses every strictly-narrower sealed unit in a bucket into one unit of
    /// `width`, when the bucket's summed estimate fits `MAX_DECODED_BYTES` and
    /// nothing at least that wide already covers it.
    fn coarsen_to_width_reporting(&mut self, width: i64, now_micros: i64) -> CoarsenReport {
        let bucket_of = |start: i64| start.div_euclid(width) * width;
        let coarsenable = |task: &MaintenanceTask| {
            matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking)
                && matches!(task.state, TaskState::Pending | TaskState::Retry)
                && !is_live_frontier(task.key.slice, now_micros)
                && task.key.slice.width() < width
        };
        // Which buckets may not be fused at this width, and why. Three distinct
        // reasons, and collapsing them into one rule is what made the old
        // day-or-nothing version dead-end:
        //
        //   Running          — claimed work. Never race it, at any width.
        //   Superseded, <= W — proven too big at a width no larger than this
        //                      one, so this one cannot fit either.
        //   Pending/Retry, >= W — the span is already queued at least this wide;
        //                      a narrower unit inside it is duplicate work.
        //
        // The middle rule is the cascade. `split_time_task` supersedes a unit
        // that did not fit and replaces it with children; without the width
        // comparison a superseded DAY would block its children at every width
        // and they would sit at ten minutes forever, which is the state prod was
        // in. With it, a superseded day frees six hours, a superseded six hours
        // frees one, and each supersede strictly lowers the ceiling — so the
        // split/fuse loop the guard exists to prevent still cannot run.
        let mut blocked: HashSet<(String, String, String, Operation, i64)> = HashSet::new();
        for task in self.snapshot.tasks.iter().filter(|task| match task.state {
            TaskState::Running => true,
            TaskState::Pending | TaskState::Retry => task.key.slice.width() >= width,
            // Superseded does NOT block, and that reversal is the point.
            //
            // It used to block every width at or above its own, so a cell split
            // all the way down carried superseded ancestors at day, 12h, 6h and
            // 1h — which between them blocked every fusion width while none of
            // them could subsume (a superseded parent must not delete the
            // children that replaced it). Its descendants were then permanently
            // stuck: prod 2026-08-19 reported `fused=0 candidates=257,535
            // blocked=249,786` on every tick, a queue that could not shrink by
            // any mechanism it had.
            //
            // The anti-loop guard it was written for is really the budget test
            // below, which is stronger: fusion happens only when the CHILDREN'S
            // summed estimate fits `MAX_DECODED_BYTES`, and a unit that fits
            // does not split, so split/fuse cannot cycle. Superseded only ever
            // meant "did not fit under the estimate of the day", and with
            // `slice_share_of_file` that estimate has changed — refusing on it
            // forever would pin the queue to a measurement already known wrong.
            TaskState::Superseded | TaskState::Complete => false,
        }) {
            let mut bucket = bucket_of(task.key.slice.start_micros);
            while bucket < task.key.slice.end_micros {
                blocked.insert((task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation, bucket));
                bucket = bucket.saturating_add(width);
            }
        }

        let mut report = CoarsenReport::default();
        let mut groups: HashMap<(String, String, String, Operation, i64), u64> = HashMap::new();
        let mut members: HashMap<(String, String, String, Operation, i64), usize> = HashMap::new();
        for task in self.snapshot.tasks.iter().filter(|task| coarsenable(task)) {
            report.candidates += 1;
            let group = (
                task.key.physical_table.clone(),
                task.key.source.clone(),
                task.key.project_id.clone(),
                task.key.operation,
                bucket_of(task.key.slice.start_micros),
            );
            if blocked.contains(&group) {
                report.blocked += 1;
                continue;
            }
            *groups.entry(group.clone()).or_default() += task.estimated_decoded_bytes;
            *members.entry(group).or_default() += 1;
        }
        // Only fuse a span that will actually FIT. Coarsening is a win because
        // one unit does one scan where 144 slices each did the same scan — but a
        // unit that cannot finish inside its deadline does none of them, and
        // that is strictly worse than the slices it replaced.
        //
        // Measured after #178 shipped: BaseRollup began timing out at 900s for
        // the first time (4 in a 10-minute window), and rollup output collapsed
        // from ~9,000 rows/min to 10 — 469 rows in 46 minutes. The split-on-claim
        // and abandon-bisect paths were expected to right-size those units and did
        // not, or not nearly fast enough.
        //
        // What this test must NOT do is give up. Day-or-nothing meant an
        // over-budget day kept all 144 of its ten-minute slices, and on an
        // uncompacted sealed partition every one of those slices re-reads the
        // WHOLE day anyway — no timestamp-stat pruning can skip a file that
        // spans it. So the fallback for a day too big to scan once was to scan
        // it 144 times. That is why this is a cascade: the day that does not fit
        // is offered six hours, then one hour, and lands at the widest span it
        // can actually finish. Prod 2026-08-19 sat at 84,834 pending base
        // rollups with coarsening logging 12, 6, 3, then nothing — everything
        // collapsible had collapsed and the rest was over budget forever.
        //
        // The children's own estimates are already summed here, so the test is
        // free.
        groups.retain(|group, bytes| {
            let fits = *bytes <= MAX_DECODED_BYTES;
            if !fits {
                report.over_budget += members.get(group).copied().unwrap_or(0);
            }
            fits
        });
        if groups.is_empty() {
            return report;
        }
        let collapsed = self.snapshot.tasks.len();
        self.snapshot.tasks.retain(|task| {
            !coarsenable(task)
                || !groups.contains_key(&(
                    task.key.physical_table.clone(),
                    task.key.source.clone(),
                    task.key.project_id.clone(),
                    task.key.operation,
                    bucket_of(task.key.slice.start_micros),
                ))
        });
        report.fused = collapsed - self.snapshot.tasks.len();
        self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
        for ((physical_table, source, project_id, operation, bucket), bytes) in groups {
            let Ok(slice) = TimeSlice::new(bucket, bucket.saturating_add(width)) else { continue };
            self.upsert(MaintenanceTask {
                key: TaskKey { physical_table, source, project_id, slice, operation },
                state: TaskState::Pending,
                deadline_micros: now_micros,
                estimated_decoded_bytes: bytes,
                hash_shard: 0,
                hash_shards: 1,
                attempts: 0,
                created_unix_ms: u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default(),
                retry_reason: None,
                publication: None,
                base_tier_present: false,
            });
        }
        report
    }

    /// Record that the base tier a queued derived unit aggregates already
    /// exists. Returns whether anything changed.
    ///
    /// The planner cannot do this through `enqueue`, because it SKIPS every day
    /// that already has rollup work queued (`want.retain(|key| !queued...)`) —
    /// and a day with a stuck derived task is exactly such a day. So the tasks
    /// that most need the proof are the ones `enqueue` can never reach: prod
    /// 2026-08-18 22:50 UTC, `pending_derived_rollup` still did not move after
    /// #184 shipped, because all 759 of them predated it.
    /// Applies to EVERY derived task in the day, at whatever width.
    ///
    /// Keyed on one exact day-wide `TaskKey`, this proved nothing on prod. The
    /// queued historical work is not day-wide: `invalidate` mints derived units
    /// at `DERIVED_SLICE_MICROS` (one hour), and `coarsen_sealed_slices` refuses
    /// to fuse a day whose day-wide unit already exists in ANY state — including
    /// `Complete`, which is exactly what a legacy rows=0 publication left behind.
    /// So the day carries hour-wide pending tasks plus a completed day-wide one,
    /// and a proof aimed at the day-wide key landed on the completed task, which
    /// is never claimed.
    ///
    /// Measured 2026-08-19 03:00 UTC by #194's census:
    ///
    ///     cells_missing=264  cells_wanted=0  defer_enqueue=false
    ///
    /// — the planner saw every hole, every one was vetoed as already-queued, and
    /// `rollup_derived_base_tier_proven` had not fired once. The tasks existed,
    /// were pending, and could not be claimed because `dependencies_complete`
    /// still had no proof for THEM.
    ///
    /// The fact being recorded is a property of the DAY — "the tier this derives
    /// from is already built" — so it belongs on every task covering that day.
    /// Why is queued work for `operation` not being claimed?
    ///
    /// Every counter this system has reports how much work EXISTS. None reports
    /// why a task that exists is passed over, and `claim_next` decides that
    /// inside filter predicates that leave no trace. That gap has now cost five
    /// fixes shipped against wrong models of the queue (#186, #189, #190, #192,
    /// #195), each individually correct.
    ///
    /// Returns `(pending, sealed, unproven, quarantined, not_yet_due)`, which
    /// between them cover every reason `claim_next` skips a pending task.
    ///
    /// `unproven` counts `!base_tier_present` rather than calling
    /// `dependencies_complete`, deliberately. That predicate is itself a scan of
    /// the whole task set, so calling it per task would make this census O(n^2)
    /// under the journal lock — ~900 x 55,000 every 60s on prod. The flag is the
    /// actionable half anyway: for a historical derived unit it is exactly what
    /// decides the dependency, and it is O(1).
    pub fn claimability_census(&self, operation: Operation, now_micros: i64) -> (usize, usize, usize, usize, usize) {
        let (mut pending, mut sealed, mut unproven, mut quarantined, mut not_due) = (0, 0, 0, 0, 0);
        for task in self.snapshot.tasks.iter().filter(|task| task.key.operation == operation) {
            if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
                continue;
            }
            pending += 1;
            sealed += usize::from(!is_live_frontier(task.key.slice, now_micros));
            unproven += usize::from(!task.base_tier_present);
            quarantined += usize::from(Self::is_quarantined(task));
            not_due += usize::from(task.deadline_micros > now_micros);
        }
        (pending, sealed, unproven, quarantined, not_due)
    }

    /// The first SEALED task of `operation` that `claim_next` would refuse, and
    /// why — as `(project, date, reason)`.
    ///
    /// Every count in `claimability_census` is a property of the task in
    /// isolation. None of them answers the question that actually matters, which
    /// is why `best_class(sealed_only = true)` returns None while sealed tasks
    /// sit pending. Prod 2026-08-19 has been in exactly that state through four
    /// fixes: 141 sealed derived units, ~60 neither quarantined nor future-dated,
    /// and not one claimed — every derived claim an hour-wide slice of today.
    ///
    /// Bounded to `LIMIT` evaluations because `dependencies_complete` is a scan;
    /// a sample is enough to name the reason, and naming it is the whole point.
    pub fn first_refused_sealed(&self, operation: Operation, now_micros: i64) -> Option<(String, String, &'static str)> {
        const LIMIT: usize = 64;
        let why = |task: &MaintenanceTask| -> &'static str {
            if task.deadline_micros > now_micros {
                "not_due"
            } else if Self::is_quarantined(task) {
                "quarantined"
            } else if !self.dependencies_complete(task) {
                "dependencies"
            } else {
                // Nothing about the task refuses it, so the refusal is upstream —
                // in class ordering or the operation cycle, not in eligibility.
                "CLAIMABLE"
            }
        };
        let describe = |task: &MaintenanceTask| {
            let date = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros)
                .map(|time| time.date_naive().to_string())
                .unwrap_or_else(|| "?".to_owned());
            (task.key.project_id.clone(), date, why(task))
        };
        let mut sealed = self.snapshot.tasks.iter().filter(|task| {
            task.key.operation == operation && matches!(task.state, TaskState::Pending | TaskState::Retry) && !is_live_frontier(task.key.slice, now_micros)
        });
        // A claimable one is the interesting answer: it means eligibility is fine
        // and the refusal is in ordering. Otherwise report the first task's reason.
        let sample: Vec<_> = sealed.by_ref().take(LIMIT).collect();
        sample.iter().find(|task| why(task) == "CLAIMABLE").or_else(|| sample.first()).map(|task| describe(task))
    }

    /// Publish which `(source, project, date)` have their BASE tier built, read
    /// from real rollup coverage. Replaces the set wholesale: coverage can go
    /// backwards (a rewrite, a vacuum), and a stale "ready" is the direction
    /// that derives from a tier that is not there.
    pub fn set_base_tier_ready(&mut self, ready: HashSet<(String, String, String)>) {
        self.base_tier_ready = ready;
    }

    pub fn base_tier_ready_len(&self) -> usize {
        self.base_tier_ready.len()
    }

    /// Which sources contributed to the published coverage and holes. Exists so
    /// a test can prove the sets are not merely the last source planned.
    pub fn base_tier_ready_sources(&self) -> HashSet<String> {
        self.base_tier_ready.iter().map(|(source, ..)| source.clone()).collect()
    }

    pub fn tier_hole_sources(&self) -> HashSet<String> {
        self.tier_holes.iter().map(|(source, ..)| source.clone()).collect()
    }

    /// Publish which `(source, project, tier table, date)` are MISSING, so
    /// `claim_next` can rank holes ahead of re-derives. Replaced wholesale for
    /// the same reason as `base_tier_ready`.
    pub fn set_tier_holes(&mut self, holes: HashSet<(String, String, String, String)>) {
        self.tier_holes = holes;
    }

    pub fn tier_holes_len(&self) -> usize {
        self.tier_holes.len()
    }

    /// Is this task filling a hole rather than re-deriving a day that already
    /// has tier output? Only meaningful for rollup operations.
    fn fills_a_hole(&self, task: &MaintenanceTask) -> bool {
        if !matches!(task.key.operation, Operation::BaseRollup | Operation::DerivedRollup) {
            return false;
        }
        chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).is_some_and(|time| {
            self.tier_holes.contains(&(task.key.source.clone(), task.key.project_id.clone(), task.key.physical_table.clone(), time.date_naive().to_string()))
        })
    }

    pub fn prove_base_tier_for_day(&mut self, key: &TaskKey, day_start: i64, day_end: i64) -> usize {
        let mut proven = 0;
        for task in &mut self.snapshot.tasks {
            // Only work that can still run. A completed task matches its own
            // day and proving it is a no-op that would make `proven` read as
            // progress when nothing became claimable.
            if !matches!(task.state, TaskState::Pending | TaskState::Retry)
                || task.base_tier_present
                || task.key.operation != key.operation
                || task.key.source != key.source
                || task.key.project_id != key.project_id
                || task.key.physical_table != key.physical_table
                || task.key.slice.start_micros < day_start
                || task.key.slice.end_micros > day_end
            {
                continue;
            }
            task.base_tier_present = true;
            self.dirty_tasks.insert(task.key.clone());
            proven += 1;
        }
        proven
    }

    pub fn upsert(&mut self, task: MaintenanceTask) {
        self.dirty_tasks.insert(task.key.clone());
        if let Some(index) = self.task_indices.get(&task.key).copied() {
            self.snapshot.tasks[index] = task;
        } else {
            let index = self.snapshot.tasks.len();
            self.task_indices.insert(task.key.clone(), index);
            self.snapshot.tasks.push(task);
        }
    }

    pub fn enqueue(&mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64) {
        self.enqueue_with_base_tier(key, deadline_micros, estimated_decoded_bytes, created_unix_ms, false);
    }

    /// `base_tier_present` records that the tier this unit aggregates already
    /// exists — see the field on [`MaintenanceTask`]. Only `plan_rollup_backfill`
    /// can prove it, because only it reads actual tier coverage.
    pub fn enqueue_with_base_tier(&mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64, base_tier_present: bool) {
        if let Some(index) = self.task_indices.get(&key).copied() {
            let task = &mut self.snapshot.tasks[index];
            if task.state != TaskState::Running {
                let new_deadline = task.deadline_micros.min(deadline_micros);
                let changed = task.state != TaskState::Pending
                    || task.deadline_micros != new_deadline
                    || task.estimated_decoded_bytes != estimated_decoded_bytes
                    || task.retry_reason.is_some()
                    || task.publication.is_some()
                    // Latching, never clearing: the planner proves presence, and
                    // a later enqueue that cannot prove it (the frontier's) is
                    // silence, not evidence of absence.
                    || (base_tier_present && !task.base_tier_present);
                if changed {
                    task.state = TaskState::Pending;
                    task.deadline_micros = new_deadline;
                    task.estimated_decoded_bytes = estimated_decoded_bytes;
                    task.retry_reason = None;
                    task.publication = None;
                    task.base_tier_present |= base_tier_present;
                    self.dirty_tasks.insert(key);
                }
            }
            return;
        }
        self.dirty_tasks.insert(key.clone());
        let index = self.snapshot.tasks.len();
        self.task_indices.insert(key.clone(), index);
        self.snapshot.tasks.push(MaintenanceTask {
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
            base_tier_present,
        });
    }

    /// Record all maintenance consequences of a source mutation. Repeated
    /// invalidations are idempotent by `TaskKey`; an already-complete slice is
    /// made pending again and its quiet-period deadline moves forward.
    pub fn invalidate(&mut self, invalidation: Invalidation<'_>) -> anyhow::Result<()> {
        let Invalidation { source_table, rollup_table, source, project_id, start_micros, end_micros, observed_at_micros, derived } = invalidation;
        // Round up, never down: a bucket can delay eligibility slightly but
        // can never publish before the full quiet period. High-rate ingest
        // then journals one deadline extension per bucket rather than one full
        // task record per event batch.
        let deadline = observed_at_micros.saturating_add(FINALIZATION_DELAY_MICROS);
        let deadline_micros = deadline
            .saturating_add(INVALIDATION_DEADLINE_BUCKET_MICROS - 1)
            .div_euclid(INVALIDATION_DEADLINE_BUCKET_MICROS)
            .saturating_mul(INVALIDATION_DEADLINE_BUCKET_MICROS);
        let created_unix_ms = u64::try_from(observed_at_micros.div_euclid(1_000)).unwrap_or_default();
        let normal_slices = TimeSlice::normal_units(start_micros, end_micros)?;
        let rollup_slices = if derived {
            let aligned_start = start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            let aligned_end = end_micros.saturating_add(DERIVED_SLICE_MICROS - 1).div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            TimeSlice::fixed_units(aligned_start, aligned_end, DERIVED_SLICE_MICROS)?
        } else {
            normal_slices.clone()
        };
        // HotPacking is deliberately NOT here. File hygiene is planned by DEBT,
        // not by the calendar: `plan_compaction_debt` scans the actual file list
        // per (project, date) every 60s and mints ONE day-wide unit when the
        // partition really has small or unsorted files
        // (`small.len() >= 2 || any !sorted`) — HotPacking for today,
        // SealedConsolidation once the day seals.
        //
        // Minting one per ten-minute slice here duplicated that with 144 units
        // per project per day whose count tracked ingest rather than
        // fragmentation. It was 5,367 pending on prod 2026-08-17 — 22% of the
        // whole journal — for an operation that produces no rollup coverage,
        // and it is a third of everything the live frontier creates. The
        // frontier wanted ~7.8 units/min against a total drain of ~11.6.
        //
        // The rest of the codebase already treated these as waste:
        // `migrate_fine_grained_backfill` and `coarsen_sealed_slices` both list
        // HotPacking as coarsenable, and the comment on the former names it in
        // the "~450 durable tasks per (project, date)" expansion that coarse
        // planning exists to undo. This stops minting them rather than
        // collapsing them afterwards.
        for (operation, slices) in
            [(Operation::Dedup, normal_slices.as_slice()), (if derived { Operation::DerivedRollup } else { Operation::BaseRollup }, rollup_slices.as_slice())]
        {
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
                        self.dirty_tasks.insert(key);
                    }
                } else {
                    let index = self.snapshot.tasks.len();
                    self.task_indices.insert(key.clone(), index);
                    self.snapshot.tasks.push(MaintenanceTask {
                        key: key.clone(),
                        state: TaskState::Pending,
                        deadline_micros,
                        estimated_decoded_bytes: 0,
                        hash_shard: 0,
                        hash_shards: 1,
                        attempts: 0,
                        created_unix_ms,
                        retry_reason: None,
                        publication: None,
                        base_tier_present: false,
                    });
                    self.dirty_tasks.insert(key);
                }
            }
        }
        Ok(())
    }

    pub fn mark_running(&mut self, key: &TaskKey) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
            return false;
        }
        task.state = TaskState::Running;
        task.attempts = task.attempts.saturating_add(1);
        true
    }

    /// Attempts after which a unit has PROVEN it does not fit its deadline.
    ///
    /// One timeout is a blip — a FairSpillPool squeeze, an object-store stall.
    /// Two is the slice itself, and is exactly the threshold `abandon_running`
    /// already uses to decide a unit is oversized rather than unlucky.
    pub const QUARANTINE_ATTEMPTS: u32 = 2;

    /// The `retry_reason` `abandon_running` writes when a WORKER gave a unit
    /// back — a deadline it could not meet, or an error inside the unit.
    pub const WORKER_FAILURE_REASON: &'static str = "worker_error";

    /// Has this unit PROVEN it cannot fit its deadline?
    ///
    /// Attempts alone is not that proof, and using it as the proxy quarantined
    /// the wrong work. A task is claimed and its attempt counted before anything
    /// about its cost is known, so a unit that was handed back for a reason
    /// unrelated to cost — `source_not_flushed`, `resolve_input`, a dependency
    /// that was not yet satisfiable — accumulates attempts identically to one
    /// that burned a 900s deadline.
    ///
    /// Measured on prod 2026-08-19: of 162 SEALED derived units — precisely the
    /// historical backfill the 30d goal needs — **109 were quarantined**, held
    /// to 2 of 16 workers. They had failed repeatedly while their base-tier
    /// dependency was unprovable, which #197/#202 then fixed. Their attempt
    /// count was stale evidence about a condition that no longer existed, and
    /// the quarantine kept punishing them for it.
    ///
    /// So require the worker's own verdict: only a unit `abandon_running` gave
    /// back is evidence about cost. Anything else retries normally.
    pub fn is_quarantined(task: &MaintenanceTask) -> bool {
        task.attempts >= Self::QUARANTINE_ATTEMPTS && task.retry_reason.as_deref() == Some(Self::WORKER_FAILURE_REASON)
    }

    /// Claim one unit. `allow_quarantined` admits units that have already timed
    /// out [`Self::QUARANTINE_ATTEMPTS`] times; the caller gates it on a small
    /// occupancy permit so proven-unfittable work cannot hold the whole pool.
    ///
    /// Measured on prod 2026-08-18 21:00 UTC over 60 minutes of logs: 47 units
    /// timed out (BaseRollup 24, HotPacking 12, SealedConsolidation 11) against
    /// a 900s deadline each. That is 42,300 of 57,600 available worker-seconds —
    /// 73% of ALL maintenance capacity — spent committing nothing, while
    /// ~40,000 pending BaseRollup units could not get a slot. Over 180s
    /// `tasks_complete` rose by 2 with `tasks_running` pinned at 16, and the one
    /// rollup that completed took 812ms.
    ///
    /// Neither existing lever reaches this. `abandon_running`'s backoff decides
    /// how OFTEN a doomed unit runs, never what it costs when it does; and its
    /// bisection makes the total worse, because halving a slice cannot halve a
    /// per-file cost — measured at ~3.2s per parquet file the same day — it only
    /// doubles the number of units paying it. #176's cap bounds occupancy, which
    /// is the right lever, but exempts BaseRollup, and BaseRollup is half the
    /// timeouts: the exemption assumed rollup units advance coverage, which is
    /// true only of one that COMPLETES.
    pub fn claim_next(&mut self, operation: Operation, now_micros: i64, allow_quarantined: bool) -> Option<MaintenanceTask> {
        // The winning scheduling class, as a streaming minimum.
        //
        // This used to call `fair_ready_tasks`, which builds a BTreeMap of every
        // ready task plus a fully materialised Vec, and then took `.find()` —
        // discarding all of it to keep one `(class, order_key)` tuple. That is
        // O(n log n) and two large allocations PER CLAIM, on every worker.
        // Tolerable at 18k tasks; at 128k (2026-08-17, after the rollup backfill
        // queued real history) it started showing up as live-frontier lag, and
        // it scales the wrong way for the 10x-projects target.
        //
        // The minimum over the same predicate is identical: groups are ordered
        // by `(class, operation.priority(), order_key)` and the operation is
        // fixed here, so the first match `find` returned was exactly the
        // smallest `(class, order_key)` among eligible tasks for this operation.
        //
        // `dependencies_complete` is itself a scan, so it is evaluated ONLY when
        // a task would actually improve the current best — otherwise this would
        // be quadratic for `DerivedRollup`.
        //
        // One claim in three is also RESERVED for sealed work. Class is strict
        // priority and ingest generates live-frontier work continuously, so
        // without a reservation class 1 never runs at all. Prod 2026-08-17, over
        // 278 consecutive task starts: every one was today or yesterday, not a
        // single sealed day. Rollup coverage for a live tenant stayed pinned at
        // two days for hours while the frontier was perfectly healthy
        // (`eligible_watermark_lag_seconds` 0) — and a 7d/14d/30d query needs
        // exactly the sealed days that never ran. Falls back to any class when
        // there is no sealed work, so quiet history never idles a worker.
        self.claim_tick = self.claim_tick.wrapping_add(1);
        // Every OTHER claim, not every third. Measured after the reservation
        // shipped: sealed work went from 0 of 278 task starts to 8 of 131
        // (6.1%), far short of the intended third, because a sealed turn falls
        // back to any class whenever the operation being claimed has no eligible
        // sealed task — and rollup work sits behind its own dedup. Raising the
        // share is the direct lever on how fast coverage walks backward.
        // Three claims in four, not one in two. Measured 2026-08-17 with the
        // per-operation gauges (#120): of 127,798 pending tasks, 118,794 are
        // sealed AND eligible right now — including 39,072 eligible BaseRollup —
        // while only ~9,000 are frontier. Yet the frontier was taking ~90% of
        // claims, because class 0 is strict priority and ingest regenerates it
        // continuously. A one-in-two reservation delivered ~10% sealed starts in
        // practice, so rollup coverage sat frozen at 2026-08-11 for hours with
        // 39k claimable rollup tasks queued behind a much smaller frontier.
        //
        // The frontier is small in volume, so one claim in four still clears it;
        // if it stops keeping up, `eligible_watermark_lag_seconds` says so and
        // this is the dial to turn back.
        // Back to one in two after an OOM. Prod 2026-08-17 09:39:02:
        //
        //   maintenance-wor invoked oom-killer
        //   Killed process (timefusion) anon-rss: 124,921,780 kB  (124.9 GB)
        //
        // RSS was 11.9 GB two minutes earlier, so this was a fan-in spike, not
        // drift. Three-in-four routes far more SEALED partitions through the
        // heavy path at once, and historical partitions are much larger than
        // frontier slices — so the same permit count admits far more bytes.
        // permits=10 + jobs=16 had run 1.5h clean before this landed; the share
        // is the variable that changed immediately before the kill.
        //
        // One in two ran ~6h without an OOM. Raising it again needs the per-sort
        // budget cut to pay for it (the documented 2026-07-04 pairing), not just
        // a bigger share.
        // ...and halve it while the frontier is behind. The comment
        // above names `eligible_watermark_lag_seconds` as the dial to turn when
        // the frontier stops keeping up; this turns it automatically, in both
        // directions, instead of waiting for someone to notice.
        //
        // Frontier lag is not just staleness, it is a per-query cost:
        // `raw_tail_duration_secs` is `FINALIZATION_DELAY + lag`, and EVERY
        // hybrid rollup query scans that tail. Prod 2026-08-17 sat at 62
        // minutes of raw tail, so each query paid an hour of raw scan no matter
        // how good the rollup coverage was.
        //
        // Safe in the direction that matters: the 3-in-4 sealed share
        // OOM-killed prod at 124.9 GB because sealed partitions are far larger
        // than frontier slices. This moves share AWAY from sealed, so it cannot
        // reproduce that.
        //
        // HALVED to one-in-four, not withdrawn. The comment above calls the
        // frontier "small in volume", but `LIVE_FRONTIER_WINDOW_MICROS` is 24
        // HOURS — a full day of ten-minute slices across every stream, which on
        // prod 2026-08-17 was ~7.8 units/min of creation against 3.8 claimed,
        // hence a lag climbing past 98 minutes. Withdrawing the reservation
        // entirely would let the frontier catch up fastest but would stop the
        // sealed backfill that builds 30d coverage, and coverage is the other
        // half of the same goal. One in four leaves both moving.
        //
        // This is not merely a freshness knob: `rollup_min_contiguous_days`
        // counts back from YESTERDAY, so a frontier that never finishes today
        // guarantees tomorrow's yesterday is holed, and the coverage metric can
        // never leave zero. Frontier health is a PREREQUISITE for the coverage
        // goal, not a competitor to it.
        //
        // DerivedRollup always takes the sealed turn, because for THIS operation
        // the reservation's premise does not hold. The frontier mints derived
        // work at `DERIVED_SLICE_MICROS` — one unit per stream per HOUR, ~24 a
        // day — against 144 Dedup plus 144 BaseRollup for the same stream-day.
        // Derived is roughly 3% of frontier creation, so preferring sealed for
        // it cannot meaningfully starve the frontier, which is the entire reason
        // the share is throttled.
        //
        // What it does buy is the goal. The derived tier is the one 14d/30d
        // dashboards read, and it is the coverage gap: prod 2026-08-19 00:10 UTC
        // had the 1m base tier 33 days deep against a 1h tier at 9-17, with 387
        // historical day-wide derived units freshly unblocked by #186/#188 — and
        // EVERY derived unit claimed in the following 12 minutes was a one-hour
        // frontier slice for today or yesterday. Class is strict priority, the
        // frontier regenerates continuously, so without a reservation the
        // historical units never run at all.
        //
        // Costs nothing when there is no sealed derived work: the caller already
        // falls back to any class.
        let sealed_turn = operation == Operation::DerivedRollup
            || if self.frontier_lag_secs.load(std::sync::atomic::Ordering::Relaxed) > FRONTIER_LAG_BUDGET_SECS {
                self.claim_tick.is_multiple_of(4)
            } else {
                self.claim_tick.is_multiple_of(2)
            };
        let claimable = |task: &MaintenanceTask| {
            task.key.operation == operation
                && matches!(task.state, TaskState::Pending | TaskState::Retry)
                && task.deadline_micros <= now_micros
                && (allow_quarantined || !Self::is_quarantined(task))
        };
        // `(class, hole_rank, width, recency)`. Class still leads, so the live
        // frontier keeps strict priority over sealed work. `hole_rank` orders
        // WITHIN a class: a cell whose tier output is missing outranks one that
        // already has output and is merely being re-derived.
        //
        // Without it, sealed rollup work is strictly newest-first, and recent
        // days are re-invalidated continuously by ongoing publication — so the
        // claim never walks back far enough to reach an old hole. Prod
        // 2026-08-19 09:00: `94c5dc1f`'s 1h tier jumped 2026-07-31 -> 08-14 for
        // a second day running, while day-wide derived units for 08-17 were
        // claimed repeatedly. Newest-first is right for FRESHNESS and wrong for
        // CONTIGUITY, and 30 contiguous days is a contiguity goal.
        let rank = |journal: &Self, task: &MaintenanceTask| -> (u8, u8, u8, i64, i64) {
            let (class, starved, width, order) = scheduling_class(task, now_micros);
            (class, starved, u8::from(!journal.fills_a_hole(task)), width, order)
        };
        let best_class = |journal: &Self, sealed_only: bool| -> Option<(u8, u8, u8, i64, i64)> {
            let mut class: Option<(u8, u8, u8, i64, i64)> = None;
            for task in journal.snapshot.tasks.iter().filter(|task| claimable(task) && !(sealed_only && is_live_frontier(task.key.slice, now_micros))) {
                let candidate = rank(journal, task);
                if class.is_none_or(|best| candidate < best) && journal.dependencies_complete(task) {
                    class = Some(candidate);
                }
            }
            class
        };
        let class = if sealed_turn { best_class(self, true).or_else(|| best_class(self, false)) } else { best_class(self, false) }?;
        let cursor = self.fair_cursors.get(&operation).map(String::as_str).unwrap_or("");
        let mut fallback: Option<&MaintenanceTask> = None;
        let mut next: Option<&MaintenanceTask> = None;
        for task in self.snapshot.tasks.iter().filter(|task| claimable(task) && rank(self, task) == class && self.dependencies_complete(task)) {
            if fallback.is_none_or(|current| {
                (&task.key.project_id, task.deadline_micros, &task.key) < (&current.key.project_id, current.deadline_micros, &current.key)
            }) {
                fallback = Some(task);
            }
            if task.key.project_id.as_str() > cursor
                && next.is_none_or(|current| {
                    (&task.key.project_id, task.deadline_micros, &task.key) < (&current.key.project_id, current.deadline_micros, &current.key)
                })
            {
                next = Some(task);
            }
        }
        let selected = next.or(fallback)?;
        let key = selected.key.clone();
        self.fair_cursors.insert(operation, key.project_id.clone());
        self.mark_running(&key);
        self.task_indices.get(&key).map(|index| self.snapshot.tasks[*index].clone())
    }

    fn dependencies_complete(&self, task: &MaintenanceTask) -> bool {
        let required = match task.key.operation {
            // Base publication performs its own bounded complete-key/tiebreak
            // dedup before aggregation. Physical source consolidation is
            // independent debt and must not block exact rollup coverage.
            Operation::BaseRollup => None,
            Operation::DerivedRollup => Some(Operation::BaseRollup),
            _ => None,
        };
        // Proven from real tier coverage, which is strictly better evidence than
        // the journal's own record of who built what. See the field's comment.
        //
        // The day-keyed set is checked first and is the one that actually works:
        // the per-task flag has to be set on exactly the right `TaskKey`, and
        // prod measured 674 of 674 pending derived tasks without it. Keying the
        // fact on the DAY it is a fact about cannot miss a task.
        if task.base_tier_present {
            return true;
        }
        if let Some(date) = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map(|time| time.date_naive().to_string())
            && self.base_tier_ready.contains(&(task.key.source.clone(), task.key.project_id.clone(), date))
        {
            return true;
        }
        required.is_none_or(|required| {
            let mut intervals = self
                .snapshot
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
                .collect::<Vec<_>>();
            intervals.sort_unstable();
            let mut covered_through = task.key.slice.start_micros;
            for interval in intervals {
                if interval.start_micros > covered_through {
                    break;
                }
                covered_through = covered_through.max(interval.end_micros);
                if covered_through >= task.key.slice.end_micros {
                    return true;
                }
            }
            false
        })
    }

    pub fn retry(&mut self, key: &TaskKey, reason: String, not_before_micros: i64) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Retry;
        crate::observability::set_maintenance_retry_reason(&reason);
        task.retry_reason = Some(reason);
        task.deadline_micros = not_before_micros;
        self.dirty_tasks.insert(key.clone());
        true
    }

    pub fn complete(&mut self, key: &TaskKey) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Complete;
        task.retry_reason = None;
        self.dirty_tasks.insert(key.clone());
        true
    }

    pub fn publish(&mut self, key: &TaskKey, publication: Publication) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Complete;
        task.retry_reason = None;
        task.publication = Some(publication);
        self.dirty_tasks.insert(key.clone());
        true
    }

    /// Supersede an oversized time unit with smaller durable children. The
    /// parent remains as a completed audit record. Hash shards intentionally
    /// stay inside a one-minute task because `TaskKey` identifies logical
    /// slice work; the worker merges all shard states before publication.
    /// A worker gave the task back without finishing it — an error, or a
    /// deadline it could not meet.
    ///
    /// Once is a blip: back off and retry it whole. Twice says the slice itself
    /// does not fit its deadline, and requeueing it unchanged is an infinite
    /// loop that holds a worker for the full deadline every pass and never
    /// produces anything — the failure this codebase has hit repeatedly, most
    /// recently as five Dedup timeouts in twelve minutes permanently occupying
    /// ~2 of 16 workers while the rollup horizon they starved sat days behind.
    /// So bisect instead, and let the halves face the same test.
    ///
    /// Byte-based splitting does not cover this. It fires on decoded bytes,
    /// while what overran was WALL TIME — a day-sized slice with modest bytes
    /// still pays an object-store round trip per file.
    pub fn abandon_running(&mut self, key: &TaskKey, now_micros: i64) {
        let attempts = self.task_indices.get(key).and_then(|index| self.snapshot.tasks.get(*index)).map_or(1, |task| task.attempts);
        // `MAX_DECODED_BYTES + 1` is the smallest input that makes
        // `byte_bounded_units` bisect: it asks for one split, not a full
        // recursive shred down to the minimum slice.
        if attempts >= 2 && self.split_time_task(key, MAX_DECODED_BYTES.saturating_add(1)) {
            return;
        }
        // Floored at this operation's OWN deadline. A unit that cannot be split
        // — repair is the standing case, since its cost is the file it rewrites
        // and time-bisection cannot shrink a file set — otherwise burns the full
        // deadline, waits out a backoff that tops out at 256s, and burns it
        // again: a ~78% duty cycle on a worker, forever, for a unit that has
        // never once produced anything.
        //
        // Measured on prod 2026-08-18: 7 Repair units timed out at 900s inside a
        // 15-minute window, which is 6,300 of the 14,400 available slot-seconds
        // — 44% of ALL maintenance capacity, spent on units that complete
        // nothing. Rollup coverage could not advance behind that no matter how
        // the scheduling cycle was weighted.
        let backoff_micros = i64::try_from((1u64 << attempts.min(8)).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        // The floor applies only after a REPEAT, for the same reason the split
        // above does: a FairSpillPool can squeeze out a perfectly sized unit that
        // would succeed untouched next pass, and making that unit wait a full
        // deadline would be a 15-minute penalty for someone else's memory spike.
        // A unit that has now failed twice is the one that is actually oversized.
        let delay_micros = if attempts >= 2 {
            let floor_micros = i64::try_from(operation_deadline_secs(key.operation).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            backoff_micros.max(floor_micros)
        } else {
            backoff_micros
        };
        self.retry(key, Self::WORKER_FAILURE_REASON.to_owned(), now_micros.saturating_add(delay_micros));
    }

    /// A unit that failed because its input did not FIT will fail identically
    /// on every retry — `retry` re-runs the same slice at the same size, so the
    /// backoff only decides how slowly it never finishes. This is
    /// [`Self::abandon_running`]'s argument, and byte-splitting answers it even
    /// more directly here: what overran was BYTES, which is exactly what
    /// `byte_bounded_units` divides.
    ///
    /// Still tolerate one: the maintenance pool is a FairSpillPool, so a unit
    /// can be squeezed out by whatever else happened to be running and succeed
    /// untouched next pass. Only a repeat says the slice itself is too big.
    ///
    /// Prod 2026-08-17 held ~5 units in this loop, each burning a worker and
    /// 50-80s of object-store work per pass, and each guarding a partition that
    /// stayed uncertified — which keeps `DedupExec` in every query plan over it.
    pub fn retry_or_split(&mut self, key: &TaskKey, reason: String, when_micros: i64, attempts: u32) {
        if attempts >= 2 && is_capacity_failure(&reason) && self.split_time_task(key, MAX_DECODED_BYTES.saturating_add(1)) {
            return;
        }
        self.retry(key, reason, when_micros);
    }

    pub fn split_time_task(&mut self, key: &TaskKey, observed_bytes: u64) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let parent = self.snapshot.tasks[index].clone();
        let children = byte_bounded_units(&parent, observed_bytes);
        if children.len() <= 1 || children.iter().any(|child| child.hash_shards > 1) {
            return false;
        }
        if let Some(index) = self.task_indices.get(key).copied() {
            let task = &mut self.snapshot.tasks[index];
            task.state = TaskState::Superseded;
            task.retry_reason = Some("split_into_smaller_slices".to_owned());
            self.dirty_tasks.insert(key.clone());
        }
        for mut child in children {
            child.state = TaskState::Pending;
            child.attempts = 0;
            child.retry_reason = None;
            child.publication = None;
            self.upsert(child);
        }
        true
    }

    /// A process may die after selecting work. Running is not a durable lease;
    /// requeue it at boot so recovery produces redundant work, never a hole.
    pub fn requeue_running(&mut self, now_micros: i64) -> usize {
        let mut count = 0;
        for task in &mut self.snapshot.tasks {
            if task.state == TaskState::Running {
                task.state = TaskState::Retry;
                task.deadline_micros = now_micros;
                task.retry_reason = Some("coordinator_restart".to_owned());
                self.dirty_tasks.insert(task.key.clone());
                count += 1;
            }
        }
        count
    }

    pub fn tasks(&self) -> impl Iterator<Item = &MaintenanceTask> {
        self.snapshot.tasks.iter()
    }

    pub fn state(&self, key: &TaskKey) -> Option<TaskState> {
        self.task_indices.get(key).map(|index| self.snapshot.tasks[*index].state)
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

    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.wal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !self.dirty_tasks.is_empty() || !self.dirty_cursors.is_empty() {
            let mut wal = OpenOptions::new().create(true).append(true).open(&self.wal_path)?;
            let mut records = Vec::new();
            for key in self.dirty_tasks.drain() {
                if let Some(index) = self.task_indices.get(&key).copied() {
                    let task = &self.snapshot.tasks[index];
                    serde_json::to_writer(&mut records, &JournalRecord::Task(task.clone()))?;
                    records.push(b'\n');
                }
            }
            for source in self.dirty_cursors.drain() {
                if let Some(delta_version) = self.snapshot.source_cursors.get(&source).copied() {
                    serde_json::to_writer(&mut records, &JournalRecord::SourceCursor { source, delta_version })?;
                    records.push(b'\n');
                }
            }
            wal.write_all(&records)?;
            wal.sync_all()?;
        }
        if fs::metadata(&self.wal_path).is_ok_and(|metadata| metadata.len() >= JOURNAL_COMPACT_BYTES) {
            self.compact()?;
        }
        self.publish_statistics();
        Ok(())
    }

    /// Rewrite the authoritative snapshot even when the WAL is below its
    /// normal size threshold. Migrations that remove tasks cannot represent
    /// those deletions as append-only WAL records, so they must force this
    /// compaction before startup continues.
    pub fn compact(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec(&self.snapshot)?;
        crate::write::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
        let wal = OpenOptions::new().create(true).write(true).truncate(true).open(&self.wal_path)?;
        wal.sync_all()?;
        self.dirty_tasks.clear();
        self.dirty_cursors.clear();
        Ok(())
    }

    pub fn publish_statistics(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        let mut counts = [0u64; 4];
        let mut backlog_bytes = 0u64;
        let mut sealed_debt_bytes = 0u64;
        let mut oldest_created = u64::MAX;
        let mut latest_frontier_rollup: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
        let mut per_operation = [0u64; 6];
        let (mut eligible_base_rollup, mut eligible_sealed) = (0u64, 0u64);
        let now_micros = crate::support::now_micros();
        for task in &self.snapshot.tasks {
            let index = match task.state {
                TaskState::Pending => 0,
                TaskState::Running => 1,
                TaskState::Retry => 2,
                TaskState::Complete => 3,
                TaskState::Superseded => 3,
            };
            counts[index] = counts[index].saturating_add(1);
            if !matches!(task.state, TaskState::Complete | TaskState::Superseded) {
                backlog_bytes = backlog_bytes.saturating_add(task.estimated_decoded_bytes);
                oldest_created = oldest_created.min(task.created_unix_ms);
                if task.key.operation == Operation::SealedConsolidation {
                    sealed_debt_bytes = sealed_debt_bytes.saturating_add(task.estimated_decoded_bytes);
                }
            }
            // Per-operation split, plus what is actually claimable now. When
            // coverage stalls the question is always: is the rollup work
            // absent, present-but-not-eligible, or present-and-eligible but
            // out-competed? Only these three views together answer it.
            if !matches!(task.state, TaskState::Complete | TaskState::Superseded) {
                per_operation[task.key.operation as usize] = per_operation[task.key.operation as usize].saturating_add(1);
                if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
                    if task.key.operation == Operation::BaseRollup {
                        eligible_base_rollup = eligible_base_rollup.saturating_add(1);
                    }
                    if !is_live_frontier(task.key.slice, now_micros) {
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
        stats.maintenance_tasks_complete.store(counts[3], Relaxed);
        stats.maintenance_backlog_bytes.store(backlog_bytes, Relaxed);
        stats.sealed_compaction_debt_bytes.store(sealed_debt_bytes, Relaxed);
        let eligible_lag_secs = frontier_lag_secs(latest_frontier_rollup.values().copied(), now_micros);
        stats.maintenance_eligible_watermark_lag_secs.store(eligible_lag_secs, Relaxed);
        self.frontier_lag_secs.store(eligible_lag_secs, Relaxed);
        stats
            .maintenance_raw_tail_duration_secs
            .store(u64::try_from(FINALIZATION_DELAY_MICROS / 1_000_000).unwrap_or_default().saturating_add(eligible_lag_secs), Relaxed);
        let processed = stats.maintenance_processed_bytes.load(Relaxed);
        let mut sample = THROUGHPUT_SAMPLE.get_or_init(|| Mutex::new((now_micros, processed))).lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = now_micros.saturating_sub(sample.0);
        if elapsed >= 1_000_000 {
            let rate = processed.saturating_sub(sample.1).saturating_mul(1_000_000) / u64::try_from(elapsed).unwrap_or(u64::MAX).max(1);
            stats.maintenance_processed_bytes_per_sec.store(rate, Relaxed);
            *sample = (now_micros, processed);
        }
        let now = u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default();
        let oldest_age_secs = if oldest_created != u64::MAX { now.saturating_sub(oldest_created) / 1_000 } else { 0 };
        stats.maintenance_oldest_task_age_secs.store(oldest_age_secs, Relaxed);
    }
}

/// `(class, operation priority, -width, -recency)` -> project -> that project's
/// queue. Ordered so smaller tuples run first; see `scheduling_class`.
type ReadyGroups<'a> = BTreeMap<(u8, u8, u8, i64, i64), HashMap<&'a str, VecDeque<&'a MaintenanceTask>>>;

/// Deadline ordering with round-robin selection among projects at the same
/// operation priority. This prevents one whale from consuming an entire pass.
pub fn fair_ready_tasks<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> Vec<&'a MaintenanceTask> {
    let mut groups: ReadyGroups<'_> = BTreeMap::new();
    for task in tasks {
        if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
            let (class, starved, width_key, order_key) = scheduling_class(task, now_micros);
            groups
                .entry((class, starved, task.key.operation.priority(), width_key, order_key))
                .or_default()
                .entry(&task.key.project_id)
                .or_default()
                .push_back(task);
        }
    }
    let mut ready = Vec::new();
    for projects in groups.values_mut() {
        let mut names: Vec<_> = projects.keys().copied().collect();
        names.sort_unstable();
        loop {
            let mut progressed = false;
            for name in &names {
                if let Some(task) = projects.get_mut(name).and_then(VecDeque::pop_front) {
                    ready.push(task);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
    }
    ready
}

/// Keep the live finalized frontier ahead of historical debt. Within the
/// frontier, newest eligible slices run first so sustained backfill cannot make
/// the raw tail grow without bound; tasks in the same minute still rotate by
/// project in `claim_next`. Once the frontier is caught up, historical work
/// returns to oldest-deadline order.
///
/// This is intentionally based on event time, not mutation deadline. A late
/// correction to old data is a bounded historical hole; treating its recent
/// mutation as live-tail work would let backfill displace current coverage.
/// A sealed task that has waited this long outranks newer sealed work.
///
/// Newest-first is right for freshness and starves anything old, because newer
/// sealed days keep arriving and never let the head of the queue advance.
/// Prod 2026-08-19, from the compaction dashboard: 2026-08-13 sat at 167 files
/// for FOUR DAYS — the same count, the same four tenants — while six younger
/// sealed days converged past it. Six successors overtaking a partition is not a
/// queue draining slowly; it is a partition the order never reaches.
///
/// Oldest-first is not the answer either, and this codebase has already tried
/// it: `scheduling_class`'s own comment records 10 of 10 historical starts
/// landing on data months old while the last 30 days went untouched. So age
/// tasks instead — the standard fix for exactly this. Normal ordering stays
/// newest-first; anything that has waited past the threshold escalates ahead of
/// it, and the escalated set is finite, so it cannot swallow the budget.
const STARVATION_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

fn scheduling_class(task: &MaintenanceTask, now_micros: i64) -> (u8, u8, i64, i64) {
    if is_live_frontier(task.key.slice, now_micros) {
        // Smaller tuples run first. Negating makes the newest minute the most
        // urgent while keeping all projects in that minute deadline-equivalent.
        // Width is not a frontier concern — these are all one slice wide.
        (0, 0, 0, -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
    } else {
        // Newest slice first here too, for the same reason the dedup drain and
        // the rollup backfill are newest-first: recent days are what dashboards
        // read, and history is debt nobody is querying.
        //
        // This was `deadline_micros` ascending — oldest-FIRST — which is the
        // opposite. Prod 2026-08-17, over 84 task starts: 74 went to the live
        // frontier (correct), and every one of the other 10 landed on
        // 2026-08-01, 07-22, 07-16, 07-15, 06-29, 06-15, 06-02, 06-01, 05-28.
        // All of the historical capacity was spent on data months old while the
        // last 30 days — the window a 30d dashboard query actually needs
        // certified and rolled up — was never reached. Coverage sat at two days.
        //
        // Eligibility is still deadline-gated in `claim_next`
        // (`deadline_micros <= now`), so retry backoff is unaffected; this only
        // orders the tasks that are already runnable.
        //
        // WIDTH outranks recency, though. A day-sized unit comes from the
        // backfill planner and is the only kind that advances the horizon; a
        // ten-minute unit is what the live path mints, and a day that has just
        // sealed carries ~144 of them per project per tier. Newest-first alone
        // therefore spends every sealed claim on yesterday's leftovers before
        // reaching the day before it — and midnight mints another day's worth,
        // so the horizon never moves. Prod 2026-08-17, 65 rollup starts in 25
        // minutes: 46 on today, 18 on yesterday, ZERO older, while 7d/14d
        // queries were refused for want of exactly those older days.
        //
        // Newest-first still breaks ties, so among backfill units recent
        // history is still built first.
        //
        // Starved work leads, then width, then recency — see `STARVATION_MICROS`.
        let waited = now_micros.saturating_sub(i64::try_from(task.created_unix_ms).unwrap_or(i64::MAX).saturating_mul(1_000));
        let starved = u8::from(waited < STARVATION_MICROS);
        (1, starved, -task.key.slice.width(), -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
    }
}

/// Does this task mean a day's ROLLUP work is already queued?
///
/// The precondition of a backfill is "nothing has rolled this day up yet", and
/// the planner enqueues exactly `Dedup` + the rollup tiers — so only those may
/// veto it. Matching every non-complete task on the source instead let
/// unrelated file debt disqualify a day forever: prod 2026-08-17 had the
/// whale's Aug 15 `SealedConsolidation` on attempt 370, which alone kept that
/// day out of every backfill pass.
pub fn blocks_rollup_backfill(task: &MaintenanceTask) -> bool {
    matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup) && task.state != TaskState::Complete
}

fn is_live_frontier(slice: TimeSlice, now_micros: i64) -> bool {
    slice.end_micros >= now_micros.saturating_sub(LIVE_FRONTIER_WINDOW_MICROS) && slice.start_micros <= now_micros
}

fn track_latest_frontier_rollup<'a>(latest: &mut HashMap<(&'a str, &'a str, &'a str), &'a MaintenanceTask>, task: &'a MaintenanceTask, now_micros: i64) {
    if task.key.operation != Operation::BaseRollup || !is_live_frontier(task.key.slice, now_micros) {
        return;
    }
    let stream = (task.key.source.as_str(), task.key.project_id.as_str(), task.key.physical_table.as_str());
    let active = u8::from(!matches!(task.state, TaskState::Complete | TaskState::Superseded));
    if latest.get(&stream).is_none_or(|current| {
        let current_active = u8::from(!matches!(current.state, TaskState::Complete | TaskState::Superseded));
        (task.key.slice.end_micros, active, task.deadline_micros) > (current.key.slice.end_micros, current_active, current.deadline_micros)
    }) {
        latest.insert(stream, task);
    }
}

fn frontier_lag_secs<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> u64 {
    tasks
        .into_iter()
        .filter(|task| !matches!(task.state, TaskState::Complete | TaskState::Superseded) && task.deadline_micros <= now_micros)
        .map(|task| u64::try_from(now_micros.saturating_sub(task.deadline_micros).div_euclid(1_000_000)).unwrap_or_default())
        .max()
        .unwrap_or_default()
}

/// Additional delay beyond the 15-minute quiet-period watermark for the live
/// rollup frontier. Historical holes remain visible through backlog and oldest
/// task age, but do not inflate the raw-tail gauge.
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

impl Resources {
    fn fits(self, available: Self) -> bool {
        self.cpu <= available.cpu
            && self.decoded_bytes <= available.decoded_bytes
            && self.object_reads <= available.object_reads
            && self.object_writes <= available.object_writes
    }

    fn checked_sub(self, request: Self) -> Option<Self> {
        Some(Self {
            cpu: self.cpu.checked_sub(request.cpu)?,
            decoded_bytes: self.decoded_bytes.checked_sub(request.decoded_bytes)?,
            object_reads: self.object_reads.checked_sub(request.object_reads)?,
            object_writes: self.object_writes.checked_sub(request.object_writes)?,
        })
    }

    fn saturating_add(self, released: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_add(released.cpu),
            decoded_bytes: self.decoded_bytes.saturating_add(released.decoded_bytes),
            object_reads: self.object_reads.saturating_add(released.object_reads),
            object_writes: self.object_writes.saturating_add(released.object_writes),
        }
    }
}

#[derive(Debug)]
struct AdmissionState {
    capacity: Resources,
    available: Resources,
}

/// Non-queuing multi-resource admission. Workers that cannot reserve every
/// resource return to the durable queue instead of sleeping on a semaphore.
#[derive(Clone, Debug)]
pub struct AdmissionController(Arc<Mutex<AdmissionState>>);

impl AdmissionController {
    pub fn new(cpu: u32, cgroup_memory_bytes: u64, object_reads: u32, object_writes: u32) -> Self {
        // At most 75% is trackable maintenance decode. The remainder is an
        // unconditional foreground/untracked-allocation reserve.
        let decoded_bytes = cgroup_memory_bytes.saturating_mul(3) / 4;
        let capacity = Resources { cpu, decoded_bytes, object_reads, object_writes };
        Self(Arc::new(Mutex::new(AdmissionState { capacity, available: capacity })))
    }

    pub fn try_acquire(&self, request: Resources) -> Option<AdmissionPermit> {
        if request.decoded_bytes > MAX_DECODED_BYTES {
            return None;
        }
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !request.fits(state.available) {
            return None;
        }
        state.available = state.available.checked_sub(request)?;
        Self::publish_utilization(&state);
        Some(AdmissionPermit { controller: self.clone(), resources: request })
    }

    pub fn utilization(&self) -> Resources {
        let state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Resources {
            cpu: state.capacity.cpu.saturating_sub(state.available.cpu),
            decoded_bytes: state.capacity.decoded_bytes.saturating_sub(state.available.decoded_bytes),
            object_reads: state.capacity.object_reads.saturating_sub(state.available.object_reads),
            object_writes: state.capacity.object_writes.saturating_sub(state.available.object_writes),
        }
    }

    fn publish_utilization(state: &AdmissionState) {
        use std::sync::atomic::Ordering::Relaxed;
        let used = Resources {
            cpu: state.capacity.cpu.saturating_sub(state.available.cpu),
            decoded_bytes: state.capacity.decoded_bytes.saturating_sub(state.available.decoded_bytes),
            object_reads: state.capacity.object_reads.saturating_sub(state.available.object_reads),
            object_writes: state.capacity.object_writes.saturating_sub(state.available.object_writes),
        };
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
        let mut state = self.controller.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.available = state.available.saturating_add(self.resources);
        debug_assert!(state.available.fits(state.capacity));
        AdmissionController::publish_utilization(&state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unit that cannot be split — repair is the standing case, since its cost
    /// is the file it rewrites and time-bisection cannot shrink a file set — must
    /// not come straight back. Burning a 900s deadline and returning 256s later
    /// is a ~78% duty cycle on a worker, forever, for a unit that has never
    /// produced anything.
    ///
    /// Prod 2026-08-18: 7 Repair units timed out at 900s inside a 15-minute
    /// window — 6,300 of 14,400 available slot-seconds, 44% of all maintenance
    /// capacity. Rollup coverage could not advance behind that whatever the
    /// scheduling cycle did.
    #[test]
    fn a_unit_that_burned_its_deadline_waits_at_least_that_long_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A single-slice repair unit: `byte_bounded_units` cannot divide it, so
        // the split path declines and the backoff is what bounds the waste.
        let mut unit = task("p", 0, 1, Operation::Repair);
        unit.attempts = 5;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 1_000_000_000;
        journal.abandon_running(&key, now);

        let deadline_micros = i64::try_from(operation_deadline_secs(Operation::Repair) * 1_000_000).expect("fits");
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        assert!(
            not_before >= now + deadline_micros,
            "a repair unit that burned {}s must wait at least that long again, waited {}s",
            deadline_micros / 1_000_000,
            (not_before - now) / 1_000_000
        );
    }

    /// A FIRST abandonment must still come back fast. The pool is a FairSpillPool,
    /// so a correctly sized unit can be squeezed out by someone else's memory
    /// spike and succeed untouched next pass — making that wait a full deadline
    /// would penalise the innocent case. Only a repeat says the unit is oversized,
    /// which is the same threshold the split above already uses.
    #[test]
    fn a_first_abandonment_is_not_floored() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, 1, Operation::Repair);
        unit.attempts = 1;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        journal.abandon_running(&key, 0);
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        assert_eq!(not_before, 2 * 1_000_000, "one failure retries on plain exponential backoff, not the deadline floor");
    }

    /// The floor must not REPLACE exponential backoff, only raise it. A unit that
    /// has failed many times should keep backing off past the deadline, or a
    /// permanently broken unit still returns every 15 minutes forever.
    #[test]
    fn the_deadline_floor_does_not_cap_exponential_backoff() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, 1, Operation::Dedup);
        unit.attempts = 8;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 0;
        journal.abandon_running(&key, now);
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        let exponential = 256 * 1_000_000i64;
        let floor = i64::try_from(operation_deadline_secs(Operation::Dedup) * 1_000_000).expect("fits");
        assert_eq!(not_before, now + exponential.max(floor), "the delay is the greater of the two, never the lesser");
    }

    fn task(project: &str, start: i64, end: i64, operation: Operation) -> MaintenanceTask {
        MaintenanceTask {
            key: TaskKey {
                physical_table: "table".into(),
                source: "source".into(),
                project_id: project.into(),
                slice: TimeSlice::new(start, end).expect("valid slice"),
                operation,
            },
            state: TaskState::Pending,
            deadline_micros: 0,
            estimated_decoded_bytes: 0,
            hash_shard: 0,
            hash_shards: 1,
            attempts: 0,
            created_unix_ms: 0,
            retry_reason: None,
            publication: None,
            base_tier_present: false,
        }
    }

    /// Only outstanding ROLLUP work may veto a rollup backfill.
    ///
    /// This predicate used to be "any non-complete task on the source", so a
    /// day carrying unrelated file debt was disqualified forever. Prod
    /// 2026-08-17: the whale's Aug 15 `SealedConsolidation` sat on attempt 370,
    /// and once the coarse-backfill migration retired the fine-grained
    /// historical tasks there was nothing left to claim and nothing allowed to
    /// replace it — every task start for 30 minutes was today's, rollup
    /// coverage froze at three days, and 7d/14d queries fell back to a full raw
    /// scan (`rollup_miss_not_built`).
    #[test]
    fn only_outstanding_rollup_work_blocks_a_backfill() {
        for operation in [Operation::SealedConsolidation, Operation::HotPacking, Operation::Repair] {
            let debt = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(!blocks_rollup_backfill(&debt), "{operation:?} is file debt, not rollup coverage; it must not veto a backfill");
        }
        for operation in [Operation::Dedup, Operation::BaseRollup, Operation::DerivedRollup] {
            let outstanding = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(blocks_rollup_backfill(&outstanding), "{operation:?} is the work a backfill would queue; re-queueing it pushes its deadline out");
            let mut done = outstanding;
            done.state = TaskState::Complete;
            assert!(!blocks_rollup_backfill(&done), "a completed {operation:?} leaves the day open to backfill again");
        }
    }

    #[test]
    fn recursively_splits_whales_and_hash_shards_one_minute() {
        let input = task("whale", 0, NORMAL_SLICE_MICROS, Operation::Dedup);
        let units = byte_bounded_units(&input, 10 * MAX_DECODED_BYTES);
        assert!(units.len() >= 10);
        assert!(units.iter().all(|unit| unit.estimated_decoded_bytes <= MAX_DECODED_BYTES));
        assert!(units.iter().all(|unit| unit.key.slice.width() >= MIN_SLICE_MICROS));

        let minute = task("whale", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let shards = byte_bounded_units(&minute, MAX_DECODED_BYTES * 3);
        assert_eq!(shards.len(), 3);
        assert!(shards.iter().all(|unit| unit.hash_shards == 3));
    }

    #[test]
    fn scheduler_rotates_projects_within_a_deadline() {
        let tasks =
            [task("a", 0, 1, Operation::Dedup), task("a", 1, 2, Operation::Dedup), task("b", 0, 1, Operation::Dedup), task("b", 1, 2, Operation::Dedup)];
        let order: Vec<_> = fair_ready_tasks(&tasks, 0).into_iter().map(|task| task.key.project_id.as_str()).collect();
        assert_eq!(order, ["a", "b", "a", "b"]);
    }

    #[test]
    fn journal_claims_rotate_projects_instead_of_restarting_at_first() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
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
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Production already carries the v1 marker. It must not suppress the
        // corrective cleanup after commit-range reconciliation ships.
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v1".to_owned(), 1);
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v2".to_owned(), 1);
        journal.upsert(task("pending-a", 0, 1, Operation::Dedup));
        journal.upsert(task("pending-b", 1, 2, Operation::BaseRollup));
        let mut complete = task("published", 2, 3, Operation::BaseRollup);
        complete.state = TaskState::Complete;
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
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("customer", 0, 1, Operation::BaseRollup).key;
        journal.enqueue(key.clone(), 10, 512, 1);
        journal.checkpoint().expect("first checkpoint");
        let first_size = fs::metadata(&journal.wal_path).expect("wal").len();

        journal.enqueue(key.clone(), 20, 512, 2);
        journal.checkpoint().expect("idempotent checkpoint");
        assert_eq!(fs::metadata(&journal.wal_path).expect("wal").len(), first_size);

        journal.enqueue(key, 5, 512, 3);
        journal.checkpoint().expect("earlier deadline checkpoint");
        assert!(fs::metadata(&journal.wal_path).expect("wal").len() > first_size);
    }

    #[test]
    fn recent_slices_precede_overdue_historical_work() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let old = task("old", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let recent = task("recent", now - MIN_SLICE_MICROS, now, Operation::BaseRollup);
        let ready = fair_ready_tasks([&old, &recent], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "recent");
    }

    #[test]
    fn newest_eligible_frontier_slice_precedes_older_frontier_debt() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let older_start = now - 12 * 60 * 60 * 1_000_000;
        let older = task("older", older_start, older_start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let newest = task("newest", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&older, &newest], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "newest");
    }

    #[test]
    fn frontier_minute_rotates_fairly_across_projects() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let start = now - 2 * MIN_SLICE_MICROS;
        let a = task("a", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let b = task("b", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let c = task("c", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&c, &a, &b], now);
        assert_eq!(ready.iter().map(|task| task.key.project_id.as_str()).collect::<Vec<_>>(), ["a", "b", "c"]);
    }

    #[test]
    fn recently_mutated_historical_hole_does_not_displace_frontier() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut correction = task("correction", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        correction.deadline_micros = now - 1;
        let mut frontier = task("frontier", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        frontier.deadline_micros = now - 60 * 1_000_000;
        let ready = fair_ready_tasks([&correction, &frontier], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "frontier");
    }

    #[test]
    fn future_clock_slice_does_not_displace_live_frontier() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let future = task("future", now + 24 * 60 * 60 * 1_000_000, now + 24 * 60 * 60 * 1_000_000 + MIN_SLICE_MICROS, Operation::BaseRollup);
        let frontier = task("frontier", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&future, &frontier], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "frontier");
    }

    /// Historical debt runs newest SLICE first, not oldest deadline first.
    ///
    /// This asserted the opposite until 2026-08-17, with no stated reason.
    /// Oldest-first is FIFO-fair, but it spends the whole non-frontier budget on
    /// the oldest data in the store — which is the data nobody queries. Prod,
    /// over 84 task starts: 74 went to the live frontier (correct) and every one
    /// of the other 10 landed on 2026-08-01, 07-22, 07-16, 07-15, 06-29, 06-15,
    /// 06-02, 06-01, 05-28. Meanwhile rollup coverage for a live tenant sat at
    /// two days and every 7d/14d/30d query fell back to a raw scan.
    ///
    /// Newest-first does not starve old debt: frontier work is rate-limited by
    /// ingest and recent slices are finite, so once the recent window drains the
    /// ordering walks backward on its own. What it guarantees is that the window
    /// a dashboard actually reads is reached FIRST.
    /// Sealed work must get a share of claims even while the frontier is busy.
    ///
    /// Class is strict priority and ingest never stops, so before the
    /// reservation class 1 simply never ran: prod 2026-08-17 went 278
    /// consecutive task starts without a single sealed day, while rollup
    /// coverage for a live tenant sat at two days and every 7d/14d/30d query
    /// fell back to a raw scan. A frontier that is healthy on its own metric
    /// (`eligible_watermark_lag_seconds` 0) tells you nothing about this.
    /// Pending work must be reportable per operation, and split by whether it
    /// is claimable right now.
    ///
    /// `tasks_pending` alone cannot distinguish "no rollup work queued" from
    /// "queued but not eligible" from "eligible but out-competed" — three states
    /// with three different fixes. Prod 2026-08-17 sat at ~128k pending with
    /// rollup coverage frozen for hours, and every attempt to tune it was a
    /// guess because these three were indistinguishable from outside.
    #[test]
    fn pending_work_is_reported_per_operation_and_by_eligibility() {
        use std::sync::atomic::Ordering::Relaxed;
        // publish_statistics reads the real clock, so eligibility has to be
        // expressed against it — an epoch-relative `now` makes every deadline
        // look long past and the eligible/pending distinction vanishes.
        let now = crate::support::now_micros();
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = |op, start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: op,
        };
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

    /// The coarse-backfill migration must be narrow: fine sealed backfill goes,
    /// everything else stays.
    ///
    /// It exists because ~450 tasks per (project, date) drained at ~19/min on
    /// prod — 111 hours for ~600 GB of real work. Dropping tasks is safe only
    /// because `plan_rollup_backfill` re-derives from rollup COVERAGE, but a
    /// migration that over-reaches would silently cancel live frontier work or
    /// the separately-planned compaction debt.
    #[test]
    fn coarse_backfill_migration_only_drops_fine_sealed_backfill() {
        let now = crate::support::now_micros();
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let day = 24 * 60 * 60 * 1_000_000i64;
        let sealed_start = now - 10 * day;
        let key = |op, start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: op,
        };
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

    #[test]
    fn sealed_work_gets_claims_while_the_frontier_is_busy() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");

        // Plenty of frontier work — the production condition, where ingest
        // keeps class 0 permanently non-empty — plus one sealed day.
        let key = |start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: Operation::BaseRollup,
        };
        for k in 1..20 {
            journal.enqueue(key(now - (k + 1) * MIN_SLICE_MICROS, now - k * MIN_SLICE_MICROS), now - 1, 1, 1);
        }
        let sealed = key(0, MIN_SLICE_MICROS);
        journal.enqueue(sealed.clone(), now - 1, 1, 1);

        // Enough sealed slices that the share, not the supply, is what limits
        // sealed claims — mirroring prod, where 118,794 of 127,798 pending tasks
        // were sealed and eligible while the frontier took ~90% of claims.
        let sealed_slices: Vec<TimeSlice> = (1..20).map(|k| TimeSlice::new(k * MIN_SLICE_MICROS, (k + 1) * MIN_SLICE_MICROS).expect("sealed slice")).collect();
        for slice in &sealed_slices {
            journal.enqueue(key(slice.start_micros, slice.end_micros), now - 1, 1, 1);
        }

        let mut sealed_claims = 0;
        for _ in 0..12 {
            let claimed = journal.claim_next(Operation::BaseRollup, now, true).expect("a task is always available");
            if !is_live_frontier(claimed.key.slice, now) {
                sealed_claims += 1;
            }
        }
        // "> 0" would pass at any share, including one too small to ever drain a
        // backlog — which is exactly what shipped first and left coverage frozen
        // for hours. Assert sealed work is the MAJORITY when it is the majority
        // of the queue.
        // Was `>= 7` (three in four). Lowered to the one-in-two floor after the
        // 2026-08-17 OOM: sealed work must still get a real share — "> 0" is the
        // bug that let an ineffective share ship — but the higher share cost
        // 124.9 GB of anon RSS and a kill, so the invariant is "a third or
        // better", not "the majority".
        assert!(
            sealed_claims >= 4,
            "sealed work got only {sealed_claims}/12 claims; below a third, a 118k-task sealed backlog never drains and long windows stay unroutable"
        );
    }

    #[test]
    fn historical_debt_runs_newest_slice_first() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        // Older slice, and the older deadline too — under the previous ordering
        // this ran first purely because it had been waiting longest.
        let mut older_slice = task("a", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        older_slice.deadline_micros = now - 2 * 60 * 1_000_000;
        let mut newer_slice = task("b", MIN_SLICE_MICROS, 2 * MIN_SLICE_MICROS, Operation::BaseRollup);
        newer_slice.deadline_micros = now - 60 * 1_000_000;
        let ready = fair_ready_tasks([&older_slice, &newer_slice], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "b", "the more recent slice is the one a dashboard query needs");
    }

    #[test]
    fn live_frontier_lag_ignores_historical_holes() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut historical = task("project", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        historical.deadline_micros = now - 4 * 60 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&historical], now), 0);
    }

    #[test]
    fn live_frontier_lag_tracks_only_each_streams_newest_slice() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut older = task("project", now - 3 * MIN_SLICE_MICROS, now - 2 * MIN_SLICE_MICROS, Operation::BaseRollup);
        older.deadline_micros = now - 10 * 60 * 1_000_000;
        let mut newest = task("project", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        newest.deadline_micros = now - 2 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&older, &newest], now), 2 * 60);
        newest.state = TaskState::Complete;
        assert_eq!(live_frontier_lag_secs([&older, &newest], now), 0, "an older hole is not the raw tail once newer coverage landed");
    }

    #[test]
    fn live_frontier_lag_reports_the_slowest_project() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut fast = task("fast", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        fast.deadline_micros = now - 60 * 1_000_000;
        let mut slow = task("slow", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        slow.deadline_micros = now - 5 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&fast, &slow], now), 5 * 60);
    }

    #[test]
    fn derived_invalidations_use_one_aligned_hour() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "derived",
                source: "source",
                project_id: "p",
                start_micros: NORMAL_SLICE_MICROS,
                end_micros: 2 * NORMAL_SLICE_MICROS,
                observed_at_micros: 0,
                derived: true,
            })
            .expect("invalidate");
        let derived = journal.tasks().find(|task| task.key.operation == Operation::DerivedRollup).expect("derived task");
        assert_eq!(derived.key.slice.width(), DERIVED_SLICE_MICROS);
        assert_eq!(derived.key.slice.start_micros % DERIVED_SLICE_MICROS, 0);
        assert!(journal.tasks().filter(|task| task.key.operation == Operation::Dedup).all(|task| task.key.slice.width() == NORMAL_SLICE_MICROS));
    }

    #[test]
    fn restart_migrates_unpublished_derived_fragments_to_hours() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(task("p", 0, NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        journal.checkpoint().expect("old checkpoint");

        let mut journal = TaskJournal::load(dir.path()).expect("journal to migrate");
        assert!(journal.migrate_derived_slices() > 0);
        journal.checkpoint().expect("migration checkpoint");
        let journal = TaskJournal::load(dir.path()).expect("migrated journal");
        assert!(journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup && task.key.slice.width() == DERIVED_SLICE_MICROS));
        assert!(journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup
            && task.key.slice.width() == NORMAL_SLICE_MICROS
            && task.state == TaskState::Superseded));
    }

    #[test]
    fn journal_round_trips_tasks_and_monotonic_cursors() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
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
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
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
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.publish(&key, Publication { source_fingerprint: 7, generation: "stable".to_owned(), rows: 0 }));
        journal.checkpoint().expect("checkpoint");

        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        let publication = loaded.published_rollups("source", "table").into_iter().next().expect("published empty slice").1;
        assert_eq!(publication.rows, 0);
        assert_eq!(publication.source_fingerprint, 7);
    }

    #[test]
    fn dropping_a_running_lease_durably_requeues_the_task() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.mark_running(&key));
        journal.checkpoint().expect("running checkpoint");
        let journal = Arc::new(Mutex::new(journal));
        let before_drop = crate::support::now_micros();
        drop(TaskLease::new(Arc::clone(&journal), key.clone()));

        let journal_guard = journal.lock().expect("lock");
        assert_eq!(journal_guard.state(&key), Some(TaskState::Retry));
        let task = journal_guard.snapshot.tasks.iter().find(|task| task.key == key).expect("requeued task");
        assert_eq!(task.retry_reason.as_deref(), Some("worker_error"));
        assert!(task.deadline_micros >= before_drop + 2_000_000, "first failed attempt must use the same exponential backoff as explicit worker errors");
        drop(journal_guard);
        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        assert_eq!(loaded.state(&key), Some(TaskState::Retry));
    }

    #[test]
    fn every_crash_boundary_recovers_to_redundant_work_or_published_coverage() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();

        // WAL/source invalidation checkpoint: a crash leaves pending work and
        // therefore no coverage claim.
        journal.upsert(input);
        journal.checkpoint().expect("invalidation checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after invalidation");
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));
        assert!(recovered.published_rollups("source", "table").is_empty());

        // Rollup staging / target-commit-before-coverage checkpoint: claims are
        // deliberately transient, so restart sees the last durable Pending
        // state directly. An already-landed target commit is safely replaced
        // by that redundant retry without a full-journal claim checkpoint.
        assert!(recovered.mark_running(&key));
        recovered.checkpoint().expect("running checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after staging");
        assert_eq!(recovered.requeue_running(100), 0);
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));

        // Coverage checkpoint is the only boundary that makes the slice
        // readable after restart, including an empty output.
        assert!(recovered.publish(&key, Publication { source_fingerprint: 9, generation: "g".to_owned(), rows: 0 }));
        recovered.checkpoint().expect("coverage checkpoint");
        let recovered = TaskJournal::load(dir.path()).expect("recover publication");
        assert_eq!(recovered.published_rollups("source", "table").len(), 1);
    }

    #[test]
    fn invalidation_is_idempotent_and_extends_the_quiet_period() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: 10,
                derived: false,
            })
            .expect("invalidate");
        journal.checkpoint().expect("first invalidation checkpoint");
        let first_wal_size = fs::metadata(&journal.wal_path).expect("wal").len();
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: 20,
                derived: false,
            })
            .expect("invalidate again");
        journal.checkpoint().expect("same-bucket checkpoint");
        assert_eq!(fs::metadata(&journal.wal_path).expect("wal").len(), first_wal_size, "same deadline bucket must not rewrite tasks");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
                derived: false,
            })
            .expect("invalidate in next bucket");
        // Two, not three: Dedup and the rollup. HotPacking is planned by DEBT
        // in `plan_compaction_debt` (one day-wide unit per project, only when
        // the partition really has small or unsorted files), never per slice —
        // minting it here made file hygiene 22% of the journal with a count
        // that tracked ingest rather than fragmentation.
        assert_eq!(journal.tasks().count(), 2);
        assert!(
            journal.tasks().all(|task| task.key.operation != Operation::HotPacking),
            "ingest must not mint file-hygiene work per slice; the debt planner owns it"
        );
        assert!(journal.tasks().all(|task| task.deadline_micros == FINALIZATION_DELAY_MICROS + 2 * INVALIDATION_DEADLINE_BUCKET_MICROS));
    }

    #[test]
    fn running_tasks_are_requeued_after_a_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.mark_running(&key));
        assert_eq!(journal.requeue_running(42), 1);
        let task = journal.tasks().next().expect("task");
        assert_eq!(task.state, TaskState::Retry);
        assert_eq!(task.deadline_micros, 42);
    }

    #[test]
    fn claim_is_durable_state_not_an_in_memory_queue_pop() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::Dedup));
        let claimed = journal.claim_next(Operation::Dedup, 0, true).expect("claim");
        assert_eq!(claimed.state, TaskState::Running);
        assert_eq!(claimed.attempts, 1);
        assert!(journal.claim_next(Operation::Dedup, 0, true).is_none());
    }

    /// Historical derived work must not lose every claim to the frontier.
    ///
    /// Prod 2026-08-19 00:10 UTC: 387 historical day-wide derived units had just
    /// been unblocked (#186/#188), the 1h tier they build was 9-17 days deep
    /// against a 33-day base tier — and EVERY derived unit claimed in the next
    /// 12 minutes was a one-hour frontier slice for today or yesterday. Class is
    /// strict priority and the frontier regenerates continuously, so without a
    /// reservation the historical units never run at all.
    #[test]
    fn a_sealed_derived_unit_is_not_starved_by_the_frontier() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 40 * 24 * 3_600_000_000i64;
        let derived = |project: &str, start: i64, width: i64| {
            let mut task = task(project, start, start + width, Operation::DerivedRollup);
            task.base_tier_present = true;
            task.deadline_micros = 0;
            task
        };
        // A one-hour frontier slice, and a day-wide sealed one ten days back.
        journal.upsert(derived("frontier", now - 3_600_000_000, 3_600_000_000));
        let sealed_start = now - 10 * 24 * 3_600_000_000;
        journal.upsert(derived("sealed", sealed_start, 24 * 3_600_000_000));

        // The frontier is behind, which is exactly when the general reservation
        // shrinks — and exactly when coverage needs the sealed unit most.
        journal.frontier_lag_secs.store(FRONTIER_LAG_BUDGET_SECS + 1, std::sync::atomic::Ordering::Relaxed);
        let claimed = journal.claim_next(Operation::DerivedRollup, now, true).expect("a derived unit is claimable");
        assert_eq!(claimed.key.project_id, "sealed", "historical derived work must win the claim, not today's");
    }

    /// A derived unit whose base TIER already exists must be claimable, even
    /// when no `BaseRollup` journal task records that it was built.
    ///
    /// Prod 2026-08-18 22:30 UTC: the 1m base tier was 33 days deep on most
    /// projects while the 1h derived tier it feeds sat at 9-17,
    /// `pending_derived_rollup` did not move by ONE task across two 240s windows
    /// with workers free, and every derived unit claimed in 20 minutes was a
    /// frontier slice whose base had completed minutes earlier. Historical days
    /// were unclaimable — `claim_next` skips a dependency-blocked task silently,
    /// with no counter and no log, which is the sixth silent refusal in this
    /// family.
    #[test]
    fn a_derived_unit_runs_when_its_base_tier_exists_without_a_journal_record() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let derived = |project: &str| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };

        // The status quo, and correct on its own terms: no completed base task
        // covers this slice, so the unit is refused.
        journal.enqueue(derived("historical"), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "without evidence the dependency gate still holds");

        // The backfill planner reads real tier coverage, so it can prove the
        // base tier is there. That must be enough.
        journal.enqueue_with_base_tier(derived("historical"), 0, 1, 0, true);
        assert_eq!(journal.claim_next(Operation::DerivedRollup, 0, true).expect("proven base tier makes the unit claimable").key.project_id, "historical");
    }

    /// The planner must be able to prove the base tier for a task it can no
    /// longer reach through `enqueue`.
    ///
    /// A blocked derived unit stays queued, which makes its day permanently
    /// ineligible for backfill admission (`want.retain(|key| !queued...)`),
    /// which is the only path that could have carried the proof. Prod
    /// 2026-08-18 22:50 UTC: `pending_derived_rollup` did not move after #184
    /// shipped, because all 759 tasks predated it.
    #[test]
    fn an_already_queued_derived_unit_can_still_be_told_its_base_tier_exists() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: blocked by the dependency gate");

        let day = (0, 24 * 3_600_000_000i64);
        assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 1, "the proof lands on an existing task");
        assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 0, "and is idempotent");
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "the unit becomes claimable without being re-enqueued");
    }

    /// The claimability census must name each reason `claim_next` skips a task.
    ///
    /// Every counter in this system reports how much work EXISTS; none reported
    /// why an existing task is passed over, and `claim_next` decides that inside
    /// filter predicates that leave no trace. Five correct fixes shipped against
    /// wrong models of the queue before this existed.
    #[test]
    fn the_claimability_census_separates_the_reasons_a_task_is_skipped() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        let now = 40 * 24 * HOUR;
        let at = |project: &str, start: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(start, start + HOUR).expect("slice"),
            operation: Operation::DerivedRollup,
        };

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

    /// Sealed work that has waited too long must overtake newer sealed work —
    /// and only then.
    ///
    /// Prod 2026-08-19, from the compaction dashboard: 2026-08-13 sat at 167
    /// files for FOUR DAYS, the same count and the same four tenants, while six
    /// younger sealed days converged past it. Six successors overtaking a
    /// partition is not a queue draining slowly; it is one the order never
    /// reaches.
    ///
    /// Both halves matter. Oldest-first was tried and is recorded in
    /// `scheduling_class`'s own comment as sending 10 of 10 historical starts to
    /// data months old while the last 30 days went untouched — so fresh work
    /// must STILL be newest-first, and only genuinely starved tasks escalate.
    #[test]
    fn sealed_work_ages_out_of_starvation_without_becoming_oldest_first() {
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let sealed = |day: i64, created_days_ago: i64| {
            let mut t = task("p", day * DAY, day * DAY + DAY, Operation::SealedConsolidation);
            t.created_unix_ms = u64::try_from((now - created_days_ago * DAY).div_euclid(1_000)).unwrap_or_default();
            t
        };

        // Both minted just now: newest day wins, exactly as before.
        let fresh_new = super::scheduling_class(&sealed(35, 0), now);
        let fresh_old = super::scheduling_class(&sealed(20, 0), now);
        assert!(fresh_new < fresh_old, "among fresh tasks the newest sealed day still leads");

        // The old day's task has now waited four days: it overtakes.
        let starved_old = super::scheduling_class(&sealed(20, 4), now);
        assert!(starved_old < fresh_new, "a task starved past the threshold overtakes newer sealed work");

        // And starvation never lets sealed work outrank the live frontier.
        let frontier = super::scheduling_class(&task("p", now - 600_000_000, now, Operation::BaseRollup), now);
        assert!(frontier < starved_old, "class still leads: the frontier outranks even starved sealed work");
    }

    /// A hole must be claimed before a day that already has tier output.
    ///
    /// Sealed rollup work is otherwise strictly newest-first, and recent days
    /// are re-invalidated continuously by ongoing publication, so the claim
    /// never walks back far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f`'s 1h tier jumped 2026-07-31 -> 08-14 for a second day running
    /// while day-wide derived units for 08-17 were claimed over and over.
    /// Newest-first is right for FRESHNESS and wrong for CONTIGUITY, and 30
    /// contiguous days is a contiguity goal.
    #[test]
    fn a_hole_outranks_a_day_that_already_has_tier_output() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let at = |project: &str, day: i64| {
            let mut t = task(project, day * DAY, day * DAY + DAY, Operation::DerivedRollup);
            t.key.physical_table = "rollup_1h".to_owned();
            t.deadline_micros = 0;
            t
        };
        // Both are SEALED (day 38+ would fall inside the live-frontier window
        // and never reach the sealed turn). Day 35 is the newer of the two, so
        // newest-first takes it; day 20 is the old hole.
        journal.upsert(at("recent", 35));
        journal.upsert(at("oldhole", 20));
        journal.set_base_tier_ready(HashSet::from([
            ("source".to_owned(), "recent".to_owned(), "1970-02-05".to_owned()),
            ("source".to_owned(), "oldhole".to_owned(), "1970-01-21".to_owned()),
        ]));

        // With no hole information, newest-first wins — the status quo.
        assert_eq!(journal.claim_next(Operation::DerivedRollup, now, true).expect("claim").key.project_id, "recent");

        // Told which cell is a hole, the hole goes first instead.
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(at("recent", 35));
        journal.upsert(at("oldhole", 20));
        journal.set_base_tier_ready(HashSet::from([
            ("source".to_owned(), "recent".to_owned(), "1970-02-05".to_owned()),
            ("source".to_owned(), "oldhole".to_owned(), "1970-01-21".to_owned()),
        ]));
        journal.set_tier_holes(HashSet::from([("source".to_owned(), "oldhole".to_owned(), "rollup_1h".to_owned(), "1970-01-21".to_owned())]));
        assert_eq!(
            journal.claim_next(Operation::DerivedRollup, now, true).expect("claim").key.project_id,
            "oldhole",
            "a missing day must outrank a newer day that already has output"
        );
    }

    /// The day-keyed ready set must unblock a derived task of ANY width.
    ///
    /// Three attempts to carry this fact as a per-task flag were inert (#184,
    /// #186, #195) because the flag had to land on exactly the right `TaskKey`
    /// and the queued work is not the width the planner assumes. Prod
    /// 2026-08-19 06:30: `derived_unproven=674` of `derived_pending=674` — the
    /// flag had never been set on ONE pending task. A day is what the fact is
    /// about, so keying on it cannot miss a task.
    #[test]
    fn the_base_tier_ready_set_unblocks_derived_work_of_any_width() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        let at = |start: i64, width: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(start, start + width).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        // An hour-wide task and a day-wide one, both inside 1970-01-01.
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

    /// The proof must reach the tasks that actually exist, at their own width.
    ///
    /// Prod's shape, not a hypothetical: `invalidate` mints derived units one
    /// HOUR wide, and `coarsen_sealed_slices` will not fuse a day whose day-wide
    /// unit already exists in any state — including `Complete`, which is what a
    /// legacy rows=0 publication leaves behind. So the day carries hour-wide
    /// pending tasks *and* a completed day-wide one, and a proof aimed at the
    /// day-wide key lands on the completed task, which is never claimed.
    ///
    /// Measured 2026-08-19 03:00 UTC by #194's census:
    /// `cells_missing=264 cells_wanted=0 defer_enqueue=false` — every hole seen,
    /// every one vetoed as already-queued, and the proof had never fired once.
    #[test]
    fn the_proof_reaches_hour_wide_tasks_under_a_completed_day_unit() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let at = |start: i64, width: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(start, start + width).expect("slice"),
            operation: Operation::DerivedRollup,
        };

        // The legacy rows=0 publication: a COMPLETE day-wide unit.
        let day_unit = at(0, DAY);
        journal.enqueue(day_unit.clone(), 0, 1, 0);
        journal.complete(&day_unit);
        // And the hour-wide work that is actually pending underneath it.
        journal.enqueue(at(0, HOUR), 0, 1, 0);
        journal.enqueue(at(HOUR, HOUR), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: the hour units are dependency-blocked");

        // Proving the DAY must reach the hour units, not just the completed key.
        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY), 2, "both pending hour units are proven");
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "an hour unit becomes claimable");

        // Scoped to the day: a task in the NEXT day must not be swept up.
        journal.enqueue(at(DAY, HOUR), 0, 1, 0);
        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY), 0, "the following day is a different fact and stays unproven");
    }

    /// The proof latches. The frontier re-enqueues the same key without it, and
    /// silence is not evidence that coverage stopped existing.
    #[test]
    fn a_proven_base_tier_is_not_forgotten_by_a_later_enqueue() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        journal.enqueue_with_base_tier(key.clone(), 0, 1, 0, true);
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "a later uninformed enqueue must not clear the proof");
    }

    /// Attempts alone must not quarantine. Only the worker's own verdict does.
    ///
    /// A task is claimed and its attempt counted before anything about its cost
    /// is known, so a unit handed back for a reason unrelated to cost —
    /// `source_not_flushed`, `resolve_input`, an unsatisfiable dependency —
    /// accumulates attempts identically to one that burned a 900s deadline.
    ///
    /// Prod 2026-08-19: of 162 SEALED derived units, the historical backfill the
    /// 30d goal needs, **109 were quarantined** and held to 2 of 16 workers.
    /// They had failed while their base-tier dependency was unprovable, which
    /// #197/#202 then fixed — stale evidence about a condition that no longer
    /// existed.
    #[test]
    fn only_a_worker_verdict_quarantines_a_unit() {
        let mut over = task("p", 0, MIN_SLICE_MICROS, Operation::DerivedRollup);
        over.attempts = TaskJournal::QUARANTINE_ATTEMPTS + 3;

        // Handed back for a reason that says nothing about cost.
        over.retry_reason = Some("source_not_flushed".to_owned());
        assert!(!TaskJournal::is_quarantined(&over), "a dependency-shaped failure is not evidence about cost");
        over.retry_reason = None;
        assert!(!TaskJournal::is_quarantined(&over), "no recorded reason is not evidence either");

        // The worker's own verdict, which IS about cost.
        over.retry_reason = Some(TaskJournal::WORKER_FAILURE_REASON.to_owned());
        assert!(TaskJournal::is_quarantined(&over), "a unit the worker gave back, repeatedly, is quarantined");

        // One such failure is still a blip, not proof.
        let mut once = over.clone();
        once.attempts = 1;
        assert!(!TaskJournal::is_quarantined(&once), "a single worker failure is a blip");
    }

    /// A unit that has proven it cannot fit its deadline must not be able to
    /// crowd out work that can.
    ///
    /// Prod 2026-08-18 21:00 UTC, 60 minutes of logs: 47 units timed out at
    /// 900s each — 42,300 of 57,600 available worker-seconds, 73% of ALL
    /// maintenance capacity, committing nothing — while ~40,000 pending
    /// BaseRollup units could not get a slot and `tasks_complete` advanced by 2
    /// per 180s with `tasks_running` pinned at 16. The one rollup that did
    /// complete took 812ms.
    ///
    /// Neither the backoff nor the bisection in `abandon_running` bounds this:
    /// the backoff sets how OFTEN a doomed unit runs, not what it costs, and
    /// halving a slice cannot halve a per-file cost (~3.2s per parquet file,
    /// measured the same day) — it doubles the number of units paying it.
    #[test]
    fn a_unit_that_cannot_fit_its_deadline_does_not_crowd_out_one_that_can() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Two units, identical but for their history. The doomed one sorts
        // FIRST on every tiebreak in `claim_next` (project id, then key), so a
        // scheduler blind to attempts is guaranteed to pick it.
        let doomed = task("a_doomed", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = doomed.key.clone();
        journal.upsert(doomed);
        journal.upsert(task("b_fresh", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        // Two runs that ended in a timeout. `mark_running` is the same call
        // `claim_next` uses to count an attempt, driven directly here so the
        // setup does not also rotate the project-fairness cursor.
        for _ in 0..TaskJournal::QUARANTINE_ATTEMPTS {
            assert!(journal.mark_running(&key));
            journal.retry(&key, TaskJournal::WORKER_FAILURE_REASON.to_owned(), 0);
        }

        // Without a quarantine slot the worker must skip it and run the unit
        // that can still finish.
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, 0, false).expect("fresh work is claimable").key.project_id,
            "b_fresh",
            "a proven-unfittable unit must not be claimed while ordinary work waits"
        );

        // Deprioritised, never abandoned: with a slot it still takes its turn,
        // or a partition whose rollup is genuinely expensive — the one the 30d
        // goal needs most — would never gain coverage at all.
        assert_eq!(journal.claim_next(Operation::BaseRollup, 0, true).expect("quarantined work still runs").key, key);
    }

    /// The stale-estimate migration must run once, and must free fusion.
    ///
    /// Every stored estimate predating `slice_share_of_file` measured whole
    /// files, so a ten-minute child of a split day carried the whole day's
    /// number. `coarsen_to_width` sums its members, so 144 of them summed 144
    /// whole days and could never fit. Prod 2026-08-19, right after the estimate
    /// fix landed: `blocked=24 over_budget=266506` — the superseded trap gone
    /// and every candidate refused on a number written before the fix existed.
    #[test]
    fn clearing_stale_estimates_runs_once_and_lets_a_split_day_fuse_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        // 144 children each carrying the WHOLE day's estimate, as a split under
        // the old ruler produced.
        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, MAX_DECODED_BYTES, 0);
        }
        assert_eq!(journal.coarsen_sealed_slices(now), 0, "summed stale estimates must refuse to fuse — that is the bug");

        let cleared = journal.clear_stale_estimates().expect("first run migrates");
        assert_eq!(cleared, 144, "every pending unit's estimate is suspect");
        assert!(journal.clear_stale_estimates().is_none(), "the migration must not run twice");

        assert!(journal.coarsen_sealed_slices(now) > 0, "with the stale numbers gone the day must fuse");
        let widths = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry))
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(widths, vec![DAY_MICROS], "one day-wide unit should remain, got {widths:?}");
    }

    /// A collapse must SURVIVE a reload.
    ///
    /// `checkpoint` appends dirty tasks to a WAL, and `JournalRecord::Task` can
    /// only upsert — there is no record meaning "this task is gone". So a pass
    /// that removes tasks and then checkpoints persists nothing, and every
    /// removed task returns when the journal is next loaded.
    ///
    /// Prod 2026-08-19 ran the whole loop invisibly: coarsening took
    /// `pending_base_rollup` from 88,618 to 2,294, the next deploy restored 81k,
    /// and the on-disk journal was still byte-identical at 84,734,124 bytes with
    /// all 173,901 tasks. On a process that restarts several times a day, an
    /// in-memory-only collapse never happened at all.
    #[test]
    fn a_collapsed_queue_stays_collapsed_across_a_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        let now = 10 * DAY_MICROS;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
                let start = day + slot * NORMAL_SLICE_MICROS;
                journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
            }
            journal.checkpoint().expect("persist the fine slices");
            assert!(journal.coarsen_sealed_slices(now) > 0, "the day must collapse");
            journal.compact().expect("a pass that REMOVES must rewrite the snapshot");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let live = reloaded.tasks().filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry)).count();
        assert_eq!(live, 1, "the collapse must survive the reload; got {live} units back");
    }

    /// A day-wide unit must SUBSUME the ten-minute units inside it.
    ///
    /// This is the shape prod was actually in on 2026-08-19, and the reason the
    /// fuse pass alone converged to nothing. `coarsen_to_width` refuses any
    /// bucket already covered by a pending unit at least as wide — correctly,
    /// because fusing there would duplicate claimed work — but a pending
    /// day-wide unit is exactly the condition under which the narrow units are
    /// redundant. So fusion collapsed the cells that had no day unit, logged
    /// 12, 6, 3, and went quiet, while `pending_base_rollup` sat at 88,100
    /// against a backfill census of `cells_missing=260 cells_wanted=0` — 260
    /// real (project, date) cells, ~339 queued units each.
    #[test]
    fn a_pending_day_unit_subsumes_the_ten_minute_units_inside_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
        }
        journal.enqueue(task("p", day, day + DAY_MICROS, Operation::BaseRollup).key, 0, 16, 0);
        assert_eq!(journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count(), 145, "144 slices plus the day unit");

        journal.coarsen_sealed_slices(now);

        let live = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry))
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(live, vec![DAY_MICROS], "the day unit must absorb all 144 slices, leaving one unit for the cell; got {live:?}");
    }

    /// ...and a SUPERSEDED day unit subsumes nothing either.
    ///
    /// `split_time_task` supersedes a unit too big to finish and replaces it
    /// with children tiling its range. If a superseded parent could subsume,
    /// it would delete the children the split just created and leave the cell
    /// with no queued work at all — strictly worse than the redundancy this
    /// pass exists to remove.
    #[test]
    fn a_superseded_day_unit_does_not_subsume_the_children_that_replaced_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        let parent = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1));
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "the parent must be superseded for this to prove anything");
        let children = journal.tasks().filter(|t| t.state == TaskState::Pending && t.key.operation == Operation::BaseRollup).count();
        assert!(children > 0, "the split must have produced children");

        journal.coarsen_sealed_slices(now);

        assert!(
            journal.tasks().any(|t| matches!(t.state, TaskState::Pending | TaskState::Retry) && t.key.operation == Operation::BaseRollup),
            "the split's children must survive; a superseded parent is not queued work"
        );
    }

    /// ...but a COMPLETE day unit subsumes nothing.
    ///
    /// Built once is not the same as queued. A narrower unit inside a completed
    /// day is a later invalidation — the day is dirty again, which is why the
    /// slice exists — and dropping it would lose that rebuild silently.
    #[test]
    fn a_complete_day_unit_does_not_subsume_a_later_invalidation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        let parent = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 16, 0);
        journal.complete(&parent);
        let start = day + 5 * NORMAL_SLICE_MICROS;
        journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);

        journal.coarsen_sealed_slices(now);

        assert!(
            journal.tasks().any(|t| t.state == TaskState::Pending && t.key.operation == Operation::BaseRollup),
            "a later invalidation inside a COMPLETE day must survive"
        );
    }

    /// A sealed day too big to scan as one unit lands at a narrower width,
    /// not back at ten minutes.
    ///
    /// This is the state prod was in on 2026-08-19: `pending_base_rollup =
    /// 84,834`, with `maintenance_sealed_slices_coarsened` logging 12, 6, 3 and
    /// then nothing for the rest of the process's life. Everything that fit a
    /// day had already been fused; every remaining day was over
    /// `MAX_DECODED_BYTES` and so kept all 144 of its ten-minute slices.
    ///
    /// That fallback is not merely slower, it is inverted. On an uncompacted
    /// sealed partition every file spans the whole day, so timestamp-stat
    /// pruning skips nothing and a ten-minute slice reads exactly the files a
    /// day unit would — measured at `scan_ms=481682` to publish 142 rows. The
    /// day-or-nothing rule therefore answered "too expensive to scan once" with
    /// "scan it 144 times".
    #[test]
    fn a_day_over_the_decode_budget_lands_at_a_narrower_width_not_at_ten_minutes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // One sealed day of ten-minute slices whose summed estimate busts the
        // day budget but leaves each six-hour quarter comfortably inside it.
        let per_slice = MAX_DECODED_BYTES / 96;
        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = 3 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, per_slice, 0);
        }
        let minted = journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count();
        assert_eq!(minted, 144, "the live path mints one unit per ten-minute slice");

        journal.coarsen_sealed_slices(now);

        let widths = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && t.state == TaskState::Pending)
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(widths.len(), 4, "a day over budget must fuse into its four six-hour quarters, got {widths:?}");
        assert!(widths.iter().all(|width| *width == 6 * 60 * 60 * 1_000_000), "expected six-hour units, got {widths:?}");
    }

    /// A sealed day's ten-minute leftovers collapse into one day unit — but
    /// never one that would undo a split.
    ///
    /// The live path mints a unit per ten-minute slice, which is right while
    /// the day is the frontier and pure overhead once it seals: ~144 units
    /// where one would do, each paying the same fixed object-store cost. Prod
    /// 2026-08-17 sat at 18,040 pending, refilled every midnight at about the
    /// rate it drained. At ten times the projects that diverges.
    ///
    /// The guard matters as much as the collapse: `split_time_task` leaves a
    /// too-big parent `Superseded`, so coarsening its children back into a day
    /// would split, coarsen, split, forever.
    #[test]
    fn a_sealed_days_fine_slices_collapse_but_never_undo_a_split() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 3: ten-minute leftovers, nothing coarse above them.
        for slot in 0..6 {
            let start = 3 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }
        // Day 5: same, but its day unit was already split into these children.
        for slot in 0..6 {
            let start = 5 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }
        let parent = task("p", 5 * DAY_MICROS, 6 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1));
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "the parent must be superseded for the guard to be exercised");

        let collapsed = journal.coarsen_sealed_slices(now);
        assert!(collapsed >= 6, "day 3's leftovers must collapse, got {collapsed}");

        let widths = |day: i64| {
            journal
                .tasks()
                .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= day * DAY_MICROS && t.key.slice.start_micros < (day + 1) * DAY_MICROS)
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths(3), vec![DAY_MICROS], "day 3 is now exactly one day-sized unit");
        assert!(
            widths(5).iter().all(|w| *w < DAY_MICROS),
            "day 5 already had a day unit that was SPLIT; recreating it would loop forever, got {:?}",
            widths(5)
        );
    }

    /// Among sealed work, a day-sized unit outranks yesterday's ten-minute
    /// leftovers even though those are newer.
    ///
    /// Day-sized units come from the backfill planner and are the only kind
    /// that advances the rollup horizon. Ten-minute units are what the live
    /// path mints, and a day that has just sealed carries ~144 of them per
    /// project per tier. Ordering sealed work newest-first alone therefore
    /// grinds all of yesterday before reaching the day before — and midnight
    /// mints a fresh day's worth, so the horizon can never move.
    ///
    /// Prod 2026-08-17, 65 rollup starts in 25 minutes: 46 on today, 18 on
    /// yesterday, ZERO on any older day, while 7d/14d queries were refused for
    /// want of exactly those older days.
    /// The sealed reservation is affordable only while the frontier keeps up.
    ///
    /// It exists because strict class-0 priority let the frontier take ~90% of
    /// claims and froze rollup coverage. But frontier lag is a per-query cost —
    /// `raw_tail_duration_secs` is `FINALIZATION_DELAY + lag` and every hybrid
    /// query scans that tail — and prod 2026-08-17 reached 62 minutes of it.
    /// So the reservation yields while the frontier is behind, and returns on
    /// its own once it is not.
    /// Coarsening must not build a day unit that cannot finish. One day unit doing
    /// one full-day scan beats 144 slices each doing the same scan — but only if
    /// it completes. A day over the decode budget publishes nothing and times out
    /// repeatedly, which is strictly worse than the slices it replaced.
    ///
    /// Measured after #178: BaseRollup timed out at 900s for the first time (4 in
    /// a 10-minute window) and rollup output collapsed from ~9,000 rows/min to 10.
    #[test]
    fn coarsening_skips_a_day_that_would_not_fit_the_decode_budget() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 1: six slices whose combined estimate fits comfortably. Must fuse.
        let small = MAX_DECODED_BYTES / 12;
        for slot in 0..6 {
            let start = DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, small, 0);
        }
        // Day 4: six slices that together blow the budget. Must stay as slices.
        let big = MAX_DECODED_BYTES / 2;
        for slot in 0..6 {
            let start = 4 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("q", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, big, 0);
        }

        journal.coarsen_sealed_slices(now);

        let widths = |project: &str, day: i64| {
            journal
                .tasks()
                .filter(|t| {
                    t.state == TaskState::Pending
                        && t.key.project_id == project
                        && t.key.slice.start_micros >= day * DAY_MICROS
                        && t.key.slice.start_micros < (day + 1) * DAY_MICROS
                })
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths("p", 1), vec![DAY_MICROS], "a day that fits must fuse into one unit");
        assert_eq!(widths("q", 4).len(), 6, "a day over budget must keep its slices — they finish, a too-big day unit never does");
        assert!(widths("q", 4).iter().all(|w| *w < DAY_MICROS));
    }

    /// A COMPLETE day unit must not block coarsening, or the queue fills with
    /// sub-day slices that can never collapse.
    ///
    /// Once a day was rolled up once, its day task stayed in the journal as
    /// Complete forever, so every later invalidation minted a ten-minute slice
    /// that was skipped here and processed alone. A slice is not cheaper than the
    /// day: an uncompacted sealed partition's files each span the WHOLE day, so a
    /// ten-minute slice reads the same files a day unit would. Prod 2026-08-18,
    /// via the phase timing from #174:
    ///
    ///     scan_ms=481682  stage_ms=30  commit_ms=961  rows=142
    ///
    /// Eight minutes of scanning for 142 rows, 144 times a day where once would
    /// do, while `maintenance_sealed_slices_coarsened` logged ZERO against 37,065
    /// pending base rollups.
    #[test]
    fn a_completed_day_unit_does_not_block_coarsening_but_a_superseded_one_does() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 2: a day unit that already ran to completion, plus fresh slices
        // minted by a later invalidation. These MUST collapse.
        let done = task("p", 2 * DAY_MICROS, 3 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(done.clone(), 0, 0, 0);
        journal.complete(&done);
        assert_eq!(journal.state(&done), Some(TaskState::Complete), "precondition");
        for slot in 0..6 {
            let start = 2 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }

        // Day 6: a day unit SPLIT because it was too big. Its children must NOT
        // collapse, or the split is undone and the two fight forever.
        let parent = task("p", 6 * DAY_MICROS, 7 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1));
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "precondition");

        journal.coarsen_sealed_slices(now);

        let widths = |day: i64| {
            journal
                .tasks()
                .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= day * DAY_MICROS && t.key.slice.start_micros < (day + 1) * DAY_MICROS)
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths(2), vec![DAY_MICROS], "a completed day must re-coarsen to one day unit, not stay as slices");
        assert!(widths(6).iter().all(|w| *w < DAY_MICROS), "a SPLIT day's children must stay split — recoarsening them loops forever");
    }

    #[test]
    fn the_sealed_reservation_yields_while_the_frontier_is_behind() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let now = 10 * DAY_MICROS;
        let claims = |lag: u64| {
            let dir = tempfile::tempdir().expect("temp dir");
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            // One frontier slice (ends at `now`) and one sealed day, both eligible.
            let frontier = task("p", now - 600_000_000, now, Operation::BaseRollup).key.clone();
            let sealed = task("p", now - 5 * DAY_MICROS, now - 4 * DAY_MICROS, Operation::BaseRollup).key.clone();
            journal.enqueue(frontier, 0, 0, 0);
            journal.enqueue(sealed, 0, 0, 0);
            journal.frontier_lag_secs.store(lag, std::sync::atomic::Ordering::Relaxed);
            let mut sealed_claims = 0;
            for _ in 0..8 {
                let Some(claimed) = journal.claim_next(Operation::BaseRollup, now, true) else { continue };
                if !is_live_frontier(claimed.key.slice, now) {
                    sealed_claims += 1;
                }
                journal.complete(&claimed.key);
                // Re-open both so every tick has a choice to make.
                journal.enqueue(claimed.key.clone(), 0, 0, 0);
            }
            sealed_claims
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

    #[test]
    fn sealed_backfill_units_outrank_yesterdays_fine_slices() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // "Now" well past both, so everything is sealed rather than frontier.
        let now = 10 * DAY_MICROS;

        // Yesterday's fine-grained leftovers: newer, and far more numerous.
        for slot in 0..12 {
            let start = 8 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            let fine = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup);
            journal.enqueue(fine.key.clone(), 0, 0, 0);
        }
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

    /// A unit that cannot finish inside its deadline must get SMALLER, not be
    /// requeued unchanged.
    ///
    /// The lease requeues whatever the worker abandoned, so a slice too big for
    /// its deadline times out, requeues identical, times out again — forever,
    /// holding a worker for the full deadline each time and never producing
    /// anything. Prod 2026-08-17: five Dedup timeouts in twelve minutes at 300s
    /// apiece, permanently occupying ~2 of 16 coordinator workers, while total
    /// task starts fell to 2.2/min and the rollup horizon it was starving sat
    /// days behind. Byte-based splitting cannot catch this — a day-sized slice
    /// with modest bytes still pays one object-store round trip per file.
    /// Verbatim prod text, 2026-08-17. Classification is string-based because
    /// the DataFusion error arrives type-erased through delta-rs/anyhow, so the
    /// strings are the contract and this test is what pins them.
    #[test]
    fn capacity_failures_are_recognised_from_prod_text() {
        assert!(is_capacity_failure(
            "dedup: Not enough memory to continue external sort. Consider increasing the memory limit config: \
             'datafusion.runtime.memory_limit', or decreasing the config: 'datafusion.execution.sort_spill_reservation_bytes'."
        ));
        assert!(is_capacity_failure("compaction: Resources exhausted: Additional allocation failed for ExternalSorter[1] with top memory consumers"));
        for benign in ["dedup: Object at location ... not found", "compaction: transaction failed: version 2667 already exists", "source_not_flushed"] {
            assert!(!is_capacity_failure(benign), "must not shrink a slice over a fault that has nothing to do with size: {benign}");
        }
    }

    /// A slice too big for the pool fails identically every pass. Back off once
    /// (a FairSpillPool squeeze is transient), then shrink it.
    #[test]
    fn a_unit_that_cannot_fit_bisects_instead_of_retrying_at_the_same_size() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("whale", 0, DAY_MICROS, Operation::Dedup).key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);
        let oom = "dedup: Not enough memory to continue external sort.".to_owned();

        journal.retry_or_split(&key, oom.clone(), 1, 1);
        assert_eq!(journal.state(&key), Some(TaskState::Retry), "one squeeze may be someone else's fault; retry it whole");

        journal.retry_or_split(&key, oom, 2, 2);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "a repeat says the slice itself does not fit");
        let widths: Vec<i64> = journal.tasks().filter(|t| t.state != TaskState::Superseded).map(|t| t.key.slice.width()).collect();
        assert!(!widths.is_empty() && widths.iter().all(|w| *w < DAY_MICROS), "bisection must leave smaller claimable children; got {widths:?}");
    }

    /// Only capacity failures shrink. A missing object or a commit conflict says
    /// nothing about size, and splitting on it would shred a healthy slice.
    #[test]
    fn an_unrelated_failure_retries_whole_however_often_it_repeats() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("steady", 0, 86_400_000_000, Operation::Dedup).key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);
        for attempts in 1..6 {
            journal.retry_or_split(&key, "dedup: object not found".to_owned(), i64::from(attempts), attempts);
            assert_eq!(journal.state(&key), Some(TaskState::Retry), "attempt {attempts} must not split");
        }
    }

    /// A derived unit must read the base files that hold its rows, whatever
    /// width the unit that WROTE them happened to use.
    ///
    /// Prod 2026-08-17, exact numbers: derived units are one hour wide, while
    /// the backfill writes base units a whole day wide and tags each file with
    /// its own slice. The old test was CONTAINMENT — `file_end <= slice_end` —
    /// which a day-tagged file can never satisfy against an hour, so every
    /// backfilled day published rows=0 and was marked complete. Project
    /// 87576849 had 17,705 rows in the 1m tier for 08-03 while its 1h unit for
    /// 08-03 produced nothing, and 14d/30d queries never routed as a result.
    #[test]
    fn a_day_wide_base_file_feeds_every_hour_wide_derived_slice_inside_it() {
        const DAY: i64 = 1_785_628_800_000_000; // 2026-08-02T00:00Z
        const HOUR: i64 = 3_600_000_000;
        let day_file = (DAY, DAY + 86_400_000_000);

        for hour in 0..24 {
            let slice = TimeSlice::new(DAY + hour * HOUR, DAY + (hour + 1) * HOUR).expect("slice");
            assert!(slice.overlaps(day_file.0, day_file.1), "hour {hour} must read the day-wide base file that holds its rows");
            // The rule this replaced, stated so the regression is unmistakable.
            let contained = day_file.0 >= slice.start_micros && day_file.1 <= slice.end_micros;
            assert!(!contained, "containment is what broke: hour {hour} could never select a day-tagged file");
        }

        // Overlap must still EXCLUDE what it should: neighbouring days.
        let slice = TimeSlice::new(DAY, DAY + HOUR).expect("slice");
        assert!(!slice.overlaps(DAY - 86_400_000_000, DAY), "the previous day ends exactly at the boundary and must not be read");
        assert!(!slice.overlaps(DAY + 86_400_000_000, DAY + 2 * 86_400_000_000), "a later day must not be read");
        // Ten-minute frontier files — the only shape that ever worked — still do.
        assert!(slice.overlaps(DAY, DAY + 600_000_000));
    }

    #[test]
    fn a_unit_that_keeps_timing_out_bisects_instead_of_retrying_forever() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY_MICROS: i64 = 86_400_000_000;
        let input = task("whale", 0, DAY_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);

        // Two abandoned runs: the first is an ordinary blip and must simply
        // retry, the second says the slice itself does not fit.
        for _ in 0..2 {
            assert!(journal.mark_running(&key));
            journal.abandon_running(&key, 0);
        }

        let widths: Vec<i64> = journal.tasks().filter(|t| t.state != TaskState::Superseded).map(|t| t.key.slice.width()).collect();
        assert!(!widths.is_empty(), "bisection must leave claimable children behind");
        assert!(widths.iter().all(|width| *width < DAY_MICROS), "a repeatedly-abandoned day must bisect; got widths {widths:?} still at the full day");
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "the parent stays as an audit record");
    }

    #[test]
    fn replanning_live_debt_reopens_one_idempotent_task() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::SealedConsolidation);
        let key = input.key.clone();
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
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut base_keys = Vec::new();
        for start in (0..DERIVED_SLICE_MICROS).step_by(NORMAL_SLICE_MICROS as usize) {
            let base = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup);
            base_keys.push(base.key.clone());
            journal.upsert(base);
        }
        journal.upsert(task("p", 0, DERIVED_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none());
        for key in base_keys {
            journal.complete(&key);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some());
    }

    #[test]
    fn oversized_task_is_replaced_by_durable_time_children() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES));
        assert_eq!(journal.tasks().filter(|task| task.state == TaskState::Pending).count(), 2);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded));
    }

    #[test]
    fn live_frontier_lag_prefers_pending_split_child_over_superseded_parent() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut input = task("p", now - NORMAL_SLICE_MICROS, now, Operation::BaseRollup);
        input.deadline_micros = now - 2 * 60 * 1_000_000;
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES));
        assert_eq!(live_frontier_lag_secs(journal.tasks(), now), 2 * 60);
    }

    #[test]
    fn completed_children_satisfy_a_larger_derived_dependency() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let base = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let base_key = base.key.clone();
        journal.upsert(base);
        journal.upsert(task("p", 0, NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.split_time_task(&base_key, 2 * MAX_DECODED_BYTES));
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
