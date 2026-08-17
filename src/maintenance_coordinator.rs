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
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const DERIVED_SLICE_MICROS: i64 = 60 * 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const INVALIDATION_DEADLINE_BUCKET_MICROS: i64 = 30 * 1_000_000;
const LIVE_FRONTIER_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
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
    /// Rotates so a fixed share of claims is reserved for sealed work. Runtime
    /// only — never journalled; losing it across a restart costs nothing.
    claim_tick: u64,
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
            let attempts = journal.task_indices.get(&self.key).and_then(|index| journal.snapshot.tasks.get(*index)).map_or(1, |task| task.attempts).min(8);
            let delay_micros = i64::try_from((1u64 << attempts).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            journal.retry(&self.key, "worker_error".to_owned(), crate::clock::now_micros().saturating_add(delay_micros));
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

    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = crate::wal::meta_path(data_dir, "maintenance_tasks.json");
        let wal_path = crate::wal::meta_path(data_dir, "maintenance_tasks.wal");
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
            claim_tick: 0,
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
        if let Some(index) = self.task_indices.get(&key).copied() {
            let task = &mut self.snapshot.tasks[index];
            if task.state != TaskState::Running {
                let new_deadline = task.deadline_micros.min(deadline_micros);
                let changed = task.state != TaskState::Pending
                    || task.deadline_micros != new_deadline
                    || task.estimated_decoded_bytes != estimated_decoded_bytes
                    || task.retry_reason.is_some()
                    || task.publication.is_some();
                if changed {
                    task.state = TaskState::Pending;
                    task.deadline_micros = new_deadline;
                    task.estimated_decoded_bytes = estimated_decoded_bytes;
                    task.retry_reason = None;
                    task.publication = None;
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
        for (operation, slices) in [
            (Operation::Dedup, normal_slices.as_slice()),
            (if derived { Operation::DerivedRollup } else { Operation::BaseRollup }, rollup_slices.as_slice()),
            (Operation::HotPacking, normal_slices.as_slice()),
        ] {
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

    pub fn claim_next(&mut self, operation: Operation, now_micros: i64) -> Option<MaintenanceTask> {
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
        let sealed_turn = self.claim_tick.is_multiple_of(2);
        let best_class = |journal: &Self, sealed_only: bool| -> Option<(u8, i64)> {
            let mut class: Option<(u8, i64)> = None;
            for task in journal.snapshot.tasks.iter().filter(|task| {
                task.key.operation == operation
                    && matches!(task.state, TaskState::Pending | TaskState::Retry)
                    && task.deadline_micros <= now_micros
                    && !(sealed_only && is_live_frontier(task.key.slice, now_micros))
            }) {
                let candidate = scheduling_class(task, now_micros);
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
        for task in self.snapshot.tasks.iter().filter(|task| {
            task.key.operation == operation
                && matches!(task.state, TaskState::Pending | TaskState::Retry)
                && task.deadline_micros <= now_micros
                && scheduling_class(task, now_micros) == class
                && self.dependencies_complete(task)
        }) {
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
                        && candidate.key.slice.end_micros > task.key.slice.start_micros
                        && candidate.key.slice.start_micros < task.key.slice.end_micros
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
        crate::metrics::set_maintenance_retry_reason(&reason);
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
        crate::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
        let wal = OpenOptions::new().create(true).write(true).truncate(true).open(&self.wal_path)?;
        wal.sync_all()?;
        self.dirty_tasks.clear();
        self.dirty_cursors.clear();
        Ok(())
    }

    pub fn publish_statistics(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::metrics::maintenance_stats();
        let mut counts = [0u64; 4];
        let mut backlog_bytes = 0u64;
        let mut sealed_debt_bytes = 0u64;
        let mut oldest_created = u64::MAX;
        let mut latest_frontier_rollup: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
        let mut per_operation = [0u64; 6];
        let (mut eligible_base_rollup, mut eligible_sealed) = (0u64, 0u64);
        let now_micros = crate::clock::now_micros();
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

/// Deadline ordering with round-robin selection among projects at the same
/// operation priority. This prevents one whale from consuming an entire pass.
pub fn fair_ready_tasks<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> Vec<&'a MaintenanceTask> {
    let mut groups: BTreeMap<(u8, u8, i64), HashMap<&str, VecDeque<&MaintenanceTask>>> = BTreeMap::new();
    for task in tasks {
        if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
            let (class, order_key) = scheduling_class(task, now_micros);
            groups.entry((class, task.key.operation.priority(), order_key)).or_default().entry(&task.key.project_id).or_default().push_back(task);
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
fn scheduling_class(task: &MaintenanceTask, now_micros: i64) -> (u8, i64) {
    if is_live_frontier(task.key.slice, now_micros) {
        // Smaller tuples run first. Negating makes the newest minute the most
        // urgent while keeping all projects in that minute deadline-equivalent.
        (0, -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
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
        (1, -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
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
        let stats = crate::metrics::maintenance_stats();
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
        assert_eq!(journal.claim_next(Operation::Dedup, 0).expect("first").key.project_id, "a");
        assert_eq!(journal.claim_next(Operation::Dedup, 0).expect("second").key.project_id, "b");
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
        let now = crate::clock::now_micros();
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

        let stats = crate::metrics::maintenance_stats();
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
        let now = crate::clock::now_micros();
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
            let claimed = journal.claim_next(Operation::BaseRollup, now).expect("a task is always available");
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
        let before_drop = crate::clock::now_micros();
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
        assert_eq!(journal.tasks().count(), 3);
        assert!(journal.tasks().any(|task| task.key.operation == Operation::HotPacking));
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
        let claimed = journal.claim_next(Operation::Dedup, 0).expect("claim");
        assert_eq!(claimed.state, TaskState::Running);
        assert_eq!(claimed.attempts, 1);
        assert!(journal.claim_next(Operation::Dedup, 0).is_none());
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
        assert!(journal.claim_next(Operation::DerivedRollup, 0).is_none());
        for key in base_keys {
            journal.complete(&key);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0).is_some());
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
        assert!(journal.claim_next(Operation::DerivedRollup, 0).is_some());
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
