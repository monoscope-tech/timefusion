//! Durable, byte-bounded work units shared by background maintenance.
//!
//! This module deliberately contains no scan implementation.  It is the
//! correctness boundary between write/Delta reconciliation and the workers:
//! work is journaled before it can be selected, and a worker can only receive
//! a unit whose decoded-byte reservation fits the configured ceiling.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

pub const NORMAL_SLICE_MICROS: i64 = 10 * 60 * 1_000_000;
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const TAG_SOURCE: &str = "timefusion.source";
pub const TAG_PROJECT: &str = "timefusion.project";
pub const TAG_SLICE_START: &str = "timefusion.slice_start_micros";
pub const TAG_SLICE_END: &str = "timefusion.slice_end_micros";
pub const TAG_SOURCE_FINGERPRINT: &str = "timefusion.source_fingerprint";
pub const TAG_GENERATION: &str = "timefusion.generation";
const JOURNAL_VERSION: u32 = 1;
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
        let whole = Self::new(start_micros, end_micros)?;
        let mut result = Vec::new();
        let mut start = whole.start_micros;
        while start < whole.end_micros {
            let end = start.saturating_add(NORMAL_SLICE_MICROS).min(whole.end_micros);
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

/// Crash-safe task journal. `checkpoint` uses the same fsync + atomic rename
/// primitive as WAL metadata; a failed completion checkpoint therefore causes
/// redundant work, never missing work.
#[derive(Debug)]
pub struct TaskJournal {
    path: PathBuf,
    snapshot: Snapshot,
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
            journal.retry(&self.key, "worker_error".to_owned(), crate::clock::now_micros().saturating_add(1_000_000));
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
    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = crate::wal::meta_path(data_dir, "maintenance_tasks.json");
        let snapshot = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes)?,
            Err(error) if error.kind() == ErrorKind::NotFound => Snapshot { version: JOURNAL_VERSION, ..Snapshot::default() },
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(snapshot.version == JOURNAL_VERSION, "unsupported maintenance task journal version {}", snapshot.version);
        Ok(Self { path, snapshot })
    }

    pub fn upsert(&mut self, task: MaintenanceTask) {
        match self.snapshot.tasks.iter_mut().find(|current| current.key == task.key) {
            Some(current) => *current = task,
            None => self.snapshot.tasks.push(task),
        }
    }

    pub fn enqueue(&mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64) {
        if let Some(task) = self.snapshot.tasks.iter_mut().find(|task| task.key == key) {
            if task.state != TaskState::Running {
                task.state = TaskState::Pending;
                task.deadline_micros = task.deadline_micros.min(deadline_micros);
                task.estimated_decoded_bytes = estimated_decoded_bytes;
                task.retry_reason = None;
                task.publication = None;
            }
            return;
        }
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
        let deadline_micros = observed_at_micros.saturating_add(FINALIZATION_DELAY_MICROS);
        let created_unix_ms = u64::try_from(observed_at_micros.div_euclid(1_000)).unwrap_or_default();
        for slice in TimeSlice::normal_units(start_micros, end_micros)? {
            for operation in [Operation::Dedup, if derived { Operation::DerivedRollup } else { Operation::BaseRollup }, Operation::HotPacking] {
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
                if let Some(task) = self.snapshot.tasks.iter_mut().find(|task| task.key == key) {
                    task.state = TaskState::Pending;
                    task.deadline_micros = task.deadline_micros.max(deadline_micros);
                    task.retry_reason = None;
                    task.publication = None;
                } else {
                    self.snapshot.tasks.push(MaintenanceTask {
                        key,
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
                }
            }
        }
        Ok(())
    }

    pub fn mark_running(&mut self, key: &TaskKey) -> bool {
        let Some(task) = self.snapshot.tasks.iter_mut().find(|task| &task.key == key) else { return false };
        if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
            return false;
        }
        task.state = TaskState::Running;
        task.attempts = task.attempts.saturating_add(1);
        true
    }

    pub fn claim_next(&mut self, operation: Operation, now_micros: i64) -> Option<MaintenanceTask> {
        let key = fair_ready_tasks(self.snapshot.tasks.iter(), now_micros)
            .into_iter()
            .find(|task| task.key.operation == operation && self.dependencies_complete(task))
            .map(|task| task.key.clone())?;
        self.mark_running(&key);
        self.snapshot.tasks.iter().find(|task| task.key == key).cloned()
    }

    fn dependencies_complete(&self, task: &MaintenanceTask) -> bool {
        let required = match task.key.operation {
            Operation::BaseRollup => Some(Operation::Dedup),
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
        let Some(task) = self.snapshot.tasks.iter_mut().find(|task| &task.key == key) else { return false };
        task.state = TaskState::Retry;
        crate::metrics::set_maintenance_retry_reason(&reason);
        task.retry_reason = Some(reason);
        task.deadline_micros = not_before_micros;
        true
    }

    pub fn complete(&mut self, key: &TaskKey) -> bool {
        let Some(task) = self.snapshot.tasks.iter_mut().find(|task| &task.key == key) else { return false };
        task.state = TaskState::Complete;
        task.retry_reason = None;
        true
    }

    pub fn publish(&mut self, key: &TaskKey, publication: Publication) -> bool {
        let Some(task) = self.snapshot.tasks.iter_mut().find(|task| &task.key == key) else { return false };
        task.state = TaskState::Complete;
        task.retry_reason = None;
        task.publication = Some(publication);
        true
    }

    /// Supersede an oversized time unit with smaller durable children. The
    /// parent remains as a completed audit record. Hash shards intentionally
    /// stay inside a one-minute task because `TaskKey` identifies logical
    /// slice work; the worker merges all shard states before publication.
    pub fn split_time_task(&mut self, key: &TaskKey, observed_bytes: u64) -> bool {
        let Some(parent) = self.snapshot.tasks.iter().find(|task| &task.key == key).cloned() else { return false };
        let children = byte_bounded_units(&parent, observed_bytes);
        if children.len() <= 1 || children.iter().any(|child| child.hash_shards > 1) {
            return false;
        }
        if let Some(task) = self.snapshot.tasks.iter_mut().find(|task| &task.key == key) {
            task.state = TaskState::Superseded;
            task.retry_reason = Some("split_into_smaller_slices".to_owned());
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
                count += 1;
            }
        }
        count
    }

    pub fn tasks(&self) -> impl Iterator<Item = &MaintenanceTask> {
        self.snapshot.tasks.iter()
    }

    pub fn state(&self, key: &TaskKey) -> Option<TaskState> {
        self.snapshot.tasks.iter().find(|task| &task.key == key).map(|task| task.state)
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
        self.snapshot.source_cursors.entry(source).and_modify(|cursor| *cursor = (*cursor).max(delta_version)).or_insert(delta_version);
    }

    pub fn checkpoint(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec(&self.snapshot)?;
        crate::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
        self.publish_statistics();
        Ok(())
    }

    pub fn publish_statistics(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::metrics::maintenance_stats();
        let mut counts = [0u64; 4];
        let mut backlog_bytes = 0u64;
        let mut sealed_debt_bytes = 0u64;
        let mut oldest_created = u64::MAX;
        let now_micros = crate::clock::now_micros();
        let mut oldest_eligible_deadline = i64::MAX;
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
                if task.deadline_micros <= now_micros {
                    oldest_eligible_deadline = oldest_eligible_deadline.min(task.deadline_micros);
                }
                if task.key.operation == Operation::SealedConsolidation {
                    sealed_debt_bytes = sealed_debt_bytes.saturating_add(task.estimated_decoded_bytes);
                }
            }
        }
        stats.maintenance_tasks_pending.store(counts[0], Relaxed);
        stats.maintenance_tasks_running.store(counts[1], Relaxed);
        stats.maintenance_tasks_retry.store(counts[2], Relaxed);
        stats.maintenance_tasks_complete.store(counts[3], Relaxed);
        stats.maintenance_backlog_bytes.store(backlog_bytes, Relaxed);
        stats.sealed_compaction_debt_bytes.store(sealed_debt_bytes, Relaxed);
        let eligible_lag_secs = if oldest_eligible_deadline == i64::MAX {
            0
        } else {
            u64::try_from(now_micros.saturating_sub(oldest_eligible_deadline).div_euclid(1_000_000)).unwrap_or_default()
        };
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
    let mut groups: BTreeMap<(u8, i64), HashMap<&str, VecDeque<&MaintenanceTask>>> = BTreeMap::new();
    for task in tasks {
        if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
            groups.entry((task.key.operation.priority(), task.deadline_micros)).or_default().entry(&task.key.project_id).or_default().push_back(task);
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
        drop(TaskLease::new(Arc::clone(&journal), key.clone()));

        assert_eq!(journal.lock().expect("lock").state(&key), Some(TaskState::Retry));
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

        // Rollup staging / target-commit-before-coverage checkpoint: Running is
        // a durable non-claim and is retried after restart. An already-landed
        // target commit is safely replaced by that redundant retry.
        assert!(recovered.mark_running(&key));
        recovered.checkpoint().expect("running checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after staging");
        assert_eq!(recovered.requeue_running(100), 1);
        assert_eq!(recovered.state(&key), Some(TaskState::Retry));

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
        assert_eq!(journal.tasks().count(), 3);
        assert!(journal.tasks().any(|task| task.key.operation == Operation::HotPacking));
        assert!(journal.tasks().all(|task| task.deadline_micros == 20 + FINALIZATION_DELAY_MICROS));
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
    fn rollup_claim_waits_for_its_slice_dependency() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let dedup = task("p", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let dedup_key = dedup.key.clone();
        journal.upsert(dedup);
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        assert!(journal.claim_next(Operation::BaseRollup, 0).is_none());
        journal.complete(&dedup_key);
        assert!(journal.claim_next(Operation::BaseRollup, 0).is_some());
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
