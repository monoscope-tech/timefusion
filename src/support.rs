//! Process-wide wall or frozen clock, plus small shared helpers and test fixtures.
//!
//! The clock can be pinned via SQL UDFs or `TIMEFUSION_FROZEN_TIME`.

use std::sync::atomic::{AtomicI64, Ordering};

/// Lock, ignoring poisoning: these mutexes guard plain data, so a panicking
/// holder leaves it usable.
pub(crate) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// An impossible epoch value marks wall-clock mode.
const WALL_SENTINEL: i64 = i64::MIN;

static FROZEN_NOW: AtomicI64 = AtomicI64::new(WALL_SENTINEL);

fn frozen_micros() -> Option<i64> {
    Some(FROZEN_NOW.load(Ordering::Acquire)).filter(|&v| v != WALL_SENTINEL)
}

pub fn init_from_env() {
    let Ok(s) = std::env::var("TIMEFUSION_FROZEN_TIME") else { return };
    set_micros(chrono::DateTime::parse_from_rfc3339(&s).unwrap_or_else(|e| panic!("TIMEFUSION_FROZEN_TIME must be RFC3339 ({s:?}): {e}")).timestamp_micros());
    tracing::warn!(frozen_at = %s, "TIMEFUSION_FROZEN_TIME set; clock is frozen (test mode)");
}

#[inline]
pub fn now_micros() -> i64 {
    frozen_micros().unwrap_or_else(|| chrono::Utc::now().timestamp_micros())
}

/// Seconds since epoch on the (possibly frozen) clock.
#[inline]
pub fn now_secs() -> u64 {
    (now_micros() / 1_000_000).max(0) as u64
}

/// Today's UTC date on the (possibly frozen) clock. Maintenance that decides
/// which partitions are sealed must use this, never `Utc::now`.
pub fn today_utc() -> chrono::NaiveDate {
    chrono::DateTime::from_timestamp_micros(now_micros()).unwrap_or_default().date_naive()
}

/// True when the clock is currently pinned (test mode).
pub fn is_frozen() -> bool {
    frozen_micros().is_some()
}

/// Install or replace the frozen time (test mode). Returns the new value.
pub fn set_micros(t: i64) -> i64 {
    FROZEN_NOW.store(t, Ordering::Release);
    t
}

/// Advance the frozen time by `delta_micros`, freezing at `wall_now + delta`
/// if it was not already frozen. Returns the new value.
pub fn advance_micros(delta_micros: i64) -> i64 {
    set_micros(now_micros().saturating_add(delta_micros))
}

/// Switch back to wall-clock mode.
pub fn unfreeze() {
    FROZEN_NOW.store(WALL_SENTINEL, Ordering::Release);
}

/// Run blocking work (`fsync`, a large synchronous serialize) without holding
/// the tokio worker its neighbouring tasks are queued on.
///
/// `block_in_place` requires the multi-thread runtime and panics elsewhere, so
/// other contexts run `f` directly — there is no shared worker queue to protect.
pub fn without_blocking_the_worker<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Coalesces concurrent durable commits into one `fsync` without weakening the
/// caller's guarantee: `commit` returns only once a commit that includes this
/// caller's mutations has completed — possibly one another caller performed.
///
/// Usage: apply your mutations, THEN call `commit`. A ticket is taken on entry,
/// so mutations applied before the call are always included; later ones may not be.
///
/// ```
/// # use timefusion::support::GroupCommit;
/// # use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
/// let group = GroupCommit::default();
/// let commits = AtomicUsize::new(0);
/// // Serial callers each get their own commit — nothing to coalesce.
/// group.commit(|| { commits.fetch_add(1, Ordering::Relaxed); Ok::<_, std::io::Error>(()) }).unwrap();
/// group.commit(|| { commits.fetch_add(1, Ordering::Relaxed); Ok::<_, std::io::Error>(()) }).unwrap();
/// assert_eq!(commits.load(Ordering::Relaxed), 2);
/// ```
#[derive(Default, Debug)]
pub struct GroupCommit {
    /// Mutations offered so far; incremented by each caller before it waits.
    applied: std::sync::atomic::AtomicU64,
    state: std::sync::Mutex<GroupCommitState>,
    settled: std::sync::Condvar,
}

#[derive(Default, Debug)]
struct GroupCommitState {
    /// Highest `applied` value a completed commit is known to cover.
    committed: u64,
    /// Whether a leader is mid-commit; followers wait rather than pile on.
    in_flight: bool,
    performed: u64,
    coalesced: u64,
}

/// The leader's hold on the commit, released in `Drop` so an unwind cannot
/// strand it. `succeeded` stays false unless the commit returned `Ok`, so
/// neither a panic nor a failure advances the durable watermark.
struct Leadership<'a> {
    group: &'a GroupCommit,
    covered: u64,
    succeeded: bool,
}

impl Drop for Leadership<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.group.state);
        state.in_flight = false;
        state.performed += 1;
        if self.succeeded {
            state.committed = state.committed.max(self.covered);
        }
        self.group.settled.notify_all();
    }
}

impl GroupCommit {
    /// Return once a commit covering this caller's already-applied mutations
    /// has completed, performing that commit if nobody else is.
    ///
    /// `do_commit` must flush *everything* outstanding, not just this caller's
    /// work — that is what lets one call satisfy many waiters. A failed or
    /// panicking commit leaves `committed` untouched so waiters retry, and
    /// hands leadership back on unwind so they are not parked forever.
    pub fn commit<E>(&self, do_commit: impl Fn() -> Result<(), E>) -> Result<(), E> {
        let ticket = self.applied.fetch_add(1, Ordering::AcqRel) + 1;
        let mut state = lock(&self.state);
        loop {
            if state.committed >= ticket {
                state.coalesced += 1;
                return Ok(());
            }
            if state.in_flight {
                state = self.settled.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            state.in_flight = true;
            // Read BEFORE the commit runs: anything applied after this point may
            // not be flushed, and must not be reported as durable.
            let covered = self.applied.load(Ordering::Acquire);
            drop(state);

            // Settles in its `Drop`, so an unwind out of `do_commit` releases
            // leadership and wakes the waiters.
            let mut leadership = Leadership { group: self, covered, succeeded: false };
            let result = do_commit();
            leadership.succeeded = result.is_ok();
            drop(leadership);
            // The leader's ticket is <= `covered`, so a successful commit always
            // covers it — return rather than loop, or it would count itself as
            // having ridden someone else's commit.
            return result;
        }
    }

    /// `(commits performed, callers that rode another caller's commit)`.
    pub fn counts(&self) -> (u64, u64) {
        let state = lock(&self.state);
        (state.performed, state.coalesced)
    }

    /// Tickets taken so far. A caller takes its ticket before it waits, so this
    /// is how many callers a commit starting now would be able to cover.
    pub fn offered(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use test_case::test_case;

    use super::*;

    /// One worker, a task blocking it, and a neighbour that must still be
    /// polled promptly.
    #[test]
    fn a_blocking_call_must_not_hold_the_worker_its_neighbours_are_queued_on() {
        const BLOCK: Duration = Duration::from_millis(400);

        // Measures how long a neighbour waits to be polled at all. A timer is a
        // weaker probe: tokio's time driver can be driven by the idle `block_on`
        // thread, so timers still fire while the worker is held. Both tasks are
        // spawned, blocker first, so it owns the worker before the neighbour queues.
        let neighbour_wait = |wrap: bool| {
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
            runtime.block_on(async move {
                let blocker = tokio::spawn(async move {
                    let block = || std::thread::sleep(BLOCK);
                    if wrap { without_blocking_the_worker(block) } else { block() }
                });
                tokio::task::yield_now().await;
                let queued_at = std::time::Instant::now();
                let neighbour = tokio::spawn(async move { queued_at.elapsed() });
                let (_, waited) = tokio::join!(blocker, neighbour);
                waited.unwrap()
            })
        };

        let blocked = neighbour_wait(false);
        let protected = neighbour_wait(true);
        assert!(blocked >= BLOCK / 2, "expected the naive call to hold the worker for ~{BLOCK:?}, neighbour waited only {blocked:?}");
        assert!(protected < BLOCK / 4, "the neighbour should run while the blocking work happens elsewhere, but waited {protected:?}");
    }

    /// Both call sites tokio could reject — rejection is a panic, not an error:
    /// a `spawn_blocking` thread (which still reports a runtime handle) and
    /// nested wrapped helpers.
    #[test_case(false => 42 ; "safe to call from a blocking thread which also sees a runtime handle")]
    #[test_case(true => 42 ; "nested helper calls are allowed")]
    fn reachable_without_panicking(nested: bool) -> i32 {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        runtime.block_on(async move {
            let joined = if nested {
                tokio::spawn(async { without_blocking_the_worker(|| without_blocking_the_worker(|| 42)) }).await
            } else {
                tokio::task::spawn_blocking(|| without_blocking_the_worker(|| 42)).await
            };
            joined.unwrap()
        })
    }

    #[test]
    fn set_and_advance() {
        let t0 = 4_000_000_000_000_000_i64;
        set_micros(t0);
        assert_eq!(now_micros(), t0);
        let t1 = advance_micros(60_000_000);
        assert_eq!(t1, t0 + 60_000_000);
        assert_eq!(now_micros(), t1);
        unfreeze();
        assert!(!is_frozen());
    }

    /// One commit is held open until all `FOLLOWERS` have taken their tickets,
    /// so they must all be covered by the single commit that follows: 2 commits
    /// for 1 + FOLLOWERS callers.
    #[test]
    fn callers_queued_during_a_commit_all_ride_the_next_one() {
        const FOLLOWERS: u64 = 8;
        let group = GroupCommit::default();
        let commits = std::sync::atomic::AtomicU64::new(0);
        let leading = std::sync::atomic::AtomicBool::new(false);
        let commit = || {
            commits.fetch_add(1, Ordering::Relaxed);
            leading.store(true, Ordering::Release);
            // Hold this commit open until every follower has queued behind it.
            while group.offered() < FOLLOWERS + 1 {
                std::thread::yield_now();
            }
            Ok::<_, std::convert::Infallible>(())
        };

        std::thread::scope(|scope| {
            scope.spawn(|| group.commit(commit).unwrap());
            while !leading.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            for _ in 0..FOLLOWERS {
                scope.spawn(|| group.commit(commit).unwrap());
            }
        });

        let (performed, coalesced) = group.counts();
        assert_eq!(commits.load(Ordering::Relaxed), 2, "{FOLLOWERS} callers queued during one commit must share the next, not fsync each");
        assert_eq!((performed, coalesced), (2, FOLLOWERS - 1), "all but the two leaders must be recorded as riding someone else's commit");
    }

    /// Commits from a fresh caller, reporting whether its own commit actually ran
    /// rather than riding a watermark a failed leader left behind.
    fn next_caller_commits(group: &GroupCommit) -> bool {
        let ran = std::sync::atomic::AtomicBool::new(false);
        group
            .commit(|| {
                ran.store(true, Ordering::Relaxed);
                Ok::<_, &str>(())
            })
            .unwrap();
        ran.load(Ordering::Relaxed)
    }

    /// A panicking leader must hand leadership back. The assertion that matters
    /// is that the second call RETURNS at all; it hangs forever without `Drop`.
    #[test]
    fn a_panicking_leader_does_not_wedge_every_caller_behind_it() {
        let group = GroupCommit::default();
        let died = std::thread::scope(|scope| scope.spawn(|| group.commit(|| -> Result<(), &str> { panic!("disk on fire") })).join());
        assert!(died.is_err(), "the panic must reach the leader's own caller");
        assert!(next_caller_commits(&group), "the next caller must be able to lead; a panic must not advance the durable watermark either");
    }

    /// A failed commit must not be inherited: the waiters behind it retry.
    #[test]
    fn a_failed_commit_is_not_reported_as_durable_to_the_waiters() {
        let group = GroupCommit::default();
        assert_eq!(group.commit(|| Err::<(), &str>("disk full")), Err("disk full"));
        assert!(next_caller_commits(&group), "the waiter behind a failed commit must perform its own");
    }
}

/// Initializes tracing once for tests.
pub fn init_test_logging() {
    use tracing_subscriber::{EnvFilter, filter::LevelFilter};
    let _ = tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into())).with_test_writer().try_init();
}

pub mod test_helpers {
    use std::{path::PathBuf, sync::Arc};

    use arrow_json::ReaderBuilder;
    use datafusion::arrow::{
        array::AsArray,
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use itertools::Itertools;
    use serde_json::{Value, json};

    use crate::{config::AppConfig, schema::get_default_schema};

    /// Process CPU across all threads, for isolated work measurements.
    pub fn process_cpu() -> anyhow::Result<std::time::Duration> {
        let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
        // SAFETY: the pointer is aligned, writable, and valid for one timespec. The
        // syscall initializes it on success; the error path never reads its contents.
        if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, time.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: successful clock_gettime initialized both timespec fields.
        let time = unsafe { time.assume_init() };
        Ok(std::time::Duration::from_secs(u64::try_from(time.tv_sec)?) + std::time::Duration::from_nanos(u64::try_from(time.tv_nsec)?))
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BufferMode {
        Enabled,
        FlushImmediately,
    }

    pub struct TestConfigBuilder {
        test_name: String,
        buffer_mode: BufferMode,
        deletion_vectors: bool,
    }

    impl TestConfigBuilder {
        pub fn new(test_name: &str) -> Self {
            Self { test_name: test_name.to_string(), buffer_mode: BufferMode::Enabled, deletion_vectors: true }
        }

        pub fn with_buffer_mode(mut self, mode: BufferMode) -> Self {
            self.buffer_mode = mode;
            self
        }

        /// No-op: rollups are unconditional. Kept as intent at the call sites.
        pub fn with_rollups(self) -> Self {
            self
        }

        /// Force the copy-on-write dedup path (whole-file rewrite) instead of the
        /// default deletion-vector path, for tests asserting file replacement or
        /// sharded rewrite output, which DV masking does not produce.
        pub fn without_deletion_vectors(mut self) -> Self {
            self.deletion_vectors = false;
            self
        }

        pub fn build(self) -> Arc<AppConfig> {
            let id = format!("{}-{}", self.test_name, &uuid::Uuid::new_v4().to_string()[..8]);
            let mut cfg = minio_base_config(&id, &format!("/tmp/timefusion-{id}"));
            cfg.buffer.timefusion_flush_immediately = self.buffer_mode == BufferMode::FlushImmediately;
            cfg.maintenance.timefusion_use_deletion_vectors = self.deletion_vectors;
            Arc::new(cfg)
        }
    }

    /// Shared MinIO + foyer-disabled config keyed by an explicit table id / data dir.
    fn minio_base_config(table_id: &str, data_dir: &str) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
        cfg.core.timefusion_table_prefix = format!("test-{table_id}");
        cfg.core.timefusion_data_dir = PathBuf::from(data_dir);
        cfg.cache.timefusion_foyer_disabled = true;
        // Dwell off: suite tests assert "sealed => next tick flushes".
        cfg.buffer.timefusion_flush_dwell_secs = 0;
        cfg
    }

    /// MinIO-backed config with an explicit table id and data dir.
    pub fn minio_test_config(table_id: &str, data_dir: &str) -> Arc<AppConfig> {
        Arc::new(minio_base_config(table_id, data_dir))
    }

    /// Live row count from the Delta log: `sum(num_records) − sum(deletion-vector
    /// cardinality)` over active files. Bypasses the routed scan path, so unlike a
    /// `query_delta_only` COUNT it is not collapsed by the read-side `DedupExec` —
    /// it reports rows still live on disk under either dedup strategy.
    pub async fn delta_physical_row_count(table_ref: &tokio::sync::RwLock<deltalake::DeltaTable>) -> anyhow::Result<i64> {
        let guard = table_ref.read().await;
        Ok(guard
            .snapshot()?
            .snapshot()
            .log_data()
            .iter()
            .map(|f| f.num_records().and_then(|n| i64::try_from(n).ok()).unwrap_or(0) - f.deletion_vector_descriptor().map_or(0, |dv| dv.cardinality))
            .sum())
    }

    /// Build a BufferedWriteLayer for tests/benches.
    pub fn test_layer(cfg: Arc<AppConfig>) -> anyhow::Result<crate::write::BufferedWriteLayer> {
        crate::write::BufferedWriteLayer::with_config(cfg, crate::read::functions::function_registry()?)
    }

    /// Collect a string column from a layer query, casting through Utf8 so
    /// Utf8View/Utf8 both work. Nulls are SKIPPED, so a short result means nulls.
    pub fn query_col_strings(layer: &crate::write::BufferedWriteLayer, project: &str, table: &str, col: &str) -> Vec<String> {
        layer
            .query(project, table, &[])
            .unwrap()
            .iter()
            .flat_map(|b| {
                let arr = cast(b.column_by_name(col).unwrap(), &DataType::Utf8).unwrap();
                arr.as_string::<i32>().iter().flatten().map(str::to_string).collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        json_to_batch_for(&get_default_schema().table_name, records)
    }

    /// `json_to_batch` against any registered table's schema.
    pub fn json_to_batch_for(table: &str, records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        let target_schema = crate::schema::get_schema(table).ok_or_else(|| anyhow::anyhow!("unknown table `{table}`"))?.schema_ref();

        // arrow-json only produces Utf8: read into a Utf8 mirror of the schema, then cast back.
        let utf8_flavoured = |dt: &DataType| match dt {
            DataType::Utf8View => DataType::Utf8,
            DataType::List(inner) if inner.data_type() == &DataType::Utf8View => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            other => other.clone(),
        };
        let json_read_schema =
            Arc::new(Schema::new(target_schema.fields().iter().map(|f| Field::new(f.name(), utf8_flavoured(f.data_type()), f.is_nullable())).collect_vec()));

        let json_data = records.iter().join("\n");

        let batch = ReaderBuilder::new(json_read_schema)
            .with_batch_size(records.len().max(1))
            .build(std::io::Cursor::new(json_data.as_bytes()))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))??;

        let columns =
            batch.columns().iter().zip(target_schema.fields()).map(|(col, field)| cast(col, field.data_type()).unwrap_or_else(|_| col.clone())).collect();
        Ok(RecordBatch::try_new(target_schema, columns)?)
    }

    pub fn test_span(id: &str, name: &str, project_id: &str) -> Value {
        test_span_ts(id, name, project_id, chrono::Utc::now().timestamp_micros())
    }

    /// `test_span` with an explicit timestamp, to target a specific MemBuffer bucket.
    pub fn test_span_ts(id: &str, name: &str, project_id: &str, ts_micros: i64) -> Value {
        let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap_or_else(chrono::Utc::now).date_naive().to_string();
        json!({
            "timestamp": ts_micros,
            "id": id,
            "name": name,
            "project_id": project_id,
            "date": date,
            "hashes": [],
            "summary": [format!("Test span: {name}")]
        })
    }

    /// Read a string cell from any String/LargeString/StringView array; panics on other types.
    pub fn array_get_str(arr: &dyn datafusion::arrow::array::Array, idx: usize) -> String {
        match arr.data_type() {
            DataType::Utf8View => arr.as_string_view().value(idx),
            DataType::Utf8 => arr.as_string::<i32>().value(idx),
            DataType::LargeUtf8 => arr.as_string::<i64>().value(idx),
            dt => panic!("expected string array but got {dt:?}"),
        }
        .to_string()
    }
}
