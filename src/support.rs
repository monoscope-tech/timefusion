//! Process-wide wall or frozen clock used by eviction and flush.
//!
//! Tests can control it through SQL UDFs or the compatible
//! `TIMEFUSION_FROZEN_TIME` environment variable.

use std::sync::atomic::{AtomicI64, Ordering};

/// Lock without letting a poisoned mutex propagate: the mutexes in this crate
/// guard plain data, so a panicking holder leaves the data usable.
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

/// Wall-clock seconds since epoch, honoring the frozen-clock test seam (see `now_micros`).
#[inline]
pub fn now_secs() -> u64 {
    (now_micros() / 1_000_000).max(0) as u64
}

/// Today's UTC date on the (possibly frozen) clock. Maintenance that decides
/// which partitions are sealed must read this rather than `Utc::now`, or a
/// frozen-clock test sees a date its fixture data never lands in.
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

/// Advance the frozen time by `delta_micros`. If the clock is *not* frozen,
/// this freezes it at `wall_now + delta_micros` so the first call from an
/// unprimed test harness has predictable behavior. Returns new value.
pub fn advance_micros(delta_micros: i64) -> i64 {
    set_micros(now_micros().saturating_add(delta_micros))
}

/// Switch back to wall-clock mode.
pub fn unfreeze() {
    FROZEN_NOW.store(WALL_SENTINEL, Ordering::Release);
}

/// Run `f` without freezing the runtime worker it lands on.
///
/// A blocking syscall inside an `async` task does not just make that task slow —
/// it holds the worker thread, so every OTHER task queued on that worker waits
/// too, including ones with nothing to do with the caller. On 2026-08-24 prod
/// showed the shape of this directly: a 500 ms timer waking **0.3–2.9 s late,
/// several times a minute**, on 48 workers with the host 40 % idle
/// (`docs/plans/2026-08-24-a-trivial-query-costs-seconds-after-hours-of-uptime.md`).
/// Workers were not busy; they were *blocked*.
///
/// `block_in_place` tells tokio to hand this worker's remaining tasks to
/// another thread before running `f`, so the blocking work costs one thread
/// rather than one thread *and* its queue. It requires the multi-thread
/// runtime and panics elsewhere, so a current-thread runtime (and any
/// non-async caller — CLI subcommands, tests) runs `f` directly, which is
/// correct: there is no shared worker queue to protect.
///
/// Use for genuinely blocking work — `fsync`, a large synchronous serialize —
/// not as a general escape from `async`.
pub fn without_blocking_the_worker<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Coalesces concurrent durable commits into one, without weakening what a
/// caller is promised when it returns.
///
/// The problem it solves: a durability barrier that every writer must cross
/// before it may acknowledge its own work costs one `fsync` per writer, so its
/// price scales with the write rate instead of with the data. Prod 2026-09-11:
/// every pgwire INSERT checkpointed the maintenance journal — an `fsync` plus a
/// full task scan — before acking the client, on a disk array already at 98.8 %
/// utilisation with an 80 ms write wait.
///
/// The contract is unchanged, which is the point. Deferring the commit would
/// be wrong: a crash between a mutation and its (never-taken) checkpoint leaves
/// a mutation with no maintenance record, and WAL replay does not re-seed one.
/// Here `commit` still returns only once a commit that **includes the caller's
/// mutations** has completed — it just may be a commit someone else performed.
///
/// Usage: apply your mutations, THEN call `commit`. A ticket is taken on entry,
/// so mutations applied after the call may or may not be included; mutations
/// applied before it always are.
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
    /// Mutations offered so far. Incremented by each caller before it waits.
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
    /// Commits actually performed, and callers that rode someone else's.
    pub performed: u64,
    pub coalesced: u64,
}

/// The leader's hold on the commit, released in `Drop` so an unwind cannot
/// strand it. `succeeded` stays false unless the commit returned `Ok`, so a
/// panic advances the durable watermark exactly as little as a failure does.
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
    /// work — that is what lets one call satisfy many waiters. A failed commit
    /// is reported to its leader and leaves `committed` untouched, so the
    /// waiters behind it retry rather than inherit a success that never
    /// happened. A PANICKING commit is treated the same way, and hands the
    /// leadership back on unwind: this is the durability barrier every write
    /// crosses, so a leader that dies holding it would park every subsequent
    /// writer on the condvar forever — the whole process stops acknowledging.
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
            // leadership and wakes the waiters instead of wedging them.
            let mut leadership = Leadership { group: self, covered, succeeded: false };
            let result = do_commit();
            leadership.succeeded = result.is_ok();
            drop(leadership);
            // The leader's own ticket is <= `covered` by construction, so a
            // successful commit always covers it — return rather than loop, or
            // it would count itself as having ridden someone else's commit.
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

    use super::*;

    /// The prod defect in miniature: one worker, a task blocking it, and a
    /// timer that has to wake on time anyway.
    ///
    /// Run directly, the blocking call owns the only worker and the 50 ms timer
    /// cannot fire until it finishes — which is the 0.3–2.9 s scheduling lag
    /// prod reports several times a minute. Through
    /// `without_blocking_the_worker`, tokio moves the timer to another thread
    /// and it wakes on schedule.
    #[test]
    fn a_blocking_call_must_not_hold_the_worker_its_neighbours_are_queued_on() {
        const BLOCK: Duration = Duration::from_millis(400);

        // How long a neighbour task waits to be polled at all — worker
        // starvation measured directly. A timer would be a weaker probe here:
        // tokio's time driver can be driven by the idle `block_on` thread, so
        // timers still fire while the worker is held.
        //
        // Both tasks are SPAWNED (`block_on` runs on the calling thread, not
        // the worker), and the blocker is spawned first so it owns the worker
        // before the neighbour is queued behind it. `wrap` is the only
        // difference between the two runs.
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

    /// `block_in_place` panics when called off a runtime worker, and a
    /// `spawn_blocking` thread still reports a runtime handle — so the flavor
    /// check alone would not save us if tokio rejected that combination. It
    /// does not, and `write_atomic_with` is reachable from both kinds of
    /// thread, so this pins the behaviour the helper depends on.
    #[test]
    fn safe_to_call_from_a_blocking_thread_which_also_sees_a_runtime_handle() {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let out = runtime.block_on(async { tokio::task::spawn_blocking(|| without_blocking_the_worker(|| 42)).await });
        assert_eq!(out.unwrap(), 42);
    }

    /// Wrapped helpers now nest — `store_sidecar` wraps a serialize whose
    /// write tail is `write_atomic_with`, itself wrapped. `block_in_place`
    /// inside `block_in_place` has to be legal for that to be a fix rather
    /// than a panic on the maintenance path.
    #[test]
    fn nested_helper_calls_are_allowed() {
        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
        let out = runtime.block_on(async { tokio::spawn(async { without_blocking_the_worker(|| without_blocking_the_worker(|| 42)) }).await });
        assert_eq!(out.unwrap(), 42);
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

    /// Deterministic, not racy. One commit is pinned open while `FOLLOWERS`
    /// callers queue behind it; it is released only once every one of them has
    /// taken its ticket (`offered`). They must therefore all be covered by the
    /// single commit that follows — 1 + FOLLOWERS callers for 2 commits, not
    /// 1 + FOLLOWERS commits.
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

    /// A panicking leader must hand leadership back, not strand every writer.
    ///
    /// This is the barrier every write crosses before it is acknowledged, so a
    /// leader that dies still holding it parks all later callers on the condvar
    /// permanently — the process stops acking writes altogether. The assertion
    /// that matters is that the second call RETURNS at all; it hangs forever
    /// without the `Drop`.
    #[test]
    fn a_panicking_leader_does_not_wedge_every_caller_behind_it() {
        let group = GroupCommit::default();
        let died = std::thread::scope(|scope| scope.spawn(|| group.commit(|| -> Result<(), &str> { panic!("disk on fire") })).join());
        assert!(died.is_err(), "the panic must reach the leader's own caller");

        let committed = std::sync::atomic::AtomicBool::new(false);
        group
            .commit(|| {
                committed.store(true, Ordering::Relaxed);
                Ok::<_, &str>(())
            })
            .unwrap();
        assert!(committed.load(Ordering::Relaxed), "the next caller must be able to lead; a panic must not advance the durable watermark either");
    }

    /// A failed commit must not be inherited: the waiters behind it retry.
    #[test]
    fn a_failed_commit_is_not_reported_as_durable_to_the_waiters() {
        let group = GroupCommit::default();
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let first = group.commit(|| {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err::<(), &str>("disk full")
        });
        assert_eq!(first, Err("disk full"));
        // The next caller must actually commit rather than read a `committed`
        // watermark the failed attempt had no right to advance.
        group
            .commit(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                Ok::<_, &str>(())
            })
            .unwrap();
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
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
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use itertools::Itertools;
    use serde_json::{Value, json};

    use crate::{config::AppConfig, schema::get_default_schema};

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

        /// Rollups are unconditional now, so this gates nothing — it stays as a
        /// declaration of intent at the call sites.
        pub fn with_rollups(self) -> Self {
            self
        }

        /// Force the COPY-ON-WRITE dedup path (whole-file rewrite) instead of the
        /// default deletion-vector path. For tests that assert copy-on-write
        /// mechanics — file replacement, sharded rewrite output — which DV
        /// deliberately does not do (it masks the same file in place). DV is the
        /// prod default; this pins the revert path that must keep working.
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
        // Dwell off: suite tests assert "sealed => next tick flushes"; the
        // dwell gate has its own dedicated unit tests.
        cfg.buffer.timefusion_flush_dwell_secs = 0;
        cfg
    }

    /// MinIO-backed config with an explicit table id and data dir. Shared by the
    /// integration tests that manage their own per-test id/path.
    pub fn minio_test_config(table_id: &str, data_dir: &str) -> Arc<AppConfig> {
        Arc::new(minio_base_config(table_id, data_dir))
    }

    /// LIVE row count from the Delta log: `sum(num_records) − sum(deletion-vector
    /// cardinality)`, over all active files. Bypasses the routed scan path — unlike
    /// a `query_delta_only` COUNT it is NOT collapsed by the read-side `DedupExec`,
    /// so it reflects rows still LIVE on disk (what the dedup *sweep* tests assert).
    ///
    /// DV-aware so it means the same thing under both dedup strategies: a
    /// copy-on-write sweep drops the loser row (num_records falls); a DV sweep
    /// masks it (num_records unchanged, but its deletion-vector cardinality rises).
    /// Either way a collapsed duplicate stops counting here, and a sweep that was
    /// budget-blocked (no DV written, no rewrite) still counts both copies.
    pub async fn delta_physical_row_count(table_ref: &tokio::sync::RwLock<deltalake::DeltaTable>) -> anyhow::Result<i64> {
        let guard = table_ref.read().await;
        // Per active file: numRecords minus its deletion-vector cardinality (the
        // masked rows), same accounting as the dedup verify path in maintain.rs.
        Ok(guard
            .snapshot()?
            .snapshot()
            .log_data()
            .iter()
            .map(|f| f.num_records().and_then(|n| i64::try_from(n).ok()).unwrap_or(0) - f.deletion_vector_descriptor().map_or(0, |dv| dv.cardinality))
            .sum())
    }

    /// Build a BufferedWriteLayer for tests/benches without repeating the registry boilerplate.
    pub fn test_layer(cfg: Arc<AppConfig>) -> anyhow::Result<crate::write::BufferedWriteLayer> {
        crate::write::BufferedWriteLayer::with_config(cfg, crate::read::functions::function_registry()?)
    }

    /// Collect a string column out of a layer query result as `Vec<String>`,
    /// casting through Utf8 so Utf8View/Utf8 storage both work. Nulls are
    /// skipped, so a shorter-than-expected result means null cells — assert on
    /// `.len()` when that matters.
    pub fn query_col_strings(layer: &crate::write::BufferedWriteLayer, project: &str, table: &str, col: &str) -> Vec<String> {
        use datafusion::arrow::array::AsArray;
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

        // arrow-json only produces Utf8, so read into a Utf8-flavoured mirror of the target schema and cast back.
        let json_read_schema = Arc::new(Schema::new(
            target_schema
                .fields()
                .iter()
                .map(|f| {
                    Field::new(
                        f.name(),
                        match f.data_type() {
                            DataType::Utf8View => DataType::Utf8,
                            DataType::List(inner) if inner.data_type() == &DataType::Utf8View => {
                                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
                            }
                            other => other.clone(),
                        },
                        f.is_nullable(),
                    )
                })
                .collect::<Vec<_>>(),
        ));

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

    /// Like `test_span` but with an explicit timestamp, for tests that need
    /// rows to land in a specific MemBuffer bucket.
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
        use datafusion::arrow::array::AsArray;
        match arr.data_type() {
            DataType::Utf8View => arr.as_string_view().value(idx),
            DataType::Utf8 => arr.as_string::<i32>().value(idx),
            DataType::LargeUtf8 => arr.as_string::<i64>().value(idx),
            dt => panic!("expected string array but got {dt:?}"),
        }
        .to_string()
    }
}
