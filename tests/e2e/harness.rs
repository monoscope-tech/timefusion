//! E2E test harness: dynamic MinIO container, full bootstrap, virtual clock,
//! pgwire client. Mirrors prod `main.rs` via `timefusion::bootstrap`.

#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
use datafusion_postgres::ServerOptions;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, core::WaitFor, runners::AsyncRunner};
use timefusion::{
    config::AppConfig,
    server::{Bootstrapped, bootstrap},
    support,
    write::BufferedWriteLayer,
};
use tokio::sync::Notify;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// MinIO release with atomic conditional PUT support. The testcontainers
/// module's older default can overwrite racing Delta commits. Modern MinIO
/// prints its readiness banner on stderr, hence the custom image below.
pub const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";

pub fn pinned_minio_image() -> GenericImage {
    GenericImage::new("minio/minio", MINIO_TAG).with_wait_for(WaitFor::message_on_stderr("API:"))
}

pub const FROZEN_START_MICROS: i64 = 1_900_000_000_000_000; // ~2030-03-15

#[derive(Clone)]
pub struct E2eEnvBuilder {
    bucket_duration_secs: u64,
    flush_interval_secs: u64,
    eviction_interval_secs: u64,
    retention_mins: u64,
    foyer_disabled: bool,
    flush_immediately: bool,
    max_memory_mb: usize,
    frozen_at_micros: i64,
    checkpoint_interval: u64,
    optimize_sort_by: bool,
    use_deletion_vectors: bool,
    warm_full_files: bool,
    dml_merge_key_prune: bool,
    tantivy_prefilter: bool,
    dml_coalesce_secs: u64,
    page_row_count_limit: Option<usize>,
    sort_skip_bytes: Option<usize>,
    light_optimize_target_size: Option<i64>,
    light_optimize_enabled: bool,
    wide_scan_max_files: Option<usize>,
    wide_scan_max_mb: Option<u64>,
    repair_resume: bool,
    mark_sorted_at_write: bool,
}

impl Default for E2eEnvBuilder {
    fn default() -> Self {
        Self {
            // Aggressive defaults for fast deterministic tests.
            bucket_duration_secs: 60,
            flush_interval_secs: 1,
            eviction_interval_secs: 1,
            retention_mins: 5,
            foyer_disabled: false,
            flush_immediately: false,
            max_memory_mb: 256,
            frozen_at_micros: FROZEN_START_MICROS,
            checkpoint_interval: 10,
            optimize_sort_by: false,
            warm_full_files: false,
            sort_skip_bytes: None,
            light_optimize_target_size: None,
            light_optimize_enabled: true,
            wide_scan_max_files: None,
            wide_scan_max_mb: None,
            // Mirror the prod default (on) so the whole e2e suite exercises the
            // merge-on-read DV write path. Opt out per-test with `without_deletion_vectors`.
            use_deletion_vectors: true,
            dml_merge_key_prune: true,
            tantivy_prefilter: true,
            // 0 = synchronous DML (prod-default off). Prod runs 60s; set >0 to
            // exercise the coalescer defer/drain path in tests.
            dml_coalesce_secs: 0,
            page_row_count_limit: None,
            repair_resume: false,
            mark_sorted_at_write: true,
        }
    }
}

impl E2eEnvBuilder {
    pub fn with_bucket_duration(mut self, d: Duration) -> Self {
        self.bucket_duration_secs = d.as_secs().max(1);
        self
    }
    pub fn with_flush_interval(mut self, d: Duration) -> Self {
        self.flush_interval_secs = d.as_secs().max(1);
        self
    }
    pub fn with_eviction_interval(mut self, d: Duration) -> Self {
        self.eviction_interval_secs = d.as_secs().max(1);
        self
    }
    pub fn with_retention(mut self, d: Duration) -> Self {
        self.retention_mins = (d.as_secs() / 60).max(1);
        self
    }
    /// Commit staged-but-uncommitted repair parquet at boot instead of deleting
    /// it. Off in prod for the first deploy, so a test that asserts on resume
    /// MUST turn it on or the resume path is a silent no-op.
    /// Leave flush output UNMARKED, so its files are footer-repair suspects.
    ///
    /// Write-time marking (2026-08-28) records a file as verified-sorted when the write stamps
    /// its footer, which means a fixture built by flushing sorted rows has NO repair work — and
    /// a test that needs repair to actually do something gets a silent no-op instead. Turning
    /// the mechanism off is the honest way to restore that precondition: it is a real config
    /// flag, not a test seam, and it gates the seeding sweep too (which would otherwise re-derive
    /// the same marks from the footers).
    pub fn without_write_time_sort_marking(mut self) -> Self {
        self.mark_sorted_at_write = false;
        self
    }
    pub fn with_repair_resume(mut self) -> Self {
        self.repair_resume = true;
        self
    }
    /// Shrink the in-process sort budget (in-memory Arrow bytes) so a test can
    /// reproduce a bin that exceeds it — the production shape, where a 256 MB
    /// FILE-byte compaction target is ~17x over a 256 MB in-memory budget.
    pub fn with_sort_skip_bytes(mut self, bytes: usize) -> Self {
        self.sort_skip_bytes = Some(bytes);
        self
    }
    /// Shrink the hot-tail compaction target so a test-sized file counts as
    /// "converged" (>= 7/8 of target) — the state a 265-778MB prod file is in.
    pub fn with_light_optimize_target(mut self, bytes: i64) -> Self {
        self.light_optimize_target_size = Some(bytes);
        self
    }
    pub fn without_light_optimize(mut self) -> Self {
        self.light_optimize_enabled = false;
        self
    }
    /// Shrink the wide-scan file budget so a test-sized file set trips the
    /// admission gate (the prod default is 256 files).
    pub fn with_wide_scan_max_files(mut self, files: usize) -> Self {
        self.wide_scan_max_files = Some(files);
        self
    }
    pub fn with_wide_scan_max_mb(mut self, mb: u64) -> Self {
        self.wide_scan_max_mb = Some(mb);
        self
    }
    pub fn with_foyer_enabled(mut self) -> Self {
        self.foyer_disabled = false;
        self
    }
    pub fn with_foyer_disabled(mut self) -> Self {
        self.foyer_disabled = true;
        self
    }
    pub fn with_flush_immediately(mut self) -> Self {
        self.flush_immediately = true;
        self
    }
    pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
        self.max_memory_mb = mb;
        self
    }
    pub fn with_frozen_at(mut self, micros: i64) -> Self {
        self.frozen_at_micros = micros;
        self
    }
    pub fn with_checkpoint_interval(mut self, n: u64) -> Self {
        self.checkpoint_interval = n;
        self
    }
    pub fn with_optimize_sort_by(mut self) -> Self {
        self.optimize_sort_by = true;
        self
    }
    /// Force small parquet data pages (row-count capped) so a few hundred rows
    /// yield many pages within one row group — exercises page-index pruning.
    pub fn with_page_row_count_limit(mut self, rows: usize) -> Self {
        self.page_row_count_limit = Some(rows);
        self
    }
    /// Warm freshly-flushed file BODIES (not just footers) into Foyer, so the
    /// first recent-window scan after a flush is served warm instead of cold
    /// from S3 — the "keep the hot tail warm" lever.
    pub fn with_warm_full_files(mut self) -> Self {
        self.warm_full_files = true;
        self
    }
    pub fn with_deletion_vectors(mut self) -> Self {
        self.use_deletion_vectors = true;
        self
    }
    pub fn without_deletion_vectors(mut self) -> Self {
        self.use_deletion_vectors = false;
        self
    }
    pub fn with_dml_merge_key_prune(mut self, on: bool) -> Self {
        self.dml_merge_key_prune = on;
        self
    }
    /// The tantivy scan prefilter (id IN-list, zero-hit file exclusion and
    /// row selection all at once). Off makes the Delta leg's file list
    /// independent of whether the sidecar index has finished building — which
    /// a flush spawns as a DETACHED task, so it is otherwise a race the test
    /// cannot observe or await.
    pub fn with_tantivy_prefilter(mut self, on: bool) -> Self {
        self.tantivy_prefilter = on;
        self
    }
    /// Defer `UPDATE ... FROM` Delta legs through the coalescer (prod runs 60s);
    /// drain explicitly with `E2eEnv::drain_dml_coalescer`. 0 = synchronous.
    pub fn with_dml_coalesce_secs(mut self, secs: u64) -> Self {
        self.dml_coalesce_secs = secs;
        self
    }

    pub async fn start(self) -> Result<E2eEnv> {
        timefusion::support::init_test_logging();

        // Freeze clock BEFORE bootstrap so background tasks see test time.
        support::set_micros(self.frozen_at_micros);

        let (minio, endpoint) = ensure_local_minio().await?;

        let test_id = Uuid::new_v4().to_string()[..8].to_string();
        let bucket = format!("e2e-{test_id}");
        let data_dir = std::env::temp_dir().join(format!("timefusion-e2e-{test_id}"));
        // Defensive: wipe before create. Each test_id is UUID-derived so this can
        // only target our own dir. CI's /tmp is shared across sequential e2e tests
        // in the same job, and `gc_wal_files` only deletes files older than
        // `retention_mins * 2` (~2h20m) — so a fresh leftover from a prior test
        // survives the gc, and `check_wal_version_stamp` then trips
        // `Unsupported WAL version: 0 (expected 1)` on what should be a fresh dir.
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).ok();

        // `<data_dir>/wal` is this test's WAL. `WalManager` opens exactly that
        // path (`cfg.core.wal_dir()`), so nothing process-global is involved and
        // concurrent tests cannot replay each other's WAL.
        std::fs::create_dir_all(data_dir.join("wal")).ok();

        // Bucket creation: MinIO default credentials are minioadmin/minioadmin.
        create_bucket(&endpoint, &bucket).await.context("create MinIO bucket")?;

        // OS-assigned port: a fixed random window across ~55 parallel test
        // processes collides — the loser's bind fails silently inside the
        // spawned task and the client connects to the *other* test's server.
        let (pg_listener, pg_port) = bind_pg_listener().await?;
        let cfg = build_config(BuildCfgArgs {
            endpoint: &endpoint,
            bucket: &bucket,
            data_dir: data_dir.clone(),
            pg_port,
            bucket_duration_secs: self.bucket_duration_secs,
            flush_interval_secs: self.flush_interval_secs,
            eviction_interval_secs: self.eviction_interval_secs,
            retention_mins: self.retention_mins,
            foyer_disabled: self.foyer_disabled,
            flush_immediately: self.flush_immediately,
            max_memory_mb: self.max_memory_mb,
            checkpoint_interval: self.checkpoint_interval,
            optimize_sort_by: self.optimize_sort_by,
            use_deletion_vectors: self.use_deletion_vectors,
            warm_full_files: self.warm_full_files,
            dml_merge_key_prune: self.dml_merge_key_prune,
            tantivy_prefilter: self.tantivy_prefilter,
            dml_coalesce_secs: self.dml_coalesce_secs,
            sort_skip_bytes: self.sort_skip_bytes,
            light_optimize_target_size: self.light_optimize_target_size,
            light_optimize_enabled: self.light_optimize_enabled,
            wide_scan_max_files: self.wide_scan_max_files,
            wide_scan_max_mb: self.wide_scan_max_mb,
            page_row_count_limit: self.page_row_count_limit,
            repair_resume: self.repair_resume,
            mark_sorted_at_write: self.mark_sorted_at_write,
            test_id: &test_id,
        });

        let bootstrapped = bootstrap(Arc::clone(&cfg)).await.context("bootstrap")?;

        // Pre-warm the default tenant table (matches integration_test pattern).
        bootstrapped.db.get_or_create_table("e2e_project", "otel_logs_and_spans").await.context("pre-warm table")?;

        // Spawn pgwire server. Shutdown via Notify (same as integration_test).
        let pg_shutdown = Arc::new(Notify::new());
        spawn_pgwire(Arc::clone(&bootstrapped.session_ctx), Arc::clone(&bootstrapped.db), pg_listener, Arc::clone(&pg_shutdown));
        wait_for_pg(pg_port).await.context("pgwire never came up")?;

        Ok(E2eEnv {
            _minio: minio,
            wal_dir: data_dir.join("wal"),
            data_dir,
            pg_port,
            pg_shutdown,
            bootstrapped: Some(bootstrapped),
            bucket,
            endpoint,
            test_id,
            builder: self,
        })
    }
}

pub struct E2eEnv {
    /// None unless this test fell back to a Docker MinIO (no endpoint env, no
    /// running :9000, no local `minio` binary).
    _minio: Option<ContainerAsync<GenericImage>>,
    pub data_dir: PathBuf,
    pub pg_port: u16,
    pub bucket: String,
    endpoint: String,
    test_id: String,
    wal_dir: PathBuf,
    builder: E2eEnvBuilder,
    pg_shutdown: Arc<Notify>,
    bootstrapped: Option<Bootstrapped>,
}

impl E2eEnv {
    pub fn builder() -> E2eEnvBuilder {
        E2eEnvBuilder::default()
    }

    /// Flush sort budget for the NEXT `restart()`. Toggling it mid-test is the
    /// only way to build the shape footer repair actually walks in prod: ONE
    /// partition holding both poisoned and correctly-sorted-but-untagged files.
    pub fn set_sort_skip_bytes(&mut self, bytes: usize) {
        self.builder.sort_skip_bytes = Some(bytes);
    }

    fn bootstrapped(&self) -> &Bootstrapped {
        self.bootstrapped.as_ref().expect("E2eEnv was already shut down via restart()")
    }

    pub fn buffered_layer(&self) -> &Arc<BufferedWriteLayer> {
        &self.bootstrapped().buffered_layer
    }

    pub fn db(&self) -> &Arc<timefusion::database::Database> {
        &self.bootstrapped().db
    }

    /// Crash-and-restart: simulate a process crash (no graceful flush) and
    /// re-bootstrap against the same MinIO bucket + data_dir. Mirrors a
    /// hard kill — WAL replay must restore any unflushed rows; rows the
    /// caller already force-flushed are read back from Delta.
    ///
    /// Uses `crash_for_test` (cancels tasks without final flush) rather
    /// than the graceful `shutdown` — otherwise the buffered layer would
    /// drain MemBuffer into Delta on the way down, defeating the WAL
    /// replay assertion.
    pub async fn restart(&mut self) -> Result<()> {
        let prev = self.bootstrapped.take().expect("already shut down");
        prev.buffered_layer.crash_for_test().await;
        self.pg_shutdown.notify_one();
        // Retire the old instance's background work (preload/warm tasks hold
        // their own Arc<Database>, so dropping `prev` alone leaves them — and
        // their in-flight Foyer fetches — running for the rest of the test;
        // see Drop for why a live fetch at Runtime teardown deadlocks).
        // Crash semantics are preserved: this cancels maintenance and closes
        // the cache but never drains MemBuffer or advances the WAL cursor.
        let _ = prev.db.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(10)).await;
        drop(prev);

        let (pg_listener, pg_port) = bind_pg_listener().await?;
        let cfg = build_config(BuildCfgArgs {
            endpoint: &self.endpoint,
            bucket: &self.bucket,
            data_dir: self.data_dir.clone(),
            pg_port,
            bucket_duration_secs: self.builder.bucket_duration_secs,
            flush_interval_secs: self.builder.flush_interval_secs,
            eviction_interval_secs: self.builder.eviction_interval_secs,
            retention_mins: self.builder.retention_mins,
            foyer_disabled: self.builder.foyer_disabled,
            flush_immediately: self.builder.flush_immediately,
            max_memory_mb: self.builder.max_memory_mb,
            checkpoint_interval: self.builder.checkpoint_interval,
            optimize_sort_by: self.builder.optimize_sort_by,
            use_deletion_vectors: self.builder.use_deletion_vectors,
            warm_full_files: self.builder.warm_full_files,
            dml_merge_key_prune: self.builder.dml_merge_key_prune,
            tantivy_prefilter: self.builder.tantivy_prefilter,
            dml_coalesce_secs: self.builder.dml_coalesce_secs,
            sort_skip_bytes: self.builder.sort_skip_bytes,
            light_optimize_target_size: self.builder.light_optimize_target_size,
            light_optimize_enabled: self.builder.light_optimize_enabled,
            wide_scan_max_files: self.builder.wide_scan_max_files,
            wide_scan_max_mb: self.builder.wide_scan_max_mb,
            page_row_count_limit: self.builder.page_row_count_limit,
            repair_resume: self.builder.repair_resume,
            mark_sorted_at_write: self.builder.mark_sorted_at_write,
            test_id: &self.test_id,
        });

        let bootstrapped = bootstrap(Arc::clone(&cfg)).await.context("re-bootstrap")?;
        bootstrapped.db.get_or_create_table("e2e_project", "otel_logs_and_spans").await.context("pre-warm table")?;

        self.pg_shutdown = Arc::new(Notify::new());
        spawn_pgwire(Arc::clone(&bootstrapped.session_ctx), Arc::clone(&bootstrapped.db), pg_listener, Arc::clone(&self.pg_shutdown));
        wait_for_pg(pg_port).await.context("pgwire never came up after restart")?;

        self.pg_port = pg_port;
        self.bootstrapped = Some(bootstrapped);
        Ok(())
    }

    pub async fn pg_client(&self) -> Result<Client> {
        connect_pg(self.pg_port).await
    }

    /// Advance the virtual clock by `delta`. Doesn't await any background work
    /// — pair with `await_next_flush` / `await_next_eviction` for assertions.
    pub fn advance(&self, delta: Duration) -> i64 {
        support::advance_micros(delta.as_micros() as i64)
    }

    /// Force-run a full flush immediately and synchronously. Returns
    /// `FlushStats` so tests can assert on what happened.
    pub async fn force_flush(&self) -> Result<timefusion::write::FlushStats> {
        self.buffered_layer().flush_all_now().await
    }

    pub async fn force_evict(&self) -> Result<()> {
        self.buffered_layer().force_evict_now().await
    }

    /// Drain the DML coalescer synchronously (runs the deferred Delta-leg
    /// merges now). No-op when coalescing is disabled (secs = 0).
    pub async fn drain_dml_coalescer(&self) {
        if let Some(c) = self.db().dml_coalescer() {
            c.drain(self.db()).await;
        }
    }

    /// Wait for the next flush-task iteration to complete (success or
    /// failure). Caller MUST call this BEFORE the action that triggers the
    /// flush (otherwise the notify can fire before we register interest).
    pub async fn await_next_flush(&self, timeout: Duration) -> Result<()> {
        let notify = self.buffered_layer().flush_tick_notify();
        tokio::time::timeout(timeout, notify.notified()).await.map_err(|_| anyhow::anyhow!("flush tick did not fire within {:?}", timeout))?;
        Ok(())
    }

    pub async fn await_next_eviction(&self, timeout: Duration) -> Result<()> {
        let notify = self.buffered_layer().eviction_tick_notify();
        tokio::time::timeout(timeout, notify.notified()).await.map_err(|_| anyhow::anyhow!("eviction tick did not fire within {:?}", timeout))?;
        Ok(())
    }

    pub fn snapshot_stats(&self) -> timefusion::write::StatsSnapshot {
        self.buffered_layer().snapshot_stats()
    }

    /// Foyer hit/miss/size snapshot. Returns `None` if Foyer was disabled
    /// via builder. Tests use this to assert cache warmth post-flush.
    pub async fn foyer_stats(&self) -> Option<timefusion::storage::CombinedCacheStats> {
        let cache = self.db().object_store_cache()?;
        Some(cache.get_stats().await)
    }
}

impl Drop for E2eEnv {
    fn drop(&mut self) {
        self.pg_shutdown.notify_one();
        // Deterministic teardown while the runtime is still alive. Leaving it
        // to Runtime::drop deadlocks: foyer's get_or_fetch spawns its fetch
        // task while holding the inflight mutex, and on a shutting-down
        // runtime tokio::spawn drops that future INLINE — RawFetch::drop then
        // re-locks the same mutex on the same thread, and BlockingPool::
        // shutdown waits on it forever (the 3×600s e2e timeouts, 2026-08-03).
        // Database::shutdown cancels the warm tasks and closes Foyer first,
        // so no fetch survives to Runtime teardown. block_in_place is fine:
        // every e2e test is `flavor = "multi_thread"`.
        if let Some(b) = self.bootstrapped.take() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let _ = tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(b.db.shutdown_by(deadline)));
        }
        // Unfreeze so we don't leak state into the next test in this binary.
        support::unfreeze();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

// helpers

struct BuildCfgArgs<'a> {
    endpoint: &'a str,
    bucket: &'a str,
    data_dir: PathBuf,
    pg_port: u16,
    bucket_duration_secs: u64,
    flush_interval_secs: u64,
    eviction_interval_secs: u64,
    retention_mins: u64,
    foyer_disabled: bool,
    flush_immediately: bool,
    max_memory_mb: usize,
    checkpoint_interval: u64,
    optimize_sort_by: bool,
    use_deletion_vectors: bool,
    warm_full_files: bool,
    dml_merge_key_prune: bool,
    tantivy_prefilter: bool,
    dml_coalesce_secs: u64,
    page_row_count_limit: Option<usize>,
    sort_skip_bytes: Option<usize>,
    light_optimize_target_size: Option<i64>,
    light_optimize_enabled: bool,
    wide_scan_max_files: Option<usize>,
    wide_scan_max_mb: Option<u64>,
    repair_resume: bool,
    mark_sorted_at_write: bool,
    test_id: &'a str,
}

fn build_config(args: BuildCfgArgs<'_>) -> Arc<AppConfig> {
    let mut cfg = AppConfig::default();
    cfg.aws.aws_s3_bucket = Some(args.bucket.to_string());
    cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
    cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
    cfg.aws.aws_s3_endpoint = args.endpoint.to_string();
    cfg.aws.aws_default_region = Some("us-east-1".to_string());
    cfg.aws.aws_allow_http = Some("true".to_string());
    cfg.core.timefusion_table_prefix = format!("e2e-{}", args.test_id);
    cfg.core.timefusion_data_dir = args.data_dir;
    cfg.core.pgwire_port = args.pg_port;
    cfg.buffer.timefusion_flush_interval_secs = args.flush_interval_secs;
    // Dwell off: e2e tests drive flushing with advance()+force_flush and
    // assert prompt visibility in Delta; the gate has dedicated unit tests.
    cfg.buffer.timefusion_flush_dwell_secs = 0;
    cfg.buffer.timefusion_eviction_interval_secs = args.eviction_interval_secs;
    cfg.buffer.timefusion_buffer_retention_mins = args.retention_mins;
    cfg.buffer.timefusion_bucket_duration_secs = args.bucket_duration_secs;
    cfg.buffer.timefusion_buffer_max_memory_mb = args.max_memory_mb;
    cfg.buffer.timefusion_flush_immediately = args.flush_immediately;
    cfg.cache.timefusion_foyer_disabled = args.foyer_disabled;
    cfg.parquet.timefusion_checkpoint_interval = args.checkpoint_interval;
    cfg.maintenance.timefusion_optimize_sort_by = args.optimize_sort_by;
    cfg.maintenance.timefusion_light_optimize_enabled = args.light_optimize_enabled;
    cfg.maintenance.timefusion_use_deletion_vectors = args.use_deletion_vectors;
    cfg.maintenance.timefusion_warm_full_files = args.warm_full_files;
    cfg.maintenance.timefusion_repair_resume_enabled = args.repair_resume;
    cfg.maintenance.timefusion_repair_mark_sorted_at_write = args.mark_sorted_at_write;
    cfg.maintenance.timefusion_dml_merge_key_prune = args.dml_merge_key_prune;
    // A 0% selectivity floor is the off switch for the WHOLE prefilter: any hit
    // set covers >= 0% of the indexed rows, so `decide_prefilter` always returns
    // `low_selectivity` and the Delta scan keeps the original predicate. Turning
    // off `timefusion_tantivy_file_pruning` alone is NOT enough — a zero-hit
    // index still yields an empty `id IN ()`, which prunes every file anyway.
    if !args.tantivy_prefilter {
        cfg.tantivy.timefusion_tantivy_prefilter_min_selectivity_pct = 0;
    }
    cfg.buffer.timefusion_dml_coalesce_secs = args.dml_coalesce_secs;
    if let Some(b) = args.sort_skip_bytes {
        cfg.maintenance.timefusion_sort_skip_bytes = b;
    }
    if let Some(t) = args.light_optimize_target_size {
        cfg.maintenance.timefusion_light_optimize_target_size = t;
    }
    if let Some(rows) = args.page_row_count_limit {
        cfg.parquet.timefusion_page_row_count_limit = rows;
    }
    if let Some(files) = args.wide_scan_max_files {
        cfg.memory.timefusion_wide_scan_max_files = files;
    }
    if let Some(mb) = args.wide_scan_max_mb {
        cfg.memory.timefusion_wide_scan_max_mb = mb;
    }
    Arc::new(cfg)
}

/// Local-first MinIO resolution, mirroring the sqllogictest harness:
///   1. `TIMEFUSION_TEST_S3_ENDPOINT` if set (CI's MinIO, or any hand-run one).
///   2. An already-running MinIO on 127.0.0.1:9000 (e.g. `make minio-start`).
///   3. The local `minio` binary — spawned DETACHED on :9000 and left running,
///      because e2e tests run as ~55 parallel processes and a per-test kill
///      would tear the server out from under every sibling. `make minio-stop`
///      reclaims it; subsequent runs reuse it (hit case 2).
///   4. Docker (testcontainers) — only when no `minio` binary is on PATH.
///
/// Per-test isolation comes from the unique bucket, never from the server.
async fn ensure_local_minio() -> Result<(Option<ContainerAsync<GenericImage>>, String)> {
    const LOCAL: &str = "127.0.0.1:9000";
    let port_open = || async { tokio::net::TcpStream::connect(LOCAL).await.is_ok() };
    if let Ok(ep) = std::env::var("TIMEFUSION_TEST_S3_ENDPOINT") {
        return Ok((None, ep));
    }
    if port_open().await {
        return Ok((None, format!("http://{LOCAL}")));
    }
    if std::process::Command::new("minio").arg("--version").output().map(|o| o.status.success()).unwrap_or(false) {
        let data_dir = std::env::temp_dir().join("timefusion-e2e-minio");
        std::fs::create_dir_all(&data_dir).ok();
        // Concurrent first-run races are fine: the losers' binds fail while the
        // health loop below waits for whichever sibling won.
        std::process::Command::new("minio")
            .args(["server", data_dir.to_str().unwrap(), "--address", LOCAL])
            .env("MINIO_ROOT_USER", "minioadmin")
            .env("MINIO_ROOT_PASSWORD", "minioadmin")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawn local minio server")?;
        for _ in 0..100 {
            if port_open().await {
                return Ok((None, format!("http://{LOCAL}")));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("local `minio` binary never came up on {LOCAL}");
    }
    let minio = pinned_minio_image()
        .with_cmd(["server", "/data"])
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .start()
        .await
        .context("start MinIO container")?;
    let host = minio.get_host().await.context("get MinIO host")?.to_string();
    let port = minio.get_host_port_ipv4(9000).await.context("get MinIO port")?;
    let endpoint = format!("http://{host}:{port}");
    Ok((Some(minio), endpoint))
}

async fn create_bucket(endpoint: &str, bucket: &str) -> Result<()> {
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "e2e");
    let cfg = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();
    let client = aws_sdk_s3::Client::from_conf(cfg);
    // create_bucket is idempotent enough — ignore BucketAlreadyOwnedByYou.
    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = format!("{e:?}");
            if msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists") {
                Ok(())
            } else {
                Err(anyhow::anyhow!("create_bucket({bucket}) failed: {msg}"))
            }
        }
    }
}

/// Bind an OS-assigned loopback port for pgwire. The listener is handed to the
/// server as-is, so there is no bind/connect race and no port window to collide in.
async fn bind_pg_listener() -> Result<(tokio::net::TcpListener, u16)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.context("bind pgwire listener")?;
    let port = listener.local_addr()?.port();
    Ok((listener, port))
}

fn spawn_pgwire(
    session_ctx: Arc<datafusion::execution::context::SessionContext>, db: Arc<timefusion::database::Database>, listener: tokio::net::TcpListener,
    shutdown: Arc<Notify>,
) {
    tokio::spawn(async move {
        let opts = ServerOptions::new();
        let auth = timefusion::server::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };
        tokio::select! {
            _ = shutdown.notified() => {},
            res = timefusion::server::serve_with_listener(listener, session_ctx, &opts, auth, None, Some(db), std::future::pending::<()>()) => {
                if let Err(e) = res {
                    eprintln!("pgwire error: {e:?}");
                }
            }
        }
    });
}

async fn connect_pg(port: u16) -> Result<Client> {
    let conn_str = format!("host=localhost port={port} user=postgres password=postgres");
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("pg conn error: {e}");
        }
    });
    Ok(client)
}

async fn wait_for_pg(port: u16) -> Result<()> {
    for _ in 0..200 {
        if connect_pg(port).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("pgwire never became ready on port {port}")
}

/// Insert one span row at `ts_micros` for an explicit `project_id`.
pub async fn insert_for(client: &tokio_postgres::Client, project_id: &str, id: &str, ts_micros: i64) -> Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], $3)",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&project_id, &id, &vec!["s"]]).await?;
    Ok(())
}

/// Insert one span row at `ts_micros` for the default `e2e_project`.
pub async fn insert_at(client: &tokio_postgres::Client, id: &str, ts_micros: i64) -> Result<()> {
    insert_for(client, "e2e_project", id, ts_micros).await
}

/// Insert one row into `mor_dormant`, which declares fewer columns than otel.
/// The deletion-vector tests moved here when `otel_logs_and_spans` flipped
/// `version_append`: under merge-on-read an UPDATE appends a row version rather
/// than masking-and-rewriting, so DV behaviour needs a non-versioned subject.
pub async fn insert_dormant_at(client: &tokio_postgres::Client, id: &str, ts_micros: i64) -> Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO mor_dormant (project_id, date, timestamp, id, name, status_code, level) \
         VALUES ($1, '{}', '{}', $2, 'span', 'OK', 'INFO')",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&"e2e_project", &id]).await?;
    Ok(())
}
