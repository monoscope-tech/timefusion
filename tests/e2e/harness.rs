//! E2E test harness: MinIO, full bootstrap, virtual clock, pgwire client.
//! Mirrors prod `main.rs` via `timefusion::bootstrap`.

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

/// MinIO release with atomic conditional PUT support (older ones can overwrite
/// racing Delta commits). Must come from quay.io — this tag is not on Docker Hub.
pub const MINIO_IMAGE: &str = "quay.io/minio/minio";
pub const MINIO_TAG: &str = "RELEASE.2025-09-07T16-13-09Z";

pub fn pinned_minio_image() -> GenericImage {
    GenericImage::new(MINIO_IMAGE, MINIO_TAG).with_wait_for(WaitFor::message_on_stderr("API:"))
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
    unordered_leg_sort_max_mb: Option<u64>,
    repair_resume: bool,
    landed_skip: bool,
    mark_sorted_at_write: bool,
    heavy_query_admission: bool,
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
            unordered_leg_sort_max_mb: None,
            use_deletion_vectors: true,
            dml_merge_key_prune: true,
            tantivy_prefilter: true,
            // 0 = synchronous DML; >0 exercises the coalescer defer/drain path.
            dml_coalesce_secs: 0,
            page_row_count_limit: None,
            repair_resume: false,
            landed_skip: false,
            mark_sorted_at_write: true,
            heavy_query_admission: false,
        }
    }
}

/// Consuming `self`-returning setters that take no argument (a flag flip).
macro_rules! flag_setters {
    ($( $(#[$m:meta])* $name:ident => $field:ident = $val:expr ),* $(,)?) => {$(
        $(#[$m])*
        pub fn $name(mut self) -> Self { self.$field = $val; self }
    )*};
}

/// Consuming `self`-returning setters that take one argument; `$val` is the
/// stored expression, so `.max(1)` / `Some(..)` transforms stay at the site.
macro_rules! value_setters {
    ($( $(#[$m:meta])* $name:ident($arg:ident : $ty:ty) => $field:ident = $val:expr ),* $(,)?) => {$(
        $(#[$m])*
        pub fn $name(mut self, $arg: $ty) -> Self { self.$field = $val; self }
    )*};
}

impl E2eEnvBuilder {
    value_setters! {
        with_bucket_duration(d: Duration) => bucket_duration_secs = d.as_secs().max(1),
        with_flush_interval(d: Duration) => flush_interval_secs = d.as_secs().max(1),
        with_eviction_interval(d: Duration) => eviction_interval_secs = d.as_secs().max(1),
        with_retention(d: Duration) => retention_mins = (d.as_secs() / 60).max(1),
        /// Shrink the in-process sort budget (in-memory Arrow bytes) so a test can
        /// reproduce a bin that exceeds it.
        with_sort_skip_bytes(bytes: usize) => sort_skip_bytes = Some(bytes),
        /// Shrink the hot-tail compaction target so a test-sized file counts as
        /// "converged" (>= 7/8 of target).
        with_light_optimize_target(bytes: i64) => light_optimize_target_size = Some(bytes),
        /// Shrink the wide-scan file budget so a test-sized file set trips the
        /// admission gate.
        with_wide_scan_max_files(files: usize) => wide_scan_max_files = Some(files),
        /// Budget for `repair_isolated_scan_ordering`; 0 turns the repair off.
        with_unordered_leg_sort_max_mb(mb: u64) => unordered_leg_sort_max_mb = Some(mb),
        with_wide_scan_max_mb(mb: u64) => wide_scan_max_mb = Some(mb),
        with_max_memory_mb(mb: usize) => max_memory_mb = mb,
        with_frozen_at(micros: i64) => frozen_at_micros = micros,
        with_checkpoint_interval(n: u64) => checkpoint_interval = n,
        /// Force small parquet data pages (row-count capped) so a few hundred rows
        /// yield many pages within one row group — exercises page-index pruning.
        with_page_row_count_limit(rows: usize) => page_row_count_limit = Some(rows),
        with_dml_merge_key_prune(on: bool) => dml_merge_key_prune = on,
        /// The tantivy scan prefilter. Off makes the Delta leg's file list
        /// independent of whether the sidecar index has finished building — a flush
        /// spawns that as a detached task, so it is otherwise an unawaitable race.
        with_tantivy_prefilter(on: bool) => tantivy_prefilter = on,
        /// Defer `UPDATE ... FROM` Delta legs through the coalescer; drain
        /// explicitly with `E2eEnv::drain_dml_coalescer`. 0 = synchronous.
        with_dml_coalesce_secs(secs: u64) => dml_coalesce_secs = secs,
    }

    flag_setters! {
        /// Leave flush output UNMARKED so its files stay footer-repair suspects;
        /// with write-time marking on, a fixture flushed from sorted rows has no
        /// repair work and a repair test silently no-ops.
        /// (`with_repair_resume` commits staged-but-uncommitted repair parquet at
        /// boot instead of deleting it; off by default, so a resume test must set it.)
        without_write_time_sort_marking => mark_sorted_at_write = false,
        /// Gate spilling-sort queries through the pool-derived heavy-query
        /// semaphore (the pgwire-path admission rule); off in prod by default.
        with_heavy_query_admission => heavy_query_admission = true,
        with_repair_resume => repair_resume = true,
        /// Decline a flush whose rows are provably already committed.
        with_landed_skip => landed_skip = true,
        without_light_optimize => light_optimize_enabled = false,
        with_foyer_enabled => foyer_disabled = false,
        with_foyer_disabled => foyer_disabled = true,
        with_flush_immediately => flush_immediately = true,
        with_optimize_sort_by => optimize_sort_by = true,
        /// Warm freshly-flushed file BODIES (not just footers) into Foyer.
        with_warm_full_files => warm_full_files = true,
        with_deletion_vectors => use_deletion_vectors = true,
        without_deletion_vectors => use_deletion_vectors = false,
    }

    pub async fn start(self) -> Result<E2eEnv> {
        timefusion::support::init_test_logging();

        // Freeze clock BEFORE bootstrap so background tasks see test time.
        support::set_micros(self.frozen_at_micros);

        let (minio, endpoint) = ensure_local_minio().await?;

        let test_id = Uuid::new_v4().to_string()[..8].to_string();
        let bucket = format!("e2e-{test_id}");
        let data_dir = std::env::temp_dir().join(format!("timefusion-e2e-{test_id}"));
        // Wipe before create: a leftover WAL dir from a prior test in the same
        // /tmp outlives WAL gc and trips the version stamp check.
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).ok();

        // `<data_dir>/wal` is this test's WAL — nothing process-global, so
        // concurrent tests cannot replay each other's WAL.
        std::fs::create_dir_all(data_dir.join("wal")).ok();

        create_bucket(&endpoint, &bucket).await.context("create MinIO bucket")?;

        let (pg_listener, pg_port) = bind_pg_listener().await?;
        let cfg = build_config(&self, &endpoint, &bucket, data_dir.clone(), pg_port, &test_id);

        let bootstrapped = bootstrap(Arc::clone(&cfg)).await.context("bootstrap")?;

        bootstrapped.db.get_or_create_table("e2e_project", "otel_logs_and_spans").await.context("pre-warm table")?;

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

    /// Buckets short enough to seal inside a test and retention short enough that
    /// `force_flush`/`force_evict` move rows into Delta, plus a pgwire client.
    pub async fn short_buckets() -> Result<(E2eEnv, Client)> {
        let env = Self::builder().with_bucket_duration(Duration::from_secs(60)).with_retention(Duration::from_secs(120)).start().await?;
        let client = env.pg_client().await?;
        Ok((env, client))
    }

    /// Flush sort budget for the NEXT `restart()` — toggling it mid-test builds a
    /// partition holding both poisoned and sorted-but-untagged files.
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

    /// Simulate a hard kill (no graceful flush) and re-bootstrap against the same
    /// bucket + data_dir, so WAL replay must restore any unflushed rows. Uses
    /// `crash_for_test`, not `shutdown`, which would drain MemBuffer into Delta.
    pub async fn restart(&mut self) -> Result<()> {
        let prev = self.bootstrapped.take().expect("already shut down");
        prev.buffered_layer.crash_for_test().await;
        self.pg_shutdown.notify_one();
        // Retire the old instance's background work: preload/warm tasks hold their
        // own Arc<Database>, so dropping `prev` alone leaves their Foyer fetches
        // live (see Drop). This never drains MemBuffer or advances the WAL cursor,
        // so crash semantics hold.
        let _ = prev.db.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(10)).await;
        drop(prev);

        let (pg_listener, pg_port) = bind_pg_listener().await?;
        let cfg = build_config(&self.builder, &self.endpoint, &self.bucket, self.data_dir.clone(), pg_port, &self.test_id);

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

    /// Advance the virtual clock by `delta`. Awaits no background work — pair with
    /// `await_next_flush` / `await_next_eviction` for assertions.
    pub fn advance(&self, delta: Duration) -> i64 {
        support::advance_micros(delta.as_micros() as i64)
    }

    /// Force-run a full flush immediately and synchronously.
    pub async fn force_flush(&self) -> Result<timefusion::write::FlushStats> {
        self.buffered_layer().flush_all_now().await
    }

    pub async fn force_evict(&self) -> Result<()> {
        self.buffered_layer().force_evict_now().await
    }

    /// Commit MemBuffer to Delta and drop it, so what follows reads from Delta
    /// (the merge scan) rather than the mem leg.
    pub async fn flush_and_evict(&self) -> Result<()> {
        self.force_flush().await?;
        self.force_evict().await
    }

    /// Drain the DML coalescer synchronously (runs the deferred Delta-leg
    /// merges now). No-op when coalescing is disabled (secs = 0).
    pub async fn drain_dml_coalescer(&self) {
        if let Some(c) = self.db().dml_coalescer() {
            c.drain(self.db()).await;
        }
    }

    /// Wait for the next flush-task iteration (success or failure). Caller MUST
    /// call this BEFORE the triggering action, or the notify can fire first.
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

    /// Foyer hit/miss/size snapshot; `None` when Foyer is disabled.
    pub async fn foyer_stats(&self) -> Option<timefusion::storage::CombinedCacheStats> {
        let cache = self.db().object_store_cache()?;
        Some(cache.get_stats().await)
    }
}

impl Drop for E2eEnv {
    fn drop(&mut self) {
        self.pg_shutdown.notify_one();
        // Must tear down while the runtime is still alive: leaving it to
        // Runtime::drop deadlocks, because foyer's get_or_fetch drops its fetch
        // future inline on a shutting-down runtime and re-locks the inflight mutex
        // it already holds. block_in_place is fine — every e2e test is multi_thread.
        if let Some(b) = self.bootstrapped.take() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let _ = tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(b.db.shutdown_by(deadline)));
        }
        support::unfreeze();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

// helpers

fn build_config(b: &E2eEnvBuilder, endpoint: &str, bucket: &str, data_dir: PathBuf, pg_port: u16, test_id: &str) -> Arc<AppConfig> {
    let mut cfg = AppConfig::default();
    cfg.aws.aws_s3_bucket = Some(bucket.to_string());
    cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
    cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
    cfg.aws.aws_s3_endpoint = endpoint.to_string();
    cfg.aws.aws_default_region = Some("us-east-1".to_string());
    cfg.aws.aws_allow_http = Some("true".to_string());
    cfg.core.timefusion_table_prefix = format!("e2e-{test_id}");
    cfg.core.timefusion_data_dir = data_dir;
    cfg.core.pgwire_port = pg_port;
    cfg.buffer.timefusion_flush_interval_secs = b.flush_interval_secs;
    // Dwell off: e2e tests drive flushing with advance()+force_flush.
    cfg.buffer.timefusion_flush_dwell_secs = 0;
    cfg.buffer.timefusion_eviction_interval_secs = b.eviction_interval_secs;
    cfg.buffer.timefusion_buffer_retention_mins = b.retention_mins;
    cfg.buffer.timefusion_bucket_duration_secs = b.bucket_duration_secs;
    cfg.buffer.timefusion_buffer_max_memory_mb = b.max_memory_mb;
    cfg.buffer.timefusion_flush_immediately = b.flush_immediately;
    cfg.cache.timefusion_foyer_disabled = b.foyer_disabled;
    cfg.parquet.timefusion_checkpoint_interval = b.checkpoint_interval;
    cfg.maintenance.timefusion_optimize_sort_by = b.optimize_sort_by;
    cfg.maintenance.timefusion_light_optimize_enabled = b.light_optimize_enabled;
    cfg.maintenance.timefusion_use_deletion_vectors = b.use_deletion_vectors;
    cfg.maintenance.timefusion_warm_full_files = b.warm_full_files;
    cfg.maintenance.timefusion_repair_resume_enabled = b.repair_resume;
    cfg.buffer.timefusion_landed_skip_enabled = b.landed_skip;
    cfg.maintenance.timefusion_repair_mark_sorted_at_write = b.mark_sorted_at_write;
    cfg.maintenance.timefusion_dml_merge_key_prune = b.dml_merge_key_prune;
    // A 0% selectivity floor is the off switch for the WHOLE prefilter. Clearing
    // `timefusion_tantivy_file_pruning` alone is NOT enough: a zero-hit index
    // still yields an empty `id IN ()`, which prunes every file.
    if !b.tantivy_prefilter {
        cfg.tantivy.timefusion_tantivy_prefilter_min_selectivity_pct = 0;
    }
    cfg.buffer.timefusion_dml_coalesce_secs = b.dml_coalesce_secs;
    if let Some(v) = b.sort_skip_bytes {
        cfg.maintenance.timefusion_sort_skip_bytes = v;
    }
    if let Some(v) = b.light_optimize_target_size {
        cfg.maintenance.timefusion_light_optimize_target_size = v;
    }
    if let Some(v) = b.page_row_count_limit {
        cfg.parquet.timefusion_page_row_count_limit = v;
    }
    if let Some(v) = b.wide_scan_max_files {
        cfg.memory.timefusion_wide_scan_max_files = v;
    }
    if let Some(v) = b.unordered_leg_sort_max_mb {
        cfg.memory.timefusion_read_sort_unordered_leg_max_mb = v;
    }
    if let Some(v) = b.wide_scan_max_mb {
        cfg.memory.timefusion_wide_scan_max_mb = v;
    }
    cfg.memory.timefusion_heavy_query_admission = b.heavy_query_admission;
    Arc::new(cfg)
}

/// Local-first MinIO resolution, mirroring the sqllogictest harness:
///   1. `TIMEFUSION_TEST_S3_ENDPOINT` if set (CI's MinIO, or any hand-run one).
///   2. An already-running MinIO on 127.0.0.1:9000 (e.g. `make minio-start`).
///   3. The local `minio` binary — spawned DETACHED on :9000 and left running,
///      since a per-test kill would tear it out from under parallel siblings.
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
        // Concurrent first-run races are fine: losers' binds fail and the health
        // loop below waits for whichever sibling won.
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
/// Never pick from a fixed port window — parallel test processes collide.
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

/// Insert one row into `mor_dormant`, a non-versioned table (unlike
/// `otel_logs_and_spans`, where an UPDATE appends a row version instead of
/// masking-and-rewriting) — deletion-vector tests need it as their subject.
pub async fn insert_dormant_at(client: &tokio_postgres::Client, id: &str, ts_micros: i64) -> Result<()> {
    insert_dormant_named(client, id, ts_micros, "span").await
}

/// Like [`insert_dormant_at`] but with an explicit `name`: a row sharing a dedup
/// KEY but differing in content gets past the ingest-time content-identity filter,
/// which would drop an exact re-send before it became a physical duplicate.
pub async fn insert_dormant_named(client: &tokio_postgres::Client, id: &str, ts_micros: i64, name: &str) -> Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap();
    let sql = format!(
        "INSERT INTO mor_dormant (project_id, date, timestamp, id, name, status_code, level) \
         VALUES ($1, '{}', '{}', $2, '{name}', 'OK', 'INFO')",
        dt.date_naive(),
        dt.format("%Y-%m-%d %H:%M:%S%.f"),
    );
    client.execute(&sql, &[&"e2e_project", &id]).await?;
    Ok(())
}
