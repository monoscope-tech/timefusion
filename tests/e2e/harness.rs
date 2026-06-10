//! E2E test harness: dynamic MinIO container, full bootstrap, virtual clock,
//! pgwire client. Mirrors prod `main.rs` via `timefusion::bootstrap`.

#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
use datafusion_postgres::ServerOptions;
use rand::RngExt;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;
use timefusion::{
    bootstrap::{self, Bootstrapped},
    buffered_write_layer::BufferedWriteLayer,
    clock,
    config::AppConfig,
};
use tokio::sync::Notify;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

pub const FROZEN_START_MICROS: i64 = 1_900_000_000_000_000; // ~2030-03-15

#[derive(Clone)]
pub struct E2eEnvBuilder {
    bucket_duration_secs:   u64,
    flush_interval_secs:    u64,
    eviction_interval_secs: u64,
    retention_mins:         u64,
    foyer_disabled:         bool,
    flush_immediately:      bool,
    max_memory_mb:          usize,
    frozen_at_micros:       i64,
}

impl Default for E2eEnvBuilder {
    fn default() -> Self {
        Self {
            // Aggressive defaults for fast deterministic tests.
            bucket_duration_secs:   60,
            flush_interval_secs:    1,
            eviction_interval_secs: 1,
            retention_mins:         5,
            foyer_disabled:         false,
            flush_immediately:      false,
            max_memory_mb:          256,
            frozen_at_micros:       FROZEN_START_MICROS,
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

    pub async fn start(self) -> Result<E2eEnv> {
        timefusion::test_utils::init_test_logging();

        // Freeze clock BEFORE bootstrap so background tasks see test time.
        clock::set_micros(self.frozen_at_micros);

        let minio = MinIO::default().start().await.context("start MinIO container")?;
        let host = minio.get_host().await.context("get MinIO host")?.to_string();
        let port = minio.get_host_port_ipv4(9000).await.context("get MinIO port")?;
        let endpoint = format!("http://{host}:{port}");

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

        // walrus-rust reads WALRUS_DATA_DIR from process env. Point it at
        // the same path the WAL itself is opened at (`cfg.core.wal_dir()` =
        // `<data_dir>/wal`) so both views agree. Without this, parallel
        // tests would replay each other's WAL on bootstrap; with it, each
        // test sees only its own WAL after restart.
        let test_wal_dir = data_dir.join("wal");
        std::fs::create_dir_all(&test_wal_dir).ok();
        // SAFETY: e2e suite runs with --test-threads=1, so no other thread
        // reads/writes WALRUS_DATA_DIR concurrently.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", &test_wal_dir) };

        // Bucket creation: MinIO default credentials are minioadmin/minioadmin.
        create_bucket(&endpoint, &bucket).await.context("create MinIO bucket")?;

        let pg_port = 5500 + rand::rng().random_range(1..400) as u16;
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
            test_id: &test_id,
        });

        let bootstrapped = bootstrap::bootstrap(Arc::clone(&cfg)).await.context("bootstrap")?;

        // Pre-warm the default tenant table (matches integration_test pattern).
        bootstrapped.db.get_or_create_table("e2e_project", "otel_logs_and_spans").await.context("pre-warm table")?;

        // Spawn pgwire server. Shutdown via Notify (same as integration_test).
        let pg_shutdown = Arc::new(Notify::new());
        spawn_pgwire(Arc::clone(&bootstrapped.session_ctx), pg_port, Arc::clone(&pg_shutdown));
        wait_for_pg(pg_port).await.context("pgwire never came up")?;

        Ok(E2eEnv {
            _minio: minio,
            data_dir,
            pg_port,
            pg_shutdown,
            bootstrapped: Some(bootstrapped),
            bucket,
            endpoint,
            test_id,
            wal_dir: test_wal_dir,
            builder: self,
        })
    }
}

pub struct E2eEnv {
    _minio:       ContainerAsync<MinIO>,
    pub data_dir: PathBuf,
    pub pg_port:  u16,
    pub bucket:   String,
    endpoint:     String,
    test_id:      String,
    wal_dir:      PathBuf,
    builder:      E2eEnvBuilder,
    pg_shutdown:  Arc<Notify>,
    bootstrapped: Option<Bootstrapped>,
}

impl E2eEnv {
    pub fn builder() -> E2eEnvBuilder {
        E2eEnvBuilder::default()
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
        drop(prev);
        // Give the pgwire accept loop a moment to release the port.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Re-point walrus to our preserved WAL dir before re-bootstrapping.
        // SAFETY: --test-threads=1 — no concurrent reader/writer of this var.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", &self.wal_dir) };

        let pg_port = 5500 + rand::rng().random_range(1..400) as u16;
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
            test_id: &self.test_id,
        });

        let bootstrapped = bootstrap::bootstrap(Arc::clone(&cfg)).await.context("re-bootstrap")?;
        bootstrapped.db.get_or_create_table("e2e_project", "otel_logs_and_spans").await.context("pre-warm table")?;

        self.pg_shutdown = Arc::new(Notify::new());
        spawn_pgwire(Arc::clone(&bootstrapped.session_ctx), pg_port, Arc::clone(&self.pg_shutdown));
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
        clock::advance_micros(delta.as_micros() as i64)
    }

    /// Force-run a full flush immediately and synchronously. Returns
    /// `FlushStats` so tests can assert on what happened.
    pub async fn force_flush(&self) -> Result<timefusion::buffered_write_layer::FlushStats> {
        self.buffered_layer().flush_all_now().await
    }

    pub async fn force_evict(&self) -> Result<()> {
        self.buffered_layer().force_evict_now().await
    }

    /// Wait for the next flush-task iteration to complete (success or
    /// failure). Caller MUST call this BEFORE the action that triggers the
    /// flush (otherwise the notify can fire before we register interest).
    pub async fn await_next_flush(&self, timeout: Duration) -> Result<()> {
        let notify = self.buffered_layer().flush_tick_notify();
        tokio::time::timeout(timeout, notify.notified())
            .await
            .map_err(|_| anyhow::anyhow!("flush tick did not fire within {:?}", timeout))?;
        Ok(())
    }

    pub async fn await_next_eviction(&self, timeout: Duration) -> Result<()> {
        let notify = self.buffered_layer().eviction_tick_notify();
        tokio::time::timeout(timeout, notify.notified())
            .await
            .map_err(|_| anyhow::anyhow!("eviction tick did not fire within {:?}", timeout))?;
        Ok(())
    }

    pub fn snapshot_stats(&self) -> timefusion::buffered_write_layer::StatsSnapshot {
        self.buffered_layer().snapshot_stats()
    }

    /// Foyer hit/miss/size snapshot. Returns `None` if Foyer was disabled
    /// via builder. Tests use this to assert cache warmth post-flush.
    pub async fn foyer_stats(&self) -> Option<timefusion::object_store_cache::CombinedCacheStats> {
        let cache = self.db().object_store_cache()?;
        Some(cache.get_stats().await)
    }
}

impl Drop for E2eEnv {
    fn drop(&mut self) {
        self.pg_shutdown.notify_one();
        // Unfreeze so we don't leak state into the next test in this binary.
        clock::unfreeze();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        // Background tasks tied to the buffered layer hold strong refs into
        // it; we can't `await shutdown()` from Drop, so we rely on tokio
        // task cancellation when the test's runtime tears down. This
        // mirrors a hard kill, which is exactly what we want for a test
        // that explicitly calls restart().
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

struct BuildCfgArgs<'a> {
    endpoint:               &'a str,
    bucket:                 &'a str,
    data_dir:               PathBuf,
    pg_port:                u16,
    bucket_duration_secs:   u64,
    flush_interval_secs:    u64,
    eviction_interval_secs: u64,
    retention_mins:         u64,
    foyer_disabled:         bool,
    flush_immediately:      bool,
    max_memory_mb:          usize,
    test_id:                &'a str,
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
    cfg.buffer.timefusion_eviction_interval_secs = args.eviction_interval_secs;
    cfg.buffer.timefusion_buffer_retention_mins = args.retention_mins;
    cfg.buffer.timefusion_bucket_duration_secs = args.bucket_duration_secs;
    cfg.buffer.timefusion_buffer_max_memory_mb = args.max_memory_mb;
    cfg.buffer.timefusion_flush_immediately = args.flush_immediately;
    cfg.cache.timefusion_foyer_disabled = args.foyer_disabled;
    Arc::new(cfg)
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

fn spawn_pgwire(session_ctx: Arc<datafusion::execution::context::SessionContext>, port: u16, shutdown: Arc<Notify>) {
    tokio::spawn(async move {
        let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
        let auth = timefusion::pgwire_handlers::AuthConfig {
            username: "postgres".into(),
            password: Some("postgres".into()),
        };
        tokio::select! {
            _ = shutdown.notified() => {},
            res = timefusion::pgwire_handlers::serve_with_logging(session_ctx, &opts, auth, None, std::future::pending::<()>()) => {
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
