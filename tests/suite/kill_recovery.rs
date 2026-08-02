//! SIGKILL durability suite — reproduction of the 2026-07-27 acked-write loss.
//!
//! Incident: TimeFusion was OOM-killed (exit 137) at 01:45:25Z. Spans for
//! 01:36–01:44Z were produced to Kafka, consumed, offsets committed, and NO
//! dead-letter message was ever emitted — yet ~200k rows across 10 tenants are
//! absent from Delta. Every layer reported success; the rows are simply gone.
//!
//! Why the existing `tests/e2e/restart_recovery.rs` cannot catch this: its
//! `E2eEnv::restart` calls `crash_for_test`, which cancels background tasks and
//! then DROPS the buffered layer / walrus handles in-process. Drop impls run —
//! flushing writer state, persisting cursors — none of which a `kill -9` grants.
//! Those tests therefore assert durability of a shutdown path prod never takes.
//!
//! This suite spawns the REAL `timefusion` binary as a child process, drives it
//! over pgwire, and `SIGKILL`s it. The invariant under test is the only one that
//! matters for the incident:
//!
//!   **If an INSERT returned success to the client, the rows MUST be queryable
//!   after a SIGKILL + restart.**
//!
//! Anything less is silent data loss that no producer-side DLQ can catch,
//! because the producer was told the write succeeded.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

const LOCAL_MINIO: &str = "127.0.0.1:9000";
const PROJECT: &str = "kill_test";

// ---------------------------------------------------------------- MinIO setup

/// An unused localhost port: bind :0, read the assignment, release it. The
/// window between release and the child's bind is small enough in practice, and
/// unlike a fixed port it cannot collide with a concurrent test or a socket the
/// previous incarnation has not finished releasing.
fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

async fn port_open(addr: &str) -> bool {
    tokio::net::TcpStream::connect(addr).await.is_ok()
}

/// Local-first MinIO, same resolution order as the sqllogictest harness:
/// explicit endpoint → already-running :9000 → spawn the local `minio` binary.
/// Docker is deliberately NOT a fallback here: this suite kills processes and
/// needs a stable endpoint across restarts. Whichever MinIO answers must
/// implement conditional PUT (`If-None-Match: *`) — Delta commit versions are
/// only atomic because of it, and a pre-2024 MinIO turns two racing commits
/// into a silent overwrite (see `e2e::harness::MINIO_TAG`).
async fn ensure_minio() -> Result<String> {
    if let Ok(ep) = std::env::var("TIMEFUSION_TEST_S3_ENDPOINT") {
        return Ok(ep);
    }
    if port_open(LOCAL_MINIO).await {
        return Ok(format!("http://{LOCAL_MINIO}"));
    }
    let has_minio = Command::new("minio").arg("--version").output().map(|o| o.status.success()).unwrap_or(false);
    anyhow::ensure!(has_minio, "no MinIO: set TIMEFUSION_TEST_S3_ENDPOINT, run `make minio-start`, or install the `minio` binary");
    let data_dir = std::env::temp_dir().join("timefusion-kill-minio");
    std::fs::create_dir_all(&data_dir).ok();
    Command::new("minio")
        .args(["server", data_dir.to_str().unwrap(), "--address", LOCAL_MINIO])
        .env("MINIO_ROOT_USER", "minioadmin")
        .env("MINIO_ROOT_PASSWORD", "minioadmin")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("spawn local minio")?;
    for _ in 0..100 {
        if port_open(LOCAL_MINIO).await {
            return Ok(format!("http://{LOCAL_MINIO}"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("local minio never came up on {LOCAL_MINIO}")
}

async fn create_bucket(endpoint: &str, bucket: &str) -> Result<()> {
    use aws_sdk_s3::config::{Credentials, Region};
    let conf = aws_sdk_s3::config::Builder::new()
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("minioadmin", "minioadmin", None, None, "kill"))
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .behavior_version(aws_config::BehaviorVersion::latest())
        .build();
    match aws_sdk_s3::Client::from_conf(conf).create_bucket().bucket(bucket).send().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = format!("{e:?}");
            anyhow::ensure!(msg.contains("BucketAlreadyOwnedByYou") || msg.contains("BucketAlreadyExists"), "create_bucket: {msg}");
            Ok(())
        }
    }
}

// ------------------------------------------------------------- TF subprocess

/// Knobs that shape the crash window under test. Defaults give the plain
/// "acked into WAL+MemBuffer, nothing flushed yet" case.
#[derive(Clone)]
struct TfOpts {
    /// Long by default so nothing flushes behind our back: the WAL is then the
    /// sole durability mechanism, which is precisely the invariant under test.
    flush_interval_secs: u64,
    buffer_max_memory_mb: usize,
    /// Extra env applied last — lets a case turn on pressure/backpressure paths.
    extra_env: Vec<(String, String)>,
}

impl Default for TfOpts {
    fn default() -> Self {
        Self { flush_interval_secs: 3600, buffer_max_memory_mb: 512, extra_env: Vec::new() }
    }
}

struct Tf {
    child: Option<Child>,
    port: u16,
    data_dir: PathBuf,
    bucket: String,
    prefix: String,
    endpoint: String,
    /// Child stdout capture (`.stderr.log` sibling for stderr) — see `spawn`.
    boot_log: PathBuf,
    opts: TfOpts,
}

impl Tf {
    async fn start(test_name: &str, opts: TfOpts) -> Result<Self> {
        let endpoint = ensure_minio().await?;
        let id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        // Per-run bucket, not a shared one: the e2e/sqllogictest suites reset
        // MinIO, which silently deletes buckets out from under a concurrent
        // suite (see CLAUDE.md). A unique prefix does not protect against that;
        // a unique bucket does. This is why CI's parallel shards saw these
        // tests fail while they passed locally.
        let bucket = format!("timefusion-kill-{id}");
        create_bucket(&endpoint, &bucket).await?;
        let prefix = format!("kill-{test_name}-{id}");
        let data_dir = std::env::temp_dir().join(format!("tf-kill-{test_name}-{id}"));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(data_dir.join("wal")).ok();
        let boot_log = data_dir.join("boot.log");
        let mut tf = Self { child: None, port: free_port()?, data_dir, bucket, prefix, endpoint, boot_log, opts };
        tf.spawn().await?;
        Ok(tf)
    }

    /// (Re)spawn the real binary against the SAME data dir + table prefix, so a
    /// restart sees the previous incarnation's WAL exactly as prod does.
    async fn spawn(&mut self) -> Result<()> {
        // A fresh port every spawn. A hash-derived fixed port collided between
        // concurrent tests, and re-binding the same port right after SIGKILL hit
        // EADDRINUSE while the dead process's socket lingered — both surfaced as
        // "never became ready", not as a durability failure.
        self.port = free_port()?;
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_timefusion"));
        cmd.env("AWS_S3_ENDPOINT", &self.endpoint)
            .env("AWS_ENDPOINT_URL", &self.endpoint)
            .env("AWS_S3_BUCKET", &self.bucket)
            .env("AWS_ACCESS_KEY_ID", "minioadmin")
            .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_ALLOW_HTTP", "true")
            .env("AWS_S3_LOCKING_PROVIDER", "")
            .env_remove("AWS_ENDPOINT_URL_DYNAMODB")
            .env("TIMEFUSION_TABLE_PREFIX", &self.prefix)
            .env("TIMEFUSION_DATA_DIR", &self.data_dir)
            .env("WALRUS_DATA_DIR", self.data_dir.join("wal"))
            .env("PGWIRE_PORT", self.port.to_string())
            .env("TIMEFUSION_FLUSH_INTERVAL_SECS", self.opts.flush_interval_secs.to_string())
            .env("TIMEFUSION_BUFFER_MAX_MEMORY_MB", self.opts.buffer_max_memory_mb.to_string())
            .env("TIMEFUSION_FOYER_DISABLED", "true")
            // Explicit password, matching connect()'s `password=postgres`. Locally
            // the binary's dotenv() found the repo .env (PGWIRE_PASSWORD=postgres)
            // and auth happened to line up; on CI there is no .env, so insecure
            // mode expected an EMPTY password and rejected the harness every
            // 100ms for 300s — every "never became ready" since the suite landed.
            .env("PGWIRE_PASSWORD", "postgres")
            .env("TIMEFUSION_ALLOW_INSECURE_AUTH", "true")
            .env("RUST_LOG", "warn,timefusion=info")
            // Capture BOTH streams to a per-spawn file: tracing writes to
            // stdout, so nulling it made every CI "never became ready" failure
            // blind — five tests red since the suite landed with zero boot logs
            // to read. The tail is surfaced in the wait_ready failure.
            .stdout(Stdio::from(std::fs::File::create(&self.boot_log)?))
            .stderr(Stdio::from(std::fs::File::create(self.boot_log.with_extension("stderr.log"))?));
        for (k, v) in &self.opts.extra_env {
            cmd.env(k, v);
        }
        self.child = Some(cmd.spawn().context("spawn timefusion binary")?);
        self.wait_ready().await
    }

    async fn wait_ready(&self) -> Result<()> {
        // 60s was enough locally but not on CI, where four test shards, MinIO
        // and a debug-build TimeFusion contend for one runner: shard 2 failed at
        // 63.8s having spawned fine. Boot here is dominated by Delta/MinIO
        // round-trips, so wait generously — a real failure to start still fails,
        // just later.
        let attempts = if std::env::var_os("CI").is_some() { 3000 } else { 600 };
        for _ in 0..attempts {
            if port_open(&format!("127.0.0.1:{}", self.port)).await && self.connect().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let tail = |p: &std::path::Path| {
            std::fs::read_to_string(p)
                .map(|c| {
                    c.lines().rev().take(40).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>().join(
                        "
",
                    )
                })
                .unwrap_or_default()
        };
        anyhow::bail!(
            "timefusion never became ready on port {}
--- child stdout (last 40 lines) ---
{}
--- child stderr ---
{}",
            self.port,
            tail(&self.boot_log),
            tail(&self.boot_log.with_extension("stderr.log"))
        )
    }

    async fn connect(&self) -> Result<Client> {
        let (client, conn) =
            tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres", self.port), NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        Ok(client)
    }

    /// SIGKILL — the faithful OOM. No Drop impls, no flush, no cursor persist.
    fn kill9(&mut self) -> Result<()> {
        let child = self.child.as_mut().context("already killed")?;
        child.kill().context("SIGKILL")?; // std's Child::kill IS SIGKILL on unix
        child.wait().context("reap")?;
        self.child = None;
        Ok(())
    }

    async fn restart(&mut self) -> Result<()> {
        if self.child.is_some() {
            self.kill9()?;
        }
        // Let the OS release the listening socket before rebinding it.
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.spawn().await
    }
}

impl Drop for Tf {
    fn drop(&mut self) {
        if let Some(c) = self.child.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

// ------------------------------------------------------------------- SQL bits

/// One multi-row INSERT. Returns only after the server acked it — which is the
/// precise moment the durability promise is made.
async fn insert_rows(client: &Client, project: &str, tag: &str, n: usize, base_ts: i64) -> Result<()> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(base_ts).unwrap();
    let values: Vec<String> = (0..n)
        .map(|i| {
            format!(
                "('{project}', '{}', '{}', '{tag}-{i}', 'span', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])",
                dt.date_naive(),
                dt.format("%Y-%m-%d %H:%M:%S%.f"),
            )
        })
        .collect();
    let sql = format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) VALUES {}",
        values.join(",")
    );
    client.execute(&sql, &[]).await.map(|_| ()).context("insert")
}

async fn count_rows(client: &Client, project: &str) -> Result<i64> {
    let row = client.query_one(&format!("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = '{project}'"), &[]).await?;
    Ok(row.get::<_, i64>(0))
}

/// Count with a retry window: right after boot the table may still be resolving.
async fn count_after_restart(tf: &Tf, project: &str) -> Result<i64> {
    let mut last = anyhow::anyhow!("no attempt");
    for _ in 0..30 {
        match tf.connect().await {
            Ok(c) => match count_rows(&c, project).await {
                Ok(n) => return Ok(n),
                Err(e) => last = e,
            },
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(last)
}

// ---------------------------------------------------------------- test matrix

/// CASE 1 — the incident in its simplest form. Rows acked into WAL+MemBuffer,
/// nothing flushed, process SIGKILLed. WAL replay must restore every acked row.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn acked_rows_survive_sigkill() -> Result<()> {
    let mut tf = Tf::start("baseline", TfOpts::default()).await?;
    let client = tf.connect().await?;
    let ts = chrono::Utc::now().timestamp_micros();

    const N: usize = 500;
    insert_rows(&client, PROJECT, "base", N, ts).await?;
    let before = count_rows(&client, PROJECT).await?;
    assert_eq!(before, N as i64, "rows not visible even before the kill");

    drop(client);
    tf.kill9()?;
    tf.restart().await?;

    let after = count_after_restart(&tf, PROJECT).await?;
    assert_eq!(after, N as i64, "ACKED WRITE LOST: {N} rows acked, {after} survived SIGKILL — WAL replay did not restore them");
    Ok(())
}

/// CASE 2 — kill while a background flush is in flight. Reproduces the prod
/// shape where an emergency flush was running as the box died: a flush that
/// advances the WAL cursor for a Delta commit that never lands must not strand
/// the entries it claimed.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn acked_rows_survive_sigkill_during_flush() -> Result<()> {
    // 2s flush interval => a flush is near-certainly mid-commit when we kill.
    let mut tf = Tf::start("during-flush", TfOpts { flush_interval_secs: 2, ..Default::default() }).await?;
    let client = tf.connect().await?;
    let ts = chrono::Utc::now().timestamp_micros();

    let mut total = 0usize;
    for round in 0..10 {
        insert_rows(&client, PROJECT, &format!("f{round}"), 200, ts).await?;
        total += 200;
    }
    let before = count_rows(&client, PROJECT).await?;
    assert_eq!(before, total as i64);

    drop(client);
    // Kill without warning, mid flush-cycle.
    tokio::time::sleep(Duration::from_millis(900)).await;
    tf.kill9()?;
    tf.restart().await?;

    let after = count_after_restart(&tf, PROJECT).await?;
    assert_eq!(after, total as i64, "ACKED WRITE LOST across flush+SIGKILL: {total} acked, {after} survived");
    Ok(())
}

/// CASE 3 — kill under memory pressure. Prod was thrashing for ~9 minutes
/// before the OOM; the pressure valve, relief flush and eviction were all live.
/// Every row the server ACKED must still survive, however hard it was squeezed.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn acked_rows_survive_sigkill_under_memory_pressure() -> Result<()> {
    let mut tf = Tf::start("pressure", TfOpts { flush_interval_secs: 3600, buffer_max_memory_mb: 8, ..Default::default() }).await?;
    let client = tf.connect().await?;
    let ts = chrono::Utc::now().timestamp_micros();

    // Push well past the 8MB budget so the pressure path is genuinely engaged.
    // Rows REJECTED with an error are fine (the producer DLQs those); rows the
    // server ACKED are the ones that must survive.
    let mut acked = 0usize;
    for round in 0..40 {
        // An Err here is explicit backpressure, which the producer DLQs — only
        // acked rows carry a durability promise.
        if insert_rows(&client, PROJECT, &format!("p{round}"), 500, ts).await.is_ok() {
            acked += 500;
        }
    }
    assert!(acked > 0, "every insert was rejected; test proves nothing");
    let before = count_rows(&client, PROJECT).await?;
    assert_eq!(before, acked as i64, "server acked {acked} rows but only {before} are readable pre-kill");

    drop(client);
    tf.kill9()?;
    tf.restart().await?;

    let after = count_after_restart(&tf, PROJECT).await?;
    assert_eq!(after, acked as i64, "ACKED WRITE LOST under pressure: {acked} acked, {after} survived SIGKILL");
    Ok(())
}

/// CASE 4 — multi-tenant. Prod lost data across 10 projects at once; WAL topics
/// are sharded per (project, table), so a per-shard cursor/hold bug can strand
/// some tenants while others survive. Single-tenant tests would miss that.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn acked_rows_survive_sigkill_multi_tenant() -> Result<()> {
    let mut tf = Tf::start("multi-tenant", TfOpts::default()).await?;
    let client = tf.connect().await?;
    let ts = chrono::Utc::now().timestamp_micros();

    let projects: Vec<String> = (0..10).map(|i| format!("kill_tenant_{i}")).collect();
    const PER: usize = 100;
    for p in &projects {
        insert_rows(&client, p, "mt", PER, ts).await?;
    }

    drop(client);
    tf.kill9()?;
    tf.restart().await?;

    let mut lost = Vec::new();
    for p in &projects {
        let n = count_after_restart(&tf, p).await?;
        if n != PER as i64 {
            lost.push(format!("{p}: {n}/{PER}"));
        }
    }
    assert!(lost.is_empty(), "ACKED WRITES LOST for tenants across SIGKILL: {lost:?}");
    Ok(())
}

/// CASE 5 — the silent-ack guard. When the server CANNOT durably accept a
/// write it must return an error, never a success tag. This is the contract the
/// producer's DLQ depends on; prod saw zero DLQ traffic while losing 200k rows,
/// so a success-on-failure path anywhere here is fatal by itself.
///
/// Armed by driving the WAL hard-backpressure breaker to its floor.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn rejected_writes_error_and_are_never_silently_acked() -> Result<()> {
    let mut tf = Tf::start(
        "no-silent-ack",
        TfOpts {
            flush_interval_secs: 3600,
            buffer_max_memory_mb: 8,
            // 0GB hard cap => the breaker is armed as soon as any WAL exists.
            extra_env: vec![("TIMEFUSION_WAL_HARD_LIMIT_GB".into(), "0".into())],
        },
    )
    .await?;
    let client = tf.connect().await?;
    let ts = chrono::Utc::now().timestamp_micros();

    let mut acked = 0usize;
    let mut rejected = 0usize;
    for round in 0..30 {
        match insert_rows(&client, PROJECT, &format!("sa{round}"), 200, ts).await {
            Ok(()) => acked += 200,
            Err(_) => rejected += 200,
        }
    }
    // Whatever the split, the readable count must equal exactly what was acked:
    // an ack that produced no row is silent loss; a row from a rejected insert
    // is a phantom write.
    let readable = count_rows(&client, PROJECT).await?;
    assert_eq!(
        readable, acked as i64,
        "ack/persist mismatch: acked={acked}, rejected={rejected}, readable={readable} — a success tag was returned for rows that never landed"
    );

    drop(client);
    tf.kill9()?;
    tf.restart().await?;
    let after = count_after_restart(&tf, PROJECT).await?;
    assert_eq!(after, acked as i64, "ACKED WRITE LOST: acked={acked}, survived={after}");
    Ok(())
}

/// CASE 6 — concurrent inserts from many connections, then kill. Prod ingest is
/// 9 monoscope consumers writing in parallel; the WAL append path shards and
/// takes per-shard locks, so a lost-hold race only shows under real concurrency.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn acked_rows_survive_sigkill_under_concurrent_writers() -> Result<()> {
    let mut tf = Tf::start("concurrent", TfOpts { flush_interval_secs: 5, ..Default::default() }).await?;
    let ts = chrono::Utc::now().timestamp_micros();

    const WRITERS: usize = 8;
    const PER_WRITER: usize = 25;
    const BATCH: usize = 20;

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let client = tf.connect().await?;
        handles.push(tokio::spawn(async move {
            let mut acked = 0usize;
            for i in 0..PER_WRITER {
                if insert_rows(&client, PROJECT, &format!("c{w}-{i}"), BATCH, ts).await.is_ok() {
                    acked += BATCH;
                }
            }
            acked
        }));
    }
    let mut acked = 0usize;
    for h in handles {
        acked += h.await?;
    }
    assert!(acked > 0);

    tf.kill9()?;
    tf.restart().await?;

    let after = count_after_restart(&tf, PROJECT).await?;
    assert_eq!(after, acked as i64, "ACKED WRITE LOST under concurrency: {acked} acked, {after} survived SIGKILL");
    Ok(())
}
