#![recursion_limit = "512"]

// Optional profiling build (--features profiling, Linux): jemalloc as the global
// allocator with its heap profiler, plus a pprof CPU sampler started in async_main.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc reads this symbol at startup, so the profiler config is baked into the
// binary (no MALLOC_CONF env; the host is read-only). Sampling is compiled in but
// inactive — re-arm at runtime via the `prof.active` mallctl.
// `dirty_decay_ms` must stay non-zero: decay 0 madvises every freed page back to
// the kernel, which costs significant CPU in page-fault churn under scan load.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[unsafe(export_name = "malloc_conf")]
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19,lg_prof_interval:35,prof_prefix:/app/data/timefusion/profiles/jeprof,background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000\0";

use std::sync::Arc;

use anyhow::Context;
use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use itertools::Itertools;
use timefusion::{
    config::{self, AppConfig},
    database::{Database, RecompressOutcome},
    observability, server, support,
    write::BufferedWriteLayer,
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

/// Stack size for every Tokio worker. Query planning recurses with schema width
/// and predicate shape, and an overflow aborts the whole process, so Tokio's
/// 2 MiB default is not enough. Reserved lazily: untouched pages cost address
/// space, not RSS.
const WORKER_STACK_BYTES: usize = 32 * 1024 * 1024;
const _: () = assert!(WORKER_STACK_BYTES >= 8 * 2 * 1024 * 1024);

fn main() -> anyhow::Result<()> {
    // Must be the first statement: `timefusion_stats` reports uptime against this.
    timefusion::observability::mark_process_start();
    dotenv().ok();
    // Before the runtime, so every worker thread/listener inherits the raised
    // limit. `bootstrap()` calls it too, for the e2e harness (skips main()).
    server::raise_file_limit();

    let subcommand = std::env::args().nth(1);
    match subcommand.as_deref() {
        Some("healthcheck") => return run_pgwire_healthcheck(),
        Some("encrypt-secret") => return config::run_cli(),
        // Must stay config/bucket-free — that is what lets it run anywhere.
        Some("sim") => return run_sim_cli(),
        _ => {}
    }

    // Maintenance CLIs get the maintenance-heavy budget shape. Must precede
    // init_config, which snapshots the tree. `run-unit` is deliberately excluded:
    // `coordinator_share_bytes()` is 0 under this profile, so its unit gets no pool.
    //
    // SAFETY: no threads exist yet - we're before the Tokio runtime is built.
    if matches!(subcommand.as_deref(), Some("optimize" | "redrive-dml" | "migrate-columns")) {
        unsafe { std::env::set_var("TIMEFUSION_BUDGET_PROFILE", "maintenance-cli") };
    }

    let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().thread_stack_size(WORKER_STACK_BYTES).build()?;
    match subcommand.as_deref() {
        Some("redrive-dml") => rt.block_on(run_redrive_dml_cli(cfg)),
        Some("optimize") => rt.block_on(run_optimize_cli(cfg)),
        Some("migrate-columns") => rt.block_on(run_migrate_columns_cli(cfg)),
        Some("run-unit") => rt.block_on(run_unit_cli(cfg)),
        Some("retention") => rt.block_on(run_retention_cli(cfg)),
        // Must END THE PROCESS here: dropping the runtime waits on lingering
        // blocking/detached threads and can hang forever. Everything durable
        // is already on disk.
        _ => match rt.block_on(async_main(cfg)) {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("fatal: {e:#}");
                std::process::exit(1)
            }
        },
    }
}

/// Docker liveness probe. The intentional early ErrorResponse with SQLSTATE
/// 57P03 is alive enough for Swarm to advance a start-first update; clients and
/// the deployment availability probe still treat it as unavailable. Any other
/// PGWire error remains unhealthy.
fn run_pgwire_healthcheck() -> anyhow::Result<()> {
    let port = std::env::var("TIMEFUSION_PGWIRE_PORT").or_else(|_| std::env::var("PGWIRE_PORT")).ok().and_then(|v| v.parse::<u16>().ok()).unwrap_or(5432);
    pgwire_ready_at(([127, 0, 0, 1], port).into())
}

/// Per-operation deadline for the readiness probe; worst case is 3x this
/// (connect + write + read) and must stay inside the Dockerfile's
/// `HEALTHCHECK --timeout` (pinned by `probe_worst_case_fits_the_docker_timeout`).
/// This is a LIVENESS probe — the handshake shares a runtime with ingest and
/// maintenance, so a sub-second budget is not one a loaded database can hold.
const PROBE_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1500);

/// Connects and reads the first PGWire response byte, printing per-stage timings
/// (connect / write / auth) on both success and failure so the Docker health log
/// reads as a stage histogram rather than a column of bare "unhealthy".
fn pgwire_ready_at(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    let timeout = PROBE_OP_TIMEOUT;
    let t0 = std::time::Instant::now();
    let stage = |t: &mut std::time::Instant| {
        let d = t.elapsed();
        *t = std::time::Instant::now();
        d.as_millis()
    };
    let mut mark = t0;

    let connect = (|| {
        let s = std::net::TcpStream::connect_timeout(&addr, timeout)?;
        s.set_read_timeout(Some(timeout))?;
        s.set_write_timeout(Some(timeout))?;
        Ok::<_, std::io::Error>(s)
    })();
    let connect_ms = stage(&mut mark);
    let mut stream = connect.inspect_err(|e| println!("probe stage=connect ms={connect_ms} result=error err={e}"))?;

    let body = b"user\0timefusion_healthcheck\0database\0postgres\0\0";
    // length | protocol 3.0 | body
    let startup = [&((8 + body.len()) as u32).to_be_bytes()[..], &196_608u32.to_be_bytes()[..], &body[..]].concat();
    let wrote = stream.write_all(&startup);
    let write_ms = stage(&mut mark);
    wrote.inspect_err(|e| println!("probe stage=write connect_ms={connect_ms} ms={write_ms} result=error err={e}"))?;

    // Auth latency exposes server task starvation that connect latency misses.
    let mut tag = [0u8; 1];
    let read = stream.read_exact(&mut tag);
    let auth_ms = stage(&mut mark);
    let total_ms = t0.elapsed().as_millis();
    if let Err(e) = &read {
        println!("probe stage=auth connect_ms={connect_ms} write_ms={write_ms} ms={auth_ms} total_ms={total_ms} result=error err={e}");
    } else {
        println!("probe connect_ms={connect_ms} write_ms={write_ms} auth_ms={auth_ms} total_ms={total_ms} result=ok tag={}", tag[0] as char);
    }
    read?;
    match tag[0] {
        b'R' => Ok(()),
        b'E' => {
            let mut length = [0u8; 4];
            stream.read_exact(&mut length)?;
            let payload_len = u32::from_be_bytes(length).saturating_sub(4) as usize;
            anyhow::ensure!(payload_len <= 64 * 1024, "PGWire ErrorResponse is unreasonably large");
            let mut payload = vec![0; payload_len];
            stream.read_exact(&mut payload)?;
            anyhow::ensure!(payload.windows(7).any(|field| field == b"C57P03\0"), "PGWire returned a non-startup error");
            Ok(())
        }
        other => anyhow::bail!("PGWire returned unexpected response tag {:?}", other as char),
    }
}

/// Argument cursor shared by every subcommand CLI below: `next()` yields the
/// flag, `value`/`parse` pull the token after it.
struct Args(std::iter::Skip<std::env::Args>);

impl Args {
    fn new() -> Self {
        Self(std::env::args().skip(2))
    }

    fn value(&mut self, flag: &str) -> anyhow::Result<String> {
        self.0.next().with_context(|| format!("{flag} needs a value"))
    }

    fn parse<T>(&mut self, flag: &str, what: &str) -> anyhow::Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        self.value(flag)?.parse().with_context(|| format!("{flag} must be {what}"))
    }

    fn hours_micros(&mut self, flag: &str) -> anyhow::Result<i64> {
        Ok((self.parse::<f64>(flag, "a number")? * 3_600_000_000.0) as i64)
    }
}

impl Iterator for Args {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        self.0.next()
    }
}

/// The subcommand parse loop every CLI below repeats: `"--flag" => action` arms
/// plus the one identical unknown-argument bail.
macro_rules! cli_args {
    ($it:expr, $usage:expr, { $($flag:literal => $arm:expr),* $(,)? }) => {
        while let Some(a) = $it.next() {
            match a.as_str() {
                $($flag => $arm,)*
                other => {
                    let usage = $usage;
                    anyhow::bail!("unknown argument: {other} ({usage})")
                }
            }
        }
    };
}

fn init_cli_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .try_init();
}

/// `timefusion sim <journal.json | data-dir | synth:whale> [--hours N]
/// [--workers N] [--streams N] [--scale F] [--seed N] [--no-mint]
/// [--floorless] [--guard-off] [--json]`
///
/// Replay a maintenance journal through the real scheduler on virtual time
/// (`timefusion::maintenance_sim`), to answer "does this policy keep up"
/// without deploying.
fn run_sim_cli() -> anyhow::Result<()> {
    use timefusion::maintenance_sim::{SimConfig, load_sandboxed, run};
    // The sim is config-free by default; install the config only when a rank()
    // kill switch is explicitly set, so an A/B arm can exercise that ordering.
    if std::env::var_os("TIMEFUSION_DEDUP_CONTIGUITY_RANK").is_some() {
        timefusion::config::init_config().map_err(|e| anyhow::anyhow!("kill-switch env set but config failed to load: {e}"))?;
    }
    let mut it = Args::new();
    let usage = "usage: timefusion sim <journal.json|data-dir|synth:whale> [--hours N] [--workers N] [--streams N] [--scale F] [--seed N] [--no-mint] [--mint] [--debris-slice-minutes N] [--floorless] [--guard-off] [--json]";
    let input = it.next().context(usage)?;
    let mut cfg = SimConfig::default();
    let mut json = false;
    let mut floorless = false;
    let mut mint = false;
    let mut debris_slice = 1i64;
    cli_args!(it, usage, {
        "--hours" => cfg.horizon_micros = it.hours_micros("--hours")?,
        "--workers" => cfg.workers = it.parse("--workers", "an integer")?,
        "--streams" => cfg.streams = Some(it.parse("--streams", "an integer")?),
        "--scale" => cfg.duration_scale = it.parse("--scale", "a number")?,
        "--seed" => cfg.seed = u64::from_str_radix(it.value("--seed")?.trim_start_matches("0x"), 16).context("--seed must be hex")?,
        "--restarts-every-hours" => cfg.restart_every_micros = it.hours_micros("--restarts-every-hours")?,
        "--restart-at-hours" => cfg.restart_at_micros = Some(it.hours_micros("--restart-at-hours")?),
        "--no-mint" => cfg.mint_frontier = false,
        // `synth:whale` disables minting by default; `--mint` turns arrivals
        // back on, which is what makes `--streams` mean anything there.
        "--mint" => mint = true,
        // Bin-width axis: same total debris work as `600 / n` units of `n` minutes.
        "--debris-slice-minutes" => debris_slice = it.parse("--debris-slice-minutes", "an integer")?,
        "--floorless" => floorless = true,
        "--guard-off" => cfg.split_guard = timefusion::maintenance_sim::SplitGuard::Off,
        "--json" => json = true,
    });
    let now = support::now_micros();
    // `synth:whale` needs no journal, and is the only input that exercises the
    // byte preflight (a real journal carries estimates the sim never reads).
    let report = if let Some(shape) = input.strip_prefix("synth:") {
        anyhow::ensure!(shape == "whale", "the only synthetic queue is `synth:whale`");
        // `--streams` scales ingesting streams, which a synthetic queue only has
        // when minting is on — otherwise the flag would be silently inert.
        anyhow::ensure!(
            cfg.streams.is_none() || mint,
            "--streams needs arrivals to scale: pass --mint (a synthetic queue disables minting by default), or use a real journal."
        );
        cfg.mint_frontier = mint;
        let queue = timefusion::maintenance_sim::synthetic_whale_queue(now, !floorless, 100, debris_slice);
        cfg.byte_model = Some(queue.model);
        run(queue.journal, &cfg, now)?
    } else {
        let (journal, _sandbox) = load_sandboxed(std::path::Path::new(&input))?;
        run(journal, &cfg, now)?
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    println!(
        "sim: {:.1}h virtual | {} workers | scale {:.2} | {} streams | seed {:#x}",
        report.hours,
        cfg.workers,
        cfg.duration_scale,
        cfg.streams.map_or("journal".to_owned(), |n| n.to_string()),
        cfg.seed
    );
    println!("pending: {} -> {} | executions: {} | splits: {}", report.pending_start, report.pending_end, report.executions, report.splits);
    println!(
        "coarsen: subsumed {} fused {} | candidates {} blocked {} over_budget {}",
        report.coarsen_subsumed, report.coarsen_fused, report.coarsen_candidates, report.coarsen_blocked, report.coarsen_over_budget
    );
    let tally = |counts: &std::collections::HashMap<String, u64>| counts.iter().sorted().map(|(op, n)| format!("{op}={n}")).join(" ");
    println!("completions: {}", tally(&report.completions));
    if !report.timeouts.is_empty() {
        println!("timeouts:    {}", tally(&report.timeouts));
    }
    println!(
        "claims by data age: frontier={} mid_band(3-31d)={} privileged(>31d)={} | day-wide claims={}",
        report.claims_frontier, report.claims_mid_band, report.claims_privileged, report.claims_day_wide
    );
    println!("frontier lag max: {}s", report.frontier_lag_secs_max);
    println!(
        "min contiguous days at end: {} (14d at {}, 30d at {})",
        report.min_contiguous_days_end,
        report.hours_to_contiguous_14.map_or("never".to_owned(), |h| format!("{h:.1}h")),
        report.hours_to_contiguous_30.map_or("never".to_owned(), |h| format!("{h:.1}h"))
    );
    for sample in &report.samples {
        println!("  h={:5.1} pending={:>7} lag={:>6}s contiguous={}", sample.hour, sample.pending, sample.frontier_lag_secs, sample.min_contiguous_days);
    }
    Ok(())
}

/// `timefusion run-unit --project ID [--source TABLE] [--date YYYY-MM-DD]
/// [--op base|derived|dedup|hot|sealed|repair] [--slice-hours N] [--offset-hours N]`
///
/// Execute ONE maintenance unit against the configured storage and print where
/// its time went (scan/stage/commit deltas + wall). Claims only the requested
/// task and preserves unrelated journal entries; normal admission and dependency
/// checks still apply.
async fn run_unit_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    let mut source = "otel_logs_and_spans".to_string();
    let mut project: Option<String> = None;
    let mut date: Option<chrono::NaiveDate> = None;
    let mut operation = timefusion::maintenance_coordinator::Operation::BaseRollup;
    let mut slice_hours: i64 = 24;
    let mut offset_hours: i64 = 0;
    let mut it = Args::new();
    cli_args!(it, "usage: timefusion run-unit --project ID [--source T] [--date D] [--op OP] [--slice-hours N] [--offset-hours N]", {
        "--source" => source = it.value("--source")?,
        "--project" => project = Some(it.value("--project")?),
        "--date" => date = Some(it.parse("--date", "YYYY-MM-DD")?),
        "--slice-hours" => slice_hours = it.parse("--slice-hours", "an integer")?,
        "--offset-hours" => offset_hours = it.parse("--offset-hours", "an integer")?,
        "--op" => {
            use timefusion::maintenance_coordinator::Operation;
            operation = match it.value("--op")?.as_str() {
                "base" => Operation::BaseRollup,
                "derived" => Operation::DerivedRollup,
                "dedup" => Operation::Dedup,
                "hot" => Operation::HotPacking,
                "sealed" => Operation::SealedConsolidation,
                "repair" => Operation::Repair,
                other => anyhow::bail!("unknown --op {other}: base|derived|dedup|hot|sealed|repair"),
            }
        },
    });
    let project = project.context("--project is required")?;
    let date = date.unwrap_or_else(|| support::today_utc() - chrono::Duration::days(1));
    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    // run-unit skips `start_maintenance_schedulers`, so load this explicitly or
    // every invocation re-selects the same already-probed file and never advances.
    db.load_verified_sorted();
    let report = db.run_unit_once(&source, &project, date, operation, slice_hours, offset_hours).await?;
    println!("{report}");
    Ok(())
}

/// `timefusion redrive-dml [--dir PATH] [--dry-run]` — replay parked quarantine/dml
/// enrichment groups (see [`timefusion::dml::redrive_dml_quarantine`]).
async fn run_redrive_dml_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    let mut dir = cfg.core.wal_dir().join(timefusion::write::wal::QUARANTINE_DIR_NAME).join("dml");
    let mut dry_run = false;
    let mut it = Args::new();
    cli_args!(it, "usage: timefusion redrive-dml [--dir PATH] [--dry-run]", {
        "--dir" => dir = it.value("--dir")?.into(),
        "--dry-run" => dry_run = true,
    });
    let db = Arc::new(Database::with_config(Arc::new(cfg.clone())).await?);
    let (ok, skipped) = timefusion::dml::redrive_dml_quarantine(&db, &dir, dry_run).await;
    println!("redrive-dml: {ok} recovered, {skipped} left parked (dir {dir:?})");
    db.shutdown().await
}

/// `s3://<bucket>/<table_prefix>/<kind>` — the tantivy and bloom sidecar roots.
async fn sidecar_store(db: &Database, cfg: &AppConfig, bucket: &str, kind: &str) -> anyhow::Result<Arc<dyn object_store::ObjectStore>> {
    db.create_object_store(&format!("s3://{bucket}/{}/{kind}", cfg.core.timefusion_table_prefix), &cfg.aws.build_storage_options(None)).await
}

async fn async_main(cfg: &'static AppConfig) -> anyhow::Result<()> {
    observability::init_telemetry(&cfg.telemetry)?;
    // Must come AFTER init_telemetry: config is built before the subscriber
    // exists, so logging the budget tree any earlier is silently swallowed.
    cfg.derived.log();
    support::init_from_env();

    // Start heap+CPU profiling (no-op unless --features profiling on Linux).
    // Early, so the profiles dir exists before jemalloc's first interval dump.
    timefusion::observability::start(cfg.core.timefusion_data_dir.clone());

    info!("Starting TimeFusion application");

    let cfg_arc = Arc::new(cfg.clone());

    // Bind the pgwire port before the slow startup work (Database open, WAL
    // recovery). Clients connecting in that window get SQLSTATE 57P03
    // ("starting up") from the early-bind responder instead of ECONNREFUSED,
    // which standard drivers retry on cleanly.
    let pg_opts = ServerOptions::new().with_host("0.0.0.0".to_string()).with_port(cfg.core.pgwire_port);
    let pg_listener = datafusion_postgres::bind_listener(pg_opts.host(), *pg_opts.port(), *pg_opts.backlog()).await?;
    let early_shutdown = tokio_util::sync::CancellationToken::new();
    let early_task = tokio::spawn({
        let shutdown = early_shutdown.clone();
        async move {
            timefusion::server::run_until_ready(&pg_listener, shutdown).await;
            pg_listener
        }
    });

    // Take exclusive ownership of the WAL directory before ANY WAL access (boot
    // GC, recovery, writes). The WAL is single-writer with no cross-process
    // coordination: two live processes on one dir fork it and silently lose the
    // older process's appends. Blocks until the previous process releases the
    // flock; held for the whole process lifetime and released even on SIGKILL.
    let _wal_dir_lock = timefusion::write::wal::WalDirLock::acquire(&cfg.core.wal_dir()).await?;

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg_arc)).await?;
    info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    info!(
        "BufferedWriteLayer config: wal_dir={:?}, flush_interval={}s, retention={}min",
        cfg.core.wal_dir(),
        cfg.buffer.flush_interval_secs(),
        cfg.buffer.retention_mins()
    );

    let db_for_callback = db.clone();
    let delta_write_callback: timefusion::write::DeltaWriteCallback =
        Arc::new(move |project_id: String, table_name: String, batches: Vec<arrow::array::RecordBatch>, wal_watermark: timefusion::write::DeltaWatermark| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                // Returns the URIs newly added by this commit; the watermark goes
                // into Delta commit metadata for crash-mid-flush recovery. It also
                // warms the just-flushed files itself — don't warm again here.
                let added = db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await?;
                // Unconditional on a successful commit: the flag means "this
                // (project, table) has Delta files", true even if file attribution
                // came back empty.
                db.mark_delta_has_files(&project_id, &table_name);
                Ok(added)
            })
        });

    // Register UDFs up front so this context's FunctionRegistry doubles as the
    // WAL-replay registry. Table providers depend on buffered_layer and are
    // registered after recovery.
    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<timefusion::read::functions::FnRegistry> = Arc::new(session_context.state());

    // Pre-init WAL GC (gated + drained-flag consumption inside the helper).
    timefusion::write::wal::boot_wal_gc(&cfg.core.wal_dir());

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(cfg_arc.clone(), registry)?
        .with_delta_writer(delta_write_callback)
        .with_coalesced_delta_writer(timefusion::server::coalesced_delta_write_callback(&db));
    info!("bootstrap.phase=buffered_write_layer_init elapsed_ms={}", t_layer.elapsed().as_millis());
    let indexed_tables = cfg.tantivy.indexed_tables();
    let bucket = cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default();
    let tantivy_svc_for_metrics = if indexed_tables.is_empty() {
        None
    } else if bucket.is_empty() {
        error!("Schema declares indexed columns but AWS_S3_BUCKET is unset — Tantivy disabled, queries will scan");
        None
    } else {
        let obj_store = sidecar_store(&db, cfg, bucket, "tantivy").await?;
        let tcfg = Arc::new(cfg.tantivy.clone());
        let svc = Arc::new(timefusion::tantivy::search::TantivyIndexService::new(obj_store.clone(), tcfg.clone(), cfg.core.timefusion_data_dir.clone()));
        layer = layer.with_tantivy_indexer(timefusion::server::tantivy_index_callback(&db, Arc::clone(&svc)));
        let search = Arc::new(timefusion::tantivy::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone(), tcfg));
        // Lets a publish seed the reader's cache and invalidate its manifest
        // in-process instead of round-tripping through S3.
        svc.with_reader(&search);
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc.clone());
        info!("Tantivy sidecar indexes active for tables: {:?}", indexed_tables);
        Some(svc)
    };
    if cfg.maintenance.timefusion_file_bloom_pruning && !bucket.is_empty() {
        let store = sidecar_store(&db, cfg, bucket, "bloom_sidecars").await?;
        db = db.with_bloom_prune(Arc::new(timefusion::read::bloom_prune::BloomPruneRegistry::new(
            store,
            cfg.maintenance.timefusion_bloom_registry_cap_mb * 1024 * 1024,
            std::time::Duration::from_secs(cfg.maintenance.timefusion_bloom_registry_refresh_secs),
        )));
    }
    let buffered_layer = Arc::new(layer);

    // Observable gauges read snapshot_stats() each export cycle, keeping the hot
    // path untouched. Weak ref so metrics don't extend the layer's lifetime.
    if let Err(e) =
        timefusion::observability::init_metrics(&cfg.telemetry, Arc::downgrade(&buffered_layer), tantivy_svc_for_metrics.as_ref().map(Arc::downgrade))
    {
        error!("Failed to initialize OTel metrics: {} — continuing without metrics export", e);
    }

    // Must start before WAL replay — that is the window where probe deadlines
    // get missed. Its OWN token: `early_shutdown` is cancelled at the early-bind
    // handoff, and the sampler has to outlive that.
    let lag_shutdown = tokio_util::sync::CancellationToken::new();
    timefusion::observability::spawn_runtime_lag_sampler(lag_shutdown.clone());

    // Fast-forward walrus cursors before WAL replay so we don't re-inject entries
    // Delta already has. A `clean_shutdown=true` snapshot on local disk skips the
    // remote scan entirely; a dirty/missing one seeds positions and then falls
    // through to the Delta verifier for commits made after the last snapshot.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| {
        // age_secs is logged only, it does not gate the skip. Backwards clock skew
        // is clamped to 0 by `saturating_sub` rather than wrapping.
        let age_secs = timefusion::support::now_micros().saturating_sub(snap.written_at_micros) / 1_000_000;
        match wal_ref.restore_cursor_snapshot(&snap) {
            Ok(tables_advanced) => {
                info!(
                    "Cursor snapshot restored: {} table(s) seeded, {} table(s) advanced, clean_shutdown={}, age={}s",
                    snap.entries.len(),
                    tables_advanced,
                    snap.clean_shutdown,
                    age_secs
                );
                snap.clean_shutdown
            }
            Err(e) => {
                warn!("Cursor snapshot restore failed, falling back to Delta scan: {}", e);
                false
            }
        }
    });
    // A dirty/missing snapshot normally requires the expensive remote scan, but
    // when every durable cursor is already at its exact local WAL tail and no
    // interrupted-recovery marker exists, there is no payload Delta could advance.
    let local_wal_consumed = !clean_snapshot
        && wal_ref.can_skip_delta_reconcile().unwrap_or_else(|e| {
            warn!("Local WAL tail/cursor proof failed, retaining Delta reconciliation: {e}");
            false
        });
    let skip_delta_scan = clean_snapshot || local_wal_consumed;
    info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} clean_snapshot={clean_snapshot} local_wal_consumed={local_wal_consumed} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if skip_delta_scan {
        info!(
            "Skipping Delta-derived cursor reconciliation ({})",
            if clean_snapshot { "cursor snapshot is clean" } else { "all local WAL cursors exactly match their tails" }
        );
    } else {
        info!(
            "Running Delta-derived cursor reconciliation (snapshot missing/dirty); scan_depth={}, concurrency={} \
             — set TIMEFUSION_DELTA_SCAN_DEPTH higher if a deployment lost more commits than that since its last clean state",
            cfg.buffer.delta_scan_depth(),
            cfg.buffer.delta_scan_concurrency()
        );
        let t_delta = std::time::Instant::now();
        match db.derive_wal_cursors_from_delta(wal_ref, Some(buffered_layer.as_ref())).await {
            Ok(0) => info!("Delta-derived cursor: no advancement needed"),
            Ok(n) => info!("Delta-derived cursor: advanced {} shard(s) past Delta watermark", n),
            Err(e) => warn!("Delta-derived cursor derivation failed (continuing with local cursor): {}", e),
        }
        info!("bootstrap.phase=delta_cursor_reconcile elapsed_ms={}", t_delta.elapsed().as_millis());
    }

    let t_wal = std::time::Instant::now();
    let recovery_stats = buffered_layer.recover_from_wal().await?;
    info!("bootstrap.phase=wal_replay entries={} elapsed_ms={}", recovery_stats.entries_replayed, t_wal.elapsed().as_millis());

    buffered_layer.start_background_tasks().await;
    info!("BufferedWriteLayer background tasks started");

    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db.start_dml_coalescer();

    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;
    // Non-blocking: snapshot load + footer warm-up off the first query's path.
    db.preload_tables();
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();

    // Hand the pre-bound listener back from the early-bind 57P03 responder: it
    // was moved into early_task and is returned as that task's value, so there is
    // no rebind and no ECONNREFUSED window.
    info!("startup complete, transferring :5432 from early-bind 57P03 responder to real PGWire server");
    early_shutdown.cancel();
    let listener = early_task.await?;

    let auth_config = timefusion::server::AuthConfig::from_core(&cfg.core)?;

    // When cancelled, the accept loop stops taking new connections so the
    // BufferedWriteLayer flush isn't racing fresh inserts. Already-accepted
    // connections finish on their own spawned tasks.
    let pgwire_shutdown = tokio_util::sync::CancellationToken::new();
    // `mut` so the shutdown select! below can borrow it for early-failure
    // detection while leaving ownership for the drain phase.
    let mut pg_task = tokio::spawn({
        let shutdown = pgwire_shutdown.clone();
        let scan_metrics = Some(db.scan_metrics.clone());
        let db_for_pg = Arc::clone(&db);
        async move {
            if let Err(e) = timefusion::server::serve_with_listener(
                listener,
                Arc::new(session_context),
                &pg_opts,
                auth_config,
                scan_metrics,
                Some(db_for_pg),
                shutdown.cancelled_owned(),
            )
            .await
            {
                error!("PGWire server error: {}", e);
            }
        }
    });

    // PGWire is serving and WAL replay has returned; only now may recovery
    // relief files be indexed.
    db.spawn_deferred_tantivy_reindex(Arc::clone(&buffered_layer));

    // Catch SIGTERM (orchestrated restart) as well as SIGINT; without it the
    // grace period expires into a SIGKILL and in-flight writes are dropped.
    let term_signal = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            sigterm.recv().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
    };

    // In a start-first rollout the replacement blocks on the shared WAL flock and
    // writes a takeover request. Handoff readiness means writes are already fenced
    // and every hold drained, so the predecessor can exit at that moment.
    let takeover_signal = async {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let wal_dir = cfg.core.wal_dir();
            if !timefusion::write::wal::takeover_requested(&wal_dir) {
                continue;
            }
            if buffered_layer.is_deploy_handoff_ready() {
                break;
            }
            // Escalation: an instance the orchestrator has lost track of is never
            // sent SIGTERM, so readiness alone would hold the WAL lock forever and
            // starve every replacement. Take the ordinary graceful path anyway —
            // it fences writes and flushes exactly like SIGTERM does.
            if timefusion::write::wal::takeover_request_age(&wal_dir).is_some_and(|age| age >= timefusion::write::wal::TAKEOVER_ESCALATE_AFTER) {
                warn!(
                    "WAL takeover requested {}s ago and this instance never reached handoff readiness; shutting down anyway so the replacement can start",
                    timefusion::write::wal::TAKEOVER_ESCALATE_AFTER.as_secs()
                );
                break;
            }
        }
    };

    // Borrow `pg_task` so it can still be awaited in the drain phase below — the
    // select! only watches it for early failure, not for ownership.
    tokio::select! {
        res = &mut pg_task => {
            match res {
                Ok(()) => error!("PGWire server task ended unexpectedly"),
                Err(e) => error!("PGWire server task panicked: {}", e),
            }
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown");
        }
        _ = term_signal => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
        _ = takeover_signal => {
            info!("Start-first replacement requested drained WAL ownership; initiating graceful handoff");
        }
    }

    // Fence writes immediately: the accept loop stops but per-connection tasks are
    // not joined, so without this barrier an already-accepted INSERT could append
    // after the final flush/snapshot and force the replacement onto dirty recovery.
    buffered_layer.stop_accepting_writes();
    let preflushed_handoff = buffered_layer.is_drained();

    // Stop maintenance first: an in-flight sweep must bail before the
    // buffered-layer flush, or it outlives the Foyer cache and hangs shutdown.
    db.cancel_maintenance();

    // Drain order: stop accepting connections, flush and checkpoint the fenced
    // buffered layer, then shut down the database (cache, foyer, log store).
    // All serial phases share one budget (TIMEFUSION_STOP_GRACE_SECS, sized to fit
    // the orchestrator's SIGTERM→SIGKILL grace). The per-phase caps below keep a
    // hung connection from starving the buffer flush + cursor snapshot; unused
    // slack flows forward because every phase works off the same absolute deadline.
    let configured_grace = cfg.buffer.stop_grace();
    // Only a layer STILL drained after the admission fence can use the fast
    // handoff — a recent FLUSH marker alone is not evidence that replay is small,
    // since rows keep arriving during an online FLUSH.
    let grace = if preflushed_handoff { configured_grace.min(Duration::from_secs(1)) } else { configured_grace };
    let deadline = tokio::time::Instant::now() + grace;
    pgwire_shutdown.cancel();
    lag_shutdown.cancel();
    let pg_drain_budget = if preflushed_handoff { Duration::from_millis(50) } else { grace.mul_f32(0.2) };
    match tokio::time::timeout(pg_drain_budget, pg_task).await {
        Ok(Ok(())) => info!("PGWire drained cleanly"),
        Ok(Err(e)) => error!("PGWire task panicked during drain: {}", e),
        Err(_) => warn!("PGWire drain exceeded its slice of the stop grace — proceeding; in-flight queries may be reset"),
    }

    if let Err(e) = buffered_layer.shutdown_by(deadline).await {
        error!("Error during buffered layer shutdown: {}", e);
    }
    // Shares the same absolute `deadline` as the flush above, so every phase that
    // can block on a slow Delta/S3 backend is bounded and process exit (and the
    // `wal.lock` release) stays inside the SIGTERM→SIGKILL window.
    if let Err(e) = db.shutdown_by(deadline).await {
        error!("Error during database shutdown: {}", e);
    }

    info!("Shutdown complete.");
    // Do NOT synchronously flush OTLP here: its exporter has a 10s network
    // timeout, and `_wal_dir_lock` is held until this future returns, so the
    // replacement would wait that long for the WAL. Losing the final telemetry
    // batch is cheaper than extending the outage.

    Ok(())
}

/// Adds nullable columns to a live table's STORED Delta schema, without
/// touching the YAML.
///
/// The YAML and the Delta transaction log are two separate schemas, and a
/// mismatch produces batch/field-count errors and rejected INSERTs. Run this
/// against every live table FIRST; only then may the YAML declare the columns.
///
/// Writes a ZERO-ROW batch at the widened schema (`SchemaMode::Merge`), so it is
/// metadata-only and idempotent.
///
///   timefusion migrate-columns --table otel_logs_and_spans \
///       --add updated_at:timestamp --add deleted:boolean [--dry-run]
///   timefusion migrate-columns --table otel_logs_and_spans \
///       --add attributes___http___route:text
async fn run_migrate_columns_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    let mut table = "otel_logs_and_spans".to_string();
    let mut adds: Vec<(String, String)> = Vec::new();
    let mut dry_run = false;
    let mut it = Args::new();
    cli_args!(it, "usage: timefusion migrate-columns --table T --add NAME:TYPE [--add ...] [--dry-run]", {
        "--table" => table = it.value("--table")?,
        "--dry-run" => dry_run = true,
        "--add" => {
            let spec = it.next().context("--add needs NAME:TYPE")?;
            let (n, t) = spec.split_once(':').context("--add expects NAME:TYPE (timestamp|boolean|bigint|double|binary|text)")?;
            adds.push((n.to_string(), t.to_string()));
        },
    });
    anyhow::ensure!(!adds.is_empty(), "nothing to do: pass at least one --add NAME:TYPE");

    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    let report = db.migrate_add_columns(&table, &adds, dry_run).await?;
    println!("table='{}' stored_columns={} requested={} missing={}", table, report.stored_before, adds.len(), report.added.len());
    for n in &report.added {
        println!("  + {n}");
    }
    match (report.added.is_empty(), dry_run) {
        (true, _) => println!("nothing to migrate — every requested column is already in the stored schema"),
        (_, true) => println!("--dry-run: no commit written"),
        _ => println!("migrated: stored schema now has {} columns", report.stored_after),
    }
    Ok(())
}

/// Retention CLI (`timefusion retention --older-than-days N --table A,B [--dry-run] [--yes]`).
///
/// Drops whole `date=` partitions older than the cutoff with a PARTITION-ONLY
/// delete: the predicate references only the `date` partition column, so the
/// builder commits pure Remove actions — no data scan, rewrite, or deletion
/// vectors. Safe to run off-box against live storage; commits OCC-retry.
///
/// **Never route retention through pgwire `DELETE`**: on a `version_append`
/// table that path APPENDS a full-row tombstone per deleted row.
///
/// **Scope: unified tables ONLY.** Every table is resolved via
/// `get_or_create_unified_table`; bring-your-own-bucket projects live in their
/// own Delta tables and are deliberately unreachable here. Do not add a
/// `--project` flag that could point at one.
///
/// Physical bytes are reclaimed by the vacuum cron only after
/// `timefusion_vacuum_retention_hours`, which keeps a bad cutoff recoverable via
/// time travel.
async fn run_retention_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();

    let mut tables: Vec<String> = Vec::new();
    let mut older_than_days: Option<i64> = None;
    let mut dry_run = false;
    let mut yes = false;
    let mut it = Args::new();
    cli_args!(it, "usage: timefusion retention --older-than-days N --table A[,B,...] [--dry-run] [--yes]", {
        "--table" => tables.extend(it.value("--table")?.split(',').map(str::to_owned)),
        "--older-than-days" => older_than_days = Some(it.parse("--older-than-days", "an integer")?),
        "--dry-run" => dry_run = true,
        "--yes" => yes = true,
    });
    let days = older_than_days.context("--older-than-days is required")?;
    anyhow::ensure!(days >= 7, "refusing a cutoff under 7 days — that is not retention, that is data loss");
    anyhow::ensure!(!tables.is_empty(), "--table is required; retention never guesses at a table list");
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(days)).date_naive().to_string();
    let predicate = format!("date < '{cutoff}'");

    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    println!("retention cutoff: {predicate}  (today - {days}d)\n");

    let mut plan: Vec<(String, usize, i64)> = Vec::new();
    for t in &tables {
        let table_ref = db.get_or_create_unified_table(t).await?;
        let (old_files, old_bytes, total) = {
            let table = table_ref.read().await;
            table.snapshot()?.log_data().iter().fold((0usize, 0i64, 0usize), |(files, bytes, total), f| {
                let past_cutoff = f.path().split("date=").nth(1).and_then(|s| s.split('/').next()).is_some_and(|date| date < cutoff.as_str());
                (files + past_cutoff as usize, bytes + if past_cutoff { f.size() } else { 0 }, total + 1)
            })
        };
        println!("  {t:52} {old_files:>6}/{total:<6} files past cutoff, {:.2} GB", old_bytes as f64 / 1e9);
        plan.push((t.clone(), old_files, old_bytes));
    }
    let (files, bytes): (usize, i64) = plan.iter().fold((0, 0), |(f, b), p| (f + p.1, b + p.2));
    println!("\nTOTAL: {files} files, {:.2} GB across {} table(s)", bytes as f64 / 1e9, plan.len());

    if dry_run {
        println!("DRY RUN — no changes made");
        return db.shutdown().await;
    }
    anyhow::ensure!(yes, "pass --yes to commit the deletion (or --dry-run to stop here)");

    for (t, old_files, _) in &plan {
        if *old_files == 0 {
            println!("  {t}: nothing past cutoff, skipping");
            continue;
        }
        let table_ref = db.get_or_create_unified_table(t).await?;
        // Snapshot clone: never hold the table lock across the commit.
        let table = { table_ref.read().await.clone() };
        let (new_table, metrics) = table.delete().with_predicate(predicate.as_str()).await.with_context(|| format!("retention delete on {t}"))?;
        anyhow::ensure!(
            metrics.num_added_files == 0,
            "{t}: retention delete REWROTE {} files — the predicate was not partition-only; aborting",
            metrics.num_added_files
        );
        println!("  {t}: removed {} files (version {})", metrics.num_removed_files, new_table.version().unwrap_or(0));
        *table_ref.write().await = new_table;
    }
    println!("\ndone — physical bytes reclaim via the vacuum cron after the {}h retention window", cfg.maintenance.timefusion_vacuum_retention_hours);
    db.shutdown().await
}

/// One-off compaction CLI (`timefusion optimize [...]`): compacts old `date=`
/// partitions outside the scheduled 48h Z-order window via `Database::compact_date`
/// per partition. Meant to run off-box so it doesn't load the live server's
/// memory; commits use the same conditional-put coordination, so concurrent
/// commits OCC-retry safely.
async fn run_optimize_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();

    let mut table = "otel_logs_and_spans".to_string();
    let mut only_date: Option<chrono::NaiveDate> = None;
    let mut older_than_hours: u64 = 48;
    let mut all = false;
    let mut dry_run = false;
    let mut project: Option<String> = None;
    let mut concurrency: Option<usize> = None;
    let mut consolidate = false;
    let mut dedup = false;
    let mut recompress = false;
    let mut target_size_mb: Option<i64> = None;
    let mut it = Args::new();
    cli_args!(it, "usage: timefusion optimize [--table T] [--date YYYY-MM-DD | --older-than-hours N | --all] [--project ID] [--concurrency N] [--consolidate [--target-size-mb N]] [--dedup] [--recompress] [--dry-run]", {
        "--table" => table = it.value("--table")?,
        "--date" => only_date = Some(it.parse("--date", "YYYY-MM-DD")?),
        "--older-than-hours" => older_than_hours = it.parse("--older-than-hours", "an integer")?,
        "--all" => all = true,
        "--dry-run" => dry_run = true,
        "--project" => project = Some(it.value("--project")?),
        "--concurrency" => concurrency = Some(it.parse("--concurrency", "an integer")?),
        "--consolidate" => consolidate = true,
        "--dedup" => dedup = true,
        "--recompress" => recompress = true,
        "--target-size-mb" => target_size_mb = Some(it.parse("--target-size-mb", "an integer")?),
    });
    anyhow::ensure!(target_size_mb.is_none() || consolidate, "--target-size-mb only applies to --consolidate");

    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    // Attach the tantivy sidecar service as the server bootstrap does; without it
    // the post-optimize reindex/GC hooks silently no-op and every CLI compaction
    // orphans the rewritten files' index entries.
    let bucket = cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default();
    let db = if !cfg.tantivy.indexed_tables().is_empty() && !bucket.is_empty() {
        let obj_store = sidecar_store(&db, cfg, bucket, "tantivy").await?;
        let svc = timefusion::tantivy::search::TantivyIndexService::new(obj_store, Arc::new(cfg.tantivy.clone()), cfg.core.timefusion_data_dir.clone());
        db.with_tantivy_indexer(Arc::new(svc))
    } else {
        db
    };
    let table_ref = db.get_or_create_unified_table(&table).await?;
    println!("table prefix='{}' → {}", cfg.core.timefusion_table_prefix, table);

    let dates: Vec<chrono::NaiveDate> = if let Some(d) = only_date {
        vec![d]
    } else {
        let cutoff = (chrono::Utc::now() - chrono::Duration::hours(older_than_hours as i64)).date_naive();
        db.partition_dates(&table_ref).await?.into_iter().filter(|d| all || *d < cutoff).collect()
    };

    let scope = match (only_date, all) {
        (Some(d), _) => format!("date={d}"),
        (None, true) => "all dates".to_string(),
        (None, false) => format!("older than {older_than_hours}h"),
    } + &project.as_deref().map_or(String::new(), |p| format!(", project_id={p}"));

    if dry_run {
        let uris: Vec<String> = timefusion::database::file_uris(&*table_ref.read().await);
        println!("DRY RUN — {} candidate partition(s) of '{}' ({}):", dates.len(), table, scope);
        let pid_frag = project.as_deref().map_or(String::new(), |p| format!("project_id={p}/"));
        let total: usize = dates
            .iter()
            .map(|d| {
                let date_frag = format!("date={d}");
                let n = uris.iter().filter(|u| u.contains(&pid_frag) && u.contains(&date_frag)).count();
                println!("  date={d}: {n} files");
                n
            })
            .sum();
        println!("total {total} files across {} candidate partition(s) (no changes made)", dates.len());
        return db.shutdown().await;
    }

    // `--recompress` is the ONLY force-rewrite. Bin-packing skips files already at
    // target and drops single-file bins, so it can never fix a partition poisoned
    // by ONE file with no `sorting_columns` footer — it reports success having
    // changed nothing. `recompress_partition` rewrites through `replace_where`
    // with the schema ORDER BY regardless of file count or size, so the output
    // carries an honest sorted footer; `--project` narrows the overwrite
    // predicate, which is what keeps the job small enough to run anywhere.
    if recompress {
        let level = cfg.parquet.timefusion_zstd_compression_level;
        let scope = project.as_deref().map_or(String::new(), |p| format!(" project={p}"));
        for d in &dates {
            match db.recompress_partition(&table_ref, &table, *d, level, project.as_deref()).await {
                Ok(RecompressOutcome::Rewritten { files }) => println!("  recompress date={d}{scope}: rewritten from {files} file(s) (sorted footer restored)"),
                Ok(RecompressOutcome::Skipped(why)) => println!("  recompress date={d}{scope}: SKIPPED — {why}"),
                Err(e) => eprintln!("  recompress date={d}{scope}: FAILED: {e}"),
            }
        }
        reconcile_tantivy(&db, &table).await;
        return db.shutdown().await;
    }
    println!("compacting {} partition(s) of '{}' ({})", dates.len(), table, scope);
    if consolidate || dedup {
        // Leveled event-time-disjoint consolidation and/or a dedup pass, run per
        // project so a busy day's tens of GB never sit in one merge. Incremental
        // per-run commits make an interrupted run resumable.
        const MAX_ATTEMPTS: u64 = 5;
        for d in &dates {
            let projects = match &project {
                Some(p) => vec![p.clone()],
                None => db.partition_projects(&table_ref, *d).await?,
            };
            if consolidate {
                let target = target_size_mb.map_or(cfg.parquet.timefusion_cold_optimize_target_size, |mb| mb * 1024 * 1024);
                for p in &projects {
                    println!("  consolidate date={d} project={p} target={}MB", target / (1024 * 1024));
                    // Committed slices are excluded from re-selection, so a retry
                    // resumes at the next slice rather than restarting.
                    for attempt in 1..=MAX_ATTEMPTS {
                        match db.consolidate_date_binned(&table_ref, &table, *d, target, Some(p), usize::MAX).await {
                            Ok(()) => break,
                            Err(e) if attempt < MAX_ATTEMPTS => {
                                eprintln!("  consolidate date={d} project={p}: attempt {attempt} failed, retrying: {e}");
                                sleep(Duration::from_secs(5 * attempt)).await;
                            }
                            Err(e) => eprintln!("  consolidate date={d} project={p}: FAILED after {attempt} attempts: {e}"),
                        }
                    }
                }
            }
            if dedup {
                for p in &projects {
                    match db.dedup_partition(&table_ref, &table, p, *d).await {
                        Ok((dropped, complete)) => println!("  dedup date={d} project={p}: dropped={dropped} complete={complete}"),
                        Err(e) => eprintln!("  dedup date={d} project={p}: FAILED: {e}"),
                    }
                }
            }
        }
        reconcile_tantivy(&db, &table).await;
        return db.shutdown().await;
    }
    let (mut tot_r, mut tot_a) = (0u64, 0u64);
    for d in &dates {
        match db.compact_date_concurrent(&table_ref, &table, *d, project.as_deref(), concurrency).await {
            Ok((r, a)) => {
                tot_r += r;
                tot_a += a;
                println!("  date={d}: removed={r} added={a}");
            }
            Err(e) => eprintln!("  date={d}: FAILED: {e}"),
        }
    }
    println!("done: {tot_r} files removed, {tot_a} files added across {} partition(s)", dates.len());
    reconcile_tantivy(&db, &table).await;
    db.shutdown().await
}

/// Post-run index reconcile: index uncovered live files (incl. leftovers from
/// earlier runs that compacted without the service attached), GC dead entries.
/// Best-effort — the coverage gate keeps queries correct either way.
async fn reconcile_tantivy(db: &Database, table: &str) {
    match db.tantivy_reconcile_table(table).await {
        Ok((0, 0, 0)) => {}
        Ok((built, removed, blobs)) => println!("tantivy reconcile: built={built} manifest_entries_removed={removed} blobs_deleted={blobs}"),
        Err(e) => eprintln!("tantivy reconcile FAILED (indexes stale until the next reconcile or server backfill): {e}"),
    }
}

#[cfg(test)]
mod healthcheck_tests {
    use super::pgwire_ready_at;

    fn one_response(response: Vec<u8>) -> std::net::SocketAddr {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut length = [0u8; 4];
            stream.read_exact(&mut length).unwrap();
            let remaining = u32::from_be_bytes(length).saturating_sub(4) as usize;
            let mut startup = vec![0u8; remaining];
            stream.read_exact(&mut startup).unwrap();
            stream.write_all(&response).unwrap();
        });
        addr
    }

    fn error_frame(code: &[u8]) -> Vec<u8> {
        let payload = [&b"C"[..], code, &[0, 0]].concat();
        [&b"E"[..], &((payload.len() + 4) as u32).to_be_bytes(), &payload[..]].concat()
    }

    #[test_case::test_case(vec![b'R'] => true ; "authentication request")]
    #[test_case::test_case(error_frame(b"57P03") => true ; "starting-up error is alive enough")]
    #[test_case::test_case(error_frame(b"XX000") => false ; "any other error is unhealthy")]
    fn liveness_accepts_authentication_and_startup_error_only(response: Vec<u8>) -> bool {
        pgwire_ready_at(one_response(response)).is_ok()
    }

    /// The probe and the Dockerfile are one budget split across two files: if
    /// Docker's `--timeout` is below the probe's worst case, Docker kills the
    /// probe before it can report, and every slow-but-alive moment reads as dead.
    #[test]
    fn probe_worst_case_fits_the_docker_timeout() {
        let line = include_str!("../Dockerfile").lines().find(|l| l.starts_with("HEALTHCHECK ")).expect("Dockerfile must declare a HEALTHCHECK");
        let flag = |name: &str| -> u64 {
            line.split_whitespace()
                .find_map(|f| f.strip_prefix(name))
                .and_then(|v| v.strip_suffix('s').unwrap_or(v).parse().ok())
                .unwrap_or_else(|| panic!("HEALTHCHECK is missing {name}: {line}"))
        };
        let docker_timeout = std::time::Duration::from_secs(flag("--timeout="));
        // connect + write + read, each bounded by PROBE_OP_TIMEOUT.
        let worst_case = super::PROBE_OP_TIMEOUT * 3;
        assert!(
            worst_case <= docker_timeout,
            "the probe can take up to {worst_case:?} but Docker kills it at {docker_timeout:?} — raise --timeout or lower PROBE_OP_TIMEOUT"
        );
        assert!(flag("--retries=") >= 5, "3 consecutive misses inside 15s is 'busy', not 'dead' (prod 2026-08-08)");
    }

    /// Workers must not run on Tokio's default stack: an overflow aborts the
    /// process, so losing this builder call is a restart loop, not a failed query.
    #[test]
    fn workers_get_more_than_the_default_stack() {
        assert!(include_str!("main.rs").contains(".thread_stack_size(WORKER_STACK_BYTES)"), "the runtime must actually be built with WORKER_STACK_BYTES");
    }
}
