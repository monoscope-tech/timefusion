// main.rs
#![recursion_limit = "512"]

// Production profiling (--features profiling, Linux): jemalloc as the global
// allocator with its heap profiler, plus a pprof CPU sampler (started in
// async_main). Deployed to attribute the prod OOM. See src/profiling.rs.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc reads this symbol at startup — bakes the profiler config into the
// binary so no MALLOC_CONF env (host is read-only) is needed.
// prof_prefix points into the data-dir volume we can read off the host.
// Analyze: `jeprof --svg <binary> <prof_prefix>.*.heap`.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[unsafe(export_name = "malloc_conf")]
// `prof:true, prof_active:false`: sampling stays compiled in but off by default —
// re-arm at runtime via the `prof.active` mallctl (no rebuild) when heap
// attribution is next needed. lg_prof_sample:19 = ~512KiB sampling; keeping it
// off saves CPU/heap on this box, whose memory headroom gates compaction.
//
// `dirty_decay_ms:10000` (was 0): decay 0 madvise()s every freed page back to
// the kernel immediately, which under maintenance load (2026-08-18 perf trace)
// cost ~18% CPU in page-fault/TLB-shootdown churn from Arrow scan buffers being
// freed and re-faulted. 10s amortizes that while still returning idle memory;
// the 85% maintenance brake remains the OOM backstop. Don't drop this back to 0
// without re-measuring under maintenance load — it was set there in 2026-08-03
// to fight OOMs from since-fixed causes (unbounded scans, DedupExec).
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19,lg_prof_interval:35,prof_prefix:/app/data/timefusion/profiles/jeprof,background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000\0";

use std::sync::Arc;

use anyhow::Context;
use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use timefusion::{
    config::{self, AppConfig},
    database::{Database, RecompressOutcome},
    observability, server, support,
    write::BufferedWriteLayer,
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

/// Stack size for every Tokio worker.
///
/// Tokio's default (2 MiB) overflowed planning a merge-on-read UPDATE on
/// 2026-08-16 (deep recursion over a wide schema + IN-list pushdown), which
/// aborts the whole process, not just the task — prod restart-looped on exit
/// 134. Plan depth follows schema width and predicate shape, not just pushdown
/// size, so this bounds the stack directly rather than the recursion. Reserved
/// lazily, so untouched pages cost address space, not RSS.
const WORKER_STACK_BYTES: usize = 32 * 1024 * 1024;
// Planning depth follows schema width and predicate shape, not just the
// pushdown cap, so keep real headroom over Tokio's 2 MiB default.
const _: () = assert!(WORKER_STACK_BYTES >= 8 * 2 * 1024 * 1024);

fn main() -> anyhow::Result<()> {
    dotenv().ok();
    // Before the runtime, so every worker thread/listener inherits the raised
    // limit. `bootstrap()` calls it too, for the e2e harness (skips main()).
    server::raise_file_limit();

    let subcommand = std::env::args().nth(1);
    if subcommand.as_deref() == Some("healthcheck") {
        return run_pgwire_healthcheck();
    }
    if subcommand.as_deref() == Some("encrypt-secret") {
        return config::run_cli();
    }
    // Replays a prod maintenance journal through the real scheduler on virtual
    // time — must stay config/bucket-free, that's what lets it answer
    // scheduler questions without a deploy.
    if subcommand.as_deref() == Some("sim") {
        return run_sim_cli();
    }

    // Maintenance CLIs get the maintenance-heavy budget shape (the server shape
    // strands cgroup memory in query/ingest slices a one-shot CLI never uses).
    // Must precede init_config, which snapshots the tree.
    //
    // `run-unit` excluded: it drives a coordinator unit whose pool comes from
    // `coordinator_share_bytes()`, which is a hard 0 under this profile
    // ("no coordinator runs under MaintenanceCli") — every invocation died at
    // `pool_size: 0.0 B`. Found 2026-08-20.
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
        _ => {
            let result = rt.block_on(async_main(cfg));
            // Must END THE PROCESS here: dropping the runtime waits on
            // lingering blocking/detached threads, and that hang left a
            // zombie container blocking swarm's replacement (2026-08-06
            // pgwire outage). Everything durable is already on disk.
            match result {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("fatal: {e:#}");
                    std::process::exit(1)
                }
            }
        }
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

/// Per-operation deadline for the readiness probe, so its worst case is 3x this
/// (connect + write + read) and must stay inside the Dockerfile's
/// `HEALTHCHECK --timeout` (pinned by `probe_worst_case_fits_the_docker_timeout`).
///
/// Was 750ms, and that was the actual killer (prod 2026-08-08): a probe measured
/// at 0.896s with no deploy in flight and the server perfectly healthy, three of
/// those in a row, and Swarm replaced the task — mid footer repair, discarding a
/// 40-minute rewrite. The handshake competes for the same runtime as ingest and
/// maintenance, so sub-second is not a budget a loaded database can hold. This
/// is a LIVENESS probe: the question is "is this still a database", not "is it
/// fast right now".
const PROBE_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1500);

/// A probe verdict is useless without knowing WHICH stage was slow: a slow
/// `connect` is the accept loop (or the listen backlog) not getting scheduled,
/// a slow `auth` read is the handshake task losing its runtime slice behind
/// CPU-bound maintenance. On 2026-08-11 a probe timeout killed the task
/// mid-repair with CPU at 805%/4800% and 17.8 of 96 GiB — neither saturation
/// nor OOM, so the deadline was measuring something we could not see.
///
/// Printed on BOTH paths (Docker records healthcheck output either way), so a
/// `docker inspect` health log reads as a stage histogram over time rather than
/// a column of bare "unhealthy". Deliberately not widened — read the stages
/// first; widening the deadline destroys the only signal there is.
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
    let mut startup = Vec::with_capacity(8 + body.len());
    startup.extend_from_slice(&((8 + body.len()) as u32).to_be_bytes());
    startup.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    startup.extend_from_slice(body);
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
    if tag[0] == b'R' {
        return Ok(());
    }
    if tag[0] == b'E' {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length)?;
        let payload_len = u32::from_be_bytes(length).saturating_sub(4) as usize;
        anyhow::ensure!(payload_len <= 64 * 1024, "PGWire ErrorResponse is unreasonably large");
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload)?;
        anyhow::ensure!(payload.windows(7).any(|field| field == b"C57P03\0"), "PGWire returned a non-startup error");
        return Ok(());
    }
    anyhow::bail!("PGWire returned unexpected response tag {:?}", tag[0] as char)
}

fn init_cli_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .try_init();
}

/// `timefusion sim <journal.json | data-dir> [--hours N] [--workers N]
/// [--streams N] [--scale F] [--seed N] [--no-mint] [--json]`
///
/// Replay a copied-out prod maintenance journal through the real scheduler on
/// virtual time (`timefusion::maintenance_sim`). The answer to "does this
/// policy keep up" without a deploy. Fetch the input with e.g.
/// `ssh ubuntu@captain.s.past3.tech 'docker cp <container>:/data/.timefusion_meta/maintenance_tasks.json -'`.
fn run_sim_cli() -> anyhow::Result<()> {
    use timefusion::maintenance_sim::{SimConfig, load_sandboxed, run};
    let mut it = std::env::args().skip(2);
    let usage = "usage: timefusion sim <journal.json|data-dir> [--hours N] [--workers N] [--streams N] [--scale F] [--seed N] [--no-mint] [--json]";
    let input = it.next().context(usage)?;
    let mut cfg = SimConfig::default();
    let mut json = false;
    while let Some(a) = it.next() {
        let mut value = |name: &str| -> anyhow::Result<String> { it.next().with_context(|| format!("{name} needs a value")) };
        match a.as_str() {
            "--hours" => cfg.horizon_micros = (value("--hours")?.parse::<f64>().context("--hours must be a number")? * 3_600_000_000.0) as i64,
            "--workers" => cfg.workers = value("--workers")?.parse().context("--workers must be an integer")?,
            "--streams" => cfg.streams = Some(value("--streams")?.parse().context("--streams must be an integer")?),
            "--scale" => cfg.duration_scale = value("--scale")?.parse().context("--scale must be a number")?,
            "--seed" => cfg.seed = u64::from_str_radix(value("--seed")?.trim_start_matches("0x"), 16).context("--seed must be hex")?,
            "--restarts-every-hours" => {
                cfg.restart_every_micros =
                    (value("--restarts-every-hours")?.parse::<f64>().context("--restarts-every-hours must be a number")? * 3_600_000_000.0) as i64
            }
            "--restart-at-hours" => {
                cfg.restart_at_micros =
                    Some((value("--restart-at-hours")?.parse::<f64>().context("--restart-at-hours must be a number")? * 3_600_000_000.0) as i64)
            }
            "--no-mint" => cfg.mint_frontier = false,
            "--json" => json = true,
            other => anyhow::bail!("unknown argument: {other} ({usage})"),
        }
    }
    let (journal, _sandbox) = load_sandboxed(std::path::Path::new(&input))?;
    let report = run(journal, &cfg, support::now_micros())?;
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
    let mut completions = report.completions.iter().collect::<Vec<_>>();
    completions.sort();
    println!("completions: {}", completions.iter().map(|(op, n)| format!("{op}={n}")).collect::<Vec<_>>().join(" "));
    if !report.timeouts.is_empty() {
        let mut timeouts = report.timeouts.iter().collect::<Vec<_>>();
        timeouts.sort();
        println!("timeouts:    {}", timeouts.iter().map(|(op, n)| format!("{op}={n}")).collect::<Vec<_>>().join(" "));
    }
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
/// its time went (scan/stage/commit/end-to-end deltas + wall). The per-unit
/// cost decomposition as a command, not a fleet-counter inference. Point
/// TIMEFUSION_DATA_DIR at a scratch dir so the journal holds no other
/// claimable work.
async fn run_unit_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    let mut source = "otel_logs_and_spans".to_string();
    let mut project: Option<String> = None;
    let mut date: Option<chrono::NaiveDate> = None;
    let mut operation = timefusion::maintenance_coordinator::Operation::BaseRollup;
    let mut slice_hours: i64 = 24;
    let mut offset_hours: i64 = 0;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        let mut value = |name: &str| -> anyhow::Result<String> { it.next().with_context(|| format!("{name} needs a value")) };
        match a.as_str() {
            "--source" => source = value("--source")?,
            "--project" => project = Some(value("--project")?),
            "--date" => date = Some(value("--date")?.parse().context("--date must be YYYY-MM-DD")?),
            "--slice-hours" => slice_hours = value("--slice-hours")?.parse().context("--slice-hours must be an integer")?,
            "--offset-hours" => offset_hours = value("--offset-hours")?.parse().context("--offset-hours must be an integer")?,
            "--op" => {
                operation = match value("--op")?.as_str() {
                    "base" => timefusion::maintenance_coordinator::Operation::BaseRollup,
                    "derived" => timefusion::maintenance_coordinator::Operation::DerivedRollup,
                    "dedup" => timefusion::maintenance_coordinator::Operation::Dedup,
                    "hot" => timefusion::maintenance_coordinator::Operation::HotPacking,
                    "sealed" => timefusion::maintenance_coordinator::Operation::SealedConsolidation,
                    "repair" => timefusion::maintenance_coordinator::Operation::Repair,
                    other => anyhow::bail!("unknown --op {other}: base|derived|dedup|hot|sealed|repair"),
                }
            }
            other => anyhow::bail!(
                "unknown argument: {other} (usage: timefusion run-unit --project ID [--source T] [--date D] [--op OP] [--slice-hours N] [--offset-hours N])"
            ),
        }
    }
    let project = project.context("--project is required")?;
    let date = date.unwrap_or_else(|| support::today_utc() - chrono::Duration::days(1));
    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    let report = db.run_unit_once(&source, &project, date, operation, slice_hours, offset_hours).await?;
    println!("{report}");
    Ok(())
}

/// `timefusion redrive-dml [--dir PATH] [--dry-run]` — replay parked quarantine/dml
/// enrichment groups (see [`timefusion::dml::redrive_dml_quarantine`]).
async fn run_redrive_dml_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    // Two-token flags need lookahead into the arg iterator, so this stays a loop.
    let mut dir = cfg.core.wal_dir().join(timefusion::write::wal::QUARANTINE_DIR_NAME).join("dml");
    let mut dry_run = false;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--dir" => dir = it.next().map(std::path::PathBuf::from).context("--dir needs a value")?,
            "--dry-run" => dry_run = true,
            other => anyhow::bail!("unknown argument: {other} (usage: timefusion redrive-dml [--dir PATH] [--dry-run])"),
        }
    }
    let db = Arc::new(Database::with_config(Arc::new(cfg.clone())).await?);
    let (ok, skipped) = timefusion::dml::redrive_dml_quarantine(&db, &dir, dry_run).await;
    println!("redrive-dml: {ok} recovered, {skipped} left parked (dir {dir:?})");
    db.shutdown().await
}

async fn async_main(cfg: &'static AppConfig) -> anyhow::Result<()> {
    // Initialize OpenTelemetry with OTLP exporter
    observability::init_telemetry(&cfg.telemetry)?;
    // AFTER init_telemetry: config is built before the subscriber exists, so
    // logging the tree at derivation time is silently swallowed — which is why
    // prod could carry TIMEFUSION_MEMORY_LIMIT_GB=26 while actually budgeting
    // 120 GiB with nothing on the box revealing the gap (2026-07-31).
    config::log_derived_budget(&cfg.derived);
    support::init_from_env();

    // Start heap+CPU profiling (no-op unless --features profiling on Linux).
    // Early, so the profiles dir exists before jemalloc's first interval dump.
    timefusion::observability::start(cfg.core.timefusion_data_dir.clone());

    info!("Starting TimeFusion application");

    // Create Arc<AppConfig> for passing to components
    let cfg_arc = Arc::new(cfg.clone());

    // Bind :5432 immediately, before the slow startup work (Database open,
    // WAL recovery — up to ~15 min when WAL has accumulated). Clients
    // connecting in this window get SQLSTATE 57P03 ("starting up") from
    // the early-bind responder instead of ECONNREFUSED, which is what
    // Hasql / pgjdbc / libpq expect during a backend restart and retry
    // on cleanly. See pgwire_early_bind for the responder.
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
    // GC below, recovery, or writes). TimeFusion's WAL is single-writer with no
    // cross-process coordination; two live processes on the same dir fork it —
    // the newer one recovers only the prefix present at its start and orphans
    // the older's concurrent appends (silent loss on an overlapping redeploy).
    // Blocks until any previous process exits and releases the flock, serving
    // 57P03 via the early-bind responder above meanwhile. Held for the whole
    // process lifetime; released by the kernel even on SIGKILL. Under a
    // start-first deploy this self-resolves (readiness is a TCP check the early
    // responder already satisfies, so the orchestrator stops the old instance,
    // which releases the lock); stop-first shortens the handoff but isn't required.
    let _wal_dir_lock = timefusion::write::wal::WalDirLock::acquire(&cfg.core.wal_dir()).await?;

    // Initialize database with explicit config
    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg_arc)).await?;
    info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    // Initialize BufferedWriteLayer with explicit config
    info!(
        "BufferedWriteLayer config: wal_dir={:?}, flush_interval={}s, retention={}min",
        cfg.core.wal_dir(),
        cfg.buffer.flush_interval_secs(),
        cfg.buffer.retention_mins()
    );

    // Create buffered layer with delta write callback
    let db_for_callback = db.clone();
    let delta_write_callback: timefusion::write::DeltaWriteCallback =
        Arc::new(move |project_id: String, table_name: String, batches: Vec<arrow::array::RecordBatch>, wal_watermark: timefusion::write::DeltaWatermark| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                // insert_records_batch returns the URIs of files newly added by this
                // commit, derived from the post-write snapshot under the same write
                // lock — no second log scan. Watermark goes into Delta commit metadata
                // for crash-mid-flush recovery.
                // insert_records_batch warms the just-flushed files itself
                // (watermark-gated) — no warm here, or every flush would issue
                // the warm GETs twice.
                let added = db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await?;
                // Unconditional on a successful commit — the flag means "this
                // (project, table) has Delta files", true as soon as the commit
                // lands even if file attribution came back empty. See the
                // coalesced callback in `bootstrap.rs` for the full rationale.
                db.mark_delta_has_files(&project_id, &table_name);
                Ok(added)
            })
        });

    // Register UDFs on the real SessionContext up front so its FunctionRegistry
    // doubles as the WAL-replay registry — no throwaway bootstrap context.
    // Table providers depend on buffered_layer and are registered after recovery.
    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<timefusion::read::functions::FnRegistry> = Arc::new(session_context.state());

    // Tantivy sidecar indexes are always-on whenever at least one table has
    // `tantivy.indexed: true` fields in its YAML schema (or appears in the
    // optional `TIMEFUSION_TANTIVY_INDEXED_TABLES` override). The query layer
    // accelerates standard SQL predicates (`=`, `LIKE 'prefix%'`) via the
    // TantivyPredicateRewriter — callers don't need to know tantivy exists.
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
        let storage_uri = format!("s3://{bucket}/{}/tantivy", cfg.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        let tcfg = Arc::new(cfg.tantivy.clone());
        let svc = Arc::new(timefusion::tantivy::search::TantivyIndexService::new(obj_store.clone(), tcfg.clone()));
        layer = layer.with_tantivy_indexer(svc.clone().callback());
        let search = Arc::new(timefusion::tantivy::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone(), tcfg));
        // Two halves of one process: let a publish seed the reader's cache and
        // invalidate its manifest instead of round-tripping through S3.
        svc.with_reader(&search);
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc.clone());
        info!("Tantivy sidecar indexes active for tables: {:?}", indexed_tables);
        Some(svc)
    };
    if cfg.maintenance.timefusion_file_bloom_pruning && !bucket.is_empty() {
        let storage_uri = format!("s3://{bucket}/{}/bloom_sidecars", cfg.core.timefusion_table_prefix);
        let store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        db = db.with_bloom_prune(Arc::new(timefusion::read::bloom_prune::BloomPruneRegistry::new(
            store,
            cfg.maintenance.timefusion_bloom_registry_cap_mb * 1024 * 1024,
            std::time::Duration::from_secs(cfg.maintenance.timefusion_bloom_registry_refresh_secs),
        )));
    }
    let buffered_layer = Arc::new(layer);

    // Initialize OpenTelemetry metrics — observable gauges read snapshot_stats()
    // each export cycle (30s), keeping the hot path untouched. Weak ref so
    // metrics don't extend the layer's lifetime.
    if let Err(e) =
        timefusion::observability::init_metrics(&cfg.telemetry, Arc::downgrade(&buffered_layer), tantivy_svc_for_metrics.as_ref().map(Arc::downgrade))
    {
        error!("Failed to initialize OTel metrics: {} — continuing without metrics export", e);
    }

    // Starts here, not after WAL replay: replay is exactly the window where a
    // probe deadline gets missed, so the sampler has to already be running to
    // catch it. Its OWN token, not `early_shutdown` — that one is cancelled at
    // the early-bind handoff, which is precisely when the sampler starts being
    // interesting.
    let lag_shutdown = tokio_util::sync::CancellationToken::new();
    timefusion::observability::spawn_runtime_lag_sampler(lag_shutdown.clone());

    // Fast-forward walrus cursors before WAL replay so we don't re-inject
    // entries Delta already has. Fast path: a `clean_shutdown=true` snapshot
    // on local disk lets us skip the ~6.5-min R2 scan entirely. Dirty/missing
    // snapshot still seeds positions, then falls through to the (env-tuned,
    // shorter) Delta verifier to catch commits made after the last snapshot.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| {
        // age_secs is surfaced in the boot log only — not gating the skip. See
        // CursorSnapshot docs for the single-writer assumption and the `rm`
        // escape hatch. Backwards clock skew (NTP correction, snapshot ported
        // across hosts) is clamped to 0 by `saturating_sub`, not wrapped negative.
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
    // A dirty/missing snapshot normally requires the expensive remote scan.
    // But when every durable cursor is already at its exact local WAL tail and
    // no interrupted-recovery marker exists, there is no payload whose cursor
    // Delta could advance. This covers the common deploy failure mode where
    // pre-deploy FLUSH drained successfully but the old container was killed
    // before it could write clean_shutdown=true.
    let local_wal_consumed = !clean_snapshot
        && match wal_ref.can_skip_delta_reconcile() {
            Ok(v) => v,
            Err(e) => {
                warn!("Local WAL tail/cursor proof failed, retaining Delta reconciliation: {e}");
                false
            }
        };
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
        match db.derive_wal_cursors_from_delta(wal_ref).await {
            Ok(0) => info!("Delta-derived cursor: no advancement needed"),
            Ok(n) => info!("Delta-derived cursor: advanced {} shard(s) past Delta watermark", n),
            Err(e) => warn!("Delta-derived cursor derivation failed (continuing with local cursor): {}", e),
        }
        info!("bootstrap.phase=delta_cursor_reconcile elapsed_ms={}", t_delta.elapsed().as_millis());
    }

    // Recover from WAL on startup
    let t_wal = std::time::Instant::now();
    let recovery_stats = buffered_layer.recover_from_wal().await?;
    info!("bootstrap.phase=wal_replay entries={} elapsed_ms={}", recovery_stats.entries_replayed, t_wal.elapsed().as_millis());

    // Start background tasks (flush and eviction)
    buffered_layer.start_background_tasks().await;
    info!("BufferedWriteLayer background tasks started");

    // Apply buffered layer to database
    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db.start_dml_coalescer();

    // Start maintenance schedulers for regular optimize and vacuum
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;
    // Non-blocking: snapshot load + footer warm-up off the first query's path.
    db.preload_tables();
    // Config-gated background index maintenance: backfill uncovered files,
    // warm the local index cache with recent blobs.
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();

    // Start PGWire server on the listener we pre-bound at the top of
    // async_main. First, hand control of that listener back from the
    // early-bind 57P03 responder.
    //
    // Ownership handoff: the listener was moved into early_task and is
    // returned as its final value, so `early_task.await?` hands back the
    // owned TcpListener — no Arc, no rebind, no ECONNREFUSED window.
    // handle_one tasks accepted just before shutdown may still be running;
    // they own only the accepted sockets and complete independently.
    info!("startup complete, transferring :5432 from early-bind 57P03 responder to real PGWire server");
    early_shutdown.cancel();
    let listener = early_task.await?;

    let auth_config = timefusion::server::AuthConfig::from_core(&cfg.core)?;

    // PGWire shutdown signal: when cancelled, the accept loop in
    // `serve_with_handlers` stops accepting new connections so the
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

    // Catch SIGTERM (k8s rolling restart) in addition to SIGINT (Ctrl-C).
    // Without SIGTERM handling, k8s sends SIGKILL after the grace period
    // and in-flight writes are dropped.
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

    // In a start-first rollout the replacement binds its isolated listener,
    // then blocks on the shared WAL flock and writes a takeover request. A
    // successful HANDOFF has already fenced writes and drained every hold, so
    // the predecessor can exit at that exact moment while continuing to serve
    // reads until the replacement actually exists. Without HANDOFF readiness,
    // requests are ignored and SIGTERM remains the only shutdown authority.
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
            // Escalation. Handoff readiness is the FAST path, not the only one:
            // an instance the orchestrator has lost track of is never sent
            // SIGTERM, so if readiness is the sole authority it holds the WAL
            // lock forever and every replacement starves behind it — measured
            // 2026-08-10 at 47 minutes with six live containers stacking up on
            // one box. A request nobody has satisfied for this long means the
            // predecessor is that instance, so take the ordinary graceful path
            // anyway; it fences writes and flushes exactly like SIGTERM does.
            if timefusion::write::wal::takeover_request_age(&wal_dir).is_some_and(|age| age >= timefusion::write::wal::TAKEOVER_ESCALATE_AFTER) {
                warn!(
                    "WAL takeover requested {}s ago and this instance never reached handoff readiness; shutting down anyway so the replacement can start",
                    timefusion::write::wal::TAKEOVER_ESCALATE_AFTER.as_secs()
                );
                break;
            }
        }
    };

    // Wait for shutdown signal. Borrow `pg_task` so we can still await it
    // in the drain phase below — the select! only watches it for early
    // failure, not for ownership.
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

    // Fence writes immediately. datafusion-postgres stops its accept loop but
    // does not join per-connection tasks; without this barrier an already-
    // accepted INSERT could append after the final flush/snapshot, forcing the
    // replacement back onto dirty recovery (or making a clean claim stale).
    buffered_layer.stop_accepting_writes();
    let preflushed_handoff = buffered_layer.is_drained();

    // Stop maintenance first: an in-flight light-optimize/dedup sweep must bail
    // before the buffered-layer flush, not compete with it and then outlive the
    // Foyer cache (a running sweep hitting a closed cache previously hung
    // shutdown until the orchestrator SIGKILLed us after the stop grace).
    db.cancel_maintenance();

    // Drain order matters:
    // 0. Stop PGWire from accepting new connections. Without this, the
    //    BufferedWriteLayer flush below races fresh inserts that pile back
    //    into MemBuffer + WAL, defeating the whole point of a graceful
    //    shutdown.
    // 1. Flush and checkpoint the fenced buffered layer.
    // 2. Shut down database (cache, foyer, log store).
    // One shutdown budget shared by all serial phases (TIMEFUSION_STOP_GRACE_SECS,
    // sized to fit the orchestrator's SIGTERM→SIGKILL grace). The drain phases
    // get small caps so a hung connection can't starve the buffer flush +
    // cursor snapshot — the phase that determines next-boot cost; their unused
    // slack flows forward automatically because the buffered layer works off
    // the same absolute deadline.
    let configured_grace = cfg.buffer.stop_grace();
    // Only a layer that is STILL drained after the admission fence can use the
    // constant-time handoff. At production ingest rates tens of thousands of
    // rows can arrive during an online FLUSH, so a recent FLUSH marker alone is
    // not evidence that replay is small. A post-FLUSH tail gets the normal
    // correctness-first budget and is flushed after the fence.
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
    // Share the same absolute `deadline` as the buffered-layer flush above so
    // the whole serial shutdown fits one stop-grace budget — every phase that
    // can block on a slow Delta/S3 backend (DML drain, foyer `close()`) is
    // bounded by it, so process exit and `wal.lock` release stay inside the
    // orchestrator's SIGTERM→SIGKILL window (issue #82).
    if let Err(e) = db.shutdown_by(deadline).await {
        error!("Error during database shutdown: {}", e);
    }

    info!("Shutdown complete.");
    // Do not synchronously flush OTLP here. Its exporter has a 10-second
    // network timeout, and `_wal_dir_lock` must remain held until this future
    // returns so no detached runtime work can overlap the replacement's WAL
    // access. Losing the final telemetry batch is preferable to extending a
    // planned database outage; normal batches are exported continuously.

    Ok(())
}

/// Adds nullable columns to a live table's STORED Delta schema, without
/// touching the YAML.
///
/// A shipped table can't gain a column via YAML alone — the YAML and the
/// Delta transaction log are two separate schemas, and a mismatch produces
/// batch/field count errors and rejected INSERTs (see 7d68f01, and the doc
/// block atop `schema_loader.rs`). Run this against prod first; only once
/// every live table has the columns may the YAML declare them.
///
/// Writes a ZERO-ROW batch at the widened schema (`SchemaMode::Merge`), so
/// it's metadata-only and idempotent.
///
///   timefusion migrate-columns --table otel_logs_and_spans \
///       --add updated_at:timestamp --add deleted:boolean [--dry-run]
async fn run_migrate_columns_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    let mut table = "otel_logs_and_spans".to_string();
    let mut adds: Vec<(String, String)> = Vec::new();
    let mut dry_run = false;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--table" => table = it.next().context("--table needs a value")?,
            "--dry-run" => dry_run = true,
            "--add" => {
                let spec = it.next().context("--add needs NAME:TYPE")?;
                let (n, t) = spec.split_once(':').context("--add expects NAME:TYPE (timestamp|boolean)")?;
                adds.push((n.to_string(), t.to_string()));
            }
            other => anyhow::bail!("unknown argument: {other} (usage: timefusion migrate-columns --table T --add NAME:TYPE [--add ...] [--dry-run])"),
        }
    }
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

/// One-off compaction CLI (`timefusion optimize [...]`): compacts old `date=`
/// partitions outside the scheduled 48h Z-order window via `Database::compact_date`
/// per partition. Meant to run off-box against prod storage so it doesn't load
/// the live server's memory; commits use the same S3/R2 conditional-put
/// coordination as the live server, so concurrent commits OCC-retry safely.
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
    // Two-token flags need lookahead into the arg iterator, so this stays a loop.
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--table" => table = it.next().context("--table needs a value")?,
            "--date" => only_date = Some(it.next().context("--date needs a value")?.parse().context("--date must be YYYY-MM-DD")?),
            "--older-than-hours" => {
                older_than_hours = it.next().context("--older-than-hours needs a value")?.parse().context("--older-than-hours must be an integer")?
            }
            "--all" => all = true,
            "--dry-run" => dry_run = true,
            "--project" => project = Some(it.next().context("--project needs a value")?),
            "--concurrency" => concurrency = Some(it.next().context("--concurrency needs a value")?.parse().context("--concurrency must be an integer")?),
            "--consolidate" => consolidate = true,
            "--dedup" => dedup = true,
            "--recompress" => recompress = true,
            "--target-size-mb" => {
                target_size_mb = Some(it.next().context("--target-size-mb needs a value")?.parse().context("--target-size-mb must be an integer")?)
            }
            other => anyhow::bail!(
                "unknown argument: {other} (usage: timefusion optimize [--table T] [--date YYYY-MM-DD | --older-than-hours N | --all] [--project ID] [--concurrency N] [--consolidate [--target-size-mb N]] [--dedup] [--recompress] [--dry-run])"
            ),
        }
    }
    if target_size_mb.is_some() && !consolidate {
        anyhow::bail!("--target-size-mb only applies to --consolidate");
    }

    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    // Attach the tantivy sidecar service exactly like the server bootstrap.
    // Without it `tantivy_indexer()` is None, so the post-optimize reindex/GC
    // hooks silently no-op: every CLI compaction orphans the rewritten files'
    // index entries and leaves its outputs unindexed until a server backfill.
    let db = match (cfg.tantivy.indexed_tables().is_empty(), cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default()) {
        (false, bucket) if !bucket.is_empty() => {
            let storage_uri = format!("s3://{bucket}/{}/tantivy", cfg.core.timefusion_table_prefix);
            let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
            db.with_tantivy_indexer(Arc::new(timefusion::tantivy::search::TantivyIndexService::new(obj_store, Arc::new(cfg.tantivy.clone()))))
        }
        _ => db,
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

    // --dry-run: list candidate partitions + file counts, mutate nothing.
    if dry_run {
        let uris: Vec<String> = timefusion::database::file_uris(&*table_ref.read().await);
        println!("DRY RUN — {} candidate partition(s) of '{}' ({}):", dates.len(), table, scope);
        let pid_frag = project.as_deref().map_or(String::new(), |p| format!("project_id={p}/"));
        let total: usize = dates
            .iter()
            .map(|d| {
                let n = uris.iter().filter(|u| u.contains(&pid_frag) && u.contains(&format!("date={d}"))).count();
                println!("  date={d}: {n} files");
                n
            })
            .sum();
        println!("total {total} files across {} candidate partition(s) (no changes made)", dates.len());
        return db.shutdown().await;
    }

    // `--recompress` is the ONLY force-rewrite. Bin-packing (`Compact`/`SortBy`,
    // and `consolidate`'s leveled variant) skips files already at target AND
    // drops single-file bins, so a lone file can never be rewritten by them —
    // which is exactly the shape of a partition poisoned by ONE file with no
    // `sorting_columns` footer. On prod 2026-08-07 that was 448 of 501 poisoned
    // partitions: `optimize` and `--consolidate` both reported success having
    // changed nothing (`removed=0 added=0`, file bytes identical).
    //
    // `recompress_partition` rewrites the partition through `replace_where`
    // with the schema ORDER BY, regardless of file count or size, so the output
    // carries an honest sorted footer. `--project` narrows the overwrite
    // predicate to `date = '...' AND project_id = '...'`, which is what makes
    // the job small enough to run on an ordinary runner.
    if recompress {
        for d in &dates {
            let level = cfg.parquet.timefusion_zstd_compression_level;
            let scope = project.as_deref().map(|p| format!(" project={p}")).unwrap_or_default();
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
        // Leveled event-time-disjoint consolidation (the cold sweep's engine,
        // pointed at any date/target) and/or a dedup pass, per project so a
        // busy day's tens of GB never sit in one merge. Oldest event-time
        // slices rewrite first; incremental per-run commits make an
        // interrupted run resumable.
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
                    // Committed runs persist across attempts (excluded from
                    // re-selection), so retrying after a transient S3/OCC error
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
    // Effectful loop: each partition awaits a compaction and prints as it lands.
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

    #[test]
    fn liveness_accepts_authentication_and_startup_error_only() {
        assert!(pgwire_ready_at(one_response(vec![b'R'])).is_ok());
        let error = |code: &[u8]| {
            let mut payload = vec![b'C'];
            payload.extend_from_slice(code);
            payload.extend_from_slice(&[0, 0]);
            let mut response = vec![b'E'];
            response.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
            response.extend_from_slice(&payload);
            response
        };
        assert!(pgwire_ready_at(one_response(error(b"57P03"))).is_ok());
        assert!(pgwire_ready_at(one_response(error(b"XX000"))).is_err());
    }

    /// The probe and the Dockerfile are one budget split across two files, and
    /// the split is only correct in one direction: if Docker's `--timeout` is
    /// below the probe's own worst case, Docker kills the probe before it can
    /// report a verdict, and every slow-but-alive moment counts as a failure.
    /// That is the prod 2026-08-08 shape — a HEALTHY task replaced mid-repair.
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

    /// Workers must not run on Tokio's default stack.
    ///
    /// A stack overflow in a worker aborts the process, so this is a restart
    /// loop rather than a failed query — that is exactly how prod fell over on
    /// 2026-08-16 while planning a merge-on-read UPDATE. The builder call is one
    /// token to lose in a refactor and nothing else would notice, so pin it.
    #[test]
    fn workers_get_more_than_the_default_stack() {
        assert!(include_str!("main.rs").contains(".thread_stack_size(WORKER_STACK_BYTES)"), "the runtime must actually be built with WORKER_STACK_BYTES");
    }
}
