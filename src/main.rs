// main.rs
#![recursion_limit = "512"]

// Production profiling (--features profiling, Linux): jemalloc as the global
// allocator with its heap profiler, plus a pprof CPU sampler (started in
// async_main). Deployed to attribute the prod OOM. See src/profiling.rs.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc reads this symbol at startup — bakes the profiler config into the
// binary so no MALLOC_CONF env (host is read-only) is needed. prof_active
// samples live allocations; lg_prof_sample:19 = ~512KiB sampling (low
// overhead); lg_prof_interval:35 auto-dumps a .heap every ~32GiB allocated so
// the retained dump window spans the whole boot→OOM RSS climb (at :33 prod
// churned ~80 dumps/min and rotation kept only the last minute — useless for
// growth diffs, 2026-07-31);
// prof_prefix points into the data-dir volume we can read off the host.
// Analyze: `jeprof --svg <binary> <prof_prefix>.*.heap`.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[unsafe(export_name = "malloc_conf")]
// background_thread + 1s dirty decay: without background threads jemalloc only
// purges dirty pages on allocation events per-arena, so 4-8GB/min of write-path
// churn retained ~2/3 of RSS as freed-but-unreturned pages (2026-07-29: 26GB
// live heap vs 115GB RSS at the cgroup OOM kill).
// `prof_active:false` since 2026-08-02: the attribution this was deployed for is
// DONE (giant-INSERT parse ASTs, snapshot refresh, RowConverter — 2026-07-31), and
// leaving sampling on costs CPU and heap on a box whose memory headroom is the
// binding constraint on compaction: the maintenance brake fires at 70% of the
// cgroup (84GB) and RSS oscillates 51-99GB, so every GB back is brake headroom.
// `lg_prof_interval:35` also dumps a .heap file every 32GB allocated, onto the
// same volume that has twice filled with maintenance spill.
//
// `prof:true` is KEPT so profiling can be re-armed at runtime via the
// `prof.active` mallctl without a rebuild — flip this to `prof_active:true` (or
// set it via mallctl) whenever heap attribution is needed again.
//
// Re-armed 2026-08-03: the OOMs did NOT stop after the 07-31 attribution fixes
// (70 cgroup kills over 08-01/02, ~hourly under dashboard load) and the 00:45Z
// kill was 125GB of pure anon — a regime the 07-31 dumps never covered. Flip
// back to false once the current eater is named.
// `dirty_decay_ms:0` since 2026-08-03: return freed dirty pages to the OS
// IMMEDIATELY. Six cgroup OOM kills tonight at anon-rss ~125GB were, per the
// 2026-07-31 attribution ratio (live 31GB vs RSS 82GB) and tonight's clean
// A/B, dominated by freed-but-unreturned churn that decay=1000 never caught
// up with (maintenance planning replays ~34k files' stats per query; Foyer
// still copies every parquet range). With decay 0 + the dirty-bin drain
// paused, RSS held ~30GB at 45min where every prior boot hit 60-100GB. The
// madvise cost is real but this box is IO-bound, not CPU-bound; do NOT raise
// this back without re-running that A/B under maintenance load.
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19,lg_prof_interval:35,prof_prefix:/app/data/timefusion/profiles/jeprof,background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0\0";

use std::sync::Arc;

use anyhow::Context;
use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use timefusion::{
    bootstrap,
    buffered_write_layer::BufferedWriteLayer,
    clock,
    config::{self, AppConfig},
    database::{Database, RecompressOutcome},
    secret_crypto, telemetry,
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

fn main() -> anyhow::Result<()> {
    // Initialize environment before any threads spawn
    dotenv().ok();
    // Before the runtime, so every worker thread and listener socket inherits
    // the raised limit. `bootstrap()` calls it too, for the e2e harness — which
    // does NOT go through main().
    bootstrap::raise_file_limit();

    let subcommand = std::env::args().nth(1);
    if subcommand.as_deref() == Some("healthcheck") {
        return run_pgwire_healthcheck();
    }
    // CLI helper: `timefusion encrypt-secret <plaintext>` — prints ciphertext
    // for use in `timefusion_projects` rows, then exits. Needs no config.
    if subcommand.as_deref() == Some("encrypt-secret") {
        return secret_crypto::run_cli();
    }

    // Maintenance CLIs get the maintenance-heavy budget shape (the server
    // shape strands a small cgroup's memory in query/ingest slices a one-shot
    // CLI never uses). Must precede init_config, which snapshots the tree.
    // SAFETY: no threads exist yet - we're before the Tokio runtime is built.
    if matches!(subcommand.as_deref(), Some("optimize" | "redrive-dml" | "migrate-columns")) {
        unsafe { std::env::set_var("TIMEFUSION_BUDGET_PROFILE", "maintenance-cli") };
    }

    // Initialize global config from environment - validates all settings upfront
    let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    // (No WALRUS_DATA_DIR here any more: `WalManager` passes `cfg.core.wal_dir()`
    // to `Walrus::with_root`, so the WAL location is a parameter rather than a
    // process-global. Same on-disk path, minus the global.)
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    match subcommand.as_deref() {
        Some("redrive-dml") => rt.block_on(run_redrive_dml_cli(cfg)),
        Some("optimize") => rt.block_on(run_optimize_cli(cfg)),
        Some("migrate-columns") => rt.block_on(run_migrate_columns_cli(cfg)),
        _ => {
            let result = rt.block_on(async_main(cfg));
            // The server path must END THE PROCESS here: dropping the runtime
            // waits on lingering blocking/detached threads, and twice on
            // 2026-08-06 that hang left a post-"Shutdown complete." zombie
            // container that blocked swarm from starting the replacement
            // (pgwire outage until a manual `docker stop`). Everything durable
            // is already on disk by now, so skip teardown and exit.
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

/// Docker readiness probe that distinguishes the early 57P03 responder from
/// the real PGWire server. A PostgreSQL AuthenticationRequest (`R`) proves the
/// request reached the real handler; the early responder returns ErrorResponse
/// (`E`) with SQLSTATE 57P03 and therefore stays out of a start-first VIP.
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

fn pgwire_ready_at(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    let timeout = PROBE_OP_TIMEOUT;
    let mut stream = std::net::TcpStream::connect_timeout(&addr, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let body = b"user\0timefusion_healthcheck\0database\0postgres\0\0";
    let mut startup = Vec::with_capacity(8 + body.len());
    startup.extend_from_slice(&((8 + body.len()) as u32).to_be_bytes());
    startup.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    startup.extend_from_slice(body);
    stream.write_all(&startup)?;
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag)?;
    anyhow::ensure!(tag[0] == b'R', "PGWire is bound but not ready (response tag {:?})", tag[0] as char);
    Ok(())
}

fn init_cli_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .try_init();
}

/// `timefusion redrive-dml [--dir PATH] [--dry-run]` — replay parked quarantine/dml
/// enrichment groups (see [`timefusion::dml_coalescer::redrive_dml_quarantine`]).
async fn run_redrive_dml_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    // Two-token flags need lookahead into the arg iterator, so this stays a loop.
    let mut dir = cfg.core.wal_dir().join(timefusion::wal::QUARANTINE_DIR_NAME).join("dml");
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
    let (ok, skipped) = timefusion::dml_coalescer::redrive_dml_quarantine(&db, &dir, dry_run).await;
    println!("redrive-dml: {ok} recovered, {skipped} left parked (dir {dir:?})");
    db.shutdown().await
}

async fn async_main(cfg: &'static AppConfig) -> anyhow::Result<()> {
    // Initialize OpenTelemetry with OTLP exporter
    telemetry::init_telemetry(&cfg.telemetry)?;
    // AFTER init_telemetry: config is built before the subscriber exists, so
    // logging the tree at derivation time is silently swallowed — which is why
    // prod could carry TIMEFUSION_MEMORY_LIMIT_GB=26 while actually budgeting
    // 120 GiB with nothing on the box revealing the gap (2026-07-31).
    config::log_derived_budget(&cfg.derived);
    clock::init_from_env();

    // Start heap+CPU profiling (no-op unless --features profiling on Linux).
    // Early, so the profiles dir exists before jemalloc's first interval dump.
    timefusion::profiling::start(cfg.core.timefusion_data_dir.clone());

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
            timefusion::pgwire_early_bind::run_until_ready(&pg_listener, shutdown).await;
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
    let _wal_dir_lock = timefusion::wal::WalDirLock::acquire(&cfg.core.wal_dir()).await?;

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
    let delta_write_callback: timefusion::buffered_write_layer::DeltaWriteCallback = Arc::new(
        move |project_id: String,
              table_name: String,
              batches: Vec<arrow::array::RecordBatch>,
              wal_watermark: timefusion::buffered_write_layer::DeltaWatermark| {
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
        },
    );

    // Register UDFs on the real SessionContext up front so its FunctionRegistry
    // doubles as the WAL-replay registry — no throwaway bootstrap context.
    // Table providers depend on buffered_layer and are registered after recovery.
    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<timefusion::functions::FnRegistry> = Arc::new(session_context.state());

    // Tantivy sidecar indexes are always-on whenever at least one table has
    // `tantivy.indexed: true` fields in its YAML schema (or appears in the
    // optional `TIMEFUSION_TANTIVY_INDEXED_TABLES` override). The query layer
    // accelerates standard SQL predicates (`=`, `LIKE 'prefix%'`) via the
    // TantivyPredicateRewriter — callers don't need to know tantivy exists.
    // Pre-init WAL GC (gated + drained-flag consumption inside the helper).
    timefusion::wal::boot_wal_gc(&cfg.core.wal_dir());

    let t_layer = std::time::Instant::now();
    let mut layer = BufferedWriteLayer::with_config(cfg_arc.clone(), registry)?
        .with_delta_writer(delta_write_callback)
        .with_coalesced_delta_writer(timefusion::bootstrap::coalesced_delta_write_callback(&db));
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
        let svc = Arc::new(timefusion::tantivy_index::service::TantivyIndexService::new(obj_store.clone(), Arc::new(cfg.tantivy.clone())));
        layer = layer.with_tantivy_indexer(svc.clone().callback());
        let search = Arc::new(timefusion::tantivy_index::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone()));
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc.clone());
        info!("Tantivy sidecar indexes active for tables: {:?}", indexed_tables);
        Some(svc)
    };
    let buffered_layer = Arc::new(layer);

    // Initialize OpenTelemetry metrics — observable gauges read snapshot_stats()
    // each export cycle (30s), keeping the hot path untouched. Weak ref so
    // metrics don't extend the layer's lifetime.
    if let Err(e) = timefusion::metrics::init_metrics(&cfg.telemetry, Arc::downgrade(&buffered_layer), tantivy_svc_for_metrics.as_ref().map(Arc::downgrade)) {
        error!("Failed to initialize OTel metrics: {} — continuing without metrics export", e);
    }

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
        let age_secs = timefusion::clock::now_micros().saturating_sub(snap.written_at_micros) / 1_000_000;
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

    let auth_config = timefusion::pgwire_handlers::AuthConfig::from_core(&cfg.core)?;

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
            if let Err(e) = timefusion::pgwire_handlers::serve_with_listener(
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

    // Start gRPC ingestion server alongside PGWire
    let grpc_port = cfg.core.grpc_port;
    // GRPC_TOKEN: required shared bearer token. Clients send
    // `Authorization: Bearer <token>` and the server (grpc_handlers.rs)
    // compares against this env var. Same fail-secure posture as
    // PGWIRE_PASSWORD — opt out for local dev only via
    // TIMEFUSION_ALLOW_INSECURE_AUTH=true.
    let grpc_token = match (&cfg.core.grpc_token, config::is_insecure_auth_allowed()) {
        (Some(t), _) if !t.is_empty() => Some(t.clone()),
        (_, true) => {
            warn!("GRPC_TOKEN unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — gRPC ingest accepts any client. Local dev ONLY.");
            None
        }
        _ => return Err(anyhow::anyhow!("GRPC_TOKEN is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true to opt into open ingest for local dev)")),
    };
    // gRPC shutdown signal: tonic's `serve_with_shutdown` polls this future
    // and stops accepting new requests once it resolves. In-flight requests
    // are then awaited up to the server's drain timeout.
    let grpc_shutdown = tokio_util::sync::CancellationToken::new();
    let grpc_task = tokio::spawn({
        let shutdown = grpc_shutdown.clone();
        let db_for_grpc = Arc::clone(&db);
        async move {
            let addr = format!("0.0.0.0:{grpc_port}").parse().expect("valid grpc addr");
            info!("Starting gRPC ingestion server on port: {}", grpc_port);
            let svc = timefusion::grpc_handlers::IngestService::new(db_for_grpc, grpc_token).into_server();
            let serve = tonic::transport::Server::builder().add_service(svc).serve_with_shutdown(addr, async move {
                shutdown.cancelled().await;
                info!("gRPC server: shutdown signal received, draining in-flight requests");
            });
            match serve.await {
                Err(e) => error!("gRPC server error: {}", e),
                Ok(()) => info!("gRPC server: shutdown complete"),
            }
        }
    });

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
            if !timefusion::wal::takeover_requested(&wal_dir) {
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
            if timefusion::wal::takeover_request_age(&wal_dir).is_some_and(|age| age >= timefusion::wal::TAKEOVER_ESCALATE_AFTER) {
                warn!(
                    "WAL takeover requested {}s ago and this instance never reached handoff readiness; shutting down anyway so the replacement can start",
                    timefusion::wal::TAKEOVER_ESCALATE_AFTER.as_secs()
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
    // 1. Tell gRPC to stop accepting new connections. tonic's
    //    serve_with_shutdown then waits for existing streams to complete.
    // 2. Once gRPC is done, the buffered layer no longer receives new
    //    writes — safe to flush + checkpoint.
    // 3. Shut down database (cache, foyer, log store).
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
    let pg_drain_budget = if preflushed_handoff { Duration::from_millis(50) } else { grace.mul_f32(0.2) };
    match tokio::time::timeout(pg_drain_budget, pg_task).await {
        Ok(Ok(())) => info!("PGWire drained cleanly"),
        Ok(Err(e)) => error!("PGWire task panicked during drain: {}", e),
        Err(_) => warn!("PGWire drain exceeded its slice of the stop grace — proceeding; in-flight queries may be reset"),
    }

    grpc_shutdown.cancel();
    let grpc_drain_budget = if preflushed_handoff { Duration::from_millis(50) } else { grace.mul_f32(0.1) };
    match tokio::time::timeout(grpc_drain_budget, grpc_task).await {
        Ok(Ok(())) => info!("gRPC drained cleanly"),
        Ok(Err(e)) => error!("gRPC task panicked during drain: {}", e),
        Err(_) => error!("gRPC drain exceeded its slice of the stop grace — proceeding; in-flight requests may be reset"),
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

/// One-off compaction CLI (`timefusion optimize [...]`). Compacts old `date=`
/// partitions — those outside the scheduled 48h Z-order window that the periodic
/// job never reaches — using `Database::compact_date` per partition. Intended to
/// run off-box against prod storage so it doesn't load the live server's memory —
/// it commits via the same S3/R2 conditional-put (If-None-Match) coordination as the
/// live server, so concurrent commits conflict-detect safely (OCC retry).
/// Evolve a live table's STORED Delta schema to include new nullable columns,
/// WITHOUT changing any YAML.
///
/// This exists because a shipped table cannot gain a column by editing the YAML
/// alone. The YAML is only one of two schemas; the other lives in each Delta
/// table's transaction log and is whatever was there at creation. Editing the
/// YAML makes the write path build wider batches while storage still declares
/// the old shape, which is how 7d68f01 produced `number of columns(94) must
/// match number of fields(92)`, 268 flush failures and rejected INSERTs within
/// minutes of deploy. See the doc block at the top of `schema_loader.rs`.
///
/// Run this against prod FIRST; only once every live table carries the columns
/// may the YAML declare them. It writes a ZERO-ROW batch at the widened schema
/// with `SchemaMode::Merge`, so it adds metadata and no data, and re-running it
/// is a no-op.
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
            db.with_tantivy_indexer(Arc::new(timefusion::tantivy_index::service::TantivyIndexService::new(obj_store, Arc::new(cfg.tantivy.clone()))))
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
        let uris: Vec<String> = table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default();
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

    fn one_response(tag: u8) -> std::net::SocketAddr {
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
            stream.write_all(&[tag]).unwrap();
        });
        addr
    }

    #[test]
    fn readiness_accepts_authentication_and_rejects_early_error() {
        assert!(pgwire_ready_at(one_response(b'R')).is_ok());
        assert!(pgwire_ready_at(one_response(b'E')).is_err());
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
}
