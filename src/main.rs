// main.rs
#![recursion_limit = "512"]

use std::sync::Arc;

use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use timefusion::{
    buffered_write_layer::BufferedWriteLayer,
    clock,
    config::{self, AppConfig},
    database::Database,
    secret_crypto, telemetry,
};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

fn main() -> anyhow::Result<()> {
    // Initialize environment before any threads spawn
    dotenv().ok();

    // CLI helper: `timefusion encrypt-secret <plaintext>` — prints ciphertext
    // for use in `timefusion_projects` rows, then exits.
    if std::env::args().nth(1).as_deref() == Some("encrypt-secret") {
        return secret_crypto::run_cli();
    }

    // Initialize global config from environment - validates all settings upfront
    let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    // Set WALRUS_DATA_DIR before Tokio runtime starts (required by walrus-rust)
    // SAFETY: No threads exist yet - we're before tokio::runtime::Builder
    unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

    // Build and run Tokio runtime after env vars are set
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async_main(cfg))
}

async fn async_main(cfg: &'static AppConfig) -> anyhow::Result<()> {
    // Initialize OpenTelemetry with OTLP exporter
    telemetry::init_telemetry(&cfg.telemetry)?;
    clock::init_from_env();

    info!("Starting TimeFusion application");

    // Create Arc<AppConfig> for passing to components
    let cfg_arc = Arc::new(cfg.clone());

    // Bind :5432 immediately, before the slow startup work (Database open,
    // WAL recovery — up to ~15 min when WAL has accumulated). Clients
    // connecting in this window get SQLSTATE 57P03 ("starting up") from
    // the early-bind responder instead of ECONNREFUSED, which is what
    // Hasql / pgjdbc / libpq expect during a backend restart and retry
    // on cleanly. See pgwire_early_bind for the responder.
    let pg_port = cfg.core.pgwire_port;
    let pg_backlog = 4096u32;
    let pg_listener = Arc::new(bind_pgwire_listener("0.0.0.0", pg_port, pg_backlog).await?);
    info!("PGWire listener bound on :{} (backlog={}) before startup work begins", pg_port, pg_backlog);
    let early_shutdown = tokio_util::sync::CancellationToken::new();
    let early_shutdown_for_task = early_shutdown.clone();
    let early_listener = Arc::clone(&pg_listener);
    let early_task = tokio::spawn(async move {
        timefusion::pgwire_early_bind::run_until_ready(&early_listener, early_shutdown_for_task).await;
    });

    // Initialize database with explicit config
    let mut db = Database::with_config(Arc::clone(&cfg_arc)).await?;
    info!("Database initialized successfully");

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
                // Capture pre-state file URIs so we can derive the post-write delta.
                let pre = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                // skip_queue=true to write directly to Delta. Watermark goes into
                // Delta commit metadata for crash-mid-flush recovery.
                db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await?;
                let post = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                let pre_set: std::collections::HashSet<String> = pre.into_iter().collect();
                let added: Vec<String> = post.into_iter().filter(|u| !pre_set.contains(u)).collect();
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
    let mut layer = BufferedWriteLayer::with_config(cfg_arc.clone(), registry)?.with_delta_writer(delta_write_callback);
    let mut tantivy_svc_for_metrics: Option<Arc<timefusion::tantivy_index::service::TantivyIndexService>> = None;
    let indexed_tables = cfg.tantivy.indexed_tables();
    if !indexed_tables.is_empty() {
        let bucket = cfg.aws.aws_s3_bucket.clone().unwrap_or_default();
        if !bucket.is_empty() {
            let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
            let storage_opts = cfg.aws.build_storage_options(None);
            let obj_store = db.create_object_store(&storage_uri, &storage_opts).await?;
            let svc = Arc::new(timefusion::tantivy_index::service::TantivyIndexService::new(
                obj_store.clone(),
                Arc::new(cfg.tantivy.clone()),
            ));
            layer = layer.with_tantivy_indexer(svc.clone().callback());
            let cache_root = cfg.core.timefusion_data_dir.clone();
            let search = Arc::new(timefusion::tantivy_index::search::TantivySearchService::new(obj_store, cache_root));
            db = db.with_tantivy_search(search).with_tantivy_indexer(svc.clone());
            tantivy_svc_for_metrics = Some(svc);
            info!("Tantivy sidecar indexes active for tables: {:?}", indexed_tables);
        } else {
            error!("Schema declares indexed columns but AWS_S3_BUCKET is unset — Tantivy disabled, queries will scan");
        }
    }
    let buffered_layer = Arc::new(layer);

    // Initialize OpenTelemetry metrics — observable gauges read snapshot_stats()
    // each export cycle (30s), keeping the hot path untouched. Weak ref so
    // metrics don't extend the layer's lifetime.
    let tantivy_weak = tantivy_svc_for_metrics.as_ref().map(Arc::downgrade);
    if let Err(e) = timefusion::metrics::init_metrics(&cfg.telemetry, Arc::downgrade(&buffered_layer), tantivy_weak) {
        error!("Failed to initialize OTel metrics: {} — continuing without metrics export", e);
    }

    // Before WAL replay, fast-forward walrus cursors to whatever each table's
    // latest Delta commits say is durable. Closes the crash-mid-flush window
    // where Delta committed but `advance_by_counts` didn't finish — without
    // this, replay re-injects entries already in Delta and the next flush
    // double-writes them. Best-effort: missing/older metadata falls back to
    // the locally-fsynced walrus state (today's at-least-once behaviour).
    match db.derive_wal_cursors_from_delta(buffered_layer.wal()).await {
        Ok(0) => info!("Delta-derived cursor: no advancement needed (clean shutdown)"),
        Ok(n) => info!("Delta-derived cursor: advanced {} shard(s) past Delta watermark", n),
        Err(e) => warn!("Delta-derived cursor derivation failed (continuing with local cursor): {}", e),
    }

    // Recover from WAL on startup
    info!("Starting WAL recovery...");
    let recovery_stats = buffered_layer.recover_from_wal().await?;
    info!(
        "WAL recovery complete: {} entries replayed in {}ms",
        recovery_stats.entries_replayed, recovery_stats.recovery_duration_ms
    );

    // Start background tasks (flush and eviction)
    buffered_layer.start_background_tasks().await;
    info!("BufferedWriteLayer background tasks started");

    // Apply buffered layer to database
    db = db.with_buffered_layer(Arc::clone(&buffered_layer));

    // Start maintenance schedulers for regular optimize and vacuum
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;

    // Start PGWire server on the listener we pre-bound at the top of
    // async_main. First, hand control of that listener back from the
    // early-bind 57P03 responder.
    info!("Stopping early-bind 57P03 responder; handing listener to real PGWire server");
    early_shutdown.cancel();
    let _ = early_task.await;
    // Both Arc handles dropped now (early_task held the only other one);
    // try_unwrap should succeed and give us the owned listener.
    let listener = Arc::try_unwrap(pg_listener)
        .map_err(|_| anyhow::anyhow!("PGWire listener still has outstanding Arc references after early-bind shutdown"))?;

    let auth_config = timefusion::pgwire_handlers::AuthConfig::from_core(&cfg.core)?;

    // PGWire shutdown signal: when cancelled, the accept loop in
    // `serve_with_handlers` stops accepting new connections so the
    // BufferedWriteLayer flush isn't racing fresh inserts. Already-accepted
    // connections finish on their own spawned tasks.
    let pgwire_shutdown = tokio_util::sync::CancellationToken::new();
    let pgwire_shutdown_for_task = pgwire_shutdown.clone();
    let pg_task = tokio::spawn(async move {
        let opts = ServerOptions::new().with_port(pg_port).with_host("0.0.0.0".to_string()).with_backlog(pg_backlog);

        if let Err(e) = timefusion::pgwire_handlers::serve_with_listener(listener, Arc::new(session_context), &opts, auth_config, async move {
            pgwire_shutdown_for_task.cancelled().await
        })
        .await
        {
            error!("PGWire server error: {}", e);
        }
    });

    // Start gRPC ingestion server alongside PGWire
    let grpc_port = cfg.core.grpc_port;
    // GRPC_TOKEN: required shared bearer token. Clients send
    // `Authorization: Bearer <token>` and the server (grpc_handlers.rs)
    // compares against this env var. Same fail-secure posture as
    // PGWIRE_PASSWORD — opt out for local dev only via
    // TIMEFUSION_ALLOW_INSECURE_AUTH=true.
    let grpc_token = {
        let allow_insecure = std::env::var("TIMEFUSION_ALLOW_INSECURE_AUTH").map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(false);
        match (&cfg.core.grpc_token, allow_insecure) {
            (Some(t), _) if !t.is_empty() => Some(t.clone()),
            (_, true) => {
                warn!("GRPC_TOKEN unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — gRPC ingest accepts any client. Local dev ONLY.");
                None
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "GRPC_TOKEN is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true to opt into open ingest for local dev)"
                ));
            }
        }
    };
    // gRPC shutdown signal: tonic's `serve_with_shutdown` polls this future
    // and stops accepting new requests once it resolves. In-flight requests
    // are then awaited up to the server's drain timeout.
    let grpc_shutdown = tokio_util::sync::CancellationToken::new();
    let grpc_shutdown_for_task = grpc_shutdown.clone();
    let db_for_grpc = Arc::clone(&db);
    let grpc_task = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{grpc_port}").parse().expect("valid grpc addr");
        info!("Starting gRPC ingestion server on port: {}", grpc_port);
        let svc = timefusion::grpc_handlers::IngestService::new(db_for_grpc, grpc_token).into_server();
        let serve = tonic::transport::Server::builder().add_service(svc).serve_with_shutdown(addr, async move {
            grpc_shutdown_for_task.cancelled().await;
            info!("gRPC server: shutdown signal received, draining in-flight requests");
        });
        if let Err(e) = serve.await {
            error!("gRPC server error: {}", e);
        } else {
            info!("gRPC server: shutdown complete");
        }
    });

    // Store references for shutdown
    let db_for_shutdown = db.clone();
    let buffered_layer_for_shutdown = Arc::clone(&buffered_layer);

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

    // Wait for shutdown signal. Borrow `pg_task` so we can still await it
    // in the drain phase below — the select! only watches it for early
    // failure, not for ownership.
    let mut pg_task = pg_task;
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
    }

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
    pgwire_shutdown.cancel();
    let pgwire_drain_deadline = Duration::from_secs(cfg.buffer.timefusion_shutdown_timeout_secs.max(5));
    match tokio::time::timeout(pgwire_drain_deadline, pg_task).await {
        Ok(Ok(())) => info!("PGWire drained cleanly"),
        Ok(Err(e)) => error!("PGWire task panicked during drain: {}", e),
        Err(_) => warn!(
            "PGWire drain exceeded {}s — proceeding with flush; some in-flight queries may be reset",
            pgwire_drain_deadline.as_secs()
        ),
    }

    grpc_shutdown.cancel();
    let grpc_drain_deadline = Duration::from_secs(cfg.buffer.timefusion_shutdown_timeout_secs.max(5));
    match tokio::time::timeout(grpc_drain_deadline, grpc_task).await {
        Ok(Ok(())) => info!("gRPC drained cleanly"),
        Ok(Err(e)) => error!("gRPC task panicked during drain: {}", e),
        Err(_) => error!(
            "gRPC drain exceeded {}s — forcing shutdown; in-flight requests may be reset",
            grpc_drain_deadline.as_secs()
        ),
    }

    if let Err(e) = buffered_layer_for_shutdown.shutdown().await {
        error!("Error during buffered layer shutdown: {}", e);
    }
    sleep(Duration::from_millis(500)).await;

    if let Err(e) = db_for_shutdown.shutdown().await {
        error!("Error during database shutdown: {}", e);
    }

    info!("Shutdown complete.");

    // Shutdown telemetry to ensure all spans are flushed
    telemetry::shutdown_telemetry();

    Ok(())
}

/// Bind the PGWire listener on `host:port` with an explicit backlog of
/// `backlog`. Uses `TcpSocket` rather than `TcpListener::bind` so we
/// control the backlog (mio's `bind` hardcodes 128); the kernel still
/// clamps to `somaxconn`. Pulled out as its own function so the early
/// 57P03 responder and the real server share one bind path.
async fn bind_pgwire_listener(host: &str, port: u16, backlog: u32) -> anyhow::Result<tokio::net::TcpListener> {
    use tokio::net::{lookup_host, TcpSocket};
    let server_addr = format!("{host}:{port}");
    let addr = lookup_host(&server_addr).await?.next().ok_or_else(|| anyhow::anyhow!("could not resolve {server_addr}"))?;
    let socket = if addr.is_ipv4() { TcpSocket::new_v4()? } else { TcpSocket::new_v6()? };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    Ok(socket.listen(backlog)?)
}
