// main.rs
#![recursion_limit = "512"]

use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use std::sync::Arc;
use timefusion::buffered_write_layer::BufferedWriteLayer;
use timefusion::clock;
use timefusion::config::{self, AppConfig};
use timefusion::database::Database;
use timefusion::telemetry;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

fn main() -> anyhow::Result<()> {
    // Initialize environment before any threads spawn
    dotenv().ok();

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
    let delta_write_callback: timefusion::buffered_write_layer::DeltaWriteCallback =
        Arc::new(move |project_id: String, table_name: String, batches: Vec<arrow::array::RecordBatch>| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                // Capture pre-state file URIs so we can derive the post-write delta.
                let pre = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                // skip_queue=true to write directly to Delta
                db.insert_records_batch(&project_id, &table_name, batches, true).await?;
                let post = db.list_file_uris(&project_id, &table_name).await.unwrap_or_default();
                let pre_set: std::collections::HashSet<String> = pre.into_iter().collect();
                let added: Vec<String> = post.into_iter().filter(|u| !pre_set.contains(u)).collect();
                Ok(added)
            })
        });

    // Optional sidecar tantivy index callback. Off by default; enabled when
    // TIMEFUSION_TANTIVY_ENABLED=true and the table is in the indexed list.
    let mut layer = BufferedWriteLayer::with_config(cfg_arc.clone())?.with_delta_writer(delta_write_callback);
    if cfg.tantivy.enabled() {
        let bucket = cfg.aws.aws_s3_bucket.clone().unwrap_or_default();
        if !bucket.is_empty() {
            let storage_uri = format!("s3://{}/{}/tantivy", bucket, cfg.core.timefusion_table_prefix);
            let storage_opts = cfg.aws.build_storage_options(None);
            let obj_store = db.create_object_store(&storage_uri, &storage_opts).await?;
            let svc = Arc::new(timefusion::tantivy_index::service::TantivyIndexService::new(obj_store.clone(), Arc::new(cfg.tantivy.clone())));
            layer = layer.with_tantivy_indexer(svc.clone().callback());
            let cache_root = cfg.core.timefusion_data_dir.clone();
            let search = Arc::new(timefusion::tantivy_index::search::TantivySearchService::new(obj_store, cache_root));
            db = db.with_tantivy_search(search).with_tantivy_indexer(svc);
            info!("Tantivy sidecar indexes enabled for tables: {:?}", cfg.tantivy.indexed_tables());
        } else {
            info!("Tantivy enabled but no AWS_S3_BUCKET configured; skipping");
        }
    }
    let buffered_layer = Arc::new(layer);

    // Initialize OpenTelemetry metrics — observable gauges read snapshot_stats()
    // each export cycle (30s), keeping the hot path untouched. Weak ref so
    // metrics don't extend the layer's lifetime.
    if let Err(e) = timefusion::metrics::init_metrics(&cfg.telemetry, Arc::downgrade(&buffered_layer)) {
        error!("Failed to initialize OTel metrics: {} — continuing without metrics export", e);
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
    let mut session_context = db.clone().create_session_context();
    db.setup_session_context(&mut session_context)?;

    // Start PGWire server
    let pg_port = cfg.core.pgwire_port;
    info!("Starting PGWire server on port: {}", pg_port);

    let auth_config = timefusion::pgwire_handlers::AuthConfig {
        username: cfg.core.pgwire_user.clone(),
        password: cfg.core.pgwire_password.clone(),
    };

    let pg_task = tokio::spawn(async move {
        let opts = ServerOptions::new().with_port(pg_port).with_host("0.0.0.0".to_string());

        if let Err(e) = timefusion::pgwire_handlers::serve_with_logging(Arc::new(session_context), &opts, auth_config).await {
            error!("PGWire server error: {}", e);
        }
    });

    // Start gRPC ingestion server alongside PGWire
    let grpc_port = cfg.core.grpc_port;
    let grpc_token = cfg.core.grpc_token.clone();
    let db_for_grpc = Arc::clone(&db);
    let grpc_task = tokio::spawn(async move {
        let addr = format!("0.0.0.0:{grpc_port}").parse().expect("valid grpc addr");
        info!("Starting gRPC ingestion server on port: {}", grpc_port);
        let svc = timefusion::grpc_handlers::IngestService::new(db_for_grpc, grpc_token).into_server();
        if let Err(e) = tonic::transport::Server::builder().add_service(svc).serve(addr).await {
            error!("gRPC server error: {}", e);
        }
    });

    // Store references for shutdown
    let db_for_shutdown = db.clone();
    let buffered_layer_for_shutdown = Arc::clone(&buffered_layer);

    // Wait for shutdown signal
    tokio::select! {
        _ = pg_task => {error!("PGWire server task failed")},
        _ = grpc_task => {error!("gRPC server task failed")},
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating shutdown");

            // Shutdown buffered layer to flush remaining data to Delta
            if let Err(e) = buffered_layer_for_shutdown.shutdown().await {
                error!("Error during buffered layer shutdown: {}", e);
            }
            sleep(Duration::from_millis(500)).await;

            // Properly shutdown the database including cache
            if let Err(e) = db_for_shutdown.shutdown().await {
                error!("Error during database shutdown: {}", e);
            }
        }
    }

    info!("Shutdown complete.");

    // Shutdown telemetry to ensure all spans are flushed
    telemetry::shutdown_telemetry();

    Ok(())
}
