// main.rs
#![recursion_limit = "512"]

use datafusion_postgres::{ServerOptions, auth::AuthManager};
use dotenv::dotenv;
use std::{env, sync::Arc};
use timefusion::batch_queue::BatchQueue;
use timefusion::database::Database;
use timefusion::telemetry;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize environment and telemetry
    dotenv().ok();

    // Initialize OpenTelemetry with OTLP exporter
    telemetry::init_telemetry()?;

    info!("Starting TimeFusion application");

    // Initialize database (will auto-detect config mode)
    let mut db = Database::new().await?;
    info!("Database initialized successfully");

    // Setup batch processing with configurable params
    let interval_ms = env::var("BATCH_INTERVAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let max_size = env::var("MAX_BATCH_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(100_000);
    let enable_queue = env::var("ENABLE_BATCH_QUEUE").unwrap_or_else(|_| "true".to_string()) == "true";

    // Create batch queue
    let batch_queue = Arc::new(BatchQueue::new(Arc::new(db.clone()), interval_ms, max_size));
    info!(
        "Batch queue configured (enabled={}, interval={}ms, max_size={})",
        enable_queue, interval_ms, max_size
    );

    // Apply and setup
    db = db.with_batch_queue(Arc::clone(&batch_queue));
    // Start maintenance schedulers for regular optimize and vacuum
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    let mut session_context = db.clone().create_session_context();
    db.setup_session_context(&mut session_context)?;

    // Start PGWire server
    let pgwire_port_var = env::var("PGWIRE_PORT");
    info!("PGWIRE_PORT environment variable: {:?}", pgwire_port_var);

    let pg_port = pgwire_port_var
        .unwrap_or_else(|_| {
            info!("PGWIRE_PORT not set, using default port 5432");
            "5432".to_string()
        })
        .parse::<u16>()
        .unwrap_or_else(|e| {
            error!("Failed to parse PGWIRE_PORT value: {:?}, using default 5432", e);
            5432
        });

    info!("Starting PGWire server on port: {}", pg_port);

    let pg_task = tokio::spawn(async move {
        let opts = ServerOptions::new().with_port(pg_port).with_host("0.0.0.0".to_string());
        let auth_manager = Arc::new(AuthManager::new());

        // Use our custom handlers that log UPDATE queries
        if let Err(e) = timefusion::pgwire_handlers::serve_with_logging(Arc::new(session_context), &opts, auth_manager).await {
            error!("PGWire server error: {}", e);
        }
    });

    // Store database for shutdown
    let db_for_shutdown = db.clone();

    // Wait for shutdown signal
    tokio::select! {
        _ = pg_task => {error!("PGWire server task failed")},
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating shutdown");

            // Shutdown batch queue to flush pending data
            batch_queue.shutdown().await;
            sleep(Duration::from_secs(1)).await;

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
