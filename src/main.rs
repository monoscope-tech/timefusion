// main.rs
use timefusion::batch_queue::{BatchQueue};
use timefusion::database::{Database};
use actix_web::{middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use datafusion_postgres::ServerOptions;
use dotenv::dotenv;
use futures::TryFutureExt;
use serde::Deserialize;
use std::{env, sync::Arc};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct AppInfo {}

#[derive(Deserialize)]
struct RegisterProjectRequest {
    project_id: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    endpoint: Option<String>,
    table_name: Option<String>,
}

#[post("/register_project")]
async fn register_project(req: web::Json<RegisterProjectRequest>, db: web::Data<Arc<Database>>) -> impl Responder {
    // Build the full S3 path for the project-specific table
    let prefix = std::env::var("TIMEFUSION_TABLE_PREFIX").unwrap_or_else(|_| "timefusion".to_string());
    let endpoint = req.endpoint.as_deref().unwrap_or("https://s3.amazonaws.com");
    let storage_uri = format!("s3://{}/{}/projects/{}/?endpoint={}", req.bucket, prefix, req.project_id, endpoint);
    
    match db
        .register_project(
            &req.project_id,
            &storage_uri,
            Some(&req.access_key),
            Some(&req.secret_key),
            Some(endpoint),
            req.table_name.as_deref(),
        )
        .await
    {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({
            "message": format!("Project '{}' registered successfully", req.project_id),
            "table_path": storage_uri
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to register project: {:?}", e)
        })),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize environment and logging
    dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    info!("Starting TimeFusion application");

    // Initialize database
    let mut db = Database::new().await?;
    info!("Database initialized successfully");

    // Setup batch processing with configurable params
    let interval_ms = env::var("BATCH_INTERVAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let max_size = env::var("MAX_BATCH_SIZE").ok().and_then(|v| v.parse().ok()).unwrap_or(1000);
    let enable_queue = env::var("ENABLE_BATCH_QUEUE").unwrap_or_else(|_| "false".to_string()) == "true";

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
    let session_context = db.create_session_context();
    db.setup_session_context(&session_context)?;

    // Wrap for sharing
    let db = Arc::new(db);
    let app_info = web::Data::new(AppInfo {});

    // Setup cancellation token for clean shutdown
    let shutdown_token = CancellationToken::new();
    let http_shutdown = shutdown_token.clone();

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


    // Start HTTP server
    let http_addr = format!("0.0.0.0:{}", env::var("PORT").unwrap_or_else(|_| "80".to_string()));
    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db.clone()))
            .app_data(app_info.clone())
            .service(register_project)
    });

    let server = match http_server.bind(&http_addr) {
        Ok(s) => {
            info!("HTTP server running on http://{}", http_addr);
            s.run()
        }
        Err(e) => {
            error!("Failed to bind HTTP server to {}: {:?}", http_addr, e);
            return Err(anyhow::anyhow!("Failed to bind HTTP server: {:?}", e));
        }
    };

    let http_server_handle = server.handle();
    let http_task = tokio::spawn(async move {
        tokio::select! {
            _ = http_shutdown.cancelled() => info!("HTTP server shutting down."),
            res = server => res.map_or_else(
                |e| error!("HTTP server failed: {:?}", e),
                |_| info!("HTTP server shut down gracefully")
            ),
        }
    });

    let pg_task = tokio::spawn(async move {
        let opts = ServerOptions::new()
            .with_port(pg_port)
            .with_host("0.0.0.0".to_string());

        datafusion_postgres::serve(Arc::new(session_context), &opts).await
    });

    // Wait for shutdown signal
    tokio::select! {
        _ = pg_task => {error!("PGWire server task failed")},
        _ = http_task.map_err(|e| error!("HTTP server task failed: {:?}", e)) => {},
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating shutdown");

            // Shutdown in order: batch queue first to flush pending data
            batch_queue.shutdown().await;
            shutdown_token.cancel();
            http_server_handle.stop(true).await;
            sleep(Duration::from_secs(1)).await;
        }
    }

    info!("Shutdown complete.");
    Ok(())
}
