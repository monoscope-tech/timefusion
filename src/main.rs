mod config;
mod database;
mod error;
mod persistent_queue;
mod telemetry;
use std::{env, sync::Arc};

use actix_web::{middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use database::Database;
use dotenv::dotenv;
use futures::TryFutureExt;
use serde::Deserialize;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_actix_web::TracingLogger;
use tracing_subscriber::EnvFilter;

use crate::{
    config::Config,
    error::{Result, TimeFusionError},
};

/// Shared application state containing the database.
struct AppState {
    db: Arc<Database>,
}

struct ShutdownSignal;

/// Request payload for project registration.
#[derive(Deserialize)]
struct RegisterProjectRequest {
    project_id: String,
    bucket:     String,
    access_key: String,
    secret_key: String,
    endpoint:   Option<String>,
}

/// The /register_project endpoint.
///
/// When this endpoint is hit, the system calls `Database::register_project`.
/// That method constructs a full S3 URI (or other object storage URI) from the provided
/// bucket name (if no scheme is provided) along with the credentials and endpoint, then
/// verifies that storage is writable via a dummy write before registering the project.
#[tracing::instrument(
    name = "HTTP /register_project",
    skip(req, app_state),
    fields(project_id = %req.project_id)
)]
#[post("/register_project")]
async fn register_project(
    req: web::Json<RegisterProjectRequest>,
    app_state: web::Data<AppState>
) -> Result<impl Responder> {
    app_state
        .db
        .register_project(
            &req.project_id,
            &req.bucket,
            Some(&req.access_key),
            Some(&req.secret_key),
            req.endpoint.as_deref(),
        )
        .await?;

    info!("Project registered successfully");
    Ok(HttpResponse::Ok().json(serde_json::json!({
        "message": format!("Project '{}' registered successfully", req.project_id)
    })))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Load configuration from environment variables.
    let config = Config::from_env().expect("Failed to load config");

    // Initialize telemetry.
    telemetry::init_telemetry();

    // Initialize tracing subscriber.
    tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env()).init();

    info!("Starting TimeFusion application");

    // Create the database.
    // The default project is registered during startup; for user-initiated project registration,
    // the provided bucket value is used to construct a proper S3 URI inside Database::register_project.
    let db = Database::new(&config).await?;
    info!("Database initialized successfully");

    // Create a DataFusion session context for queries and compaction.
    let session_context = db.create_session_context();
    db.setup_session_context(&session_context)
        .expect("Failed to setup session context");

    let db = Arc::new(db);
    let (shutdown_tx, _shutdown_rx) = mpsc::channel::<ShutdownSignal>(1);
    let shutdown_token = CancellationToken::new();
    let http_shutdown = shutdown_token.clone();

    // Spawn a shutdown monitor to flush pending writes.
    let db_clone = db.clone();
    let shutdown_monitor = shutdown_token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown_monitor.cancelled() => {
                db_clone.flush_pending_writes().await.unwrap_or_else(|e| {
                    error!("Failed to flush pending writes: {:?}", e);
                });
                info!("Database writes completed during shutdown");
            }
        }
    });

    // Determine PGWire server port: check for a PGWIRE_PORT environment variable,
    // falling back to the port from the configuration.
    let pgwire_port = env::var("PGWIRE_PORT")
        .ok()
        .and_then(|port_str| port_str.parse::<u16>().ok())
        .unwrap_or(config.pg_port);

    info!("Starting PGWire server on port: {}", pgwire_port);
    let pg_server = db
        .start_pgwire_server(session_context.clone(), pgwire_port, shutdown_token.clone())
        .await?;
    
    sleep(Duration::from_secs(1)).await;
    if pg_server.is_finished() {
        error!("PGWire server failed to start, aborting...");
        return Err(TimeFusionError::Generic(anyhow::anyhow!(
            "PGWire server failed to start"
        )));
    }

    let http_addr = format!("0.0.0.0:{}", config.http_port);
    // Clone the database for the HTTP server.
    let db_for_http = db.clone();
    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(TracingLogger::default())
            .wrap(Logger::default())
            .app_data(web::Data::new(AppState { db: db_for_http.clone() }))
            .service(register_project)
    });

    let server = match http_server.bind(&http_addr) {
        Ok(s) => {
            info!("HTTP server running on http://{}", http_addr);
            s.run()
        }
        Err(e) => {
            error!("Failed to bind HTTP server to {}: {:?}", http_addr, e);
            return Err(TimeFusionError::Io(e));
        }
    };

    // Spawn a periodic compaction task (every 24 hours).
    let db_compaction = db.clone();
    let compaction_shutdown = shutdown_token.clone();
    let compaction_session = session_context.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = compaction_shutdown.cancelled() => {
                    info!("Compaction background task shutting down");
                    break;
                }
                _ = sleep(Duration::from_secs(24 * 3600)) => {
                    info!("Starting periodic compaction");
                    if let Err(e) = db_compaction.compact(&compaction_session).await {
                        error!("Periodic compaction failed: {:?}", e);
                    } else {
                        info!("Periodic compaction completed successfully");
                    }
                }
            }
        }
    });

    let http_handle = server.handle();
    let http_task = tokio::spawn(async move {
        tokio::select! {
            _ = http_shutdown.cancelled() => info!("HTTP server shutting down."),
            res = server => res.map_or_else(
                |e| error!("HTTP server failed: {:?}", e),
                |_| info!("HTTP server shut down gracefully")
            ),
        }
    });

    tokio::select! {
        _ = pg_server.map_err(|e| error!("PGWire server task failed: {:?}", e)) => {},
        _ = http_task.map_err(|e| error!("HTTP server task failed: {:?}", e)) => {},
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating shutdown.");
            shutdown_token.cancel();
            let _ = shutdown_tx.send(ShutdownSignal).await;
            http_handle.stop(true).await;
            sleep(Duration::from_secs(1)).await;
        }
    }

    info!("Shutdown complete.");
    Ok(())
}
