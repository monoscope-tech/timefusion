// src/main.rs
mod database;
mod error;
mod persistent_queue;
mod telemetry;
use std::{env, sync::Arc};

use actix_web::{App, HttpResponse, HttpServer, Responder, middleware::Logger, post, web};
use database::Database;
use dotenv::dotenv;
use futures::TryFutureExt;
use serde::Deserialize;
use tokio::{
    sync::mpsc,
    time::{Duration, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_actix_web::TracingLogger;

use crate::error::{Result, TimeFusionError};

struct AppState {
    db: Arc<Database>,
}

struct ShutdownSignal;

#[derive(Deserialize)]
struct RegisterProjectRequest {
    project_id: String,
    bucket:     String,
    access_key: String,
    secret_key: String,
    endpoint:   Option<String>,
}

#[tracing::instrument(
    name = "HTTP /register_project",
    skip(req, app_state),
    fields(project_id = %req.project_id)
)]
#[post("/register_project")]
async fn register_project(req: web::Json<RegisterProjectRequest>, app_state: web::Data<AppState>) -> Result<impl Responder> {
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

    // Initialize tracing & metrics
    telemetry::init_telemetry();

    info!("Starting TimeFusion application");

    let db = Database::new().await?;
    info!("Database initialized successfully");

    let session_context = db.create_session_context();
    db.setup_session_context(&session_context)?;
    info!("Session context setup complete");

    let db = Arc::new(db);
    let (shutdown_tx, _shutdown_rx) = mpsc::channel::<ShutdownSignal>(1);
    let shutdown_token = CancellationToken::new();
    let http_shutdown = shutdown_token.clone();

    // Spawn database write monitor
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

    let pg_port = env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string()).parse::<u16>().unwrap_or(5432);
    let pg_server = db.start_pgwire_server(session_context.clone(), pg_port, shutdown_token.clone()).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;
    if pg_server.is_finished() {
        error!("PGWire server failed to start, aborting...");
        return Err(TimeFusionError::Generic(anyhow::anyhow!("PGWire server failed to start")));
    }

    let http_addr = format!("0.0.0.0:{}", env::var("PORT").unwrap_or_else(|_| "80".to_string()));
    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(TracingLogger::default())
            .wrap(Logger::default())
            .app_data(web::Data::new(AppState { db: db.clone() }))
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
