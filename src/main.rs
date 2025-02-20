mod database;
mod ingest;
mod persistent_queue;
mod pgserver_message;
mod pgwire_integration;
mod utils;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use actix_web::middleware::Logger;
use chrono::Utc;
use database::Database;
use pgwire_integration::{DfSessionService, run_pgwire_server, HandlerFactory};
use persistent_queue::PersistentQueue;
use serde_json::json;
use std::sync::Arc;
use std::str;
use std::env;
use tokio::time::{sleep, Duration};
use anyhow::Context;
use tokio_util::sync::CancellationToken;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;
use dotenv::dotenv;

/// Health endpoint that returns version, timestamp, and diagnostics.
async fn health() -> impl Responder {
    let health_status = json!({
        "status": "OK",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": Utc::now().to_rfc3339(),
        "database": "healthy"
    });
    HttpResponse::Ok().json(health_status)
}

/// Metrics endpoint returning Prometheus‑formatted metrics.
async fn metrics() -> impl Responder {
    use prometheus::{Encoder, TextEncoder, gather};
    let encoder = TextEncoder::new();
    let metric_families = gather();
    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!("Error encoding metrics: {:?}", e);
        return HttpResponse::InternalServerError().body("Error encoding metrics");
    }
    HttpResponse::Ok()
        .content_type(encoder.format_type())
        .body(buffer)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file.
    dotenv().ok();

    // Initialize structured logging.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    info!("Starting production-grade application");

    // Retrieve configuration values.
    let bucket = env::var("S3_BUCKET_NAME")
        .context("S3_BUCKET_NAME environment variable not set")?;
    // Use PORT environment variable; default to "80" if not set.
    let http_port = env::var("PORT").unwrap_or_else(|_| "80".to_string());
    let pgwire_port = env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string());
    let s3_uri = format!("s3://{}/delta_table", bucket);

    // Register S3 handlers.
    deltalake::aws::register_handlers(None);

    // Initialize the Database.
    let db = Arc::new(Database::new().await.context("Failed to initialize Database")?);
    db.add_project("events", &s3_uri)
        .await
        .context("Failed to add project 'events'")?;
    db.create_events_table("events", &s3_uri)
        .await
        .context("Failed to create events table")?;
    
    // Write a sample record.
    let now = Utc::now();
    db.write("events", now, Some(now), None, Some("data1"))
        .await
        .context("Failed to write sample record")?;
    
    // Run a sample query.
    db.query("SELECT * FROM \"table\" WHERE project_id = 'events'")
        .await
        .context("Failed to run sample query")?
        .show()
        .await
        .context("Failed to display query result")?;
    
    db.compact("events")
        .await
        .context("Failed to compact project 'events'")?;
    
    // Initialize persistent queue using the absolute path (as configured in the Dockerfile).
    let queue = Arc::new(PersistentQueue::new("/app/queue_db")?);
    
    // Initialize the ingestion status store.
    let status_store = ingest::IngestStatusStore::new();

    // Create a shutdown cancellation token.
    let shutdown_token = CancellationToken::new();

    // Spawn a background task to flush the persistent queue.
    {
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        let status_store_clone = status_store.clone();
        let flush_shutdown = shutdown_token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = flush_shutdown.cancelled() => {
                        info!("Persistent queue flush task received shutdown signal. Exiting.");
                        break;
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        let records = match queue_clone.dequeue_all().await {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Error during dequeue_all: {:?}", e);
                                Vec::new()
                            }
                        };
                        if !records.is_empty() {
                            info!("Flushing {} enqueued records", records.len());
                            for (key, record) in records {
                                let key_vec = key.to_vec();
                                let id = match str::from_utf8(&key_vec) {
                                    Ok(s) => s.to_string(),
                                    Err(e) => {
                                        error!("Error reading key: {:?}", e);
                                        continue;
                                    }
                                };
                                status_store_clone.set_status(id.clone(), "Processing".to_string());
                                if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(&record.timestamp) {
                                    let ts = timestamp.with_timezone(&Utc);
                                    let st = record.start_time.as_deref()
                                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                                        .map(|dt| dt.with_timezone(&Utc));
                                    let et = record.end_time.as_deref()
                                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                                        .map(|dt| dt.with_timezone(&Utc));
                                    match db_clone.write(&record.project_id, ts, st, et, record.payload.as_deref()).await {
                                        Ok(()) => {
                                            status_store_clone.set_status(id.clone(), "Ingested".to_string());
                                            let queue_clone2 = queue_clone.clone();
                                            let key_vec_owned = key_vec.clone();
                                            tokio::spawn(async move {
                                                info!("Attempting to remove key: {:?}", key_vec_owned);
                                                let owned_key = sled::IVec::from(key_vec_owned);
                                                let remove_res = tokio::task::spawn_blocking(move || {
                                                    queue_clone2.remove_sync(owned_key)
                                                }).await;
                                                match remove_res {
                                                    Ok(Ok(())) => info!("Successfully removed key."),
                                                    Ok(Err(e)) => error!("Removal failed: {:?}", e),
                                                    Err(e) => error!("Join error during removal: {:?}", e),
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            error!("Error writing record from queue: {:?}", e);
                                            status_store_clone.set_status(id.clone(), format!("Failed: {:?}", e));
                                        }
                                    }
                                } else {
                                    error!("Invalid timestamp in queued record");
                                    let queue_clone3 = queue_clone.clone();
                                    let key_vec_owned = key_vec.clone();
                                    let remove_res = tokio::task::spawn_blocking(move || {
                                        queue_clone3.remove_sync(sled::IVec::from(key_vec_owned))
                                    }).await;
                                    match remove_res {
                                        Ok(Ok(())) => info!("Successfully removed key for invalid timestamp."),
                                        Ok(Err(e)) => error!("Removal failed for invalid timestamp: {:?}", e),
                                        Err(e) => error!("Join error during removal for invalid timestamp: {:?}", e),
                                    }
                                    status_store_clone.set_status(id.clone(), "Invalid timestamp".to_string());
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    
    // Start the PGWire server.
    let pg_service = DfSessionService::new(db.get_session_context(), db.clone());
    let handler_factory = HandlerFactory(Arc::new(pg_service));
    let pg_addr = format!("0.0.0.0:{}", pgwire_port);
    info!("Spawning PGWire server task on {}", pg_addr);
    let pg_server = tokio::spawn(async move {
        if let Err(e) = run_pgwire_server(handler_factory, &pg_addr).await {
            error!("PGWire server error: {:?}", e);
        }
    });
    
    // Start the HTTP server with Logger middleware, health, and metrics endpoints.
    let http_addr = format!("0.0.0.0:{}", http_port);
    let http_server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(web::Data::new(status_store.clone()))
            .service(ingest::ingest)
            .service(ingest::get_status)
            .route("/health", web::get().to(health))
            .route("/metrics", web::get().to(metrics))
    })
    .bind(&http_addr)
    .context(format!("Failed to bind HTTP server to {}", http_addr))?
    .run();
    
    info!("HTTP server running on http://{}", http_addr);
    
    // Wait for either server failure or Ctrl+C.
    tokio::select! {
        res = pg_server => {
            res.context("PGWire server task failed")?;
        },
        res = http_server => {
            res.context("HTTP server failed")?;
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating graceful shutdown.");
            shutdown_token.cancel();
        }
    }
    
    // Allow a brief pause for cleanup.
    sleep(Duration::from_secs(1)).await;
    
    info!("Shutdown complete.");
    
    Ok(())
}
