// src/main.rs
mod database;
mod ingest;
mod persistent_queue;
mod pgwire_integration;
mod utils;
mod metrics;
mod metrics_middleware;

use actix_web::{web, HttpResponse, HttpServer, Responder, get, middleware::Logger, App};
use actix_files::Files;
use chrono::Utc;
use database::Database;
use ingest::{ingest as ingest_handler, get_status, get_all_data, get_data_by_id, IngestStatusStore};
use pgwire_integration::{DfSessionService, run_pgwire_server, HandlerFactory};
use persistent_queue::PersistentQueue;
use serde_json::json;
use std::sync::Arc;
use std::env;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;
use dotenv::dotenv;
use tokio::task::spawn_blocking;
use std::time::Duration as StdDuration;
use anyhow::Context;
use metrics::{UPTIME_GAUGE, COMPACTION_COUNTER, HTTP_REQUEST_COUNTER};
use prometheus::core::Collector; // Added to fix E0599

#[get("/dashboard")]
async fn dashboard(
    db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
    app_info: web::Data<AppInfo>,
    status_store: web::Data<Arc<IngestStatusStore>>,
) -> impl Responder {
    let uptime = Utc::now().signed_duration_since(app_info.start_time).num_seconds() as f64;
    UPTIME_GAUGE.set(uptime);
    let compactions = COMPACTION_COUNTER.get();
    let http_requests: f64 = {
        let mfs = HTTP_REQUEST_COUNTER.collect(); // CounterVec implements Collector
        let mut total = 0.0;
        for mf in mfs {
            for m in mf.get_metric() {
                total += m.get_counter().get_value();
            }
        }
        total
    };
    let queue_size = queue.get_ref().len().await.unwrap_or(0);
    let db_status = match db.query("SELECT 1 AS test").await {
        Ok(_) => "healthy",
        Err(e) => {
            error!("Database health check failed: {:?}", e);
            "unhealthy"
        },
    };
    let recent_statuses = {
        let statuses = status_store.inner.read().unwrap();
        statuses.iter().take(5).map(|(id, status)| json!({"id": id, "status": status})).collect::<Vec<_>>()
    };

    let html = include_str!("dashboard/dashboard.html")
        .replace("{{uptime}}", &uptime.to_string())
        .replace("{{compactions}}", &compactions.to_string())
        .replace("{{http_requests}}", &http_requests.to_string())
        .replace("{{queue_size}}", &queue_size.to_string())
        .replace("{{db_status}}", db_status)
        .replace("{{recent_statuses}}", &serde_json::to_string(&recent_statuses).unwrap());
    HttpResponse::Ok().content_type("text/html").body(html)
}

struct AppInfo {
    start_time: chrono::DateTime<Utc>,
}

async fn landing() -> impl Responder {
    HttpResponse::TemporaryRedirect()
        .append_header(("Location", "/dashboard"))
        .finish()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting TimeFusion application");

    let bucket = env::var("S3_BUCKET_NAME")
        .context("S3_BUCKET_NAME environment variable not set")?;
    let http_port = env::var("PORT").unwrap_or_else(|_| "80".to_string());
    let pgwire_port = env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string());
    let s3_uri = format!("s3://{}/delta_table", bucket);

    deltalake::aws::register_handlers(None);

    let db = Arc::new(Database::new().await.context("Failed to initialize Database")?);
    db.add_project("events", &s3_uri)
        .await
        .context("Failed to add project 'events'")?;
    db.create_events_table("events", &s3_uri)
        .await
        .context("Failed to create events table")?;

    let queue = Arc::new(PersistentQueue::new("/app/queue_db")
        .context("Failed to initialize PersistentQueue")?);
    let status_store = ingest::IngestStatusStore::new();
    let app_info = web::Data::new(AppInfo { start_time: Utc::now() });

    let db_for_compaction = db.clone();
    tokio::spawn(async move {
        let mut compaction_interval = tokio::time::interval(StdDuration::from_secs(24 * 3600));
        loop {
            compaction_interval.tick().await;
            if let Err(e) = db_for_compaction.compact_all_projects().await {
                error!("Error during periodic compaction: {:?}", e);
            } else {
                info!("Periodic compaction completed successfully.");
            }
        }
    });

    let shutdown_token = CancellationToken::new();
    let queue_shutdown = shutdown_token.clone();
    let http_shutdown = shutdown_token.clone();
    let pgwire_shutdown = shutdown_token.clone();

    let pg_service = DfSessionService::new(db.get_session_context(), db.clone());
    let handler_factory = HandlerFactory(Arc::new(pg_service));

    let pg_addr = format!("0.0.0.0:{}", pgwire_port);
    info!("Spawning PGWire server task on {}", pg_addr);
    let pg_server = tokio::spawn({
        let pg_addr = pg_addr.clone();
        let handler_factory = handler_factory.clone();
        async move {
            if let Err(e) = run_pgwire_server(handler_factory, &pg_addr, pgwire_shutdown).await {
                error!("PGWire server error: {:?}", e);
            }
        }
    });

    let flush_task = {
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        let status_store_clone = status_store.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = queue_shutdown.cancelled() => {
                        info!("Queue flush task shutting down.");
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
                                process_record(&db_clone, &queue_clone, &status_store_clone, key, record).await;
                            }
                        }
                    }
                }
            }
        })
    };

    let http_addr = format!("0.0.0.0:{}", http_port);
    info!("Binding HTTP server to {}", http_addr);
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(metrics_middleware::MetricsMiddleware)
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(web::Data::new(status_store.clone()))
            .app_data(web::Data::new(app_info.clone()))
            .service(web::resource("/").route(web::get().to(landing)))
            .service(dashboard)
            .service(ingest_handler)
            .service(get_status)
            .service(get_all_data)
            .service(get_data_by_id)
            .service(Files::new("/dashboard/static", "./src/dashboard").show_files_listing())
    })
    .bind(&http_addr)
    .context(format!("Failed to bind HTTP server to {}", http_addr))?
    .run();

    let http_server_handle = server.handle();
    let http_task = tokio::spawn(async move {
        tokio::select! {
            _ = http_shutdown.cancelled() => {
                info!("HTTP server shutting down.");
            }
            result = server => {
                if let Err(e) = result {
                    error!("HTTP server failed: {:?}", e);
                }
            }
        }
    });

    info!("HTTP server running on http://{}", http_addr);

    tokio::select! {
        res = pg_server => res.context("PGWire server task failed")?,
        res = http_task => res.context("HTTP server task failed")?,
        res = flush_task => res.context("Queue flush task failed")?,
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating shutdown.");
            shutdown_token.cancel();
            http_server_handle.stop(true).await;
            sleep(Duration::from_secs(1)).await;
        }
    }

    info!("Shutdown complete.");
    Ok(())
}

async fn process_record(
    db: &Arc<Database>,
    queue: &Arc<PersistentQueue>,
    status_store: &ingest::IngestStatusStore,
    key: sled::IVec,
    record: persistent_queue::IngestRecord,
) {
    use std::str;
    let id = str::from_utf8(&key).unwrap_or("unknown").to_string();
    status_store.set_status(id.clone(), "Processing".to_string());
    if chrono::DateTime::parse_from_rfc3339(&record.timestamp).is_ok() {
        match db.write(&record).await {
            Ok(()) => {
                status_store.set_status(id.clone(), "Ingested".to_string());
                if let Err(e) = spawn_blocking({
                    let queue = queue.clone();
                    move || queue.remove_sync(key)
                }).await {
                    error!("Failed to remove record: {:?}", e);
                }
                if let Err(e) = db.refresh_table(&record.project_id).await {
                    error!("Failed to refresh table: {:?}", e);
                }
            }
            Err(e) => {
                error!("Error writing record: {:?}", e);
                status_store.set_status(id, format!("Failed: {:?}", e));
            }
        }
    } else {
        error!("Invalid timestamp in record: {}", record.timestamp);
        status_store.set_status(id, "Invalid timestamp".to_string());
        let _ = spawn_blocking({
            let queue = queue.clone();
            move || queue.remove_sync(key)
        }).await;
    }
}