mod database;
mod ingest;
mod persistent_queue;
mod pgserver_message;
mod pgwire_integration;
mod utils;
mod metrics;
mod metrics_middleware;

// Remove duplicate import of the ingest module; functions will be referenced as ingest::function

use actix_web::{web, App, HttpResponse, HttpServer, Responder, get};
use actix_web::middleware::Logger;
use chrono::Utc;
use database::Database;
use pgwire_integration::{DfSessionService, run_pgwire_server, HandlerFactory};
use persistent_queue::PersistentQueue;
use serde_json::json;
use std::sync::Arc;
use std::env;
use tokio::time::{sleep, Duration, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;
use dotenv::dotenv;
use tokio::task::spawn_blocking;
use std::time::Duration as StdDuration;
use anyhow::Context;
use metrics::{UPTIME_GAUGE, gather_metrics};
use prometheus::core::Collector;

// ---------------------------------------------------------------------

async fn dashboard_metrics() -> impl Responder {
    // Get uptime from the gauge.
    let uptime = UPTIME_GAUGE.get();
    // Get compaction count (assuming your COMPACTION_COUNTER is defined in metrics).
    let compactions = metrics::COMPACTION_COUNTER.get();

    // Aggregate HTTP request total from our CounterVec.
    let http_requests: f64 = {
        let mfs = metrics::HTTP_REQUEST_COUNTER.collect();
        let mut total = 0.0;
        for mf in mfs {
            for m in mf.get_metric() {
                total += m.get_counter().get_value();
            }
        }
        total
    };

    let data = json!({
         "uptime_seconds": uptime,
         "compactions": compactions,
         "http_requests_total": http_requests,
    });
    HttpResponse::Ok().json(data)
}
// ---------------------------------------------------------------------

/// AppInfo holds the application start time.
struct AppInfo {
    start_time: chrono::DateTime<Utc>,
}

/// Landing page/dashboard.
async fn landing() -> impl Responder {
    let html = r#"
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>timefusion by APITOOLKIT Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; background: #f4f4f4; margin: 0; padding: 0; }
      header { background-color: #1E90FF; color: #fff; padding: 20px; text-align: center; }
      .container { margin: 20px; }
      .card { background: #fff; padding: 20px; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
      pre { white-space: pre-wrap; word-wrap: break-word; }
    </style>
    <!-- Load Chart.js from CDN -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  </head>
  <body>
    <header>
      <h1>timefusion by APITOOLKIT Dashboard</h1>
    </header>
    <div class="container">
      <div class="card">
        <h2>Health Status</h2>
        <div id="healthStatus">Loading health...</div>
      </div>
      <div class="card">
        <h2>Metrics</h2>
        <canvas id="metricsChart" width="400" height="200"></canvas>
      </div>
    </div>
    <script>
      // Fetch health data and update the dashboard.
      fetch('/health')
        .then(response => response.json())
        .then(data => {
          document.getElementById('healthStatus').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        })
        .catch(error => {
          document.getElementById('healthStatus').innerHTML = 'Error loading health data.';
        });

      // Fetch real metrics and render the chart.
      fetch('/dashboard/metrics')
        .then(response => response.json())
        .then(data => {
          const labels = Object.keys(data);
          const values = Object.values(data);
          const ctx = document.getElementById('metricsChart').getContext('2d');
          new Chart(ctx, {
            type: 'bar',
            data: {
              labels: labels,
              datasets: [{
                label: 'Real Metrics',
                data: values,
                backgroundColor: [
                  'rgba(30, 144, 255, 0.6)', 
                  'rgba(75, 192, 192, 0.6)', 
                  'rgba(255, 99, 132, 0.6)'
                ]
              }]
            },
            options: {
              responsive: true,
              scales: { y: { beginAtZero: true } }
            }
          });
        })
        .catch(error => console.error('Error fetching dashboard metrics:', error));
    </script>
  </body>
</html>
    "#;
    HttpResponse::Ok().content_type("text/html").body(html)
}

async fn health_endpoint(db: web::Data<Arc<Database>>, app_info: web::Data<AppInfo>) -> impl Responder {
    let uptime = Utc::now().signed_duration_since(app_info.start_time).num_seconds();
    UPTIME_GAUGE.set(uptime as f64);
    let db_status = match db.query("SELECT 1 AS test").await {
        Ok(_) => "healthy",
        Err(_) => "unhealthy",
    };
    let health_status = json!({
        "status": "OK",
        "version": "timefusion by APITOOLKIT",
        "timestamp": Utc::now().to_rfc3339(),
        "uptime_seconds": uptime,
        "database": db_status
    });
    HttpResponse::Ok().json(health_status)
}

async fn metrics_endpoint() -> impl Responder {
    let body = gather_metrics();
    HttpResponse::Ok().content_type("text/plain; version=0.0.4").body(body)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    info!("Starting production-grade application");

    let bucket = env::var("S3_BUCKET_NAME")
        .context("S3_BUCKET_NAME environment variable not set")?;
    let http_port = env::var("PORT").unwrap_or_else(|_| "80".to_string());
    let pgwire_port = env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string());
    let s3_uri = format!("s3://{}/delta_table", bucket);

    deltalake::aws::register_handlers(None);

    let db = Arc::new(Database::new().await.context("Failed to initialize Database - aborting")?);
    db.add_project("events", &s3_uri)
        .await
        .context("Failed to add project 'events'")?;
    db.create_events_table("events", &s3_uri)
        .await
        .context("Failed to create events table")?;
    
    let queue = Arc::new(PersistentQueue::new("/app/queue_db")
        .context("Failed to initialize PersistentQueue - aborting")?);
    
    let status_store = ingest::IngestStatusStore::new();
    let app_info = web::Data::new(AppInfo { start_time: Utc::now() });

    // Spawn periodic compaction task every 24 hours.
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
    let pg_shutdown = shutdown_token.clone();

    let pg_service = DfSessionService::new(db.get_session_context(), db.clone());
    let handler_factory = HandlerFactory(Arc::new(pg_service));

    let pg_addr = format!("0.0.0.0:{}", pgwire_port);
    info!("Spawning PGWire server task on {}", pg_addr);
    let pg_server = tokio::spawn({
        let pg_addr = pg_addr.clone();
        let handler_factory = handler_factory.clone();
        async move {
            if let Err(e) = run_pgwire_server(handler_factory, &pg_addr, pg_shutdown).await {
                error!("PGWire server error: {:?}", e);
            }
        }
    });

    let flush_task = {
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        let status_store_clone = status_store.clone();
        let queue_shutdown = queue_shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = queue_shutdown.cancelled() => {
                        info!("Queue flush task received shutdown signal. Flushing remaining records.");
                        let records = match queue_clone.dequeue_all().await {
                            Ok(r) => r,
                            Err(e) => {
                                error!("Final dequeue failed: {:?}", e);
                                Vec::new()
                            }
                        };
                        for (key, record) in records {
                            process_record(&db_clone, &queue_clone, &status_store_clone, key, record).await;
                        }
                        info!("Queue flush task exiting.");
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
            .app_data(app_info.clone())
            .service(web::resource("/").route(web::get().to(landing)))
            .service(web::resource("/health").route(web::get().to(health_endpoint)))
            .service(web::resource("/metrics").route(web::get().to(metrics_endpoint)))
            .service(web::resource("/dashboard/metrics").route(web::get().to(dashboard_metrics)))
            .service(ingest::ingest)
            .service(ingest::get_status)
            .service(ingest::get_all_data)
            .service(ingest::get_data_by_id)
    })
    .bind(&http_addr)
    .context(format!("Failed to bind HTTP server to {} - aborting", http_addr))?
    .run();
    let http_server_handle = server.handle();
    let http_task = tokio::spawn(async move {
        tokio::select! {
            _ = http_shutdown.cancelled() => {
                info!("HTTP server received shutdown signal.");
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
        res = pg_server => {
            res.context("PGWire server task failed")?;
            shutdown_token.cancel();
        }
        res = http_task => {
            res.context("HTTP server task failed")?;
            shutdown_token.cancel();
        }
        res = flush_task => {
            res.context("Queue flush task failed")?;
            shutdown_token.cancel();
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, initiating graceful shutdown.");
            shutdown_token.cancel();
            if let Err(e) = timeout(Duration::from_secs(30), http_server_handle.stop(true)).await {
                error!("HTTP server did not stop within 30 seconds: {:?}", e);
            }
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
    let key_vec = key.to_vec();
    let id = match str::from_utf8(&key_vec) {
        Ok(s) => s.to_string(),
        Err(e) => {
            error!("Error reading key: {:?}", e);
            return;
        }
    };
    status_store.set_status(id.clone(), "Processing".to_string());
    if chrono::DateTime::parse_from_rfc3339(&record.timestamp).is_ok() {
        match db.write(&record).await {
            Ok(()) => {
                status_store.set_status(id.clone(), "Ingested".to_string());
                let remove_res = spawn_blocking({
                    let queue = queue.clone();
                    move || queue.remove_sync(key)
                }).await;
                match remove_res {
                    Ok(Ok(())) => info!("Successfully removed key."),
                    Ok(Err(e)) => error!("Removal failed: {:?}", e),
                    Err(e) => error!("Join error during removal: {:?}", e),
                }
            }
            Err(e) => {
                error!("Error writing record from queue: {:?}", e);
                status_store.set_status(id.clone(), format!("Failed: {:?}", e));
            }
        }
    } else {
        error!("Invalid timestamp in queued record");
        let remove_res = spawn_blocking({
            let queue = queue.clone();
            move || queue.remove_sync(key)
        }).await;
        match remove_res {
            Ok(Ok(())) => info!("Successfully removed key for invalid timestamp."),
            Ok(Err(e)) => error!("Removal failed for invalid timestamp: {:?}", e),
            Err(e) => error!("Join error during removal for invalid timestamp: {:?}", e),
        }
        status_store.set_status(id.clone(), "Invalid timestamp".to_string());
    }
}
