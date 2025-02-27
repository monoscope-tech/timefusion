mod database;
mod ingest;
mod persistent_queue;
mod pgserver_message;
mod pgwire_integration;
mod utils;
mod metrics;
mod metrics_middleware;

use actix_web::{web, HttpResponse, HttpServer, Responder, get, middleware::Logger};
use chrono::Utc;
use database::Database;
use ingest::{ingest as ingest_handler, get_status, get_all_data, get_data_by_id};
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

#[get("/dashboard/metrics")]
async fn dashboard_metrics(
    db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
) -> impl Responder {
    let uptime = UPTIME_GAUGE.get();
    let compactions = metrics::COMPACTION_COUNTER.get();
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
    let queue_size = queue.get_ref().len().await.unwrap_or(0); // Assumes len() is implemented
    let db_status = match db.query("SELECT 1 AS test").await {
        Ok(_) => 1.0,
        Err(_) => 0.0,
    };

    let data = json!({
        "uptime_seconds": uptime,
        "compactions": compactions,
        "http_requests_total": http_requests,
        "queue_size": queue_size,
        "db_status": db_status,
    });
    HttpResponse::Ok().json(data)
}

struct AppInfo {
    start_time: chrono::DateTime<Utc>,
}

async fn landing() -> impl Responder {
    let html = r#"
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>TimeFusion by APITOOLKIT Dashboard</title>
    <style>
      body { font-family: 'Segoe UI', Arial, sans-serif; background: #eef2f7; margin: 0; padding: 0; color: #333; }
      header { background: linear-gradient(90deg, #1E90FF, #00BFFF); color: #fff; padding: 25px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.2); }
      h1 { margin: 0; font-size: 2.5em; }
      .container { max-width: 1200px; margin: 30px auto; padding: 0 15px; }
      .card { background: #fff; padding: 20px; margin-bottom: 20px; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); transition: transform 0.2s; }
      .card:hover { transform: translateY(-5px); }
      .card h2 { margin-top: 0; color: #1E90FF; font-size: 1.5em; }
      pre { background: #f9f9f9; padding: 10px; border-radius: 5px; font-size: 0.9em; }
      button { background: #1E90FF; color: #fff; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; font-size: 1em; }
      button:hover { background: #00BFFF; }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  </head>
  <body>
    <header>
      <h1>TimeFusion by APITOOLKIT Dashboard</h1>
    </header>
    <div class="container">
      <div class="card">
        <h2>Health Status</h2>
        <button onclick="fetchHealth()">Refresh Health</button>
        <div id="healthStatus">Loading health...</div>
      </div>
      <div class="card">
        <h2>System Metrics</h2>
        <button onclick="fetchMetrics()">Refresh Metrics</button>
        <canvas id="metricsChart" width="400" height="200"></canvas>
      </div>
    </div>
    <script>
      let metricsChart;

      function fetchHealth() {
        fetch('/health')
          .then(response => response.json())
          .then(data => {
            document.getElementById('healthStatus').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
          })
          .catch(error => {
            document.getElementById('healthStatus').innerHTML = 'Error loading health data: ' + error;
          });
      }

      function fetchMetrics() {
        fetch('/dashboard/metrics')
          .then(response => response.json())
          .then(data => {
            const labels = Object.keys(data);
            const values = Object.values(data);
            const ctx = document.getElementById('metricsChart').getContext('2d');
            if (metricsChart) metricsChart.destroy();
            metricsChart = new Chart(ctx, {
              type: 'bar',
              data: {
                labels: labels,
                datasets: [{
                  label: 'System Metrics',
                  data: values,
                  backgroundColor: [
                    'rgba(30, 144, 255, 0.6)',
                    'rgba(75, 192, 192, 0.6)',
                    'rgba(255, 99, 132, 0.6)',
                    'rgba(255, 205, 86, 0.6)',
                    'rgba(54, 162, 235, 0.6)'
                  ],
                  borderColor: [
                    'rgba(30, 144, 255, 1)',
                    'rgba(75, 192, 192, 1)',
                    'rgba(255, 99, 132, 1)',
                    'rgba(255, 205, 86, 1)',
                    'rgba(54, 162, 235, 1)'
                  ],
                  borderWidth: 1
                }]
              },
              options: {
                responsive: true,
                scales: {
                  y: { beginAtZero: true, title: { display: true, text: 'Value' } },
                  x: { title: { display: true, text: 'Metric' } }
                },
                plugins: {
                  legend: { position: 'top' },
                  title: { display: true, text: 'Real-Time System Metrics' }
                }
              }
            });
          })
          .catch(error => console.error('Error fetching metrics:', error));
      }

      fetchHealth();
      fetchMetrics();
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
        Err(e) => {
            error!("Database health check failed: {:?}", e);
            "unhealthy"
        },
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
    dotenv().ok();

    // Basic tracing setup without OpenTelemetry
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
        actix_web::App::new()
            .wrap(Logger::default())
            .wrap(metrics_middleware::MetricsMiddleware)
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(web::Data::new(status_store.clone()))
            .app_data(web::Data::new(app_info.clone()))
            .service(web::resource("/").route(web::get().to(landing)))
            .service(web::resource("/health").route(web::get().to(health_endpoint)))
            .service(web::resource("/metrics").route(web::get().to(metrics_endpoint)))
            .service(dashboard_metrics)
            .service(ingest_handler)
            .service(get_status)
            .service(get_all_data)
            .service(get_data_by_id)
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
                if let Err(e) = db.refresh_table(&record.project_id).await {
                    error!("Failed to refresh table after write: {:?}", e);
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