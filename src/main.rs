mod database;
mod ingest;
mod persistent_queue;
mod utils;
mod metrics;
mod metrics_middleware;

use actix_web::{
    web, App, HttpResponse, HttpServer, Responder, get, middleware::Logger,
};
use chrono::Utc;
use database::Database;
use ingest::start_ingestion_service;
use datafusion_postgres::{DfSessionService, HandlerFactory};
use persistent_queue::{PersistentQueue, IngestRecord};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use std::env;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{info, error, debug};
use tracing_subscriber::EnvFilter;
use dotenv::dotenv;
use tokio::task::spawn_blocking;
use std::time::Duration as StdDuration;
use metrics::{INGESTION_COUNTER, ERROR_COUNTER};
use std::collections::{HashMap, VecDeque};
use tokio::sync::Mutex as TokioMutex;
use datafusion::arrow::array::Float64Array;
use url::Url;
use tokio::net::TcpListener;
use pgwire::tokio::process_socket;

#[derive(Clone)]
struct AppInfo {
    start_time: chrono::DateTime<Utc>,
    trends: Arc<TokioMutex<VecDeque<TrendData>>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct TrendData {
    timestamp: String,
    ingestion_rate: f64,
    queue_size: usize,
    avg_latency: f64,
}

#[derive(Serialize)]
struct DashboardData {
    uptime: f64,
    http_requests: f64,
    queue_size: usize,
    queue_alert: bool,
    db_status: String,
    ingestion_rate: f64,
    avg_latency: f64,
    latency_alert: bool,
    recent_statuses: Vec<serde_json::Value>,
    status_counts: HashMap<String, i32>,
    recent_records: Vec<serde_json::Value>,
    trends: Vec<TrendData>,
}

// Placeholder IngestStatusStore since it’s not defined in ingest.rs yet
#[derive(Clone)]
pub struct IngestStatusStore {
    inner: Arc<std::sync::RwLock<HashMap<String, String>>>,
}

impl IngestStatusStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::RwLock::new(HashMap::new())),
        }
    }

    pub fn set_status(&self, id: String, status: String) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(id, status);
    }
}

#[get("/dashboard")]
async fn dashboard(
    db: web::Data<Arc<Database>>,
    queue: web::Data<Arc<PersistentQueue>>,
    app_info: web::Data<AppInfo>,
    status_store: web::Data<Arc<IngestStatusStore>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let uptime = Utc::now().signed_duration_since(app_info.start_time).num_seconds() as f64;
    let http_requests = 0.0; // Placeholder
    let queue_size = queue.get_ref().len().await.unwrap_or(0);
    let db_status = match db.query("SELECT 1 AS test").await {
        Ok(_) => "success",
        Err(_) => "error",
    }.to_string();
    let ingestion_rate = INGESTION_COUNTER.get() as f64 / 60.0;

    let start = query.get("start").and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
    let end = query.get("end").and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok());
    let records_query = if let (Some(start), Some(end)) = (start, end) {
        format!(
            "SELECT project_id, id, timestamp, trace_id, span_id, event_type, duration_ns \
             FROM telemetry_events WHERE timestamp >= '{}' AND timestamp <= '{}' ORDER BY timestamp DESC LIMIT 10",
            start.to_rfc3339(),
            end.to_rfc3339()
        )
    } else {
        "SELECT project_id, id, timestamp, trace_id, span_id, event_type, duration_ns FROM telemetry_events ORDER BY timestamp DESC LIMIT 10".to_string()
    };

    let avg_latency = match db
        .query("SELECT AVG(duration_ns) AS avg_latency FROM telemetry_events WHERE duration_ns IS NOT NULL")
        .await
    {
        Ok(df) => df.collect().await.ok().and_then(|batches| {
            batches.get(0).map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .map_or(0.0, |arr| arr.value(0) / 1_000_000.0)
            })
        }).unwrap_or(0.0),
        Err(_) => 0.0,
    };

    let latency_alert = avg_latency > 200.0;
    let queue_alert = queue_size > 50;

    let recent_statuses = status_store.inner.read().unwrap().iter()
        .take(10)
        .map(|(id, status)| serde_json::json!({ "id": id, "status": status }))
        .collect::<Vec<_>>();
    let status_counts = recent_statuses.iter().fold(HashMap::new(), |mut acc, status| {
        *acc.entry(status["status"].as_str().unwrap_or("Unknown").to_string()).or_insert(0) += 1;
        acc
    });
    let recent_records = match db.query(&records_query).await {
        Ok(df) => df.collect().await.ok().map_or(vec![], |batches| ingest::record_batches_to_json_rows(&batches).unwrap_or_default()),
        Err(_) => vec![],
    };

    let mut trends = app_info.trends.lock().await;
    let trend_data = TrendData {
        timestamp: Utc::now().to_rfc3339(),
        ingestion_rate,
        queue_size,
        avg_latency,
    };
    trends.push_back(trend_data);
    let cutoff = Utc::now() - chrono::Duration::hours(1);
    while trends.front().map_or(false, |t| chrono::DateTime::parse_from_rfc3339(&t.timestamp).unwrap() < cutoff) {
        trends.pop_front();
    }
    let trends_vec = if let (Some(start), Some(end)) = (start, end) {
        trends.iter().filter(|t| {
            let ts = chrono::DateTime::parse_from_rfc3339(&t.timestamp).unwrap();
            ts >= start && ts <= end
        }).cloned().collect::<Vec<_>>()
    } else {
        trends.iter().cloned().collect::<Vec<_>>()
    };

    let data = DashboardData {
        uptime,
        http_requests,
        queue_size,
        queue_alert,
        db_status,
        ingestion_rate,
        avg_latency,
        latency_alert,
        recent_statuses,
        status_counts,
        recent_records,
        trends: trends_vec,
    };

    let html = include_str!("dashboard/dashboard.html")
        .replace("{{uptime}}", &format!("{:.0}", data.uptime))
        .replace("{{http_requests}}", &format!("{:.0}", data.http_requests))
        .replace("{{queue_size}}", &data.queue_size.to_string())
        .replace("{{queue_alert}}", &data.queue_alert.to_string())
        .replace("{{db_status}}", &data.db_status)
        .replace("{{ingestion_rate}}", &format!("{:.2}", data.ingestion_rate))
        .replace("{{avg_latency}}", &format!("{:.2}", data.avg_latency))
        .replace("{{latency_alert}}", &data.latency_alert.to_string())
        .replace("{{recent_statuses}}", &serde_json::to_string(&data.recent_statuses).unwrap())
        .replace("{{recent_records}}", &serde_json::to_string(&data.recent_records).unwrap())
        .replace("{{status_counts}}", &serde_json::to_string(&data.status_counts).unwrap())
        .replace("{{trends}}", &serde_json::to_string(&data.trends).unwrap());
    HttpResponse::Ok().content_type("text/html").body(html)
}

#[get("/export_records")]
async fn export_records(
    db: web::Data<Arc<Database>>,
    query: web::Query<HashMap<String, String>>,
) -> impl Responder {
    let start = query.get("start").and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok());
    let end = query.get("end").and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok());

    let records_query = if let (Some(start), Some(end)) = (start, end) {
        format!(
            "SELECT project_id, id, timestamp, trace_id, span_id, event_type, duration_ns \
             FROM telemetry_events WHERE timestamp >= '{}' AND timestamp <= '{}'",
            start.to_rfc3339(),
            end.to_rfc3339()
        )
    } else {
        "SELECT project_id, id, timestamp, trace_id, span_id, event_type, duration_ns FROM telemetry_events".to_string()
    };

    let records = match db.query(&records_query).await {
        Ok(df) => df.collect().await.ok().map_or(vec![], |batches| ingest::record_batches_to_json_rows(&batches).unwrap_or_default()),
        Err(_) => vec![],
    };

    let mut csv = String::from("Project ID,Record ID,Timestamp,Trace ID,Span ID,Event Type,Latency (ms)\n");
    for record in records {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            record["project_id"].as_str().unwrap_or("N/A"),
            record["id"].as_str().unwrap_or("N/A"),
            record["timestamp"].as_str().unwrap_or("N/A"),
            record["trace_id"].as_str().unwrap_or("N/A"),
            record["span_id"].as_str().unwrap_or("N/A"),
            record["event_type"].as_str().unwrap_or("N/A"),
            (record["duration_ns"].as_i64().unwrap_or(0) as f64 / 1_000_000.0)
        ));
    }

    HttpResponse::Ok()
        .content_type("text/csv")
        .append_header(("Content-Disposition", "attachment; filename=\"records.csv\""))
        .body(csv)
}

#[get("/")]
async fn landing() -> impl Responder {
    HttpResponse::TemporaryRedirect()
        .append_header(("Location", "/dashboard"))
        .finish()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug")))
        .init();

    info!("Starting TimeFusion application");

    // AWS configuration from environment variables.
    let bucket = env::var("AWS_S3_BUCKET")
        .expect("AWS_S3_BUCKET environment variable not set");
    let aws_endpoint = env::var("AWS_S3_ENDPOINT")
        .unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
    let storage_uri = format!("s3://{}/?endpoint={}", bucket, aws_endpoint);
    info!("Storage URI configured: {}", storage_uri);

    // Set AWS_ENDPOINT so that the underlying S3 client uses the specified endpoint.
    unsafe {
        env::set_var("AWS_ENDPOINT", &aws_endpoint);
    }

    let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
    deltalake::aws::register_handlers(Some(aws_url));
    info!("AWS handlers registered");

    // Initialize the database.
    let db = match Database::new().await {
        Ok(db) => {
            info!("Database initialized successfully");
            Arc::new(db)
        },
        Err(e) => {
            error!("Failed to initialize Database: {:?}", e);
            return Err(e);
        }
    };

    // Use the fixed table name "telemetry_events".
    if let Err(e) = db.add_project("telemetry_events", &storage_uri).await {
        error!("Failed to add table 'telemetry_events': {:?}", e);
        return Err(e);
    }
    match db.create_events_table("telemetry_events", &storage_uri).await {
        Ok(_) => info!("Events table created successfully"),
        Err(e) => {
            if e.to_string().contains("already exists") {
                info!("Events table already exists, skipping creation");
            } else {
                error!("Failed to create events table: {:?}", e);
                return Err(e);
            }
        }
    }

    let queue = match PersistentQueue::new("/app/queue_db") {
        Ok(q) => {
            info!("PersistentQueue initialized successfully");
            Arc::new(q)
        },
        Err(e) => {
            error!("Failed to initialize PersistentQueue: {:?}", e);
            return Err(e.into());
        }
    };

    let status_store = Arc::new(IngestStatusStore::new());
    let app_info = web::Data::new(AppInfo {
        start_time: Utc::now(),
        trends: Arc::new(TokioMutex::new(VecDeque::new())),
    });

    // Spawn ingestion service using ingest.rs
    let ingestion_queue = Arc::new(tokio::sync::RwLock::new(Vec::new()));
    start_ingestion_service(db.clone(), ingestion_queue.clone()).await?;

    // Spawn periodic compaction.
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

    // --- DataFusion-Postgres Integration ---
    let pg_addr = format!("0.0.0.0:{}", env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string()));
    info!("Starting DataFusion-Postgres server on {}", pg_addr);
    let listener = TcpListener::bind(&pg_addr).await?;
    let df_session_service = DfSessionService::new(db.get_session_context());
    let handler_factory = HandlerFactory(Arc::new(df_session_service));

    let pg_server = tokio::spawn({
        let pg_shutdown = pgwire_shutdown.clone();
        async move {
            loop {
                tokio::select! {
                    _ = pg_shutdown.cancelled() => {
                        info!("DataFusion-Postgres server shutting down.");
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((socket, peer_addr)) => {
                                info!("Accepted PG connection from {:?}", peer_addr);
                                let handler = HandlerFactory(handler_factory.0.clone());
                                tokio::spawn(async move {
                                    if let Err(e) = process_socket(socket, None, handler).await {
                                        error!("Error processing PG connection: {:?}", e);
                                    }
                                });
                            },
                            Err(e) => {
                                error!("Error accepting PG connection: {:?}", e);
                            }
                        }
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }
    });

    // --- Queue Flush Task ---
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
                        debug!("Checking queue for records to flush");
                        let records = match queue_clone.dequeue_all().await {
                            Ok(r) => {
                                debug!("Dequeued {} records", r.len());
                                r
                            },
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

    // --- HTTP Server ---
    let http_addr = format!("0.0.0.0:{}", env::var("PORT").unwrap_or_else(|_| "80".to_string()));
    info!("Binding HTTP server to {}", http_addr);
    let server = HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(metrics_middleware::MetricsMiddleware)
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(web::Data::new(status_store.clone()))
            .app_data(app_info.clone())
            .service(landing)
            .service(dashboard)
            .service(export_records)
            // Add ingestion endpoints if implemented in ingest.rs
    })
    .bind(&http_addr)?
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
                } else {
                    info!("HTTP server shut down gracefully");
                }
            }
        }
    });

    info!("HTTP server running on http://{}", http_addr);

    // --- Wait for Shutdown Signal or Task Completion ---
    tokio::select! {
        res = pg_server => {
            if let Err(e) = res {
                error!("DataFusion-Postgres server task failed: {:?}", e);
            }
        },
        res = http_task => {
            if let Err(e) = res {
                error!("HTTP server task failed: {:?}", e);
            }
        },
        res = flush_task => {
            if let Err(e) = res {
                error!("Queue flush task failed: {:?}", e);
            }
        },
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
    status_store: &IngestStatusStore,
    key: sled::IVec,
    record: IngestRecord,
) {
    use std::str;
    let id = str::from_utf8(&key).unwrap_or("unknown").to_string();
    status_store.set_status(id.clone(), "Processing".to_string());
    if chrono::DateTime::parse_from_rfc3339(&record.timestamp).is_ok() {
        match db.write(&record).await {
            Ok(()) => {
                INGESTION_COUNTER.inc();
                status_store.set_status(id.clone(), "Ingested".to_string());
                if let Err(e) = spawn_blocking({
                    let queue = queue.clone();
                    move || queue.remove_sync(key)
                }).await {
                    error!("Failed to remove record: {:?}", e);
                }
            }
            Err(e) => {
                ERROR_COUNTER.inc();
                error!("Error writing record: {:?}", e);
                status_store.set_status(id, format!("Failed: {:?}", e));
            }
        }
    } else {
        ERROR_COUNTER.inc();
        error!("Invalid timestamp in record: {}", record.timestamp);
        status_store.set_status(id, "Invalid timestamp".to_string());
        let _ = spawn_blocking({
            let queue = queue.clone();
            move || queue.remove_sync(key)
        }).await;
    }
}