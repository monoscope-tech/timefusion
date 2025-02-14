mod database;
mod ingest;
mod persistent_queue;
mod pgserver_message;
mod pgwire_integration;
mod utils;

use actix_web::{web, App, HttpServer};
use chrono::Utc;
use database::Database;
use pgwire_integration::{DfSessionService, run_pgwire_server, HandlerFactory};
use persistent_queue::PersistentQueue;
use std::sync::Arc;
use std::str;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Register S3 handlers for Delta Lake.
    deltalake::aws::register_handlers(None);

    let bucket = std::env::var("S3_BUCKET_NAME")
        .expect("S3_BUCKET_NAME environment variable not set");
    let s3_uri = format!("s3://{}/delta_table", bucket);
    
    // Initialize the Database.
    let db = Arc::new(Database::new().await?);
    db.add_project("events", &s3_uri).await?;
    
    // Write a sample record.
    let now = Utc::now();
    db.write("events", now, Some(now), None, Some("data1")).await?;
    
    // Run a sample query.
    db.query("SELECT * FROM \"table_events\" WHERE project_id = 'events'")
        .await?
        .show()
        .await?;
    
    db.compact("events").await?;
    
    // Initialize the persistent queue.
    let queue = Arc::new(PersistentQueue::new("./queue_db"));
    
    // Initialize the ingestion status store.
    let status_store = Arc::new(ingest::IngestStatusStore::new());

    // Spawn a background task to flush the persistent queue.
    {
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        let status_store_clone = status_store.clone();
        tokio::spawn(async move {
            loop {
                let records = match queue_clone.dequeue_all().await {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("Error in dequeue_all: {}", e);
                        Vec::new()
                    }
                };
                if !records.is_empty() {
                    println!("Flushing {} enqueued records", records.len());
                    for (key, record) in records {
                        let key_vec = key.to_vec();
                        let id = match str::from_utf8(&key_vec) {
                            Ok(s) => s.to_string(),
                            Err(e) => {
                                eprintln!("Error reading key: {}", e);
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
                                        let owned_key = sled::IVec::from(key_vec_owned);
                                        let remove_res = tokio::task::spawn_blocking(move || {
                                            queue_clone2.remove_sync(owned_key)
                                        }).await;
                                        match remove_res {
                                            Ok(Ok(())) => println!("Successfully removed key."),
                                            Ok(Err(e)) => eprintln!("Removal failed: {}", e),
                                            Err(e) => eprintln!("Spawn blocking join error during removal: {}", e),
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!("Error writing record from queue: {}", e);
                                    status_store_clone.set_status(id.clone(), format!("Failed: {}", e));
                                }
                            }
                        } else {
                            eprintln!("Invalid timestamp in queued record");
                            let queue_clone3 = queue_clone.clone();
                            let key_vec_owned = key_vec.clone();
                            let remove_res = tokio::task::spawn_blocking(move || {
                                queue_clone3.remove_sync(sled::IVec::from(key_vec_owned))
                            }).await;
                            match remove_res {
                                Ok(Ok(())) => println!("Successfully removed key for invalid timestamp."),
                                Ok(Err(e)) => eprintln!("Removal failed for invalid timestamp: {}", e),
                                Err(e) => eprintln!("Spawn blocking join error during removal for invalid timestamp: {}", e),
                            }
                            status_store_clone.set_status(id.clone(), "Invalid timestamp".to_string());
                        }
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }
    
    // Start the PGWire server.
    let pg_service = DfSessionService::new(db.get_session_context(), db.clone());
    let handler_factory = HandlerFactory(Arc::new(pg_service));
    let pg_addr = "127.0.0.1:5432";
    println!("Spawning PGWire server task on {}", pg_addr);
    let pg_server = tokio::spawn(async move {
        if let Err(e) = run_pgwire_server(handler_factory, pg_addr).await {
            eprintln!("PGWire server error: {:?}", e);
        }
    });
    
    // Start the HTTP server.
    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(web::Data::new(status_store.clone()))
            .service(ingest::ingest)
            .service(ingest::get_status)
    })
    .bind("127.0.0.1:8080")?
    .run();
    
    println!("HTTP server running on http://127.0.0.1:8080");
    
    tokio::select! {
        res = pg_server => res?,
        res = http_server => res?,
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down.");
        }
    }
    
    Ok(())
}
