// main.rs
mod database;
mod persistent_queue;
use std::{env, sync::Arc};

use actix_web::{middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use database::Database;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Float64Array, Int32Array, StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    config::ConfigOptions,
    execution::context::SessionContext,
    logical_expr::{create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility},
};
use datafusion_postgres::{DfSessionService, HandlerFactory};
use dotenv::dotenv;
use persistent_queue::{IngestRecord, PersistentQueue};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;

fn register_pg_settings_table(ctx: &SessionContext) -> datafusion::error::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("setting", DataType::Utf8, false),
    ]));

    let names = vec!["TimeZone".to_string(), "client_encoding".to_string(), "datestyle".to_string(), "client_min_messages".to_string()];
    let settings = vec!["UTC".to_string(), "UTF8".to_string(), "ISO, MDY".to_string(), "notice".to_string()];

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(names)), Arc::new(StringArray::from(settings))])?;

    ctx.register_batch("pg_settings", batch)?;
    Ok(())
}

fn register_set_config_udf(ctx: &SessionContext) {
    let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
        let param_value_array = match &args[1] {
            ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>().expect("set_config second arg must be a StringArray"),
            _ => panic!("set_config second arg must be an array"),
        };

        let mut builder = StringBuilder::new();
        for i in 0..param_value_array.len() {
            if param_value_array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_value(param_value_array.value(i));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    });

    let set_config_udf = create_udf(
        "set_config",
        vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
        DataType::Utf8,
        Volatility::Volatile,
        set_config_fn,
    );

    ctx.register_udf(set_config_udf);
}

#[derive(Clone)]
struct AppInfo {}

#[derive(Deserialize)]
struct RegisterProjectRequest {
    project_id: String,
    bucket:     String,
    access_key: String,
    secret_key: String,
    endpoint:   String,
}

#[post("/register_project")]
async fn register_project(req: web::Json<RegisterProjectRequest>, db: web::Data<Arc<Database>>) -> impl Responder {
    match db.register_project(&req.project_id, &req.bucket, &req.access_key, &req.secret_key, &req.endpoint).await {
        Ok(()) => HttpResponse::Ok().json(serde_json::json!({
            "message": format!("Project '{}' registered successfully", req.project_id)
        })),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": format!("Failed to register project: {:?}", e)
        })),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("timefusion=debug,pgwire=trace,datafusion=debug")))
        .init();

    info!("Starting TimeFusion application");

    let bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET environment variable not set");
    let aws_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
    let storage_uri = format!("s3://{}/?endpoint={}", bucket, aws_endpoint);
    info!("Storage URI configured: {}", storage_uri);
    unsafe {
        env::set_var("AWS_ENDPOINT", &aws_endpoint);
    }
    let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
    deltalake::aws::register_handlers(Some(aws_url));
    info!("AWS handlers registered");

    let db = match Database::new(&storage_uri).await {
        Ok(mut db) => {
            info!("Database initialized successfully");
            let mut options = ConfigOptions::new();
            options.set("datafusion.sql_parser.enable_information_schema", "true");
            let ctx = SessionContext::new_with_config(options.into());

            let initial_catalogs = db.get_session_context().catalog_names();
            info!("Initial catalogs: {:?}", initial_catalogs);

            let catalog = db.get_session_context().catalog("datafusion");
            if let Some(catalog) = catalog {
                let schema_names = catalog.schema_names();
                info!("Schemas in 'datafusion' catalog: {:?}", schema_names);
                if schema_names.is_empty() {
                    warn!("No schemas found in 'datafusion' catalog; proceeding with empty context");
                } else {
                    for schema_name in schema_names {
                        if let Some(schema) = catalog.schema(&schema_name) {
                            let table_names = schema.table_names();
                            info!("Tables in schema '{}': {:?}", schema_name, table_names);
                            for table_name in table_names {
                                if let Ok(Some(table_provider)) = schema.table(&table_name).await {
                                    ctx.register_table(&table_name, table_provider)?;
                                    info!("Registered table: {}", table_name);
                                } else {
                                    warn!("Failed to load table provider for: {}", table_name);
                                }
                            }
                        } else {
                            warn!("Schema not found: {}", schema_name);
                        }
                    }
                }
            } else {
                warn!("'datafusion' catalog not found; proceeding with empty context");
            }

            register_pg_settings_table(&ctx)?;
            register_set_config_udf(&ctx);

            let final_catalogs = ctx.catalog_names();
            info!("Final catalogs: {:?}", final_catalogs);

            db.ctx = ctx;
            Arc::new(db)
        }
        Err(e) => {
            error!("Failed to initialize Database: {:?}", e);
            return Err(e);
        }
    };

    let queue_db_path = env::var("QUEUE_DB_PATH").unwrap_or_else(|_| "/app/queue_db".to_string());
    let queue_file_path = format!("{}/queue.db", queue_db_path);
    info!("Using queue DB path: {}", queue_file_path);
    let queue = match PersistentQueue::new(&queue_file_path).await {
        Ok(q) => {
            info!("PersistentQueue initialized successfully");
            Arc::new(q)
        }
        Err(e) => {
            error!("Failed to initialize PersistentQueue: {:?}", e);
            return Err(e.into());
        }
    };

    let app_info = web::Data::new(AppInfo {});

    let shutdown_token = CancellationToken::new();
    let queue_shutdown = shutdown_token.clone();
    let http_shutdown = shutdown_token.clone();
    let pgwire_shutdown = shutdown_token.clone();

    let pg_service = Arc::new(DfSessionService::new(db.get_session_context().clone()));
    let handler_factory = Arc::new(HandlerFactory(pg_service.clone()));
    let pg_addr = env::var("PGWIRE_PORT").unwrap_or_else(|_| "5432".to_string());
    let pg_listener = TcpListener::bind(format!("0.0.0.0:{}", pg_addr)).await?;
    info!("PGWire server running on 0.0.0.0:{}", pg_addr);

    let pg_server = tokio::spawn({
        let handler_factory = handler_factory.clone();
        async move {
            loop {
                tokio::select! {
                    _ = pgwire_shutdown.cancelled() => {
                        info!("PGWire server shutting down.");
                        break;
                    }
                    result = pg_listener.accept() => {
                        match result {
                            Ok((socket, addr)) => {
                                info!("PGWire: Accepted connection from {}", addr);
                                debug!("PGWire: Received connection from {}, preparing to process", addr);
                                let handler_factory = handler_factory.clone();
                                tokio::spawn(async move {
                                    debug!("PGWire: Starting to process socket for {}", addr);
                                    match pgwire::tokio::process_socket(socket, None, handler_factory).await {
                                        Ok(()) => {
                                            info!("PGWire: Connection from {} processed successfully", addr);
                                            debug!("PGWire: Socket processing completed for {}", addr);
                                        }
                                        Err(e) => {
                                            error!("PGWire: Error processing connection from {}: {:?}", addr, e);
                                            debug!("PGWire: Failed socket details: {:?}", e);
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                error!("PGWire: Error accepting connection: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if pg_server.is_finished() {
        error!("PGWire server failed to start, aborting...");
        return Err(anyhow::anyhow!("PGWire server failed to start"));
    }

    let flush_task = {
        let db_clone = db.clone();
        let queue_clone = queue.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = queue_shutdown.cancelled() => {
                        info!("Queue flush task shutting down.");
                        break;
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        debug!("Checking queue for records to flush");
                        let mut records = Vec::new();
                        while let Ok(Some(record)) = queue_clone.dequeue().await {
                            records.push(record);
                        }
                        if !records.is_empty() {
                            info!("Flushing {} enqueued records", records.len());
                            for record in records {
                                process_record(&db_clone, &queue_clone, "default", record).await;
                            }
                        }
                    }
                }
            }
        })
    };

    let http_addr = format!("0.0.0.0:{}", env::var("PORT").unwrap_or_else(|_| "80".to_string()));
    info!("Binding HTTP server to {}", http_addr);
    let server = match HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(queue.clone()))
            .app_data(app_info.clone())
            .service(register_project)
    })
    .bind(&http_addr)
    {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to bind HTTP server to {}: {:?}", http_addr, e);
            return Err(anyhow::anyhow!("Failed to bind HTTP server: {:?}", e));
        }
    }
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

    tokio::select! {
        res = pg_server => {
            if let Err(e) = res {
                error!("PGWire server task failed: {:?}", e);
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

async fn process_record(db: &Arc<Database>, _queue: &Arc<PersistentQueue>, project_id: &str, record: IngestRecord) {
    match db.insert_record(project_id, &record).await {
        Ok(()) => {}
        Err(e) => {
            error!("Error writing record for project '{}': {:?}", project_id, e);
        }
    }
}
