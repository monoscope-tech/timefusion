// main.rs
mod database;
mod persistent_queue;
use actix_web::{middleware::Logger, post, web, App, HttpResponse, HttpServer, Responder};
use database::Database;
use datafusion::{
    arrow::{
        array::{Array, StringArray, StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    config::ConfigOptions,
    execution::context::SessionContext,
    logical_expr::{create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility},
};
use datafusion_postgres::{DfSessionService, HandlerFactory};
use dotenv::dotenv;
use futures::TryFutureExt;
use persistent_queue::OtelLogsAndSpans;
use serde::Deserialize;
use std::{env, sync::Arc};
use tokio::{
    net::TcpListener,
    time::{sleep, Duration},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

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
    bucket: String,
    access_key: String,
    secret_key: String,
    endpoint: Option<String>,
}

#[post("/register_project")]
async fn register_project(req: web::Json<RegisterProjectRequest>, db: web::Data<Arc<Database>>) -> impl Responder {
    match db
        .register_project(
            &req.project_id,
            &req.bucket,
            Some(&req.access_key),
            Some(&req.secret_key),
            req.endpoint.as_deref(),
        )
        .await
    {
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

    let mut db = Database::new().await?;
    info!("Database initialized successfully");
    let mut options = ConfigOptions::new();
    let _ = options.set("datafusion.sql_parser.enable_information_schema", "true");
    let session_context = SessionContext::new_with_config(options.into());

    let initial_catalogs = db.session_context.catalog_names();
    info!("Initial catalogs: {:?}", initial_catalogs);

    let catalog = db.session_context.catalog("datafusion");
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
                            session_context.register_table(&table_name, table_provider)?;
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

    register_pg_settings_table(&session_context)?;
    register_set_config_udf(&session_context);

    info!("Final catalogs: {:?}", session_context.catalog_names());

    db.session_context = session_context;
    let db = Arc::new(db);

    // Queue logic removed - writing directly to Delta Lake

    let app_info = web::Data::new(AppInfo {});

    let shutdown_token = CancellationToken::new();
    // Queue shutdown token removed
    let http_shutdown = shutdown_token.clone();
    let pgwire_shutdown = shutdown_token.clone();

    let pg_service = Arc::new(DfSessionService::new(db.session_context.clone()));
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
                                debug!("PGWire: Received connection from {}, preparing to process", addr);
                                let handler_factory = handler_factory.clone();
                                tokio::spawn(async move {
                                    match pgwire::tokio::process_socket(socket, None, handler_factory).await {
                                        Ok(()) => {
                                            info!("PGWire: Connection from {} processed successfully", addr);
                                        }
                                        Err(e) => {
                                            error!("PGWire: Error processing connection from {}: {:?}", addr, e);
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

    // Removed queue flush task - now writing directly to Delta Lake

    let http_addr = format!("0.0.0.0:{}", env::var("PORT").unwrap_or_else(|_| "80".to_string()));
    info!("Binding HTTP server to {}", http_addr);
    let server = match HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db.clone()))
            // Queue removed, now writing directly to Delta Lake
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
            _ = http_shutdown.cancelled() => info!("HTTP server shutting down."),
            res = server => res.map_or_else(
                |e| error!("HTTP server failed: {:?}", e),
                |_| info!("HTTP server shut down gracefully")
            ),
        }
    });
    info!("HTTP server running on http://{}", http_addr);

    tokio::select! {
        _ = pg_server.map_err(|e| error!("PGWire server task failed: {:?}", e)) => {},
        _ = http_task.map_err(|e| error!("HTTP server task failed: {:?}", e)) => {},
        // Queue flush task removed
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
