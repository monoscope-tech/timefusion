use datafusion::execution::context::SessionContext;
use datafusion::error::Result as DFResult;
use tracing::{debug, warn};

/// Initialize pg_catalog in the session context by copying catalog tables
pub async fn init_pg_catalog(ctx: &SessionContext) -> DFResult<()> {
    let database_name = std::env::var("TIMEFUSION_DATABASE")
        .unwrap_or_else(|_| "timefusion".to_string());
    let schema_name = std::env::var("TIMEFUSION_SCHEMA")
        .unwrap_or_else(|_| "public".to_string());

    match datafusion_pg_catalog::get_base_session_context(
        None, // Don't use file-based schema, use in-memory
        database_name.clone(),
        schema_name.clone(),
        None, // No custom table lister function
    ).await {
        Ok((pg_ctx, _log)) => {
            // Copy pg_catalog and information_schema from pg_ctx to our context
            if let Some(catalog) = pg_ctx.catalog("datafusion") {
                // Register pg_catalog schema
                if let Some(pg_schema) = catalog.schema("pg_catalog") {
                    let table_count = pg_schema.table_names().len();
                    if let Some(our_catalog) = ctx.catalog("datafusion") {
                        our_catalog.register_schema("pg_catalog", pg_schema)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Failed to register pg_catalog schema: {}", e)
                            ))?;
                        debug!("Registered pg_catalog schema with {} tables", table_count);
                    }
                }

                // Register information_schema if available
                if let Some(info_schema) = catalog.schema("information_schema") {
                    if let Some(our_catalog) = ctx.catalog("datafusion") {
                        our_catalog.register_schema("information_schema", info_schema)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Failed to register information_schema: {}", e)
                            ))?;
                        debug!("Registered information_schema");
                    }
                }
            }
            Ok(())
        }
        Err(e) => {
            warn!("Failed to initialize pg_catalog (continuing without it): {}", e);
            Ok(())
        }
    }
}
