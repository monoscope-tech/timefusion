use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use tracing::debug;

/// Initialize pg_catalog in the session context
/// Note: pg_catalog is automatically handled by datafusion-postgres in newer versions
pub async fn init_pg_catalog(_ctx: &SessionContext) -> DFResult<()> {
    // pg_catalog is now managed by datafusion-postgres automatically
    // This function is kept for API compatibility
    debug!("pg_catalog initialization skipped - handled by datafusion-postgres");
    Ok(())
}
