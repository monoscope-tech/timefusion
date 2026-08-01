//! Shared error-wrapping helpers to collapse the repetitive `.map_err(|e| ...)`
//! closures scattered across the write/query paths. Each preserves the original
//! DataFusionError variant and message text.

use std::fmt::Display;

use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use datafusion_postgres::pgwire::error::PgWireError;

/// Wrap an Arrow error as `DataFusionError::ArrowError`. Unlike
/// `DataFusionError::from`, skips backtrace capture — these fire on hot paths.
pub fn arrow_err(e: ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// `.map_err(exec_err("context"))` → `Execution("context: {e}")`.
pub fn exec_err<E: Display>(ctx: &'static str) -> impl Fn(E) -> DataFusionError {
    move |e| DataFusionError::Execution(format!("{ctx}: {e}"))
}

/// `.map_err(wal_err("op"))` → `External("WAL op failed: {e}")`.
pub fn wal_err<E: Display>(op: &'static str) -> impl Fn(E) -> DataFusionError {
    move |e| DataFusionError::External(format!("WAL {op} failed: {e}").into())
}

/// Wrap any std error as `PgWireError::ApiError` — the pgwire escape hatch for
/// errors that carry no SQLSTATE of their own.
pub fn api_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> PgWireError {
    PgWireError::ApiError(Box::new(e))
}
