//! Small extension traits that remove the repeated error-mapping boilerplate
//! recurring across the crate. The two most common shapes are:
//!
//! ```ignore
//! something().map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
//! something().map_err(|e| DataFusionError::Execution(format!("context: {e}")))?;
//! ```
//!
//! which become `something().into_df()?` and `something().exec_context("context")?`.

use std::fmt::Display;

use datafusion::error::{DataFusionError, Result as DFResult};

/// Convert an Arrow result into a DataFusion result, wrapping the error as
/// [`DataFusionError::ArrowError`].
///
/// Replaces `.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))`.
pub trait ArrowResultExt<T> {
    fn into_df(self) -> DFResult<T>;
}

impl<T> ArrowResultExt<T> for Result<T, arrow_schema::ArrowError> {
    #[inline]
    fn into_df(self) -> DFResult<T> {
        self.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Attach a static context message to any displayable error and surface it as
/// [`DataFusionError::Execution`].
///
/// Replaces `.map_err(|e| DataFusionError::Execution(format!("msg: {e}")))`.
pub trait ExecResultExt<T> {
    fn exec_context(self, msg: &str) -> DFResult<T>;
}

impl<T, E: Display> ExecResultExt<T> for Result<T, E> {
    #[inline]
    fn exec_context(self, msg: &str) -> DFResult<T> {
        self.map_err(|e| DataFusionError::Execution(format!("{msg}: {e}")))
    }
}
