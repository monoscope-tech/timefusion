//! Arrow data encoding and type mapping for Postgres(pgwire).

// #[cfg(all(feature = "arrow", feature = "datafusion"))]
// compile_error!("Feature arrow and datafusion cannot be enabled at same time. Use no-default-features when activating datafusion");

pub mod datatypes;
pub mod encoder;
mod error;
#[cfg(feature = "postgis")]
pub mod geo_encoder;
pub mod list_encoder;
pub mod row_encoder;
pub mod struct_encoder;

#[cfg(feature = "datafusion")]
pub use datatypes::df::encode_dataframe;

pub use datatypes::encode_recordbatch;
