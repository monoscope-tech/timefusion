//! Per-parquet-file Tantivy index: parallel sidecar indexes that pre-filter
//! `(timestamp, id)` candidates so Delta/MemBuffer scans stay narrow.
//!
//! Layout: one tantivy index per Delta parquet file, scoped per `project_id`.
//! Schema is derived from the YAML `TableSchema` via `schema::build_for_table`.
//! Indexes always store `_timestamp` (i64, fast) and `_id` (text raw); user
//! columns are indexed-only unless explicitly marked `stored: true`.

pub mod builder;
pub mod manifest;
pub mod reader;
pub mod schema;
pub mod search;
pub mod service;
pub mod store;
pub mod udf;

pub use builder::{IndexBuildStats, build_in_memory};
pub use reader::{Hit, query_index};
pub use schema::{TS_FIELD, ID_FIELD, build_for_table};
