//! In-memory tantivy index for a single MemBuffer bucket.
//!
//! Each `TimeBucket` of a tantivy-eligible table holds an `Option<BucketTextIndex>`
//! that's built on first text-match query and re-used until the bucket's
//! row count grows (cheap monotonic check; no per-insert lock contention).
//! Indexes are dropped when the bucket drains or is evicted — they're a
//! pure query cache, never the authoritative source.
//!
//! Lifecycle:
//! ```
//!   first text_match query           bucket drains
//!         │                                │
//!         ▼                                ▼
//!   build_from_batches()  ←─→  search()  drop_cache()
//!         │
//!         └─► cached until row_count > built_with_rows
//! ```
//!
//! Memory profile: each index holds `~2× indexed text size` in postings.
//! For 10 minutes of moderate log ingest (~100MB indexed text) that's
//! ~200MB per active bucket. Acceptable when there are ≤ flush_interval
//! buckets active at once; outside that window the post-flush callback
//! takes over and these in-memory copies are released.

use anyhow::{Context, Result, anyhow};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tantivy::Index;
use tantivy::query::QueryParser;

use crate::schema_loader::TableSchema;
use crate::tantivy_index::builder;
use crate::tantivy_index::reader::Hit;
use crate::tantivy_index::schema::{BuiltSchema, register_tokenizers};
use crate::tantivy_index::udf::TextMatchPred;

/// A built tantivy index covering all rows currently in a bucket.
pub struct BucketTextIndex {
    pub index: Index,
    pub built_schema: Arc<BuiltSchema>,
    /// Row count at build time. The cache is valid while
    /// `bucket.row_count == indexed_rows`. When more rows arrive we
    /// rebuild on next query; the original SQL predicate keeps results
    /// correct in the meantime.
    pub indexed_rows: usize,
}

impl BucketTextIndex {
    /// Build (or return None if the table has no indexed fields) from the
    /// bucket's current batches. Caller decides whether to cache the result.
    pub fn build(table: &TableSchema, batches: &[RecordBatch], row_count: usize) -> Result<Option<Self>> {
        // Skip if no indexed fields — there's no useful work to do.
        if !table.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)) {
            return Ok(None);
        }
        if batches.is_empty() {
            return Ok(None);
        }
        let (index, built_schema, _stats) = builder::build_in_memory(table, batches).with_context(|| format!("build mem-index for {}", table.table_name))?;
        register_tokenizers(&index);
        Ok(Some(Self { index, built_schema: Arc::new(built_schema), indexed_rows: row_count }))
    }

    /// Run a `text_match`-style query against this index and return hits.
    pub fn search(&self, pred: &TextMatchPred) -> Result<Vec<Hit>> {
        let schema = self.index.schema();
        let field = schema.get_field(&pred.column).map_err(|_| anyhow!("field {} not in mem-index", pred.column))?;
        let mut qp = QueryParser::for_index(&self.index, vec![field]);
        // AND multi-token queries — see comments in search.rs for why this
        // is critical for n-gram-indexed columns.
        qp.set_conjunction_by_default();
        let q = qp.parse_query(&pred.query).map_err(|e| anyhow!("parse mem-index query '{}': {e}", pred.query))?;
        crate::tantivy_index::reader::query_index(&self.index, &*q, None)
    }
}
