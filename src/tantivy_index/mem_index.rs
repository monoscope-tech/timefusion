//! In-memory tantivy index for a single MemBuffer bucket.
//!
//! Each `TimeBucket` of a tantivy-eligible table holds an `Option<BucketTextIndex>`
//! that's built on first text-match query and re-used until the bucket's
//! row count grows (cheap monotonic check; no per-insert lock contention).
//! Indexes are dropped when the bucket drains or is evicted — they're a
//! pure query cache, never the authoritative source.
//!
//! Memory profile: each index holds `~2× indexed text size` in postings —
//! ~200MB per active bucket at 10 minutes of moderate log ingest. Acceptable
//! while ≤ flush_interval buckets are active; past that window the post-flush
//! callback takes over and these in-memory copies are released.

use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::record_batch::RecordBatch;
use tantivy::Index;

use crate::{
    schema_loader::TableSchema,
    tantivy_index::{
        builder,
        reader::{Hit, PredsQuery, build_node_query, query_index},
        schema::{BuiltSchema, indexed_field_names, register_tokenizers},
        udf::PredNode,
    },
};

/// A built tantivy index covering all rows currently in a bucket.
pub struct BucketTextIndex {
    pub index: Index,
    pub built_schema: Arc<BuiltSchema>,
    /// Row count at build time. The cache is valid while
    /// `bucket.row_count == indexed_rows`. When more rows arrive we
    /// rebuild on next query; the original SQL predicate keeps results
    /// correct in the meantime.
    pub indexed_rows: usize,
    /// Approximate memory cost in bytes (see `estimate_index_size`);
    /// drives the `MemBuffer` LRU budget.
    pub size_bytes: usize,
}

impl BucketTextIndex {
    /// Build (or return None if the table has no indexed fields) from the
    /// bucket's current batches. Caller decides whether to cache the result.
    pub fn build(table: &TableSchema, batches: &[RecordBatch], row_count: usize) -> Result<Option<Self>> {
        let indexed = indexed_field_names(table);
        if indexed.is_empty() || batches.is_empty() {
            return Ok(None);
        }
        let size_bytes = estimate_index_size(&indexed, batches);
        let (index, built_schema, _stats) = builder::build_in_memory(table, batches).with_context(|| format!("build mem-index for {}", table.table_name))?;
        register_tokenizers(&index);
        Ok(Some(Self { index, built_schema: Arc::new(built_schema), indexed_rows: row_count, size_bytes }))
    }

    /// Evaluate a routable predicate tree as ONE combined query. Shares the
    /// query builder with the Delta sidecar search so both sides interpret
    /// predicates identically (And→Must, Or→Should).
    pub fn search_node(&self, node: &PredNode) -> Result<Vec<Hit>> {
        match build_node_query(&self.index, node)? {
            PredsQuery::MissingField => Err(anyhow!("field not in mem-index (schema drift within bucket lifetime)")),
            PredsQuery::Query(q) => query_index(&self.index, q.as_ref(), None),
        }
    }
}

/// Approximate the memory cost of an index built from these batches:
/// indexed-text bytes × 2 (postings + skip-list overhead, conservative for
/// trigram tokenizers). Used by the `MemBuffer` LRU budget — accurate to
/// within ~2× is sufficient since the budget is itself a soft cap.
fn estimate_index_size(indexed_fields: &[String], batches: &[RecordBatch]) -> usize {
    use arrow::array::{Array, AsArray};
    batches
        .iter()
        .flat_map(|batch| indexed_fields.iter().filter_map(move |name| batch.column_by_name(name)))
        .map(|arr| match arr.as_string_opt::<i32>() {
            Some(a) => a.value_data().len(),
            // Utf8View has no contiguous value buffer — total array bytes
            // over-count by view/validity overhead but stay in magnitude.
            None if arr.as_string_view_opt().is_some() => arr.get_array_memory_size(),
            None => 0,
        })
        .sum::<usize>()
        .saturating_mul(2)
}
