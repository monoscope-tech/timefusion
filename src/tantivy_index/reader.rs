//! Run text/range queries against a built tantivy index and return
//! `(timestamp_micros, id)` candidate pairs for downstream Delta filtering.
//!
//! Query input is a tantivy `Query` constructed by the caller (typically via
//! `QueryParser::for_index(...)` or hand-built `BooleanQuery` + `RangeQuery`).
//! That keeps this module agnostic to how the SQL pushdown layer expresses
//! predicates.

use anyhow::{Result, anyhow};
use tantivy::{Index, TantivyDocument, collector::TopDocs, query::Query, schema::Value};

use crate::tantivy_index::schema::{ID_FIELD, TS_FIELD};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Hit {
    pub timestamp_micros: i64,
    pub id:               String,
}

/// Run a tantivy `Query` against the index and return hits up to `limit`.
/// `limit = None` returns up to a hard cap (currently 1M) to bound memory.
pub fn query_index(index: &Index, query: &dyn Query, limit: Option<usize>) -> Result<Vec<Hit>> {
    let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
    let searcher = reader.searcher();
    let schema = index.schema();
    let ts_field = schema.get_field(TS_FIELD).map_err(|e| anyhow!("missing _timestamp: {e}"))?;
    let id_field = schema.get_field(ID_FIELD).map_err(|e| anyhow!("missing _id: {e}"))?;

    let cap = limit.unwrap_or(1_000_000);
    let top = searcher.search(query, &TopDocs::with_limit(cap)).map_err(|e| anyhow!("search: {e}"))?;
    let mut hits = Vec::with_capacity(top.len());
    for (_score, addr) in top {
        let doc: TantivyDocument = searcher.doc(addr).map_err(|e| anyhow!("doc fetch: {e}"))?;
        let ts = doc.get_first(ts_field).and_then(|v| v.as_i64()).ok_or_else(|| anyhow!("hit missing _timestamp"))?;
        let id = doc.get_first(id_field).and_then(|v| v.as_str()).map(|s| s.to_string()).ok_or_else(|| anyhow!("hit missing _id"))?;
        hits.push(Hit { timestamp_micros: ts, id });
    }
    Ok(hits)
}
