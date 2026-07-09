//! Run text/range queries against a built tantivy index and return
//! `(timestamp_micros, id)` candidate pairs for downstream Delta filtering.
//!
//! `build_preds_query` is the single place SQL-side `text_match` predicates
//! become a tantivy query (AND of per-field parsed queries) — shared by the
//! Delta sidecar search and the MemBuffer bucket index so both interpret
//! predicates identically.

use anyhow::{Result, anyhow};
use tantivy::{
    Index, Searcher, TantivyDocument,
    collector::TopDocs,
    query::{BooleanQuery, Occur, Query, QueryParser},
    schema::Value,
};

use crate::tantivy_index::{
    schema::{ID_FIELD, ROW_ORDINAL_FIELD, TS_FIELD},
    udf::TextMatchPred,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Hit {
    pub timestamp_micros: i64,
    pub id: String,
    /// Row offset within the covered parquet file, when the index carries the
    /// `_row_ordinal` fast field. Only meaningful for read-back-built indexes
    /// (`ManifestEntry.ordinals_valid`) — see schema.rs.
    pub row_ordinal: Option<u64>,
}

/// Outcome of compiling predicates against a concrete index's schema.
pub enum PredsQuery {
    Query(Box<dyn Query>),
    /// The index predates one of the queried fields (schema evolution) —
    /// it cannot answer the predicate; callers must treat this as a
    /// coverage gap, not an empty result.
    MissingField,
}

/// Compile `preds` (implicitly AND-ed) into one tantivy query for `index`.
pub fn build_preds_query(index: &Index, preds: &[TextMatchPred]) -> Result<PredsQuery> {
    if preds.is_empty() {
        return Err(anyhow!("no predicates"));
    }
    let node = match preds.len() {
        1 => crate::tantivy_index::udf::PredNode::Leaf(preds[0].clone()),
        _ => crate::tantivy_index::udf::PredNode::And(preds.iter().cloned().map(crate::tantivy_index::udf::PredNode::Leaf).collect()),
    };
    build_node_query(index, &node)
}

/// Compile a routable predicate tree into one tantivy query for `index`:
/// And→all-Must, Or→all-Should (matches ≥1). Leaf parsing is conjunctive
/// across tokens — critical for n-gram: "hello" tokenizes into trigrams
/// `hel`,`ell`,`llo` and ALL must match (a single matching trigram doesn't
/// imply substring presence).
pub fn build_node_query(index: &Index, node: &crate::tantivy_index::udf::PredNode) -> Result<PredsQuery> {
    use crate::tantivy_index::udf::PredNode;
    let q = match node {
        PredNode::Leaf(p) => {
            let Ok(field) = index.schema().get_field(&p.column) else {
                return Ok(PredsQuery::MissingField);
            };
            let mut qp = QueryParser::for_index(index, vec![field]);
            qp.set_conjunction_by_default();
            qp.parse_query(&p.query).map_err(|e| anyhow!("parse query '{}': {e}", p.query))?
        }
        PredNode::And(kids) | PredNode::Or(kids) => {
            let occur = if matches!(node, PredNode::And(_)) { Occur::Must } else { Occur::Should };
            let mut subs: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(kids.len());
            for k in kids {
                match build_node_query(index, k)? {
                    PredsQuery::MissingField => return Ok(PredsQuery::MissingField),
                    PredsQuery::Query(q) => subs.push((occur, q)),
                }
            }
            Box::new(BooleanQuery::new(subs))
        }
    };
    Ok(PredsQuery::Query(q))
}

/// Run a tantivy `Query` against the index and return hits up to `limit`.
/// `limit = None` returns up to a hard cap (currently 1M) to bound memory.
pub fn query_index(index: &Index, query: &dyn Query, limit: Option<usize>) -> Result<Vec<Hit>> {
    let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
    query_with_searcher(&reader.searcher(), query, limit)
}

/// As `query_index`, but with a caller-provided (cached) searcher.
/// Hit extraction prefers fast fields (`_timestamp` is always FAST; `_id`
/// is FAST on indexes built after 2026-07-05) and falls back to the doc
/// store per segment for older indexes.
pub fn query_with_searcher(searcher: &Searcher, query: &dyn Query, limit: Option<usize>) -> Result<Vec<Hit>> {
    let schema = searcher.schema();
    let ts_field = schema.get_field(TS_FIELD).map_err(|e| anyhow!("missing _timestamp: {e}"))?;
    let id_field = schema.get_field(ID_FIELD).map_err(|e| anyhow!("missing _id: {e}"))?;

    let cap = limit.unwrap_or(1_000_000);
    let top = searcher.search(query, &TopDocs::with_limit(cap)).map_err(|e| anyhow!("search: {e}"))?;
    // Per-segment fast-field columns, resolved lazily on first hit in that
    // segment. `None` in the outer Option = not yet resolved; inner `None`
    // = this segment has no fast `_id` (pre-fast-field index) → doc store.
    type FfCols = (
        tantivy::columnar::Column<i64>,
        tantivy::columnar::StrColumn,
        Option<tantivy::columnar::Column<u64>>,
    );
    let mut ff_cols: Vec<Option<Option<FfCols>>> = vec![None; searcher.segment_readers().len()];
    let mut hits = Vec::with_capacity(top.len());
    let mut id_buf = String::new();
    for (_score, addr) in top {
        let seg = addr.segment_ord as usize;
        let cols = ff_cols[seg].get_or_insert_with(|| {
            let ff = searcher.segment_reader(addr.segment_ord).fast_fields();
            match (ff.i64(TS_FIELD), ff.str(ID_FIELD)) {
                (Ok(ts), Ok(Some(id))) => Some((ts, id, ff.u64(ROW_ORDINAL_FIELD).ok())),
                _ => None,
            }
        });
        if let Some((ts_col, id_col, ord_col)) = cols {
            let ts = ts_col.first(addr.doc_id);
            let ord = id_col.term_ords(addr.doc_id).next();
            if let (Some(ts), Some(ord)) = (ts, ord) {
                id_buf.clear();
                if id_col.ord_to_str(ord, &mut id_buf).map_err(|e| anyhow!("fast _id read: {e}"))? {
                    hits.push(Hit {
                        timestamp_micros: ts,
                        id: id_buf.clone(),
                        row_ordinal: ord_col.as_ref().and_then(|c| c.first(addr.doc_id)),
                    });
                    continue;
                }
            }
            // Fast columns exist but this doc lacks values (shouldn't happen
            // for required fields) — fall through to the doc store.
        }
        let doc: TantivyDocument = searcher.doc(addr).map_err(|e| anyhow!("doc fetch: {e}"))?;
        let ts = doc.get_first(ts_field).and_then(|v| v.as_i64()).ok_or_else(|| anyhow!("hit missing _timestamp"))?;
        let id = doc.get_first(id_field).and_then(|v| v.as_str()).map(|s| s.to_string()).ok_or_else(|| anyhow!("hit missing _id"))?;
        hits.push(Hit {
            timestamp_micros: ts,
            id,
            row_ordinal: None,
        });
    }
    Ok(hits)
}
