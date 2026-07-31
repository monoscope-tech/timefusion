//! Run text/range queries against a built tantivy index and return
//! `(timestamp_micros, id)` candidate pairs for downstream Delta filtering.
//!
//! `build_preds_query` is the single place SQL-side `text_match` predicates
//! become a tantivy query (AND of per-field parsed queries) — shared by the
//! Delta sidecar search and the MemBuffer bucket index so both interpret
//! predicates identically.

use std::collections::HashSet;

use anyhow::{Result, anyhow};
use tantivy::{
    Index, Searcher, TantivyDocument, Term,
    collector::TopDocs,
    query::{BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::{Field, FieldType, IndexRecordOption, Value},
};

use crate::tantivy_index::{
    schema::{ID_FIELD, NGRAM3_TOKENIZER, ROW_ORDINAL_FIELD, TS_FIELD},
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
            let schema = index.schema();
            let Ok(field) = schema.get_field(&p.column) else {
                return Ok(PredsQuery::MissingField);
            };
            let tokenizer = match schema.get_field_entry(field).field_type() {
                FieldType::Str(o) => o.get_indexing_options().map(|i| i.tokenizer().to_string()),
                _ => None,
            };
            let ngram = tokenizer.as_deref() == Some(NGRAM3_TOKENIZER);
            match &tokenizer {
                // On ngram3 a trailing `*` is only a routing marker (prefix ⊆
                // substring), so the literal can always be analyzed. On raw/
                // default it carries real prefix semantics that a conjunction of
                // whole terms would UNDER-match, so those keep the parser.
                Some(tok) if ngram || !p.query.ends_with('*') => analyzed_conjunction_query(index, field, tok, &p.query)?,
                _ => {
                    let mut qp = QueryParser::for_index(index, vec![field]);
                    qp.set_conjunction_by_default();
                    qp.parse_query(&p.query).map_err(|e| anyhow!("parse query '{}': {e}", p.query))?
                }
            }
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

/// Compile a routed literal against `field` WITHOUT tantivy's `QueryParser`:
/// run the field's own analyzer over the literal and AND the resulting terms.
///
/// The parser's grammar is fatal here: routed literals are user data, and a
/// whitespace-adjacent `-` (`"accept -header"`) becomes a MustNot clause while
/// bare `AND`/`OR`/`NOT` become operators — and on a raw-tokenized field any
/// whitespace splits one indexed token into two unmatchable ones. Such a query
/// parses "successfully" and returns ZERO hits, so the intersecting
/// `id IN (hits)` prefilter silently drops matching rows.
///
/// Matching semantics vs. the old parser path: `QueryParser` turned a
/// multi-token word into a PhraseQuery (for ngram3, consecutive trigrams). A
/// conjunction of the same terms is a strict SUPERSET of that — always sound
/// for a prefilter, which may only ever over-select. Preserved from the old
/// path: whitespace splits into independently-required words, a word too short
/// to produce a token simply broadens (is dropped), and a literal that yields
/// no token at all errors out into a full scan.
fn analyzed_conjunction_query(index: &Index, field: Field, tokenizer: &str, query: &str) -> Result<Box<dyn Query>> {
    let mut analyzer = index.tokenizers().get(tokenizer).ok_or_else(|| anyhow!("tokenizer {tokenizer} not registered"))?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
    // ngram3 keeps the parser's whitespace-AND behaviour (each word required
    // independently, short words dropped). The other tokenizers must see the
    // WHOLE literal: `default` splits it itself, and `raw` indexes the value as
    // one token, so pre-splitting it would under-match.
    let words: Vec<&str> = if tokenizer == NGRAM3_TOKENIZER { query.split_whitespace().collect() } else { vec![query] };
    for word in words {
        // Plan-time classification appends `*` for `LIKE 'foo%'`; on a 3-gram
        // field a prefix match is a subset of the substring match, so dropping
        // the marker keeps the prefilter a superset. (Non-ngram3 `*` queries
        // never reach here — see the caller.)
        let mut stream = analyzer.token_stream(word.trim_end_matches('*'));
        stream.process(&mut |t| {
            if seen.insert(t.text.clone()) {
                clauses.push((Occur::Must, Box::new(TermQuery::new(Term::from_field_text(field, &t.text), IndexRecordOption::Basic)) as Box<dyn Query>));
            }
        });
    }
    if clauses.is_empty() {
        return Err(anyhow!("query '{query}' produces no {tokenizer} terms"));
    }
    Ok(Box::new(BooleanQuery::new(clauses)))
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
    type FfCols = (tantivy::columnar::Column<i64>, tantivy::columnar::StrColumn, Option<tantivy::columnar::Column<u64>>);
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
                    hits.push(Hit { timestamp_micros: ts, id: id_buf.clone(), row_ordinal: ord_col.as_ref().and_then(|c| c.first(addr.doc_id)) });
                    continue;
                }
            }
            // Fast columns exist but this doc lacks values (shouldn't happen
            // for required fields) — fall through to the doc store.
        }
        let doc: TantivyDocument = searcher.doc(addr).map_err(|e| anyhow!("doc fetch: {e}"))?;
        let ts = doc.get_first(ts_field).and_then(|v| v.as_i64()).ok_or_else(|| anyhow!("hit missing _timestamp"))?;
        let id = doc.get_first(id_field).and_then(|v| v.as_str()).map(|s| s.to_string()).ok_or_else(|| anyhow!("hit missing _id"))?;
        hits.push(Hit { timestamp_micros: ts, id, row_ordinal: None });
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use tantivy::doc;

    use super::*;
    use crate::{
        schema_loader::{FieldDef, TableSchema, TantivyFieldConfig},
        tantivy_index::{schema::build_for_table, udf::PredNode},
    };

    /// Index three docs whose text contains tokens that tantivy's `QueryParser`
    /// grammar would swallow (`-x` → MustNot, bare `NOT` → operator). Before
    /// the parser was taken out of the substring path these queries parsed
    /// "successfully" and returned zero hits, so the `id IN (hits)` prefilter
    /// silently dropped the matching rows.
    fn ngram_index() -> Index {
        let table = TableSchema {
            table_name: "t".into(),
            partitions: vec![],
            sorting_columns: vec![],
            z_order_columns: vec![],
            time_column: None,
            dedup_keys: vec![],
            dedup_tiebreak: None,
            tombstone_column: None,
            version_append: false,
            fields: ["body", "level"]
                .into_iter()
                .map(|name| FieldDef {
                    name: name.into(),
                    data_type: "String".into(),
                    nullable: true,
                    // body → ngram3 (default), level → raw single token.
                    tantivy: Some(TantivyFieldConfig { indexed: true, tokenizer: (name == "level").then(|| "raw".to_string()), flatten: None }),
                    dictionary: None,
                    bloom_filter: false,
                })
                .collect(),
        };
        let built = build_for_table(&table);
        let index = Index::create_in_ram(built.schema.clone());
        crate::tantivy_index::schema::register_tokenizers(&index);
        let (body, level) = (built.user_fields["body"].field, built.user_fields["level"].field);
        let mut w = index.writer(15_000_000).expect("writer");
        let docs = [("accept -header now", "-alpha"), ("err -1234 code", "ERROR"), ("foo NOT bar baz", "two words"), ("unrelated payload", "INFO")];
        for (i, (text, lvl)) in docs.iter().enumerate() {
            w.add_document(doc!(built.timestamp => i as i64, built.id => format!("id{i}"), body => *text, level => *lvl)).expect("add");
        }
        w.commit().expect("commit");
        index
    }

    fn hit_ids(index: &Index, query: &str) -> Result<Vec<String>> {
        hit_ids_on(index, "body", query)
    }

    fn hit_ids_on(index: &Index, column: &str, query: &str) -> Result<Vec<String>> {
        let node = PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() });
        let PredsQuery::Query(q) = build_node_query(index, &node)? else { panic!("field must exist") };
        let mut ids: Vec<String> = query_index(index, &*q, None)?.into_iter().map(|h| h.id).collect();
        ids.sort();
        Ok(ids)
    }

    #[test]
    fn query_grammar_chars_in_routed_substrings_still_hit() {
        let index = ngram_index();
        // `-` adjacent to whitespace used to become a MustNot clause.
        assert_eq!(hit_ids(&index, "accept -header").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids(&index, "err -1234").unwrap(), vec!["id1"]);
        // Bare `NOT` used to be parsed as an operator.
        assert_eq!(hit_ids(&index, "foo NOT bar").unwrap(), vec!["id2"]);
        // Single words and the full literal still work.
        assert_eq!(hit_ids(&index, "header").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids(&index, "accept -header now").unwrap(), vec!["id0"]);
        // Case-insensitive (LowerCaser in tf_ngram3).
        assert_eq!(hit_ids(&index, "ACCEPT").unwrap(), vec!["id0"]);
        // Prefix marker from `LIKE 'foo%'` routing is dropped (substring ⊇ prefix).
        assert_eq!(hit_ids(&index, "accept*").unwrap(), vec!["id0"]);
        // Still selective: a literal present in no doc returns nothing.
        assert!(hit_ids(&index, "zzzqqq").unwrap().is_empty());
    }

    #[test]
    fn short_tokens_broaden_and_all_short_literals_scan() {
        let index = ngram_index();
        // A <3-char word yields no trigram → dropped (broadens), never empties.
        assert_eq!(hit_ids(&index, "header xy").unwrap(), vec!["id0"]);
        // Nothing left to query → error, which callers treat as "no prefilter".
        assert!(hit_ids(&index, "xy").is_err());
    }

    #[test]
    fn raw_tokenized_terms_match_the_whole_indexed_value() {
        let index = ngram_index();
        assert_eq!(hit_ids_on(&index, "level", "ERROR").unwrap(), vec!["id1"]);
        // A leading `-` used to parse as a lone MustNot clause (∅ hits), and
        // whitespace used to AND-split a value indexed as ONE raw token.
        assert_eq!(hit_ids_on(&index, "level", "-alpha").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids_on(&index, "level", "two words").unwrap(), vec!["id2"]);
        // Raw stays exact/case-sensitive: no partial or case-folded matches.
        assert!(hit_ids_on(&index, "level", "error").unwrap().is_empty());
        assert!(hit_ids_on(&index, "level", "two").unwrap().is_empty());
    }
}
