//! Build a Tantivy `Schema` from the YAML `TableSchema`.
//!
//! Always emits two reserved fields:
//! - `_timestamp`: i64 microseconds, STORED + FAST (range queries, sort)
//! - `_id`: text raw tokenizer, STORED (returned to caller for prefilter)
//!
//! User fields are honored from `FieldDef.tantivy`. Only fields with
//! `indexed: true` produce a tantivy field. Tokenizer choice:
//!   "raw"     → keyword (exact match, single token; case-sensitive)
//!   "default" → tantivy default tokenizer (lowercase + word split)
//!   "ngram3"  → lowercased 3-grams; supports `LIKE '%substr%'`, `'%suffix'`,
//!               and `ILIKE 'word'`. Larger postings than word tokenizer
//!               but the trigram dictionary is bounded (~10k entries for
//!               ASCII), so net index size is typically 1.5–2× vs default.
//!
//! **Default (no tokenizer specified)**: `ngram3` — substring search is the
//! dominant pattern for logs/traces. Opt-down to `raw`/`default` for
//! point-lookup-only columns (IDs, enums).

use std::collections::HashMap;

use tantivy::{
    Index,
    schema::{FAST, Field, INDEXED, IndexRecordOption, NumericOptions, STORED, Schema, SchemaBuilder, TextFieldIndexing, TextOptions},
    tokenizer::{AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Tokenizer},
};

use crate::schema_loader::{FieldDef, TableSchema, TantivyFieldConfig};

/// Tokenizer name we use for n-gram indexing. Combined with `LowerCaser` so
/// `ILIKE` semantics fall out automatically.
pub const NGRAM3_TOKENIZER: &str = "tf_ngram3";
/// Tokenizer name we use for word-level indexing (lowercase + word split +
/// ASCII folding + max-length cap). Same name as tantivy's default so
/// the `TEXT` field options can reuse it.
pub const DEFAULT_TOKENIZER: &str = "default";
/// Tokenizer name for keyword/exact-match indexing.
pub const RAW_TOKENIZER: &str = "raw";
/// Token length cap; bounds posting growth on pathological inputs.
const MAX_TOKEN_LEN: usize = 256;

// User fields are indexed-only by design: tantivy is a search index, not a
// document store — the authoritative row payload lives in Delta/parquet.
// Only `_timestamp` and `_id` are stored, because the reader needs them to
// emit `(timestamp, id)` hits that the SQL layer joins back against Delta.

pub const TS_FIELD: &str = "_timestamp";
pub const ID_FIELD: &str = "_id";
/// Global row offset of the doc within the file the index covers (FAST).
/// Only meaningful when the index was built by reading the parquet back in
/// row order (`ManifestEntry.ordinals_valid`) — the flush path indexes
/// pre-sort batches whose order differs from the written file.
pub const ROW_ORDINAL_FIELD: &str = "_row_ordinal";

/// Result of building a tantivy schema for a table.
pub struct BuiltSchema {
    pub schema: Schema,
    pub timestamp: Field,
    pub id: Field,
    pub row_ordinal: Field,
    /// Map of source-column-name → tantivy field. Only contains user columns
    /// that were `indexed: true` in YAML. Variants/lists are included here.
    pub user_fields: HashMap<String, UserField>,
}

#[derive(Debug, Clone)]
pub struct UserField {
    pub field: Field,
    pub source: FieldDef,
}

pub fn build_for_table(table: &TableSchema) -> BuiltSchema {
    let mut b = SchemaBuilder::new();
    let timestamp = b.add_i64_field(TS_FIELD, NumericOptions::default() | STORED | FAST | INDEXED);
    let id = b.add_text_field(ID_FIELD, raw_id_options());
    let row_ordinal = b.add_u64_field(ROW_ORDINAL_FIELD, NumericOptions::default() | FAST);

    let user_fields: HashMap<_, _> = table
        .fields
        .iter()
        .filter(|fd| fd.name != TS_FIELD && fd.name != ID_FIELD)
        .filter_map(|fd| fd.tantivy.as_ref().filter(|cfg| cfg.indexed).map(|cfg| (fd, cfg)))
        .map(|(fd, cfg)| (fd.name.clone(), UserField { field: b.add_text_field(&fd.name, text_options_for(cfg)), source: fd.clone() }))
        .collect();
    BuiltSchema { schema: b.build(), timestamp, id, row_ordinal, user_fields }
}

fn raw_id_options() -> TextOptions {
    // FAST (raw-normalized) lets the reader pull hit ids from the columnar
    // store instead of per-doc doc-store fetches. STORED is kept so indexes
    // remain readable by the pre-fast-field fallback path (and older readers).
    TextOptions::default()
        .set_indexing_options(TextFieldIndexing::default().set_tokenizer(RAW_TOKENIZER).set_index_option(IndexRecordOption::Basic))
        .set_fast(Some(RAW_TOKENIZER))
        | STORED
}

/// Canonicalize a YAML tokenizer name. Absent *and* unknown names fall through
/// to ngram3 (better-than-nothing rather than panic): the vast majority of
/// log/trace text queries use `LIKE '%substr%'` / `ILIKE`, which only the
/// n-gram index can accelerate.
fn canonical_tokenizer(cfg: &TantivyFieldConfig) -> &'static str {
    match cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER) {
        RAW_TOKENIZER => RAW_TOKENIZER,
        DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
        _ => NGRAM3_TOKENIZER,
    }
}

fn text_options_for(cfg: &TantivyFieldConfig) -> TextOptions {
    let name = canonical_tokenizer(cfg);
    // WithFreqsAndPositions is needed for phrase queries (which n-gram matching
    // reduces to: consecutive trigrams of the query string).
    let index_option = if name == RAW_TOKENIZER { IndexRecordOption::Basic } else { IndexRecordOption::WithFreqsAndPositions };
    TextOptions::default().set_indexing_options(TextFieldIndexing::default().set_tokenizer(name).set_index_option(index_option))
}

/// Register TimeFusion's custom tokenizers on a tantivy `Index`. Must be
/// called immediately after `Index::create*` and on every reader open;
/// tantivy's tokenizer registry is per-index, not global.
///
/// Registers:
/// - `tf_ngram3`: 3-grams over lowercased + ASCII-folded text, with a 256-char
///   length cap to bound posting growth on pathological inputs.
/// - `default`, `raw`: already registered by tantivy; no-op (just here so the
///   caller doesn't need to remember which are built-in).
pub fn register_tokenizers(index: &Index) {
    /// Shared filter chain: length cap → lowercase → ASCII fold.
    fn analyzer<T: Tokenizer>(tokenizer: T) -> TextAnalyzer {
        TextAnalyzer::builder(tokenizer).filter(RemoveLongFilter::limit(MAX_TOKEN_LEN)).filter(LowerCaser).filter(AsciiFoldingFilter).build()
    }
    let tokenizers = index.tokenizers();
    tokenizers.register(NGRAM3_TOKENIZER, analyzer(NgramTokenizer::new(3, 3, false).expect("3-gram bounds are valid")));
    // Re-register the built-in "raw"/"default" chains explicitly so behavior is
    // pinned even if upstream changes them.
    tokenizers.register(RAW_TOKENIZER, TextAnalyzer::builder(RawTokenizer::default()).build());
    tokenizers.register(DEFAULT_TOKENIZER, analyzer(SimpleTokenizer::default()));
}

/// Helper for tests and pushdown rule: which user fields are configured?
pub fn indexed_field_names(table: &TableSchema) -> Vec<String> {
    table.fields.iter().filter(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)).map(|f| f.name.clone()).collect()
}
