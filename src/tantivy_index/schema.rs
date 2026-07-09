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
    schema::{FAST, Field, FieldType, INDEXED, IndexRecordOption, NumericOptions, STORED, Schema, SchemaBuilder, TextFieldIndexing, TextOptions},
    tokenizer::{AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, TextAnalyzer},
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

    let mut user_fields = HashMap::new();
    for fd in &table.fields {
        let Some(cfg) = &fd.tantivy else { continue };
        if !cfg.indexed {
            continue;
        }
        if fd.name == TS_FIELD || fd.name == ID_FIELD {
            continue;
        }
        let opts = text_options_for(cfg);
        let f = b.add_text_field(&fd.name, opts);
        user_fields.insert(fd.name.clone(), UserField { field: f, source: fd.clone() });
    }
    BuiltSchema { schema: b.build(), timestamp, id, row_ordinal, user_fields }
}

fn raw_id_options() -> TextOptions {
    // FAST (raw-normalized) lets the reader pull hit ids from the columnar
    // store instead of per-doc doc-store fetches. STORED is kept so indexes
    // remain readable by the pre-fast-field fallback path (and older readers).
    TextOptions::default()
        .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw").set_index_option(IndexRecordOption::Basic))
        .set_fast(Some("raw"))
        | STORED
}

/// Map a YAML tokenizer name to tantivy `TextOptions`. Unknown names fall
/// through to the default (ngram3) — better-than-nothing rather than panic.
///
/// Default (when YAML omits `tokenizer`): `ngram3`. The vast majority of
/// log/trace text queries use `LIKE '%substr%'` or `ILIKE`, which only
/// the n-gram index can accelerate.
fn text_options_for(cfg: &TantivyFieldConfig) -> TextOptions {
    let tok = cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER);
    let name = match tok {
        RAW_TOKENIZER => RAW_TOKENIZER,
        DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
        // Both "ngram3" and any unknown value default to ngram3 — most
        // useful for substring queries. Document the convention in YAML.
        _ => NGRAM3_TOKENIZER,
    };
    let index_option = if name == RAW_TOKENIZER {
        IndexRecordOption::Basic
    } else {
        // WithFreqsAndPositions is needed for phrase queries (which n-gram
        // matching reduces to: consecutive trigrams of the query string).
        IndexRecordOption::WithFreqsAndPositions
    };
    TextOptions::default().set_indexing_options(TextFieldIndexing::default().set_tokenizer(name).set_index_option(index_option))
}

/// Resolve the tokenizer for a field (defaulting to ngram3). Used by the
/// rewriter to decide which LIKE/ILIKE patterns it can accelerate.
pub fn resolved_tokenizer(table: &TableSchema, name: &str) -> Option<&'static str> {
    let cfg = table.fields.iter().find(|f| f.name == name)?.tantivy.as_ref()?;
    if !cfg.indexed {
        return None;
    }
    Some(match cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER) {
        RAW_TOKENIZER => RAW_TOKENIZER,
        DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
        _ => NGRAM3_TOKENIZER,
    })
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
    let ngram = TextAnalyzer::builder(NgramTokenizer::new(3, 3, false).expect("valid ngram"))
        .filter(RemoveLongFilter::limit(256))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build();
    index.tokenizers().register(NGRAM3_TOKENIZER, ngram);
    // Re-register a known-good "raw" (case-sensitive single token) to make
    // exact-match queries deterministic across tantivy versions.
    let raw = TextAnalyzer::builder(RawTokenizer::default()).build();
    index.tokenizers().register(RAW_TOKENIZER, raw);
    // "default" stays as tantivy's built-in (SimpleTokenizer + LowerCaser),
    // but re-register explicitly so behavior is pinned even if upstream
    // changes the default chain.
    let default = TextAnalyzer::builder(SimpleTokenizer::default()).filter(RemoveLongFilter::limit(256)).filter(LowerCaser).filter(AsciiFoldingFilter).build();
    index.tokenizers().register(DEFAULT_TOKENIZER, default);
}

/// Helper for tests and pushdown rule: which user fields are configured?
pub fn indexed_field_names(table: &TableSchema) -> Vec<String> {
    table.fields.iter().filter_map(|f| f.tantivy.as_ref().filter(|t| t.indexed).map(|_| f.name.clone())).collect()
}

/// Returns the tokenizer name for a field, if it's indexed.
pub fn field_tokenizer<'a>(table: &'a TableSchema, name: &str) -> Option<&'a str> {
    table.fields.iter().find(|f| f.name == name)?.tantivy.as_ref()?.tokenizer.as_deref()
}

#[allow(dead_code)]
fn _force_use(s: &Schema) {
    for (_, fe) in s.fields() {
        let _: &FieldType = fe.field_type();
    }
}
