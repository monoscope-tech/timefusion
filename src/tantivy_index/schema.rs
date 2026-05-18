//! Build a Tantivy `Schema` from the YAML `TableSchema`.
//!
//! Always emits two reserved fields:
//! - `_timestamp`: i64 microseconds, STORED + FAST (range queries, sort)
//! - `_id`: text raw tokenizer, STORED (returned to caller for prefilter)
//!
//! User fields are honored from `FieldDef.tantivy`. Only fields with
//! `indexed: true` produce a tantivy field. The tokenizer choice maps:
//!   "raw"     → keyword (exact match, single token)
//!   "default" → tantivy default tokenizer (lowercase + simple split)
//! Unknown tokenizers fall back to "default" with a warning.

use crate::schema_loader::{FieldDef, TableSchema, TantivyFieldConfig};
use std::collections::HashMap;
use tantivy::schema::{Field, FieldType, IndexRecordOption, NumericOptions, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED, STORED, TEXT};

pub const TS_FIELD: &str = "_timestamp";
pub const ID_FIELD: &str = "_id";

/// Result of building a tantivy schema for a table.
pub struct BuiltSchema {
    pub schema: Schema,
    pub timestamp: Field,
    pub id: Field,
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
    let id = b.add_text_field(ID_FIELD, raw_text_options(true));

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
    BuiltSchema { schema: b.build(), timestamp, id, user_fields }
}

fn raw_text_options(stored: bool) -> TextOptions {
    let indexing = TextFieldIndexing::default()
        .set_tokenizer("raw")
        .set_index_option(IndexRecordOption::Basic);
    let mut opts = TextOptions::default().set_indexing_options(indexing);
    if stored {
        opts = opts | STORED;
    }
    opts
}

fn text_options_for(cfg: &TantivyFieldConfig) -> TextOptions {
    let tok = cfg.tokenizer.as_deref().unwrap_or("default");
    let mut opts = match tok {
        "raw" => TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::Basic),
        ),
        _ => TEXT.into(),
    };
    if cfg.stored {
        opts = opts | STORED;
    }
    opts
}

/// Helper for tests and pushdown rule: which user fields are configured?
pub fn indexed_field_names(table: &TableSchema) -> Vec<String> {
    table
        .fields
        .iter()
        .filter_map(|f| f.tantivy.as_ref().filter(|t| t.indexed).map(|_| f.name.clone()))
        .collect()
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
