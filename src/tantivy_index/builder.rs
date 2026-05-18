//! Build a tantivy index from a stream of `RecordBatch`es.
//!
//! Strategy: in-memory `tantivy::Index` (RAMDirectory) — caller is responsible
//! for serializing it to bytes (see `store::pack_index`). Index is wrapped in
//! a single segment per batch group; segments are merged before close to keep
//! the on-disk footprint small.
//!
//! Field mapping (from `schema.rs`):
//! - `_timestamp` ← row's `timestamp` column (Timestamp microseconds)
//! - `_id`        ← row's `id` column (Utf8/Utf8View)
//! - User fields  ← columns marked `tantivy: { indexed: true }` in YAML
//!
//! Variant handling: convert via `parquet_variant_compute::VariantArray` and
//! flatten to text. `flatten: "json"` writes the JSON string; `flatten: "kv"`
//! writes "k1:v1 k2:v2 …" tokens (key+value flattened). Nested objects are
//! traversed recursively.

use crate::schema_loader::TableSchema;
use crate::tantivy_index::schema::{BuiltSchema, build_for_table};
use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{Array, ArrayRef, AsArray, ListArray, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;
use tantivy::{Index, IndexWriter, doc, schema::Schema as TSchema};

/// Heap reserved per tantivy `IndexWriter`. Surfaced so the
/// `BufferedWriteLayer` can subtract peak in-flight tantivy memory from the
/// MemBuffer budget (`max_memory_bytes`).
pub const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Default, Clone)]
pub struct IndexBuildStats {
    pub rows: u64,
    pub batches: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
}

/// Build an in-memory tantivy `Index` from `batches`. Returns the index and
/// row-level stats. Caller serializes the index (via `store::pack_index`) to
/// bytes for upload.
pub fn build_in_memory(table: &TableSchema, batches: &[RecordBatch]) -> Result<(Index, BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let index = Index::create_in_ram(built.schema.clone());
    let stats = index_to_writer(&built, &index, batches)?;
    Ok((index, built, stats))
}

/// Append `batches` to an existing tantivy `Index` (created in RAM or on disk).
/// Used by `store::build_to_dir` to write directly to a `MmapDirectory`.
pub fn index_to_writer(built: &BuiltSchema, index: &Index, batches: &[RecordBatch]) -> Result<IndexBuildStats> {
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("create tantivy writer")?;
    let mut stats = IndexBuildStats::default();
    for batch in batches {
        index_batch(built, &mut writer, batch, &mut stats)?;
        stats.batches += 1;
    }
    writer.commit().context("tantivy commit")?;
    Ok(stats)
}

fn index_batch(built: &BuiltSchema, writer: &mut IndexWriter, batch: &RecordBatch, stats: &mut IndexBuildStats) -> Result<()> {
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp").map_err(|e| anyhow!("missing timestamp column: {e}"))?;
    let id_idx = schema.index_of("id").map_err(|e| anyhow!("missing id column: {e}"))?;

    let ts_col = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| anyhow!("timestamp column is not TimestampMicrosecondArray (got {:?})", batch.column(ts_idx).data_type()))?;
    let id_extract = string_extractor(batch.column(id_idx))?;

    // Pre-resolve user-field columns once per batch.
    struct UserCol<'a> {
        field: tantivy::schema::Field,
        column: &'a ArrayRef,
        kind: ColKind,
    }
    let mut user_cols: Vec<UserCol> = Vec::new();
    for (name, uf) in &built.user_fields {
        let Ok(idx) = schema.index_of(name) else { continue };
        let kind = ColKind::detect(batch.column(idx).data_type(), uf.source.tantivy.as_ref().and_then(|t| t.flatten.as_deref()))?;
        user_cols.push(UserCol { field: uf.field, column: batch.column(idx), kind });
    }

    for row in 0..batch.num_rows() {
        let ts = ts_col.value(row);
        stats.min_timestamp_micros = Some(stats.min_timestamp_micros.map_or(ts, |m| m.min(ts)));
        stats.max_timestamp_micros = Some(stats.max_timestamp_micros.map_or(ts, |m| m.max(ts)));
        let id = id_extract(row).unwrap_or_default();
        let mut doc = doc!(built.timestamp => ts, built.id => id);
        for uc in &user_cols {
            if uc.column.is_null(row) {
                continue;
            }
            if let Some(text) = uc.kind.extract(uc.column, row)? {
                if !text.is_empty() {
                    doc.add_text(uc.field, &text);
                }
            }
        }
        writer.add_document(doc).context("add_document")?;
        stats.rows += 1;
    }
    Ok(())
}

enum ColKind {
    Utf8,
    Utf8View,
    ListUtf8,
    VariantJson,
    VariantKv,
}

impl ColKind {
    fn detect(dt: &DataType, flatten: Option<&str>) -> Result<Self> {
        Ok(match dt {
            DataType::Utf8 => Self::Utf8,
            DataType::Utf8View => Self::Utf8View,
            DataType::List(_) => Self::ListUtf8,
            DataType::Struct(_) => match flatten.unwrap_or("json") {
                "kv" => Self::VariantKv,
                _ => Self::VariantJson,
            },
            other => bail!("unsupported tantivy source column type {other:?}"),
        })
    }

    fn extract(&self, col: &ArrayRef, row: usize) -> Result<Option<String>> {
        Ok(match self {
            Self::Utf8 => col.as_any().downcast_ref::<StringArray>().map(|a| a.value(row).to_string()),
            Self::Utf8View => col.as_any().downcast_ref::<StringViewArray>().map(|a| a.value(row).to_string()),
            Self::ListUtf8 => list_to_text(col.as_any().downcast_ref::<ListArray>().context("list cast")?, row)?,
            Self::VariantJson => variant_to_text(col, row, false)?,
            Self::VariantKv => variant_to_text(col, row, true)?,
        })
    }
}

fn string_extractor(col: &ArrayRef) -> Result<Box<dyn Fn(usize) -> Option<String> + '_>> {
    Ok(match col.data_type() {
        DataType::Utf8 => {
            let a = col.as_string::<i32>();
            Box::new(move |i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) })
        }
        DataType::Utf8View => {
            let a = col.as_string_view();
            Box::new(move |i| if a.is_null(i) { None } else { Some(a.value(i).to_string()) })
        }
        other => bail!("id column must be Utf8/Utf8View, got {other:?}"),
    })
}

fn list_to_text(arr: &ListArray, row: usize) -> Result<Option<String>> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let inner = arr.value(row);
    let mut parts: Vec<String> = Vec::new();
    if let Some(s) = inner.as_any().downcast_ref::<StringArray>() {
        for i in 0..s.len() {
            if !s.is_null(i) {
                parts.push(s.value(i).to_string());
            }
        }
    } else if let Some(s) = inner.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..s.len() {
            if !s.is_null(i) {
                parts.push(s.value(i).to_string());
            }
        }
    } else {
        bail!("list element type unsupported for tantivy: {:?}", inner.data_type());
    }
    Ok(Some(parts.join(" ")))
}

fn variant_to_text(col: &ArrayRef, row: usize, kv: bool) -> Result<Option<String>> {
    let struct_arr = col.as_any().downcast_ref::<StructArray>().context("variant should be StructArray")?;
    if struct_arr.is_null(row) {
        return Ok(None);
    }
    let variant_arr = VariantArray::try_new(struct_arr).map_err(|e| anyhow!("VariantArray::try_new: {e}"))?;
    if variant_arr.is_null(row) {
        return Ok(None);
    }
    let json = variant_arr.value(row).to_json_string().map_err(|e| anyhow!("variant→json: {e}"))?;
    if !kv {
        return Ok(Some(json));
    }
    // kv flatten: parse JSON, walk to leaves, emit "path:value path:value …".
    let v: serde_json::Value = serde_json::from_str(&json).map_err(|e| anyhow!("kv json parse: {e}"))?;
    let mut buf = String::with_capacity(json.len());
    flatten_kv(&v, "", &mut buf);
    Ok(Some(buf))
}

fn flatten_kv(v: &serde_json::Value, prefix: &str, out: &mut String) {
    use serde_json::Value::*;
    match v {
        Object(map) => {
            for (k, val) in map {
                let next = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                flatten_kv(val, &next, out);
            }
        }
        Array(items) => {
            for item in items {
                flatten_kv(item, prefix, out);
            }
        }
        Null => {}
        other => {
            if !out.is_empty() {
                out.push(' ');
            }
            if !prefix.is_empty() {
                out.push_str(prefix);
                out.push(':');
            }
            match other {
                String(s) => out.push_str(s),
                _ => out.push_str(&other.to_string()),
            }
        }
    }
}

/// Returns the schema attached to a tantivy index (helper for tests).
pub fn index_schema(index: &Index) -> TSchema {
    index.schema()
}
