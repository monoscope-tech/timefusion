//! Build a tantivy index from a stream of `RecordBatch`es.
//!
//! Strategy: in-memory `tantivy::Index` (RAMDirectory) — caller is responsible
//! for serializing it to bytes (see `store::pack_index`). Each build is a
//! one-shot single commit; whether its segments are merged before close is the
//! caller's choice via `MergeMode` (see that type for why deferring is safe).
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

use anyhow::{Context, Result, anyhow, bail};
use arrow::{
    array::{Array, ArrayRef, AsArray, ListArray, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;
use tantivy::{Index, IndexWriter, doc, merge_policy::NoMergePolicy, schema::Schema as TSchema};
use tracing::{debug, warn};

use crate::{
    schema_loader::TableSchema,
    tantivy_index::schema::{BuiltSchema, build_for_table},
};

/// Heap reserved per tantivy `IndexWriter`. Surfaced so the
/// `BufferedWriteLayer` can subtract peak in-flight tantivy memory from the
/// MemBuffer budget (`max_memory_bytes`).
pub const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

/// Segment-count safety valve for `MergeMode::Deferred`: past this many
/// segments a build merges inline anyway, because per-query cost is linear in
/// segment count (one term-dictionary seek per segment) and an index whose
/// parquet never gets compacted would otherwise stay pathological forever.
/// Generous on purpose — a normal flush bucket lands well under it, so the
/// valve only fires for outlier-sized buckets.
pub const MAX_DEFERRED_SEGMENTS: usize = 32;

/// When a build is allowed to spend CPU on segment merges.
///
/// Merging is *semantically invisible*: it changes neither the hit set nor any
/// stored/fast-field value, only the number of segment readers a query opens
/// and the packed blob's size. That is what makes deferring it safe.
///
/// It is not cheap though — tantivy schedules merges from
/// `SegmentUpdater::consider_merge_options()`, which runs on **every in-flight
/// segment flush**, not just on commit. Under the default `LogMergePolicy` that
/// puts `TermMerger` work concurrently with `add_document`, inside the ingest
/// window, ×`tantivy_spawn_sem` concurrent builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// Ingest path (post-flush sidecar build, MemBuffer bucket index): install
    /// `NoMergePolicy` and ship whatever segments the per-thread arena flushes
    /// produced. Bounded by `MAX_DEFERRED_SEGMENTS`; collapsed to one segment
    /// later by the post-optimize / backfill rebuilds, which run `Now`.
    Deferred,
    /// Maintenance path (post-optimize reindex, startup backfill, WAL-recovery
    /// reindex): merge everything into a single segment after the commit. These
    /// callers are already off the ingest path, so this is the merge cadence.
    Now,
}

#[derive(Debug, Default, Clone)]
pub struct IndexBuildStats {
    pub rows: u64,
    pub batches: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Segments in the finished index (1 when merged).
    pub segments: usize,
}

/// Build an in-memory tantivy `Index` from `batches`. Returns the index and
/// row-level stats. Caller serializes the index (via `store::pack_index`) to
/// bytes for upload.
pub fn build_in_memory(table: &TableSchema, batches: &[RecordBatch]) -> Result<(Index, BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let index = Index::create_in_ram(built.schema.clone());
    crate::tantivy_index::schema::register_tokenizers(&index);
    // Bucket indexes are a query-time cache rebuilt on every row-count change —
    // merging them is pure waste.
    let stats = index_to_writer(&built, &index, batches, MergeMode::Deferred)?;
    Ok((index, built, stats))
}

/// Append `batches` to an existing tantivy `Index` (created in RAM or on disk).
/// Used by `store::build_to_dir` to write directly to a `MmapDirectory`.
pub fn index_to_writer(built: &BuiltSchema, index: &Index, batches: &[RecordBatch], merge: MergeMode) -> Result<IndexBuildStats> {
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("create tantivy writer")?;
    // Merges are driven explicitly below, never by the writer's own policy: the
    // default `LogMergePolicy` fires from `consider_merge_options()` on every
    // in-flight segment flush, i.e. *while* documents are still being added.
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut stats = IndexBuildStats::default();
    for batch in batches {
        index_batch(built, &mut writer, batch, &mut stats)?;
        stats.batches += 1;
    }
    writer.commit().context("tantivy commit")?;
    let segment_ids = index.searchable_segment_ids().map_err(|e| anyhow!("list segments: {e}"))?;
    stats.segments = segment_ids.len();
    let over_valve = stats.segments > MAX_DEFERRED_SEGMENTS;
    if stats.segments > 1 && (merge == MergeMode::Now || over_valve) {
        if over_valve && merge == MergeMode::Deferred {
            warn!("tantivy build produced {} segments (> {MAX_DEFERRED_SEGMENTS}); merging inline", stats.segments);
        }
        writer.merge(&segment_ids).wait().map_err(|e| anyhow!("merge segments: {e}"))?;
        stats.segments = 1;
        crate::metrics::record_tantivy_merge_executed();
    } else if stats.segments > 1 {
        debug!("tantivy build deferring merge of {} segments", stats.segments);
        crate::metrics::record_tantivy_merge_deferred();
    }
    // Join background merge threads before returning: an explicit `merge()` runs
    // on tantivy's threadpool, and dropping the writer does NOT wait for it.
    // If the caller then tars the index dir (`store::pack_dir`) while a merge is still
    // GC-ing source segments, the archive captures vanished/half-rewritten files —
    // surfacing as non-fatal `tar append` (build) / `tar unpack` (read) failures that
    // silently disable the index. Waiting leaves the dir quiescent.
    writer.wait_merging_threads().context("wait merging threads")?;
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
        // stats.rows counts docs already added → the global ordinal of this
        // one, valid as a parquet row index only for read-back builds.
        let mut doc = doc!(built.timestamp => ts, built.id => id, built.row_ordinal => stats.rows);
        for uc in &user_cols {
            if uc.column.is_null(row) {
                continue;
            }
            if let Some(text) = uc.kind.extract(uc.column, row)?
                && !text.is_empty()
            {
                doc.add_text(uc.field, &text);
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

/// Render one Variant row to text. `kv=false` → canonical JSON (the same
/// `parquet_variant_json` serializer used everywhere: the wire, the coercion
/// path, and `text_match`'s row-eval), so all three agree byte-for-byte.
pub(crate) fn variant_to_text(col: &ArrayRef, row: usize, kv: bool) -> Result<Option<String>> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{StringArray, TimestampMicrosecondArray},
        datatypes::{Field, Schema as ArrowSchema, TimeUnit},
    };
    use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};

    use super::*;
    use crate::{
        schema_loader::{FieldDef, TantivyFieldConfig},
        tantivy_index::reader::{Hit, query_index},
    };

    fn table() -> TableSchema {
        let f = |name: &str, dt: &str, tv: Option<TantivyFieldConfig>| FieldDef {
            name: name.into(),
            data_type: dt.into(),
            nullable: true,
            tantivy: tv,
            dictionary: None,
            bloom_filter: false,
        };
        TableSchema {
            table_name: "logs".into(),
            partitions: vec![],
            sorting_columns: vec![],
            z_order_columns: vec![],
            time_column: None,
            dedup_keys: vec![],
            dedup_tiebreak: None,
            tombstone_column: None,
            version_append: false,
            fields: vec![
                f("timestamp", "Timestamp(Microsecond, Some(\"UTC\"))", None),
                f("id", "Utf8", None),
                f("level", "Utf8", Some(TantivyFieldConfig { indexed: true, tokenizer: Some("raw".into()), flatten: None })),
            ],
        }
    }

    /// One row per batch so each `index_to_writer` call is a separate commit
    /// producing its own (tiny, same-sized) segment — the shape the default
    /// `LogMergePolicy` would collapse once ≥8 pile up in one level.
    fn batch(n: i64) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![n * 1_000]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec![format!("id{n}")])),
                Arc::new(StringArray::from(vec![if n % 2 == 0 { "INFO" } else { "ERROR" }])),
            ],
        )
        .unwrap()
    }

    fn error_hits(index: &Index, built: &BuiltSchema) -> Vec<Hit> {
        let q = TermQuery::new(Term::from_field_text(built.user_fields["level"].field, "ERROR"), IndexRecordOption::Basic);
        let mut hits = query_index(index, &q, None).expect("query");
        hits.sort_by(|a, b| a.timestamp_micros.cmp(&b.timestamp_micros));
        hits
    }

    /// Phase 4: merging must not run on the ingest path. Pins all three
    /// invariants on one index — (i) 12 deferred commits leave 12 segments
    /// (default `LogMergePolicy` merges at ≥8 same-level segments, so an
    /// accidental policy regression fails here), (ii) the explicit merge path
    /// collapses them to one segment, (iii) the hit set is byte-identical
    /// across the merge, which is what makes deferral safe.
    #[test]
    fn deferred_commits_do_not_merge_and_explicit_merge_preserves_hits() {
        let built = build_for_table(&table());
        let index = Index::create_in_ram(built.schema.clone());
        crate::tantivy_index::schema::register_tokenizers(&index);

        for n in 0..12 {
            let stats = index_to_writer(&built, &index, &[batch(n)], MergeMode::Deferred).expect("deferred build");
            assert_eq!(stats.rows, 1);
            assert_eq!(stats.segments as i64, n + 1, "deferred build must add a segment, never merge");
        }
        let unmerged = error_hits(&index, &built);
        assert_eq!(unmerged.len(), 6, "6 odd-numbered rows are ERROR");
        assert_eq!(index.searchable_segment_ids().unwrap().len(), 12);

        // Maintenance cadence: no new documents, merge what's there.
        let stats = index_to_writer(&built, &index, &[], MergeMode::Now).expect("merge build");
        assert_eq!(stats.segments, 1, "explicit merge must collapse segments");
        assert_eq!(index.searchable_segment_ids().unwrap().len(), 1);

        // Merging is semantically invisible: same hits, same ts/id/ordinals.
        assert_eq!(error_hits(&index, &built), unmerged);
    }
}
