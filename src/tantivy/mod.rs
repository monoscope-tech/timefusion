//! Per-parquet-file Tantivy index: parallel sidecar indexes that pre-filter
//! `(timestamp, id)` candidates so Delta/MemBuffer scans stay narrow.
//!
//! Layout: one tantivy index per Delta parquet file, scoped per `project_id`.
//! Schema is derived from the YAML `TableSchema` via `schema::build_for_table`.
//! Indexes always store `_timestamp` (i64, fast) and `_id` (text raw); user
//! columns are indexed-only unless explicitly marked `stored: true`.

pub mod search;
pub mod udf;

pub use search::{Hit, query_index};

// Build a tantivy index from a stream of `RecordBatch`es.
//
// Strategy: in-memory `tantivy::Index` (RAMDirectory) — caller is responsible
// for serializing it to bytes (see `store::pack_index`). Each build is a
// one-shot single commit; whether its segments are merged before close is the
// caller's choice via `MergeMode` (see that type for why deferring is safe).
//
// Field mapping (from `schema.rs`):
// - `_timestamp` ← row's `timestamp` column (Timestamp microseconds)
// - `_id`        ← row's `id` column (Utf8/Utf8View)
// - User fields  ← columns marked `tantivy: { indexed: true }` in YAML
//
// Variant handling: convert via `parquet_variant_compute::VariantArray` and
// flatten to text. `flatten: "json"` writes the JSON string; `flatten: "kv"`
// writes "k1:v1 k2:v2 …" tokens (key+value flattened). Nested objects are
// traversed recursively.

use anyhow::{Context, Result, anyhow, bail};
use arrow::{
    array::{Array, ArrayRef, ListArray, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;
use tantivy::{Index, IndexWriter, doc, merge_policy::NoMergePolicy};
use tracing::{debug, warn};

use crate::schema::TableSchema;

/// Heap reserved per writer and charged against the MemBuffer budget.
pub const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

/// Deferred builds merge past this cap to bound per-query segment cost.
pub const MAX_DEFERRED_SEGMENTS: usize = 32;

/// When a build is allowed to spend CPU on segment merges.
///
/// Merging is logically invisible but expensive during ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// Defers ingest-path merges up to [`MAX_DEFERRED_SEGMENTS`].
    Deferred,
    /// Merges maintenance-path indexes after commit.
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
    crate::tantivy::register_tokenizers(&index);
    let stats = index_to_writer(&built, &index, batches, MergeMode::Deferred)?;
    Ok((index, built, stats))
}

/// Append `batches` to an existing tantivy `Index` (created in RAM or on disk).
/// Used by `store::build_to_dir` to write directly to a `MmapDirectory`.
pub fn index_to_writer(built: &BuiltSchema, index: &Index, batches: &[RecordBatch], merge: MergeMode) -> Result<IndexBuildStats> {
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("create tantivy writer")?;
    // Explicit merges keep `TermMerger` off the ingest path.
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut stats = IndexBuildStats::default();
    batches.iter().try_for_each(|batch| index_batch(built, &mut writer, batch, &mut stats))?;
    stats.batches = batches.len() as u32;
    finish_writer(index, writer, stats, merge)
}

/// Build a committed-file index from a bounded channel of decoded parquet
/// batches. The reader and Tantivy writer run concurrently, so only the
/// channel's small window remains live; the old committed-file path collected
/// the entire wide parquet into Arrow before indexing and a sub-512 MiB file
/// OOM-killed a 12 GiB repair cgroup in production.
///
/// Must run on a blocking thread: `IndexWriter` is CPU/blocking work and
/// `blocking_recv` intentionally keeps it off Tokio's async workers.
pub fn build_stream_to_dir(
    table: &TableSchema, dir: &std::path::Path, mut batches: tokio::sync::mpsc::Receiver<RecordBatch>, merge: MergeMode,
) -> Result<(BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let mmap_dir = tantivy::directory::MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::create(mmap_dir, built.schema.clone(), Default::default()).map_err(|e| anyhow!("create disk index: {e}"))?;
    crate::tantivy::register_tokenizers(&index);
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("create tantivy writer")?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut stats = IndexBuildStats::default();
    while let Some(batch) = batches.blocking_recv() {
        index_batch(&built, &mut writer, &batch, &mut stats)?;
        stats.batches = stats.batches.saturating_add(1);
    }
    let stats = finish_writer(&index, writer, stats, merge)?;
    Ok((built, stats))
}

fn finish_writer(index: &Index, mut writer: IndexWriter, mut stats: IndexBuildStats, merge: MergeMode) -> Result<IndexBuildStats> {
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
        crate::observability::record_tantivy_merge_executed();
    } else if stats.segments > 1 {
        debug!("tantivy build deferring merge of {} segments", stats.segments);
        crate::observability::record_tantivy_merge_deferred();
    }
    // Packing requires all background merge file mutations to finish.
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
    let id_col = batch.column(id_idx);
    let id_kind = match id_col.data_type() {
        DataType::Utf8 => ColKind::Utf8,
        DataType::Utf8View => ColKind::Utf8View,
        other => bail!("id column must be Utf8/Utf8View, got {other:?}"),
    };

    // Pre-resolve user-field columns once per batch.
    struct UserCol<'a> {
        field: tantivy::schema::Field,
        column: &'a ArrayRef,
        kind: ColKind,
    }
    let user_cols: Vec<UserCol> = built
        .user_fields
        .iter()
        .filter_map(|(name, uf)| schema.index_of(name).ok().map(|idx| (batch.column(idx), uf)))
        .map(|(column, uf)| {
            Ok(UserCol { field: uf.field, column, kind: ColKind::detect(column.data_type(), uf.source.tantivy.as_ref().and_then(|t| t.flatten.as_deref()))? })
        })
        .collect::<Result<_>>()?;

    for row in 0..batch.num_rows() {
        let ts = ts_col.value(row);
        stats.min_timestamp_micros = Some(stats.min_timestamp_micros.map_or(ts, |m| m.min(ts)));
        stats.max_timestamp_micros = Some(stats.max_timestamp_micros.map_or(ts, |m| m.max(ts)));
        let id = id_kind.extract(id_col, row)?.unwrap_or_default();
        // stats.rows counts docs already added → the global ordinal of this
        // one, valid as a parquet row index only for read-back builds.
        let mut doc = doc!(built.timestamp => ts, built.id => id, built.row_ordinal => stats.rows);
        for uc in &user_cols {
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
        if col.is_null(row) {
            return Ok(None);
        }
        Ok(match self {
            Self::Utf8 => Some(col.as_any().downcast_ref::<StringArray>().context("utf8 cast")?.value(row).to_string()),
            Self::Utf8View => Some(col.as_any().downcast_ref::<StringViewArray>().context("utf8view cast")?.value(row).to_string()),
            Self::ListUtf8 => Some(list_to_text(col.as_any().downcast_ref::<ListArray>().context("list cast")?, row)?),
            Self::VariantJson => variant_to_text(col, row, false)?,
            Self::VariantKv => variant_to_text(col, row, true)?,
        })
    }
}

fn list_to_text(arr: &ListArray, row: usize) -> Result<String> {
    let inner = arr.value(row);
    let parts: Vec<&str> = if let Some(s) = inner.as_any().downcast_ref::<StringArray>() {
        s.iter().flatten().collect()
    } else if let Some(s) = inner.as_any().downcast_ref::<StringViewArray>() {
        s.iter().flatten().collect()
    } else {
        bail!("list element type unsupported for tantivy: {:?}", inner.data_type())
    };
    Ok(parts.join(" "))
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

#[cfg(test)]
mod builder_tests {
    use std::sync::Arc;

    use arrow::{
        array::{StringArray, TimestampMicrosecondArray},
        datatypes::{Field, Schema as ArrowSchema, TimeUnit},
    };
    use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};

    use super::*;
    use crate::{
        schema::{FieldDef, TantivyFieldConfig},
        tantivy::search::{Hit, query_index},
    };

    fn table() -> TableSchema {
        let f = |name: &str, dt: &str, tv: Option<TantivyFieldConfig>| FieldDef {
            name: name.into(),
            data_type: dt.into(),
            nullable: true,
            tantivy: tv,
            dictionary: None,
            bloom_filter: false,
            mutable: false,
        };
        TableSchema {
            rollups: vec![],
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
        crate::tantivy::register_tokenizers(&index);

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

    #[test]
    fn committed_file_build_consumes_a_bounded_batch_stream() {
        assert_eq!(crate::tantivy::PARQUET_INDEX_BATCH_WINDOW, 2);
        let tmp = tempfile::tempdir().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(crate::tantivy::PARQUET_INDEX_BATCH_WINDOW);
        std::thread::scope(|scope| {
            let dir = tmp.path();
            let table = table();
            let build = scope.spawn(move || build_stream_to_dir(&table, dir, rx, MergeMode::Now).unwrap());
            for n in 0..3 {
                tx.blocking_send(batch(n)).unwrap();
            }
            drop(tx);
            let (built, stats) = build.join().unwrap();
            assert_eq!(stats.batches, 3);
            assert_eq!(stats.rows, 3);
            assert_eq!(stats.segments, 1);
            let index = crate::tantivy::open_index(tmp.path()).unwrap();
            assert_eq!(error_hits(&index, &built).len(), 1);
        });
    }
}

// ===== schema =====
// Build a Tantivy `Schema` from the YAML `TableSchema`.
//
// Always emits two reserved fields:
// - `_timestamp`: i64 microseconds, STORED + FAST (range queries, sort)
// - `_id`: text raw tokenizer, STORED (returned to caller for prefilter)
//
// User fields are honored from `FieldDef.tantivy`. Only fields with
// `indexed: true` produce a tantivy field. Tokenizer choice:
//   "raw"     → keyword (exact match, single token; case-sensitive)
//   "default" → tantivy default tokenizer (lowercase + word split)
//   "ngram3"  → lowercased 3-grams; supports `LIKE '%substr%'`, `'%suffix'`,
//               and `ILIKE 'word'`. Larger postings than word tokenizer
//               but the trigram dictionary is bounded (~10k entries for
//               ASCII), so net index size is typically 1.5–2× vs default.
//
// **Default (no tokenizer specified)**: `ngram3` — substring search is the
// dominant pattern for logs/traces. Opt-down to `raw`/`default` for
// point-lookup-only columns (IDs, enums).

use std::collections::HashMap;

use tantivy::{
    schema::{FAST, Field, INDEXED, IndexRecordOption, NumericOptions, STORED, Schema, SchemaBuilder, TextFieldIndexing, TextOptions},
    tokenizer::{AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Tokenizer},
};

use crate::schema::{FieldDef, TantivyFieldConfig};

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

// ===== manifest =====
// Per-(table, project_id) manifest mapping parquet file URI → tantivy
// index blob URI. Tracks build status so the read-side can fall back to a
// full scan when an index is missing or marked failed.
//
// Manifest is JSON, persisted to object storage via temp+rename. We use
// `ObjectStore::put` (PUT-overwrite) — collisions are resolved by a coarse
// in-process lock (DashMap entry per (table, project_id)) plus an etag
// check on read. Good enough for low-frequency manifest writes; if multiple
// writers race, last-writer-wins (entries are idempotent upserts).

use std::{collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use serde::{Deserialize, Serialize};

use crate::write::mem_buffer::TableKey;

pub const MANIFEST_PREFIX: &str = "index_manifests";
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, educe::Educe)]
#[educe(Default)]
pub struct Manifest {
    #[educe(Default = SCHEMA_VERSION)]
    pub version: u32,
    pub entries: BTreeMap<String, ManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Object-store path to the index tar.zst, or `None` if build failed.
    pub index: Option<String>,
    pub rows: u64,
    pub built_at: DateTime<Utc>,
    pub schema_version: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Set when build failed; `index` will be None.
    pub error: Option<String>,
    /// Parquet file URIs that this index covers. Populated from the Delta
    /// write commit's add-actions. Used by `gc_after_compaction` to detect
    /// stale entries: when any of these URIs is no longer live (i.e. it was
    /// compacted away), the entry no longer authoritatively covers its rows
    /// and can be dropped. Older entries built before this field existed
    /// will deserialize to an empty Vec.
    #[serde(default)]
    pub covered_files: Vec<String>,
    /// True when the index's `_row_ordinal` fast field equals parquet row
    /// order — i.e. the index was built by reading the committed file back
    /// (compaction reindex / backfill). Flush-path indexes see batches
    /// BEFORE the writer's sort, so their ordinals must not drive row
    /// selection. Old entries deserialize to false.
    #[serde(default)]
    pub ordinals_valid: bool,
}

/// Object-store path of the manifest for a given table/project.
pub fn manifest_path(table: &str, project_id: &str) -> ObjPath {
    ObjPath::from(format!("{MANIFEST_PREFIX}/{table}/{project_id}/manifest.json"))
}

/// Project ids that have a manifest under this table's prefix — the GC's
/// authoritative iteration set. Manifests are keyed by the project uuid taken
/// from the parquet URI at build time, so a fixed "default"+custom-projects
/// list never visits unified tenants' manifests and their entries outlive
/// every compaction.
pub async fn list_manifest_projects(store: &dyn ObjectStore, table: &str) -> Result<Vec<String>> {
    let prefix = ObjPath::from(format!("{MANIFEST_PREFIX}/{table}"));
    let listing = store.list_with_delimiter(Some(&prefix)).await.context("list manifest prefixes")?;
    Ok(listing.common_prefixes.iter().filter_map(|p| p.parts().next_back().map(|s| s.as_ref().to_string())).collect())
}

pub async fn load_manifest(store: &dyn ObjectStore, table: &str, project_id: &str) -> Result<Manifest> {
    match store.get(&manifest_path(table, project_id)).await {
        Ok(result) => serde_json::from_slice(&result.bytes().await.context("read manifest bytes")?).context("parse manifest json"),
        Err(object_store::Error::NotFound { .. }) => Ok(Manifest::default()),
        Err(e) => Err(e).context("load manifest"),
    }
}

pub async fn save_manifest(store: &dyn ObjectStore, table: &str, project_id: &str, manifest: &Manifest) -> Result<()> {
    let body = serde_json::to_vec_pretty(manifest).context("serialize manifest")?;
    store.put(&manifest_path(table, project_id), body.into()).await.context("put manifest").map(drop)
}

type ManifestLocks = dashmap::DashMap<TableKey, Arc<tokio::sync::Mutex<()>>>;

/// Load the manifest, apply `f`, and save it back. The shared load/save
/// skeleton behind `upsert` and `remove_many`. Serialized per
/// (table, project_id) — concurrent bucket flushes upserting the same
/// manifest would otherwise interleave load/save and drop each other's
/// entries (last-writer-wins), silently un-covering files and disabling
/// the prefilter via the coverage gate.
/// `f` returns whatever the caller needs out of the mutation (GC needs the
/// entries it removed, so it can delete their blobs) plus whether the manifest
/// actually changed — a no-op mutation must not rewrite the object.
pub async fn mutate<R, F: FnOnce(&mut Manifest) -> (R, bool)>(store: &dyn ObjectStore, table: &str, project_id: &str, f: F) -> Result<R> {
    static LOCKS: std::sync::OnceLock<ManifestLocks> = std::sync::OnceLock::new();
    let lock = LOCKS.get_or_init(Default::default).entry((table.into(), project_id.into())).or_default().clone();
    let _guard = lock.lock().await;
    let mut m = load_manifest(store, table, project_id).await?;
    let (out, dirty) = f(&mut m);
    if dirty {
        save_manifest(store, table, project_id, &m).await?;
    }
    Ok(out)
}

/// Idempotent upsert: load, mutate, save.
impl ManifestEntry {
    /// Entry recorded when the index build itself failed: no index, no rows,
    /// but the covered files are still tracked so GC can reap it later.
    pub fn failed(error: String, covered_files: Vec<String>) -> Self {
        Self {
            index: None,
            rows: 0,
            built_at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error: Some(error),
            covered_files,
            ordinals_valid: false,
        }
    }
}

pub async fn upsert_manifest(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_key: &str, entry: ManifestEntry) -> Result<()> {
    mutate(store, table, project_id, |m| {
        m.entries.insert(parquet_key.to_string(), entry);
        ((), true)
    })
    .await
}

/// Upsert many entries under ONE load+save of the manifest.
///
/// `upsert_manifest` costs a full read-modify-write of the whole manifest —
/// 745 KB and 950 entries for the busiest project — under a per-(table,project)
/// lock, so N builds for one project pay that N times and serialize on it. That
/// is the backfill's throughput ceiling, not the indexing: measured 2026-08-22
/// at ~60 builds/hr against ~85/hr accrual, i.e. coverage that cannot converge.
/// Batching makes a 150-file pass cost roughly one manifest write per project.
pub async fn upsert_manifest_many(store: &dyn ObjectStore, table: &str, project_id: &str, entries: Vec<(String, ManifestEntry)>) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    mutate(store, table, project_id, |m| {
        for (key, entry) in entries {
            m.entries.insert(key, entry);
        }
        ((), true)
    })
    .await
}

/// Remove entries by parquet key (used during compaction GC).
pub async fn remove_manifest_entries(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_keys: &[String]) -> Result<()> {
    if parquet_keys.is_empty() {
        return Ok(());
    }
    mutate(store, table, project_id, |m| {
        let removed = parquet_keys.iter().filter(|k| m.entries.remove(*k).is_some()).count();
        ((), removed > 0)
    })
    .await
}

// ===== store =====
// Pack/unpack tantivy indexes for object-store transport.
//
// Cold form: a single `tar.zst` blob per parquet file.
// Warm form: an extracted directory (used to mmap-open via tantivy::Index).
//
// Path conventions (rooted under whatever prefix the caller chose):
//   indexes/{table}/v1/{project_id}/{file_uuid}.tantivy.tar.zst
//
// `pack_index` serializes the in-memory `Index` to bytes; `unpack_to_dir`
// is the inverse. Upload/download are thin wrappers around `ObjectStore`.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use tantivy::directory::MmapDirectory;

pub const INDEX_PREFIX: &str = "indexes";
pub const INDEX_VERSION: &str = "v1";
pub const BLOB_SUFFIX: &str = ".tantivy.tar.zst";
/// Decoded Arrow batches allowed between the parquet reader and Tantivy
/// writer. Backpressure at two bounds source-row memory independent of file
/// size while keeping decode and indexing overlapped.
pub const PARQUET_INDEX_BATCH_WINDOW: usize = 2;

/// Object-store path for a given parquet file's index blob.
pub fn blob_path(table: &str, project_id: &str, file_uuid: &str) -> ObjPath {
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{project_id}/{file_uuid}{BLOB_SUFFIX}"))
}

/// Partition-mirrored index blob path derived from a parquet file's path
/// relative to its Delta table root, e.g.
///   project_id=<uuid>/date=<d>/part-<id>-c000.zstd.parquet
/// → indexes/{table}/v1/project_id=<uuid>/date=<d>/part-<id>-c000.zstd.tantivy.tar.zst
///
/// A pure suffix swap under the version prefix, so the mapping is 1:1 with the
/// parquet tree and reversible (`index_to_parquet_rel` is the inverse):
/// "does every live parquet have an index?" / "are there orphan blobs?" reduce
/// to a list + diff against the Delta add-file set.
pub fn index_path_for_parquet(table: &str, parquet_rel: &str) -> ObjPath {
    let stem = parquet_rel.strip_suffix(".parquet").unwrap_or(parquet_rel);
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{stem}{BLOB_SUFFIX}"))
}

/// Inverse of `index_path_for_parquet`: recover the table-relative parquet
/// path from an index blob path, or `None` if it isn't a partition-mirrored
/// blob for `table`. Used by reconcile to detect orphan blobs (no live parquet).
pub fn index_to_parquet_rel(table: &str, blob_path: &str) -> Option<String> {
    let prefix = format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/");
    let stem = blob_path.strip_prefix(&prefix)?.strip_suffix(BLOB_SUFFIX)?;
    Some(format!("{stem}.parquet"))
}

/// Stream one committed parquet through a bounded two-batch channel into the
/// on-disk Tantivy writer, then pack and verify the completed index. This is the
/// memory-bounded counterpart to [`build_and_pack`], retained for flush-time
/// in-memory batches.
pub async fn build_parquet_and_pack(
    store: Arc<dyn ObjectStore>, parquet_rel: &str, table: &'static TableSchema, level: i32, merge: MergeMode,
) -> Result<(Bytes, IndexBuildStats)> {
    use deltalake::datafusion::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
    use futures::TryStreamExt;

    let path = ObjPath::from(parquet_rel);
    let meta = store.head(&path).await.with_context(|| format!("head {parquet_rel}"))?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let mut stream = ParquetRecordBatchStreamBuilder::new(reader).await.context("parquet stream builder")?.build().context("build parquet stream")?;
    let tmp = tempfile::tempdir().context("build_parquet_and_pack: tempdir")?;
    let dir = tmp.path().to_owned();
    let (tx, rx) = tokio::sync::mpsc::channel(PARQUET_INDEX_BATCH_WINDOW);
    let build = tokio::task::spawn_blocking(move || crate::tantivy::build_stream_to_dir(table, &dir, rx, merge));

    let decode = async {
        while let Some(batch) = stream.try_next().await.context("decode parquet batch")? {
            tx.send(batch).await.map_err(|_| anyhow!("tantivy streaming writer stopped before parquet decode completed"))?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(tx);
    let built = build.await.context("join streaming tantivy build")?;
    decode?;
    let (_built, stats) = built?;
    tokio::task::spawn_blocking(move || {
        let blob = pack_dir(tmp.path(), level)?;
        verify_blob(&blob).context("verify packed blob")?;
        Ok::<_, anyhow::Error>((blob, stats))
    })
    .await
    .context("join tantivy pack")?
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot, then
/// pack it into a `tar.zst` blob. Avoids any RAM→disk copy.
pub fn build_and_pack(table: &TableSchema, batches: &[RecordBatch], level: i32, merge: MergeMode) -> Result<(Bytes, IndexBuildStats)> {
    let tmp = tempfile::tempdir().context("build_and_pack: tempdir")?;
    let (_built, stats) = build_to_dir(table, batches, tmp.path(), merge)?;
    Ok((pack_dir(tmp.path(), level)?, stats))
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot.
pub fn build_to_dir(table: &TableSchema, batches: &[RecordBatch], dir: &Path, merge: MergeMode) -> Result<(BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let mmap_dir = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::create(mmap_dir, built.schema.clone(), Default::default()).map_err(|e| anyhow!("create disk index: {e}"))?;
    register_tokenizers(&index);
    let stats = index_to_writer(&built, &index, batches, merge)?;
    Ok((built, stats))
}

/// Tar+zstd a directory into a Bytes buffer.
pub fn pack_dir(dir: &Path, level: i32) -> Result<Bytes> {
    let tar_buf = {
        let mut tar = tar::Builder::new(Vec::new());
        tar.append_dir_all(".", dir).context("tar append")?;
        tar.into_inner().context("tar finish")?
    };
    zstd::encode_all(&tar_buf[..], level).map(Bytes::from).context("zstd encode")
}

/// Unpack a tar.zst blob into a fresh directory under `dest`.
pub fn unpack_to_dir(blob: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest).context("mkdir dest")?;
    let tar_bytes = zstd::decode_all(blob).context("zstd decode")?;
    tar::Archive::new(&tar_bytes[..]).unpack(dest).context("tar unpack")
}

/// Round-trip a freshly packed blob (unpack + open) before publishing it, so a
/// structurally-corrupt archive is never uploaded. Blob paths are immutable and
/// reader-cached, so a poison blob would otherwise fail every future read until
/// a manual reindex.
pub fn verify_blob(blob: &[u8]) -> Result<()> {
    let tmp = tempfile::tempdir().context("verify: tempdir")?;
    unpack_to_dir(blob, tmp.path())?;
    open_index(tmp.path()).map(drop)
}

/// Open an unpacked tantivy index for querying.
pub fn open_index(dir: &Path) -> Result<Index> {
    let mm = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::open(mm).map_err(|e| anyhow!("open index: {e}"))?;
    // Tokenizer registry is per-Index, not persisted, so the reader must
    // re-register exactly the same chains the writer used. Mismatch ⇒ silent
    // miss (tantivy looks up by name and falls back to default).
    register_tokenizers(&index);
    Ok(index)
}

pub async fn upload(store: &dyn ObjectStore, path: &ObjPath, blob: Bytes) -> Result<()> {
    store.put(path, blob.into()).await.with_context(|| format!("upload {path}")).map(drop)
}

pub async fn download(store: &dyn ObjectStore, path: &ObjPath) -> Result<Bytes> {
    let result = store.get(path).await.with_context(|| format!("get {path}"))?;
    result.bytes().await.with_context(|| format!("read {path}"))
}

pub async fn delete(store: &dyn ObjectStore, path: &ObjPath) -> Result<()> {
    store.delete(path).await.with_context(|| format!("delete {path}")).map(drop)
}

/// Local cache directory for a (project_id, table, file_uuid).
pub fn local_cache_path(root: &Path, table: &str, project_id: &str, file_uuid: &str) -> PathBuf {
    root.join("tantivy_cache").join(table).join(project_id).join(file_uuid)
}

#[cfg(test)]
mod store_tests {
    use super::*;

    #[test]
    fn parquet_index_path_is_partition_mirrored_and_reversible() {
        let table = "otel_logs_and_spans";
        let rel = "project_id=abc-123/date=2026-06-30/part-00000-deadbeef-c000.zstd.parquet";
        let blob = index_path_for_parquet(table, rel).to_string();
        assert_eq!(blob, "indexes/otel_logs_and_spans/v1/project_id=abc-123/date=2026-06-30/part-00000-deadbeef-c000.zstd.tantivy.tar.zst");
        // inverse recovers the exact parquet rel path
        assert_eq!(index_to_parquet_rel(table, &blob).as_deref(), Some(rel));
        // a blob for a different table / a non-blob path is not ours
        assert_eq!(index_to_parquet_rel("other_table", &blob), None);
        assert_eq!(index_to_parquet_rel(table, "indexes/otel_logs_and_spans/v1/foo.txt"), None);
    }
}

// ===== mem_index =====
// In-memory tantivy index for a single MemBuffer bucket.
//
// Each `TimeBucket` of a tantivy-eligible table holds an `Option<BucketTextIndex>`
// that's built on first text-match query and re-used until the bucket's
// row count grows (cheap monotonic check; no per-insert lock contention).
// Indexes are dropped when the bucket drains or is evicted — they're a
// pure query cache, never the authoritative source.
//
// Memory profile: each index holds `~2× indexed text size` in postings —
// ~200MB per active bucket at 10 minutes of moderate log ingest. Acceptable
// while ≤ flush_interval buckets are active; past that window the post-flush
// callback takes over and these in-memory copies are released.

use crate::tantivy::{
    search::{PredsQuery, build_node_query},
    udf::PredNode,
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
        let (index, built_schema, _stats) = build_in_memory(table, batches).with_context(|| format!("build mem-index for {}", table.table_name))?;
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
