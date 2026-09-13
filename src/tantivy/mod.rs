//! Per-parquet-file Tantivy index: sidecar indexes that pre-filter
//! `(timestamp, id)` candidates so Delta/MemBuffer scans stay narrow.
//!
//! One index per Delta parquet file, scoped per `project_id`; schema derived
//! from the YAML `TableSchema`. Indexes always store `_timestamp` and `_id`.

pub mod histogram;
pub(crate) mod planner;
pub mod search;
pub mod udf;
pub mod visibility;

pub use search::{Hit, query_index};

// Field mapping: `_timestamp` ← `timestamp` column (micros), `_id` ← `id`
// column (Utf8/Utf8View), user fields ← columns with `tantivy.indexed: true`.
// Variant columns flatten to text: `flatten: "json"` writes the JSON string,
// `flatten: "kv"` writes "k1:v1 k2:v2 …" tokens, recursing into nested objects.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};
use arrow::{
    array::{Array, ArrayRef, ListArray, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use itertools::{Either, Itertools};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;
use serde::{Deserialize, Serialize};
use tantivy::{
    Index, IndexWriter,
    directory::MmapDirectory,
    doc,
    merge_policy::NoMergePolicy,
    schema::{FAST, Field, INDEXED, IndexRecordOption, NumericOptions, STORED, Schema, SchemaBuilder, TextFieldIndexing, TextOptions},
    tokenizer::{AsciiFoldingFilter, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Tokenizer},
};
use tracing::{debug, warn};

use crate::{
    schema::{FieldDef, TableSchema, TantivyFieldConfig, TantivyListMode},
    tantivy::{
        search::{PredsQuery, build_node_query},
        udf::PredNode,
    },
    write::mem_buffer::TableKey,
};

/// Heap reserved per writer and charged against the MemBuffer budget.
pub const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

/// Deferred builds merge past this cap to bound per-query segment cost.
pub const MAX_DEFERRED_SEGMENTS: usize = 32;

/// When a build is allowed to spend CPU on segment merges. Merging is
/// logically invisible but expensive during ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// Defers ingest-path merges up to [`MAX_DEFERRED_SEGMENTS`].
    Deferred,
    /// Merges maintenance-path indexes after commit.
    Now,
}

#[derive(Debug, Default, Clone)]
pub struct IndexBuildStats {
    /// Fields actually encoded as separate exact array elements in this build.
    pub element_fields: std::collections::BTreeSet<String>,
    pub rows: u64,
    pub batches: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Segments in the finished index (1 when merged).
    pub segments: usize,
}

/// Build an in-memory tantivy `Index` from `batches`. Returns the index and
/// row-level stats; the caller serializes it to bytes (`pack_dir`) for upload.
pub fn build_in_memory(table: &TableSchema, batches: &[RecordBatch]) -> Result<(Index, BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let index = Index::create_in_ram(built.schema.clone());
    register_tokenizers(&index);
    let stats = index_to_writer(&built, &index, batches, MergeMode::Deferred)?;
    Ok((index, built, stats))
}

/// Append `batches` to an existing tantivy `Index` (created in RAM or on disk)
/// as a single commit, returning the accumulated build stats.
pub fn index_to_writer(
    built: &BuiltSchema, index: &Index, batches: impl IntoIterator<Item = impl std::borrow::Borrow<RecordBatch>>, merge: MergeMode,
) -> Result<IndexBuildStats> {
    let mut writer: IndexWriter = index.writer(WRITER_HEAP_BYTES).context("create tantivy writer")?;
    // Explicit merges keep `TermMerger` off the ingest path.
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut stats = IndexBuildStats { element_fields: built.element_fields(), ..Default::default() };
    for batch in batches {
        index_batch(built, &mut writer, batch.borrow(), &mut stats)?;
        stats.batches = stats.batches.saturating_add(1);
    }
    finish_writer(index, writer, stats, merge)
}

/// Create an on-disk tantivy index with this crate's tokenizers registered.
fn create_disk_index(built: &BuiltSchema, dir: &Path) -> Result<Index> {
    let mmap_dir = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::create(mmap_dir, built.schema.clone(), Default::default()).map_err(|e| anyhow!("create disk index: {e}"))?;
    register_tokenizers(&index);
    Ok(index)
}

/// Build a committed-file index from a bounded channel of decoded parquet
/// batches, so only the channel's small window is live at once.
///
/// Must run on a blocking thread: `IndexWriter` is CPU/blocking work and
/// `blocking_recv` would stall a Tokio async worker.
pub fn build_stream_to_dir(
    table: &TableSchema, dir: &Path, mut batches: tokio::sync::mpsc::Receiver<RecordBatch>, merge: MergeMode,
) -> Result<(BuiltSchema, IndexBuildStats)> {
    build_to_dir(table, std::iter::from_fn(|| batches.blocking_recv()), dir, merge)
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
    anyhow::ensure!(ts_col.null_count() == 0, "index timestamp column contains nulls");
    for name in built.element_fields() {
        anyhow::ensure!(schema.index_of(&name).is_ok(), "missing element index column: {name}");
    }
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
        list_mode: TantivyListMode,
    }
    let user_cols: Vec<UserCol> = built
        .user_fields
        .iter()
        .filter_map(|(name, uf)| schema.index_of(name).ok().map(|idx| (batch.column(idx), uf)))
        .map(|(column, uf)| {
            let cfg = uf.source.tantivy.as_ref().context("indexed field lacks configuration")?;
            anyhow::ensure!(
                cfg.list_mode != TantivyListMode::Elements
                    || (matches!(column.data_type(), DataType::List(f) if matches!(f.data_type(), DataType::Utf8 | DataType::Utf8View))
                        && canonical_tokenizer(cfg) == RAW_TOKENIZER
                        && cfg.flatten.is_none()),
                "element index `{}` requires List(Utf8), raw tokenizer and no flattening",
                uf.source.name
            );
            Ok(UserCol { field: uf.field, column, kind: ColKind::detect(column, cfg.flatten.as_deref())?, list_mode: cfg.list_mode })
        })
        .collect::<Result<_>>()?;

    // Nulls are ruled out above, so the column min/max is the row-wise fold.
    stats.min_timestamp_micros = stats.min_timestamp_micros.into_iter().chain(arrow::compute::min(ts_col)).min();
    stats.max_timestamp_micros = stats.max_timestamp_micros.into_iter().chain(arrow::compute::max(ts_col)).max();

    for row in 0..batch.num_rows() {
        let id = id_kind.extract(id_col, row)?.unwrap_or_default();
        // stats.rows is the doc's global ordinal; only a valid parquet row
        // index for read-back builds.
        let mut doc = doc!(built.timestamp => ts_col.value(row), built.id => id, built.row_ordinal => stats.rows);
        for uc in &user_cols {
            if uc.list_mode == TantivyListMode::Elements {
                if !uc.column.is_null(row) {
                    let arr = uc.column.as_any().downcast_ref::<ListArray>().context("element index requires list")?;
                    // Raw terms preserve punctuation, whitespace and empty strings.
                    // Repeated terms share a document posting, so they count once.
                    list_strs(&arr.value(row))?.for_each(|value| doc.add_text(uc.field, value));
                }
            } else if let Some(text) = uc.kind.extract(uc.column, row)?
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
    VariantJson(VariantArray),
    VariantKv(VariantArray),
}

impl ColKind {
    fn detect(column: &ArrayRef, flatten: Option<&str>) -> Result<Self> {
        Ok(match column.data_type() {
            DataType::Utf8 => Self::Utf8,
            DataType::Utf8View => Self::Utf8View,
            DataType::List(_) => Self::ListUtf8,
            DataType::Struct(_) => {
                let array = VariantArray::try_new(column.as_ref()).context("prepare variant index column")?;
                match flatten.unwrap_or("json") {
                    "kv" => Self::VariantKv(array),
                    _ => Self::VariantJson(array),
                }
            }
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
            Self::VariantJson(array) => prepared_variant_to_text(array, row, false)?,
            Self::VariantKv(array) => prepared_variant_to_text(array, row, true)?,
        })
    }
}

/// Every non-null string of one `List(Utf8|Utf8View)` row value.
fn list_strs(inner: &ArrayRef) -> Result<impl Iterator<Item = &str>> {
    if let Some(values) = inner.as_any().downcast_ref::<StringArray>() {
        Ok(Either::Left(values.iter().flatten()))
    } else if let Some(values) = inner.as_any().downcast_ref::<StringViewArray>() {
        Ok(Either::Right(values.iter().flatten()))
    } else {
        bail!("list element type unsupported for tantivy: {:?}", inner.data_type())
    }
}

fn list_to_text(arr: &ListArray, row: usize) -> Result<String> {
    // Space-join, not "skip when empty": empty elements are real terms.
    Ok(list_strs(&arr.value(row))?.join(" "))
}

/// Render one Variant row to text. `kv=false` → canonical JSON, produced by the
/// same serializer as the wire and `text_match` row-eval so all agree byte-for-byte.
pub(crate) fn variant_to_text(col: &ArrayRef, row: usize, kv: bool) -> Result<Option<String>> {
    let struct_arr = col.as_any().downcast_ref::<StructArray>().context("variant should be StructArray")?;
    if struct_arr.is_null(row) {
        return Ok(None);
    }
    let variant_arr = VariantArray::try_new(struct_arr).map_err(|e| anyhow!("VariantArray::try_new: {e}"))?;
    prepared_variant_to_text(&variant_arr, row, kv)
}

fn prepared_variant_to_text(variant_arr: &VariantArray, row: usize, kv: bool) -> Result<Option<String>> {
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
        Object(map) => map.iter().for_each(|(k, val)| {
            flatten_kv(val, &if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") }, out);
        }),
        Array(items) => items.iter().for_each(|item| flatten_kv(item, prefix, out)),
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
            ..Default::default()
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
                f("level", "Utf8", Some(TantivyFieldConfig { indexed: true, tokenizer: Some("raw".into()), ..Default::default() })),
            ],
        }
    }

    fn arrow_schema(level: DataType) -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
            Field::new("level", level, true),
        ]))
    }

    /// One row per batch so each `index_to_writer` call is a separate commit
    /// producing its own segment.
    fn batch(n: i64) -> RecordBatch {
        RecordBatch::try_new(
            arrow_schema(DataType::Utf8),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![n * 1_000]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec![format!("id{n}")])),
                Arc::new(StringArray::from(vec![if n % 2 == 0 { "INFO" } else { "ERROR" }])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn list_elements_preserve_exact_terms_and_legacy_text() -> Result<()> {
        use arrow::array::{ListBuilder, StringBuilder};
        for (mode, cases) in [
            (TantivyListMode::Elements, vec![("err:a", 2), ("x y", 1), ("err:a x y", 0), ("", 1), ("*", 1), ("α", 1)]),
            (TantivyListMode::JoinedText, vec![("err:a", 0), ("x y", 0), ("err:a x y", 1), ("", 0), ("* α", 1)]),
        ] {
            let mut table = table();
            table.fields[2].data_type = "List(Utf8)".into();
            table.fields[2].tantivy.as_mut().unwrap().list_mode = mode;
            let mut lists = ListBuilder::new(StringBuilder::new());
            for values in [
                Some(vec![Some("err:a"), Some("x y")]),
                Some(vec![Some("err:a"), Some("err:a")]),
                Some(vec![Some("")]),
                Some(vec![None]),
                Some(vec![]),
                None,
                Some(vec![Some("*"), Some("α")]),
            ] {
                values.iter().flatten().for_each(|value| lists.values().append_option(*value));
                lists.append(values.is_some());
            }
            let values: ArrayRef = Arc::new(lists.finish());
            let input = RecordBatch::try_new(
                arrow_schema(values.data_type().clone()),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![0; 7]).with_timezone("UTC")),
                    Arc::new(StringArray::from_iter_values((0..7).map(|i| i.to_string()))),
                    values,
                ],
            )?;
            let (index, built, stats) = build_in_memory(&table, std::slice::from_ref(&input))?;
            assert_eq!(stats.element_fields.contains("level"), mode == TantivyListMode::Elements);
            let mut entry = ManifestEntry::failed("old build".into(), vec!["file".into()]);
            entry.index = Some("index".into());
            entry.error = None;
            assert_eq!(entry.covers_current_elements(&table), mode == TantivyListMode::JoinedText, "legacy index must not stop element backfill");
            entry.element_fields = stats.element_fields.clone();
            assert_eq!(entry.covers_current_elements(&table), mode == TantivyListMode::JoinedText, "element histograms need physical ordinals");
            entry.ordinals_valid = true;
            assert!(entry.covers_current_elements(&table));
            entry.schema_version = SCHEMA_VERSION + 1;
            assert!(!entry.covers_current_elements(&table));
            let reader = index.reader()?;
            for (term, expected) in cases {
                let q = TermQuery::new(Term::from_field_text(built.user_fields["level"].field, term), IndexRecordOption::Basic);
                assert_eq!(reader.searcher().search(&q, &tantivy::collector::Count)?, expected, "{mode:?}: {term:?}");
                if mode == TantivyListMode::Elements {
                    let predicate = histogram::Membership::Contains { column: "level".into(), value: term.into() };
                    let counts = histogram::HistogramWindow::new(-1, 1, 1, 0, 2)?.count_rows(
                        std::slice::from_ref(&input),
                        &arrow::buffer::BooleanBuffer::new_set(7),
                        Some(&predicate),
                    )?;
                    assert_eq!(counts.values().sum::<u64>(), expected as u64, "captured-row membership must match exact indexed elements");
                }
            }
            if mode == TantivyListMode::Elements {
                table.fields[2].tantivy.as_mut().unwrap().tokenizer = Some("default".into());
                assert!(build_in_memory(&table, &[input]).is_err(), "element mode must reject tokenized text");
            }
        }
        Ok(())
    }

    fn error_hits(index: &Index, built: &BuiltSchema) -> Vec<Hit> {
        let q = TermQuery::new(Term::from_field_text(built.user_fields["level"].field, "ERROR"), IndexRecordOption::Basic);
        let mut hits = query_index(index, &q, None).expect("query");
        hits.sort_by_key(|hit| hit.timestamp_micros);
        hits
    }

    /// Merging must not run on the ingest path: deferred commits never merge,
    /// an explicit merge collapses to one segment, and hits are unchanged.
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

        // No new documents, merge what's there.
        let stats = index_to_writer(&built, &index, std::iter::empty::<RecordBatch>(), MergeMode::Now).expect("merge build");
        assert_eq!(stats.segments, 1, "explicit merge must collapse segments");
        assert_eq!(index.searchable_segment_ids().unwrap().len(), 1);

        assert_eq!(error_hits(&index, &built), unmerged);
    }

    /// A COST guard, not a correctness one: the build must put its scratch on
    /// the volume it was handed and write NOTHING under the process temp dir.
    #[test]
    fn index_builds_keep_scratch_off_the_process_temp_dir() {
        let scratch = tempfile::tempdir().expect("scratch root");
        let (blob, stats) = build_and_pack(&table(), &[batch(0), batch(1)], 1, MergeMode::Now, scratch.path()).expect("build");
        verify_blob(&blob).expect("verify");

        assert_eq!(stats.rows, 2, "build must actually have indexed the batches");
        // Asserts the ROOT, not leftover files: scratch dirs are reclaimed on
        // drop, so counting files under a redirected TMPDIR proves nothing.
        assert!(scratch.path().join("tantivy_scratch").is_dir(), "build must root its scratch under the volume it was given");
    }

    /// COST guard: verification must not materialise the index on disk a
    /// second time — a disk-writing version still passes every correctness
    /// assertion, so the assertion has to be that nothing was written.
    #[test]
    fn verifying_a_blob_materializes_nothing_on_disk() {
        let scratch = tempfile::tempdir().expect("scratch");
        let (blob, _) = build_and_pack(&table(), &[batch(0), batch(1)], 1, MergeMode::Now, scratch.path()).expect("build");

        let before = std::fs::read_dir(scratch_root(scratch.path())).map(|e| e.count()).unwrap_or(0);
        verify_blob(&blob).expect("fresh blob verifies");
        let after = std::fs::read_dir(scratch_root(scratch.path())).map(|e| e.count()).unwrap_or(0);

        assert_eq!(before, after, "verification must not create a scratch directory");
        assert!(verify_blob(&blob[..blob.len() / 2]).is_err(), "a truncated archive must still be rejected");
        assert!(verify_blob(b"not an archive").is_err(), "garbage must still be rejected");
    }

    #[test]
    fn startup_reaps_orphaned_scratch_but_spares_foreign_entries() {
        let root = tempfile::tempdir().expect("root");
        let base = scratch_root(root.path());
        std::fs::create_dir_all(base.join(".tmpORPHAN")).expect("orphan");
        std::fs::write(base.join(".tmpORPHAN/big.store"), b"x").expect("orphan file");
        std::fs::create_dir_all(base.join("tantivy_cache_like")).expect("foreign");

        reap_orphaned_scratch_dirs(root.path());
        // The reaper runs off-thread; join by waiting for the observable effect.
        for _ in 0..200 {
            if !base.join(".tmpORPHAN").exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert!(!base.join(".tmpORPHAN").exists(), "orphaned scratch dir must be reclaimed at startup");
        assert!(base.join("tantivy_cache_like").exists(), "reaper must only touch TempDir's own .tmp* names");
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
// Build a Tantivy `Schema` from the YAML `TableSchema`. Always emits
// `_timestamp` (i64 micros, STORED+FAST) and `_id` (raw text, STORED); user
// fields with `tantivy.indexed: true` become text fields. Tokenizer choice:
//   "raw"     → keyword (exact match, single token; case-sensitive)
//   "default" → lowercase + word split
//   "ngram3"  → lowercased 3-grams; supports `LIKE '%substr%'` and `ILIKE`.
// No tokenizer specified defaults to `ngram3`.

/// Tokenizer name for n-gram indexing. Combined with `LowerCaser` so `ILIKE`
/// semantics fall out automatically.
pub const NGRAM3_TOKENIZER: &str = "tf_ngram3";
/// Word-level indexing (lowercase + word split + ASCII fold + length cap).
/// Named after tantivy's default so `TEXT` field options can reuse it.
pub const DEFAULT_TOKENIZER: &str = "default";
/// Tokenizer name for keyword/exact-match indexing.
pub const RAW_TOKENIZER: &str = "raw";
/// Token length cap; bounds posting growth on pathological inputs.
const MAX_TOKEN_LEN: usize = 256;

// User fields are indexed-only: only `_timestamp` and `_id` are stored, because
// the reader emits `(timestamp, id)` hits the SQL layer joins back to Delta.

pub const TS_FIELD: &str = "_timestamp";
pub const ID_FIELD: &str = "_id";
/// Global row offset of the doc within the file the index covers (FAST). Only
/// meaningful when the index was built by reading the parquet back in row order
/// (`ManifestEntry.ordinals_valid`); flush-path indexes see pre-sort batches.
pub const ROW_ORDINAL_FIELD: &str = "_row_ordinal";

/// Result of building a tantivy schema for a table.
pub struct BuiltSchema {
    pub schema: Schema,
    pub timestamp: Field,
    pub id: Field,
    pub row_ordinal: Field,
    /// Source-column-name → tantivy field, for `indexed: true` columns only.
    pub user_fields: HashMap<String, UserField>,
}

impl BuiltSchema {
    fn element_fields(&self) -> std::collections::BTreeSet<String> {
        element_field_names(self.user_fields.values().map(|f| &f.source))
    }
}

/// Columns declared as exact-element (`list_mode: elements`) indexes — the one
/// definition shared by build stats and the manifest coverage check.
fn element_field_names<'a>(fields: impl IntoIterator<Item = &'a FieldDef>) -> std::collections::BTreeSet<String> {
    fields
        .into_iter()
        .filter(|field| field.tantivy.as_ref().is_some_and(|config| config.indexed && config.list_mode == TantivyListMode::Elements))
        .map(|field| field.name.clone())
        .collect()
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
    // FAST lets the reader pull hit ids columnar instead of per-doc doc-store
    // fetches; STORED is kept so older readers can still open the index.
    TextOptions::default()
        .set_indexing_options(TextFieldIndexing::default().set_tokenizer(RAW_TOKENIZER).set_index_option(IndexRecordOption::Basic))
        .set_fast(Some(RAW_TOKENIZER))
        | STORED
}

/// Canonicalize a YAML tokenizer name. Absent *and* unknown names fall through
/// to ngram3 rather than panicking.
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
pub fn register_tokenizers(index: &Index) {
    /// Shared filter chain: length cap → lowercase → ASCII fold.
    fn analyzer<T: Tokenizer>(tokenizer: T) -> TextAnalyzer {
        TextAnalyzer::builder(tokenizer).filter(RemoveLongFilter::limit(MAX_TOKEN_LEN)).filter(LowerCaser).filter(AsciiFoldingFilter).build()
    }
    let tokenizers = index.tokenizers();
    tokenizers.register(NGRAM3_TOKENIZER, analyzer(NgramTokenizer::new(3, 3, false).expect("3-gram bounds are valid")));
    // Re-register the built-in chains so behavior is pinned against upstream.
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
// Manifest is JSON written with PUT-overwrite; concurrent writers are
// serialized by a coarse in-process lock per (table, project_id). Across
// processes it is last-writer-wins, which is safe because entries are
// idempotent upserts.

pub const MANIFEST_PREFIX: &str = "index_manifests";
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, educe::Educe)]
#[educe(Default)]
pub struct Manifest {
    #[educe(Default = SCHEMA_VERSION)]
    pub version: u32,
    pub entries: BTreeMap<String, ManifestEntry>,
    /// Replaced generations stay readable through manifest caches and in-flight
    /// queries. GC removes them after the grace period, retrying failed deletes.
    #[serde(default)]
    pub retired_blobs: BTreeMap<String, DateTime<Utc>>,
    /// Bounded daily proofs survive eviction of the large logical-count cache.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) count_proofs: BTreeMap<chrono::NaiveDate, visibility::PartitionCountProof>,
}

impl Manifest {
    /// Selects at most one physical-ordinal index for each captured file.
    /// Results preserve source order; missing or obsolete coverage stays uncovered.
    /// Absolute manifest URIs are resolved against the captured table root.
    pub fn histogram_entries<'a>(&'a self, root: &url::Url, files: &[visibility::SnapshotFile]) -> Result<Vec<Option<HistogramEntry<'a>>>> {
        anyhow::ensure!(self.version == SCHEMA_VERSION, "unsupported histogram manifest version");
        let paths = files.iter().map(|file| file.path.as_str()).collect::<std::collections::BTreeSet<_>>();
        anyhow::ensure!(paths.len() == files.len(), "histogram snapshot has duplicate physical file paths");
        let mut selected = BTreeMap::new();
        for (key, entry) in &self.entries {
            if entry.schema_version != SCHEMA_VERSION || entry.index.is_none() || entry.error.is_some() || !entry.ordinals_valid {
                continue;
            }
            let [source] = entry.covered_files.as_slice() else { continue };
            let Some(source) = visibility::relative_source_path(root, source)? else { continue };
            if paths.contains(source.as_str()) {
                anyhow::ensure!(
                    selected.insert(source.clone(), HistogramEntry { key, entry }).is_none(),
                    "histogram manifest has overlapping ordinal coverage for {source}"
                );
            }
        }
        Ok(files.iter().map(|file| selected.remove(file.path.as_str())).collect())
    }

    fn insert(&mut self, key: String, entry: ManifestEntry) {
        if let Some(blob) = &entry.index {
            self.retired_blobs.remove(blob);
        }
        if let Some(old) = self.entries.insert(key.clone(), entry)
            && let Some(blob) = old.index
            && self.entries[&key].index.as_ref() != Some(&blob)
        {
            self.retired_blobs.entry(blob).or_insert_with(Utc::now);
        }
    }
}

/// An entry borrowed from the manifest retained by the query snapshot.
pub struct HistogramEntry<'a> {
    pub key: &'a str,
    pub entry: &'a ManifestEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, educe::Educe)]
#[educe(Default)]
pub struct ManifestEntry {
    /// Missing in legacy manifests: those indexes store joined list text and
    /// cannot answer exact element predicates, even if the field name exists.
    #[serde(default)]
    pub element_fields: std::collections::BTreeSet<String>,
    /// Object-store path to the index tar.zst, or `None` if build failed.
    pub index: Option<String>,
    pub rows: u64,
    #[educe(Default(expression = Utc::now()))]
    pub built_at: DateTime<Utc>,
    #[educe(Default = SCHEMA_VERSION)]
    pub schema_version: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Set when build failed; `index` will be None.
    pub error: Option<String>,
    /// Parquet file URIs this index covers. GC drops the entry once any of
    /// them is no longer live (compacted away).
    #[serde(default)]
    pub covered_files: Vec<String>,
    /// True when `_row_ordinal` equals parquet row order, i.e. the index was
    /// built by reading the committed file back. Flush-path indexes see
    /// batches BEFORE the writer's sort, so their ordinals must not drive
    /// row selection.
    #[serde(default)]
    pub ordinals_valid: bool,
}

/// Object-store path of the manifest for a given table/project.
pub fn manifest_path(table: &str, project_id: &str) -> ObjPath {
    ObjPath::from(format!("{MANIFEST_PREFIX}/{table}/{project_id}/manifest.json"))
}

/// Project ids that have a manifest under this table's prefix — the GC's
/// authoritative iteration set. Manifests are keyed by the project uuid from
/// the parquet URI, so a fixed project list would miss unified tenants.
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

/// Load the manifest, apply `f`, and save it back, serialized per
/// (table, project_id): concurrent flushes would otherwise interleave
/// load/save and silently drop each other's entries.
///
/// `f` returns the caller's result plus whether the manifest actually changed;
/// a no-op mutation must not rewrite the object.
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

impl ManifestEntry {
    /// Whether this entry has the list representation requested by the table.
    /// Used by both the coverage census and maintenance backfill.
    /// Element histograms also require one-file physical ordinal coverage.
    pub fn covers_current_elements(&self, table: &TableSchema) -> bool {
        self.index.is_some()
            && self.error.is_none()
            && self.schema_version == SCHEMA_VERSION
            && (self.element_fields.is_empty() || (self.ordinals_valid && self.covered_files.len() == 1))
            && self.element_fields == element_field_names(&table.fields)
    }
    /// Entry recorded when the index build itself failed: no index, no rows,
    /// but the covered files are still tracked so GC can reap it later.
    pub fn failed(error: String, covered_files: Vec<String>) -> Self {
        Self { error: Some(error), covered_files, ..Default::default() }
    }
}

/// Idempotent upsert: load, mutate, save.
pub async fn upsert_manifest(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_key: &str, entry: ManifestEntry) -> Result<()> {
    upsert_manifest_many(store, table, project_id, vec![(parquet_key.to_string(), entry)]).await
}

/// Upsert many entries under ONE load+save of the manifest. Each
/// `upsert_manifest` is a full read-modify-write under a per-(table,project)
/// lock, so batching is what keeps backfill throughput off that ceiling.
pub async fn upsert_manifest_many(store: &dyn ObjectStore, table: &str, project_id: &str, entries: Vec<(String, ManifestEntry)>) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    mutate(store, table, project_id, |m| {
        entries.into_iter().for_each(|(key, entry)| m.insert(key, entry));
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
// Pack/unpack tantivy indexes for object-store transport. Cold form is a single
// `tar.zst` blob per parquet file; warm form is an extracted directory that
// tantivy mmap-opens. Paths: indexes/{table}/v1/{project_id}/{uuid}.tantivy.tar.zst

pub const INDEX_PREFIX: &str = "indexes";
pub const INDEX_VERSION: &str = "v1";
pub const BLOB_SUFFIX: &str = ".tantivy.tar.zst";
/// Decoded Arrow batches allowed between the parquet reader and Tantivy writer:
/// bounds source-row memory independently of file size, while keeping decode
/// and indexing overlapped.
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
/// parquet tree and reversible (`index_to_parquet_rel` is the inverse).
pub fn index_path_for_parquet(table: &str, parquet_rel: &str) -> ObjPath {
    let stem = parquet_rel.strip_suffix(".parquet").unwrap_or(parquet_rel);
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{stem}{BLOB_SUFFIX}"))
}

/// Gives each successful build an immutable object identity. A rebuild can
/// change field representation or physical ordinals while retaining its source file.
fn generation_blob_path(base: &ObjPath, generation: uuid::Uuid) -> ObjPath {
    let stem = base.as_ref().strip_suffix(BLOB_SUFFIX).unwrap_or(base.as_ref());
    ObjPath::from(format!("{stem}.generation-{generation}{BLOB_SUFFIX}"))
}

fn split_blob_generation(blob: &str) -> Option<(&str, &str)> {
    let (stem, generation) = blob.strip_suffix(BLOB_SUFFIX)?.rsplit_once(".generation-")?;
    uuid::Uuid::parse_str(generation).ok()?;
    Some((stem, generation))
}

/// Inverse of `index_path_for_parquet`: recover the table-relative parquet
/// path from an index blob path, or `None` if it isn't a partition-mirrored
/// blob for `table`. Used by reconcile to detect orphan blobs (no live parquet).
pub fn index_to_parquet_rel(table: &str, blob_path: &str) -> Option<String> {
    let prefix = format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/");
    let blob = blob_path.strip_prefix(&prefix)?;
    let stem = split_blob_generation(blob).map(|(stem, _)| stem).or_else(|| blob.strip_suffix(BLOB_SUFFIX))?;
    Some(format!("{stem}.parquet"))
}

/// Stream one committed parquet through a bounded channel into the on-disk
/// Tantivy writer, then pack and verify the completed index — the
/// memory-bounded counterpart to [`build_and_pack`].
pub async fn build_parquet_and_pack(
    store: Arc<dyn ObjectStore>, parquet_rel: &str, table: &'static TableSchema, level: i32, merge: MergeMode, scratch: &Path,
) -> Result<(Bytes, IndexBuildStats)> {
    use deltalake::datafusion::parquet::arrow::{
        ProjectionMask,
        async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    };
    use futures::TryStreamExt;

    let path = ObjPath::from(parquet_rel);
    let meta = store.head(&path).await.with_context(|| format!("head {parquet_rel}"))?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    // Decode exactly the columns the index consumes — same index schema and
    // every physical row, without reading unrelated column chunks.
    let fields: std::collections::HashSet<&str> =
        table.fields.iter().filter_map(|f| f.tantivy.as_ref()?.indexed.then_some(f.name.as_str())).chain(["timestamp", "id"]).collect();
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.context("parquet stream builder")?;
    let columns = builder.schema().fields().iter().enumerate().filter_map(|(index, field)| fields.contains(field.name().as_str()).then_some(index));
    let projection = ProjectionMask::roots(builder.parquet_schema(), columns);
    let mut stream = builder.with_projection(projection).build().context("build parquet stream")?;
    let tmp = scratch_tempdir(scratch).context("build_parquet_and_pack: tempdir")?;
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

/// Scratch directory for an index build, rooted on the data volume.
///
/// Deliberately NOT `std::env::temp_dir()`: in a container that is the overlay2
/// copy-on-write layer, where multi-GB build scratch is unsized, unobservable,
/// and contends with the write path.
fn scratch_tempdir(root: &Path) -> Result<tempfile::TempDir> {
    let base = scratch_root(root);
    std::fs::create_dir_all(&base).with_context(|| format!("create scratch root {}", base.display()))?;
    tempfile::TempDir::new_in(&base).with_context(|| format!("tempdir under {}", base.display()))
}

pub(crate) fn scratch_root(root: &Path) -> std::path::PathBuf {
    root.join("tantivy_scratch")
}

/// Delete scratch directories left by a previous process, once at startup.
/// `TempDir` reclaims on drop, but a hard kill skips that and multi-GB indexes
/// would accumulate on the volume that also carries the WAL.
///
/// Snapshot-then-delete off-thread, and only `TempDir`'s own `.tmp*` names:
/// enumerating lazily would race directories a live build is creating.
///
/// Runs at most ONCE per process — a second service sharing the root would see
/// the first's live scratch as orphans.
pub fn reap_orphaned_scratch_dirs(root: &Path) {
    static REAPED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if REAPED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let base = scratch_root(root);
    let orphans: Vec<std::path::PathBuf> = std::fs::read_dir(&base)
        .map(|entries| entries.flatten().filter(|e| e.file_name().to_string_lossy().starts_with(".tmp")).map(|e| e.path()).collect())
        .unwrap_or_default();
    if orphans.is_empty() {
        return;
    }
    std::thread::Builder::new()
        .name("tantivy-scratch-reap".into())
        .spawn(move || {
            let removed = orphans.iter().filter(|path| std::fs::remove_dir_all(path).is_ok()).count();
            warn!("reaped {removed} orphaned tantivy scratch dir(s) of {} found", orphans.len());
        })
        .map_or_else(|e| warn!("tantivy scratch reap: cannot spawn reaper for {base:?}: {e}"), |_| ());
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot, then
/// pack it into a `tar.zst` blob. Avoids any RAM→disk copy.
pub fn build_and_pack(table: &TableSchema, batches: &[RecordBatch], level: i32, merge: MergeMode, scratch: &Path) -> Result<(Bytes, IndexBuildStats)> {
    let tmp = scratch_tempdir(scratch).context("build_and_pack: tempdir")?;
    let (_built, stats) = build_to_dir(table, batches, tmp.path(), merge)?;
    Ok((pack_dir(tmp.path(), level)?, stats))
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot.
pub fn build_to_dir(
    table: &TableSchema, batches: impl IntoIterator<Item = impl std::borrow::Borrow<RecordBatch>>, dir: &Path, merge: MergeMode,
) -> Result<(BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let index = create_disk_index(&built, dir)?;
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

/// Round-trip a freshly packed blob (decode every entry + open it) before
/// publishing, so a structurally-corrupt archive is never uploaded: blob paths
/// are immutable and reader-cached, so a poison blob fails every future read
/// until a manual reindex. Stages into a `RamDirectory` — `zstd::decode_all`
/// already holds the whole tar in memory, so writing it to disk is pure waste.
pub fn verify_blob(blob: &[u8]) -> Result<()> {
    let tar_bytes = zstd::decode_all(blob).context("zstd decode")?;
    let staged = tantivy::directory::RamDirectory::create();
    let mut files = 0usize;
    for entry in tar::Archive::new(&tar_bytes[..]).entries().context("tar entries")? {
        let mut entry = entry.context("tar entry")?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        // `pack_dir` tars with `append_dir_all(".", …)` so paths arrive as
        // `./meta.json`; a tantivy index dir is flat, so the file name is the key.
        let entry_path = entry.path().context("tar entry path")?.into_owned();
        let Some(name) = entry_path.file_name().map(PathBuf::from) else { continue };
        let mut bytes = Vec::with_capacity(usize::try_from(entry.size()).unwrap_or(0));
        std::io::Read::read_to_end(&mut entry, &mut bytes).context("read tar entry")?;
        tantivy::Directory::atomic_write(&staged, &name, &bytes).context("stage tar entry")?;
        files += 1;
    }
    if files == 0 {
        anyhow::bail!("packed blob contains no files");
    }
    open_index_in(staged).map(drop)
}

/// Open an unpacked tantivy index for querying.
pub fn open_index(dir: &Path) -> Result<Index> {
    open_index_in(MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?)
}

fn open_index_in(dir: impl tantivy::Directory) -> Result<Index> {
    let index = Index::open(dir).map_err(|e| anyhow!("open index: {e}"))?;
    // Registry is per-Index and not persisted: the reader must re-register the
    // same chains the writer used, or lookups silently fall back to default.
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
        assert_eq!(index_to_parquet_rel(table, &blob).as_deref(), Some(rel));
        assert_eq!(index_to_parquet_rel("other_table", &blob), None);
        assert_eq!(index_to_parquet_rel(table, "indexes/otel_logs_and_spans/v1/foo.txt"), None);
    }
}

// ===== mem_index =====
// In-memory tantivy index for a single MemBuffer bucket, built on first
// text-match query and reused until the bucket's row count grows. Dropped when
// the bucket drains or is evicted — a pure query cache, never authoritative.
// Each index costs roughly 2x the indexed text size in postings.

/// A built tantivy index covering all rows currently in a bucket.
pub struct BucketTextIndex {
    pub index: Index,
    pub built_schema: Arc<BuiltSchema>,
    /// Row count at build time; the cache is valid only while
    /// `bucket.row_count == indexed_rows`.
    pub indexed_rows: usize,
    /// Approximate memory cost in bytes; drives the `MemBuffer` LRU budget.
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
        Ok(Some(Self { index, built_schema: Arc::new(built_schema), indexed_rows: row_count, size_bytes }))
    }

    /// Evaluate a routable predicate tree as ONE combined query, using the same
    /// query builder as the Delta sidecar search (And→Must, Or→Should).
    pub fn search_node(&self, node: &PredNode) -> Result<Vec<Hit>> {
        match build_node_query(&self.index, node)? {
            PredsQuery::MissingField => Err(anyhow!("field not in mem-index (schema drift within bucket lifetime)")),
            PredsQuery::Query(q) => query_index(&self.index, q.as_ref(), None),
        }
    }
}

/// Approximate memory cost of an index built from these batches: indexed-text
/// bytes x2 for postings. Feeds the `MemBuffer` LRU budget, itself a soft cap.
fn estimate_index_size(indexed_fields: &[String], batches: &[RecordBatch]) -> usize {
    use arrow::array::AsArray;
    batches
        .iter()
        .flat_map(|batch| indexed_fields.iter().filter_map(move |name| batch.column_by_name(name)))
        .map(|arr| match arr.as_string_opt::<i32>() {
            Some(a) => a.value_data().len(),
            // Utf8View has no contiguous value buffer; total array bytes
            // over-count but stay in magnitude.
            None if arr.as_string_view_opt().is_some() => arr.get_array_memory_size(),
            None => 0,
        })
        .sum::<usize>()
        .saturating_mul(2)
}
