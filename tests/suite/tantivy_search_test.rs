//! End-to-end tantivy index/search tests: build via callback, query via the
//! search service. No Delta involved.

use std::sync::{Arc, atomic::Ordering::Relaxed};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as OsResult, memory::InMemory, path::Path,
};
use tempfile::TempDir;
use timefusion::{
    config::TantivyConfig,
    schema::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy::{
        ManifestEntry, load_manifest,
        search::{TantivyIndexService, TantivySearchService},
        udf::TextMatchPred,
        upsert_manifest,
    },
};

/// The config production uses: derived `TantivyConfig::default()` is all
/// zeros/false and bypasses the `#[serde_inline_default]` attributes, so tests
/// that depend on default-driven behaviour must deserialize instead.
fn prod_defaults() -> TantivyConfig {
    serde_json::from_str("{}").expect("TantivyConfig has a default for every field")
}

fn level_error_node() -> timefusion::tantivy::udf::PredNode {
    timefusion::tantivy::udf::PredNode::Leaf(TextMatchPred { column: "level".into(), query: "ERROR".into() })
}

/// Arrow spelling of the reserved timestamp column, shared by every fixture table.
pub(crate) const TS_TYPE: &str = "Timestamp(Microsecond, Some(\"UTC\"))";

pub(crate) fn tantivy_cfg(tokenizer: &str, flatten: Option<&str>) -> TantivyFieldConfig {
    TantivyFieldConfig { indexed: true, tokenizer: Some(tokenizer.into()), flatten: flatten.map(Into::into), ..Default::default() }
}

pub(crate) fn field(name: &str, data_type: &str, nullable: bool, tantivy: Option<TantivyFieldConfig>) -> FieldDef {
    FieldDef { name: name.into(), data_type: data_type.into(), nullable, tantivy, ..Default::default() }
}

/// `TableSchema` has no `Default`: no partitions, no dedup, no rollups, sorted
/// by `timestamp` ascending.
pub(crate) fn table_schema(table_name: &str, fields: Vec<FieldDef>) -> TableSchema {
    TableSchema {
        table_name: table_name.into(),
        fields,
        sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: false, nulls_first: false }],
        rollups: vec![],
        partitions: vec![],
        z_order_columns: vec![],
        time_column: None,
        dedup_keys: vec![],
        dedup_tiebreak: None,
        tombstone_column: None,
        version_append: false,
    }
}

/// The `logs` fixture: `timestamp`/`id` unindexed, `level` raw-tokenized.
pub(crate) fn logs_schema() -> TableSchema {
    table_schema(
        "logs",
        vec![field("timestamp", TS_TYPE, false, None), field("id", "Utf8", false, None), field("level", "Utf8", true, Some(tantivy_cfg("raw", None)))],
    )
}

/// Rows of `logs_schema()`. `with_hashes` appends the all-null `hashes`
/// List(Utf8) column the otel table carries; the `logs` fixture omits it.
pub(crate) fn logs_batch(rows: &[(i64, &str, &str)], with_hashes: bool) -> RecordBatch {
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>()));
    let mut fields = vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
    ];
    let mut columns = vec![ts, id, level];
    if with_hashes {
        let hashes = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        columns.push(arrow::array::new_null_array(&hashes, rows.len()));
        fields.push(Field::new("hashes", hashes, true));
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
}

/// Store + indexer + reader over one cache dir. Which config a constructor
/// picks is semantic, not style — derived default and `prod_defaults()` differ
/// in behaviour, so never substitute one for the other.
struct Env {
    table: &'static str,
    project: &'static str,
    store: Arc<dyn ObjectStore>,
    svc: Arc<TantivyIndexService>,
    search: Arc<TantivySearchService>,
    _cache: TempDir,
}

impl Env {
    fn new(table: &'static str, project: &'static str, store: Arc<dyn ObjectStore>, index_cfg: TantivyConfig, search_cfg: TantivyConfig) -> Self {
        let cache = TempDir::new().unwrap();
        Self {
            table,
            project,
            // Scratch under the per-Env TempDir keeps concurrent tests isolated.
            svc: Arc::new(TantivyIndexService::new(store.clone(), Arc::new(index_cfg), cache.path().join("scratch"))),
            search: Arc::new(TantivySearchService::new(store.clone(), cache.path().to_path_buf(), Arc::new(search_cfg))),
            store,
            _cache: cache,
        }
    }
    /// Indexer on zstd level 3 (cheap builds), reader on the derived default.
    fn zstd3(table: &'static str, project: &'static str) -> Self {
        let cfg = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
        Self::new(table, project, Arc::new(InMemory::new()), cfg, TantivyConfig::default())
    }
    /// Both sides on the DERIVED default.
    fn plain(table: &'static str, project: &'static str) -> Self {
        Self::new(table, project, Arc::new(InMemory::new()), TantivyConfig::default(), TantivyConfig::default())
    }
    /// Both sides on the config production actually deserializes.
    fn prod(table: &'static str, project: &'static str) -> Self {
        Self::new(table, project, Arc::new(InMemory::new()), prod_defaults(), prod_defaults())
    }
    /// Wires publish-time cache seeding; chained before the first publish.
    fn seeded(self) -> Self {
        self.svc.with_reader(&self.search);
        self
    }
    async fn publish(&self, rows: &[(i64, &str, &str)], uris: &[&str]) {
        self.svc.clone().batch_callback()(
            self.project.to_string(),
            self.table.to_string(),
            vec![logs_batch(rows, true)],
            uris.iter().map(|u| (*u).to_string()).collect(),
        )
        .await
        .expect("callback");
    }
    async fn manifest(&self) -> timefusion::tantivy::Manifest {
        load_manifest(self.store.as_ref(), self.table, self.project).await.unwrap()
    }
    async fn hits(&self, field: &str, query: &str) -> Option<Vec<timefusion::tantivy::search::Hit>> {
        self.search.search(self.table, self.project, field, query).await.unwrap()
    }
    async fn gc(&self, live: &[&str]) -> timefusion::tantivy::search::GcReport {
        self.svc.gc_after_compaction(self.table, self.project, &live.iter().map(|u| (*u).to_string()).collect::<Vec<_>>()).await.unwrap()
    }
    async fn carry_forward(&self, removed: &[&str], added: &[&str]) -> bool {
        let owned = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        self.svc.carry_forward_after_compaction(self.table, self.project, &owned(removed), &owned(added)).await.unwrap()
    }
}

#[tokio::test]
async fn histogram_reads_masked_buckets_without_materializing_hits() -> anyhow::Result<()> {
    use arrow::{
        array::{Array, ListBuilder, StringBuilder},
        buffer::BooleanBuffer,
    };
    use timefusion::tantivy::{
        MergeMode, build_and_pack,
        histogram::{HistogramWindow, Membership},
        search::HistogramFile,
        upload,
    };

    let mut table = logs_schema();
    table.fields[2].data_type = "List(Utf8)".into();
    table.fields[2].tantivy.as_mut().unwrap().list_mode = timefusion::schema::TantivyListMode::Elements;
    let base = logs_batch(&[(1, "one", "a"), (2, "two", "b"), (3, "three", "a")], true);
    let mut lists = ListBuilder::new(StringBuilder::new());
    for tag in ["a", "b", "a"] {
        lists.values().append_value(tag);
        lists.values().append_value(tag);
        lists.append(true);
    }
    let lists = Arc::new(lists.finish());
    let input_schema =
        Arc::new(ArrowSchema::new(vec![base.schema().field(0).clone(), base.schema().field(1).clone(), Field::new("level", lists.data_type().clone(), true)]));
    let input = RecordBatch::try_new(input_schema, vec![base.column(0).clone(), base.column(1).clone(), lists])?;
    let (blob, stats) = build_and_pack(&table, std::slice::from_ref(&input), 3, MergeMode::Now, &std::env::temp_dir())?;
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    upload(store.as_ref(), &Path::from("histogram"), blob).await?;
    let mut entry = ManifestEntry {
        index: Some("histogram".into()),
        rows: stats.rows,
        element_fields: stats.element_fields,
        ordinals_valid: true,
        covered_files: vec!["file".into()],
        ..Default::default()
    };
    let files = ["file", "uncovered"].map(|path| timefusion::tantivy::visibility::SnapshotFile {
        path: path.into(),
        size: 0,
        partition_values: Default::default(),
        deletion_vector: None,
    });
    let mut manifest = timefusion::tantivy::Manifest::default();
    manifest.entries.insert("current".into(), entry.clone());
    let mut obsolete = entry.clone();
    obsolete.ordinals_valid = false;
    manifest.entries.insert("old-flush".into(), obsolete);
    let root = url::Url::parse("s3://bucket/tables/logs")?;
    let selected = manifest.histogram_entries(&root, &files)?;
    assert_eq!(selected[0].as_ref().unwrap().key, "current");
    assert!(selected[1].is_none(), "uncovered sources must remain available to fallback");
    entry.covered_files = vec!["s3://bucket/tables/logs/file".into()];
    manifest.entries.insert("overlap".into(), entry.clone());
    assert!(manifest.histogram_entries(&root, &files).is_err(), "overlapping entries must not double count a source");
    manifest.entries.remove("overlap");
    assert!(manifest.histogram_entries(&root, &[files[0].clone(), files[0].clone()]).is_err());
    let cache = TempDir::new()?;
    let mut config = prod_defaults();
    config.timefusion_tantivy_prefilter_max_hits = 1;
    let service = TantivySearchService::new(store, cache.path().into(), Arc::new(config));
    let predicate = Membership::Contains { column: "level".into(), value: "a".into() };
    let union = Membership::Or(Box::new(predicate.clone()), Box::new(predicate.clone()));
    let window = HistogramWindow::new(0, 4, 2, 0, 2)?;
    for predicate in [Some(&predicate), Some(&union), None] {
        let result = service
            .histogram_file(
                "logs",
                "p",
                window,
                predicate,
                HistogramFile {
                    table_root: &root,
                    manifest_key: "file",
                    source_file: "file",
                    entry: &entry,
                    visible: BooleanBuffer::from(vec![true, false, true]),
                },
            )
            .await?;
        assert_eq!(result, std::collections::BTreeMap::from([(0, 1), (2, 1)]));
        assert_eq!(window.count_rows(std::slice::from_ref(&input), &BooleanBuffer::from(vec![true, false, true]), predicate)?, result);
    }
    assert_eq!(service.stats.hits_materialized.load(Relaxed), 0);
    let rows = timefusion::tantivy::visibility::ResolvedSnapshot {
        sources: vec![
            timefusion::tantivy::visibility::SourceRows { batches: vec![input.clone()], live: BooleanBuffer::new_set(3) },
            timefusion::tantivy::visibility::SourceRows { batches: vec![input.slice(1, 1)], live: BooleanBuffer::new_set(1) },
        ],
        winners: vec![BooleanBuffer::from(vec![true, false, true]), BooleanBuffer::new_set(1)],
    };
    for mode in ["indexed", "uncovered", "missing_blob"] {
        if mode != "indexed" {
            manifest.entries.clear();
        }
        if mode == "missing_blob" {
            let mut missing = entry.clone();
            missing.index = Some("absent-index".into());
            manifest.entries.insert("absent-file".into(), missing);
        }
        let result = service
            .histogram_snapshot(
                "logs",
                "p",
                window,
                None,
                timefusion::tantivy::search::HistogramSnapshot { table_root: &root, files: &files[..1], manifest: &manifest, rows: &rows },
            )
            .await?;
        assert_eq!(result.counts, std::collections::BTreeMap::from([(0, 1), (2, 2)]));
        assert_eq!(result.indexed_sources, usize::from(mode == "indexed"));
        assert_eq!(result.scanned_sources, if mode == "indexed" { 1 } else { 2 });
        assert_eq!(result.index_errors.len(), usize::from(mode == "missing_blob"));
    }
    entry.ordinals_valid = false;
    assert!(
        service
            .histogram_file(
                "logs",
                "p",
                window,
                Some(&predicate),
                HistogramFile { table_root: &root, manifest_key: "file", source_file: "file", entry: &entry, visible: BooleanBuffer::new_set(3) }
            )
            .await
            .is_err(),
        "flush-order indexes cannot use physical masks"
    );
    Ok(())
}

#[tokio::test]
async fn callback_builds_index_and_search_returns_hits() {
    // `otel_logs_and_spans` is used because the schema loader only knows tables
    // from the compiled YAML. Only timestamp/id/level are supplied — schema
    // validation is on the Delta side, not tantivy.
    let env = Env::zstd3("otel_logs_and_spans", "p1");

    env.publish(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "INFO")], &["test-uri"]).await;

    let m = env.manifest().await;
    assert_eq!(m.entries.len(), 1);
    let entry = m.entries.values().next().unwrap();
    assert_eq!(entry.rows, 3);
    assert!(entry.index.is_some());
    assert_eq!(entry.min_timestamp_micros, Some(1_000_000));
    assert_eq!(entry.max_timestamp_micros, Some(3_000_000));

    let hits = env.hits("level", "ERROR").await.expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
    assert_eq!(hits[0].timestamp_micros, 2_000_000);

    assert_eq!(hits, env.hits("level", "ERROR").await.unwrap());
}

#[tokio::test]
async fn multi_pred_and_is_single_pass_and_conjunctive() {
    // Two predicates run as ONE combined query per index: only the row matching
    // BOTH survives, and indexed_rows counts the index set once.
    let env = Env::plain("otel_logs_and_spans", "p-multipred");
    env.publish(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "ERROR")], &["f1"]).await;

    let preds = vec![TextMatchPred { column: "level".into(), query: "ERROR".into() }, TextMatchPred { column: "id".into(), query: "c".into() }];
    let node = timefusion::tantivy::udf::PredNode::from_preds(&preds).expect("non-empty");
    let r = env.search.search_with_stats(env.table, env.project, &node, 1000, None).await.unwrap().expect("usable");
    assert_eq!(r.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(), vec!["c".to_string()]);
    assert_eq!(r.indexed_rows, 3, "denominator must count the index set once, not per predicate");
}

#[tokio::test]
async fn single_file_flush_publishes_partition_mirrored_blob() {
    // A single-file flush keys the manifest entry by table-relative path and
    // uploads the blob at the partition-mirrored location, while covering the
    // original absolute URI for the coverage gate.
    let env = Env::plain("otel_logs_and_spans", "p-mirrored");
    let uri = format!("s3://bucket/timefusion/default/{}/project_id={}/date=2026-07-05/part-00000-abc-c000.zstd.parquet", env.table, env.project);
    let rel = format!("project_id={}/date=2026-07-05/part-00000-abc-c000.zstd.parquet", env.project);
    env.publish(&[(1_000_000, "a", "ERROR")], &[uri.as_str()]).await;

    let m = env.manifest().await;
    let entry = m.entries.get(&rel).expect("manifest keyed by table-relative parquet path");
    assert_eq!(entry.covered_files, vec![uri], "covered_files must keep the absolute URI");
    let blob = entry.index.as_ref().expect("index built");
    assert_eq!(timefusion::tantivy::index_to_parquet_rel(env.table, blob).as_deref(), Some(rel.as_str()), "generation must retain source identity");

    assert_eq!(env.hits("level", "ERROR").await.unwrap().len(), 1);
}

#[tokio::test]
async fn reader_cache_avoids_reopen_across_queries() {
    let env = Env::plain("otel_logs_and_spans", "p-readercache");
    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;

    for _ in 0..3 {
        assert_eq!(env.hits("level", "ERROR").await.unwrap().len(), 1);
    }
    assert_eq!(env.search.stats.index_opens.load(Relaxed), 1, "one cold open; subsequent queries must hit the reader LRU");
}

/// Seeding on publish must make the first query read local disk instead of S3;
/// proven by failing every GET after publish.
#[tokio::test]
async fn seeded_cache_serves_first_query_without_object_store_reads() {
    assert!(prod_defaults().seed_cache_on_publish(), "seeding is the default this test covers");
    let store = Arc::new(FailAfterArm::new(Arc::new(InMemory::new())));
    let env = Env::new("otel_logs_and_spans", "p-seeded", store.clone(), prod_defaults(), prod_defaults()).seeded();

    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    assert_eq!(env.search.stats.cache_seeded.load(Relaxed), 1, "publish must seed the reader's extracted-index cache");
    assert_eq!(env.search.stats.cache_seed_failures.load(Relaxed), 0);

    // Warm the manifest cache, then cut S3 off.
    env.hits("level", "ERROR").await.unwrap();
    let fetches_before = env.search.stats.blob_fetches.load(Relaxed);
    store.arm();

    let hits = env.search.search(env.table, env.project, "level", "ERROR").await.expect("search must not touch S3").expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(env.search.stats.blob_fetches.load(Relaxed), fetches_before, "a seeded index must never be re-downloaded");
    assert_eq!(fetches_before, 0, "the very first query must already be served locally");
}

#[tokio::test]
async fn rebuilding_the_same_file_replaces_cached_terms_without_overwriting_old_blob() {
    let env = Env::prod("otel_logs_and_spans", "generation-test").seeded();
    let uri = format!("s3://bucket/{}/project_id={}/date=2026-09-08/part-file.parquet", env.table, env.project);
    let mut previous_blob = None;
    for level in ["ERROR", "INFO"] {
        env.publish(&[(0, "event", level)], &[uri.as_str()]).await;
        let manifest = env.manifest().await;
        assert_eq!(manifest.entries.len(), 1);
        let blob = manifest.entries.values().next().unwrap().index.clone().unwrap();
        if let Some(old) = previous_blob {
            assert_ne!(blob, old, "replacement must have a distinct immutable identity");
            env.store.head(&Path::from(old)).await.expect("old snapshot can still read its blob");
        }
        previous_blob = Some(blob);
        assert_eq!(env.hits("level", "ERROR").await.unwrap().len(), usize::from(level == "ERROR"));
        assert_eq!(env.hits("level", "INFO").await.unwrap().len(), usize::from(level == "INFO"));
    }
    assert_eq!(env.search.stats.blob_fetches.load(Relaxed), 0, "both generations use their own seeded cache");
    let manifest = env.manifest().await;
    assert_eq!(manifest.retired_blobs.len(), 1, "replaced generation must remain tracked for collection");
    let retired = manifest.retired_blobs.keys().next().unwrap().clone();
    assert_eq!(env.gc(&[uri.as_str()]).await.blobs_deleted, 0, "recent snapshots retain the old generation");
    timefusion::tantivy::mutate(env.store.as_ref(), env.table, env.project, |manifest| {
        manifest.retired_blobs.values_mut().for_each(|at| *at = chrono::Utc::now() - chrono::Duration::days(2));
        ((), true)
    })
    .await
    .unwrap();
    assert_eq!(env.gc(&[uri.as_str()]).await.blobs_deleted, 1);
    assert!(matches!(env.store.head(&Path::from(retired)).await, Err(object_store::Error::NotFound { .. })));
    env.store.head(&Path::from(previous_blob.unwrap())).await.expect("current generation must remain readable");
    assert!(env.manifest().await.retired_blobs.is_empty());
}

/// Installing the same blob twice must converge on one readable dir.
#[tokio::test]
async fn concurrent_install_of_same_index_is_idempotent() {
    let env = Env::prod("otel_logs_and_spans", "p-race").seeded();
    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;

    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    assert_eq!(env.search.stats.cache_seed_failures.load(Relaxed), 0, "re-seeding an existing dir must not be an error");
    assert_eq!(env.hits("level", "ERROR").await.unwrap().len(), 1, "index stays readable after a repeat install");
}

/// A publish must leave the manifest cache warm AND current, never evict it.
#[tokio::test]
async fn publishing_keeps_the_manifest_cache_warm_and_current() {
    let store = Arc::new(FailAfterArm::new(Arc::new(InMemory::new())));
    let env = Env::new("otel_logs_and_spans", "p-manifest-warm", store.clone(), prod_defaults(), prod_defaults()).seeded();

    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    env.hits("level", "ERROR").await.unwrap();
    let loads_after_first = env.search.stats.manifest_loads.load(Relaxed);

    // Cutting the object store off makes a reload fail, so a passing search
    // proves the publish did not evict the cached manifest.
    env.publish(&[(2_000_000, "b", "ERROR")], &["f2"]).await;
    store.arm();

    let hits = env.search.search(env.table, env.project, "level", "ERROR").await.expect("must not reload the manifest").expect("usable index");
    assert_eq!(env.search.stats.manifest_loads.load(Relaxed), loads_after_first, "a publish must not force a manifest reload");
    let ids: Vec<_> = hits.iter().map(|h| h.id.as_str()).collect();
    assert!(ids.contains(&"b"), "the just-published entry must be visible without a reload, got {ids:?}");
}

/// N entries committed as one batch must leave exactly the manifest N
/// immediate upserts would: the batch merges, it never replaces.
#[tokio::test(flavor = "multi_thread")]
async fn a_batched_manifest_commit_lands_every_deferred_entry() {
    let env = Env::prod("otel_logs_and_spans", "p-batch").seeded();

    for (ts, id, uri) in [(1_000_000, "a", "f1"), (2_000_000, "b", "f2"), (3_000_000, "c", "f3")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }
    let m = env.manifest().await;
    assert_eq!(m.entries.len(), 3);

    // The batch must carry ONLY the new entries: a batch containing everything
    // would look correct even if the implementation cleared the manifest first.
    let existing: Vec<String> = m.entries.keys().cloned().collect();
    let template = m.entries.values().next().unwrap().clone();
    let fresh: Vec<_> = ["k-fresh-1", "k-fresh-2"].iter().map(|k| ((*k).to_string(), template.clone())).collect();
    timefusion::tantivy::upsert_manifest_many(env.store.as_ref(), env.table, env.project, fresh.clone()).await.unwrap();

    let after = env.manifest().await;
    assert_eq!(after.entries.len(), existing.len() + fresh.len(), "batch must ADD to the manifest, not replace it");
    for k in existing.iter().chain(fresh.iter().map(|(k, _)| k)) {
        assert!(after.entries.contains_key(k), "batch lost an entry: {k}");
    }
    // An empty batch must not rewrite the manifest at all.
    timefusion::tantivy::upsert_manifest_many(env.store.as_ref(), env.table, env.project, Vec::new()).await.unwrap();
    assert_eq!(env.manifest().await.entries.len(), after.entries.len());
}

/// The mirror of the test above: a GC MUST drop the cached manifest, or the
/// plan path routes at deleted blobs for up to a full TTL.
#[tokio::test(flavor = "multi_thread")]
async fn gc_after_compaction_drops_the_cached_manifest() {
    let env = Env::prod("otel_logs_and_spans", "p-manifest-gc").seeded();

    for (ts, id, uri) in [(1_000_000, "a", "f1"), (2_000_000, "b", "f2")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }
    let warm = env.hits("level", "ERROR").await.unwrap();
    assert_eq!(warm.len(), 2, "both publishes should be visible before the GC");
    let loads_before = env.search.stats.manifest_loads.load(Relaxed);

    assert_eq!(env.gc(&["f1"]).await.entries_removed, 1);

    let after = env.hits("level", "ERROR").await.unwrap();
    assert!(env.search.stats.manifest_loads.load(Relaxed) > loads_before, "GC must invalidate the cache so the next query reloads the pruned manifest");
    let ids: Vec<_> = after.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, ["a"], "the GC'd entry must not be consulted, got {ids:?}");
}

/// An object store that serves normally until `arm()`, then fails every GET,
/// so a test can assert "this read did not go to S3" positively.
#[derive(Debug)]
struct FailAfterArm {
    inner: Arc<dyn ObjectStore>,
    armed: std::sync::atomic::AtomicBool,
}

impl FailAfterArm {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner, armed: std::sync::atomic::AtomicBool::new(false) }
    }
    fn arm(&self) {
        self.armed.store(true, Relaxed);
    }
    fn check(&self) -> OsResult<()> {
        if self.armed.load(Relaxed) {
            return Err(object_store::Error::NotSupported { source: "object store is armed to fail".into() });
        }
        Ok(())
    }
}

impl std::fmt::Display for FailAfterArm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailAfterArm({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailAfterArm {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    /// `head()` also routes here, so arming this blocks every read shape the
    /// search path can use.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.check()?;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(&self, locations: BoxStream<'static, OsResult<Path>>) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.check()?;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn callback_skips_when_table_not_indexed() {
    // Tantivy is auto-on for any table whose schema declares `tantivy.indexed`
    // fields; a table with no schema at all must be a no-op.
    let env = Env::plain("no_such_table", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &[]).await;
    assert!(env.manifest().await.entries.is_empty(), "no manifest entry should be written for an unknown table");
}

#[tokio::test]
async fn search_falls_back_when_manifest_entry_marked_failed() {
    // An entry whose build failed (index=None, error=Some) must be skipped, and
    // `None` returned so the caller falls back to a full scan + UDF post-filter.
    let env = Env::plain("logs", "p1");
    upsert_manifest(env.store.as_ref(), "logs", "p1", "bucket-bad", ManifestEntry::failed("simulated build failure".into(), vec![])).await.unwrap();
    assert!(env.hits("level", "ERROR").await.is_none());
}

#[tokio::test]
async fn gc_after_compaction_clears_manifest_and_blobs() {
    let env = Env::zstd3("otel_logs_and_spans", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &["file_a"]).await;
    env.publish(&[(2_000_000, "b", "ERROR")], &["file_b"]).await;
    assert_eq!(env.manifest().await.entries.len(), 2);

    // file_a has been compacted away; only its entry should be dropped.
    let report = env.gc(&["file_b"]).await;
    assert_eq!(report.entries_removed, 1, "only one entry should be stale");
    assert_eq!(report.kept, 1, "the entry covering file_b should be kept");

    let m_after = env.manifest().await;
    assert_eq!(m_after.entries.len(), 1, "one entry should remain");
    let surviving = m_after.entries.values().next().unwrap();
    assert_eq!(surviving.covered_files, vec!["file_b".to_string()]);

    assert_eq!(env.gc(&[]).await.entries_removed, 1);
    assert!(env.manifest().await.entries.is_empty());
}

/// Carry-forward must match inputs whatever path form the caller uses: the
/// optimize path passes absolute URIs, the wave path Delta-relative `add.path`.
/// Compared raw they never match and the mechanism refuses every time.
#[tokio::test]
async fn carry_forward_matches_relative_and_absolute_paths_alike() {
    let env = Env::zstd3("otel_logs_and_spans", "p-relpaths");
    // Covered under an ABSOLUTE uri, as the flush/optimize paths record it.
    let abs = "s3://bucket/timefusion/default/otel_logs_and_spans/project_id=p-relpaths/date=2026-08-01/in.parquet";
    env.publish(&[(1_000_000, "a", "ERROR")], &[abs]).await;

    // Removed given RELATIVE, as `StagedBin::target_paths` holds it.
    let rel = "project_id=p-relpaths/date=2026-08-01/in.parquet";
    let applied = env.carry_forward(&[rel], &["out"]).await;
    assert!(applied, "a relative input path must match an absolute covered_files entry, or the wave path silently never carries forward");
    let m = env.manifest().await;
    assert!(m.entries.values().any(|e| e.covered_files.contains(&"out".to_string())), "the output must end up covered");
}

/// A compaction whose inputs were ALL covered leaves its output covered without
/// a build; one with an uncovered input must refuse, or the output holds rows no
/// index has seen while the read path trusts coverage to skip files.
#[tokio::test]
async fn carry_forward_covers_a_rewrite_only_when_every_input_was_covered() {
    let env = Env::zstd3("otel_logs_and_spans", "p-carry");
    for (ts, id, uri) in [(1_000_000, "a", "in_a"), (2_000_000, "b", "in_b")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }

    let refused = env.carry_forward(&["in_a", "never_indexed"], &["out"]).await;
    assert!(!refused, "an uncovered input means the output holds unseen rows — carrying forward would be a false negative");
    let m = env.manifest().await;
    assert!(m.entries.values().all(|e| !e.covered_files.contains(&"out".to_string())), "a refused carry-forward must not half-apply");

    let applied = env.carry_forward(&["in_a", "in_b"], &["out"]).await;
    assert!(applied, "every input was covered, so the output's rows are all already indexed");
    let m = env.manifest().await;
    let covering: Vec<_> = m.entries.values().filter(|e| e.covered_files.contains(&"out".to_string())).collect();
    assert_eq!(covering.len(), 2, "every entry covering an input must cover the output, or zero-hit pruning could drop rows it holds");
    assert!(covering.iter().all(|e| !e.ordinals_valid), "row ordinals are per-file positions; the output's are not the inputs'");
}

/// A multi-file entry must not take its live siblings down with it when one
/// covered file is compacted away. Keeping the entry is sound because the index
/// is a candidate generator, but row ordinals are per-file positions and must be
/// invalidated, or a pruned entry would re-enable them against the wrong file.
#[tokio::test]
async fn gc_keeps_a_multi_file_entry_for_its_surviving_files() {
    let env = Env::zstd3("otel_logs_and_spans", "p1");
    // Two added files in ONE commit => one entry covering both.
    env.publish(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR")], &["file_a", "file_b"]).await;

    assert_eq!(env.gc(&["file_b"]).await.entries_removed, 0, "file_b is still live, so its entry must survive");
    let m = env.manifest().await;
    let e = m.entries.values().next().expect("entry kept");
    assert_eq!(e.covered_files, vec!["file_b".to_string()], "the departed file must be pruned from covered_files");
    assert!(!e.ordinals_valid, "a pruned entry's ordinals no longer address its remaining file");

    assert_eq!(env.gc(&[]).await.entries_removed, 1, "an entry with no live covered file is stale");
    assert!(env.manifest().await.entries.is_empty());
}

#[tokio::test]
async fn search_time_prunes_non_overlapping_indexes() {
    // A query window overlapping only the OLD index must return just its hits
    // and never download the NEW blob; with no window, both are searched. Sound
    // because a pruned index only covers rows the timestamp filter excludes.
    let env = Env::zstd3("otel_logs_and_spans", "p1");

    let old_ts = 1_000_000_000i64; // ~16:40 1970
    let new_ts = 2_000_000_000_000i64; // ~2033 — far from the old window
    env.publish(&[(old_ts, "old1", "ERROR")], &["uri-old"]).await;
    env.publish(&[(new_ts, "new1", "ERROR")], &["uri-new"]).await;

    // Window around the OLD index only → prune the NEW one.
    let r = env
        .search
        .search_with_stats(env.table, env.project, &level_error_node(), 1000, Some((old_ts - 100, old_ts + 100)))
        .await
        .unwrap()
        .expect("old index overlaps → usable");
    assert_eq!(r.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(), vec!["old1".to_string()], "time-pruning must return only the overlapping index's hits");

    let r_all = env.search.search_with_stats(env.table, env.project, &level_error_node(), 1000, None).await.unwrap().unwrap();
    let mut all: Vec<String> = r_all.hits.iter().map(|h| h.id.clone()).collect();
    all.sort();
    assert_eq!(all, vec!["new1".to_string(), "old1".to_string()], "no range must search all indexes");
}

#[tokio::test]
async fn search_skips_indexes_that_dont_have_the_field() {
    // An index without the queried field must be skipped, not error.
    // `context___trace_state` has no `tantivy:` config, so no index is usable.
    let env = Env::zstd3("otel_logs_and_spans", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &["uri"]).await;

    assert!(env.hits("context___trace_state", "anything").await.is_none());
}

#[tokio::test]
async fn a_fat_needle_aborts_before_materializing_hits() {
    // The over-cap abort verdict must be reached by counting, not by
    // materializing O(total hits) and then throwing the work away.
    let env = Env::prod("otel_logs_and_spans", "p-fatneedle");
    let rows: Vec<(i64, String, &str)> = (0..50).map(|i| (1_000_000 + i as i64, format!("id-{i}"), "ERROR")).collect();
    let rows_ref: Vec<(i64, &str, &str)> = rows.iter().map(|(t, id, l)| (*t, id.as_str(), *l)).collect();
    env.publish(&rows_ref, &["fat-uri"]).await;

    let r = env.search.search_with_stats(env.table, env.project, &level_error_node(), 10, None).await.unwrap();
    assert!(r.is_none(), "an over-cap needle must abort the prefilter");
    let materialized = env.search.stats.hits_materialized.load(Relaxed);
    assert!(materialized <= 22, "abort must not materialize O(total hits); materialized {materialized} for cap 10");

    // A selective needle on the same index still completes untruncated.
    let node = timefusion::tantivy::udf::PredNode::Leaf(TextMatchPred { column: "id".into(), query: "id-7".into() });
    let r = env.search.search_with_stats(env.table, env.project, &node, 10, None).await.unwrap().expect("usable");
    assert_eq!(r.hits.len(), 1);
}
