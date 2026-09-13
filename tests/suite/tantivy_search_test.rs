//! Tier-3/4: end-to-end search service test (build via callback,
//! then query via search service). No Delta — we just verify the index
//! pipeline produces correct (timestamp, id) hits and that operational
//! failure paths behave correctly.

use std::sync::{Arc, atomic::Ordering::Relaxed};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use object_store::{ObjectStoreExt, memory::InMemory};
use tempfile::TempDir;
use timefusion::{
    config::TantivyConfig,
    schema::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy::{
        ManifestEntry, SCHEMA_VERSION, load_manifest,
        search::{TantivyIndexService, TantivySearchService},
        udf::TextMatchPred,
        upsert_manifest,
    },
};

/// `TantivyConfig::default()` is the DERIVED `Default` — it returns zeros and
/// `false`, bypassing every `#[serde_inline_default]` on the struct. Prod never
/// sees that: it deserializes the config, which applies the inline defaults. So
/// any test asserting on default-driven behaviour must deserialize too, or it
/// tests a configuration that cannot exist in production.
fn prod_defaults() -> TantivyConfig {
    serde_json::from_str("{}").expect("TantivyConfig has a default for every field")
}

fn level_error_node() -> timefusion::tantivy::udf::PredNode {
    timefusion::tantivy::udf::PredNode::Leaf(TextMatchPred { column: "level".into(), query: "ERROR".into() })
}

#[allow(dead_code)]
fn schema_with(level_indexed: bool) -> TableSchema {
    TableSchema {
        rollups: vec![],
        table_name: "logs".into(),
        partitions: vec![],
        sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: false, nulls_first: false }],
        z_order_columns: vec![],
        time_column: None,
        dedup_keys: vec![],
        dedup_tiebreak: None,
        tombstone_column: None,
        version_append: false,
        fields: vec![
            FieldDef {
                name: "timestamp".into(),
                data_type: "Timestamp(Microsecond, Some(\"UTC\"))".into(),
                nullable: false,
                tantivy: None,
                dictionary: None,
                bloom_filter: false,
                mutable: false,
            },
            FieldDef { name: "id".into(), data_type: "Utf8".into(), nullable: false, tantivy: None, dictionary: None, bloom_filter: false, mutable: false },
            FieldDef {
                name: "level".into(),
                data_type: "Utf8".into(),
                nullable: true,
                tantivy: level_indexed.then(|| TantivyFieldConfig {
                    indexed: true,
                    tokenizer: Some("raw".into()),
                    flatten: None,
                    list_mode: Default::default(),
                }),
                dictionary: None,
                bloom_filter: false,
                mutable: false,
            },
        ],
    }
}

fn batch(rows: &[(i64, &str, &str)]) -> RecordBatch {
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>()));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
        Field::new("hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
    ]));
    let hashes = arrow::array::new_null_array(schema.field(3).data_type(), rows.len());
    RecordBatch::try_new(schema, vec![ts, id, level, hashes]).unwrap()
}

/// Store + indexer + reader over one cache dir — the setup every scenario below
/// shares. Which config a test picks is SEMANTIC, not style: derived
/// `TantivyConfig::default()` is all-zeros/false while `prod_defaults()` is what
/// production deserializes (see `prod_defaults`), so the constructors keep them
/// apart and never substitute one for the other.
struct Env {
    table: &'static str,
    project: &'static str,
    store: Arc<dyn object_store::ObjectStore>,
    svc: Arc<TantivyIndexService>,
    search: Arc<TantivySearchService>,
    _cache: TempDir,
}

impl Env {
    fn new(table: &'static str, project: &'static str, store: Arc<dyn object_store::ObjectStore>, index_cfg: TantivyConfig, search_cfg: TantivyConfig) -> Self {
        let cache = TempDir::new().unwrap();
        Self {
            table,
            project,
            // Scratch lives under the per-Env TempDir, so concurrent tests cannot
            // collide and nothing is left behind.
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
        self.svc.clone().batch_callback()(self.project.to_string(), self.table.to_string(), vec![batch(rows)], uris.iter().map(|u| (*u).to_string()).collect())
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

    let mut table = schema_with(true);
    table.fields[2].data_type = "List(Utf8)".into();
    table.fields[2].tantivy.as_mut().unwrap().list_mode = timefusion::schema::TantivyListMode::Elements;
    let base = batch(&[(1, "one", "a"), (2, "two", "b"), (3, "three", "a")]);
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
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    upload(store.as_ref(), &object_store::path::Path::from("histogram"), blob).await?;
    let mut entry = ManifestEntry::failed("pending".into(), vec!["file".into()]);
    entry.index = Some("histogram".into());
    entry.error = None;
    entry.rows = stats.rows;
    entry.element_fields = stats.element_fields;
    entry.ordinals_valid = true;
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
    // Manually register the schema is tricky here because the schema_loader
    // pulls from compiled YAML. Use the otel_logs_and_spans table instead and
    // is configured for tantivy in the production YAML.
    let env = Env::zstd3("otel_logs_and_spans", "p1");

    // here are timestamp/id/level — the rest of the columns can be missing
    // because schema validation is on the Delta side, not tantivy.
    env.publish(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "INFO")], &["test-uri"]).await;

    let m = env.manifest().await;
    assert_eq!(m.entries.len(), 1);
    let entry = m.entries.values().next().unwrap();
    assert_eq!(entry.rows, 3);
    assert!(entry.index.is_some());
    assert_eq!(entry.min_timestamp_micros, Some(1_000_000));
    assert_eq!(entry.max_timestamp_micros, Some(3_000_000));

    // Search via TantivySearchService
    let hits = env.hits("level", "ERROR").await.expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
    assert_eq!(hits[0].timestamp_micros, 2_000_000);

    assert_eq!(hits, env.hits("level", "ERROR").await.unwrap());
}

#[tokio::test]
async fn multi_pred_and_is_single_pass_and_conjunctive() {
    // Two predicates run as ONE combined query per index: only the row
    // matching BOTH survives, and indexed_rows counts the index set once
    // (not once per predicate).
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
    // A flush commit that added exactly one parquet file must key the
    // manifest entry by the table-relative path and upload the blob at the
    // partition-mirrored location (suffix swap), while still covering the
    // ORIGINAL absolute URI for the coverage gate.
    let env = Env::plain("otel_logs_and_spans", "p-mirrored");
    let uri = format!("s3://bucket/timefusion/default/{}/project_id={}/date=2026-07-05/part-00000-abc-c000.zstd.parquet", env.table, env.project);
    let rel = format!("project_id={}/date=2026-07-05/part-00000-abc-c000.zstd.parquet", env.project);
    env.publish(&[(1_000_000, "a", "ERROR")], &[uri.as_str()]).await;

    let m = env.manifest().await;
    let entry = m.entries.get(&rel).expect("manifest keyed by table-relative parquet path");
    assert_eq!(entry.covered_files, vec![uri], "covered_files must keep the absolute URI");
    let blob = entry.index.as_ref().expect("index built");
    assert_eq!(timefusion::tantivy::index_to_parquet_rel(env.table, blob).as_deref(), Some(rel.as_str()), "generation must retain source identity");

    // And the read side must find + query it.
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

/// Seeding on publish must make the first query read local disk instead of S3.
///
/// Asserted through a deliberately hostile object store: after the index is
/// published, every subsequent GET fails. A search that still succeeds can only
/// have been served from the local extraction — which is exactly the property
/// ("never fetch from S3 what we already built locally") being claimed.
#[tokio::test]
async fn seeded_cache_serves_first_query_without_object_store_reads() {
    assert!(prod_defaults().seed_cache_on_publish(), "seeding is the default this test covers");
    let store = Arc::new(FailAfterArm::new(Arc::new(InMemory::new())));
    let env = Env::new("otel_logs_and_spans", "p-seeded", store.clone(), prod_defaults(), prod_defaults()).seeded();

    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    assert_eq!(env.search.stats.cache_seeded.load(Relaxed), 1, "publish must seed the reader's extracted-index cache");
    assert_eq!(env.search.stats.cache_seed_failures.load(Relaxed), 0);

    // The manifest was read once during publish; keep it cached, then cut S3 off.
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
            env.store.head(&object_store::path::Path::from(old)).await.expect("old snapshot can still read its blob");
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
    assert!(matches!(env.store.head(&object_store::path::Path::from(retired)).await, Err(object_store::Error::NotFound { .. })));
    env.store.head(&object_store::path::Path::from(previous_blob.unwrap())).await.expect("current generation must remain readable");
    assert!(env.manifest().await.retired_blobs.is_empty());
}

/// Installing the same blob twice — the indexer seeding while a query
/// downloads — must converge on one readable dir, not error or corrupt.
#[tokio::test]
async fn concurrent_install_of_same_index_is_idempotent() {
    let env = Env::prod("otel_logs_and_spans", "p-race").seeded();
    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;

    // Publish again over the top: same key, same blob path, dir already present.
    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    assert_eq!(env.search.stats.cache_seed_failures.load(Relaxed), 0, "re-seeding an existing dir must not be an error");
    assert_eq!(env.hits("level", "ERROR").await.unwrap().len(), 1, "index stays readable after a repeat install");
}

/// A publish must leave the manifest cache WARM, not empty.
///
/// Regression guard for a measured prod defect: the first version of this path
/// called `remove()` on publish, and prod reported `manifest_hit_pct = 0.0`
/// across 56 loads for 54 queries — busy projects publish far more often than
/// they are queried, so every publish threw away the entry the next query
/// needed and the 300s TTL bought nothing.
#[tokio::test]
async fn publishing_keeps_the_manifest_cache_warm_and_current() {
    let store = Arc::new(FailAfterArm::new(Arc::new(InMemory::new())));
    let env = Env::new("otel_logs_and_spans", "p-manifest-warm", store.clone(), prod_defaults(), prod_defaults()).seeded();

    // First publish + query: populates the manifest cache.
    env.publish(&[(1_000_000, "a", "ERROR")], &["f1"]).await;
    env.hits("level", "ERROR").await.unwrap();
    let loads_after_first = env.search.stats.manifest_loads.load(Relaxed);

    // Second publish, then cut the object store off entirely. If publishing had
    // evicted the manifest, this search would have to reload it and would fail.
    env.publish(&[(2_000_000, "b", "ERROR")], &["f2"]).await;
    store.arm();

    let hits = env.search.search(env.table, env.project, "level", "ERROR").await.expect("must not reload the manifest").expect("usable index");
    assert_eq!(env.search.stats.manifest_loads.load(Relaxed), loads_after_first, "a publish must not force a manifest reload");
    // ...and the cache must be CURRENT, not merely warm: the second publish's
    // rows have to be visible, which is the thing a stale cache would lose.
    let ids: Vec<_> = hits.iter().map(|h| h.id.as_str()).collect();
    assert!(ids.contains(&"b"), "the just-published entry must be visible without a reload, got {ids:?}");
}

/// Batched manifest commits must be indistinguishable from per-build ones.
/// The backfill defers its manifest writes because each is a full
/// read-modify-write of the whole manifest under a per-project lock — the
/// measured cause of coverage not converging — so what matters is that N
/// deferred entries committed together leave exactly the manifest N immediate
/// upserts would have, and that a counting error (last-write-wins on a shared
/// snapshot) would show up as missing entries.
#[tokio::test(flavor = "multi_thread")]
async fn a_batched_manifest_commit_lands_every_deferred_entry() {
    let env = Env::prod("otel_logs_and_spans", "p-batch").seeded();

    // Three files, published normally, give us real blobs to point entries at.
    for (ts, id, uri) in [(1_000_000, "a", "f1"), (2_000_000, "b", "f2"), (3_000_000, "c", "f3")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }
    let m = env.manifest().await;
    assert_eq!(m.entries.len(), 3);

    // Re-commit all three as ONE batch, plus a fresh key: the batch must add
    // the new entry and preserve the existing ones, not replace the manifest.
    // The batch carries ONLY the new entries — never the existing ones. That is
    // what makes merge-vs-replace observable: a batch containing everything
    // would still leave the manifest correct if the implementation cleared it
    // first, and an earlier version of this test passed against exactly that
    // injected bug. The keys are bucket/parquet-rel, not covered-file URIs.
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

/// The mirror image of the test above, and the reason both are needed: a
/// publish must NOT drop the cached manifest, but a GC MUST. GC prunes entries
/// on S3 and deletes their blobs; leaving this process's copy cached would keep
/// the plan path routing at a blob that no longer exists for up to a full TTL.
#[tokio::test(flavor = "multi_thread")]
async fn gc_after_compaction_drops_the_cached_manifest() {
    let env = Env::prod("otel_logs_and_spans", "p-manifest-gc").seeded();

    for (ts, id, uri) in [(1_000_000, "a", "f1"), (2_000_000, "b", "f2")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }
    let warm = env.hits("level", "ERROR").await.unwrap();
    assert_eq!(warm.len(), 2, "both publishes should be visible before the GC");
    let loads_before = env.search.stats.manifest_loads.load(Relaxed);

    // f2 is no longer live — compaction rewrote it away.
    assert_eq!(env.gc(&["f1"]).await.entries_removed, 1);

    let after = env.hits("level", "ERROR").await.unwrap();
    assert!(env.search.stats.manifest_loads.load(Relaxed) > loads_before, "GC must invalidate the cache so the next query reloads the pruned manifest");
    let ids: Vec<_> = after.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, ["a"], "the GC'd entry must not be consulted, got {ids:?}");
}

/// An object store that serves normally until `arm()`, then fails every GET.
/// Lets a test assert "this read did not go to S3" positively, rather than by
/// inferring it from a counter that a future refactor could stop incrementing.
#[derive(Debug)]
struct FailAfterArm {
    inner: Arc<dyn object_store::ObjectStore>,
    armed: std::sync::atomic::AtomicBool,
}

impl FailAfterArm {
    fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner, armed: std::sync::atomic::AtomicBool::new(false) }
    }
    fn arm(&self) {
        self.armed.store(true, Relaxed);
    }
    fn check(&self) -> object_store::Result<()> {
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
impl object_store::ObjectStore for FailAfterArm {
    async fn put_opts(
        &self, location: &object_store::path::Path, payload: object_store::PutPayload, opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self, location: &object_store::path::Path, opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    /// The one method that matters: `head()` also routes here, so arming this
    /// blocks every read shape the search path can use.
    async fn get_opts(&self, location: &object_store::path::Path, options: object_store::GetOptions) -> object_store::Result<object_store::GetResult> {
        self.check()?;
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self, locations: futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&object_store::path::Path>) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&object_store::path::Path>) -> object_store::Result<object_store::ListResult> {
        self.check()?;
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &object_store::path::Path, to: &object_store::path::Path, options: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn callback_skips_when_table_not_indexed() {
    // Tantivy is now auto-on for any table whose schema declares
    // `tantivy.indexed: true` fields. Pass a synthetic table name with
    // no schema and no override-list match — callback must be a no-op.
    let env = Env::plain("no_such_table", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &[]).await;
    assert!(env.manifest().await.entries.is_empty(), "no manifest entry should be written for an unknown table");
}

#[tokio::test]
async fn search_falls_back_when_manifest_entry_marked_failed() {
    // Simulate an entry whose build failed: index=None, error=Some.
    // search() must skip it and return zero hits (no panic).
    let env = Env::plain("logs", "p1");
    upsert_manifest(
        env.store.as_ref(),
        "logs",
        "p1",
        "bucket-bad",
        ManifestEntry {
            element_fields: Default::default(),
            index: None,
            rows: 0,
            built_at: chrono::Utc::now(),
            schema_version: SCHEMA_VERSION,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error: Some("simulated build failure".into()),
            covered_files: vec![],
            ordinals_valid: false,
        },
    )
    .await
    .unwrap();
    // the caller falls back to full scan + UDF post-filter.
    assert!(env.hits("level", "ERROR").await.is_none());
}

#[tokio::test]
async fn gc_after_compaction_clears_manifest_and_blobs() {
    let env = Env::zstd3("otel_logs_and_spans", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &["file_a"]).await;
    env.publish(&[(2_000_000, "b", "ERROR")], &["file_b"]).await;
    assert_eq!(env.manifest().await.entries.len(), 2);

    // Compaction has rewritten file_a away but file_b survives. Only the
    // entry covering file_a should be dropped.
    let report = env.gc(&["file_b"]).await;
    assert_eq!(report.entries_removed, 1, "only one entry should be stale");
    assert_eq!(report.kept, 1, "the entry covering file_b should be kept");

    let m_after = env.manifest().await;
    assert_eq!(m_after.entries.len(), 1, "one entry should remain");
    let surviving = m_after.entries.values().next().unwrap();
    assert_eq!(surviving.covered_files, vec!["file_b".to_string()]);

    // Calling GC with no live URIs should drop the remaining entry.
    assert_eq!(env.gc(&[]).await.entries_removed, 1);
    assert!(env.manifest().await.entries.is_empty());
}

/// Carry-forward must match inputs whatever PATH FORM the caller uses. The
/// optimize path passes absolute URIs (`get_file_uris`); the wave path passes
/// Delta-relative `add.path`. Comparing them raw never matches, so the whole
/// mechanism would refuse every time — implemented and inert, which is this
/// subsystem's recurring failure mode.
#[tokio::test]
async fn carry_forward_matches_relative_and_absolute_paths_alike() {
    let env = Env::zstd3("otel_logs_and_spans", "p-relpaths");
    // Covered under an ABSOLUTE uri, exactly as the flush/optimize paths record it.
    let abs = "s3://bucket/timefusion/default/otel_logs_and_spans/project_id=p-relpaths/date=2026-08-01/in.parquet";
    env.publish(&[(1_000_000, "a", "ERROR")], &[abs]).await;

    // Removed given RELATIVE, as `StagedBin::target_paths` holds it.
    let rel = "project_id=p-relpaths/date=2026-08-01/in.parquet";
    let applied = env.carry_forward(&[rel], &["out"]).await;
    assert!(applied, "a relative input path must match an absolute covered_files entry, or the wave path silently never carries forward");
    let m = env.manifest().await;
    assert!(m.entries.values().any(|e| e.covered_files.contains(&"out".to_string())), "the output must end up covered");
}

/// Carry-forward: a compaction whose inputs were ALL covered leaves its output
/// covered without a single build — and one whose inputs were not must refuse,
/// because the output would then hold rows no index has seen and the read path
/// trusts coverage to skip files. The refusal is the correctness half; the
/// no-build is the throughput half (builds run ~4/hr on prod).
#[tokio::test]
async fn carry_forward_covers_a_rewrite_only_when_every_input_was_covered() {
    let env = Env::zstd3("otel_logs_and_spans", "p-carry");
    for (ts, id, uri) in [(1_000_000, "a", "in_a"), (2_000_000, "b", "in_b")] {
        env.publish(&[(ts, id, "ERROR")], &[uri]).await;
    }

    // An input nobody indexed => refuse, and change nothing.
    let refused = env.carry_forward(&["in_a", "never_indexed"], &["out"]).await;
    assert!(!refused, "an uncovered input means the output holds unseen rows — carrying forward would be a false negative");
    let m = env.manifest().await;
    assert!(m.entries.values().all(|e| !e.covered_files.contains(&"out".to_string())), "a refused carry-forward must not half-apply");

    // Both inputs covered => the output is covered, by edit not by build.
    let applied = env.carry_forward(&["in_a", "in_b"], &["out"]).await;
    assert!(applied, "every input was covered, so the output's rows are all already indexed");
    let m = env.manifest().await;
    let covering: Vec<_> = m.entries.values().filter(|e| e.covered_files.contains(&"out".to_string())).collect();
    assert_eq!(covering.len(), 2, "every entry covering an input must cover the output, or zero-hit pruning could drop rows it holds");
    assert!(covering.iter().all(|e| !e.ordinals_valid), "row ordinals are per-file positions; the output's are not the inputs'");
}

/// A multi-file entry must not take its live siblings down with it. A flush
/// commit that adds more than one file publishes ONE entry covering all of
/// them (`bucket-{uuid}`); compacting away a single member used to drop the
/// whole entry, un-covering files nothing had touched. That collateral is
/// proportional to compaction rate and is a standing source of the coverage
/// divergence measured 2026-08-22 (~60 uncovered files/hr with every rewrite
/// path reindexing its own output successfully).
///
/// Keeping the entry is sound because the index is a candidate generator:
/// hits for the departed file's rows are false positives the scan filters, and
/// `zero_hit` pruning only ever gets more conservative. Row ORDINALS are not
/// sound across the change — they are per-file positions, and pruning a
/// two-file entry down to one would make `covered_files.len() == 1` re-enable
/// them against the wrong file — so the survivor gives them up.
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

    // Last member gone => nothing left to cover, entry and blob go.
    assert_eq!(env.gc(&[]).await.entries_removed, 1, "an entry with no live covered file is stale");
    assert!(env.manifest().await.entries.is_empty());
}

#[tokio::test]
async fn search_time_prunes_non_overlapping_indexes() {
    // Two indexes in disjoint time windows. A query whose window overlaps only
    // the OLD one must return only its hits (and never download the NEW blob) —
    // this is the fix for the cold-old-data latency cliff. With no window, both
    // are searched (today's behavior). Correctness: a pruned index only covers
    // rows outside the window, which the query's timestamp filter excludes.
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

    // No range → both indexes searched (unchanged behavior).
    let r_all = env.search.search_with_stats(env.table, env.project, &level_error_node(), 1000, None).await.unwrap().unwrap();
    let mut all: Vec<String> = r_all.hits.iter().map(|h| h.id.clone()).collect();
    all.sort();
    assert_eq!(all, vec!["new1".to_string(), "old1".to_string()], "no range must search all indexes");
}

#[tokio::test]
async fn search_skips_indexes_that_dont_have_the_field() {
    // An older index won't have a newly-added field. search() must not error;
    // it should simply skip those indexes and return hits from the others.
    let env = Env::zstd3("otel_logs_and_spans", "p1");
    env.publish(&[(1_000_000, "a", "INFO")], &["uri"]).await;

    // Querying a field that isn't tantivy-indexed (context___trace_state has no
    // `tantivy:` config) yields no usable index → None. NB: parent_id/id/trace_id
    // ARE indexed now (P0 equality routing), so this uses a still-unindexed field.
    assert!(env.hits("context___trace_state", "anything").await.is_none());
}

#[tokio::test]
async fn a_fat_needle_aborts_before_materializing_hits() {
    // Prod 2026-08-22 (P5/P4 anomaly): a needle matching ~4.5M of 4.9M rows
    // cost 4-6s of UNCACHEABLE plan time per query because search_with_stats
    // materialized whole per-index hit vectors (TopDocs cap 1M, doc-store read
    // per hit) before the cumulative max_hits abort threw the work away. The
    // abort verdict must be reached by counting, not by materializing O(hits).
    let env = Env::prod("otel_logs_and_spans", "p-fatneedle");
    let rows: Vec<(i64, String, &str)> = (0..50).map(|i| (1_000_000 + i as i64, format!("id-{i}"), "ERROR")).collect();
    let rows_ref: Vec<(i64, &str, &str)> = rows.iter().map(|(t, id, l)| (*t, id.as_str(), *l)).collect();
    env.publish(&rows_ref, &["fat-uri"]).await;

    let r = env.search.search_with_stats(env.table, env.project, &level_error_node(), 10, None).await.unwrap();
    assert!(r.is_none(), "an over-cap needle must abort the prefilter");
    let materialized = env.search.stats.hits_materialized.load(Relaxed);
    assert!(materialized <= 22, "abort must not materialize O(total hits); materialized {materialized} for cap 10");

    // And a selective needle on the same index still completes untruncated.
    let node = timefusion::tantivy::udf::PredNode::Leaf(TextMatchPred { column: "id".into(), query: "id-7".into() });
    let r = env.search.search_with_stats(env.table, env.project, &node, 10, None).await.unwrap().expect("usable");
    assert_eq!(r.hits.len(), 1);
}
