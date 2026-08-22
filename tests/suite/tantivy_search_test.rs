//! Tier-3/4: end-to-end search service test (build via callback,
//! then query via search service). No Delta — we just verify the index
//! pipeline produces correct (timestamp, id) hits and that operational
//! failure paths behave correctly.

use std::sync::{Arc, atomic::Ordering::Relaxed};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use object_store::memory::InMemory;
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
                tantivy: level_indexed.then(|| TantivyFieldConfig { indexed: true, tokenizer: Some("raw".into()), flatten: None }),
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
    ]));
    RecordBatch::try_new(schema, vec![ts, id, level]).unwrap()
}

#[tokio::test]
async fn callback_builds_index_and_search_returns_hits() {
    // Manually register the schema is tricky here because the schema_loader
    // pulls from compiled YAML. Use the otel_logs_and_spans table instead and
    // is configured for tantivy in the production YAML.
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.clone().callback();

    // here are timestamp/id/level — the rest of the columns can be missing
    // because schema validation is on the Delta side, not tantivy.
    let b = batch(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "INFO")]);
    cb(project_id.to_string(), table_name.to_string(), vec![b], vec!["test-uri".into()]).await.expect("callback");

    let m = load_manifest(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m.entries.len(), 1);
    let entry = m.entries.values().next().unwrap();
    assert_eq!(entry.rows, 3);
    assert!(entry.index.is_some());
    assert_eq!(entry.min_timestamp_micros, Some(1_000_000));
    assert_eq!(entry.max_timestamp_micros, Some(3_000_000));

    // Search via TantivySearchService
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store.clone(), cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    let hits = search.search(table_name, project_id, "level", "ERROR").await.expect("search").expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
    assert_eq!(hits[0].timestamp_micros, 2_000_000);

    let hits2 = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert_eq!(hits, hits2);
}

#[tokio::test]
async fn multi_pred_and_is_single_pass_and_conjunctive() {
    // Two predicates run as ONE combined query per index: only the row
    // matching BOTH survives, and indexed_rows counts the index set once
    // (not once per predicate).
    let table_name = "otel_logs_and_spans";
    let project_id = "p-multipred";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(TantivyConfig::default())));
    let cb = svc.callback();
    let b = batch(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "ERROR")]);
    cb(project_id.to_string(), table_name.to_string(), vec![b], vec!["f1".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    let preds = vec![TextMatchPred { column: "level".into(), query: "ERROR".into() }, TextMatchPred { column: "id".into(), query: "c".into() }];
    let node = timefusion::tantivy::udf::PredNode::from_preds(&preds).expect("non-empty");
    let r = search.search_with_stats(table_name, project_id, &node, 1000, None).await.unwrap().expect("usable");
    assert_eq!(r.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(), vec!["c".to_string()]);
    assert_eq!(r.indexed_rows, 3, "denominator must count the index set once, not per predicate");
}

#[tokio::test]
async fn single_file_flush_publishes_partition_mirrored_blob() {
    // A flush commit that added exactly one parquet file must key the
    // manifest entry by the table-relative path and upload the blob at the
    // partition-mirrored location (suffix swap), while still covering the
    // ORIGINAL absolute URI for the coverage gate.
    let table_name = "otel_logs_and_spans";
    let project_id = "p-mirrored";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(TantivyConfig::default())));
    let cb = svc.callback();
    let uri = format!("s3://bucket/timefusion/default/{table_name}/project_id={project_id}/date=2026-07-05/part-00000-abc-c000.zstd.parquet");
    let rel = format!("project_id={project_id}/date=2026-07-05/part-00000-abc-c000.zstd.parquet");
    let b = batch(&[(1_000_000, "a", "ERROR")]);
    cb(project_id.to_string(), table_name.to_string(), vec![b], vec![uri.clone()]).await.unwrap();

    let m = load_manifest(store.as_ref(), table_name, project_id).await.unwrap();
    let entry = m.entries.get(&rel).expect("manifest keyed by table-relative parquet path");
    assert_eq!(entry.covered_files, vec![uri], "covered_files must keep the absolute URI");
    let blob = entry.index.as_ref().expect("index built");
    assert_eq!(blob, &timefusion::tantivy::index_path_for_parquet(table_name, &rel).to_string(), "blob must live at the partition-mirrored path");

    // And the read side must find + query it.
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    let hits = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert_eq!(hits.len(), 1);
}

#[tokio::test]
async fn reader_cache_avoids_reopen_across_queries() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p-readercache";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(TantivyConfig::default())));
    let cb = svc.callback();
    let b = batch(&[(1_000_000, "a", "ERROR")]);
    cb(project_id.to_string(), table_name.to_string(), vec![b], vec!["f1".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    for _ in 0..3 {
        let hits = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
        assert_eq!(hits.len(), 1);
    }
    assert_eq!(search.stats.index_opens.load(Relaxed), 1, "one cold open; subsequent queries must hit the reader LRU");
}

/// Seeding on publish must make the first query read local disk instead of S3.
///
/// Asserted through a deliberately hostile object store: after the index is
/// published, every subsequent GET fails. A search that still succeeds can only
/// have been served from the local extraction — which is exactly the property
/// ("never fetch from S3 what we already built locally") being claimed.
#[tokio::test]
async fn seeded_cache_serves_first_query_without_object_store_reads() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p-seeded";
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FailAfterArm::new(inner));
    let cfg = Arc::new(prod_defaults());
    assert!(cfg.seed_cache_on_publish(), "seeding is the default this test covers");

    let cache = TempDir::new().unwrap();
    let svc = Arc::new(TantivyIndexService::new(store.clone(), cfg.clone()));
    let search = Arc::new(TantivySearchService::new(store.clone(), cache.path().to_path_buf(), cfg));
    svc.with_reader(&search);

    let cb = svc.clone().callback();
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(1_000_000, "a", "ERROR")])], vec!["f1".into()]).await.unwrap();
    assert_eq!(search.stats.cache_seeded.load(Relaxed), 1, "publish must seed the reader's extracted-index cache");
    assert_eq!(search.stats.cache_seed_failures.load(Relaxed), 0);

    // The manifest was read once during publish; keep it cached, then cut S3 off.
    search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    let fetches_before = search.stats.blob_fetches.load(Relaxed);
    store.arm();

    let hits = search.search(table_name, project_id, "level", "ERROR").await.expect("search must not touch S3").expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(search.stats.blob_fetches.load(Relaxed), fetches_before, "a seeded index must never be re-downloaded");
    assert_eq!(fetches_before, 0, "the very first query must already be served locally");
}

/// Installing the same blob twice — the indexer seeding while a query
/// downloads — must converge on one readable dir, not error or corrupt.
#[tokio::test]
async fn concurrent_install_of_same_index_is_idempotent() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p-race";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = Arc::new(prod_defaults());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), cfg.clone()));
    let cache = TempDir::new().unwrap();
    let search = Arc::new(TantivySearchService::new(store.clone(), cache.path().to_path_buf(), cfg));
    svc.with_reader(&search);

    let cb = svc.clone().callback();
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(1_000_000, "a", "ERROR")])], vec!["f1".into()]).await.unwrap();

    // Publish again over the top: same key, same blob path, dir already present.
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(1_000_000, "a", "ERROR")])], vec!["f1".into()]).await.unwrap();
    assert_eq!(search.stats.cache_seed_failures.load(Relaxed), 0, "re-seeding an existing dir must not be an error");
    let hits = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert_eq!(hits.len(), 1, "index stays readable after a repeat install");
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
    let table_name = "otel_logs_and_spans";
    let project_id = "p-manifest-warm";
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FailAfterArm::new(inner));
    let cfg = Arc::new(prod_defaults());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), cfg.clone()));
    let cache = TempDir::new().unwrap();
    let search = Arc::new(TantivySearchService::new(store.clone(), cache.path().to_path_buf(), cfg));
    svc.with_reader(&search);
    let cb = svc.clone().callback();

    // First publish + query: populates the manifest cache.
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(1_000_000, "a", "ERROR")])], vec!["f1".into()]).await.unwrap();
    search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    let loads_after_first = search.stats.manifest_loads.load(Relaxed);

    // Second publish, then cut the object store off entirely. If publishing had
    // evicted the manifest, this search would have to reload it and would fail.
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(2_000_000, "b", "ERROR")])], vec!["f2".into()]).await.unwrap();
    store.arm();

    let hits = search.search(table_name, project_id, "level", "ERROR").await.expect("must not reload the manifest").expect("usable index");
    assert_eq!(search.stats.manifest_loads.load(Relaxed), loads_after_first, "a publish must not force a manifest reload");
    // ...and the cache must be CURRENT, not merely warm: the second publish's
    // rows have to be visible, which is the thing a stale cache would lose.
    let ids: Vec<_> = hits.iter().map(|h| h.id.as_str()).collect();
    assert!(ids.contains(&"b"), "the just-published entry must be visible without a reload, got {ids:?}");
}

/// The mirror image of the test above, and the reason both are needed: a
/// publish must NOT drop the cached manifest, but a GC MUST. GC prunes entries
/// on S3 and deletes their blobs; leaving this process's copy cached would keep
/// the plan path routing at a blob that no longer exists for up to a full TTL.
#[tokio::test(flavor = "multi_thread")]
async fn gc_after_compaction_drops_the_cached_manifest() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p-manifest-gc";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = Arc::new(prod_defaults());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), cfg.clone()));
    let cache = TempDir::new().unwrap();
    let search = Arc::new(TantivySearchService::new(store.clone(), cache.path().to_path_buf(), cfg));
    svc.with_reader(&search);
    let cb = svc.clone().callback();

    for (ts, id, uri) in [(1_000_000, "a", "f1"), (2_000_000, "b", "f2")] {
        cb(project_id.to_string(), table_name.to_string(), vec![batch(&[(ts, id, "ERROR")])], vec![uri.into()]).await.unwrap();
    }
    let warm = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert_eq!(warm.len(), 2, "both publishes should be visible before the GC");
    let loads_before = search.stats.manifest_loads.load(Relaxed);

    // f2 is no longer live — compaction rewrote it away.
    let report = svc.gc_after_compaction(table_name, project_id, &["f1".to_string()]).await.unwrap();
    assert_eq!(report.entries_removed, 1);

    let after = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert!(search.stats.manifest_loads.load(Relaxed) > loads_before, "GC must invalidate the cache so the next query reloads the pruned manifest");
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
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig::default();
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.callback();
    let b = batch(&[(1_000_000, "a", "INFO")]);
    cb("p1".into(), "no_such_table".into(), vec![b], vec![]).await.expect("noop callback");
    let m = load_manifest(store.as_ref(), "no_such_table", "p1").await.unwrap();
    assert!(m.entries.is_empty(), "no manifest entry should be written for an unknown table");
}

#[tokio::test]
async fn search_falls_back_when_manifest_entry_marked_failed() {
    // Simulate an entry whose build failed: index=None, error=Some.
    // search() must skip it and return zero hits (no panic).
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    upsert_manifest(
        store.as_ref(),
        "logs",
        "p1",
        "bucket-bad",
        ManifestEntry {
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
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    // the caller falls back to full scan + UDF post-filter.
    let hits = search.search("logs", "p1", "level", "ERROR").await.unwrap();
    assert!(hits.is_none());
}

#[tokio::test]
async fn gc_after_compaction_clears_manifest_and_blobs() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.clone().callback();
    cb(project_id.into(), table_name.into(), vec![batch(&[(1_000_000, "a", "INFO")])], vec!["file_a".into()]).await.unwrap();
    cb(project_id.into(), table_name.into(), vec![batch(&[(2_000_000, "b", "ERROR")])], vec!["file_b".into()]).await.unwrap();
    let m_before = load_manifest(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m_before.entries.len(), 2);

    // Compaction has rewritten file_a away but file_b survives. Only the
    // entry covering file_a should be dropped.
    let report = svc.gc_after_compaction(table_name, project_id, &["file_b".to_string()]).await.unwrap();
    assert_eq!(report.entries_removed, 1, "only one entry should be stale");
    assert_eq!(report.kept, 1, "the entry covering file_b should be kept");

    let m_after = load_manifest(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m_after.entries.len(), 1, "one entry should remain");
    let surviving = m_after.entries.values().next().unwrap();
    assert_eq!(surviving.covered_files, vec!["file_b".to_string()]);

    // Calling GC with no live URIs should drop the remaining entry.
    let report2 = svc.gc_after_compaction(table_name, project_id, &[]).await.unwrap();
    assert_eq!(report2.entries_removed, 1);
    let m_final = load_manifest(store.as_ref(), table_name, project_id).await.unwrap();
    assert!(m_final.entries.is_empty());
}

#[tokio::test]
async fn search_time_prunes_non_overlapping_indexes() {
    // Two indexes in disjoint time windows. A query whose window overlaps only
    // the OLD one must return only its hits (and never download the NEW blob) —
    // this is the fix for the cold-old-data latency cliff. With no window, both
    // are searched (today's behavior). Correctness: a pruned index only covers
    // rows outside the window, which the query's timestamp filter excludes.
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.callback();

    let old_ts = 1_000_000_000i64; // ~16:40 1970
    let new_ts = 2_000_000_000_000i64; // ~2033 — far from the old window
    cb(project_id.into(), table_name.into(), vec![batch(&[(old_ts, "old1", "ERROR")])], vec!["uri-old".into()]).await.unwrap();
    cb(project_id.into(), table_name.into(), vec![batch(&[(new_ts, "new1", "ERROR")])], vec!["uri-new".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));

    // Window around the OLD index only → prune the NEW one.
    let r = search
        .search_with_stats(table_name, project_id, &level_error_node(), 1000, Some((old_ts - 100, old_ts + 100)))
        .await
        .unwrap()
        .expect("old index overlaps → usable");
    assert_eq!(r.hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>(), vec!["old1".to_string()], "time-pruning must return only the overlapping index's hits");

    // No range → both indexes searched (unchanged behavior).
    let r_all = search.search_with_stats(table_name, project_id, &level_error_node(), 1000, None).await.unwrap().unwrap();
    let mut all: Vec<String> = r_all.hits.iter().map(|h| h.id.clone()).collect();
    all.sort();
    assert_eq!(all, vec!["new1".to_string(), "old1".to_string()], "no range must search all indexes");
}

#[tokio::test]
async fn search_skips_indexes_that_dont_have_the_field() {
    // An older index won't have a newly-added field. search() must not error;
    // it should simply skip those indexes and return hits from the others.
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig { timefusion_tantivy_compression_level: 3, ..Default::default() };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.callback();
    let b = batch(&[(1_000_000, "a", "INFO")]);
    cb(project_id.into(), table_name.into(), vec![b], vec!["uri".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(TantivyConfig::default()));
    // Querying a field that isn't tantivy-indexed (context___trace_state has no
    // `tantivy:` config) yields no usable index → None. NB: parent_id/id/trace_id
    // ARE indexed now (P0 equality routing), so this uses a still-unindexed field.
    let hits = search.search(table_name, project_id, "context___trace_state", "anything").await.unwrap();
    assert!(hits.is_none());
}

#[tokio::test]
async fn a_fat_needle_aborts_before_materializing_hits() {
    // Prod 2026-08-22 (P5/P4 anomaly): a needle matching ~4.5M of 4.9M rows
    // cost 4-6s of UNCACHEABLE plan time per query because search_with_stats
    // materialized whole per-index hit vectors (TopDocs cap 1M, doc-store read
    // per hit) before the cumulative max_hits abort threw the work away. The
    // abort verdict must be reached by counting, not by materializing O(hits).
    let table_name = "otel_logs_and_spans";
    let project_id = "p-fatneedle";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(prod_defaults())));
    let cb = svc.callback();
    let rows: Vec<(i64, String, &str)> = (0..50).map(|i| (1_000_000 + i as i64, format!("id-{i}"), "ERROR")).collect();
    let rows_ref: Vec<(i64, &str, &str)> = rows.iter().map(|(t, id, l)| (*t, id.as_str(), *l)).collect();
    cb(project_id.to_string(), table_name.to_string(), vec![batch(&rows_ref)], vec!["fat-uri".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf(), Arc::new(prod_defaults()));
    let r = search.search_with_stats(table_name, project_id, &level_error_node(), 10, None).await.unwrap();
    assert!(r.is_none(), "an over-cap needle must abort the prefilter");
    let materialized = search.stats.hits_materialized.load(Relaxed);
    assert!(materialized <= 22, "abort must not materialize O(total hits); materialized {materialized} for cap 10");

    // And a selective needle on the same index still completes untruncated.
    let node = timefusion::tantivy::udf::PredNode::Leaf(TextMatchPred { column: "id".into(), query: "id-7".into() });
    let r = search.search_with_stats(table_name, project_id, &node, 10, None).await.unwrap().expect("usable");
    assert_eq!(r.hits.len(), 1);
}
