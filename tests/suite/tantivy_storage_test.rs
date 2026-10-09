//! Tantivy blob storage roundtrip + manifest tests over `object_store::InMemory`.

use std::sync::Arc;

use arrow::array::RecordBatch;
use object_store::memory::InMemory;
use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};
use tempfile::TempDir;
use timefusion::tantivy::{
    IndexBuildStats, ManifestEntry, MergeMode, SCHEMA_VERSION, build_for_table, delete, download, load_manifest, remove_manifest_entries,
    search::{Hit, query_index},
    unpack_to_dir, upload, upsert_manifest, verify_blob,
};

use super::tantivy_search_test::{logs_batch, logs_schema};

/// Manifest entry with all other fields at `Default` (`schema_version` = `SCHEMA_VERSION`).
fn entry(index: Option<&str>, rows: u64, error: Option<&str>) -> ManifestEntry {
    ManifestEntry { index: index.map(Into::into), rows, error: error.map(Into::into), ..Default::default() }
}

fn batch() -> RecordBatch {
    logs_batch(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "INFO")], false)
}

#[tokio::test]
async fn pack_upload_download_unpack_query_roundtrip() {
    let table = logs_schema();
    let batches = vec![batch()];

    let (blob, stats): (_, IndexBuildStats) =
        timefusion::tantivy::build_and_pack(&table, &batches, 3, MergeMode::Deferred, &std::env::temp_dir()).expect("build_and_pack");
    assert_eq!(stats.rows, 3);
    assert!(!blob.is_empty());

    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let path = timefusion::tantivy::blob_path("logs", "proj1", "00000000-0000-0000-0000-000000000001");
    upload(store_obj.as_ref(), &path, blob.clone()).await.expect("upload");

    let dl = timefusion::tantivy::download(store_obj.as_ref(), &path).await.expect("download");
    assert_eq!(dl, blob);

    let dir = TempDir::new().unwrap();
    unpack_to_dir(&dl[..], dir.path()).expect("unpack");
    let idx = timefusion::tantivy::open_index(dir.path()).expect("open");
    let built = build_for_table(&table);
    let level_field = built.user_fields.get("level").unwrap().field;
    let q = TermQuery::new(Term::from_field_text(level_field, "ERROR"), IndexRecordOption::Basic);
    let hits = query_index(&idx, &q, None).expect("query");
    assert_eq!(hits, vec![Hit { timestamp_micros: 2_000_000, id: "b".into(), row_ordinal: Some(1) }]);

    delete(store_obj.as_ref(), &path).await.expect("delete");
    assert!(timefusion::tantivy::download(store_obj.as_ref(), &path).await.is_err());
}

/// Indexing a parquet file read back from object storage publishes a searchable
/// blob at the partition-mirrored path, with the manifest keyed by the parquet
/// rel path. Must use `otel_logs_and_spans` — the only schema `build_index_for_file`
/// can look up.
#[tokio::test]
async fn build_index_for_file_reads_parquet_and_publishes_searchable_index() {
    use serde_json::json;
    use timefusion::{
        config::TantivyConfig,
        support::test_helpers::json_to_batch,
        tantivy::search::{TantivyIndexService, TantivySearchService},
    };

    const TABLE: &str = "otel_logs_and_spans";
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let parquet_rel = "project_id=p1/date=2026-06-30/part-00000-test-c000.zstd.parquet";

    // `id` is a raw-tokenized indexed column.
    let b = json_to_batch(
        ["aaa", "row-b", "ccc"]
            .iter()
            .enumerate()
            .map(|(i, id)| {
                json!({ "timestamp": 1_700_000_000_000_000i64 + i as i64, "id": id, "name": "n",
                        "level": "INFO", "project_id": "p1", "date": "2026-06-30", "hashes": [], "summary": ["s"] })
            })
            .collect(),
    )
    .expect("json_to_batch");

    let mut buf: Vec<u8> = Vec::new();
    {
        use deltalake::datafusion::parquet::arrow::ArrowWriter;
        let mut w = ArrowWriter::try_new(&mut buf, b.schema(), None).unwrap();
        w.write(&b).unwrap();
        w.close().unwrap();
    }
    object_store::ObjectStoreExt::put(store_obj.as_ref(), &object_store::path::Path::from(parquet_rel), buf.into()).await.expect("put parquet");

    let svc = Arc::new(TantivyIndexService::new(
        store_obj.clone(),
        Arc::new(TantivyConfig::default()),
        std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4())),
    ));
    let parquet_uri = format!("s3://bucket/tf/{TABLE}/{parquet_rel}");
    svc.build_index_for_file(TABLE, "p1", parquet_rel, &parquet_uri, store_obj.clone()).await.expect("build_index_for_file");

    let m = load_manifest(store_obj.as_ref(), TABLE, "p1").await.unwrap();
    let entry = m.entries.get(parquet_rel).expect("manifest entry keyed by parquet rel");
    assert_eq!(entry.rows, 3);
    assert!(entry.error.is_none());
    let expected_blob = entry.index.as_ref().expect("published blob");
    assert_eq!(timefusion::tantivy::index_to_parquet_rel(TABLE, expected_blob).as_deref(), Some(parquet_rel));
    assert_eq!(entry.covered_files, vec![parquet_uri.clone()], "covered_files must carry the absolute URI (coverage gate / GC keying)");
    assert!(entry.ordinals_valid, "read-back build indexes parquet row order → ordinals valid for row selection");
    download(store_obj.as_ref(), &object_store::path::Path::from(expected_blob.as_str())).await.expect("blob exists");

    let cache = TempDir::new().unwrap();
    let search = Arc::new(TantivySearchService::new(store_obj.clone(), cache.path().to_path_buf(), Arc::new(TantivyConfig::default())));
    let hits = search.search(TABLE, "p1", "id", "row-b").await.expect("search").expect("some hits");
    assert_eq!(hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["row-b"]);
}

#[test]
fn verify_blob_accepts_built_index_and_rejects_corruption() {
    // A corrupt blob must be rejected before publish: blob paths are immutable,
    // so a poison blob fails every future read.
    let (blob, _) = timefusion::tantivy::build_and_pack(&logs_schema(), &[batch()], 3, MergeMode::Now, &std::env::temp_dir()).expect("build_and_pack");
    verify_blob(&blob).expect("freshly built blob must verify");

    // Must error, not panic.
    assert!(timefusion::tantivy::verify_blob(&blob[..blob.len() / 2]).is_err(), "corrupt blob must be rejected");
    assert!(timefusion::tantivy::verify_blob(b"not a tantivy archive").is_err(), "garbage blob must be rejected");
}

#[tokio::test]
async fn manifest_load_default_when_missing() {
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let m = load_manifest(store_obj.as_ref(), "logs", "proj1").await.expect("load empty");
    assert_eq!(m.version, SCHEMA_VERSION);
    assert!(m.entries.is_empty());
}

#[tokio::test]
async fn manifest_upsert_and_remove_roundtrip() {
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let built = ManifestEntry {
        min_timestamp_micros: Some(1_000_000),
        max_timestamp_micros: Some(2_000_000),
        covered_files: vec!["part-uuid-1.parquet".into()],
        ..entry(Some("indexes/logs/v1/proj1/uuid-1.tantivy.tar.zst"), 100, None)
    };
    upsert_manifest(store_obj.as_ref(), "logs", "proj1", "part-uuid-1.parquet", built).await.expect("upsert 1");
    upsert_manifest(store_obj.as_ref(), "logs", "proj1", "part-uuid-2.parquet", entry(None, 0, Some("boom"))).await.expect("upsert 2");

    let m = load_manifest(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    assert_eq!(m.entries.len(), 2);
    assert_eq!(m.entries["part-uuid-1.parquet"].rows, 100);
    assert!(m.entries["part-uuid-2.parquet"].error.is_some());

    remove_manifest_entries(store_obj.as_ref(), "logs", "proj1", &["part-uuid-1.parquet".into()]).await.unwrap();
    let m = load_manifest(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    assert_eq!(m.entries.len(), 1);
    assert!(m.entries.contains_key("part-uuid-2.parquet"));
}

#[tokio::test]
async fn concurrent_upserts_last_writer_wins() {
    // Last-writer-wins is the documented behavior: losing an entry to the race is
    // acceptable, corrupting the manifest is not.
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let writers = [("part-uuid-A.parquet", "a", 1u64), ("part-uuid-B.parquet", "b", 2)].map(|(key, blob, rows)| {
        let s = store_obj.clone();
        tokio::spawn(async move { upsert_manifest(s.as_ref(), "logs", "proj1", key, entry(Some(blob), rows, None)).await })
    });
    for w in writers {
        w.await.unwrap().unwrap();
    }
    let m = load_manifest(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    assert!(!m.entries.is_empty());
}

#[tokio::test]
async fn a_manifest_publish_rewrites_only_its_date_and_a_window_reads_only_its_dates() -> anyhow::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;

    use object_store::{ObjectStore, ObjectStoreExt};
    use timefusion::tantivy::{Manifest, manifest_path, manifest_root_path, manifest_shard_path, search::TantivySearchService, shard_dates};
    let counting = Arc::new(super::tantivy_search_test::FailAfterArm::new(Arc::new(InMemory::new())));
    let store: Arc<dyn ObjectStore> = counting.clone();
    let (day1, day2) = ("p/date=2026-10-01/a.parquet", "p/date=2026-10-02/b.parquet");
    let legacy =
        serde_json::json!({"version": SCHEMA_VERSION, "entries": {day1: entry(None, 1, None), day2: entry(None, 2, None), "bucket-x": entry(None, 3, None)}});
    store.put(&manifest_path("logs", "p"), serde_json::to_vec_pretty(&legacy)?.into()).await?;
    let keys = |m: &Manifest| m.entries.keys().cloned().collect::<Vec<_>>();
    assert_eq!(keys(&load_manifest(store.as_ref(), "logs", "p").await?), ["bucket-x", day1, day2]);

    // The first publish migrates the legacy file into shards, then a publish puts only its own date.
    upsert_manifest(store.as_ref(), "logs", "p", "p/date=2026-10-02/c.parquet", entry(None, 0, None)).await?;
    let etags = async || {
        let mut tags = vec![];
        for shard in ["2026-10-01", "2026-10-02", "undated"] {
            tags.push(store.head(&manifest_shard_path("logs", "p", shard)).await?.e_tag);
        }
        anyhow::Ok(tags)
    };
    let before = etags().await?;
    upsert_manifest(store.as_ref(), "logs", "p", "p/date=2026-10-02/d.parquet", entry(None, 0, None)).await?;
    let after = etags().await?;
    assert_eq!((before[0] == after[0], before[1] == after[1], before[2] == after[2]), (true, false, true));
    assert_eq!(load_manifest(store.as_ref(), "logs", "p").await?.entries.len(), 5);

    // A window GETs the root and its own shards once, then serves from cache.
    let cfg = timefusion::config::TantivyConfig { timefusion_tantivy_manifest_ttl_secs: 60, ..Default::default() };
    let search = TantivySearchService::new(store.clone(), tempfile::tempdir()?.path().into(), Arc::new(cfg));
    let day = 1_790_899_200_000_000; // 2026-10-02T00:00Z
    for expected_gets in [3, 0] {
        let gets = counting.gets.load(Relaxed);
        let window = search.load_manifest_cached("logs", "p", shard_dates(day, day + 3_600_000_000)).await?;
        assert_eq!(keys(&window), ["bucket-x", day2, "p/date=2026-10-02/c.parquet", "p/date=2026-10-02/d.parquet"]);
        assert_eq!(counting.gets.load(Relaxed) - gets, expected_gets);
    }

    // A date emptied of entries leaves the root, then the store.
    remove_manifest_entries(store.as_ref(), "logs", "p", &[day1.into()]).await?;
    assert!(store.head(&manifest_shard_path("logs", "p", "2026-10-01")).await.is_err());
    let root: serde_json::Value = serde_json::from_slice(&store.get(&manifest_root_path("logs", "p")).await?.bytes().await?)?;
    assert_eq!(root["shards"], serde_json::json!(["2026-10-02", "undated"]));
    Ok(())
}

#[tokio::test]
async fn a_bundle_opens_from_its_head_and_answers_like_the_unpacked_index() -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

    use bytes::Bytes;
    use timefusion::tantivy::{
        MergeMode, build_to_dir,
        hotcache::{BundleDirectory, BundleSource, pack_bundle},
        open_index, open_index_in,
        search::{PredsQuery, build_node_query},
        udf::{PredNode, TextMatchPred},
    };
    #[derive(Debug)]
    struct Counting(Bytes, AtomicUsize);
    impl BundleSource for Counting {
        fn read(&self, range: std::ops::Range<u64>) -> std::io::Result<Bytes> {
            self.1.fetch_add(1, Relaxed);
            self.0.read(range)
        }
    }
    let rows: Vec<(i64, String, String)> = (0..50_000).map(|i| (i, format!("id{i}"), format!("L{}", i % 5_000))).collect();
    let rows: Vec<(i64, &str, &str)> = rows.iter().map(|(t, id, level)| (*t, id.as_str(), level.as_str())).collect();
    let built = TempDir::new()?;
    build_to_dir(&logs_schema(), [logs_batch(&rows, false)], built.path(), MergeMode::Now)?;
    let blob = pack_bundle(built.path(), 3)?;
    verify_blob(&blob)?;
    assert!(verify_blob(&blob[..blob.len() - 1]).is_err(), "a truncated bundle must be rejected");

    let unpacked = TempDir::new()?;
    unpack_to_dir(&blob[..], unpacked.path())?;
    for entry in std::fs::read_dir(built.path())? {
        let name = entry?.file_name();
        assert_eq!(std::fs::read(built.path().join(&name))?, std::fs::read(unpacked.path().join(&name))?, "{name:?} round-trips");
    }

    let source = Arc::new(Counting(blob.clone(), AtomicUsize::new(0)));
    let bundle = open_index_in(BundleDirectory::new(blob, source.clone())?)?;
    let searcher = bundle.reader()?.searcher();
    assert_eq!(source.1.load(Relaxed), 0, "opening a reader must read only the head");
    let plain = open_index(unpacked.path())?;
    for (column, query, expected) in [("level", "L7", 10), ("level", "L4999", 10), ("level", "nope", 0), ("id", "id31337", 1)] {
        let node = PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() });
        let count = |index: &tantivy::Index, searcher: &tantivy::Searcher| -> anyhow::Result<usize> {
            let PredsQuery::Query(q) = build_node_query(index, &node)? else { anyhow::bail!("{column} is not indexed") };
            Ok(searcher.search(&*q, &tantivy::collector::Count)?)
        };
        assert_eq!((count(&bundle, &searcher)?, count(&plain, &plain.reader()?.searcher())?), (expected, expected), "{column}={query}");
    }
    assert!(source.1.load(Relaxed) > 0, "term lookups read past the head");
    Ok(())
}
