//! Tier-2: storage roundtrip + manifest tests using `object_store::InMemory`.
//! No MinIO required; the same code paths are exercised against any
//! `ObjectStore` impl (S3/MinIO/file).

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use chrono::Utc;
use object_store::memory::InMemory;
use tantivy::{Term, query::TermQuery, schema::IndexRecordOption};
use tempfile::TempDir;
use timefusion::{
    schema_loader::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy_index::{
        builder::IndexBuildStats,
        manifest::{self, ManifestEntry},
        query_index,
        reader::Hit,
        schema::build_for_table,
        store,
    },
};

fn table() -> TableSchema {
    TableSchema {
        table_name: "logs".into(),
        partitions: vec![],
        sorting_columns: vec![SortingColumnDef {
            name: "timestamp".into(),
            descending: false,
            nulls_first: false,
        }],
        z_order_columns: vec![],
        time_column: None,
        dedup_keys: vec![],
        dedup_tiebreak: None,
        fields: vec![
            FieldDef {
                name: "timestamp".into(),
                data_type: "Timestamp(Microsecond, Some(\"UTC\"))".into(),
                nullable: false,
                tantivy: None,
                dictionary: None,
                bloom_filter: false,
            },
            FieldDef {
                name: "id".into(),
                data_type: "Utf8".into(),
                nullable: false,
                tantivy: None,
                dictionary: None,
                bloom_filter: false,
            },
            FieldDef {
                name: "level".into(),
                data_type: "Utf8".into(),
                nullable: true,
                tantivy: Some(TantivyFieldConfig {
                    indexed: true,
                    tokenizer: Some("raw".into()),
                    flatten: None,
                }),
                dictionary: None,
                bloom_filter: false,
            },
        ],
    }
}

fn batch() -> RecordBatch {
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000, 3_000_000]).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let level: ArrayRef = Arc::new(StringArray::from(vec!["INFO", "ERROR", "INFO"]));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(schema, vec![ts, id, level]).unwrap()
}

#[tokio::test]
async fn pack_upload_download_unpack_query_roundtrip() {
    let table = table();
    let batches = vec![batch()];

    // Build & pack
    let (blob, stats): (_, IndexBuildStats) = store::build_and_pack(&table, &batches, 3).expect("build_and_pack");
    assert_eq!(stats.rows, 3);
    assert!(!blob.is_empty());

    // Upload to in-memory store
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let path = store::blob_path("logs", "proj1", "00000000-0000-0000-0000-000000000001");
    store::upload(store_obj.as_ref(), &path, blob.clone()).await.expect("upload");

    // Download
    let dl = store::download(store_obj.as_ref(), &path).await.expect("download");
    assert_eq!(dl, blob);

    // Unpack to a fresh dir, open, query
    let dir = TempDir::new().unwrap();
    store::unpack_to_dir(&dl, dir.path()).expect("unpack");
    let idx = store::open_index(dir.path()).expect("open");
    let built = build_for_table(&table);
    let level_field = built.user_fields.get("level").unwrap().field;
    let q = TermQuery::new(Term::from_field_text(level_field, "ERROR"), IndexRecordOption::Basic);
    let hits = query_index(&idx, &q, None).expect("query");
    assert_eq!(
        hits,
        vec![Hit {
            timestamp_micros: 2_000_000,
            id: "b".into(),
            row_ordinal: Some(1),
        }]
    );

    // Delete, then ensure it's gone
    store::delete(store_obj.as_ref(), &path).await.expect("delete");
    assert!(store::download(store_obj.as_ref(), &path).await.is_err());
}

/// Phase 2 primitive: index a parquet file read back from object storage
/// (no live flush batches), published at the deterministic partition-mirrored
/// path with the manifest keyed by the parquet rel path. Confirms the round
/// trip is searchable — the reused builder behind reconcile/backfill/reindex.
/// Uses the real `otel_logs_and_spans` schema (the only registered schema
/// `build_index_for_file` can look up) over an InMemory store (no MinIO).
#[tokio::test]
async fn build_index_for_file_reads_parquet_and_publishes_searchable_index() {
    use serde_json::json;
    use timefusion::{
        config::TantivyConfig,
        tantivy_index::{search::TantivySearchService, service::TantivyIndexService},
        test_utils::test_helpers::json_to_batch,
    };

    const TABLE: &str = "otel_logs_and_spans";
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let parquet_rel = "project_id=p1/date=2026-06-30/part-00000-test-c000.zstd.parquet";

    // A 3-row otel batch; `id` is a raw-tokenized indexed column (P0).
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
    object_store::ObjectStoreExt::put(store_obj.as_ref(), &object_store::path::Path::from(parquet_rel), buf.into())
        .await
        .expect("put parquet");

    // Build the index for that committed file.
    let svc = Arc::new(TantivyIndexService::new(store_obj.clone(), Arc::new(TantivyConfig::default())));
    let parquet_uri = format!("s3://bucket/tf/{TABLE}/{parquet_rel}");
    svc.build_index_for_file(TABLE, "p1", parquet_rel, &parquet_uri, store_obj.clone()).await.expect("build_index_for_file");

    // Manifest entry is keyed by the parquet rel path, points at the
    // deterministic partition-mirrored blob, and the blob exists.
    let m = manifest::load(store_obj.as_ref(), TABLE, "p1").await.unwrap();
    let entry = m.entries.get(parquet_rel).expect("manifest entry keyed by parquet rel");
    assert_eq!(entry.rows, 3);
    assert!(entry.error.is_none());
    let expected_blob = store::index_path_for_parquet(TABLE, parquet_rel).to_string();
    assert_eq!(entry.index.as_deref(), Some(expected_blob.as_str()));
    assert_eq!(
        entry.covered_files,
        vec![parquet_uri.clone()],
        "covered_files must carry the absolute URI (coverage gate / GC keying)"
    );
    assert!(
        entry.ordinals_valid,
        "read-back build indexes parquet row order → ordinals valid for row selection"
    );
    store::download(store_obj.as_ref(), &object_store::path::Path::from(expected_blob)).await.expect("blob exists");

    // And the published index is actually searchable end-to-end.
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store_obj.clone(), cache.path().to_path_buf());
    let hits = search.search(TABLE, "p1", "id", "row-b").await.expect("search").expect("some hits");
    assert_eq!(hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["row-b"]);
}

#[tokio::test]
async fn manifest_load_default_when_missing() {
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let m = manifest::load(store_obj.as_ref(), "logs", "proj1").await.expect("load empty");
    assert_eq!(m.version, manifest::SCHEMA_VERSION);
    assert!(m.entries.is_empty());
}

#[tokio::test]
async fn manifest_upsert_and_remove_roundtrip() {
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let entry = ManifestEntry {
        index: Some("indexes/logs/v1/proj1/uuid-1.tantivy.tar.zst".into()),
        rows: 100,
        built_at: Utc::now(),
        schema_version: manifest::SCHEMA_VERSION,
        min_timestamp_micros: Some(1_000_000),
        max_timestamp_micros: Some(2_000_000),
        error: None,
        covered_files: vec!["part-uuid-1.parquet".into()],
        ordinals_valid: false,
    };
    manifest::upsert(store_obj.as_ref(), "logs", "proj1", "part-uuid-1.parquet", entry.clone()).await.expect("upsert 1");
    manifest::upsert(
        store_obj.as_ref(),
        "logs",
        "proj1",
        "part-uuid-2.parquet",
        ManifestEntry {
            index: None,
            rows: 0,
            built_at: Utc::now(),
            schema_version: 1,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error: Some("boom".into()),
            covered_files: vec![],
            ordinals_valid: false,
        },
    )
    .await
    .expect("upsert 2");

    let m = manifest::load(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    assert_eq!(m.entries.len(), 2);
    assert_eq!(m.entries["part-uuid-1.parquet"].rows, 100);
    assert!(m.entries["part-uuid-2.parquet"].error.is_some());

    manifest::remove_many(store_obj.as_ref(), "logs", "proj1", &["part-uuid-1.parquet".into()]).await.unwrap();
    let m = manifest::load(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    assert_eq!(m.entries.len(), 1);
    assert!(m.entries.contains_key("part-uuid-2.parquet"));
}

#[tokio::test]
async fn concurrent_upserts_last_writer_wins() {
    // Simulates two concurrent upserts to the same project. Last-writer-wins
    // is the documented behavior; both writes must produce a valid manifest
    // (no corruption), and the final manifest must contain at least one entry.
    let store_obj: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let s1 = store_obj.clone();
    let s2 = store_obj.clone();
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move {
            manifest::upsert(
                s1.as_ref(),
                "logs",
                "proj1",
                "part-uuid-A.parquet",
                ManifestEntry {
                    index: Some("a".into()),
                    rows: 1,
                    built_at: Utc::now(),
                    schema_version: 1,
                    min_timestamp_micros: None,
                    max_timestamp_micros: None,
                    error: None,
                    covered_files: vec![],
                    ordinals_valid: false,
                },
            )
            .await
        }),
        tokio::spawn(async move {
            manifest::upsert(
                s2.as_ref(),
                "logs",
                "proj1",
                "part-uuid-B.parquet",
                ManifestEntry {
                    index: Some("b".into()),
                    rows: 2,
                    built_at: Utc::now(),
                    schema_version: 1,
                    min_timestamp_micros: None,
                    max_timestamp_micros: None,
                    error: None,
                    covered_files: vec![],
                    ordinals_valid: false,
                },
            )
            .await
        }),
    );
    r1.unwrap().unwrap();
    r2.unwrap().unwrap();
    let m = manifest::load(store_obj.as_ref(), "logs", "proj1").await.unwrap();
    // At least one of them survived. Race is acceptable; corruption is not.
    assert!(!m.entries.is_empty());
}
