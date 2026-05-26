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
        table_name:      "logs".into(),
        partitions:      vec![],
        sorting_columns: vec![SortingColumnDef {
            name:        "timestamp".into(),
            descending:  false,
            nulls_first: false,
        }],
        z_order_columns: vec![],
        time_column:     None,
        fields:          vec![
            FieldDef {
                name:         "timestamp".into(),
                data_type:    "Timestamp(Microsecond, Some(\"UTC\"))".into(),
                nullable:     false,
                tantivy:      None,
                dictionary:   None,
                bloom_filter: false,
            },
            FieldDef {
                name:         "id".into(),
                data_type:    "Utf8".into(),
                nullable:     false,
                tantivy:      None,
                dictionary:   None,
                bloom_filter: false,
            },
            FieldDef {
                name:         "level".into(),
                data_type:    "Utf8".into(),
                nullable:     true,
                tantivy:      Some(TantivyFieldConfig {
                    indexed:   true,
                    tokenizer: Some("raw".into()),
                    flatten:   None,
                }),
                dictionary:   None,
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
            id:               "b".into(),
        }]
    );

    // Delete, then ensure it's gone
    store::delete(store_obj.as_ref(), &path).await.expect("delete");
    assert!(store::download(store_obj.as_ref(), &path).await.is_err());
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
        index:                Some("indexes/logs/v1/proj1/uuid-1.tantivy.tar.zst".into()),
        rows:                 100,
        built_at:             Utc::now(),
        schema_version:       manifest::SCHEMA_VERSION,
        min_timestamp_micros: Some(1_000_000),
        max_timestamp_micros: Some(2_000_000),
        error:                None,
        covered_files:        vec!["part-uuid-1.parquet".into()],
    };
    manifest::upsert(store_obj.as_ref(), "logs", "proj1", "part-uuid-1.parquet", entry.clone()).await.expect("upsert 1");
    manifest::upsert(
        store_obj.as_ref(),
        "logs",
        "proj1",
        "part-uuid-2.parquet",
        ManifestEntry {
            index:                None,
            rows:                 0,
            built_at:             Utc::now(),
            schema_version:       1,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error:                Some("boom".into()),
            covered_files:        vec![],
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
                    index:                Some("a".into()),
                    rows:                 1,
                    built_at:             Utc::now(),
                    schema_version:       1,
                    min_timestamp_micros: None,
                    max_timestamp_micros: None,
                    error:                None,
                    covered_files:        vec![],
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
                    index:                Some("b".into()),
                    rows:                 2,
                    built_at:             Utc::now(),
                    schema_version:       1,
                    min_timestamp_micros: None,
                    max_timestamp_micros: None,
                    error:                None,
                    covered_files:        vec![],
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
