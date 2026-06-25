//! Tier-3/4: end-to-end search service test (build via callback,
//! then query via search service). No Delta — we just verify the index
//! pipeline produces correct (timestamp, id) hits and that operational
//! failure paths behave correctly.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use object_store::memory::InMemory;
use tempfile::TempDir;
use timefusion::{
    config::TantivyConfig,
    schema_loader::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy_index::{
        manifest::{self, ManifestEntry},
        search::TantivySearchService,
        service::TantivyIndexService,
    },
};

#[allow(dead_code)]
fn schema_with(level_indexed: bool) -> TableSchema {
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
        dedup_keys:      vec![],
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
                tantivy:      level_indexed.then(|| TantivyFieldConfig {
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
    // build batches that match its required columns. We index "level" which
    // is configured for tantivy in the production YAML.
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        ..Default::default()
    };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.clone().callback();

    // Build a batch matching the prod schema. Only the columns we care about
    // here are timestamp/id/level — the rest of the columns can be missing
    // because schema validation is on the Delta side, not tantivy.
    let b = batch(&[(1_000_000, "a", "INFO"), (2_000_000, "b", "ERROR"), (3_000_000, "c", "INFO")]);
    cb(project_id.to_string(), table_name.to_string(), vec![b], vec!["test-uri".into()]).await.expect("callback");

    // Manifest has one entry now
    let m = manifest::load(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m.entries.len(), 1);
    let entry = m.entries.values().next().unwrap();
    assert_eq!(entry.rows, 3);
    assert!(entry.index.is_some());
    assert_eq!(entry.min_timestamp_micros, Some(1_000_000));
    assert_eq!(entry.max_timestamp_micros, Some(3_000_000));

    // Search via TantivySearchService
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store.clone(), cache.path().to_path_buf());
    let hits = search.search(table_name, project_id, "level", "ERROR").await.expect("search").expect("usable index");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
    assert_eq!(hits[0].timestamp_micros, 2_000_000);

    // Cache hit: re-run; must return same answers
    let hits2 = search.search(table_name, project_id, "level", "ERROR").await.unwrap().unwrap();
    assert_eq!(hits, hits2);
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
    let m = manifest::load(store.as_ref(), "no_such_table", "p1").await.unwrap();
    assert!(m.entries.is_empty(), "no manifest entry should be written for an unknown table");
}

#[tokio::test]
async fn search_falls_back_when_manifest_entry_marked_failed() {
    // Simulate an entry whose build failed: index=None, error=Some.
    // search() must skip it and return zero hits (no panic).
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    manifest::upsert(
        store.as_ref(),
        "logs",
        "p1",
        "bucket-bad",
        ManifestEntry {
            index:                None,
            rows:                 0,
            built_at:             chrono::Utc::now(),
            schema_version:       manifest::SCHEMA_VERSION,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error:                Some("simulated build failure".into()),
            covered_files:        vec![],
        },
    )
    .await
    .unwrap();
    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf());
    // Manifest has only failed entries → no usable index → returns None so
    // the caller falls back to full scan + UDF post-filter.
    let hits = search.search("logs", "p1", "level", "ERROR").await.unwrap();
    assert!(hits.is_none());
}

#[tokio::test]
async fn gc_after_compaction_clears_manifest_and_blobs() {
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        ..Default::default()
    };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.clone().callback();
    // First flush wrote file_a; second flush wrote file_b.
    cb(
        project_id.into(),
        table_name.into(),
        vec![batch(&[(1_000_000, "a", "INFO")])],
        vec!["file_a".into()],
    )
    .await
    .unwrap();
    cb(
        project_id.into(),
        table_name.into(),
        vec![batch(&[(2_000_000, "b", "ERROR")])],
        vec!["file_b".into()],
    )
    .await
    .unwrap();
    let m_before = manifest::load(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m_before.entries.len(), 2);

    // Compaction has rewritten file_a away but file_b survives. Only the
    // entry covering file_a should be dropped.
    let report = svc.gc_after_compaction(table_name, project_id, &["file_b".to_string()]).await.unwrap();
    assert_eq!(report.entries_removed, 1, "only one entry should be stale");
    assert_eq!(report.kept, 1, "the entry covering file_b should be kept");

    let m_after = manifest::load(store.as_ref(), table_name, project_id).await.unwrap();
    assert_eq!(m_after.entries.len(), 1, "one entry should remain");
    let surviving = m_after.entries.values().next().unwrap();
    assert_eq!(surviving.covered_files, vec!["file_b".to_string()]);

    // Calling GC with no live URIs should drop the remaining entry.
    let report2 = svc.gc_after_compaction(table_name, project_id, &[]).await.unwrap();
    assert_eq!(report2.entries_removed, 1);
    let m_final = manifest::load(store.as_ref(), table_name, project_id).await.unwrap();
    assert!(m_final.entries.is_empty());
}

#[tokio::test]
async fn search_skips_indexes_that_dont_have_the_field() {
    // An older index won't have a newly-added field. search() must not error;
    // it should simply skip those indexes and return hits from the others.
    let table_name = "otel_logs_and_spans";
    let project_id = "p1";
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cfg = TantivyConfig {
        timefusion_tantivy_compression_level: 3,
        ..Default::default()
    };
    let svc = Arc::new(TantivyIndexService::new(store.clone(), Arc::new(cfg)));
    let cb = svc.callback();
    let b = batch(&[(1_000_000, "a", "INFO")]);
    cb(project_id.into(), table_name.into(), vec![b], vec!["uri".into()]).await.unwrap();

    let cache = TempDir::new().unwrap();
    let search = TantivySearchService::new(store, cache.path().to_path_buf());
    // Querying a non-indexed field (e.g. parent_id) should yield no usable index → None.
    let hits = search.search(table_name, project_id, "parent_id", "anything").await.unwrap();
    assert!(hits.is_none());
}
