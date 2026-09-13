//! Integration tests for file-level needle pruning (bloom sidecars).

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
use object_store::memory::InMemory;
use serde_json::json;
use timefusion::{
    database::Database,
    read::bloom_prune::BloomPruneRegistry,
    support::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch},
};

/// A minimal otel_logs_and_spans row carrying a known `context___trace_id`.
fn row(id: &str, project_id: &str, ts: i64, trace_id: &str) -> serde_json::Value {
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    json!({
        "timestamp": ts, "id": id, "name": "n", "project_id": project_id, "date": date,
        "hashes": [], "summary": [], "context___trace_id": trace_id,
    })
}

/// 3h ago: keeps the ±1h window in `count_by_trace_id` on the row's partition day.
fn ts() -> i64 {
    (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros()
}

/// One Delta commit of `rows` into otel_logs_and_spans.
async fn insert(db: &Arc<Database>, project_id: &str, rows: Vec<serde_json::Value>) -> Result<()> {
    db.insert_records_batch(project_id, "otel_logs_and_spans", vec![json_to_batch(rows)?], true, None).await?;
    Ok(())
}

/// Database + bloom registry + a fresh project id. The registry's sidecar store is
/// deliberately separate from the table's store (reconcile reads one, writes the other).
async fn setup(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let reg = Arc::new(BloomPruneRegistry::new(Arc::new(InMemory::new()), 64 << 20, Duration::from_secs(300)));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    Ok((db, project_id))
}

/// `COUNT(*)` for a project/trace_id; the time bound is required for the scan to
/// consult the bloom registry.
async fn count_by_trace_id(db: &Arc<Database>, project_id: &str, trace_id: &str, ts: i64) -> Result<i64> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let lo = ts - 3_600_000_000;
    let hi = ts + 3_600_000_000;
    let sql = format!(
        "SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}' \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi})"
    );
    let res = ctx.sql(&sql).await?.collect().await?;
    Ok(res[0].column(0).as_primitive::<Int64Type>().value(0))
}

#[tokio::test]
async fn bloom_sidecar_build_has_no_false_negatives() -> Result<()> {
    let (db, project_id) = setup("bloom_no_fn").await?;
    let ts = ts();

    let rows: Vec<_> = (0..100).map(|i| row(&format!("id-{i}"), &project_id, ts, &format!("trace-{i}"))).collect();
    insert(&db, &project_id, rows).await?;

    let (built, errors) = db.bloom_sidecar_reconcile().await?;
    assert!(built >= 1, "reconcile should have built at least one file's sidecar, got {built}");
    assert_eq!(errors, 0);

    assert_eq!(count_by_trace_id(&db, &project_id, "trace-42", ts).await?, 1, "present trace_id must be returned");
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-does-not-exist", ts).await?, 0, "absent trace_id must return zero rows");
    Ok(())
}

#[tokio::test]
async fn bloom_pruning_never_drops_updated_or_deleted_versions() -> Result<()> {
    let (db, project_id) = setup("bloom_mor").await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let ts = ts();
    let trace_id = "trace-mor";

    insert(&db, &project_id, vec![row("mor-1", &project_id, ts, trace_id)]).await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1);

    // UPDATE appends a new version; both versions carry the needle, so neither file
    // may be bloom-rejected out from under the fresher version.
    let sql = format!("UPDATE otel_logs_and_spans SET hashes = make_array('v2') WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}'");
    ctx.sql(&sql).await?.collect().await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1, "must still resolve to exactly the latest version, not 0 or 2");

    // DELETE appends a tombstone; an older physical copy must not resurrect the row.
    let del = format!("DELETE FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}'");
    ctx.sql(&del).await?.collect().await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 0, "tombstoned row must not be resurrected");
    Ok(())
}

#[tokio::test]
async fn bloom_pruning_excludes_files_and_empty_needle_scans_zero_files() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let (db, project_id) = setup("bloom_exclude").await?;
    let ts = ts();

    // Two commits => two files, same project/date, disjoint trace_id sets.
    insert(&db, &project_id, vec![row("a1", &project_id, ts, "trace-A")]).await?;
    insert(&db, &project_id, vec![row("b1", &project_id, ts, "trace-B")]).await?;
    db.bloom_sidecar_reconcile().await?;

    let reg = db.bloom_prune().expect("registry attached");
    let before = reg.stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-A", ts).await?, 1);
    assert!(reg.stats.files_rejected.load(Relaxed) > before, "querying a needle present in only one file must reject the other");

    let before2 = reg.stats.files_rejected.load(Relaxed);
    // Needle in neither file: exercises the empty-include-selection path.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(reg.stats.files_rejected.load(Relaxed) >= before2 + 2, "an all-rejected needle must reject every in-window file");
    Ok(())
}

/// Split path: tantivy covers some files while others are unindexed. Bloom rejection
/// must reach both legs without dropping rows, and an all-rejected needle must take
/// the empty-include arm rather than falling back to an unrestricted scan.
#[tokio::test]
async fn split_path_bloom_prunes_indexed_and_raw_legs() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use timefusion::tantivy::search::{TantivyIndexService, TantivySearchService};

    let cfg = TestConfigBuilder::new("bloom_split").with_buffer_mode(BufferMode::Enabled).build();
    let reg = Arc::new(BloomPruneRegistry::new(Arc::new(InMemory::new()), 64 << 20, Duration::from_secs(300)));
    let db = Database::with_config(Arc::clone(&cfg)).await?;
    let storage_uri = format!("s3://{}/{}/tantivy", cfg.aws.aws_s3_bucket.clone().unwrap(), cfg.core.timefusion_table_prefix);
    let tstore = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
    let tcfg = Arc::new(cfg.tantivy.clone());
    let svc = Arc::new(TantivyIndexService::new(tstore.clone(), tcfg.clone(), std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4()))));
    let search = Arc::new(TantivySearchService::new(tstore, cfg.core.timefusion_data_dir.clone(), tcfg));
    let db = Arc::new(db.with_tantivy_search(search.clone()).with_tantivy_indexer(svc.clone()).with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = ts();

    // File 1 is tantivy-covered via the indexer callback (what the flush hook does);
    // file 2 is never indexed.
    let b1 = json_to_batch(vec![row("a1", &project_id, ts, "trace-covered")])?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![b1.clone()], true, None).await?;
    let file1: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    svc.clone().batch_callback()(project_id.clone(), "otel_logs_and_spans".into(), vec![b1], file1.clone()).await?;
    insert(&db, &project_id, vec![row("b1", &project_id, ts, "trace-raw")]).await?;
    let all: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    assert!(all.len() > file1.len(), "second insert must add an uncovered file");
    db.bloom_sidecar_reconcile().await?;

    let stats = &db.bloom_prune().unwrap().stats;
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-raw", ts).await?, 1, "raw-leg row must survive the split");
    assert!(stats.files_rejected.load(Relaxed) > before, "covered file must be bloom-rejected for the raw needle");

    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-covered", ts).await?, 1, "indexed-leg row must survive");
    assert!(stats.files_rejected.load(Relaxed) > before, "raw file must be bloom-rejected for the covered needle");

    // Proves the split branch ran rather than the no-coverage fallback.
    assert!(search.stats.queries.load(Relaxed) > 0, "equality routing must have engaged the tantivy prefilter");

    // Needle in neither file: the split path's empty-include arm.
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(stats.files_rejected.load(Relaxed) >= before + 2, "an all-absent needle must reject every file on the split path");
    Ok(())
}
