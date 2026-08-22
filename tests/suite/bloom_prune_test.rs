//! Integration tests for file-level needle pruning (bloom sidecars).
//! docs/plans/2026-08-22-file-level-needle-pruning.md

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

/// Database + a bloom registry attached, plus a fresh project id. The registry's
/// own object store (sidecar storage) is independent of the table's S3 store —
/// `bloom_sidecar_reconcile` reads parquet through the table's store and writes
/// sidecars through the registry's.
async fn setup(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let reg = Arc::new(BloomPruneRegistry::new(Arc::new(InMemory::new()), 64 << 20, Duration::from_secs(300)));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    Ok((db, project_id))
}

/// `COUNT(*)` for a project/trace_id, bounded by a same-day time window (so the
/// scan's plan path actually consults the bloom registry).
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
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // ~100 rows, one distinguished present trace_id among many distinct ones.
    let rows: Vec<_> = (0..100).map(|i| row(&format!("id-{i}"), &project_id, ts, &format!("trace-{i}"))).collect();
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(rows)?], true, None).await?;

    let (built, errors) = db.bloom_sidecar_reconcile().await?;
    assert!(built >= 1, "reconcile should have built at least one file's sidecar, got {built}");
    assert_eq!(errors, 0);

    // Present needle: bloom must never produce a false negative.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-42", ts).await?, 1, "present trace_id must be returned");
    // Absent needle: bloom pruning should reject the file and the row genuinely doesn't exist.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-does-not-exist", ts).await?, 0, "absent trace_id must return zero rows");
    Ok(())
}

#[tokio::test]
async fn bloom_pruning_never_drops_updated_or_deleted_versions() -> Result<()> {
    let (db, project_id) = setup("bloom_mor").await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let trace_id = "trace-mor";

    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![row("mor-1", &project_id, ts, trace_id)])?], true, None).await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1);

    // UPDATE appends a new version (version_append table); the needle column
    // is untouched, so both versions still carry it and the file must not be
    // bloom-rejected out from under the fresher version.
    let sql = format!("UPDATE otel_logs_and_spans SET hashes = make_array('v2') WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}'");
    ctx.sql(&sql).await?.collect().await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1, "must still resolve to exactly the latest version, not 0 or 2");

    // DELETE appends a tombstoned version — the row must disappear, not be
    // resurrected by an older, non-tombstoned physical copy the bloom kept.
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
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // Two independent Delta commits (two files, same project/date) with
    // disjoint trace_id sets.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![row("a1", &project_id, ts, "trace-A")])?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![row("b1", &project_id, ts, "trace-B")])?], true, None).await?;
    db.bloom_sidecar_reconcile().await?;

    let reg = db.bloom_prune().expect("registry attached");
    let before = reg.stats.files_rejected.load(Relaxed);
    // Needle only in file A: file B must be provably rejected.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-A", ts).await?, 1);
    assert!(reg.stats.files_rejected.load(Relaxed) > before, "querying a needle present in only one file must reject the other");

    let before2 = reg.stats.files_rejected.load(Relaxed);
    // Needle in neither file: both files rejected, zero rows, no crash on the
    // empty-include-selection path.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(reg.stats.files_rejected.load(Relaxed) >= before2 + 2, "an all-rejected needle must reject every in-window file");
    Ok(())
}

/// The SPLIT path — the branch prod runs on nearly every point lookup: tantivy
/// covers SOME files (equality routing engages the prefilter, `covered_files`
/// is Some) while others are raw debt. Bloom rejection must reach BOTH legs
/// without dropping rows, and the all-rejected case must take the
/// empty-include arm, not the unrestricted fallback.
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
    let svc = Arc::new(TantivyIndexService::new(tstore.clone(), tcfg.clone()));
    let search = Arc::new(TantivySearchService::new(tstore, cfg.core.timefusion_data_dir.clone(), tcfg));
    let db = Arc::new(db.with_tantivy_search(search.clone()).with_tantivy_indexer(svc.clone()).with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // File 1: tantivy-COVERED (manifest published via the indexer callback,
    // exactly what the flush hook does). File 2: raw debt, never indexed.
    let b1 = json_to_batch(vec![row("a1", &project_id, ts, "trace-covered")])?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![b1.clone()], true, None).await?;
    let file1: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    svc.clone().callback()(project_id.clone(), "otel_logs_and_spans".into(), vec![b1], file1.clone()).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![row("b1", &project_id, ts, "trace-raw")])?], true, None).await?;
    let all: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    assert!(all.len() > file1.len(), "second insert must add an uncovered file");
    db.bloom_sidecar_reconcile().await?;

    let stats = &db.bloom_prune().unwrap().stats;
    // Needle only in the RAW file: the raw leg must serve it (no row loss
    // through the split) while bloom rejects the covered file.
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-raw", ts).await?, 1, "raw-leg row must survive the split");
    assert!(stats.files_rejected.load(Relaxed) > before, "covered file must be bloom-rejected for the raw needle");

    // Needle only in the COVERED file: indexed leg serves it, raw file rejected.
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-covered", ts).await?, 1, "indexed-leg row must survive");
    assert!(stats.files_rejected.load(Relaxed) > before, "raw file must be bloom-rejected for the covered needle");

    // Prove this exercised the SPLIT branch, not the no-coverage fallback:
    // the prefilter ran (so `covered_files` was Some) while raw debt existed.
    assert!(search.stats.queries.load(Relaxed) > 0, "equality routing must have engaged the tantivy prefilter");

    // Needle in NEITHER: all in-window files rejected — the split path's
    // empty-include arm, and still zero rows.
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(stats.files_rejected.load(Relaxed) >= before + 2, "an all-absent needle must reject every file on the split path");
    Ok(())
}
