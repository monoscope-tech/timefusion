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
