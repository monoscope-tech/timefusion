//! Read-side dedup (Phase 2): when two Delta files in the same partition
//! hold the same `(id, timestamp)` row, the user-facing query must return
//! a single row. Phase 1's compaction sweep only runs on the maintenance
//! tick — queries hitting Delta before that sweep landed must still see
//! the deduplicated view.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
use serial_test::serial;
use timefusion::{
    database::Database,
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span_ts},
};

async fn count(db: &Arc<Database>, sql: &str) -> Result<i64> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let r = ctx.sql(sql).await?.collect().await?;
    Ok(r[0].column(0).as_primitive::<Int64Type>().value(0))
}

#[serial]
#[tokio::test]
async fn read_dedup_collapses_overlapping_delta_files() -> Result<()> {
    let cfg = TestConfigBuilder::new("read_dedup").with_buffer_mode(BufferMode::Enabled).build();
    // SAFETY: walrus-rust reads WALRUS_DATA_DIR from process env. #[serial]
    // serializes the test suite; this set is racy in principle but safe here.
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = chrono::Utc::now().timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };

    // Two separate Delta commits, two files, same (id, timestamp). This is
    // the cross-flush dupe Phase 1 compaction collapses on the maintenance
    // tick — Phase 2 must hide it from live queries too.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Use SELECT name (not COUNT(*)) so DataFusion can't shortcut to
    // file-level statistics — we must exercise the actual scan stream.
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let rows = ctx
        .sql(&format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id))
        .await?
        .collect()
        .await?;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "read-side dedup should collapse cross-file duplicates on the live query path");

    Ok(())
}

#[serial]
#[tokio::test]
async fn read_dedup_skips_non_overlapping_partitions() -> Result<()> {
    // Two distinct (id, ts) rows in the same partition share no key. The
    // overlap-stats gate must skip DeduplicateExec entirely — query returns
    // both rows and pays no sort/distinct cost.
    let cfg = TestConfigBuilder::new("read_dedup_skip").with_buffer_mode(BufferMode::Enabled).build();
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let now = chrono::Utc::now().timestamp_micros();
    db.insert_records_batch(
        &project_id,
        "otel_logs_and_spans",
        vec![json_to_batch(vec![test_span_ts("id_a", "a", &project_id, now)])?],
        true,
        None,
    )
    .await?;
    db.insert_records_batch(
        &project_id,
        "otel_logs_and_spans",
        vec![json_to_batch(vec![test_span_ts("id_b", "b", &project_id, now + 1)])?],
        true,
        None,
    )
    .await?;

    let n = count(&db, &format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await?;
    assert_eq!(n, 2, "non-overlapping rows must not be deduplicated");
    Ok(())
}
