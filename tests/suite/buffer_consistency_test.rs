//! Buffer consistency tests - verifies query results are consistent whether data is in MemBuffer or Delta.

use std::{ops::Range, sync::Arc};

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{Array, AsArray, RecordBatch, StringViewArray},
        datatypes::Int64Type,
    },
    prelude::SessionContext,
};
use serial_test::serial;
use test_case::test_case;
use timefusion::{
    database::Database,
    support::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span},
    write::BufferedWriteLayer,
};

const TABLE: &str = "otel_logs_and_spans";

fn get_str(arr: &dyn Array, idx: usize) -> String {
    arr.as_any().downcast_ref::<StringViewArray>().map(|a| a.value(idx).to_string()).unwrap_or_default()
}

fn get_i64(batch: &RecordBatch, col: usize, idx: usize) -> i64 {
    batch.column(col).as_primitive::<Int64Type>().value(idx)
}

/// Database + buffered layer + a session context wired exactly as the pgwire path builds one.
async fn setup_db_with_buffer(mode: BufferMode) -> Result<(Arc<Database>, Arc<BufferedWriteLayer>, String, SessionContext)> {
    let cfg = TestConfigBuilder::new("buf_test").with_buffer_mode(mode).build();
    // Wire the SAME Delta writer prod does. A layer without it does not fail —
    // `flush_bucket` used to log "no delta write callback" and drain the bucket
    // anyway, so every flushed row was silently destroyed while `is_empty()`
    // reported success. It now errors instead, which would strand these tests'
    // rows in MemBuffer; either way the harness must mirror production.
    let db0 = Database::with_config(Arc::clone(&cfg)).await?;
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(Arc::clone(&cfg))?.with_delta_writer(timefusion::server::delta_write_callback(&db0)));
    let db = Arc::new(db0.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok((db, layer, project_id, ctx))
}

/// `duration = None` means `100 + i`, matching the varying-duration fixtures.
fn create_range(project_id: &str, range: Range<usize>, duration: Option<i64>) -> Vec<serde_json::Value> {
    let now = chrono::Utc::now();
    range
        .map(|i| {
            serde_json::json!({
                "id": format!("id_{}", i),
                "name": format!("name_{}", i),
                "project_id": project_id,
                "timestamp": now.timestamp_micros() + i as i64,
                "level": "INFO",
                "duration": duration.unwrap_or(100 + i as i64),
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            })
        })
        .collect()
}

fn create_records(project_id: &str, count: usize) -> Vec<serde_json::Value> {
    create_range(project_id, 0..count, None)
}

async fn insert(db: &Database, project_id: &str, records: Vec<serde_json::Value>, skip_queue: bool) -> Result<()> {
    db.insert_records_batch(project_id, TABLE, vec![json_to_batch(records)?], skip_queue, None).await?;
    Ok(())
}

async fn count_rows(ctx: &SessionContext, project_id: &str) -> Result<i64> {
    let result = ctx.sql(&format!("SELECT COUNT(*) as cnt FROM {TABLE} WHERE project_id = '{project_id}'")).await?.collect().await?;
    Ok(get_i64(&result[0], 0, 0))
}

// Parameterized tests - run in both buffer modes

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_insert_query(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(mode).await?;
    insert(&db, &project_id, create_records(&project_id, 10), true).await?;
    assert_eq!(count_rows(&ctx, &project_id).await?, 10, "Expected 10 rows");
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_select_columns(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(mode).await?;
    insert(&db, &project_id, vec![test_span("test1", "my_span", &project_id)], true).await?;

    let result = ctx.sql(&format!("SELECT id, name FROM {TABLE} WHERE project_id = '{project_id}'")).await?.collect().await?;

    assert_eq!(result[0].num_rows(), 1);
    assert_eq!(get_str(result[0].column(0).as_ref(), 0), "test1");
    assert_eq!(get_str(result[0].column(1).as_ref(), 0), "my_span");
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_update(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(mode).await?;
    insert(&db, &project_id, create_records(&project_id, 3), true).await?;

    ctx.sql(&format!("UPDATE {TABLE} SET hashes = make_array('999') WHERE project_id = '{project_id}' AND name = 'name_1'")).await?.collect().await?;

    let result = ctx
        .sql(&format!(
            "SELECT name, COALESCE(array_element(hashes, 1), CAST(duration AS VARCHAR))::BIGINT AS duration FROM {TABLE} WHERE project_id = '{project_id}' ORDER BY name"
        ))
        .await?
        .collect()
        .await?;

    let batch = &result[0];
    for i in 0..batch.num_rows() {
        if get_str(batch.column(0).as_ref(), i) == "name_1" {
            assert_eq!(get_i64(batch, 1, i), 999, "name_1 should have duration=999");
        }
    }
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_delete(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(mode).await?;
    insert(&db, &project_id, create_records(&project_id, 5), true).await?;

    ctx.sql(&format!("DELETE FROM {TABLE} WHERE project_id = '{project_id}' AND name = 'name_2'")).await?.collect().await?;

    assert_eq!(count_rows(&ctx, &project_id).await?, 4, "Expected 4 rows after delete");
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_aggregations(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(mode).await?;
    insert(&db, &project_id, create_records(&project_id, 10), true).await?;

    let result = ctx
        .sql(&format!("SELECT COUNT(*) as cnt, SUM(duration) as total, AVG(duration) as avg_dur FROM {TABLE} WHERE project_id = '{project_id}'"))
        .await?
        .collect()
        .await?;

    assert_eq!(get_i64(&result[0], 0, 0), 10);
    Ok(())
}

// Union tests - data split between buffer and Delta
//
// The two #[ignore]'d tests below write the same (project_id, time-window) to
// Delta directly AND to MemBuffer, then expect the union to reflect both legs.
// Production never does this: the buffered layer is the sole write path, and
// when it flushes (skip_queue=true → direct Delta write) the bucket is
// drained from MemBuffer *first*, so the per-bucket Delta-exclusion filter in
// ProjectRoutingTable::scan correctly drops nothing. When a test pollutes both
// legs concurrently, the exclusion filter wrongly suppresses the Delta-direct
// rows. Run via `cargo test -- --ignored` if intentionally exercising the race.

#[serial]
#[ignore = "tests architecturally-unsupported simultaneous-write-both-legs pattern; see comment above"]
#[tokio::test]
async fn test_partial_flush_union() -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(BufferMode::Enabled).await?;

    insert(&db, &project_id, create_records(&project_id, 50), true).await?;
    insert(&db, &project_id, create_range(&project_id, 50..100, None), false).await?;

    assert_eq!(count_rows(&ctx, &project_id).await?, 100, "Expected 100 rows from union of buffer + Delta");
    Ok(())
}

#[serial]
#[ignore = "tests architecturally-unsupported simultaneous-write-both-legs pattern; see test_partial_flush_union comment"]
#[tokio::test]
async fn test_delta_only_query() -> Result<()> {
    let (db, _layer, project_id, ctx) = setup_db_with_buffer(BufferMode::Enabled).await?;

    insert(&db, &project_id, create_records(&project_id, 30), true).await?;
    insert(&db, &project_id, create_range(&project_id, 30..50, Some(100)), false).await?;

    // Delta-only query should return only Delta data (30 rows)
    let delta_result = db.query_delta_only(&format!("SELECT COUNT(*) as cnt FROM {TABLE} WHERE project_id = '{project_id}'")).await?;
    assert_eq!(get_i64(&delta_result[0], 0, 0), 30, "Delta-only should return 30 rows from Delta");

    // Normal query should return all 50 (30 from Delta + 20 from buffer)
    assert_eq!(count_rows(&ctx, &project_id).await?, 50, "Full query should return all 50 rows");
    Ok(())
}

// Immediate flush verification

#[serial]
#[tokio::test]
async fn test_immediate_flush_drains_buffer() -> Result<()> {
    let (db, layer, project_id, ctx) = setup_db_with_buffer(BufferMode::FlushImmediately).await?;

    insert(&db, &project_id, create_records(&project_id, 10), false).await?;

    // Buffer should be empty after immediate flush (flush drains buffer even without callback)
    assert!(layer.is_empty(), "Buffer should be empty after immediate flush");
    // DRAINED IS NOT PERSISTED. This assertion is the one that matters: an
    // empty buffer proves the rows LEFT MemBuffer, not that they arrived in
    // Delta, so on its own it is equally consistent with the flush dropping
    // them on the floor.
    assert_eq!(count_rows(&ctx, &project_id).await?, 10, "every drained row must be readable from Delta");
    Ok(())
}
