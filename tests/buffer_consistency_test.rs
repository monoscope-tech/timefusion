//! Buffer consistency tests - verifies query results are consistent whether data is in MemBuffer or Delta.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{Array, AsArray, StringViewArray};
use serial_test::serial;
use test_case::test_case;
use timefusion::{
    buffered_write_layer::BufferedWriteLayer,
    database::Database,
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span},
};

fn get_str(arr: &dyn Array, idx: usize) -> String {
    arr.as_any().downcast_ref::<StringViewArray>().map(|a| a.value(idx).to_string()).unwrap_or_default()
}

async fn setup_db_with_buffer(mode: BufferMode) -> Result<(Arc<Database>, Arc<BufferedWriteLayer>, String)> {
    let cfg = TestConfigBuilder::new("buf_test").with_buffer_mode(mode).build();
    // SAFETY: walrus-rust reads WALRUS_DATA_DIR from environment. We use #[serial] on all tests
    // to prevent concurrent access to this process-global state. This is inherently racy but
    // acceptable for tests since they run sequentially.
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let layer = Arc::new(BufferedWriteLayer::with_config(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    Ok((db, layer, project_id))
}

fn create_records(project_id: &str, count: usize) -> Vec<serde_json::Value> {
    let now = chrono::Utc::now();
    (0..count)
        .map(|i| {
            serde_json::json!({
                "id": format!("id_{}", i),
                "name": format!("name_{}", i),
                "project_id": project_id,
                "timestamp": now.timestamp_micros() + i as i64,
                "level": "INFO",
                "duration": 100 + i as i64,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            })
        })
        .collect()
}

// =============================================================================
// Parameterized tests - run in both buffer modes
// =============================================================================

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_insert_query(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(mode).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let records = create_records(&project_id, 10);
    let batch = json_to_batch(records)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

    let result = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?
        .collect()
        .await?;

    let count = result[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(count, 10, "Expected 10 rows");
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_select_columns(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(mode).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let batch = json_to_batch(vec![test_span("test1", "my_span", &project_id)])?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

    let result = ctx
        .sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?
        .collect()
        .await?;

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
    let (db, _layer, project_id) = setup_db_with_buffer(mode).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let records = create_records(&project_id, 3);
    let batch = json_to_batch(records)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

    ctx.sql(&format!(
        "UPDATE otel_logs_and_spans SET duration = 999 WHERE project_id = '{}' AND name = 'name_1'",
        project_id
    ))
    .await?
    .collect()
    .await?;

    let result = ctx
        .sql(&format!(
            "SELECT name, duration FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY name",
            project_id
        ))
        .await?
        .collect()
        .await?;

    let batch = &result[0];
    for i in 0..batch.num_rows() {
        let name = get_str(batch.column(0).as_ref(), i);
        let duration = batch.column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(i);
        if name == "name_1" {
            assert_eq!(duration, 999, "name_1 should have duration=999");
        }
    }
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_delete(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(mode).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let records = create_records(&project_id, 5);
    let batch = json_to_batch(records)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

    ctx.sql(&format!(
        "DELETE FROM otel_logs_and_spans WHERE project_id = '{}' AND name = 'name_2'",
        project_id
    ))
    .await?
    .collect()
    .await?;

    let result = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?
        .collect()
        .await?;

    let count = result[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(count, 4, "Expected 4 rows after delete");
    Ok(())
}

#[test_case(BufferMode::Enabled ; "buffered")]
#[test_case(BufferMode::FlushImmediately ; "immediate")]
#[serial]
#[tokio::test]
async fn test_aggregations(mode: BufferMode) -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(mode).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let records = create_records(&project_id, 10);
    let batch = json_to_batch(records)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

    let result = ctx
        .sql(&format!(
            "SELECT COUNT(*) as cnt, SUM(duration) as total, AVG(duration) as avg_dur FROM otel_logs_and_spans WHERE project_id = '{}'",
            project_id
        ))
        .await?
        .collect()
        .await?;

    let batch = &result[0];
    let cnt = batch.column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(cnt, 10);
    Ok(())
}

// =============================================================================
// Union tests - data split between buffer and Delta
// =============================================================================

#[serial]
#[tokio::test]
async fn test_partial_flush_union() -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(BufferMode::Enabled).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    // Insert first batch directly to Delta (skip_queue=true)
    let batch1 = json_to_batch(create_records(&project_id, 50))?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch1], true).await?;

    // Insert second batch to buffer only (skip_queue=false, no callback so no flush to Delta)
    let now = chrono::Utc::now();
    let records2: Vec<_> = (50..100)
        .map(|i| {
            serde_json::json!({
                "id": format!("id_{}", i),
                "name": format!("name_{}", i),
                "project_id": &project_id,
                "timestamp": now.timestamp_micros() + i as i64,
                "level": "INFO",
                "duration": 100 + i as i64,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            })
        })
        .collect();
    let batch2 = json_to_batch(records2)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch2], false).await?;

    // Query should return all 100 rows (50 from Delta + 50 from buffer)
    let result = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?
        .collect()
        .await?;

    let count = result[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(count, 100, "Expected 100 rows from union of buffer + Delta");
    Ok(())
}

#[serial]
#[tokio::test]
async fn test_delta_only_query() -> Result<()> {
    let (db, _layer, project_id) = setup_db_with_buffer(BufferMode::Enabled).await?;

    // Insert directly to Delta (skip_queue=true)
    let batch1 = json_to_batch(create_records(&project_id, 30))?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch1], true).await?;

    // Insert to buffer only (skip_queue=false, no callback so stays in buffer)
    let now = chrono::Utc::now();
    let records2: Vec<_> = (30..50)
        .map(|i| {
            serde_json::json!({
                "id": format!("id_{}", i),
                "name": format!("name_{}", i),
                "project_id": &project_id,
                "timestamp": now.timestamp_micros() + i as i64,
                "level": "INFO",
                "duration": 100,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            })
        })
        .collect();
    let batch2 = json_to_batch(records2)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch2], false).await?;

    // Delta-only query should return only Delta data (30 rows)
    let delta_result = db
        .query_delta_only(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?;

    let delta_count = delta_result[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(delta_count, 30, "Delta-only should return 30 rows from Delta");

    // Normal query should return all 50 (30 from Delta + 20 from buffer)
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let full_result = ctx
        .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
        .await?
        .collect()
        .await?;

    let full_count = full_result[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
    assert_eq!(full_count, 50, "Full query should return all 50 rows");
    Ok(())
}

// =============================================================================
// Immediate flush verification
// =============================================================================

#[serial]
#[tokio::test]
async fn test_immediate_flush_drains_buffer() -> Result<()> {
    let (db, layer, project_id) = setup_db_with_buffer(BufferMode::FlushImmediately).await?;

    // Insert with immediate mode through buffer (skip_queue=false)
    let batch = json_to_batch(create_records(&project_id, 10))?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false).await?;

    // Buffer should be empty after immediate flush (flush drains buffer even without callback)
    assert!(layer.is_empty(), "Buffer should be empty after immediate flush");
    Ok(())
}
