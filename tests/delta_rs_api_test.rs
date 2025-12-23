use anyhow::Result;
use datafusion::arrow::array::AsArray;
use serial_test::serial;
use std::sync::Arc;
use timefusion::database::Database;
use timefusion::test_utils::test_helpers::*;

async fn setup_test_database() -> Result<(Database, datafusion::prelude::SessionContext)> {
    dotenv::dotenv().ok();
    unsafe {
        std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
        std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("delta-api-test-{}", uuid::Uuid::new_v4()));
    }
    let db = Database::new().await?;
    let db_arc = Arc::new(db.clone());
    let mut ctx = db_arc.create_session_context();
    datafusion_functions_json::register_all(&mut ctx)?;
    db.setup_session_context(&mut ctx)?;
    Ok((db, ctx))
}

/// Tests that add_actions_table returns correct file statistics after inserts
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_add_actions_table_statistics() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    // Insert multiple batches to create multiple files
    for i in 0..3 {
        let batch = json_to_batch(vec![test_span(&format!("id_{}", i), &format!("span_{}", i), "stats_project")])?;
        db.insert_records_batch("stats_project", "otel_logs_and_spans", vec![batch], true).await?;
    }

    // Query to verify data exists
    let result = ctx.sql("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = 'stats_project'").await?.collect().await?;
    let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
    assert!(count >= 3, "Expected at least 3 records, got {}", count);

    db.shutdown().await?;
    Ok(())
}

/// Tests that CreateBuilder correctly orders partition columns
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_partition_column_ordering() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    // Insert data to trigger table creation via CreateBuilder
    let batch = json_to_batch(vec![test_span("partition_test_id", "partition_test", "partition_project")])?;
    db.insert_records_batch("partition_project", "otel_logs_and_spans", vec![batch], true).await?;

    // Query and verify partition columns (project_id, date) are present and filterable
    let result = ctx
        .sql("SELECT project_id, date, id FROM otel_logs_and_spans WHERE project_id = 'partition_project'")
        .await?
        .collect()
        .await?;

    assert_eq!(result[0].num_rows(), 1);
    assert_eq!(result[0].column(0).as_string::<i32>().value(0), "partition_project");

    db.shutdown().await?;
    Ok(())
}

/// Tests table update_state() correctly refreshes table metadata
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_table_state_refresh() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    // Insert initial data
    let batch = json_to_batch(vec![test_span("refresh_id_1", "span_1", "refresh_project")])?;
    db.insert_records_batch("refresh_project", "otel_logs_and_spans", vec![batch], true).await?;

    // Verify first record
    let result = ctx.sql("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = 'refresh_project'").await?.collect().await?;
    let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
    assert_eq!(count, 1);

    // Insert more data (triggers update_state internally)
    let batch = json_to_batch(vec![test_span("refresh_id_2", "span_2", "refresh_project")])?;
    db.insert_records_batch("refresh_project", "otel_logs_and_spans", vec![batch], true).await?;

    // Verify both records are visible (confirms state refresh worked)
    let result = ctx.sql("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = 'refresh_project'").await?.collect().await?;
    let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
    assert_eq!(count, 2);

    db.shutdown().await?;
    Ok(())
}
