use std::sync::Arc;

use anyhow::Result;
use datafusion::{arrow::array::AsArray, prelude::SessionContext};
use serial_test::serial;
use timefusion::{
    database::Database,
    support::test_helpers::{array_get_str as get_str, *},
};

async fn setup_test_database() -> Result<(Database, SessionContext)> {
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

/// One span into `project`, committed straight to Delta (skip_queue).
async fn insert_span(db: &Database, project: &str, id: &str, name: &str) -> Result<()> {
    let batch = json_to_batch(vec![test_span(id, name, project)])?;
    db.insert_records_batch(project, "otel_logs_and_spans", vec![batch], true, None).await?;
    Ok(())
}

async fn count(ctx: &SessionContext, project: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{project}'");
    Ok(ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0))
}

// `config::init_config` is OnceLock-cached, so only the first test's
// TIMEFUSION_TABLE_PREFIX takes effect and the rest share one Delta table; these
// tests are #[ignore]d and must be run one at a time (`-- --ignored`).

#[serial]
#[ignore = "shares OnceLock config across tests in CI; see file-level comment"]
#[tokio::test(flavor = "multi_thread")]
async fn test_add_actions_table_statistics() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    for i in 0..3 {
        insert_span(&db, "stats_project", &format!("id_{i}"), &format!("span_{i}")).await?;
    }

    let cnt = count(&ctx, "stats_project").await?;
    assert!(cnt >= 3, "Expected at least 3 records, got {cnt}");

    db.shutdown().await?;
    Ok(())
}

#[serial]
#[ignore = "shares OnceLock config across tests in CI; see file-level comment"]
#[tokio::test(flavor = "multi_thread")]
async fn test_partition_column_ordering() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    insert_span(&db, "partition_project", "partition_test_id", "partition_test").await?;

    let result = ctx.sql("SELECT project_id, date, id FROM otel_logs_and_spans WHERE project_id = 'partition_project'").await?.collect().await?;

    assert_eq!(result[0].num_rows(), 1);
    assert_eq!(get_str(result[0].column(0).as_ref(), 0), "partition_project");

    db.shutdown().await?;
    Ok(())
}

#[serial]
#[ignore = "shares OnceLock config across tests in CI; see file-level comment"]
#[tokio::test(flavor = "multi_thread")]
async fn test_table_state_refresh() -> Result<()> {
    let (db, ctx) = setup_test_database().await?;

    insert_span(&db, "refresh_project", "refresh_id_1", "span_1").await?;
    assert_eq!(count(&ctx, "refresh_project").await?, 1);

    insert_span(&db, "refresh_project", "refresh_id_2", "span_2").await?;
    assert_eq!(count(&ctx, "refresh_project").await?, 2);

    db.shutdown().await?;
    Ok(())
}
