#[cfg(test)]
mod test_dml_operations {
    use anyhow::Result;
    use datafusion::arrow;
    use datafusion::arrow::array::AsArray;
    use serial_test::serial;
    use std::sync::Arc;
    use timefusion::database::Database;
    use tracing::{Level, info};

    fn init_tracing() {
        let subscriber = tracing_subscriber::fmt().with_max_level(Level::INFO).with_target(false).finish();
        let _ = tracing::subscriber::set_global_default(subscriber);
    }

    struct EnvGuard {
        keys: Vec<(String, Option<String>)>,
    }

    // SAFETY: All tests using EnvGuard are marked #[serial], ensuring single-threaded
    // execution. No other threads read env vars during test execution.
    impl EnvGuard {
        fn set(key: &str, value: &str) -> Self {
            let old = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            Self {
                keys: vec![(key.to_string(), old)],
            }
        }

        fn add(&mut self, key: &str, value: &str) {
            let old = std::env::var(key).ok();
            unsafe { std::env::set_var(key, value) };
            self.keys.push((key.to_string(), old));
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, old) in &self.keys {
                match old {
                    Some(v) => unsafe { std::env::set_var(key, v) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    fn setup_test_env() -> EnvGuard {
        dotenv::dotenv().ok();
        let mut guard = EnvGuard::set("AWS_S3_BUCKET", "timefusion-tests");
        guard.add("TIMEFUSION_TABLE_PREFIX", &format!("test-{}", uuid::Uuid::new_v4()));
        guard
    }

    // ==========================================================================
    // Delta-Only DML Tests (no buffered layer - operations go directly to Delta)
    // These tests verify that UPDATE/DELETE work correctly on Delta Lake tables.
    // ==========================================================================

    fn create_test_records(now: chrono::DateTime<chrono::Utc>) -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "id": "1",
                "name": "Alice",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 100,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "2",
                "name": "Bob",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 200,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "3",
                "name": "Charlie",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 300,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
        ]
    }

    // UPDATE Tests

    #[serial]
    #[tokio::test]
    async fn test_update_query() -> Result<()> {
        init_tracing();
        let _env_guard = setup_test_env();

        let db = Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Test UPDATE with WHERE clause
        info!("Executing UPDATE query");
        let df = ctx.sql("UPDATE otel_logs_and_spans SET duration = 500 WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let result = df.collect().await?;

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);

        let rows_updated = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify the update
        let df = ctx.sql("SELECT id, name, duration FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);

        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();
        let duration_col_idx = batch.schema().fields().iter().position(|f| f.name() == "duration").unwrap();

        let name_col = batch.column(name_col_idx).as_string::<i32>();
        let duration_col = batch.column(duration_col_idx).as_primitive::<arrow::datatypes::Int64Type>();

        for i in 0..batch.num_rows() {
            match name_col.value(i) {
                "Bob" => assert_eq!(duration_col.value(i), 500, "Bob's duration should be updated to 500"),
                "Alice" => assert_eq!(duration_col.value(i), 100, "Alice's duration should remain 100"),
                "Charlie" => assert_eq!(duration_col.value(i), 300, "Charlie's duration should remain 300"),
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    // DELETE Tests

    #[serial]
    #[tokio::test]
    async fn test_delete_with_predicate() -> Result<()> {
        init_tracing();
        let _env_guard = setup_test_env();

        let db = Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Test DELETE with WHERE clause
        info!("Executing DELETE query");
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);

        let rows_deleted = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 1, "Expected 1 row to be deleted");

        // Verify the delete
        let df = ctx.sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 2); // Only Alice and Charlie should remain

        let id_col_idx = batch.schema().fields().iter().position(|f| f.name() == "id").unwrap();
        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();

        let id_col = batch.column(id_col_idx).as_string::<i32>();
        let name_col = batch.column(name_col_idx).as_string::<i32>();

        assert_eq!(id_col.value(0), "1");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(id_col.value(1), "3");
        assert_eq!(name_col.value(1), "Charlie");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_delete_all_matching() -> Result<()> {
        setup_test_env();

        let db = Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = vec![
            serde_json::json!({
                "id": "1",
                "name": "Record1",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 100,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "2",
                "name": "Record2",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 200,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "3",
                "name": "Record3",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 300,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "4",
                "name": "Record4",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 400,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
        ];

        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Delete all ERROR level records
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;

        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 3, "Expected 3 rows to be deleted");

        // Verify only the INFO record remains
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(count, 1, "Expected 1 row to remain");

        // Verify it's the right record
        let df = ctx.sql("SELECT id, level FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let batch = &results[0];

        let id_col = batch.column(0).as_string::<i32>();
        let level_col = batch.column(1).as_string::<i32>();

        assert_eq!(id_col.value(0), "2");
        assert_eq!(level_col.value(0), "INFO");

        Ok(())
    }

    // ==========================================================================
    // Delta UPDATE with multiple columns test
    // ==========================================================================

    #[serial]
    #[tokio::test]
    async fn test_update_multiple_columns() -> Result<()> {
        init_tracing();
        let _env_guard = setup_test_env();

        let db = Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        // Insert directly to Delta (skip_queue=true)
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Update multiple columns at once
        info!("Executing multi-column UPDATE query");
        let df = ctx
            .sql("UPDATE otel_logs_and_spans SET duration = 999, level = 'WARN' WHERE project_id = 'test_project' AND name = 'Alice'")
            .await?;
        let result = df.collect().await?;

        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify both columns were updated
        let df = ctx
            .sql("SELECT name, duration, level FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Alice'")
            .await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let duration_idx = batch.schema().fields().iter().position(|f| f.name() == "duration").unwrap();
        let level_idx = batch.schema().fields().iter().position(|f| f.name() == "level").unwrap();

        let duration_col = batch.column(duration_idx).as_primitive::<arrow::datatypes::Int64Type>();
        let level_col = batch.column(level_idx).as_string::<i32>();

        assert_eq!(duration_col.value(0), 999, "Duration should be updated to 999");
        assert_eq!(level_col.value(0), "WARN", "Level should be updated to WARN");

        Ok(())
    }

    // ==========================================================================
    // Delta DELETE then verify row counts test
    // ==========================================================================

    #[serial]
    #[tokio::test]
    async fn test_delete_verify_counts() -> Result<()> {
        init_tracing();
        let _env_guard = setup_test_env();

        let db = Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();

        // Create 5 records
        let records = vec![
            serde_json::json!({
                "id": "1", "name": "R1", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 100, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "2", "name": "R2", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 200, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "3", "name": "R3", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "ERROR", "status_code": "ERROR",
                "duration": 300, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "4", "name": "R4", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 400, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "5", "name": "R5", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "ERROR", "status_code": "ERROR",
                "duration": 500, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
        ];

        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Verify initial count
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let initial_count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(initial_count, 5, "Should have 5 rows initially");

        // Delete ERROR records
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;
        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 2, "Should delete 2 ERROR records");

        // Verify final count
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let final_count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(final_count, 3, "Should have 3 rows after delete");

        Ok(())
    }
}
