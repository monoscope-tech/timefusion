#[cfg(test)]
mod test_delete_operations {
    use anyhow::Result;
    use datafusion::arrow::array::AsArray;
    use std::sync::Arc;
    use timefusion::database::Database;
    use tracing::{info, Level};
    use tracing_subscriber;
    use uuid;
    use dotenv;
    use serial_test::serial;
    use chrono;
    use serde_json;

    #[serial]
    #[tokio::test]
    async fn test_delete_with_predicate() -> Result<()> {
        // Initialize tracing
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_target(false)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        // Set up test S3 configuration
        dotenv::dotenv().ok();
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }

        // Initialize database
        let db = Database::new().await?;
        let db = Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Use otel_logs_and_spans table which has a predefined schema
        let now = chrono::Utc::now();
        let records = vec![
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
        ];
        
        // Convert JSON to batch
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        
        // Insert data through the database to create the Delta table
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Test DELETE with WHERE clause
        info!("Executing DELETE query");
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;
        
        // Check that we got a result
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        
        // The result should contain the number of rows deleted
        let column = batch.column(0);
        let array = column.as_primitive::<arrow::datatypes::Int64Type>();
        let rows_deleted = array.value(0);
        assert_eq!(rows_deleted, 1, "Expected 1 row to be deleted");

        // Verify the delete by querying the table
        let df = ctx.sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;
        
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 2); // Only Alice and Charlie should remain
        
        // Get column indices by name
        let id_col_idx = batch.schema().fields().iter().position(|f| f.name() == "id").unwrap();
        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();
        
        // Verify that Bob was deleted
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
        // Set up test S3 configuration
        dotenv::dotenv().ok();
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }
        
        // Initialize database
        let db = Database::new().await?;
        let db = Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Insert test data with multiple records matching delete criteria
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
}