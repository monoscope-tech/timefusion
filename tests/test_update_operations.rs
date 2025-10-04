#[cfg(test)]
mod test_update_operations {
    use anyhow::Result;
    use datafusion::arrow;
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
    async fn test_update_query() -> Result<()> {
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
                "level": "INFO",
                "status_code": "OK",
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

        // Test UPDATE with WHERE clause
        info!("Executing UPDATE query");
        let df = ctx.sql("UPDATE otel_logs_and_spans SET duration = 500 WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let result = df.collect().await?;
        
        // Check that we got a result
        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);
        
        // The result should contain the number of rows updated
        let column = batch.column(0);
        let array = column.as_primitive::<arrow::datatypes::Int64Type>();
        let rows_updated = array.value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify the update by querying the table
        let df = ctx.sql("SELECT id, name, duration FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;
        
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);
        
        // Get column indices by name
        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();
        let duration_col_idx = batch.schema().fields().iter().position(|f| f.name() == "duration").unwrap();
        
        // Check Bob's duration was updated to 500
        let name_col = batch.column(name_col_idx).as_string::<i32>();
        let duration_col = batch.column(duration_col_idx).as_primitive::<arrow::datatypes::Int64Type>();
        
        // Find Bob's row and check the duration
        for i in 0..batch.num_rows() {
            if name_col.value(i) == "Bob" {
                assert_eq!(duration_col.value(i), 500, "Bob's duration should be updated to 500");
            } else if name_col.value(i) == "Alice" {
                assert_eq!(duration_col.value(i), 100, "Alice's duration should remain 100");
            } else if name_col.value(i) == "Charlie" {
                assert_eq!(duration_col.value(i), 300, "Charlie's duration should remain 300");
            }
        }

        Ok(())
    }

    // TODO: Update this test to use otel_logs_and_spans schema
    // #[serial]
    // #[tokio::test]
    #[allow(dead_code)]
    async fn test_update_multiple_columns() -> Result<()> {
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

        // Create a Delta table by inserting data directly through the database
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("value1", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("value2", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("project_id", arrow::datatypes::DataType::Utf8, false),
        ]));
        
        // Create test data
        let id_array = arrow::array::StringArray::from(vec!["1", "2"]);
        let name_array = arrow::array::StringArray::from(vec!["Alice", "Bob"]);
        let value1_array = arrow::array::Int32Array::from(vec![100, 200]);
        let value2_array = arrow::array::Int32Array::from(vec![1000, 2000]);
        let project_array = arrow::array::StringArray::from(vec!["test_project", "test_project"]);
        
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value1_array),
                Arc::new(value2_array),
                Arc::new(project_array),
            ],
        )?;
        
        // Insert data through the database to create the Delta table
        db.insert_records_batch("test_project", "test_multi_update", vec![batch], true).await?;

        // Update multiple columns
        let df = ctx.sql("UPDATE test_multi_update SET value1 = 999, value2 = 9999 WHERE project_id = 'test_project' AND id = '1'").await?;
        let result = df.collect().await?;
        
        assert_eq!(result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 1);

        // Verify the update
        let df = ctx.sql("SELECT * FROM test_multi_update WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;
        let batch = &results[0];
        
        let value1_col = batch.column(2).as_primitive::<arrow::datatypes::Int32Type>();
        let value2_col = batch.column(3).as_primitive::<arrow::datatypes::Int32Type>();
        
        assert_eq!(value1_col.value(0), 999);   // Alice updated
        assert_eq!(value2_col.value(0), 9999);  // Alice updated
        assert_eq!(value1_col.value(1), 200);   // Bob unchanged
        assert_eq!(value2_col.value(1), 2000);  // Bob unchanged

        Ok(())
    }
}