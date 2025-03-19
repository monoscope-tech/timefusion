#[cfg(test)]
#[allow(warnings)] // Suppress warnings for the test module
mod sqllogictest_tests {
    use sqllogictest::{Runner, DefaultColumnType, DBOutput, AsyncDB};
    use std::sync::Arc;
    use tokio;
    use timefusion::database::Database;
    use timefusion::utils::value_to_string;
    use std::error::Error as StdError;
    use datafusion::error::DataFusionError;
    use uuid::Uuid;

    // Custom error type to wrap anyhow::Error
    #[derive(Debug)]
    struct TestError(anyhow::Error);

    impl StdError for TestError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            self.0.source()
        }
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl From<anyhow::Error> for TestError {
        fn from(err: anyhow::Error) -> Self {
            TestError(err)
        }
    }

    impl From<DataFusionError> for TestError {
        fn from(err: DataFusionError) -> Self {
            TestError(anyhow::Error::from(err))
        }
    }

    // Define a custom database wrapper for sqllogictest
    struct TimeFusionDB {
        db: Arc<Database>,
        project_id: String, // Store the generated projectId
        id: String,        // Store the generated id
    }

    #[async_trait::async_trait]
    impl AsyncDB for TimeFusionDB {
        type Error = TestError;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            // Replace placeholders with generated UUIDs
            let sql = sql
                .replace("PROJECT_ID_PLACEHOLDER", &self.project_id)
                .replace("ID_PLACEHOLDER", &self.id);
            println!("Executing SQL: {}", sql);

            let df = self.db.query(&sql).await.map_err(|e| {
                println!("Query error: {:?}", e);
                TestError::from(e)
            })?;
            println!("Query executed successfully, collecting batches...");
            let batches = df.collect().await.map_err(|e| {
                println!("Collect error: {:?}", e);
                TestError::from(e)
            })?;
            println!("Collected {} batches", batches.len());

            if batches.is_empty() {
                println!("No batches returned, assuming statement complete");
                return Ok(DBOutput::StatementComplete(0));
            }

            let mut rows = Vec::new();
            let mut row_count = 0;
            for batch in &batches {
                row_count += batch.num_rows();
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::new();
                    for col_idx in 0..batch.num_columns() {
                        let value = value_to_string(batch.column(col_idx).as_ref(), row_idx);
                        row.push(value);
                    }
                    rows.push(row);
                }
            }

            if sql.trim().to_lowercase().starts_with("insert") ||
               sql.trim().to_lowercase().starts_with("update") ||
               sql.trim().to_lowercase().starts_with("delete") {
                println!("Returning StatementComplete with row count: {}", row_count);
                Ok(DBOutput::StatementComplete(row_count as u64))
            } else {
                println!("Returning Rows with {} rows and {} columns", rows.len(), batches[0].num_columns());
                Ok(DBOutput::Rows { 
                    types: vec![DefaultColumnType::Text; batches[0].num_columns()], 
                    rows 
                })
            }
        }

        fn engine_name(&self) -> &str {
            "TimeFusion"
        }

        async fn shutdown(&mut self) {
            // No-op shutdown for testing purposes
        }
    }

    #[tokio::test]
    async fn run_sqllogictest() -> anyhow::Result<()> {
        // Initialize logging with a default level
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

        // Initialize the database
        let db: Arc<Database> = Arc::new(Database::new().await?);
        
        // Use an in-memory table for testing
        let storage_uri = "memory://test_table";
        println!("Creating events table: telemetry_events at {}", storage_uri);
        db.create_events_table("telemetry_events", storage_uri).await?;

        // Generate random UUIDs
        let project_id = Uuid::new_v4().to_string();
        let id = Uuid::new_v4().to_string();
        println!("Generated projectId: {}, id: {}", project_id, id);

        // Create a closure that implements MakeConnection
        let make_db = || async {
            Ok(TimeFusionDB { 
                db: db.clone(),
                project_id: project_id.clone(),
                id: id.clone(),
            }) as Result<TimeFusionDB, TestError>
        };

        // Create a runner with the connection factory
        let mut runner = Runner::new(make_db);

        // Specify the path to your .slt files
        let test_file = "tests/example.slt";
        println!("Running SQL logic test from file: {}", test_file);
        
        // Run the tests
        runner.run_file_async(test_file).await?;
        
        Ok(())
    }
}