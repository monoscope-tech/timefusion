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
    use log::debug; // Add logging for debugging

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
    }

    #[async_trait::async_trait]
    impl AsyncDB for TimeFusionDB {
        type Error = TestError;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            debug!("Executing SQL: {}", sql);

            let df = self.db.query(sql).await.map_err(TestError::from)?;
            debug!("Query executed successfully, collecting batches...");
            let batches = df.collect().await.map_err(TestError::from)?;
            debug!("Collected {} batches", batches.len());

            if batches.is_empty() {
                debug!("No batches returned, assuming statement complete");
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
                debug!("Returning StatementComplete with row count: {}", row_count);
                Ok(DBOutput::StatementComplete(row_count as u64))
            } else {
                debug!("Returning Rows with {} rows and {} columns", rows.len(), batches[0].num_columns());
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
        // Initialize logging
        env_logger::init();

        // Initialize the database
        let db: Arc<Database> = Arc::new(Database::new().await?);
        
        // Use an in-memory table for testing
        let storage_uri = "memory://test_table";
        debug!("Creating events table: telemetry_events at {}", storage_uri);
        db.create_events_table("telemetry_events", storage_uri).await?;

        // Create a closure that implements MakeConnection
        let make_db = || async {
            Ok(TimeFusionDB { db: db.clone() }) as Result<TimeFusionDB, TestError>
        };

        // Create a runner with the connection factory
        let mut runner = Runner::new(make_db);

        // Specify the path to your .slt files
        let test_file = "tests/example.slt";
        debug!("Running SQL logic test from file: {}", test_file);
        
        // Run the tests
        runner.run_file_async(test_file).await?;
        
        Ok(())
    }
}