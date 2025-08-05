#[cfg(test)]
mod sqllogictest_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use datafusion_postgres::ServerOptions;
    use dotenv::dotenv;
    use serial_test::serial;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use std::{
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
        fmt,
    };
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::{NoTls, Row};
    use uuid::Uuid;

    // Custom error type that wraps both anyhow and tokio_postgres errors
    #[derive(Debug)]
    enum TestError {
        Postgres(tokio_postgres::Error),
        Other(String),
    }

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TestError::Postgres(e) => write!(f, "Postgres error: {}", e),
                TestError::Other(s) => write!(f, "Error: {}", s),
            }
        }
    }

    impl std::error::Error for TestError {}

    impl From<tokio_postgres::Error> for TestError {
        fn from(e: tokio_postgres::Error) -> Self {
            TestError::Postgres(e)
        }
    }

    impl From<anyhow::Error> for TestError {
        fn from(e: anyhow::Error) -> Self {
            TestError::Other(e.to_string())
        }
    }

    struct TestDB {
        client: tokio_postgres::Client,
    }

    #[async_trait]
    impl AsyncDB for TestDB {
        type Error = TestError;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let sql = sql.trim();
            println!("Executing SQL: {}", sql);
            let is_query = sql.to_lowercase().starts_with("select");

            if !is_query {
                let affected = self.client.execute(sql, &[]).await?;
                println!("Statement executed, {} rows affected", affected);
                return Ok(DBOutput::StatementComplete(affected as u64));
            }

            let rows = self.client.query(sql, &[]).await?;
            println!("Query returned {} rows", rows.len());
            if rows.is_empty() {
                return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
            }

            let types = rows[0]
                .columns()
                .iter()
                .map(|col| match col.type_().name() {
                    "int2" | "int4" | "int8" => DefaultColumnType::Integer,
                    _ => DefaultColumnType::Text,
                })
                .collect();

            let result_rows = rows.iter().map(format_row).collect();

            Ok(DBOutput::Rows { types, rows: result_rows })
        }

        fn engine_name(&self) -> &str {
            "timefusion-postgres"
        }

        async fn shutdown(&mut self) {}
    }

    fn format_row(row: &Row) -> Vec<String> {
        row.columns()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let type_name = col.type_().name();

                match type_name {
                    "int2" => row
                        .try_get::<_, Option<i16>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int2".to_string()),
                    "int4" => row
                        .try_get::<_, Option<i32>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int4".to_string()),
                    "int8" => row
                        .try_get::<_, Option<i64>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:int8".to_string()),
                    "float4" | "float8" | "numeric" => row
                        .try_get::<_, Option<f64>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:float".to_string()),
                    "bool" => row
                        .try_get::<_, Option<bool>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "error:bool".to_string()),
                    "timestamp" => row
                        .try_get::<_, Option<chrono::NaiveDateTime>>(i)
                        .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| {
                            row.try_get::<_, Option<String>>(i)
                                .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                                .unwrap_or_else(|_| "[timestamp]".to_string())
                        }),
                    _ => row
                        .try_get::<_, Option<String>>(i)
                        .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| type_name.to_string()),
                }
            })
            .collect()
    }

    async fn connect_with_retry(timeout: Duration) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let start = Instant::now();
        let conn_string = "host=localhost port=5433 user=postgres password=postgres";

        while start.elapsed() < timeout {
            match tokio_postgres::connect(conn_string, NoTls).await {
                Ok((client, connection)) => {
                    let handle = tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                    return Ok((client, handle));
                }
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }

        // Final attempt
        let (client, connection) = tokio_postgres::connect(conn_string, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok((client, handle))
    }

    async fn start_test_server() -> Result<Arc<Notify>> {
        let test_id = Uuid::new_v4().to_string();
        dotenv().ok();

        unsafe {
            std::env::set_var("PGWIRE_PORT", "5433");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-slt-{}", test_id));
        }

        // Use a shareable notification
        let shutdown_signal = Arc::new(Notify::new());
        let shutdown_signal_clone = shutdown_signal.clone();

        tokio::spawn(async move {
            let db = Database::new().await.expect("Failed to create database");
            let mut session_context = db.create_session_context();
            db.setup_session_context(&mut session_context).expect("Failed to setup session context");

            let opts = ServerOptions::new()
                .with_port(5433)
                .with_host("0.0.0.0".to_string());

            // Wait for shutdown signal or server termination
            tokio::select! {
                _ = shutdown_signal_clone.notified() => {},
                res = datafusion_postgres::serve(Arc::new(session_context), &opts) => {
                    if let Err(e) = res {
                        eprintln!("PGWire server error: {:?}", e);
                    }
                }
            }
        });

        // Wait for server to be ready
        let _ = connect_with_retry(Duration::from_secs(5)).await?;

        Ok(shutdown_signal)
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn run_sqllogictest() -> Result<()> {
        // Wrap the entire test in a timeout
        tokio::time::timeout(Duration::from_secs(120), async {
        let shutdown_signal = start_test_server().await?;

        let _factory = || async move {
            let (client, _) = connect_with_retry(Duration::from_secs(3)).await?;
            Ok::<TestDB, TestError>(TestDB { client })
        };

        // Auto-discover all .slt test files
        let test_dir = Path::new("tests");
        let mut test_files = Vec::new();
        
        if test_dir.is_dir() {
            for entry in std::fs::read_dir(test_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("slt") {
                    test_files.push(path);
                }
            }
        }
        
        // Sort files for consistent test order
        test_files.sort();
        
        println!("Found {} .slt test files", test_files.len());
        for file in &test_files {
            println!("  - {}", file.display());
        }

        let mut all_passed = true;
        for test_file in test_files {
            let test_path = test_file.as_path();
            println!("Running SQLLogicTest: {}", test_path.display());
            
            let factory_clone = || async move {
                let (client, _) = connect_with_retry(Duration::from_secs(3)).await?;
                Ok::<TestDB, TestError>(TestDB { client })
            };
            
            // Add timeout for individual test files (30 seconds each)
            let test_result = tokio::time::timeout(
                Duration::from_secs(30),
                sqllogictest::Runner::new(factory_clone).run_file_async(test_path)
            ).await;
            
            match test_result {
                Ok(Ok(_)) => println!("✓ {} passed", test_path.display()),
                Ok(Err(e)) => {
                    eprintln!("✗ {} failed: {:?}", test_path.display(), e);
                    all_passed = false;
                }
                Err(_) => {
                    eprintln!("✗ {} timed out after 30 seconds", test_path.display());
                    all_passed = false;
                }
            }
        }

        // Always shut down the server
        shutdown_signal.notify_one();

        if all_passed {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Some SQLLogicTests failed"))
        }
        }).await
        .map_err(|_| anyhow::anyhow!("Test timed out after 120 seconds"))?
    }
}
