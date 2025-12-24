#[cfg(test)]
mod sqllogictest_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use datafusion_postgres::{ServerOptions, auth::AuthManager};
    use dotenv::dotenv;
    use serial_test::serial;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use std::{
        fmt,
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
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
            // Only print SQL in verbose mode
            if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                println!("Executing SQL: {}", sql);
            }
            let is_query = sql.to_lowercase().starts_with("select");

            if !is_query {
                let affected = self.client.execute(sql, &[]).await?;
                if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                    println!("Statement executed, {} rows affected", affected);
                }
                return Ok(DBOutput::StatementComplete(affected));
            }

            let rows = self.client.query(sql, &[]).await?;
            if std::env::var("SQLLOGICTEST_VERBOSE").is_ok() {
                println!("Query returned {} rows", rows.len());
            }
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

    async fn connect_with_retry(port: u16, timeout: Duration) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let start = Instant::now();
        let conn_string = format!("host=localhost port={} user=postgres password=postgres", port);

        while start.elapsed() < timeout {
            match tokio_postgres::connect(&conn_string, NoTls).await {
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
        let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok((client, handle))
    }

    async fn start_test_server() -> Result<(Arc<Notify>, u16)> {
        let test_id = Uuid::new_v4().to_string();
        dotenv().ok();

        // Use a unique port for each test run
        let port = 5433 + (std::process::id() % 100) as u16;
        unsafe {
            std::env::set_var("PGWIRE_PORT", port.to_string());
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-slt-{}", test_id));
        }

        // Use a shareable notification
        let shutdown_signal = Arc::new(Notify::new());
        let shutdown_signal_clone = shutdown_signal.clone();

        tokio::spawn(async move {
            let db = Database::new().await.expect("Failed to create database");
            let db = Arc::new(db);
            let mut session_context = db.clone().create_session_context();
            db.setup_session_context(&mut session_context).expect("Failed to setup session context");

            let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
            let auth_manager = Arc::new(AuthManager::new());

            // Wait for shutdown signal or server termination
            tokio::select! {
                _ = shutdown_signal_clone.notified() => {},
                res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(session_context), &opts, auth_manager) => {
                    if let Err(e) = res {
                        eprintln!("PGWire server error: {:?}", e);
                    }
                }
            }
        });

        // Wait for server to be ready
        let _ = connect_with_retry(port, Duration::from_secs(5)).await?;

        Ok((shutdown_signal, port))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    #[ignore] // Slow integration test - run with: cargo test --test sqllogictest -- --ignored
    async fn run_sqllogictest() -> Result<()> {
        let (shutdown_signal, port) = start_test_server().await?;

        let _factory = || async move {
            let (client, _) = connect_with_retry(port, Duration::from_secs(3)).await?;
            Ok::<TestDB, TestError>(TestDB { client })
        };

        // Auto-discover all .slt test files
        let test_dir = Path::new("tests/slt");
        let mut test_files = Vec::new();

        // Check if a specific test file is requested via environment variable
        let test_filter = std::env::var("SQLLOGICTEST_FILE").ok();

        // Pretty output mode
        let pretty_mode = std::env::var("SQLLOGICTEST_PRETTY").is_ok();

        if test_dir.is_dir() {
            for entry in std::fs::read_dir(test_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("slt") {
                    if let Some(ref filter) = test_filter {
                        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                        if filename.contains(filter) {
                            test_files.push(path);
                        }
                    } else {
                        test_files.push(path);
                    }
                }
            }
        }

        test_files.sort();

        if pretty_mode {
            println!("\n🧪 SQLLogicTest Runner");
            println!("{}", "=".repeat(50));
        }

        if let Some(ref filter) = test_filter {
            println!("\n📁 Filtering for test files containing: '{}'", filter);
        }

        println!("\n📋 Found {} test files:", test_files.len());
        for file in &test_files {
            println!("   • {}", file.file_name().unwrap().to_string_lossy());
        }

        if test_files.is_empty() {
            shutdown_signal.notify_one();
            if let Some(ref filter) = test_filter {
                return Err(anyhow::anyhow!("No test files found matching filter '{}'", filter));
            } else {
                return Err(anyhow::anyhow!("No .slt test files found in tests/slt directory"));
            }
        }

        let mut all_passed = true;
        for test_file in test_files {
            let test_path = test_file.as_path();
            if pretty_mode {
                println!("\n\n🔄 Running: {}", test_path.file_name().unwrap().to_string_lossy());
                println!("{}", "-".repeat(50));
            } else {
                println!("\nRunning SQLLogicTest: {}", test_path.display());
            }

            let (cleanup_client, _) = connect_with_retry(port, Duration::from_secs(3)).await?;
            let tables_to_drop = ["test_table", "events", "t", "numeric_test", "percentile_test", "test_spans"];
            for table in &tables_to_drop {
                let drop_sql = format!("DROP TABLE IF EXISTS {}", table);
                let _ = cleanup_client.execute(&drop_sql, &[]).await;
            }

            let factory_clone = || async move {
                let (client, _) = connect_with_retry(port, Duration::from_secs(3)).await?;
                Ok::<TestDB, TestError>(TestDB { client })
            };

            let test_result = sqllogictest::Runner::new(factory_clone).run_file_async(test_path).await;

            match test_result {
                Ok(_) => {
                    if pretty_mode {
                        println!("✅ PASSED: {}", test_path.file_name().unwrap().to_string_lossy());
                    } else {
                        println!("✓ {} passed", test_path.display());
                    }
                }
                Err(e) => {
                    if pretty_mode {
                        eprintln!("❌ FAILED: {}", test_path.file_name().unwrap().to_string_lossy());
                        eprintln!("   Error: {:?}", e);
                    } else {
                        eprintln!("✗ {} failed: {:?}", test_path.display(), e);
                    }
                    all_passed = false;
                }
            }
        }

        shutdown_signal.notify_one();

        if pretty_mode {
            println!("\n{}", "=".repeat(50));
            if all_passed {
                println!("✅ All tests passed!");
            } else {
                println!("❌ Some tests failed");
            }
        }

        if all_passed { Ok(()) } else { Err(anyhow::anyhow!("Some SQLLogicTests failed")) }
    }
}
