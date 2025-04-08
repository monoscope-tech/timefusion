#[cfg(test)]
mod sqllogictest_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use chrono;
    use dotenv::dotenv;
    use serial_test::serial;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use std::{path::Path, time::Duration};
    use timefusion::database::Database;
    use tokio::{sync::oneshot, time::sleep};
    use tokio_postgres::{NoTls, Row};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    // Thin wrapper for Postgres client
    struct TestDB {
        client: tokio_postgres::Client,
    }

    #[async_trait]
    impl AsyncDB for TestDB {
        type Error = tokio_postgres::Error;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let sql = sql.trim();
            
            // Check if this is a query (SELECT) or statement (INSERT, etc.)
            let is_query = sql.to_lowercase().starts_with("select");
            
            if !is_query {
                // For non-SELECT queries, execute and return completion
                let affected = self.client.execute(sql, &[]).await?;
                return Ok(DBOutput::StatementComplete(affected as u64));
            }
            
            // For SELECT queries, get the rows and return formatted result
            let rows = self.client.query(sql, &[]).await?;
            if rows.is_empty() {
                return Ok(DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            }
            
            // Convert column types to DefaultColumnType
            let types = rows[0].columns().iter().map(|col| {
                match col.type_().name() {
                    "int2" | "int4" | "int8" => DefaultColumnType::Integer,
                    "float4" | "float8" | "numeric" => DefaultColumnType::Text, // Use Text for floating point
                    "timestamp" => DefaultColumnType::Text, // Handle timestamp as text
                    _ => DefaultColumnType::Text,
                }
            }).collect();
            
            // Format each row as vectors of strings
            let mut result_rows = Vec::new();
            for row in &rows {
                result_rows.push(format_row(row));
            }
            
            Ok(DBOutput::Rows {
                types,
                rows: result_rows,
            })
        }
        
        fn engine_name(&self) -> &str {
            "timefusion-postgres"
        }
        
        async fn shutdown(&mut self) {
            // No-op for this implementation
        }
    }
    
    // Helper to format a row into strings - safely handling each type
    fn format_row(row: &Row) -> Vec<String> {
        row.columns().iter().enumerate().map(|(i, col)| {
            // Get the type name for debugging
            let type_name = col.type_().name();
            
            // Try different type conversions in order
            match type_name {
                "int2" => match row.try_get::<_, Option<i16>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:int2"),
                },
                "int4" => match row.try_get::<_, Option<i32>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:int4"),
                },
                "int8" => match row.try_get::<_, Option<i64>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:int8"),
                },
                "float4" => match row.try_get::<_, Option<f32>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:float4"),
                },
                "float8" => match row.try_get::<_, Option<f64>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:float8"),
                },
                "bool" => match row.try_get::<_, Option<bool>>(i) {
                    Ok(v) => v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("error:bool"),
                },
                // For timestamps, try multiple approaches
                "timestamp" => {
                    // First try as chrono::NaiveDateTime which is what tokio-postgres uses
                    match row.try_get::<_, Option<chrono::NaiveDateTime>>(i) {
                        Ok(Some(v)) => v.to_string(),
                        Ok(None) => "NULL".to_string(),
                        // If that fails, try as string
                        Err(_) => match row.try_get::<_, Option<String>>(i) {
                            Ok(Some(v)) => v,
                            Ok(None) => "NULL".to_string(),
                            // Last resort: try as timestamptz
                            Err(_) => match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(i) {
                                Ok(Some(v)) => v.to_string(),
                                Ok(None) => "NULL".to_string(),
                                Err(_) => "[timestamp]".to_string()
                            }
                        }
                    }
                },
                // For all other types, try as string
                _ => match row.try_get::<_, Option<String>>(i) {
                    Ok(v) => v.unwrap_or_else(|| "NULL".to_string()),
                    Err(_) => format!("{}", type_name),
                },
            }
        }).collect()
    }

    async fn start_test_server() -> Result<(oneshot::Sender<()>, tokio_postgres::Client)> {
        let test_id = Uuid::new_v4().to_string();
        dotenv().ok();

        unsafe {
            std::env::set_var("PGWIRE_PORT", "5433");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-slt-{}", test_id));
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Spawn server
        tokio::spawn(async move {
            let db = Database::new().await.expect("Failed to create database");
            let session_context = db.create_session_context();
            db.setup_session_context(&session_context).expect("Failed to setup session context");

            let shutdown_token = CancellationToken::new();
            let pg_server = db
                .start_pgwire_server(
                    session_context,
                    5433, // Test port 
                    shutdown_token.clone(),
                )
                .await
                .expect("Failed to start PGWire server");

            // Wait for shutdown signal
            let _ = shutdown_rx.await;
            shutdown_token.cancel();
            let _ = pg_server.await;
        });

        // Give server time to start, using a more efficient approach with retry
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(5);
        
        // Try connecting with a retry loop
        while start_time.elapsed() < timeout {
            match tokio_postgres::connect(
                "host=localhost port=5433 user=postgres password=postgres", 
                NoTls
            ).await {
                Ok((client, connection)) => {
                    // Handle connection
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                    
                    // Connected successfully, return the client
                    return Ok((shutdown_tx, client));
                },
                Err(_) => {
                    // Small backoff between connection attempts
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
        
        // If we've reached here, do one final attempt or fail
        let (client, connection) = tokio_postgres::connect(
            "host=localhost port=5433 user=postgres password=postgres",
            NoTls,
        ).await?;

        // Handle connection
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok((shutdown_tx, client))
    }

    #[tokio::test]
    #[serial]
    async fn run_sqllogictest() -> Result<()> {
        // We don't need the client from start_test_server, we'll create a fresh one for each test
        let (shutdown_tx, _) = start_test_server().await?;
        
        // Create a factory function to create new connections for each test
        let factory = || {
            async move {
                // Create a fresh connection each time with retry logic
                let start_time = std::time::Instant::now();
                let timeout = Duration::from_secs(3);
                
                while start_time.elapsed() < timeout {
                    match tokio_postgres::connect(
                        "host=localhost port=5433 user=postgres password=postgres",
                        NoTls,
                    ).await {
                        Ok((client, connection)) => {
                            // Spawn a task to drive the connection
                            tokio::spawn(async move {
                                if let Err(e) = connection.await {
                                    eprintln!("Connection error: {}", e);
                                }
                            });
                            
                            return Ok(TestDB { client });
                        },
                        Err(_) => {
                            // Small backoff before retry
                            sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                
                // Final attempt
                let (client, connection) = tokio_postgres::connect(
                    "host=localhost port=5433 user=postgres password=postgres",
                    NoTls,
                ).await?;
                
                // Spawn a task to drive the connection
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("Connection error: {}", e);
                    }
                });
                
                Ok(TestDB { client })
            }
        };
        
        let test_file = Path::new("tests/example.slt");
        
        // Run the test using the factory function with detailed error reporting
        let result = match sqllogictest::Runner::new(factory).run_file_async(test_file).await {
            Ok(_) => {
                println!("SQLLogicTest passed successfully!");
                Ok(())
            },
            Err(e) => {
                eprintln!("SQLLogicTest failed: {:?}", e);
                
                // Try to perform a diagnostic query to help troubleshoot timestamp issues
                match tokio_postgres::connect(
                    "host=localhost port=5433 user=postgres password=postgres",
                    NoTls,
                ).await {
                    Ok((client, connection)) => {
                        // Spawn connection handler
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("Diagnostic connection error: {}", e);
                            }
                        });
                        
                        // Try to execute a diagnostic query to verify timestamp handling
                        match client.query("SELECT TIMESTAMP '2023-01-01T10:00:00Z' as test_timestamp", &[]).await {
                            Ok(rows) => {
                                if !rows.is_empty() {
                                    let timestamp_type = rows[0].columns()[0].type_().name();
                                    eprintln!("Diagnostic: timestamp SQL type is '{}'", timestamp_type);
                                    
                                    // Try different type accesses to debug the issue
                                    if let Ok(ts) = rows[0].try_get::<_, chrono::NaiveDateTime>(0) {
                                        eprintln!("Diagnostic: timestamp as NaiveDateTime: {}", ts);
                                    }
                                    if let Ok(ts) = rows[0].try_get::<_, String>(0) {
                                        eprintln!("Diagnostic: timestamp as String: {}", ts);
                                    }
                                }
                            },
                            Err(query_err) => {
                                eprintln!("Diagnostic query failed: {}", query_err);
                            }
                        }
                    },
                    Err(conn_err) => {
                        eprintln!("Failed to create diagnostic connection: {}", conn_err);
                    }
                }
                
                Err(anyhow::anyhow!("SQLLogicTest failed: {:?}", e))
            }
        };
            
        // Always shut down the server
        let _ = shutdown_tx.send(());
        
        result
    }
}