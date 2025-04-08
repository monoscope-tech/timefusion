#[cfg(test)]
mod sqllogictest_tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use dotenv::dotenv;
    use serial_test::serial;
    use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType};
    use std::{path::Path, sync::Arc, time::{Duration, Instant}};
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::{NoTls, Row};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    struct TestDB {
        client: tokio_postgres::Client,
    }

    #[async_trait]
    impl AsyncDB for TestDB {
        type Error = tokio_postgres::Error;
        type ColumnType = DefaultColumnType;

        async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let sql = sql.trim();
            let is_query = sql.to_lowercase().starts_with("select");
            
            if !is_query {
                let affected = self.client.execute(sql, &[]).await?;
                return Ok(DBOutput::StatementComplete(affected as u64));
            }
            
            let rows = self.client.query(sql, &[]).await?;
            if rows.is_empty() {
                return Ok(DBOutput::Rows { types: vec![], rows: vec![] });
            }
            
            let types = rows[0].columns().iter().map(|col| {
                match col.type_().name() {
                    "int2" | "int4" | "int8" => DefaultColumnType::Integer,
                    _ => DefaultColumnType::Text,
                }
            }).collect();
            
            let result_rows = rows.iter().map(format_row).collect();
            
            Ok(DBOutput::Rows { types, rows: result_rows })
        }
        
        fn engine_name(&self) -> &str {
            "timefusion-postgres"
        }
        
        async fn shutdown(&mut self) {}
    }
    
    fn format_row(row: &Row) -> Vec<String> {
        row.columns().iter().enumerate().map(|(i, col)| {
            let type_name = col.type_().name();
            
            match type_name {
                "int2" => row.try_get::<_, Option<i16>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| "error:int2".to_string()),
                "int4" => row.try_get::<_, Option<i32>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| "error:int4".to_string()),
                "int8" => row.try_get::<_, Option<i64>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| "error:int8".to_string()),
                "float4" | "float8" | "numeric" => row.try_get::<_, Option<f64>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| "error:float".to_string()),
                "bool" => row.try_get::<_, Option<bool>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| "error:bool".to_string()),
                "timestamp" => row.try_get::<_, Option<chrono::NaiveDateTime>>(i)
                    .map(|v| v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| row.try_get::<_, Option<String>>(i)
                        .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                        .unwrap_or_else(|_| "[timestamp]".to_string())),
                _ => row.try_get::<_, Option<String>>(i)
                    .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|_| type_name.to_string()),
            }
        }).collect()
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
                },
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
            let session_context = db.create_session_context();
            db.setup_session_context(&session_context).expect("Failed to setup session context");

            let shutdown_token = CancellationToken::new();
            let pg_server = db
                .start_pgwire_server(session_context, 5433, shutdown_token.clone())
                .await
                .expect("Failed to start PGWire server");

            // Wait for shutdown signal
            shutdown_signal_clone.notified().await;
            shutdown_token.cancel();
            let _ = pg_server.await;
        });

        // Wait for server to be ready
        let _ = connect_with_retry(Duration::from_secs(5)).await?;
        
        Ok(shutdown_signal)
    }

    #[tokio::test]
    #[serial]
    async fn run_sqllogictest() -> Result<()> {
        let shutdown_signal = start_test_server().await?;
        
        let factory = || {
            async move {
                let (client, _) = connect_with_retry(Duration::from_secs(3)).await?;
                Ok(TestDB { client })
            }
        };
        
        let test_file = Path::new("tests/example.slt");
        let result = sqllogictest::Runner::new(factory).run_file_async(test_file).await;
        
        // Always shut down the server
        shutdown_signal.notify_one();
        
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("SQLLogicTest failed: {:?}", e)),
        }
    }
}