#[cfg(test)]
mod integration {
    use anyhow::Result;
    use dotenv::dotenv;
    use scopeguard;
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::sleep};
    use tokio_postgres::{Client, NoTls};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    async fn connect_with_retry(timeout: Duration) -> Result<(Client, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
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

    async fn start_test_server() -> Result<(Arc<Notify>, String)> {
        let test_id = Uuid::new_v4().to_string();
        let _ = env_logger::builder().is_test(true).try_init();
        dotenv().ok();

        unsafe {
            std::env::set_var("PGWIRE_PORT", "5433");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", test_id));
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
        
        Ok((shutdown_signal, test_id))
    }

    #[tokio::test]
    #[serial]
    async fn test_postgres_integration() -> Result<()> {
        let (shutdown_signal, test_id) = start_test_server().await?;
        let shutdown = || { shutdown_signal.notify_one(); };
        
        // Use a guard to ensure we notify of shutdown even if the test panics
        let shutdown_guard = scopeguard::guard((), |_| shutdown());

        // Connect to database
        let (client, _) = connect_with_retry(Duration::from_secs(3)).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;

        // Insert test data
        let timestamp_str = format!("'{}'", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));
        let insert_query = format!(
            "INSERT INTO otel_logs_and_spans (project_id, timestamp, id, name, status_code, status_message, level) 
             VALUES ($1, {}, $2, $3, $4, $5, $6)",
            timestamp_str
        );

        // Run the test with proper error handling
        let result = async {
            // Insert initial record
            client.execute(
                &insert_query,
                &[&"test_project", &test_id, &"test_span_name", &"OK", &"Test integration", &"INFO"],
            ).await?;

            // Verify record count
            let rows = client.query(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE id = $1", 
                &[&test_id]
            ).await?;
            
            assert_eq!(rows[0].get::<_, i64>(0), 1, "Should have found exactly one row");

            // Verify field values
            let detail_rows = client.query(
                "SELECT name, status_code FROM otel_logs_and_spans WHERE id = $1", 
                &[&test_id]
            ).await?;

            assert_eq!(detail_rows.len(), 1, "Should have found exactly one detailed row");
            assert_eq!(detail_rows[0].get::<_, String>(0), "test_span_name", "Name should match");
            assert_eq!(detail_rows[0].get::<_, String>(1), "OK", "Status code should match");

            // Insert multiple records in a batch
            for i in 0..5 {
                let span_id = Uuid::new_v4().to_string();
                client.execute(
                    &insert_query,
                    &[
                        &"test_project", 
                        &span_id, 
                        &format!("batch_span_{}", i), 
                        &"OK", 
                        &format!("Batch test {}", i), 
                        &"INFO"
                    ],
                ).await?;
            }

            // Query with filter to get total count
            let count_rows = client.query(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", 
                &[&"test_project"]
            ).await?;

            assert_eq!(count_rows[0].get::<_, i64>(0), 6, "Should have a total of 6 records (1 initial + 5 batch)");
            
            Ok::<_, tokio_postgres::Error>(())
        }.await;
        
        // Drop the guard to ensure shutdown happens
        std::mem::drop(shutdown_guard);
        shutdown();
        
        // Map postgres errors to anyhow
        result.map_err(|e| anyhow::anyhow!("Test failed: {}", e))
    }
}