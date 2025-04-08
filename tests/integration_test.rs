#[cfg(test)]
mod integration {
    use dotenv::dotenv;
    use serial_test::serial;
    use std::time::Duration;
    use timefusion::database::Database;
    use tokio::sync::oneshot;
    use tokio::time::sleep;
    use tokio_postgres::NoTls;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    async fn start_test_server() -> anyhow::Result<(oneshot::Sender<()>, String)> {
        let test_id = Uuid::new_v4().to_string();
        let _ = env_logger::builder().is_test(true).try_init();
        dotenv().ok();

        unsafe {
            std::env::set_var("PGWIRE_PORT", "5433");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", test_id));
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tokio::spawn(async move {
            let db = match Database::new().await {
                Ok(db) => db,
                Err(e) => {
                    panic!("Failed to create database: {:?}", e);
                }
            };

            let session_context = db.create_session_context();
            db.setup_session_context(&session_context).expect("Failed to setup session context");

            let shutdown_token = CancellationToken::new();
            let pg_server = db
                .start_pgwire_server(
                    session_context,
                    5433,
                    shutdown_token.clone(),
                )
                .await
                .expect("Failed to start PGWire server");

            let _ = shutdown_rx.await;
            shutdown_token.cancel();
            let _ = pg_server.await;
        });

        // Give server time to start
        sleep(Duration::from_secs(2)).await;

        Ok((shutdown_tx, test_id))
    }

    #[tokio::test]
    #[serial]
    async fn test_postgres_integration() -> anyhow::Result<()> {
        let (shutdown_tx, test_id) = start_test_server().await?;

        // Connect to database
        let connection_string = "host=localhost port=5433 user=postgres password=postgres";
        let (client, connection) = match tokio_postgres::connect(connection_string, NoTls).await {
            Ok((client, connection)) => (client, connection),
            Err(e) => {
                shutdown_tx.send(()).ok();
                return Err(anyhow::anyhow!("Failed to connect to PostgreSQL: {:?}", e));
            }
        };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Allow connection to stabilize
        sleep(Duration::from_millis(500)).await;

        // Insert test data
        let timestamp_str = format!("'{}'", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));

        match client
            .execute(
                format!(
                    "INSERT INTO otel_logs_and_spans (
                    project_id, timestamp, id, 
                    name, status_code, status_message, 
                    level
                ) VALUES ($1, {}, $2, $3, $4, $5, $6)",
                    timestamp_str
                )
                .as_str(),
                &[&"test_project", &test_id, &"test_span_name", &"OK", &"Test integration", &"INFO"],
            )
            .await
        {
            Ok(_) => (),
            Err(e) => {
                shutdown_tx.send(()).ok();
                return Err(anyhow::anyhow!("Failed to insert test data: {:?}", e));
            }
        }

        // Query back the data and check with the client directly
        let rows = match client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE id = $1", &[&test_id]).await {
            Ok(rows) => rows,
            Err(e) => {
                shutdown_tx.send(()).ok();
                return Err(anyhow::anyhow!("Failed to query data: {:?}", e));
            }
        };

        assert_eq!(rows[0].get::<_, i64>(0), 1, "Should have found exactly one row");

        // Verify field values
        let detail_rows = match client.query("SELECT name, status_code FROM otel_logs_and_spans WHERE id = $1", &[&test_id]).await {
            Ok(rows) => rows,
            Err(e) => {
                shutdown_tx.send(()).ok();
                return Err(anyhow::anyhow!("Failed to query detailed data: {:?}", e));
            }
        };

        assert_eq!(detail_rows.len(), 1, "Should have found exactly one detailed row");
        assert_eq!(detail_rows[0].get::<_, String>(0), "test_span_name", "Name should match");
        assert_eq!(detail_rows[0].get::<_, String>(1), "OK", "Status code should match");

        // Insert multiple records
        for i in 0..5 {
            let span_id = Uuid::new_v4().to_string();
            let batch_name = format!("batch_span_{}", i);
            let batch_message = format!("Batch test {}", i);

            match client
                .execute(
                    format!(
                        "INSERT INTO otel_logs_and_spans (
                        project_id, timestamp, id,
                        name, status_code, status_message,
                        level
                    ) VALUES ($1, {}, $2, $3, $4, $5, $6)",
                        timestamp_str
                    )
                    .as_str(),
                    &[&"test_project", &span_id, &batch_name, &"OK", &batch_message, &"INFO"],
                )
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    shutdown_tx.send(()).ok();
                    return Err(anyhow::anyhow!("Failed to insert batch record: {:?}", e));
                }
            };
        }

        // Give a little time for all inserts to complete
        sleep(Duration::from_millis(500)).await;

        // Query with filter to get total count
        let count_rows = match client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await {
            Ok(rows) => rows,
            Err(e) => {
                shutdown_tx.send(()).ok();
                return Err(anyhow::anyhow!("Failed to query total count: {:?}", e));
            }
        };

        assert_eq!(count_rows[0].get::<_, i64>(0), 6, "Should have a total of 6 records (1 initial + 5 batch)");

        let _ = shutdown_tx.send(());

        Ok(())
    }
}