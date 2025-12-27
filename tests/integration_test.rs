#[cfg(test)]
mod integration {
    use anyhow::Result;
    use datafusion_postgres::{ServerOptions, auth::AuthManager};
    // Not using dotenv - all env vars set explicitly in TestServer::start()
    use rand::Rng;
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use timefusion::database::Database;
    use tokio::sync::Notify;
    use tokio_postgres::{Client, NoTls};
    use uuid::Uuid;

    struct TestServer {
        port: u16,
        test_id: String,
        shutdown: Arc<Notify>,
    }

    impl TestServer {
        async fn start() -> Result<Self> {
            let _ = env_logger::builder().is_test(true).try_init();
            // Don't use dotenv() - set all environment variables explicitly
            // to match the lib tests which work correctly

            let test_id = Uuid::new_v4().to_string();
            let port = 5433 + rand::rng().random_range(1..100) as u16;

            unsafe {
                // Core settings
                std::env::set_var("PGWIRE_PORT", port.to_string());
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", test_id));

                // S3/MinIO settings - same as lib tests
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
                std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
                std::env::set_var("AWS_S3_ENDPOINT", "http://127.0.0.1:9000");
                std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
                std::env::set_var("AWS_ALLOW_HTTP", "true");

                // Disable config database
                std::env::set_var("AWS_S3_LOCKING_PROVIDER", "");

                // Foyer cache settings - use unique cache dir per test to avoid conflicts
                std::env::set_var("TIMEFUSION_FOYER_MEMORY_MB", "64");
                std::env::set_var("TIMEFUSION_FOYER_DISK_GB", "1");
                std::env::set_var("TIMEFUSION_FOYER_TTL_SECONDS", "60");
                std::env::set_var("TIMEFUSION_FOYER_SHARDS", "4");
                std::env::set_var("TIMEFUSION_FOYER_CACHE_DIR", format!("/tmp/timefusion_cache_{}", test_id));
            }

            // Create database OUTSIDE the spawn to ensure table initialization completes
            // in the main test context.
            let db = Database::new().await?;
            let db = Arc::new(db);

            // Pre-warm the table by creating it now, outside the PGWire handler context.
            db.get_or_create_table("test_project", "otel_logs_and_spans").await?;

            let db_clone = db.clone();
            let shutdown = Arc::new(Notify::new());
            let shutdown_clone = shutdown.clone();

            tokio::spawn(async move {
                let mut ctx = db_clone.clone().create_session_context();
                db_clone.setup_session_context(&mut ctx).expect("Failed to setup context");

                let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
                let auth_manager = Arc::new(AuthManager::new());

                tokio::select! {
                    _ = shutdown_clone.notified() => {},
                    res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &opts, auth_manager) => {
                        if let Err(e) = res {
                            eprintln!("Server error: {:?}", e);
                        }
                    }
                }
            });

            // Wait for server readiness
            Self::connect(port).await?;

            Ok(Self { port, test_id, shutdown })
        }

        async fn connect(port: u16) -> Result<Client> {
            let conn_str = format!("host=localhost port={port} user=postgres password=postgres");

            for _ in 0..100 {
                if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                    return Ok(client);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }

            Err(anyhow::anyhow!("Failed to connect after timeout"))
        }

        async fn client(&self) -> Result<Client> {
            Self::connect(self.port).await
        }

        fn insert_sql() -> String {
            format!(
                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) 
                 VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
                chrono::Utc::now().date_naive(),
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
            )
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.notify_one();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_postgres_integration() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let insert = TestServer::insert_sql();

        // Insert and verify single record
        client
            .execute(
                &insert,
                &[
                    &"test_project",
                    &server.test_id,
                    &"test_span_name",
                    &"OK",
                    &"Test integration",
                    &"INFO",
                    &vec!["Integration test summary"],
                ],
            )
            .await?;

        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &server.test_id],
            )
            .await?
            .get(0);
        assert_eq!(count, 1);

        // Verify field values
        let row = client
            .query_one(
                "SELECT name, status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &server.test_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "test_span_name");
        assert_eq!(row.get::<_, String>(1), "OK");

        // Batch insert
        for i in 0..5 {
            client
                .execute(
                    &insert,
                    &[
                        &"test_project",
                        &Uuid::new_v4().to_string(),
                        &format!("batch_span_{i}"),
                        &"OK",
                        &format!("Batch test {i}"),
                        &"INFO",
                        &vec![format!("Batch test summary {i}")],
                    ],
                )
                .await?;
        }

        // Verify total count
        let total: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?.get(0);
        assert_eq!(total, 6);

        // Verify schema
        let rows = client.query("SELECT * FROM otel_logs_and_spans WHERE project_id = $1 LIMIT 1", &[&"test_project"]).await?;
        assert_eq!(rows[0].columns().len(), 89);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_operations() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let insert = TestServer::insert_sql();

        // Insert test data
        let span_id = Uuid::new_v4().to_string();
        client
            .execute(
                &insert,
                &[&"test_project", &span_id, &"original_name", &"OK", &"Original message", &"INFO", &vec!["Original summary"]],
            )
            .await?;

        // Test single field update
        client
            .execute(
                "UPDATE otel_logs_and_spans SET status_message = $1 WHERE project_id = $2 AND id = $3",
                &[&"Updated message", &"test_project", &span_id],
            )
            .await?;

        let row = client
            .query_one(
                "SELECT status_message FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &span_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "Updated message");

        // Test multiple field update
        client
            .execute(
                "UPDATE otel_logs_and_spans SET status_code = $1, level = $2 WHERE project_id = $3 AND id = $4",
                &[&"ERROR", &"ERROR", &"test_project", &span_id],
            )
            .await?;

        let row = client
            .query_one(
                "SELECT status_code, level FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &span_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "ERROR");
        assert_eq!(row.get::<_, String>(1), "ERROR");

        // Test conditional update
        for i in 0..3 {
            let status = if i % 2 == 0 { "OK" } else { "ERROR" };
            client
                .execute(
                    &insert,
                    &[&"test_project", &format!("update_test_{}", i), &"test", &status, &"Message", &"INFO", &vec!["Summary"]],
                )
                .await?;
        }

        client
            .execute(
                "UPDATE otel_logs_and_spans SET status_code = $1 WHERE project_id = $2 AND status_code = $3",
                &[&"SUCCESS", &"test_project", &"OK"],
            )
            .await?;

        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2",
                &[&"test_project", &"SUCCESS"],
            )
            .await?
            .get(0);
        assert_eq!(count, 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_operations() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let insert = TestServer::insert_sql();

        // Insert test data
        let span_id = Uuid::new_v4().to_string();
        client
            .execute(
                &insert,
                &[&"test_project", &span_id, &"to_delete", &"OK", &"Message", &"INFO", &vec!["Summary"]],
            )
            .await?;

        // Verify insertion
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &span_id],
            )
            .await?
            .get(0);
        assert_eq!(count, 1);

        // Delete the record
        client
            .execute(
                "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &span_id],
            )
            .await?;

        // Verify deletion
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &span_id],
            )
            .await?
            .get(0);
        assert_eq!(count, 0);

        // Test conditional delete
        for i in 0..4 {
            let status = match i % 3 {
                0 => "OK",
                1 => "ERROR",
                _ => "WARNING",
            };
            client
                .execute(
                    &insert,
                    &[&"test_project", &format!("delete_test_{}", i), &"test", &status, &"Message", &"INFO", &vec!["Summary"]],
                )
                .await?;
        }

        // Delete all ERROR records
        client
            .execute(
                "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2",
                &[&"test_project", &"ERROR"],
            )
            .await?;

        let error_count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2",
                &[&"test_project", &"ERROR"],
            )
            .await?
            .get(0);
        assert_eq!(error_count, 0);

        let total_count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?.get(0);
        assert_eq!(total_count, 3);

        Ok(())
    }
}
