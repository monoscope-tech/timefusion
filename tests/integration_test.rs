#[cfg(test)]
mod integration {
    use std::{sync::Arc, time::Duration};

    use anyhow::Result;
    use datafusion_postgres::ServerOptions;
    use rand::RngExt;
    use serial_test::serial;
    use timefusion::{database::Database, test_utils::test_helpers::minio_test_config};
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
            timefusion::test_utils::init_test_logging();

            let test_id = Uuid::new_v4().to_string();
            let port = 5433 + rand::rng().random_range(1..100) as u16;

            let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-{test_id}"));

            // Create database with explicit config - no global state
            let db = Database::with_config(cfg).await?;
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
                let auth_config = timefusion::pgwire_handlers::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };

                tokio::select! {
                    _ = shutdown_clone.notified() => {},
                    res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &opts, auth_config, None, None, std::future::pending::<()>()) => {
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
            .execute(&insert, &[&"test_project", &server.test_id, &"test_span_name", &"OK", &"Test integration", &"INFO", &vec!["Integration test summary"]])
            .await?;

        let count: i64 =
            client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &server.test_id]).await?.get(0);
        assert_eq!(count, 1);

        // Verify field values
        let row = client
            .query_one("SELECT name, status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &server.test_id])
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

        // Targeted column selection — keeps the test focused on a specific row's
        // typed columns. VariantSelectRewriter already serializes Variant columns
        // to JSON at the root projection, so `SELECT *` would also work end-to-end;
        // this assertion just doesn't need every field.
        let row = client.query_one("SELECT id, name, status_code, level FROM otel_logs_and_spans WHERE project_id = $1 LIMIT 1", &[&"test_project"]).await?;
        assert_eq!(row.columns().len(), 4);

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
        client.execute(&insert, &[&"test_project", &span_id, &"original_name", &"OK", &"Original message", &"INFO", &vec!["Original summary"]]).await?;

        // Test single field update
        client
            .execute("UPDATE otel_logs_and_spans SET status_message = $1 WHERE project_id = $2 AND id = $3", &[&"Updated message", &"test_project", &span_id])
            .await?;

        let row = client.query_one("SELECT status_message FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?;
        assert_eq!(row.get::<_, String>(0), "Updated message");

        // Test multiple field update
        client
            .execute(
                "UPDATE otel_logs_and_spans SET status_code = $1, level = $2 WHERE project_id = $3 AND id = $4",
                &[&"ERROR", &"ERROR", &"test_project", &span_id],
            )
            .await?;

        let row =
            client.query_one("SELECT status_code, level FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?;
        assert_eq!(row.get::<_, String>(0), "ERROR");
        assert_eq!(row.get::<_, String>(1), "ERROR");

        // Test conditional update
        for i in 0..3 {
            let status = if i % 2 == 0 { "OK" } else { "ERROR" };
            client.execute(&insert, &[&"test_project", &format!("update_test_{}", i), &"test", &status, &"Message", &"INFO", &vec!["Summary"]]).await?;
        }

        client
            .execute("UPDATE otel_logs_and_spans SET status_code = $1 WHERE project_id = $2 AND status_code = $3", &[&"SUCCESS", &"test_project", &"OK"])
            .await?;

        let count: i64 = client
            .query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2", &[&"test_project", &"SUCCESS"])
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
        client.execute(&insert, &[&"test_project", &span_id, &"to_delete", &"OK", &"Message", &"INFO", &vec!["Summary"]]).await?;

        // Verify insertion
        let count: i64 =
            client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?.get(0);
        assert_eq!(count, 1);

        // Delete the record
        client.execute("DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?;

        // Verify deletion
        let count: i64 =
            client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?.get(0);
        assert_eq!(count, 0);

        // Test conditional delete
        for i in 0..4 {
            let status = match i % 3 {
                0 => "OK",
                1 => "ERROR",
                _ => "WARNING",
            };
            client.execute(&insert, &[&"test_project", &format!("delete_test_{}", i), &"test", &status, &"Message", &"INFO", &vec!["Summary"]]).await?;
        }

        // Delete all ERROR records
        client.execute("DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2", &[&"test_project", &"ERROR"]).await?;

        let error_count: i64 =
            client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2", &[&"test_project", &"ERROR"]).await?.get(0);
        assert_eq!(error_count, 0);

        let total_count: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?.get(0);
        assert_eq!(total_count, 3);

        Ok(())
    }

    /// End-to-end coverage of the Variant pipeline:
    ///   INSERT (Utf8 literal → VariantInsertRewriter wraps with json_to_variant)
    ///   → Delta/MemBuffer (binary Variant storage)
    ///   → SELECT (VariantSelectRewriter wraps root projection with variant_to_json)
    ///   → pgwire (wire bytes are JSON text, not raw binary)
    /// Regression guard for PR's core contract.
    ///
    /// The Variant extension marker is re-stamped at UDF entry by
    /// `functions::VariantExtWrapper` because the marker that
    /// `patch_table_scan` sets on the LogicalPlan's Field metadata is
    /// stripped on its way to the physical executor's per-row Field.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_variant_column_round_trips_as_json() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let span_id = Uuid::new_v4().to_string();
        let attrs_json = r#"{"http":{"method":"GET","status":200},"user":"alice"}"#;

        client
            .execute(
                &format!(
                    "INSERT INTO otel_logs_and_spans \
                     (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary, attributes) \
                     VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7, '{}')",
                    chrono::Utc::now().date_naive(),
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    attrs_json
                ),
                &[&"test_project", &span_id, &"variant_round_trip", &"OK", &"with attrs", &"INFO", &vec!["summary"]],
            )
            .await?;

        // Bare projection: hits wrap_root_projection's Projection arm directly.
        // Bare Variant columns surface as jsonb (OID 3802), decoded binary as serde_json::Value
        // (see jsonb_oid_test::bare_variant_column_returns_jsonb_oid for the wire contract).
        let row = client.query_one("SELECT attributes FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &span_id]).await?;
        let parsed: serde_json::Value = row.get(0);
        assert_eq!(parsed["http"]["method"], "GET");
        assert_eq!(parsed["user"], "alice");

        // Sort/Limit peel path: VariantSelectRewriter must wrap through Sort+Limit.
        let row = client
            .query_one(
                "SELECT attributes FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2 \
                 ORDER BY timestamp DESC LIMIT 1",
                &[&"test_project", &span_id],
            )
            .await?;
        let parsed: serde_json::Value = row.get(0);
        assert_eq!(parsed["http"]["status"], 200, "Sort+Limit path must round-trip variant as jsonb");

        Ok(())
    }
    // Regression: 2026-07-14 prod outage — `impl Drop for Database` cancelled the
    // SHARED maintenance_shutdown token on every clone drop (Database is Clone),
    // silently killing all cron jobs (optimize/checkpoint/vacuum/...), the DML
    // coalescer, and dedup sweeps minutes after boot. Only the LAST clone's drop
    // may cancel it.
    #[tokio::test]
    #[serial]
    async fn database_clone_drop_keeps_maintenance_alive() -> Result<()> {
        let test_id = Uuid::new_v4().to_string();
        let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-{test_id}"));
        let db = Database::with_config(cfg).await?;
        drop(db.clone());
        assert!(!db.is_maintenance_cancelled(), "dropping a Database clone must not cancel the shared maintenance token");
        Ok(())
    }

    // The 2026-06-16 OR-on-indexed-column correctness bug is covered by the fast
    // `mem_buffer::tests::membuffer_or_equality_on_utf8view_keeps_all_matches`
    // unit test (raw MemBuffer) and the `or_utf8view_delta` e2e suite (full
    // pgwire path, Delta + MemBuffer-tantivy). A pgwire integration repro here
    // would just re-run the same assertions via 200 sequential single-row
    // INSERTs (~80s) — redundant.
}
