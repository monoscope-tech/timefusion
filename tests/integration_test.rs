#[cfg(test)]
mod integration {
    use anyhow::Result;
    use datafusion_postgres::{ServerOptions, auth::AuthManager};
    use dotenv::dotenv;
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
            dotenv().ok();

            let test_id = Uuid::new_v4().to_string();
            let port = 5433 + rand::rng().random_range(1..100) as u16;

            unsafe {
                std::env::set_var("PGWIRE_PORT", port.to_string());
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", test_id));
            }

            let shutdown = Arc::new(Notify::new());
            let shutdown_clone = shutdown.clone();

            tokio::spawn(async move {
                let db = Database::new().await.expect("Failed to create database");
                let db = Arc::new(db);
                let mut ctx = db.clone().create_session_context();
                db.setup_session_context(&mut ctx).expect("Failed to setup context");

                // Pre-create the table by inserting a dummy record
                // This avoids table creation hanging when done through PGWire
                println!("Pre-creating table with initial insert...");
                let schema = timefusion::schema_loader::get_default_schema();
                let arrow_schema = schema.schema_ref();

                // Create a minimal batch with required fields
                use datafusion::arrow::array::*;
                use datafusion::arrow::buffer::OffsetBuffer;
                use datafusion::arrow::datatypes::*;
                use datafusion::arrow::record_batch::RecordBatch;

                let mut columns: Vec<std::sync::Arc<dyn Array>> = Vec::new();
                for field in arrow_schema.fields() {
                    let array: std::sync::Arc<dyn Array> = match field.data_type() {
                        DataType::Utf8 => {
                            let val = if field.name() == "project_id" { "_warmup_" }
                            else if field.name() == "id" { "_warmup_id_" }
                            else if field.name() == "name" { "_warmup_" }
                            else { "" };
                            std::sync::Arc::new(StringArray::from(vec![val]))
                        },
                        DataType::Date32 => std::sync::Arc::new(Date32Array::from(vec![19000])),
                        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                            let arr = TimestampMicrosecondArray::from(vec![chrono::Utc::now().timestamp_micros()]);
                            match tz {
                                Some(tz) => std::sync::Arc::new(arr.with_timezone(tz.to_string())),
                                None => std::sync::Arc::new(arr),
                            }
                        },
                        DataType::Int8 => std::sync::Arc::new(Int8Array::from(vec![0i8])),
                        DataType::Int16 => std::sync::Arc::new(Int16Array::from(vec![0i16])),
                        DataType::Int32 => std::sync::Arc::new(Int32Array::from(vec![0i32])),
                        DataType::Int64 => std::sync::Arc::new(Int64Array::from(vec![0i64])),
                        DataType::UInt8 => std::sync::Arc::new(UInt8Array::from(vec![0u8])),
                        DataType::UInt16 => std::sync::Arc::new(UInt16Array::from(vec![0u16])),
                        DataType::UInt32 => std::sync::Arc::new(UInt32Array::from(vec![0u32])),
                        DataType::UInt64 => std::sync::Arc::new(UInt64Array::from(vec![0u64])),
                        DataType::Float32 => std::sync::Arc::new(Float32Array::from(vec![0.0f32])),
                        DataType::Float64 => std::sync::Arc::new(Float64Array::from(vec![0.0f64])),
                        DataType::Boolean => std::sync::Arc::new(BooleanArray::from(vec![false])),
                        DataType::List(_) => {
                            let values = StringArray::from(vec![] as Vec<&str>);
                            let offsets = OffsetBuffer::from_lengths([0]);
                            std::sync::Arc::new(ListArray::try_new(
                                std::sync::Arc::new(Field::new("item", DataType::Utf8, true)),
                                offsets,
                                std::sync::Arc::new(values),
                                None
                            ).unwrap())
                        },
                        _ => std::sync::Arc::new(NullArray::new(1)),
                    };
                    columns.push(array);
                }

                let batch = RecordBatch::try_new(arrow_schema.clone(), columns).expect("Failed to create warmup batch");
                // Pre-create for "test_project" which is what the test uses
                db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true)
                    .await
                    .expect("Failed to pre-create table");
                println!("Table pre-created successfully for test_project, about to start PGWire server...");

                let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
                println!("Server options created for port {}", port);
                let auth_manager = Arc::new(AuthManager::new());
                println!("About to start PGWire server with tokio::select...");

                tokio::select! {
                    _ = shutdown_clone.notified() => {
                        println!("Shutdown signal received");
                    },
                    res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &opts, auth_manager) => {
                        println!("PGWire server returned: {:?}", res.is_ok());
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
    #[ignore] // Slow integration test - run with: cargo test --test integration_test -- --ignored
    async fn test_postgres_integration() -> Result<()> {
        println!("Test: Starting test server...");
        let server = TestServer::start().await?;
        println!("Test: Server started, getting client...");
        let client = server.client().await?;
        println!("Test: Got client, preparing INSERT...");
        let insert = TestServer::insert_sql();

        // Insert and verify single record
        println!("Test: Executing INSERT...");
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
        println!("Test: INSERT completed!");

        println!("Test: Querying count...");
        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &server.test_id],
            )
            .await?
            .get(0);
        println!("Test: Count query returned: {}", count);
        assert_eq!(count, 1);

        // Verify field values
        println!("Test: Verifying field values...");
        let row = client
            .query_one(
                "SELECT name, status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &server.test_id],
            )
            .await?;
        println!("Test: Field values query completed");
        assert_eq!(row.get::<_, String>(0), "test_span_name");
        assert_eq!(row.get::<_, String>(1), "OK");

        // Batch insert with small delay to avoid potential race conditions
        println!("Test: Starting batch inserts...");
        for i in 0..5 {
            println!("Test: Batch insert {}", i);
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
            // Small delay between inserts to avoid overwhelming the delta-rs write pipeline
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("Test: Batch inserts completed");

        // Verify total count
        println!("Test: Verifying total count...");
        let total: i64 = client.query_one("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?.get(0);
        println!("Test: Total count: {}", total);
        assert_eq!(total, 6);

        // Verify schema
        println!("Test: Verifying schema...");
        let rows = client.query("SELECT * FROM otel_logs_and_spans WHERE project_id = $1 LIMIT 1", &[&"test_project"]).await?;
        println!("Test: Schema columns: {}", rows[0].columns().len());
        // Schema may have additional columns from schema evolution
        assert!(rows[0].columns().len() >= 87, "Expected at least 87 columns, got {}", rows[0].columns().len());

        println!("Test: ALL TESTS PASSED!");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    #[ignore] // Slow integration test - run with: cargo test --test integration_test -- --ignored
    async fn test_concurrent_postgres_requests() -> Result<()> {
        let server = TestServer::start().await?;
        let insert = TestServer::insert_sql();

        const CLIENTS: usize = 3;
        const OPS_PER_CLIENT: usize = 5;

        // Concurrent inserts with mixed reads
        let mut handles = vec![];
        for client_id in 0..CLIENTS {
            let server_port = server.port;
            let test_prefix = format!("{}-client-{client_id}", server.test_id);
            let insert = insert.clone();

            handles.push(tokio::spawn(async move {
                let client = TestServer::connect(server_port).await?;
                for op in 0..OPS_PER_CLIENT {
                    let span_id = format!("{test_prefix}-op-{op}");
                    client
                        .execute(
                            &insert,
                            &[
                                &"test_project",
                                &span_id,
                                &format!("concurrent_span_{client_id}_{op}"),
                                &"OK",
                                &"Test",
                                &"INFO",
                                &vec![format!("Concurrent test summary: client {} op {}", client_id, op)],
                            ],
                        )
                        .await?;

                    if op % 2 == 0 {
                        client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            }));
        }

        for handle in handles {
            handle.await??;
        }

        // Verify results
        let client = server.client().await?;
        let count: i64 = client
            .query_one(
                &format!(
                    "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project' AND id LIKE '{}%'",
                    server.test_id
                ),
                &[],
            )
            .await?
            .get(0);
        assert_eq!(count, (CLIENTS * OPS_PER_CLIENT) as i64);

        // Concurrent read performance test
        let mut read_handles = vec![];
        for _ in 0..3 {
            let server_port = server.port;
            let test_id = server.test_id.clone();

            read_handles.push(tokio::spawn(async move {
                let client = TestServer::connect(server_port).await?;
                for j in 0..5 {
                    match j % 3 {
                        0 => client.query("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = $1", &[&"test_project"]).await?,
                        1 => {
                            client
                                .query(
                                    &format!("SELECT name FROM otel_logs_and_spans WHERE project_id = 'test_project' AND id LIKE '{test_id}%' LIMIT 10"),
                                    &[],
                                )
                                .await?
                        }
                        _ => {
                            client
                                .query(
                                    "SELECT status_code, COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project' GROUP BY status_code",
                                    &[],
                                )
                                .await?
                        }
                    };
                }
                Ok::<_, anyhow::Error>(())
            }));
        }

        for handle in read_handles {
            handle.await??;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    #[ignore] // Slow integration test - run with: cargo test --test integration_test -- --ignored
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
    #[ignore] // Slow integration test - run with: cargo test --test integration_test -- --ignored
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
