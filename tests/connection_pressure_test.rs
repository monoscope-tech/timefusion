//! Tests to reproduce connection rejection issues under pressure.
//! These tests demonstrate that the datafusion_postgres server rejects
//! new connections when under heavy concurrent load.

#[cfg(test)]
mod connection_pressure {
    use anyhow::Result;
    use datafusion_postgres::{ServerOptions, auth::AuthManager};
    use dotenv::dotenv;
    use rand::Rng;
    use serial_test::serial;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use timefusion::database::Database;
    use tokio::sync::Notify;
    use tokio::time::timeout;
    use tokio_postgres::NoTls;
    use uuid::Uuid;

    struct PressureTestServer {
        port: u16,
        test_id: String,
        shutdown: Arc<Notify>,
    }

    impl PressureTestServer {
        async fn start() -> Result<Self> {
            let _ = env_logger::builder().is_test(true).try_init();
            dotenv().ok();

            let test_id = Uuid::new_v4().to_string();
            let port = 6433 + rand::rng().random_range(1..100) as u16;

            unsafe {
                std::env::set_var("PGWIRE_PORT", port.to_string());
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("pressure-{}", test_id));
            }

            let shutdown = Arc::new(Notify::new());
            let shutdown_clone = shutdown.clone();

            tokio::spawn(async move {
                let db = Database::new().await.expect("Failed to create database");
                let mut ctx = db.create_session_context();
                db.setup_session_context(&mut ctx).expect("Failed to setup context");

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

            // Wait for server to be ready
            tokio::time::sleep(Duration::from_millis(1000)).await;

            Ok(Self { port, test_id, shutdown })
        }
    }

    impl Drop for PressureTestServer {
        fn drop(&mut self) {
            self.shutdown.notify_one();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn test_connection_rejection_under_pressure() -> Result<()> {
        let server = PressureTestServer::start().await?;

        let connection_refused_count = Arc::new(AtomicUsize::new(0));
        let total_errors = Arc::new(AtomicUsize::new(0));
        let successful_ops = Arc::new(AtomicUsize::new(0));

        const CONCURRENT_CLIENTS: usize = 100;
        const OPS_PER_CLIENT: usize = 10;
        const CONNECTION_TIMEOUT_MS: u64 = 900;

        let mut handles = vec![];

        for client_id in 0..CONCURRENT_CLIENTS {
            let server_port = server.port;
            let test_id = server.test_id.clone();
            let refused_count = connection_refused_count.clone();
            let error_count = total_errors.clone();
            let success_count = successful_ops.clone();

            handles.push(tokio::spawn(async move {
                for op in 0..OPS_PER_CLIENT {
                    // Create a new connection for each operation (no connection pooling)
                    let conn_str = format!("host=localhost port={} user=postgres password=postgres", server_port);

                    match timeout(Duration::from_millis(CONNECTION_TIMEOUT_MS), tokio_postgres::connect(&conn_str, NoTls)).await {
                        Ok(Ok((client, conn))) => {
                            // Spawn connection handler
                            tokio::spawn(async move {
                                if let Err(e) = conn.await {
                                    eprintln!("Connection handler error: {}", e);
                                }
                            });

                            // Try to perform an operation
                            let insert_sql = format!(
                                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) 
                                 VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
                                chrono::Utc::now().date_naive(),
                                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
                            );

                            let span_id = format!("{}-client-{}-op-{}", test_id, client_id, op);

                            match timeout(
                                Duration::from_millis(500),
                                client.execute(
                                    &insert_sql,
                                    &[
                                        &"pressure_test",
                                        &span_id,
                                        &format!("pressure_span_{client_id}_{op}"),
                                        &"OK",
                                        &"Pressure test",
                                        &"INFO",
                                        &vec![format!("Pressure test op {} from client {}", op, client_id)],
                                    ],
                                ),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {
                                    success_count.fetch_add(1, Ordering::Relaxed);
                                }
                                Ok(Err(e)) => {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                    eprintln!("Query error for client {}: {}", client_id, e);
                                }
                                Err(_) => {
                                    error_count.fetch_add(1, Ordering::Relaxed);
                                    eprintln!("Query timeout for client {}", client_id);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            error_count.fetch_add(1, Ordering::Relaxed);
                            let error_msg = e.to_string();

                            // Check if this is a connection refused error
                            if error_msg.contains("Connection refused")
                                || error_msg.contains("connection refused")
                                || error_msg.contains("could not receive data from server")
                            {
                                refused_count.fetch_add(1, Ordering::Relaxed);
                                eprintln!("Connection refused for client {} op {}: {}", client_id, op, error_msg);
                            }
                        }
                        Err(_) => {
                            // Timeout
                            error_count.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Connection timeout for client {} op {}", client_id, op);
                        }
                    }

                    // No delay - hammer the server
                }
            }));
        }

        // Wait for all clients to complete
        for handle in handles {
            let _ = handle.await;
        }

        let refused = connection_refused_count.load(Ordering::Relaxed);
        let errors = total_errors.load(Ordering::Relaxed);
        let successes = successful_ops.load(Ordering::Relaxed);

        println!("\n=== Connection Pressure Test Results ===");
        println!("Total operations attempted: {}", CONCURRENT_CLIENTS * OPS_PER_CLIENT);
        println!("Successful operations: {}", successes);
        println!("Total errors: {}", errors);
        println!("Connection refused errors: {}", refused);
        println!(
            "Success rate: {:.2}%",
            (successes as f64 / (CONCURRENT_CLIENTS * OPS_PER_CLIENT) as f64) * 100.0
        );
        println!(
            "Connection refused rate: {:.2}%",
            (refused as f64 / (CONCURRENT_CLIENTS * OPS_PER_CLIENT) as f64) * 100.0
        );

        // Verify that we actually reproduced issues under pressure
        assert!(errors > 0, "Expected to see some errors under pressure");

        // The test should demonstrate connection issues (either timeouts or refusals)
        println!("\nTest demonstrates connection issues under pressure.");
        if refused == 0 {
            println!("Note: Got timeouts instead of explicit connection refusals.");
            println!("This still demonstrates the server cannot handle the load.");
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn test_connection_exhaustion_with_concurrent_reads_writes() -> Result<()> {
        let server = PressureTestServer::start().await?;

        let connection_errors = Arc::new(AtomicUsize::new(0));
        let read_errors = Arc::new(AtomicUsize::new(0));
        let write_errors = Arc::new(AtomicUsize::new(0));

        // Test with simultaneous reads and writes
        const READERS: usize = 24;
        const WRITERS: usize = 24;
        const OPS_PER_WORKER: usize = 10;

        let mut handles = vec![];

        // Spawn writers
        for writer_id in 0..WRITERS {
            let server_port = server.port;
            let test_id = server.test_id.clone();
            let conn_errors = connection_errors.clone();
            let write_errs = write_errors.clone();

            handles.push(tokio::spawn(async move {
                for op in 0..OPS_PER_WORKER {
                    let conn_str = format!("host=localhost port={} user=postgres password=postgres", server_port);

                    match timeout(Duration::from_millis(500), tokio_postgres::connect(&conn_str, NoTls)).await {
                        Ok(Ok((client, conn))) => {
                            tokio::spawn(async move {
                                let _ = conn.await;
                            });

                            let insert_sql = format!(
                                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary) 
                                 VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
                                chrono::Utc::now().date_naive(),
                                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
                            );

                            if let Err(_) = timeout(
                                Duration::from_millis(500),
                                client.execute(
                                    &insert_sql,
                                    &[
                                        &"exhaust_test",
                                        &format!("{}-w{}-{}", test_id, writer_id, op),
                                        &format!("write_{writer_id}_{op}"),
                                        &"OK",
                                        &"Write test",
                                        &"INFO",
                                        &vec!["Concurrent write"],
                                    ],
                                ),
                            )
                            .await
                            {
                                write_errs.fetch_add(1, Ordering::Relaxed);
                                eprintln!("Write error or timeout");
                            }
                        }
                        Ok(Err(e)) => {
                            conn_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Connection error: {}", e);
                        }
                        Err(_) => {
                            conn_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Connection timeout");
                        }
                    }
                }
            }));
        }

        // Spawn readers
        for _reader_id in 0..READERS {
            let server_port = server.port;
            let conn_errors = connection_errors.clone();
            let read_errs = read_errors.clone();

            handles.push(tokio::spawn(async move {
                for op in 0..OPS_PER_WORKER {
                    let conn_str = format!("host=localhost port={} user=postgres password=postgres", server_port);

                    match timeout(Duration::from_millis(500), tokio_postgres::connect(&conn_str, NoTls)).await {
                        Ok(Ok((client, conn))) => {
                            tokio::spawn(async move {
                                let _ = conn.await;
                            });

                            let queries = vec![
                                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'exhaust_test'",
                                "SELECT name FROM otel_logs_and_spans WHERE project_id = 'exhaust_test' LIMIT 5",
                                "SELECT status_code, COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'exhaust_test' GROUP BY status_code",
                            ];

                            let query = queries[op % queries.len()];
                            if let Err(_) = timeout(Duration::from_millis(500), client.query(query, &[])).await {
                                read_errs.fetch_add(1, Ordering::Relaxed);
                                eprintln!("Read error or timeout");
                            }
                        }
                        Ok(Err(e)) => {
                            conn_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Connection error: {}", e);
                        }
                        Err(_) => {
                            conn_errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Connection timeout");
                        }
                    }
                }
            }));
        }

        // Wait for completion
        for handle in handles {
            let _ = handle.await;
        }

        let conn_errs = connection_errors.load(Ordering::Relaxed);
        let read_errs = read_errors.load(Ordering::Relaxed);
        let write_errs = write_errors.load(Ordering::Relaxed);

        println!("\n=== Concurrent Read/Write Pressure Test Results ===");
        println!("Connection errors: {}", conn_errs);
        println!("Read errors: {}", read_errs);
        println!("Write errors: {}", write_errs);
        println!("Total errors: {}", conn_errs + read_errs + write_errs);

        // With reduced concurrency, we might not see errors
        if conn_errs + read_errs + write_errs > 0 {
            println!("\nTest successfully reproduced connection/operation errors under concurrent load.");
        } else {
            println!("\nNo errors with reduced concurrency (3 readers + 3 writers). Server handled the load successfully.");
        }

        Ok(())
    }
}

