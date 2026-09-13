//! Tests to reproduce connection rejection issues under pressure.
//! These tests demonstrate that the datafusion_postgres server rejects
//! new connections when under heavy concurrent load.

#[cfg(test)]
mod connection_pressure {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use datafusion_postgres::ServerOptions;
    use dotenv::dotenv;
    use serial_test::serial;
    use timefusion::database::Database;
    use tokio::{sync::Notify, time::timeout};
    use tokio_postgres::{Client, NoTls};
    use uuid::Uuid;

    struct PressureTestServer {
        port: u16,
        test_id: String,
        shutdown: Arc<Notify>,
    }

    impl PressureTestServer {
        async fn start() -> Result<Self> {
            timefusion::support::init_test_logging();
            dotenv().ok();

            let test_id = Uuid::new_v4().to_string();
            // Kernel-assigned free port, not a random pick from a 100-wide
            // window: these servers now start concurrently with every other
            let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();

            unsafe {
                std::env::set_var("PGWIRE_PORT", port.to_string());
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("pressure-{}", test_id));
            }

            let shutdown = Arc::new(Notify::new());
            let shutdown_clone = shutdown.clone();

            tokio::spawn(async move {
                let db = Database::new().await.expect("Failed to create database");
                let db = Arc::new(db);
                let mut ctx = db.clone().create_session_context();
                db.setup_session_context(&mut ctx).expect("Failed to setup context");

                let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
                let auth_config = timefusion::server::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };

                tokio::select! {
                    _ = shutdown_clone.notified() => {},
                    res = timefusion::server::serve_with_logging(Arc::new(ctx), &opts, auth_config, None, None, std::future::pending::<()>()) => {
                        if let Err(e) = res {
                            eprintln!("Server error: {:?}", e);
                        }
                    }
                }
            });

            tokio::time::sleep(Duration::from_millis(1000)).await;

            Ok(Self { port, test_id, shutdown })
        }
    }

    impl Drop for PressureTestServer {
        fn drop(&mut self) {
            self.shutdown.notify_one();
        }
    }

    /// Error/success tally shared by every worker in a pressure run.
    #[derive(Default)]
    struct Tally {
        /// Connect failures that the client reported as an explicit refusal.
        refused: AtomicUsize,
        /// All connect failures (refusals and connect timeouts).
        conn_errors: AtomicUsize,
        /// Failures of a write operation (query error or operation timeout).
        write_errors: AtomicUsize,
        /// Failures of a read operation (query error or operation timeout).
        read_errors: AtomicUsize,
        successes: AtomicUsize,
    }

    impl Tally {
        fn get(c: &AtomicUsize) -> usize {
            c.load(Ordering::Relaxed)
        }
        fn bump(c: &AtomicUsize) {
            c.fetch_add(1, Ordering::Relaxed);
        }

        /// Connect with a deadline, classifying every failure mode into the tally.
        async fn connect(&self, port: u16, connect_timeout_ms: u64, who: &str) -> Option<Client> {
            let conn_str = format!("host=localhost port={} user=postgres password=postgres", port);
            match timeout(Duration::from_millis(connect_timeout_ms), tokio_postgres::connect(&conn_str, NoTls)).await {
                Ok(Ok((client, conn))) => {
                    tokio::spawn(async move {
                        if let Err(e) = conn.await {
                            eprintln!("Connection handler error: {}", e);
                        }
                    });
                    Some(client)
                }
                Ok(Err(e)) => {
                    Self::bump(&self.conn_errors);
                    let msg = e.to_string();
                    if msg.contains("Connection refused") || msg.contains("connection refused") || msg.contains("could not receive data from server") {
                        Self::bump(&self.refused);
                        eprintln!("Connection refused for {}: {}", who, msg);
                    } else {
                        eprintln!("Connection error for {}: {}", who, msg);
                    }
                    None
                }
                Err(_) => {
                    Self::bump(&self.conn_errors);
                    eprintln!("Connection timeout for {}", who);
                    None
                }
            }
        }

        /// Run one client operation under a 500ms deadline, tallying the outcome
        /// into `errs` (the read- or write-side counter).
        async fn op<T, E: std::fmt::Display>(&self, errs: &AtomicUsize, fut: impl Future<Output = std::result::Result<T, E>>, who: &str) {
            match timeout(Duration::from_millis(500), fut).await {
                Ok(Ok(_)) => Self::bump(&self.successes),
                Ok(Err(e)) => {
                    Self::bump(errs);
                    eprintln!("Operation error for {}: {}", who, e);
                }
                Err(_) => {
                    Self::bump(errs);
                    eprintln!("Operation timeout for {}", who);
                }
            }
        }
    }

    /// The INSERT every writer issues; date/timestamp are inlined SQL literals,
    /// as in the original tests.
    fn insert_sql() -> String {
        format!(
            "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary)
                                 VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
            chrono::Utc::now().date_naive(),
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        )
    }

    struct Row {
        project: &'static str,
        id: String,
        name: String,
        status_message: &'static str,
        summary: Vec<String>,
    }

    async fn insert_row(tally: &Tally, client: &Client, row: Row, who: &str) {
        let sql = insert_sql();
        tally.op(&tally.write_errors, client.execute(&sql, &[&row.project, &row.id, &row.name, &"OK", &row.status_message, &"INFO", &row.summary]), who).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[serial]
    async fn test_connection_rejection_under_pressure() -> Result<()> {
        let server = PressureTestServer::start().await?;
        let tally = Arc::new(Tally::default());

        const CONCURRENT_CLIENTS: usize = 100;
        const OPS_PER_CLIENT: usize = 10;
        const CONNECTION_TIMEOUT_MS: u64 = 900;

        let handles: Vec<_> = (0..CONCURRENT_CLIENTS)
            .map(|client_id| {
                let (port, test_id, tally) = (server.port, server.test_id.clone(), tally.clone());
                tokio::spawn(async move {
                    // No delay between ops - hammer the server.
                    for op in 0..OPS_PER_CLIENT {
                        let who = format!("client {} op {}", client_id, op);
                        if let Some(client) = tally.connect(port, CONNECTION_TIMEOUT_MS, &who).await {
                            insert_row(
                                &tally,
                                &client,
                                Row {
                                    project: "pressure_test",
                                    id: format!("{}-client-{}-op-{}", test_id, client_id, op),
                                    name: format!("pressure_span_{client_id}_{op}"),
                                    status_message: "Pressure test",
                                    summary: vec![format!("Pressure test op {} from client {}", op, client_id)],
                                },
                                &who,
                            )
                            .await;
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.await;
        }

        let refused = Tally::get(&tally.refused);
        let errors = Tally::get(&tally.conn_errors) + Tally::get(&tally.write_errors) + Tally::get(&tally.read_errors);
        let successes = Tally::get(&tally.successes);
        let attempted = (CONCURRENT_CLIENTS * OPS_PER_CLIENT) as f64;

        println!("\n=== Connection Pressure Test Results ===");
        println!("Total operations attempted: {}", CONCURRENT_CLIENTS * OPS_PER_CLIENT);
        println!("Successful operations: {}", successes);
        println!("Total errors: {}", errors);
        println!("Connection refused errors: {}", refused);
        println!("Success rate: {:.2}%", (successes as f64 / attempted) * 100.0);
        println!("Connection refused rate: {:.2}%", (refused as f64 / attempted) * 100.0);

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
        let tally = Arc::new(Tally::default());

        const READERS: usize = 24;
        const WRITERS: usize = 24;
        const OPS_PER_WORKER: usize = 10;
        const CONNECT_TIMEOUT_MS: u64 = 500;

        const READ_QUERIES: [&str; 3] = [
            "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'exhaust_test'",
            "SELECT name FROM otel_logs_and_spans WHERE project_id = 'exhaust_test' LIMIT 5",
            "SELECT status_code, COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'exhaust_test' GROUP BY status_code",
        ];

        // `is_writer` false => reader; both halves hammer the same server concurrently.
        let handles: Vec<_> = (0..WRITERS + READERS)
            .map(|worker| {
                let (is_writer, worker_id) = (worker < WRITERS, worker % WRITERS);
                let (port, test_id, tally) = (server.port, server.test_id.clone(), tally.clone());
                tokio::spawn(async move {
                    for op in 0..OPS_PER_WORKER {
                        let who = format!("{} {} op {}", if is_writer { "writer" } else { "reader" }, worker_id, op);
                        let Some(client) = tally.connect(port, CONNECT_TIMEOUT_MS, &who).await else {
                            continue;
                        };
                        if is_writer {
                            insert_row(
                                &tally,
                                &client,
                                Row {
                                    project: "exhaust_test",
                                    id: format!("{}-w{}-{}", test_id, worker_id, op),
                                    name: format!("write_{worker_id}_{op}"),
                                    status_message: "Write test",
                                    summary: vec!["Concurrent write".to_string()],
                                },
                                &who,
                            )
                            .await;
                        } else {
                            tally.op(&tally.read_errors, client.query(READ_QUERIES[op % READ_QUERIES.len()], &[]), &who).await;
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.await;
        }

        let (conn_errs, read_errs, write_errs) = (Tally::get(&tally.conn_errors), Tally::get(&tally.read_errors), Tally::get(&tally.write_errors));

        println!("\n=== Concurrent Read/Write Pressure Test Results ===");
        println!("Connection errors: {}", conn_errs);
        println!("Read errors: {}", read_errs);
        println!("Write errors: {}", write_errs);
        println!("Total errors: {}", conn_errs + read_errs + write_errs);

        // With reduced concurrency, we might not see errors
        if conn_errs + read_errs + write_errs > 0 {
            println!("\nTest successfully reproduced connection/operation errors under concurrent load.");
        } else {
            println!("\nNo errors with this concurrency level. Server handled the load successfully.");
        }

        Ok(())
    }
}
