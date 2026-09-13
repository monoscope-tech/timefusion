//! Pgwire server behaviour under heavy concurrent connect/query load.

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
        // Kernel-assigned port: these servers start concurrently with other tests.
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
    /// Connect failures the client reported as an explicit refusal.
    refused: AtomicUsize,
    /// All connect failures (refusals and connect timeouts).
    conn_errors: AtomicUsize,
    write_errors: AtomicUsize,
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
        let conn_str = format!("host=localhost port={port} user=postgres password=postgres");
        let msg = match timeout(Duration::from_millis(connect_timeout_ms), tokio_postgres::connect(&conn_str, NoTls)).await {
            Ok(Ok((client, conn))) => {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        eprintln!("Connection handler error: {e}");
                    }
                });
                return Some(client);
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => "connection timeout".to_string(),
        };
        Self::bump(&self.conn_errors);
        let refused = ["Connection refused", "connection refused", "could not receive data from server"].iter().any(|m| msg.contains(m));
        if refused {
            Self::bump(&self.refused);
        }
        eprintln!("Connection {} for {who}: {msg}", if refused { "refused" } else { "error" });
        None
    }

    /// Run one client operation under a 500ms deadline, tallying the outcome into `errs`.
    async fn op<T, E: std::fmt::Display>(&self, errs: &AtomicUsize, fut: impl Future<Output = std::result::Result<T, E>>, who: &str) {
        let msg = match timeout(Duration::from_millis(500), fut).await {
            Ok(Ok(_)) => return Self::bump(&self.successes),
            Ok(Err(e)) => e.to_string(),
            Err(_) => "operation timeout".to_string(),
        };
        Self::bump(errs);
        eprintln!("Operation error for {who}: {msg}");
    }
}

/// The INSERT every writer issues; date/timestamp are inlined SQL literals.
fn insert_sql() -> String {
    format!(
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary)
                             VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
        chrono::Utc::now().date_naive(),
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    )
}

/// One writer op: a single INSERT whose row identifies its worker.
async fn insert_row(tally: &Tally, client: &Client, project: &'static str, test_id: &str, worker_id: usize, op: usize, who: &str) {
    let sql = insert_sql();
    let (id, name) = (format!("{test_id}-w{worker_id}-{op}"), format!("write_{worker_id}_{op}"));
    let summary = vec![format!("{project} op {op} from worker {worker_id}")];
    tally.op(&tally.write_errors, client.execute(&sql, &[&project, &id, &name, &"OK", &"Pressure test", &"INFO", &summary]), who).await;
}

/// `writers` INSERT workers plus `readers` SELECT workers, each doing `ops`
/// connect-then-operate cycles concurrently with no delay between ops.
async fn run_pressure(server: &PressureTestServer, project: &'static str, writers: usize, readers: usize, ops: usize, connect_timeout_ms: u64) -> Arc<Tally> {
    let tally = Arc::new(Tally::default());
    let reads = Arc::new([
        format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project}'"),
        format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project}' LIMIT 5"),
        format!("SELECT status_code, COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project}' GROUP BY status_code"),
    ]);

    let handles: Vec<_> = (0..writers + readers)
        .map(|worker| {
            let (is_writer, worker_id) = (worker < writers, worker % writers);
            let (port, test_id, tally, reads) = (server.port, server.test_id.clone(), tally.clone(), reads.clone());
            tokio::spawn(async move {
                for op in 0..ops {
                    let who = format!("{} {} op {}", if is_writer { "writer" } else { "reader" }, worker_id, op);
                    let Some(client) = tally.connect(port, connect_timeout_ms, &who).await else {
                        continue;
                    };
                    if is_writer {
                        insert_row(&tally, &client, project, &test_id, worker_id, op, &who).await;
                    } else {
                        tally.op(&tally.read_errors, client.query(reads[op % reads.len()].as_str(), &[]), &who).await;
                    }
                }
            })
        })
        .collect();
    for handle in handles {
        let _ = handle.await;
    }
    tally
}

/// Prints a finished run's breakdown and returns its total error count.
fn report(title: &str, tally: &Tally, attempted: usize) -> usize {
    let (conn, read, write) = (Tally::get(&tally.conn_errors), Tally::get(&tally.read_errors), Tally::get(&tally.write_errors));
    let (refused, successes, errors) = (Tally::get(&tally.refused), Tally::get(&tally.successes), conn + read + write);
    let pct = |n: usize| n as f64 / attempted as f64 * 100.0;
    println!(
        "\n=== {title} ===\nTotal operations attempted: {attempted}\nSuccessful operations: {successes}\n\
         Connection errors: {conn} (connection refused: {refused})\nRead errors: {read}\nWrite errors: {write}\nTotal errors: {errors}\n\
         Success rate: {:.2}%\nConnection refused rate: {:.2}%",
        pct(successes),
        pct(refused)
    );
    errors
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn test_connection_rejection_under_pressure() -> Result<()> {
    const CONCURRENT_CLIENTS: usize = 100;
    const OPS_PER_CLIENT: usize = 10;
    const CONNECTION_TIMEOUT_MS: u64 = 900;

    let server = PressureTestServer::start().await?;
    let tally = run_pressure(&server, "pressure_test", CONCURRENT_CLIENTS, 0, OPS_PER_CLIENT, CONNECTION_TIMEOUT_MS).await;
    let errors = report("Connection Pressure Test Results", &tally, CONCURRENT_CLIENTS * OPS_PER_CLIENT);

    assert!(errors > 0, "Expected to see some errors under pressure");

    println!("\nTest demonstrates connection issues under pressure.");
    if Tally::get(&tally.refused) == 0 {
        println!("Note: Got timeouts instead of explicit connection refusals.");
        println!("This still demonstrates the server cannot handle the load.");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[serial]
async fn test_connection_exhaustion_with_concurrent_reads_writes() -> Result<()> {
    const READERS: usize = 24;
    const WRITERS: usize = 24;
    const OPS_PER_WORKER: usize = 10;
    const CONNECT_TIMEOUT_MS: u64 = 500;

    let server = PressureTestServer::start().await?;
    let tally = run_pressure(&server, "exhaust_test", WRITERS, READERS, OPS_PER_WORKER, CONNECT_TIMEOUT_MS).await;
    let errors = report("Concurrent Read/Write Pressure Test Results", &tally, (WRITERS + READERS) * OPS_PER_WORKER);

    println!(
        "{}",
        if errors > 0 {
            "\nTest successfully reproduced connection/operation errors under concurrent load."
        } else {
            "\nNo errors with this concurrency level. Server handled the load successfully."
        }
    );

    Ok(())
}
