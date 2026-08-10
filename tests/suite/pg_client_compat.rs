//! Wire-level PostgreSQL client compatibility regressions.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result};
use datafusion_postgres::ServerOptions;
use timefusion::{database::Database, test_utils::test_helpers::minio_test_config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

struct TestServer {
    db: Arc<Database>,
    port: u16,
    shutdown: Arc<Notify>,
}

impl TestServer {
    async fn start() -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let db = Arc::new(Database::with_config(minio_test_config(&id, &format!("/tmp/timefusion-{id}"))).await?);
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            let mut ctx = db_clone.clone().create_session_context();
            db_clone.setup_session_context(&mut ctx).expect("setup context");
            let options = ServerOptions::new().with_host("127.0.0.1".into()).with_port(port);
            let auth = timefusion::pgwire_handlers::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };
            tokio::select! {
                _ = shutdown_clone.notified() => {}
                result = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &options, auth, None, None, std::future::pending::<()>()) => {
                    if let Err(error) = result { eprintln!("server error: {error:?}"); }
                }
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return Ok(Self { db, port, shutdown });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("pgwire server did not accept connections within 10s")
    }

    async fn client(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=postgres password=postgres", self.port), NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

fn startup_packet() -> Vec<u8> {
    let mut body = 196_610_i32.to_be_bytes().to_vec();
    for (name, value) in [("user", "postgres"), ("database", "postgres"), ("_pq_.test_protocol_negotiation", "1")] {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    packet.extend(body);
    packet
}

#[tokio::test(flavor = "multi_thread")]
async fn protocol_3_2_reports_unsupported_options_before_authentication() -> Result<()> {
    let server = TestServer::start().await?;
    let mut socket = TcpStream::connect(("127.0.0.1", server.port)).await?;
    socket.write_all(&startup_packet()).await?;

    let mut header = [0; 5];
    socket.read_exact(&mut header).await?;
    assert_eq!(header[0], b'v');
    let message_len = i32::from_be_bytes(header[1..].try_into()?) as usize;
    let mut payload = vec![0; message_len.checked_sub(4).context("invalid protocol-negotiation message length")?];
    socket.read_exact(&mut payload).await?;

    assert_eq!(i32::from_be_bytes(payload[..4].try_into()?), 196_610);
    assert_eq!(i32::from_be_bytes(payload[4..8].try_into()?), 1);
    let options = std::str::from_utf8(&payload[8..])?.split('\0').filter(|option| !option.is_empty()).collect::<Vec<_>>();
    assert_eq!(options, ["_pq_.test_protocol_negotiation"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_query_does_not_create_a_routing_scan() -> Result<()> {
    let server = TestServer::start().await?;
    let before = server.db.scan_metrics.provider_scan_total.load(Ordering::Relaxed);
    let count: i64 = server.client().await?.query_one("SELECT COUNT(*) FROM pg_catalog.pg_class", &[]).await?.get(0);
    assert!(count > 0);
    assert_eq!(server.db.scan_metrics.provider_scan_total.load(Ordering::Relaxed), before);
    Ok(())
}
