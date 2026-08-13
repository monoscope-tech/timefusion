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

/// pgAdmin's connect-time role probe, with the bound parameter it really sends.
///
/// The query is unplannable — `ARRAY(WITH RECURSIVE ...)` hits both DataFusion's
/// missing array-subquery constructor and a recursive-CTE planning bug — so
/// `PgCompatibilityHook` answers it instead. This covers what the .slt case
/// cannot: the extended protocol with a parameter bound, where the hook's plan
/// declares no placeholders. Booleans must arrive as real bools, not the
/// strings "t"/"f", because pgAdmin treats any non-empty string as true.
#[tokio::test(flavor = "multi_thread")]
async fn pgadmin_role_probe_answers_with_a_bound_parameter() -> Result<()> {
    let server = TestServer::start().await?;
    let row = server
        .client()
        .await?
        .query_one(
            "SELECT roles.oid AS id, roles.rolname AS name, roles.rolsuper AS is_superuser,
             CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END AS can_create_role,
             CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreatedb END AS can_create_db,
             CASE WHEN $1 = any(array(WITH RECURSIVE cte AS (
               SELECT pg_roles.oid, pg_roles.rolname FROM pg_catalog.pg_roles WHERE pg_roles.oid = roles.oid
               UNION ALL
               SELECT m.roleid, pgr.rolname FROM cte cte_1
                 JOIN pg_catalog.pg_auth_members m ON m.member = cte_1.oid
                 JOIN pg_catalog.pg_roles pgr ON pgr.oid = m.roleid)
               SELECT rolname FROM cte)) THEN true ELSE false END AS can_signal_backend
             FROM pg_catalog.pg_roles AS roles WHERE rolname = session_user",
            &[&"pg_signal_backend"],
        )
        .await?;
    assert_eq!(row.get::<_, i32>("id"), 0);
    assert_eq!(row.get::<_, &str>("name"), "postgres");
    for flag in ["is_superuser", "can_create_role", "can_create_db", "can_signal_backend"] {
        assert!(row.get::<_, bool>(flag), "{flag} should be true for a superuser");
    }
    Ok(())
}
