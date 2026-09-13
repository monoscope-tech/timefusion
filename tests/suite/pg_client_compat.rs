//! Wire-level PostgreSQL client compatibility regressions.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use datafusion_postgres::ServerOptions;
use timefusion::{database::Database, support::test_helpers::minio_test_config};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Notify,
};
use tokio_postgres::NoTls;
use uuid::Uuid;

/// Connect to a local pgwire server, retrying until `timeout` elapses; the
/// connection task is spawned, so callers only need the client.
pub(crate) async fn connect_with_retry(port: u16, timeout: Duration) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let conn_str = format!("host=127.0.0.1 port={port} user=postgres password=postgres");
    let deadline = tokio::time::Instant::now() + timeout;
    let (client, connection) = loop {
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok(pair) => break pair,
            Err(e) if tokio::time::Instant::now() >= deadline => return Err(e),
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    };
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("conn error: {e}");
        }
    });
    Ok(client)
}

pub(crate) struct TestServer {
    pub(crate) port: u16,
    shutdown: Arc<Notify>,
}

impl TestServer {
    pub(crate) async fn start() -> Result<Self> {
        Self::start_with_tables(&[]).await
    }

    /// `tables` are pre-created under `test_project` before the server starts,
    /// so schema failures surface here rather than as a lazy-create error later.
    pub(crate) async fn start_with_tables(tables: &[&str]) -> Result<Self> {
        timefusion::support::init_test_logging();
        let id = Uuid::new_v4().to_string();
        // OS-assigned free port: bind, capture, drop; the re-bind race is harmless.
        let port = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();
        let db = Arc::new(Database::with_config(minio_test_config(&id, &format!("/tmp/timefusion-{id}"))).await?);
        for table in tables {
            db.get_or_create_table("test_project", table).await?;
        }
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            let mut ctx = db_clone.clone().create_session_context();
            db_clone.setup_session_context(&mut ctx).expect("setup context");
            let options = ServerOptions::new().with_host("127.0.0.1".into()).with_port(port);
            let auth = timefusion::server::AuthConfig { username: "postgres".into(), password: Some("postgres".into()) };
            tokio::select! {
                _ = shutdown_clone.notified() => {}
                result = timefusion::server::serve_with_logging(Arc::new(ctx), &options, auth, None, None, std::future::pending::<()>()) => {
                    if let Err(error) = result { eprintln!("server error: {error:?}"); }
                }
            }
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while tokio::time::Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
                return Ok(Self { port, shutdown });
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        anyhow::bail!("pgwire server did not accept connections within 10s")
    }

    /// The server accepts TCP before it can authenticate, so this retries.
    pub(crate) async fn client(&self) -> Result<tokio_postgres::Client> {
        connect_with_retry(self.port, Duration::from_secs(10)).await.context("pgwire server did not accept connections within 10s")
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
    timefusion::observability::init_local_metrics_for_test();
    let server = TestServer::start().await?;
    let before = timefusion::observability::counter_value(timefusion::database::scan_metric_names::PROVIDER_SCAN_TOTAL);
    let count: i64 = server.client().await?.query_one("SELECT COUNT(*) FROM pg_catalog.pg_class", &[]).await?.get(0);
    assert!(count > 0);
    assert_eq!(timefusion::observability::counter_value(timefusion::database::scan_metric_names::PROVIDER_SCAN_TOTAL), before);
    Ok(())
}

/// pgAdmin's connect-time role probe: unplannable SQL answered by
/// `PgCompatibilityHook` over the extended protocol with a parameter bound,
/// where the hook's plan declares no placeholders. Booleans must arrive as real
/// bools, not "t"/"f" strings — pgAdmin treats any non-empty string as true.
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

/// Runs `sql` over the SIMPLE protocol against a fresh server and renders every
/// row as `chart_name=chart_data`, sorted so multi-branch results are stable.
async fn simple_query_charts(sql: &str) -> Result<Vec<String>> {
    let server = TestServer::start().await?;
    let messages = server.client().await?.simple_query(sql).await?;
    let mut charts: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(format!("{}={}", row.get("chart_name").unwrap_or_default(), row.get("chart_data").unwrap_or_default()))
            }
            _ => None,
        })
        .collect();
    charts.sort();
    Ok(charts)
}

/// `row_to_json(t)` names a whole row, which DataFusion rejects during planning;
/// RowToJsonRecordRewriter turns it into named_struct. Pins the SIMPLE protocol,
/// which the .slt harness does not exercise.
#[tokio::test(flavor = "multi_thread")]
async fn pgadmin_dashboard_row_to_json_over_simple_protocol() -> Result<()> {
    let charts = simple_query_charts(
        "SELECT 'session_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data \
         FROM (SELECT (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS \"total\", \
                      (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'active') AS \"active\") t",
    )
    .await?;
    assert_eq!(charts, [r#"session_stats={"active":0,"total":0}"#]);
    Ok(())
}

/// The row_to_json rewrite must reach every UNION ALL branch, not just a
/// top-level Select, and must survive capitalised quoted aliases.
#[tokio::test(flavor = "multi_thread")]
async fn pgadmin_dashboard_rewrites_every_union_branch() -> Result<()> {
    let charts = simple_query_charts(
        "SELECT 'session_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data \
         FROM (SELECT (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS \"Total\", \
                      (SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'idle') AS \"Idle\") t \
         UNION ALL \
         SELECT 'tps_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data \
         FROM (SELECT (SELECT count(*) FROM pg_catalog.pg_stat_database) AS \"Transactions\") t",
    )
    .await?;
    assert_eq!(charts.len(), 2, "both union branches must return");
    assert_eq!(charts, [r#"session_stats={"Idle":0,"Total":0}"#, r#"tps_stats={"Transactions":0}"#]);
    Ok(())
}
