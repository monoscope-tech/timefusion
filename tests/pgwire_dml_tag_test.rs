//! Wire-level regression test: pgwire `Describe Statement` for an
//! INSERT/UPDATE/DELETE without RETURNING must reply `NoData`, not a
//! `RowDescription` announcing the synthetic `count` column. Strict
//! prepare-validating clients (pgjdbc, Npgsql, psycopg3, sqlx, Hasql) drop
//! the write otherwise; lenient drivers (tokio-postgres' `execute`, asyncpg)
//! silently discard the extra DataRows, which is why our previous integration
//! tests missed the bug.
//!
//! Requires MinIO on 127.0.0.1:9000 (`make minio-start`).

mod pgwire_dml_tag {
    use std::{sync::Arc, time::Duration};

    use anyhow::{Context, Result};
    use datafusion_postgres::ServerOptions;
    use serial_test::serial;
    use timefusion::{database::Database, test_utils::test_helpers::minio_test_config};
    use tokio::{net::TcpListener, sync::Notify};
    use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
    use uuid::Uuid;

    const SPAN_INSERT_COLS: &str =
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary)";

    struct TestServer {
        port: u16,
        shutdown: Arc<Notify>,
    }

    impl TestServer {
        async fn start() -> Result<Self> {
            timefusion::test_utils::init_test_logging();
            let test_id = Uuid::new_v4().to_string();
            // OS-assigned free port: bind, capture, drop. The tiny race window
            // before the server re-binds is harmless in practice.
            let port = TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port();
            let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-{test_id}"));
            let db = Arc::new(Database::with_config(cfg).await?);
            // Pre-create both tables touched by the suite so failures here
            // (e.g. Variant schema misconfig) surface deterministically rather
            // than as a confusing lazy-create error during prepare().
            db.get_or_create_table("test_project", "otel_logs_and_spans").await?;
            db.get_or_create_table("test_project", "variant_bench").await?;

            let db_clone = db.clone();
            let shutdown = Arc::new(Notify::new());
            let shutdown_clone = shutdown.clone();
            tokio::spawn(async move {
                let mut ctx = db_clone.clone().create_session_context();
                db_clone.setup_session_context(&mut ctx).expect("setup ctx");
                let opts = ServerOptions::new().with_port(port).with_host("127.0.0.1".to_string());
                let auth = timefusion::pgwire_handlers::AuthConfig {
                    username: "postgres".into(),
                    password: Some("postgres".into()),
                };
                tokio::select! {
                    _ = shutdown_clone.notified() => {},
                    res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &opts, auth, None, std::future::pending::<()>()) => {
                        if let Err(e) = res { eprintln!("server error: {e:?}"); }
                    }
                }
            });
            Self::connect(port).await?;
            Ok(Self { port, shutdown })
        }

        async fn connect(port: u16) -> Result<Client> {
            let conn_str = format!("host=127.0.0.1 port={port} user=postgres password=postgres");
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut last_err = None;
            while tokio::time::Instant::now() < deadline {
                match tokio_postgres::connect(&conn_str, NoTls).await {
                    Ok((client, conn)) => {
                        tokio::spawn(async move {
                            if let Err(e) = conn.await {
                                eprintln!("conn error: {e}");
                            }
                        });
                        return Ok(client);
                    }
                    Err(e) => last_err = Some(e),
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(last_err.map(anyhow::Error::from).unwrap_or_else(|| anyhow::anyhow!("no connect attempt")))
                .context("pgwire server did not accept connections within 10s")
        }

        async fn client(&self) -> Result<Client> {
            Self::connect(self.port).await
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.notify_one();
        }
    }

    /// Hasql/pgjdbc poison-row surface: prepared DML must describe as NoData.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn prepared_dml_describes_as_no_data() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        let cases: &[(&str, String)] = &[
            (
                "INSERT",
                format!("{SPAN_INSERT_COLS} VALUES ($1, CURRENT_DATE, NOW(), $2, $3, $4, $5, $6, ARRAY[]::text[], $7)"),
            ),
            (
                "UPDATE",
                "UPDATE otel_logs_and_spans SET status_message = $1 WHERE project_id = $2 AND id = $3".into(),
            ),
            ("DELETE", "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2".into()),
            // Variant column path — exercises VariantInsertRewriter, monoscope's actual prod path.
            (
                "Variant INSERT",
                "INSERT INTO variant_bench (project_id, date, timestamp, id, shape, payload, payload_json) \
                                VALUES ($1, CURRENT_DATE, NOW(), $2, 'flat', $3, $4)"
                    .into(),
            ),
        ];

        for (label, sql) in cases {
            let stmt = client.prepare(sql).await?;
            assert!(
                stmt.columns().is_empty(),
                "{label}: expected NoData, got {:?}",
                stmt.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),
            );
        }
        Ok(())
    }

    /// Describe fix must not break Execute: bind + execute writes the row and
    /// the CommandComplete tag reports `affected = 1`.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn prepared_insert_executes_and_writes_row() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let id = Uuid::new_v4().to_string();
        let sql = format!("{SPAN_INSERT_COLS} VALUES ($1, CURRENT_DATE, NOW(), $2, 'n', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])");
        let n = client.execute(&sql, &[&"test_project", &id]).await?;
        assert_eq!(n, 1);

        let row = client
            .query_one("SELECT id FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &id])
            .await?;
        assert_eq!(row.get::<_, String>(0), id);
        Ok(())
    }

    /// Independent strict Rust client: sqlx surfaces the same Describe
    /// metadata pgjdbc/Hasql validate. Catches a regression in case a
    /// tokio-postgres-specific quirk ever masks the wire bug.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn sqlx_describe_insert_returns_no_columns() -> Result<()> {
        use sqlx::{Column, Connection, Executor};

        let server = TestServer::start().await?;
        let url = format!("postgres://postgres:postgres@localhost:{}/postgres", server.port);
        let mut conn = sqlx::postgres::PgConnection::connect(&url).await?;

        let describe = conn
            .describe(&format!(
                "{SPAN_INSERT_COLS} VALUES ($1, CURRENT_DATE, NOW(), $2, 'n', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])"
            ))
            .await?;
        assert!(
            describe.columns.is_empty(),
            "sqlx::describe must report no columns for INSERT without RETURNING; got {:?}",
            describe.columns.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
        );
        Ok(())
    }

    /// Simple-query path: no `Row` messages may precede `CommandComplete`.
    /// `simple_query` exposes the raw stream where `execute` would discard rows.
    /// SQL is built by interpolation (not parameterised) because simple-query
    /// is by definition the no-parameters wire path — `$N` placeholders only
    /// exist in the extended/prepared protocol.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn simple_query_insert_sends_no_row_messages() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let id = Uuid::new_v4().to_string();
        let sql = format!("{SPAN_INSERT_COLS} VALUES ('test_project', CURRENT_DATE, NOW(), '{id}', 'n', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])");
        let msgs = client.simple_query(&sql).await?;
        assert!(
            !msgs.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))),
            "INSERT must not emit DataRow messages"
        );
        assert!(
            msgs.iter().any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
            "expected CommandComplete"
        );
        Ok(())
    }
}
