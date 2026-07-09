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
        port:     u16,
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
                    res = timefusion::pgwire_handlers::serve_with_logging(Arc::new(ctx), &opts, auth, None, None, std::future::pending::<()>()) => {
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

    /// Regression guard for the per-execute planning fix (2026-07-08).
    /// Monoscope's bulk insert is `INSERT … SELECT … FROM unnest($N::text[])`
    /// — a Projection/Unnest subtree that `try_fast_path_insert` (Values-only)
    /// declines, so it flows through the executor fall-through. That path now
    /// builds the physical plan directly (skipping the redundant per-execute
    /// `state.optimize()` that `DataFrame::collect` re-ran). This asserts the
    /// unnest INSERT still writes every row and reports the right affected count
    /// through the rewritten path. (The fast_path_hits/fallthrough counters are
    /// wired into `timefusion_stats` for prod, but the `set_global` OnceLock is
    /// first-write-wins so they can't be asserted across the shared-process
    /// serial harness.)
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn insert_select_unnest_falls_through_and_writes_rows() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        // unnest shape (monoscope prod) → fast-path decline → executor fall-through.
        let ids: Vec<String> = vec![Uuid::new_v4().to_string(), Uuid::new_v4().to_string()];
        let pids = vec!["test_project".to_string(); 2];
        let dates = vec!["2026-07-08".to_string(); 2];
        let tss = vec!["2026-07-08T00:00:00Z".to_string(); 2];
        let names = vec!["n".to_string(); 2];
        let hashes = vec!["".to_string(); 2];
        let summaries = vec!["a\u{1f}b".to_string(); 2];
        let unnest_n = client
            .execute(
                "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, hashes, summary) \
                 SELECT u.pid, u.d::date, u.ts::timestamp, u.id, u.nm, string_to_array(u.h, chr(31)), string_to_array(u.s, chr(31)) \
                 FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) \
                 AS u(pid, d, ts, id, nm, h, s)",
                &[&pids, &dates, &tss, &ids, &names, &hashes, &summaries],
            )
            .await?;
        assert_eq!(unnest_n, 2, "unnest INSERT must report all rows affected");

        for id in &ids {
            let row = client.query_one("SELECT id FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", id]).await?;
            assert_eq!(&row.get::<_, String>(0), id, "unnest row must be readable back");
        }

        // A parameterised UPDATE is cached + pre-optimized, so it too takes the
        // skip-optimize fall-through branch. Assert the affected-row count is
        // exact — proof that bypassing the execute-time re-optimize preserves
        // DML semantics for the non-INSERT shapes, not just INSERT.
        let upd = client
            .execute(
                "UPDATE otel_logs_and_spans SET name = $1 WHERE project_id = $2 AND id = $3",
                &[&"renamed", &"test_project", &ids[0]],
            )
            .await?;
        assert_eq!(upd, 1, "pre-optimized UPDATE (skip-optimize branch) must affect exactly the matched row");
        let name: String = client
            .query_one(
                "SELECT name FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&"test_project", &ids[0]],
            )
            .await?
            .get(0);
        assert_eq!(name, "renamed", "UPDATE must actually mutate the row through the rewritten path");
        Ok(())
    }

    /// Regression: monoscope's UPDATE-2 dual-write (`BackgroundJobs.hs`
    /// `dualExecPgTf`) sends the span/trace/tag arrays as bound params
    /// (`unnest($1::text[])`, Hasql `#{spanIds'}::text[]`) over the extended
    /// protocol. This crashed prod with `Internal error: Assertion failed:
    /// expr.is_empty(): Unnest(…)` even after `c8724f3` disabled leaf-expression
    /// pushdown — that flag never covered this path. The real trigger is
    /// `plan_cache::fold_literal_casts`, run by the pgwire DML hook after param
    /// substitution: it rebuilt every node via `Unnest::with_new_exprs`, which
    /// asserts an empty expr list while `Unnest::expressions()` returns its
    /// `exec_columns`. Only a real bind + execute hits that hook, so pure
    /// `optimize()` / `create_physical_plan()` on the TF session never reproduced it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn update_from_param_unnest_executes() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let id = Uuid::new_v4().to_string();

        client
            .execute(
                "INSERT INTO otel_logs_and_spans \
                   (project_id, date, timestamp, id, name, context___span_id, context___trace_id, hashes, summary) \
                 VALUES ('test_project', CURRENT_DATE, NOW(), $1, 'n', 's1', 't1', ARRAY[]::text[], ARRAY['s'])",
                &[&id],
            )
            .await?;

        let span_ids: Vec<String> = vec!["s1".into(), "s2".into()];
        let trace_ids: Vec<String> = vec!["t1".into(), "t2".into()];
        let tags: Vec<String> = vec!["pat:a".into(), "pat:b".into()];
        // The regression is the crash: before the fix, executing this bound
        // multi-column-unnest UPDATE returned `Internal error: Assertion failed:
        // expr.is_empty()`. Reaching a clean `CommandComplete` (any row count) is
        // the guard. Row-matching across MemBuffer/Delta is a separate concern.
        client
            .execute(
                "UPDATE otel_logs_and_spans o \
                    SET hashes = COALESCE(o.hashes, '{}'::text[]) || ARRAY[u.tag] \
                    FROM ( \
                      SELECT unnest($1::text[]) AS span_id, \
                             unnest($2::text[]) AS trace_id, \
                             unnest($3::text[]) AS tag \
                    ) u \
                    WHERE o.project_id = 'test_project' \
                      AND o.timestamp >= '2020-01-01T00:00:00Z' \
                      AND o.timestamp <  '2099-01-01T00:00:00Z' \
                      AND o.context___span_id = u.span_id \
                      AND o.context___trace_id = u.trace_id \
                      AND NOT (COALESCE(o.hashes, '{}'::text[]) @> ARRAY[u.tag])",
                &[&span_ids, &trace_ids, &tags],
            )
            .await?;
        Ok(())
    }
}
