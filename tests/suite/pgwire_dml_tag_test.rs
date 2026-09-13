//! Wire-level tests: pgwire `Describe Statement` for an INSERT/UPDATE/DELETE
//! without RETURNING must reply `NoData`, not a `RowDescription`.
//!
//! Requires MinIO on 127.0.0.1:9000 (`make minio-start`).

mod pgwire_dml_tag {
    use anyhow::Result;
    use serial_test::serial;
    use tokio_postgres::{Client, SimpleQueryMessage};
    use uuid::Uuid;

    use crate::pg_client_compat::TestServer;

    const SPAN_INSERT_COLS: &str =
        "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary)";

    /// `project_expr`/`id_expr` are SQL fragments (a literal or a `$N` placeholder),
    /// so the same row works on both the extended and simple protocols.
    fn span_insert(project_expr: &str, id_expr: &str) -> String {
        format!("{SPAN_INSERT_COLS} VALUES ({project_expr}, CURRENT_DATE, NOW(), {id_expr}, 'n', 'OK', 'm', 'INFO', ARRAY[]::text[], ARRAY['s'])")
    }

    /// Starts the server with both tables this file touches pre-created.
    async fn start() -> Result<(TestServer, Client)> {
        let server = TestServer::start_with_tables(&["otel_logs_and_spans", "variant_bench"]).await?;
        let client = server.client().await?;
        Ok((server, client))
    }

    /// Prepared DML must describe as NoData, or strict clients drop the write.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn prepared_dml_describes_as_no_data() -> Result<()> {
        let (_server, client) = start().await?;

        let cases: &[(&str, String)] = &[
            ("INSERT", format!("{SPAN_INSERT_COLS} VALUES ($1, CURRENT_DATE, NOW(), $2, $3, $4, $5, $6, ARRAY[]::text[], $7)")),
            ("UPDATE", "UPDATE otel_logs_and_spans SET status_message = $1 WHERE project_id = $2 AND id = $3".into()),
            ("DELETE", "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2".into()),
            // Variant column path — exercises VariantInsertRewriter.
            (
                "Variant INSERT",
                "INSERT INTO variant_bench (project_id, date, timestamp, id, shape, payload, payload_json) \
                                VALUES ($1, CURRENT_DATE, NOW(), $2, 'flat', $3, $4)"
                    .into(),
            ),
        ];

        for (label, sql) in cases {
            let stmt = client.prepare(sql).await?;
            assert!(stmt.columns().is_empty(), "{label}: expected NoData, got {:?}", stmt.columns().iter().map(|c| c.name()).collect::<Vec<_>>(),);
        }
        Ok(())
    }

    /// Bind + execute still writes the row, and on the simple-query path no
    /// `Row` message may precede `CommandComplete`. `simple_query` is used
    /// because `execute` would discard stray rows; its SQL is interpolated
    /// since `$N` placeholders only exist in the extended protocol.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn prepared_and_simple_query_insert_write_rows_without_row_messages() -> Result<()> {
        let (_server, client) = start().await?;

        // Extended protocol: bind + execute.
        let id = Uuid::new_v4().to_string();
        let n = client.execute(&span_insert("$1", "$2"), &[&"test_project", &id]).await?;
        assert_eq!(n, 1);
        let row = client.query_one("SELECT id FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&"test_project", &id]).await?;
        assert_eq!(row.get::<_, String>(0), id);

        // Simple-query protocol: raw message stream.
        let simple_id = Uuid::new_v4().to_string();
        let msgs = client.simple_query(&span_insert("'test_project'", &format!("'{simple_id}'"))).await?;
        assert!(!msgs.iter().any(|m| matches!(m, SimpleQueryMessage::Row(_))), "INSERT must not emit DataRow messages");
        assert!(msgs.iter().any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))), "expected CommandComplete");
        Ok(())
    }

    /// Second, independent client: sqlx surfaces the same Describe metadata,
    /// so a tokio-postgres quirk cannot mask the wire bug.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn sqlx_describe_insert_returns_no_columns() -> Result<()> {
        use sqlx::{Column, Connection, Executor};

        let (server, _client) = start().await?;
        let url = format!("postgres://postgres:postgres@localhost:{}/postgres", server.port);
        let mut conn = sqlx::postgres::PgConnection::connect(&url).await?;

        let describe = conn.describe(&span_insert("$1", "$2")).await?;
        assert!(
            describe.columns.is_empty(),
            "sqlx::describe must report no columns for INSERT without RETURNING; got {:?}",
            describe.columns.iter().map(|c| c.name().to_string()).collect::<Vec<_>>(),
        );
        Ok(())
    }

    /// An UPDATE whose FROM unnests bound array params must execute. Only a
    /// real bind + execute reaches the pgwire DML hook that rebuilds plan nodes
    /// after param substitution; `optimize()`/`create_physical_plan()` alone
    /// does not reproduce it.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn update_from_param_unnest_executes() -> Result<()> {
        let (_server, client) = start().await?;
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
        // The guard is reaching a clean `CommandComplete` (any row count), not
        // the number of rows matched.
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
