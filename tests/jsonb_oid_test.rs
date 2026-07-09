//! Regression: `jsonb_build_array` / `to_jsonb` must surface PG OID 3802 (jsonb)
//! over the wire, not 25 (text). Binary protocol must also include the
//! `0x01` jsonb version byte so strict drivers (e.g. hasql) accept the row.
//!
//! Background: monoscope's hasql jsonb decoder rejected TF responses because
//! TF used to alias `jsonb_*` to `json_*` and emit Utf8View → text OID.

#[cfg(test)]
mod jsonb_oid {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use anyhow::Result;
    use datafusion_postgres::ServerOptions;
    use serial_test::serial;
    use timefusion::{config::AppConfig, database::Database};
    use tokio::sync::Notify;
    use tokio_postgres::{NoTls, types::Type};
    use uuid::Uuid;

    const JSONB_OID: u32 = 3802;
    const JSON_OID: u32 = 114;

    struct Server {
        port:     u16,
        shutdown: Arc<Notify>,
    }

    impl Server {
        async fn start() -> Result<Self> {
            timefusion::test_utils::init_test_logging();
            let test_id = Uuid::new_v4().to_string();
            let port = 5600 + (rand::random::<u16>() % 200);

            let mut cfg = AppConfig::default();
            cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
            cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
            cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
            cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
            cfg.aws.aws_default_region = Some("us-east-1".to_string());
            cfg.aws.aws_allow_http = Some("true".to_string());
            cfg.core.timefusion_table_prefix = format!("test-{}", test_id);
            cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-{}", test_id));
            cfg.cache.timefusion_foyer_disabled = true;

            let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
            db.get_or_create_table("test_project", "otel_logs_and_spans").await?;

            let shutdown = Arc::new(Notify::new());
            let sd = shutdown.clone();
            let db_clone = db.clone();
            tokio::spawn(async move {
                let mut ctx = db_clone.clone().create_session_context();
                db_clone.setup_session_context(&mut ctx).unwrap();
                let opts = ServerOptions::new().with_port(port).with_host("0.0.0.0".to_string());
                let auth = timefusion::pgwire_handlers::AuthConfig {
                    username: "postgres".into(),
                    password: Some("postgres".into()),
                };
                tokio::select! {
                    _ = sd.notified() => {}
                    _ = timefusion::pgwire_handlers::serve_with_logging(
                        Arc::new(ctx), &opts, auth, None, None, std::future::pending::<()>()
                    ) => {}
                }
            });

            // Wait for readiness
            let conn_str = format!("host=localhost port={port} user=postgres password=postgres");
            for _ in 0..100 {
                if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                    tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    drop(client);
                    return Ok(Self { port, shutdown });
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            anyhow::bail!("server never came up")
        }

        async fn connect(&self) -> Result<tokio_postgres::Client> {
            let conn_str = format!("host=localhost port={} user=postgres password=postgres", self.port);
            let (client, conn) = tokio_postgres::connect(&conn_str, NoTls).await?;
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Ok(client)
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.shutdown.notify_one();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn jsonb_build_array_returns_jsonb_oid() -> Result<()> {
        let server = Server::start().await?;
        let client = server.connect().await?;

        // tokio-postgres `prepare` round-trips RowDescription; column type OID
        // is what hasql / strict drivers inspect.
        let stmt = client.prepare("SELECT jsonb_build_array(1, 'a', true) AS j").await?;
        assert_eq!(
            stmt.columns()[0].type_().oid(),
            JSONB_OID,
            "jsonb_build_array must surface PG jsonb OID, not text"
        );

        // Binary decode via serde_json::Value (tokio-postgres uses binary by default).
        let row = client.query_one("SELECT jsonb_build_array(1, 'a', true) AS j", &[]).await?;
        let v: serde_json::Value = row.get(0);
        assert_eq!(v, serde_json::json!([1, "a", true]));

        // to_jsonb same story
        let stmt2 = client.prepare("SELECT to_jsonb('{\"k\":\"v\"}') AS j").await?;
        assert_eq!(stmt2.columns()[0].type_().oid(), JSONB_OID);

        // json (non-b) variants must still be text — we did NOT alias json_build_array.
        let stmt3 = client.prepare("SELECT json_build_array(1) AS j").await?;
        let oid = stmt3.columns()[0].type_().oid();
        assert!(oid != JSONB_OID, "json_build_array must not claim jsonb (was {oid})");
        // It's actually text today; pin that so we notice if it changes.
        assert_eq!(oid, Type::TEXT.oid(), "json_build_array stays text-typed; OID {oid}");
        let _ = JSON_OID; // referenced for future use

        Ok(())
    }

    /// Regression: bare Variant columns (e.g. `context` in otel_logs_and_spans)
    /// get wrapped with `variant_to_json()` by VariantPgwireRootWrap, but the
    /// wrap surfaced text OID 25 — monoscope's hasql decoder expects jsonb 3802
    /// (UnexpectedColumnTypeStatementError 3802 25 on logItemDetails).
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn bare_variant_column_returns_jsonb_oid() -> Result<()> {
        let server = Server::start().await?;
        let client = server.connect().await?;

        client
            .execute(
                &format!(
                    "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, hashes, summary, context) \
                     VALUES ('test_project', {}, '{}', 'jsonb-oid-row', ARRAY[]::text[], ARRAY['s'], '{{\"trace_id\":\"abc\"}}')",
                    chrono::Utc::now().date_naive(),
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
                ),
                &[],
            )
            .await?;

        // Mirrors monoscope's logItemDetails query shape: bare Variant column in projection.
        let stmt = client
            .prepare("SELECT context FROM otel_logs_and_spans WHERE project_id = 'test_project' AND id = 'jsonb-oid-row' LIMIT 1")
            .await?;
        assert_eq!(
            stmt.columns()[0].type_().oid(),
            JSONB_OID,
            "bare Variant column must surface jsonb OID, not text"
        );

        // Binary decode (tokio-postgres uses binary format, same as hasql) —
        // exercises the 0x01 jsonb version-byte path with real row data.
        let row = client.query_one(&stmt, &[]).await?;
        let v: serde_json::Value = row.get(0);
        assert_eq!(v["trace_id"], "abc");

        Ok(())
    }
}
