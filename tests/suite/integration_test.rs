#[cfg(test)]
mod integration {
    use std::time::Duration;

    use anyhow::Result;
    use serial_test::serial;
    use timefusion::{database::Database, support::test_helpers::minio_test_config};
    use tokio_postgres::{Client, types::ToSql};
    use uuid::Uuid;

    use crate::pgwire_harness::TestServer;

    const PROJECT: &str = "test_project";

    /// Insert one row into `test_project` with level=INFO and empty hashes.
    async fn insert_row(client: &Client, id: &str, name: &str, status_code: &str, status_message: &str, summary: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO otel_logs_and_spans (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary)
             VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7)",
            chrono::Utc::now().date_naive(),
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        );
        exec(client, &sql, &[&PROJECT, &id, &name, &status_code, &status_message, &"INFO", &vec![summary.to_string()]]).await
    }

    async fn exec(client: &Client, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<()> {
        client.execute(sql, params).await?;
        Ok(())
    }

    async fn count(client: &Client, filter: &str, params: &[&(dyn ToSql + Sync)]) -> Result<i64> {
        Ok(client.query_one(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE {filter}"), params).await?.get(0))
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_postgres_integration() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        insert_row(&client, &server.test_id, "test_span_name", "OK", "Test integration", "Integration test summary").await?;

        assert_eq!(count(&client, "project_id = $1 AND id = $2", &[&PROJECT, &server.test_id]).await?, 1);

        let row = client.query_one("SELECT name, status_code FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&PROJECT, &server.test_id]).await?;
        assert_eq!(row.get::<_, String>(0), "test_span_name");
        assert_eq!(row.get::<_, String>(1), "OK");

        for i in 0..5 {
            let id = Uuid::new_v4().to_string();
            insert_row(&client, &id, &format!("batch_span_{i}"), "OK", &format!("Batch test {i}"), &format!("Batch test summary {i}")).await?;
        }

        assert_eq!(count(&client, "project_id = $1", &[&PROJECT]).await?, 6);

        let row = client.query_one("SELECT id, name, status_code, level FROM otel_logs_and_spans WHERE project_id = $1 LIMIT 1", &[&PROJECT]).await?;
        assert_eq!(row.columns().len(), 4);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_update_operations() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        let span_id = Uuid::new_v4().to_string();
        insert_row(&client, &span_id, "original_name", "OK", "Original message", "Original summary").await?;

        let set_hashes_1 = "UPDATE otel_logs_and_spans SET hashes = make_array($1) WHERE project_id = $2 AND id = $3";
        exec(&client, set_hashes_1, &[&"Updated message", &PROJECT, &span_id]).await?;

        let row = client.query_one("SELECT array_element(hashes, 1) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&PROJECT, &span_id]).await?;
        assert_eq!(row.get::<_, String>(0), "Updated message");

        let set_hashes_2 = "UPDATE otel_logs_and_spans SET hashes = make_array($1, $2) WHERE project_id = $3 AND id = $4";
        exec(&client, set_hashes_2, &[&"ERROR", &"ERROR", &PROJECT, &span_id]).await?;

        let row = client
            .query_one(
                "SELECT array_element(hashes, 1), array_element(hashes, 2) FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2",
                &[&PROJECT, &span_id],
            )
            .await?;
        assert_eq!(row.get::<_, String>(0), "ERROR");
        assert_eq!(row.get::<_, String>(1), "ERROR");

        for i in 0..3 {
            let status = if i % 2 == 0 { "OK" } else { "ERROR" };
            insert_row(&client, &format!("update_test_{i}"), "test", status, "Message", "Summary").await?;
        }

        let set_by_status = "UPDATE otel_logs_and_spans SET hashes = make_array($1) WHERE project_id = $2 AND status_code = $3";
        exec(&client, set_by_status, &[&"SUCCESS", &PROJECT, &"OK"]).await?;

        // THREE, not two: `status_code` is immutable, so the first span stays 'OK'
        // and is matched by this conditional UPDATE too.
        assert_eq!(count(&client, "project_id = $1 AND array_element(hashes, 1) = $2", &[&PROJECT, &"SUCCESS"]).await?, 3);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_delete_operations() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        let span_id = Uuid::new_v4().to_string();
        insert_row(&client, &span_id, "to_delete", "OK", "Message", "Summary").await?;
        assert_eq!(count(&client, "project_id = $1 AND id = $2", &[&PROJECT, &span_id]).await?, 1);

        exec(&client, "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2", &[&PROJECT, &span_id]).await?;
        assert_eq!(count(&client, "project_id = $1 AND id = $2", &[&PROJECT, &span_id]).await?, 0);

        for i in 0..4 {
            let status = match i % 3 {
                0 => "OK",
                1 => "ERROR",
                _ => "WARNING",
            };
            insert_row(&client, &format!("delete_test_{i}"), "test", status, "Message", "Summary").await?;
        }

        exec(&client, "DELETE FROM otel_logs_and_spans WHERE project_id = $1 AND status_code = $2", &[&PROJECT, &"ERROR"]).await?;

        assert_eq!(count(&client, "project_id = $1 AND status_code = $2", &[&PROJECT, &"ERROR"]).await?, 0);
        assert_eq!(count(&client, "project_id = $1", &[&PROJECT]).await?, 3);

        Ok(())
    }

    /// End-to-end Variant pipeline: INSERT of a JSON literal stores binary Variant,
    /// and SELECT returns it as JSON over the wire.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn test_variant_column_round_trips_as_json() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;
        let span_id = Uuid::new_v4().to_string();
        let attrs_json = r#"{"http":{"method":"GET","status":200},"user":"alice"}"#;

        let sql = format!(
            "INSERT INTO otel_logs_and_spans \
             (project_id, date, timestamp, id, name, status_code, status_message, level, hashes, summary, attributes) \
             VALUES ($1, {}, '{}', $2, $3, $4, $5, $6, ARRAY[]::text[], $7, '{}')",
            chrono::Utc::now().date_naive(),
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"),
            attrs_json
        );
        exec(&client, &sql, &[&PROJECT, &span_id, &"variant_round_trip", &"OK", &"with attrs", &"INFO", &vec!["summary"]]).await?;

        async fn attributes(client: &Client, id: &str, tail: &str) -> Result<serde_json::Value> {
            let sql = format!("SELECT attributes FROM otel_logs_and_spans WHERE project_id = $1 AND id = $2 {tail}");
            Ok(client.query_one(&sql, &[&PROJECT, &id]).await?.get(0))
        }

        // Bare Variant columns surface as jsonb (OID 3802), decoded as serde_json::Value.
        let parsed = attributes(&client, &span_id, "").await?;
        assert_eq!(parsed["http"]["method"], "GET");
        assert_eq!(parsed["user"], "alice");

        // The JSON wrapping must also survive a Sort+Limit above the projection.
        let parsed = attributes(&client, &span_id, "ORDER BY timestamp DESC LIMIT 1").await?;
        assert_eq!(parsed["http"]["status"], 200, "Sort+Limit path must round-trip variant as jsonb");

        Ok(())
    }
    // `Database` is Clone and shares one maintenance shutdown token: only the LAST
    // clone's drop may cancel it, or every cron job dies on the first clone drop.
    #[tokio::test]
    #[serial]
    async fn database_clone_drop_keeps_maintenance_alive() -> Result<()> {
        let test_id = Uuid::new_v4().to_string();
        let cfg = minio_test_config(&test_id, &format!("/tmp/timefusion-{test_id}"));
        // Schedulers must be running: their task closures hold guard-less clones,
        // otherwise the guard either fires early or can never fire (cycle).
        let db = Database::with_config(cfg).await?.start_maintenance_schedulers().await?;
        drop(db.clone());
        assert!(!db.is_maintenance_cancelled(), "dropping a Database clone must not cancel the shared maintenance token");
        db.cancel_maintenance();
        Ok(())
    }

    // A reused prepared statement carrying BOTH a bound param and now() must
    // re-evaluate now() on every execute, not freeze it at parse time.
    // tokio-postgres caches prepared statements by SQL text, so the second
    // `query()` below reuses the first statement.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn mixed_now_and_param_prepared_statement_stays_fresh() -> Result<()> {
        // Time-fn shape caching is read from the global config.
        let mut cfg = timefusion::config::AppConfig::default();
        cfg.memory.timefusion_plan_cache_time_fns = true;
        timefusion::config::set_config_for_test(cfg);

        let server = TestServer::start().await?;
        let client = server.client().await?;

        // The `::timestamptz` cast gives the injected now() placeholder a concrete type.
        let sql = "SELECT $1::bigint AS a, now()::timestamptz AS t";
        let r1 = client.query_one(sql, &[&42i64]).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let r2 = client.query_one(sql, &[&7i64]).await?; // same SQL → reused prepared stmt

        assert_eq!(r1.get::<_, i64>("a"), 42);
        assert_eq!(r2.get::<_, i64>("a"), 7);

        let t1: std::time::SystemTime = r1.get("t");
        let t2: std::time::SystemTime = r2.get("t");
        assert!(t2 > t1, "now() must advance across executes of a reused prepared statement (t1={t1:?} t2={t2:?})");

        drop(server);
        Ok(())
    }
}
