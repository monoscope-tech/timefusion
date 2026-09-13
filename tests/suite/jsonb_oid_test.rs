//! `jsonb_*` results must surface PG OID 3802 (jsonb), not 25 (text), and the
//! binary encoding must carry the `0x01` jsonb version byte.

#[cfg(test)]
mod jsonb_oid {
    use anyhow::Result;
    use serial_test::serial;
    use tokio_postgres::types::Type;

    use crate::pgwire_harness::TestServer;

    const JSONB_OID: u32 = 3802;

    /// Type OID of the first column reported in `sql`'s RowDescription.
    async fn column_oid(client: &tokio_postgres::Client, sql: &str) -> Result<u32> {
        Ok(client.prepare(sql).await?.columns()[0].type_().oid())
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn jsonb_build_array_returns_jsonb_oid() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

        for (sql, want, why) in [
            ("SELECT jsonb_build_array(1, 'a', true) AS j", JSONB_OID, "jsonb_build_array must surface PG jsonb OID, not text"),
            ("SELECT to_jsonb('{\"k\":\"v\"}') AS j", JSONB_OID, "to_jsonb must surface PG jsonb OID, not text"),
            // json (non-b) variants must stay text-typed.
            ("SELECT json_build_array(1) AS j", Type::TEXT.oid(), "json_build_array stays text-typed, must not claim jsonb"),
        ] {
            assert_eq!(column_oid(&client, sql).await?, want, "{why} ({sql})");
        }

        // tokio-postgres decodes in binary format by default.
        let row = client.query_one("SELECT jsonb_build_array(1, 'a', true) AS j", &[]).await?;
        let v: serde_json::Value = row.get(0);
        assert_eq!(v, serde_json::json!([1, "a", true]));

        Ok(())
    }

    /// A bare Variant column is wrapped by `variant_to_json()`; that wrap must
    /// still report jsonb, not text.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn bare_variant_column_returns_jsonb_oid() -> Result<()> {
        let server = TestServer::start().await?;
        let client = server.client().await?;

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

        let stmt = client.prepare("SELECT context FROM otel_logs_and_spans WHERE project_id = 'test_project' AND id = 'jsonb-oid-row' LIMIT 1").await?;
        assert_eq!(stmt.columns()[0].type_().oid(), JSONB_OID, "bare Variant column must surface jsonb OID, not text");

        // Binary decode exercises the 0x01 jsonb version-byte path.
        let row = client.query_one(&stmt, &[]).await?;
        let v: serde_json::Value = row.get(0);
        assert_eq!(v["trace_id"], "abc");

        Ok(())
    }
}
