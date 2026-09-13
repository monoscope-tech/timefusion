#[cfg(test)]
mod test_json_functions {
    use anyhow::Result;
    use datafusion::prelude::SessionContext;
    use test_case::test_case;
    use timefusion::{database::Database, support::test_helpers::array_get_str as get_str};

    async fn session() -> Result<SessionContext> {
        let db = std::sync::Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;
        Ok(ctx)
    }

    /// Runs each `setup` statement, then `SELECT {expr}`, returning the single string cell.
    async fn eval_str(setup: &[&str], expr: &str) -> String {
        let ctx = session().await.expect("session");
        for stmt in setup {
            ctx.sql(stmt).await.expect("setup sql").collect().await.expect("setup collect");
        }
        let results = ctx.sql(&format!("SELECT {expr}")).await.expect("sql").collect().await.expect("collect");
        assert_eq!(results.len(), 1, "{expr}");
        assert_eq!(results[0].num_rows(), 1, "{expr}");
        get_str(results[0].column(0).as_ref(), 0)
    }

    const MAKE_TEST_TABLE: &[&str] = &[
        "CREATE TABLE test_table (id VARCHAR, name VARCHAR, duration BIGINT, summary VARCHAR)",
        r#"INSERT INTO test_table VALUES ('001', 'test_span', 1500, '{"status": "ok"}')"#,
    ];

    // to_jsonb is registered as an alias of to_json.
    #[test_case(&[], "json_build_array('a', 'b', 'c')" => r#"["a","b","c"]"# ; "json_build_array")]
    #[test_case(&[], r#"to_json('{"hello": "world"}')"# => r#"{"hello":"world"}"# ; "to_json object")]
    #[test_case(&[], "to_json(123)" => "123" ; "to_json number")]
    #[test_case(&[], r#"to_jsonb('{"hello": "world"}')"# => r#"{"hello":"world"}"# ; "to_jsonb alias object")]
    #[test_case(&[], "to_jsonb(123)" => "123" ; "to_jsonb alias number")]
    #[test_case(&[], "to_char(TIMESTAMP '2025-08-07T10:00:00Z', 'YYYY-MM-DD HH24:MI:SS')" => "2025-08-07 10:00:00" ; "to_char")]
    #[test_case(MAKE_TEST_TABLE, "json_build_array(id, name, duration, to_json(summary)) FROM test_table" => r#"["001","test_span",1500,{"status":"ok"}]"# ; "nested to_json inside json_build_array over a table")]
    #[tokio::test]
    async fn pg_json_scalar_functions(setup: &'static [&'static str], expr: &'static str) -> String {
        eval_str(setup, expr).await
    }

    #[tokio::test]
    async fn test_extract_epoch() -> Result<()> {
        let ctx = session().await?;

        let results = ctx.sql("SELECT extract_epoch(TIMESTAMP '2025-08-07T10:00:00Z') as result").await?.collect().await?;
        assert_eq!(results.len(), 1);
        let column = results[0].column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
        // The timestamp is interpreted as UTC.
        assert_eq!(value.value(0), 1754560800.0);

        Ok(())
    }

    // Paths here are Postgres SQL/JSON-path, NOT RFC 9535: dot-quoted members,
    // `like_regex ... flag "i"`, `starts with`, and the `::jsonpath` cast.
    #[tokio::test]
    async fn test_jsonb_path_exists_pg_dialect() -> Result<()> {
        let ctx = session().await?;

        async fn eval(ctx: &SessionContext, predicate: &str) -> Result<bool> {
            let batch = &ctx.sql(&format!("SELECT {predicate} AS r")).await?.collect().await?[0];
            Ok(batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>().unwrap().value(0))
        }

        for (predicate, want) in [
            // array membership
            (r#"jsonb_path_exists(json_to_variant('["pat:ed6bf5b6","other"]'), '$[*] ? (@ == "pat:ed6bf5b6")')"#, true),
            (r#"jsonb_path_exists(json_to_variant('["other"]'), '$[*] ? (@ == "pat:ed6bf5b6")')"#, false),
            // dot-quoted member
            (r#"jsonb_path_exists(json_to_variant('[{"error_type":"boom"}]'), '$[*]."error_type" ? (@ == "boom")')"#, true),
            // like_regex + flag "i"
            (r#"jsonb_path_exists(json_to_variant('{"msg":"ABCdef"}'), '$."msg" ? (@ like_regex "^abc.*" flag "i")')"#, true),
            // `starts with`
            (r#"jsonb_path_exists(json_to_variant('[{"path":"/api/x"}]'), '$[*]."path" ? (@ starts with "/api")')"#, true),
            // `::jsonpath` cast — SqlToRel rejects the unknown SQL type without the TypePlanner.
            (r#"jsonb_path_exists(json_to_variant('["pat:ed6bf5b6"]'), '$[*] ? (@ == "pat:ed6bf5b6")'::jsonpath)"#, true),
        ] {
            assert_eq!(eval(&ctx, predicate).await?, want, "expected {want} from: {predicate}");
        }
        // NULL input → SQL NULL, not false (the simple-path fast lane must honour the null buffer).
        let batch = &ctx.sql(r#"SELECT jsonb_path_exists(json_to_variant(NULL), '$.a') AS r"#).await?.collect().await?[0];
        assert!(batch.column(0).is_null(0), "NULL variant input must yield NULL, not false");

        Ok(())
    }

    #[tokio::test]
    async fn test_jsonb_path_query_first_returns_the_matched_value() -> Result<()> {
        let ctx = session().await?;

        async fn text(ctx: &SessionContext, expr: &str) -> Result<Option<String>> {
            let batch = &ctx.sql(&format!("SELECT {expr} AS r")).await?.collect().await?[0];
            let col = batch.column(0);
            Ok((!col.is_null(0)).then(|| get_str(col.as_ref(), 0).to_string()))
        }

        const EVENTS: &str = r#"[{"event_name":"exception","event_attributes":{"exception":{"type":"TypeError","message":"Cannot read cart"}}}]"#;
        let path = r#"'$[*] ? (@.event_name == "exception").event_attributes.exception.type'"#;

        // `#>> '{}'` is the whole document as TEXT, so the JSON string is unwrapped.
        assert_eq!(
            text(&ctx, &format!(r#"jsonb_path_query_first(json_to_variant('{EVENTS}'), {path}) #>> '{{}}'"#)).await?,
            Some("TypeError".to_string()),
            "the composed expression monoscope emits must yield unquoted text"
        );
        // Same over a plain JSON string column, not just Variant.
        assert_eq!(
            text(&ctx, &format!(r#"jsonb_path_query_first('{EVENTS}', {path}) #>> '{{}}'"#)).await?,
            Some("TypeError".to_string()),
            "JSON-string input must behave like Variant input"
        );
        // Without the `#>>`, the function itself returns jsonb — so, quoted.
        assert_eq!(
            text(&ctx, &format!(r#"jsonb_path_query_first(json_to_variant('{EVENTS}'), {path})"#)).await?,
            Some("\"TypeError\"".to_string()),
            "bare jsonb_path_query_first returns jsonb, which for a string leaf is quoted"
        );
        // No match, NULL input and non-exception events all mean NULL, never an error.
        for (label, expr) in [
            ("no exception event", format!(r#"jsonb_path_query_first(json_to_variant('[{{"event_name":"log"}}]'), {path})"#)),
            ("empty array", format!(r#"jsonb_path_query_first(json_to_variant('[]'), {path})"#)),
            ("NULL input", format!(r#"jsonb_path_query_first(json_to_variant(NULL), {path})"#)),
        ] {
            assert_eq!(text(&ctx, &expr).await?, None, "{label} must be NULL");
        }
        // First match wins.
        let two = r#"[{"event_name":"exception","event_attributes":{"exception":{"type":"First"}}},{"event_name":"exception","event_attributes":{"exception":{"type":"Second"}}}]"#;
        assert_eq!(
            text(&ctx, &format!(r#"jsonb_path_query_first(json_to_variant('{two}'), {path}) #>> '{{}}'"#)).await?,
            Some("First".to_string()),
            "with several exception events the FIRST must win"
        );

        // COALESCE: the flattened column wins, the span event is the fallback when it is NULL.
        assert_eq!(
            text(&ctx, &format!(r#"COALESCE(CAST(NULL AS VARCHAR), jsonb_path_query_first(json_to_variant('{EVENTS}'), {path}) #>> '{{}}')"#)).await?,
            Some("TypeError".to_string()),
            "the COALESCE fallback must reach the span event when the flattened column is NULL"
        );

        // Real dashboard-widget SQL; planning successfully IS the assertion.
        for sql in [
            r#"SELECT distinct_count(approx_count_distinct(attributes___session___id))::float AS dcount_attributes_session_id FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.348826Z' AND '2026-08-30T13:58:53.348826Z' and ((resource___telemetry___sdk___language = 'webjs' AND attributes___session___id IS NOT NULL AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = '')))"#,
            r#"SELECT distinct_count(approx_count_distinct(attributes___user___id))::float AS dcount_attributes_user_id FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.403066Z' AND '2026-08-30T13:58:53.403066Z' and ((resource___telemetry___sdk___language = 'webjs' AND attributes___user___id IS NOT NULL AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = '')))"#,
            r#"SELECT extract(epoch from time_bucket('10 seconds', timestamp))::integer, 'value', count(*)::float AS count_ FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.404809Z' AND '2026-08-30T13:58:53.404809Z' and ((resource___telemetry___sdk___language = 'webjs' AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = ''))) GROUP BY time_bucket('10 seconds', timestamp) ORDER BY time_bucket('10 seconds', timestamp) DESC"#,
        ] {
            ctx.sql(sql).await.map_err(|e| anyhow::anyhow!("production widget SQL must plan, got: {e}\n  sql: {sql}"))?;
        }

        Ok(())
    }
}
