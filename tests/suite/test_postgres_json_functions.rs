#[cfg(test)]
mod test_json_functions {
    use anyhow::Result;
    use timefusion::{database::Database, support::test_helpers::array_get_str as get_str};

    #[tokio::test]
    async fn test_json_build_array() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let df = ctx.sql("SELECT json_build_array('a', 'b', 'c') as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), r#"["a","b","c"]"#);

        Ok(())
    }

    #[tokio::test]
    async fn test_to_json() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let df = ctx.sql(r#"SELECT to_json('{"hello": "world"}') as result"#).await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), r#"{"hello":"world"}"#);

        let df = ctx.sql("SELECT to_json(123) as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), "123");

        Ok(())
    }

    #[tokio::test]
    async fn test_to_jsonb_alias() -> Result<()> {
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // to_jsonb is registered as an alias of to_json — Postgres syntax used by monoscope queries.
        let df = ctx.sql(r#"SELECT to_jsonb('{"hello": "world"}') as result"#).await?;
        let results = df.collect().await?;
        assert_eq!(get_str(results[0].column(0).as_ref(), 0), r#"{"hello":"world"}"#);

        let df = ctx.sql("SELECT to_jsonb(123) as result").await?;
        let results = df.collect().await?;
        assert_eq!(get_str(results[0].column(0).as_ref(), 0), "123");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_epoch() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let df = ctx.sql("SELECT extract_epoch(TIMESTAMP '2025-08-07T10:00:00Z') as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::Float64Array>().unwrap();
        // The timestamp is interpreted as UTC
        assert_eq!(value.value(0), 1754560800.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_to_char() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let df = ctx.sql("SELECT to_char(TIMESTAMP '2025-08-07T10:00:00Z', 'YYYY-MM-DD HH24:MI:SS') as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), "2025-08-07 10:00:00");

        Ok(())
    }

    // Regression: task-jsonpath-pg-compat. Monoscope (src/Pkg/Parser/Expr.hs)
    // emits Postgres SQL/JSON-path (`$[*] ? (@ == "x")`, dot-quoted members,
    // `like_regex ... flag "i"`, `starts with`), which is NOT RFC 9535. The old
    // serde_json_path engine couldn't parse it, so a log-pattern click returned
    // an empty result set. We now use the sql-json-path crate (PG-parity parser)
    // plus a TypePlanner that resolves the `::jsonpath` cast to Utf8.
    #[tokio::test]
    async fn test_jsonb_path_exists_pg_dialect() -> Result<()> {
        let db = std::sync::Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        async fn eval(ctx: &datafusion::prelude::SessionContext, predicate: &str) -> Result<bool> {
            let batch = &ctx.sql(&format!("SELECT {predicate} AS r")).await?.collect().await?[0];
            Ok(batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::BooleanArray>().unwrap().value(0))
        }

        // #1 array membership / log-pattern filter — the dominant case.
        assert!(
            eval(&ctx, r#"jsonb_path_exists(json_to_variant('["pat:ed6bf5b6","other"]'), '$[*] ? (@ == "pat:ed6bf5b6")')"#).await?,
            "present value must match"
        );
        assert!(!eval(&ctx, r#"jsonb_path_exists(json_to_variant('["other"]'), '$[*] ? (@ == "pat:ed6bf5b6")')"#).await?, "absent value must not match");
        // #2 nested-field equality with a dot-quoted member on a JSON array.
        assert!(
            eval(&ctx, r#"jsonb_path_exists(json_to_variant('[{"error_type":"boom"}]'), '$[*]."error_type" ? (@ == "boom")')"#).await?,
            "nested dot-quoted member equality must match"
        );
        // #3 like_regex + flag "i" — monoscope always appends `flag "i"`; RFC 9535 has no regex.
        assert!(
            eval(&ctx, r#"jsonb_path_exists(json_to_variant('{"msg":"ABCdef"}'), '$."msg" ? (@ like_regex "^abc.*" flag "i")')"#).await?,
            "case-insensitive like_regex must match"
        );
        // #4 `starts with` operator.
        assert!(
            eval(&ctx, r#"jsonb_path_exists(json_to_variant('[{"path":"/api/x"}]'), '$[*]."path" ? (@ starts with "/api")')"#).await?,
            "starts with must match"
        );
        // #5 the `::jsonpath` cast (TypePlanner → Utf8). This is what monoscope actually
        // sends over the wire; SqlToRel would otherwise reject the unknown SQL type.
        assert!(
            eval(&ctx, r#"jsonb_path_exists(json_to_variant('["pat:ed6bf5b6"]'), '$[*] ? (@ == "pat:ed6bf5b6")'::jsonpath)"#).await?,
            "::jsonpath cast must plan and match"
        );
        // #6 NULL input row → SQL NULL, not false. The simple-path fast lane must gate
        // on the input's null buffer (regression: code-review found it returned false).
        let batch = &ctx.sql(r#"SELECT jsonb_path_exists(json_to_variant(NULL), '$.a') AS r"#).await?.collect().await?[0];
        assert!(batch.column(0).is_null(0), "NULL variant input must yield NULL, not false");

        Ok(())
    }

    // Regression: the RUM dashboard widgets rendered error overlays with
    // "Error during planning: Invalid function 'jsonb_path_query_first'".
    // Monoscope's KQL compiler (shared/src/Pkg/Parser/Expr.hs,
    // `transformFlattenedAttribute`) emits this for EVERY `attributes.exception.*`
    // field, because an OTel SDK may carry the exception as a span event rather
    // than a flattened attribute, so it COALESCEs both sources. Anything
    // exception-related — log explorer, monitors, dashboards — failed to plan.
    #[tokio::test]
    async fn test_jsonb_path_query_first_returns_the_matched_value() -> Result<()> {
        let db = std::sync::Arc::new(Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        async fn text(ctx: &datafusion::prelude::SessionContext, expr: &str) -> Result<Option<String>> {
            let batch = &ctx.sql(&format!("SELECT {expr} AS r")).await?.collect().await?[0];
            let col = batch.column(0);
            Ok((!col.is_null(0)).then(|| get_str(col.as_ref(), 0).to_string()))
        }

        // The exact span-event shape monoscope reads, and the exact path it emits.
        const EVENTS: &str = r#"[{"event_name":"exception","event_attributes":{"exception":{"type":"TypeError","message":"Cannot read cart"}}}]"#;
        let path = r#"'$[*] ? (@.event_name == "exception").event_attributes.exception.type'"#;

        // `#>> '{}'` is the whole document as TEXT: the JSON string must be unwrapped.
        // `TypeError`, never `"TypeError"` — a widget showing the quotes is still broken.
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
        // No match, NULL input, and non-exception events all mean NULL, never an error:
        // a span without an exception is the common case, not a failure.
        for (label, expr) in [
            ("no exception event", format!(r#"jsonb_path_query_first(json_to_variant('[{{"event_name":"log"}}]'), {path})"#)),
            ("empty array", format!(r#"jsonb_path_query_first(json_to_variant('[]'), {path})"#)),
            ("NULL input", format!(r#"jsonb_path_query_first(json_to_variant(NULL), {path})"#)),
        ] {
            assert_eq!(text(&ctx, &expr).await?, None, "{label} must be NULL");
        }
        // First match wins, as the name promises.
        let two = r#"[{"event_name":"exception","event_attributes":{"exception":{"type":"First"}}},{"event_name":"exception","event_attributes":{"exception":{"type":"Second"}}}]"#;
        assert_eq!(
            text(&ctx, &format!(r#"jsonb_path_query_first(json_to_variant('{two}'), {path}) #>> '{{}}'"#)).await?,
            Some("First".to_string()),
            "with several exception events the FIRST must win"
        );

        // The full COALESCE monoscope actually emits: the flattened column wins when
        // present, and the span event is the fallback when it is NULL.
        assert_eq!(
            text(&ctx, &format!(r#"COALESCE(CAST(NULL AS VARCHAR), jsonb_path_query_first(json_to_variant('{EVENTS}'), {path}) #>> '{{}}')"#)).await?,
            Some("TypeError".to_string()),
            "the COALESCE fallback must reach the span event when the flattened column is NULL"
        );

        // The verbatim widget SQL from the production log, which is what actually
        // rendered the error overlay. Planning is the assertion: it died at
        // "Invalid function 'jsonb_path_query_first'" before reaching execution.
        for sql in [
            r#"SELECT distinct_count(approx_count_distinct(attributes___session___id))::float AS dcount_attributes_session_id FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.348826Z' AND '2026-08-30T13:58:53.348826Z' and ((resource___telemetry___sdk___language = 'webjs' AND attributes___session___id IS NOT NULL AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = '')))"#,
            r#"SELECT distinct_count(approx_count_distinct(attributes___user___id))::float AS dcount_attributes_user_id FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.403066Z' AND '2026-08-30T13:58:53.403066Z' and ((resource___telemetry___sdk___language = 'webjs' AND attributes___user___id IS NOT NULL AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = '')))"#,
            r#"SELECT extract(epoch from time_bucket('10 seconds', timestamp))::integer, 'value', count(*)::float AS count_ FROM otel_logs_and_spans WHERE project_id='00000000-0000-0000-0000-000000000000' and timestamp BETWEEN '2026-08-30T12:58:53.404809Z' AND '2026-08-30T13:58:53.404809Z' and ((resource___telemetry___sdk___language = 'webjs' AND (status_code = 'ERROR' OR COALESCE(attributes___exception___type, (jsonb_path_query_first(events, '$[*] ? (@.event_name == "exception").event_attributes.exception.type') #>> '{}')) IS NOT NULL) AND ('' = '' OR resource___service___name = ''))) GROUP BY time_bucket('10 seconds', timestamp) ORDER BY time_bucket('10 seconds', timestamp) DESC"#,
        ] {
            ctx.sql(sql).await.map_err(|e| anyhow::anyhow!("production widget SQL must plan, got: {e}\n  sql: {sql}"))?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_query() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        ctx.sql("CREATE TABLE test_table (id VARCHAR, name VARCHAR, duration BIGINT, summary VARCHAR)").await?.collect().await?;
        ctx.sql(r#"INSERT INTO test_table VALUES ('001', 'test_span', 1500, '{"status": "ok"}')"#).await?.collect().await?;

        let df = ctx.sql("SELECT json_build_array(id, name, duration, to_json(summary)) as result FROM test_table").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), r#"["001","test_span",1500,{"status":"ok"}]"#);

        Ok(())
    }
}
