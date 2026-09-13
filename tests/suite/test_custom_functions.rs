#[cfg(test)]
mod tests {
    use anyhow::Result;
    use datafusion::prelude::*;
    use test_case::test_case;
    use timefusion::{read::functions::register_custom_functions, support::test_helpers::array_get_str as get_str};

    /// Runs `SELECT {expr}` with the custom functions registered, asserting a
    /// single row, and returns that cell as text.
    async fn eval(expr: &str) -> String {
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx).unwrap();
        let results = ctx.sql(&format!("SELECT {expr}")).await.unwrap().collect().await.unwrap();
        assert_eq!(results.len(), 1, "{expr}");
        assert_eq!(results[0].num_rows(), 1, "{expr}");
        get_str(results[0].column(0).as_ref(), 0)
    }

    #[test_case("to_char(TIMESTAMP '2024-01-15 14:30:45', 'YYYY-MM-DD')" => "2024-01-15" ; "to_char date only")]
    #[test_case("to_char(TIMESTAMP '2024-01-15 14:30:45', 'YYYY-MM-DD HH24:MI:SS')" => "2024-01-15 14:30:45" ; "to_char date and time")]
    #[test_case("to_char(TIMESTAMP '2024-01-15 14:30:45', 'Month DD, YYYY')" => "January 15, 2024" ; "to_char full month name")]
    #[test_case("to_char(TIMESTAMP '2024-01-15 14:30:45', 'Mon DD, YYYY')" => "Jan 15, 2024" ; "to_char abbreviated month name")]
    // UTC 14:30:45 -> America/New_York (UTC-5 in January) = 09:30:45.
    #[test_case("to_char(at_time_zone(TIMESTAMP '2024-01-15 14:30:45 UTC', 'America/New_York'), 'YYYY-MM-DD HH24:MI:SS')" => "2024-01-15 09:30:45" ; "at_time_zone converts UTC to New York")]
    #[test_case("CASE WHEN at_time_zone(TIMESTAMP '2024-01-15 14:30:45 UTC', 'America/New_York') IS NOT NULL THEN 'one non-null row' END" => "one non-null row" ; "bare at_time_zone yields one non-null row")]
    #[tokio::test]
    async fn to_char_and_at_time_zone(expr: &'static str) -> String {
        eval(expr).await
    }

    /// Pins Postgres `SUBSTRING(x FROM 'regex')` semantics: whole match vs first
    /// capture group, NULL on no match, and that the offset forms still mean
    /// `substr`. Must use `create_session_context` — expr-planner ORDER matters,
    /// and only the real session builds it.
    ///
    /// Known gap, not asserted: over pgwire the all-literal form still fails,
    /// because the plan cache parameterizes arg 2 into an Int64 placeholder.
    #[tokio::test]
    async fn substring_from_regex_matches_postgres_semantics() -> Result<()> {
        let db = std::sync::Arc::new(timefusion::database::Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // (sql, expected) — None expects a NULL result.
        for (sql, expected) in [
            // No capturing group: the whole match.
            (r#"SELECT SUBSTRING('GET /widget.png?w=3 HTTP/1.1' FROM 'widget.png[^"]{0,20}')"#, Some("widget.png?w=3 HTTP/1.1")),
            // One capturing group: that group, NOT the whole match.
            (r#"SELECT SUBSTRING('"GET / HTTP/1.1" 404 12' FROM 'HTTP/[0-9.]+" ([0-9]{3})')"#, Some("404")),
            // No match is NULL, not the empty string.
            (r#"SELECT SUBSTRING('nothing here' FROM 'HTTP/[0-9.]+')"#, None),
            // Offsets are untouched: both spellings still mean `substr`.
            ("SELECT SUBSTRING('abcdef' FROM 3)", Some("cdef")),
            ("SELECT SUBSTRING('abcdef' FROM 2 FOR 3)", Some("bcd")),
        ] {
            let results = ctx.sql(sql).await?.collect().await?;
            let column = results[0].column(0);
            match expected {
                Some(want) => assert_eq!(get_str(column.as_ref(), 0), want, "{sql}"),
                None => assert!(column.is_null(0), "{sql} should be NULL, got {:?}", get_str(column.as_ref(), 0)),
            }
        }

        Ok(())
    }

    /// Drives a regex-over-Variant query the way the plan cache does
    /// (parse → statement_to_plan → optimize), which is a different path from
    /// the scalar results test above.
    #[tokio::test]
    async fn plan_and_optimize_matches_the_plan_cache_path() -> Result<()> {
        use datafusion::sql::parser::DFParser;

        let db = std::sync::Arc::new(timefusion::database::Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;
        let state = ctx.state();

        let sql = r#"SELECT SUBSTRING(variant_to_json(body)::TEXT FROM 'widget.png[^"]{0,20}') AS u,
                            SUBSTRING(variant_to_json(body)::TEXT FROM 'HTTP/[0-9.]+" ([0-9]{3})') AS status,
                            count(*)
                     FROM otel_logs_and_spans
                     WHERE project_id = 'p' AND variant_to_json(body)::TEXT LIKE '%widget.png%'
                     GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20"#;

        let stmt = DFParser::parse_sql(sql)?.pop_front().expect("one statement");
        let plan = state.statement_to_plan(stmt).await?;
        state.optimize(&plan)?;
        Ok(())
    }

    /// The bucket width may be an INTERVAL (Timescale spelling) or a string (KQL
    /// emits this); both spellings must agree.
    #[test_case("INTERVAL '5 minutes'" => "2026-08-31 14:35:00" ; "interval 5 minutes")]
    #[test_case("'5 minutes'" => "2026-08-31 14:35:00" ; "string 5 minutes lands on the same bucket")]
    #[test_case("INTERVAL '1 hour'" => "2026-08-31 14:00:00" ; "interval 1 hour")]
    #[test_case("INTERVAL '1 day'" => "2026-08-31 00:00:00" ; "interval 1 day")]
    #[tokio::test]
    async fn time_bucket_accepts_an_interval(width: &'static str) -> String {
        eval(&format!("to_char(time_bucket({width}, TIMESTAMPTZ '2026-08-31 14:37:45+00'), 'YYYY-MM-DD HH24:MI:SS')")).await
    }

    /// A month is 28-31 days: refuse it instead of bucketing by a wrong width.
    #[tokio::test]
    async fn time_bucket_refuses_month_widths() -> Result<()> {
        let mut ctx = SessionContext::new();
        register_custom_functions(&mut ctx)?;

        let err = match ctx.sql("SELECT time_bucket(INTERVAL '1 month', TIMESTAMPTZ '2026-08-31 14:37:45+00')").await {
            Ok(df) => df.collect().await.err().map(|e| e.to_string()).unwrap_or_default(),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("month"), "month intervals must be refused with a clear message, got: {err}");

        Ok(())
    }
}
