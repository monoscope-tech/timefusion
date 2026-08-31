#[cfg(test)]
mod tests {
    use anyhow::Result;
    use datafusion::prelude::*;
    use timefusion::{read::functions::register_custom_functions, support::test_helpers::array_get_str as get_str};

    #[tokio::test]
    async fn test_to_char_function() -> Result<()> {
        let mut ctx = SessionContext::new();

        // Register our custom functions
        register_custom_functions(&mut ctx)?;

        let timestamp = "2024-01-15 14:30:45";

        let test_cases = vec![
            ("YYYY-MM-DD", "2024-01-15"),
            ("YYYY-MM-DD HH24:MI:SS", "2024-01-15 14:30:45"),
            ("Month DD, YYYY", "January 15, 2024"),
            ("Mon DD, YYYY", "Jan 15, 2024"),
        ];

        for (format, expected) in test_cases {
            let sql = format!("SELECT to_char(TIMESTAMP '{}', '{}') as formatted", timestamp, format);

            let df = ctx.sql(&sql).await?;
            let results = df.collect().await?;

            assert_eq!(results.len(), 1);
            let batch = &results[0];
            assert_eq!(batch.num_rows(), 1);

            let actual = get_str(batch.column(0).as_ref(), 0);

            assert_eq!(actual, expected, "Format '{}' failed", format);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_at_time_zone_function() -> Result<()> {
        let mut ctx = SessionContext::new();

        // Register our custom functions
        register_custom_functions(&mut ctx)?;

        let sql = "SELECT at_time_zone(TIMESTAMP '2024-01-15 14:30:45 UTC', 'America/New_York') as ny_time";

        let df = ctx.sql(sql).await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        // The at_time_zone function converts to the target timezone
        let sql2 = "SELECT to_char(at_time_zone(TIMESTAMP '2024-01-15 14:30:45 UTC', 'America/New_York'), 'YYYY-MM-DD HH24:MI:SS') as formatted";

        let df2 = ctx.sql(sql2).await?;
        let results2 = df2.collect().await?;

        assert_eq!(results2.len(), 1);
        let batch2 = &results2[0];
        let actual = get_str(batch2.column(0).as_ref(), 0);

        // UTC 14:30:45 -> America/New_York (UTC-5 in January) = 09:30:45
        assert_eq!(actual, "2024-01-15 09:30:45");

        Ok(())
    }

    /// `SUBSTRING(x FROM 'regex')` is Postgres regex extraction, but sqlparser
    /// lowers it to the same 2-arg `substr` as the offset form, so DataFusion
    /// rejected it with "Function 'substr' requires Int64, but received String"
    /// and the whole statement failed to plan.
    ///
    /// Pins PG's two result rules (whole match vs first capture group), the
    /// NULL-on-no-match case, and that the offset forms still route to `substr`.
    ///
    /// Built through `create_session_context`, not a bare `SessionContext`: the
    /// fix is partly one of expr-planner ORDER, and only the real session builds
    /// that order.
    #[tokio::test]
    async fn substring_from_regex_matches_postgres_semantics() -> Result<()> {
        let db = std::sync::Arc::new(timefusion::database::Database::new().await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // (sql, expected) — None expects a NULL result.
        let cases: Vec<(&str, Option<&str>)> = vec![
            // No capturing group: the whole match. The pattern an operator ran
            // against widget access logs on 2026-08-31.
            (r#"SELECT SUBSTRING('GET /widget.png?w=3 HTTP/1.1' FROM 'widget.png[^"]{0,20}')"#, Some("widget.png?w=3 HTTP/1.1")),
            // One capturing group: that group, NOT the whole match.
            (r#"SELECT SUBSTRING('"GET / HTTP/1.1" 404 12' FROM 'HTTP/[0-9.]+" ([0-9]{3})')"#, Some("404")),
            // No match is NULL, not the empty string.
            (r#"SELECT SUBSTRING('nothing here' FROM 'HTTP/[0-9.]+')"#, None),
            // Offsets are untouched: both spellings still mean `substr`.
            ("SELECT SUBSTRING('abcdef' FROM 3)", Some("cdef")),
            ("SELECT SUBSTRING('abcdef' FROM 2 FOR 3)", Some("bcd")),
        ];

        for (sql, expected) in cases {
            let results = ctx.sql(sql).await?.collect().await?;
            let column = results[0].column(0);
            match expected {
                Some(want) => assert_eq!(get_str(column.as_ref(), 0), want, "{sql}"),
                None => assert!(column.is_null(0), "{sql} should be NULL, got {:?}", get_str(column.as_ref(), 0)),
            }
        }

        Ok(())
    }
}
