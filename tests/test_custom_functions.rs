#[cfg(test)]
mod test_custom_functions {
    use anyhow::Result;
    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::*;
    use timefusion::functions::register_custom_functions;

    #[tokio::test]
    async fn test_to_char_function() -> Result<()> {
        // Create a new SessionContext
        let mut ctx = SessionContext::new();

        // Register our custom functions
        register_custom_functions(&mut ctx)?;

        // Create a test timestamp
        let timestamp = "2024-01-15 14:30:45";

        // Test various format patterns
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

            let array = batch.column(0).as_string::<i32>();
            let actual = array.value(0);

            assert_eq!(actual, expected, "Format '{}' failed", format);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_at_time_zone_function() -> Result<()> {
        // Create a new SessionContext
        let mut ctx = SessionContext::new();

        // Register our custom functions
        register_custom_functions(&mut ctx)?;

        // Test timezone conversion with a simpler query
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
        let array = batch2.column(0).as_string::<i32>();
        let actual = array.value(0);

        // UTC 14:30:45 -> America/New_York (UTC-5 in January) = 09:30:45
        assert_eq!(actual, "2024-01-15 09:30:45");

        Ok(())
    }

}
