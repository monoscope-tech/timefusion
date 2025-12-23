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

    // TODO: There's a DataFusion optimizer issue with timestamp timezone handling
    // that causes schema mismatches. This needs to be investigated further.
    #[tokio::test]
    #[ignore]
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

        // The at_time_zone function preserves the instant in time
        // We can verify it works by formatting the result
        let sql2 = "SELECT to_char(at_time_zone(TIMESTAMP '2024-01-15 14:30:45 UTC', 'America/New_York'), 'YYYY-MM-DD HH24:MI:SS') as formatted";

        let df2 = ctx.sql(sql2).await?;
        let results2 = df2.collect().await?;

        assert_eq!(results2.len(), 1);
        let batch2 = &results2[0];
        let array = batch2.column(0).as_string::<i32>();
        let actual = array.value(0);

        // The time should be the same since AT TIME ZONE preserves the instant
        assert_eq!(actual, "2024-01-15 14:30:45");

        Ok(())
    }

    #[tokio::test]
    #[ignore = "UPDATE/DELETE only work on Delta tables, not in-memory tables"]
    async fn test_update_delete_syntax() -> Result<()> {
        let ctx = SessionContext::new();

        // Create a simple test table
        ctx.sql("CREATE TABLE test_table (id INT, name VARCHAR, status VARCHAR)").await?;
        ctx.sql("INSERT INTO test_table VALUES (1, 'test1', 'active'), (2, 'test2', 'inactive')").await?;

        // Test UPDATE
        let update_result = ctx.sql("UPDATE test_table SET status = 'updated' WHERE id = 1").await?;
        let _ = update_result.collect().await?; // Execute the update

        let df = ctx.sql("SELECT status FROM test_table WHERE id = 1").await?;
        let results = df.collect().await?;
        assert!(!results.is_empty(), "Expected results from SELECT after UPDATE");
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(results[0].column(0).as_string::<i32>().value(0), "updated");

        // Test DELETE
        let delete_result = ctx.sql("DELETE FROM test_table WHERE id = 2").await?;
        let _ = delete_result.collect().await?; // Execute the delete

        let df = ctx.sql("SELECT COUNT(*) as cnt FROM test_table").await?;
        let results = df.collect().await?;
        assert!(!results.is_empty(), "Expected results from COUNT after DELETE");
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(results[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0), 1);

        Ok(())
    }
}
