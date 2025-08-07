#[cfg(test)]
mod test_json_functions {
    use anyhow::Result;
    use timefusion::database::Database;

    #[tokio::test]
    async fn test_json_build_array() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let mut ctx = db.create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test json_build_array with literals
        let df = ctx.sql("SELECT json_build_array('a', 'b', 'c') as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        assert_eq!(value.value(0), r#"["a","b","c"]"#);

        Ok(())
    }

    #[tokio::test]
    async fn test_to_json() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let mut ctx = db.create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test to_json with string
        let df = ctx.sql(r#"SELECT to_json('{"hello": "world"}') as result"#).await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        assert_eq!(value.value(0), r#""{\"hello\": \"world\"}""#);

        // Test to_json with number
        let df = ctx.sql("SELECT to_json(123) as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        assert_eq!(value.value(0), "123");

        Ok(())
    }

    #[tokio::test]
    async fn test_extract_epoch() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let mut ctx = db.create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test extract_epoch
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
        let mut ctx = db.create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test to_char
        let df = ctx.sql("SELECT to_char(TIMESTAMP '2025-08-07T10:00:00Z', 'YYYY-MM-DD HH24:MI:SS') as result").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        assert_eq!(value.value(0), "2025-08-07 10:00:00");

        Ok(())
    }

    #[tokio::test]
    async fn test_complex_query() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let mut ctx = db.create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Create test table and insert data
        ctx.sql("CREATE TABLE test_table (id VARCHAR, name VARCHAR, duration BIGINT, summary VARCHAR)").await?.collect().await?;
        ctx.sql(r#"INSERT INTO test_table VALUES ('001', 'test_span', 1500, '{"status": "ok"}')"#).await?.collect().await?;

        // Test complex json_build_array query
        let df = ctx.sql("SELECT json_build_array(id, name, duration, to_json(summary)) as result FROM test_table").await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        let value = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();
        // to_json converts the string to a JSON string (with quotes and escaping)
        // The JSON string becomes a quoted string in the array
        assert_eq!(value.value(0), r#"["001","test_span",1500,"\"{\\\"status\\\": \\\"ok\\\"}\""]"#);

        Ok(())
    }
}
