#[cfg(test)]
mod test_json_functions {
    use anyhow::Result;
    use timefusion::{database::Database, test_utils::test_helpers::array_get_str as get_str};

    #[tokio::test]
    async fn test_json_build_array() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test json_build_array with literals
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

        // Test to_json with string
        let df = ctx.sql(r#"SELECT to_json('{"hello": "world"}') as result"#).await?;
        let results = df.collect().await?;
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let column = batch.column(0);
        assert_eq!(get_str(column.as_ref(), 0), r#"{"hello":"world"}"#);

        // Test to_json with number
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
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Test to_char
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

    #[tokio::test]
    async fn test_complex_query() -> Result<()> {
        // Initialize database
        let db = Database::new().await?;
        let db = std::sync::Arc::new(db);
        let mut ctx = db.clone().create_session_context();
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
        assert_eq!(get_str(column.as_ref(), 0), r#"["001","test_span",1500,{"status":"ok"}]"#);

        Ok(())
    }
}
