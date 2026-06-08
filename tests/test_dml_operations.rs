#[cfg(test)]
mod test_dml_operations {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::Result;
    use datafusion::{
        arrow,
        arrow::array::{Array, AsArray, StringArray, StringViewArray},
    };
    use serial_test::serial;
    use timefusion::{config::AppConfig, database::Database};
    use tracing::info;

    /// Helper function to get string value from either Utf8View or Utf8 array
    fn get_str(arr: &dyn Array, idx: usize) -> String {
        if let Some(sv) = arr.as_any().downcast_ref::<StringViewArray>() {
            sv.value(idx).to_string()
        } else if let Some(s) = arr.as_any().downcast_ref::<StringArray>() {
            s.value(idx).to_string()
        } else {
            panic!("Expected string array but got {:?}", arr.data_type());
        }
    }

    fn create_test_config(test_id: &str) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
        cfg.core.timefusion_table_prefix = format!("test-{}", test_id);
        cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-dml-{}", test_id));
        cfg.cache.timefusion_foyer_disabled = true;
        Arc::new(cfg)
    }

    // ==========================================================================
    // Delta-Only DML Tests (no buffered layer - operations go directly to Delta)
    // These tests verify that UPDATE/DELETE work correctly on Delta Lake tables.
    // ==========================================================================

    fn create_test_records(now: chrono::DateTime<chrono::Utc>) -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({
                "id": "1",
                "name": "Alice",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 100,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "2",
                "name": "Bob",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 200,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "3",
                "name": "Charlie",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 300,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
        ]
    }

    // UPDATE Tests

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_query() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Test UPDATE with WHERE clause
        info!("Executing UPDATE query");
        let df = ctx.sql("UPDATE otel_logs_and_spans SET duration = 500 WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let result = df.collect().await?;

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);

        let rows_updated = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify the update
        let df = ctx.sql("SELECT id, name, duration FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3);

        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();
        let duration_col_idx = batch.schema().fields().iter().position(|f| f.name() == "duration").unwrap();

        let name_col = batch.column(name_col_idx).as_ref();
        let duration_col = batch.column(duration_col_idx).as_primitive::<arrow::datatypes::Int64Type>();

        for i in 0..batch.num_rows() {
            match get_str(name_col, i).as_str() {
                "Bob" => assert_eq!(duration_col.value(i), 500, "Bob's duration should be updated to 500"),
                "Alice" => assert_eq!(duration_col.value(i), 100, "Alice's duration should remain 100"),
                "Charlie" => assert_eq!(duration_col.value(i), 300, "Charlie's duration should remain 300"),
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    // DELETE Tests

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_with_predicate() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Test DELETE with WHERE clause
        info!("Executing DELETE query");
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 1);

        let rows_deleted = batch.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 1, "Expected 1 row to be deleted");

        // Verify the delete
        let df = ctx.sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'test_project' ORDER BY id").await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 2); // Only Alice and Charlie should remain

        let id_col_idx = batch.schema().fields().iter().position(|f| f.name() == "id").unwrap();
        let name_col_idx = batch.schema().fields().iter().position(|f| f.name() == "name").unwrap();

        let id_col = batch.column(id_col_idx).as_ref();
        let name_col = batch.column(name_col_idx).as_ref();

        assert_eq!(get_str(id_col, 0), "1");
        assert_eq!(get_str(name_col, 0), "Alice");
        assert_eq!(get_str(id_col, 1), "3");
        assert_eq!(get_str(name_col, 1), "Charlie");

        Ok(())
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete_all_matching() -> Result<()> {
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = vec![
            serde_json::json!({
                "id": "1",
                "name": "Record1",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 100,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "2",
                "name": "Record2",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "INFO",
                "status_code": "OK",
                "duration": 200,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "3",
                "name": "Record3",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 300,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
            serde_json::json!({
                "id": "4",
                "name": "Record4",
                "project_id": "test_project",
                "timestamp": now.timestamp_micros(),
                "level": "ERROR",
                "status_code": "ERROR",
                "duration": 400,
                "date": now.date_naive().to_string(),
                "hashes": [],
                "summary": []
            }),
        ];

        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Delete all ERROR level records
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;

        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 3, "Expected 3 rows to be deleted");

        // Verify only the INFO record remains
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(count, 1, "Expected 1 row to remain");

        // Verify it's the right record
        let df = ctx.sql("SELECT id, level FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let batch = &results[0];

        let id_col = batch.column(0).as_ref();
        let level_col = batch.column(1).as_ref();

        assert_eq!(get_str(id_col, 0), "2");
        assert_eq!(get_str(level_col, 0), "INFO");

        Ok(())
    }

    // ==========================================================================
    // Delta UPDATE with multiple columns test
    // ==========================================================================

    #[serial]
    #[tokio::test]
    async fn test_update_multiple_columns() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;

        // Insert directly to Delta (skip_queue=true)
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Update multiple columns at once
        info!("Executing multi-column UPDATE query");
        let df = ctx
            .sql("UPDATE otel_logs_and_spans SET duration = 999, level = 'WARN' WHERE project_id = 'test_project' AND name = 'Alice'")
            .await?;
        let result = df.collect().await?;

        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify both columns were updated
        let df = ctx
            .sql("SELECT name, duration, level FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Alice'")
            .await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let duration_idx = batch.schema().fields().iter().position(|f| f.name() == "duration").unwrap();
        let level_idx = batch.schema().fields().iter().position(|f| f.name() == "level").unwrap();

        let duration_col = batch.column(duration_idx).as_primitive::<arrow::datatypes::Int64Type>();
        let level_col = batch.column(level_idx).as_ref();

        assert_eq!(duration_col.value(0), 999, "Duration should be updated to 999");
        assert_eq!(get_str(level_col, 0), "WARN", "Level should be updated to WARN");

        Ok(())
    }

    // ==========================================================================
    // Delta DELETE then verify row counts test
    // ==========================================================================

    #[serial]
    #[tokio::test]
    async fn test_delete_verify_counts() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();

        // Create 5 records
        let records = vec![
            serde_json::json!({
                "id": "1", "name": "R1", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 100, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "2", "name": "R2", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 200, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "3", "name": "R3", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "ERROR", "status_code": "ERROR",
                "duration": 300, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "4", "name": "R4", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
                "duration": 400, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
            serde_json::json!({
                "id": "5", "name": "R5", "project_id": "test_project",
                "timestamp": now.timestamp_micros(), "level": "ERROR", "status_code": "ERROR",
                "duration": 500, "date": now.date_naive().to_string(), "hashes": [], "summary": []
            }),
        ];

        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Verify initial count
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let initial_count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(initial_count, 5, "Should have 5 rows initially");

        // Delete ERROR records
        let df = ctx.sql("DELETE FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'").await?;
        let result = df.collect().await?;
        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_deleted, 2, "Should delete 2 ERROR records");

        // Verify final count
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let final_count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(final_count, 3, "Should have 3 rows after delete");

        Ok(())
    }

    // Regression: DataFusion's CommonSubexprEliminate optimizer wraps the UPDATE
    // assignment Projection in an inner Projection that defines synthetic
    // `__common_expr_*` columns. extract_dml_info used to overwrite the real
    // assignments with that inner Projection's contents, so mem_buffer failed
    // with "Column '__common_expr_1' not found".
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_with_common_subexpression() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // `duration + 100` appears twice in SET — CSE-eligible subexpr that
        // the optimizer hoists into a `__common_expr_*` alias.
        info!("Executing UPDATE with CSE-eligible subexpression");
        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans \
                 SET duration = duration + 100, \
                     status_message = CAST(duration + 100 AS VARCHAR) \
                 WHERE project_id = 'test_project' AND name = 'Bob'",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected Bob's row to be updated");

        let df = ctx.sql("SELECT duration FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let results = df.collect().await?;
        let duration = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(duration, 300, "Bob's duration should be 200 + 100 = 300");

        Ok(())
    }

    // ==========================================================================
    // UPDATE ... FROM Tests
    // ==========================================================================

    /// Helper: select `duration` for `name` in `test_project`, ordered by name.
    async fn duration_by_name(ctx: &datafusion::prelude::SessionContext, name: &str) -> Result<i64> {
        let q = format!("SELECT duration FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = '{}'", name);
        let df = ctx.sql(&q).await?;
        let results = df.collect().await?;
        assert!(!results.is_empty() && results[0].num_rows() == 1, "duration_by_name: expected 1 row for {}", name);
        Ok(results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0))
    }

    /// VALUES-list source: smallest possible `UPDATE ... FROM` shape.
    /// Sets Bob.duration = 500 and Alice.duration = 999 via a single statement.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_values() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d
                   FROM (VALUES ('Bob', 500), ('Alice', 999)) AS u(name, d)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 2, "Expected 2 rows updated (Bob, Alice)");

        assert_eq!(duration_by_name(&ctx, "Bob").await?, 500);
        assert_eq!(duration_by_name(&ctx, "Alice").await?, 999);
        assert_eq!(duration_by_name(&ctx, "Charlie").await?, 300, "Charlie unchanged");
        Ok(())
    }

    /// Source row whose key doesn't match any target row must not affect any target row.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_no_match_no_change() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d
                   FROM (VALUES ('Nobody', 42)) AS u(name, d)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 0, "Expected 0 rows updated");

        // All three original durations intact.
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 200);
        assert_eq!(duration_by_name(&ctx, "Alice").await?, 100);
        assert_eq!(duration_by_name(&ctx, "Charlie").await?, 300);
        Ok(())
    }

    /// Extra `WHERE` predicate AND-ed with the join keys must narrow the update.
    /// Source matches Bob and Alice; predicate further constrains to Bob only.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_with_predicate() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d
                   FROM (VALUES ('Bob', 777), ('Alice', 888)) AS u(name, d)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name
                     AND otel_logs_and_spans.name = 'Bob'",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1, "Predicate should narrow to Bob only");

        assert_eq!(duration_by_name(&ctx, "Bob").await?, 777);
        assert_eq!(duration_by_name(&ctx, "Alice").await?, 100, "Alice excluded by predicate");
        assert_eq!(duration_by_name(&ctx, "Charlie").await?, 300);
        Ok(())
    }

    /// Structural mirror of monoscope's UPDATE-2 SQL: parallel unnested text
    /// arrays as the source rowset, table aliases (`o`, `u`), array-append
    /// into a list column. If DataFusion's planner or our rewriters fall over
    /// on this shape (unnest inside FROM subquery, user aliases on both
    /// sides, list-typed assignment target) this catches it pre-prod.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_unnest_text_array() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Monoscope UPDATE-2 lifted shape: parallel unnest of text[] arrays
        // bound as `u(span_name, tag)`, append into `hashes`. Join is on
        // `name` here (single key); monoscope uses two: span_id + trace_id —
        // structurally identical.
        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans o
                    SET hashes = COALESCE(o.hashes, '{}'::text[]) || ARRAY[u.tag]
                    FROM (
                      SELECT unnest(ARRAY['Bob', 'Alice']::text[])      AS span_name,
                             unnest(ARRAY['pat:bob', 'pat:alice']::text[]) AS tag
                    ) u
                    WHERE o.project_id = 'test_project'
                      AND o.name = u.span_name",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 2, "Expected 2 rows tagged (Bob, Alice)");
        Ok(())
    }

    /// Idempotency aspect of the monoscope UPDATE-2 shape: re-running the
    /// same statement with the `NOT @>` predicate must touch zero rows.
    /// Currently `#[ignore]`d — `MergeBuilder` returns the rows that the
    /// join matched even when the SET expression produces an unchanged value
    /// after the WHEN MATCHED predicate trims them. Untangling needs deeper
    /// investigation of MergeBuilder's accounting of WHEN MATCHED with
    /// non-trivial predicate filtering. Monoscope's re-extraction safety on
    /// TF will rely on this — track as a follow-up.
    #[ignore = "MergeBuilder idempotency under NOT @> predicate — see comment"]
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_unnest_text_array_idempotent() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let sql = "UPDATE otel_logs_and_spans o
                    SET hashes = COALESCE(o.hashes, '{}'::text[]) || ARRAY[u.tag]
                    FROM (
                      SELECT unnest(ARRAY['Bob', 'Alice']::text[])      AS span_name,
                             unnest(ARRAY['pat:bob', 'pat:alice']::text[]) AS tag
                    ) u
                    WHERE o.project_id = 'test_project'
                      AND o.name = u.span_name
                      AND NOT (COALESCE(o.hashes, '{}'::text[]) @> ARRAY[u.tag])";
        let _ = ctx.sql(sql).await?.collect().await?;
        let r2 = ctx.sql(sql).await?.collect().await?;
        let n = r2[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(n, 0, "Re-running idempotent UPDATE must touch zero rows");
        Ok(())
    }

    /// Multi-column SET in a single `UPDATE ... FROM` — mirrors the monoscope
    /// pattern of assigning several fields from a joined source row.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_multi_column_set() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d, level = u.lvl
                   FROM (VALUES ('Bob', 1234, 'WARN')) AS u(name, d, lvl)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name",
            )
            .await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(rows_updated, 1);

        let df = ctx
            .sql("SELECT duration, level FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Bob'")
            .await?;
        let results = df.collect().await?;
        let b = &results[0];
        assert_eq!(b.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 1234);
        assert_eq!(get_str(b.column(1).as_ref(), 0), "WARN");
        Ok(())
    }
}
