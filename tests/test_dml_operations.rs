#[cfg(test)]
mod test_dml_operations {
    use std::sync::Arc;

    use anyhow::Result;
    use datafusion::{arrow, arrow::array::AsArray};
    use serial_test::serial;
    use timefusion::{
        config::AppConfig,
        database::Database,
        test_utils::test_helpers::{array_get_str as get_str, minio_test_config},
    };
    use tracing::info;

    fn create_test_config(test_id: &str) -> Arc<AppConfig> {
        minio_test_config(test_id, &format!("/tmp/timefusion-dml-{test_id}"))
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

        let rows_updated = batch.column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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

        let rows_deleted = batch.column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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

        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let df = ctx.sql("UPDATE otel_logs_and_spans SET duration = 999, level = 'WARN' WHERE project_id = 'test_project' AND name = 'Alice'").await?;
        let result = df.collect().await?;

        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(rows_updated, 1, "Expected 1 row to be updated");

        // Verify both columns were updated
        let df = ctx.sql("SELECT name, duration, level FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Alice'").await?;
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
        let rows_deleted = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
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
        let n = r2[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(n, 0, "Re-running idempotent UPDATE must touch zero rows");
        Ok(())
    }

    /// Regression: main.rs creates the pgwire SessionContext (and its
    /// DmlQueryPlanner) BEFORE attaching the BufferedWriteLayer (the WAL-replay
    /// registry needs the context first). The planner used to capture a
    /// pre-layer clone of Database, so every pgwire UPDATE/DELETE silently
    /// skipped the mem-buffer leg: updates to rows still in the buffer matched
    /// zero rows and were lost when the row later flushed with pre-update
    /// values. The layer must be late-binding — visible to sessions created
    /// before it was attached.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_mem_leg_survives_late_layer_attach() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        // SAFETY: walrus-rust reads WALRUS_DATA_DIR from environment; #[serial]
        // prevents concurrent access to this process-global.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
        let layer = Arc::new(timefusion::test_utils::test_helpers::test_layer(Arc::clone(&cfg))?);

        let db0 = Database::with_config(cfg).await?;
        // Session context created BEFORE the layer is attached — main.rs order.
        let mut ctx = Arc::new(db0.clone()).create_session_context();
        let db = Arc::new(db0.with_buffered_layer(Arc::clone(&layer)));
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let records = create_test_records(now);
        let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
        // skip_queue=false → rows land in the buffer, not Delta.
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], false, None).await?;

        let df = ctx.sql("UPDATE otel_logs_and_spans SET duration = 500 WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let result = df.collect().await?;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(rows_updated, 1, "UPDATE must reach buffer rows through a session created before the layer was attached");
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 500);
        Ok(())
    }

    /// Regression: the UPDATE's Delta leg must not hold the table's write lock
    /// across the multi-second merge (update_state → scan → parquet rewrite →
    /// commit). It used to, which convoyed every reader (SELECT needs the read
    /// lock) and every insert (commit swap needs the write lock) behind each
    /// UPDATE — the mechanical cause of prod flush starvation at ~2k UPDATEs/h.
    /// A SELECT and an insert issued mid-UPDATE must complete while the UPDATE
    /// is still running.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_does_not_block_readers_or_writers() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_id);
        let db = Arc::new(Database::with_config(cfg).await?);
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // Many separate commits → many files; the merge below matches one row
        // in every file, so it must rewrite all of them — slow enough to
        // observe concurrent operations overlapping it.
        let now = chrono::Utc::now();
        const FILES: usize = 24;
        const ROWS_PER_FILE: usize = 400;
        for f in 0..FILES {
            let records: Vec<serde_json::Value> = (0..ROWS_PER_FILE)
                .map(|r| {
                    serde_json::json!({
                        "id": format!("f{f}_r{r}"),
                        "name": if r == 0 { format!("T{f}") } else { format!("f{f}_r{r}") },
                        "project_id": "test_project",
                        "timestamp": now.timestamp_micros(),
                        "level": "INFO", "status_code": "OK", "duration": 100,
                        "date": now.date_naive().to_string(), "hashes": [], "summary": []
                    })
                })
                .collect();
            let batch = timefusion::test_utils::test_helpers::json_to_batch(records)?;
            db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;
        }

        let values = (0..FILES).map(|f| format!("('T{f}', {f})")).collect::<Vec<_>>().join(", ");
        let sql = format!(
            "UPDATE otel_logs_and_spans SET duration = u.d \
             FROM (VALUES {values}) AS u(name, d) \
             WHERE project_id = 'test_project' AND otel_logs_and_spans.name = u.name"
        );
        let update_ctx = ctx.clone();
        let update_handle = tokio::spawn(async move { update_ctx.sql(&sql).await?.collect().await.map_err(anyhow::Error::from) });

        // Give the UPDATE time to get into its Delta merge.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        assert!(!update_handle.is_finished(), "UPDATE finished too fast to observe concurrency — grow FILES/ROWS_PER_FILE");

        // Reader mid-UPDATE.
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(count as usize, FILES * ROWS_PER_FILE);
        assert!(!update_handle.is_finished(), "SELECT should complete while the UPDATE is still merging — reader was convoyed behind the DML write lock");

        // Writer mid-UPDATE (direct Delta insert commits + swaps the handle).
        let extra = timefusion::test_utils::test_helpers::json_to_batch(vec![serde_json::json!({
            "id": "extra", "name": "Extra", "project_id": "test_project",
            "timestamp": now.timestamp_micros(), "level": "INFO", "status_code": "OK",
            "duration": 1, "date": now.date_naive().to_string(), "hashes": [], "summary": []
        })])?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![extra], true, None).await?;
        assert!(!update_handle.is_finished(), "insert should complete while the UPDATE is still merging — writer was convoyed behind the DML write lock");

        let result = update_handle.await??;
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(rows_updated as usize, FILES);

        // The mid-UPDATE insert must not just be un-blocked — its row must
        // survive the UPDATE's snapshot swap.
        let df = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test_project'").await?;
        let results = df.collect().await?;
        let final_count = results[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(final_count as usize, FILES * ROWS_PER_FILE + 1, "concurrent insert's row lost across the DML swap");
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
        let rows_updated = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(rows_updated, 1);

        let df = ctx.sql("SELECT duration, level FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        let results = df.collect().await?;
        let b = &results[0];
        assert_eq!(b.column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 1234);
        assert_eq!(get_str(b.column(1).as_ref(), 0), "WARN");
        Ok(())
    }

    // ==========================================================================
    // DML coalescer + flush-watermark tests
    // ==========================================================================

    /// With `TIMEFUSION_DML_COALESCE_SECS > 0`, the Delta leg of
    /// `UPDATE ... FROM` defers: statements return without touching Delta,
    /// same-shape statements coalesce into one drained merge, and same-key
    /// statements apply in arrival order (split into merge rounds).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_coalesced_defers_and_drains() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let mut cfg = (*create_test_config(&test_id)).clone();
        cfg.buffer.timefusion_dml_coalesce_secs = 3600; // timer never fires; drains are explicit
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        db.start_dml_coalescer();
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let batch = timefusion::test_utils::test_helpers::json_to_batch(create_test_records(now))?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        let update = |val: i64, name: &str| {
            format!(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d
                   FROM (VALUES ('{name}', {val})) AS u(name, d)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name"
            )
        };
        for sql in [update(500, "Bob"), update(999, "Alice")] {
            let result = ctx.sql(&sql).await?.collect().await?;
            let rows = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
            assert_eq!(rows, 0, "deferred statement reports 0 rows (no mem leg here)");
        }
        // Nothing merged yet: Delta untouched until a drain.
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 200);
        assert_eq!(duration_by_name(&ctx, "Alice").await?, 100);

        db.dml_coalescer().expect("coalescer enabled").drain(&db).await;
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 500);
        assert_eq!(duration_by_name(&ctx, "Alice").await?, 999);
        assert_eq!(duration_by_name(&ctx, "Charlie").await?, 300, "unmatched row untouched");

        // Same key updated twice before one drain: rounds apply in arrival
        // order, so the later statement wins.
        ctx.sql(&update(777, "Bob")).await?.collect().await?;
        ctx.sql(&update(888, "Bob")).await?.collect().await?;
        db.dml_coalescer().expect("coalescer enabled").drain(&db).await;
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 888);
        Ok(())
    }

    /// Decisive check for the prod 2026-07-09 MERGE cardinality failures: a SINGLE
    /// `UPDATE ... FROM` whose source carries TWO rows for one join key (the
    /// monoscope enrichment shape — a span with multiple hash tags unnests to
    /// repeated (span_id, trace_id)). If the coalescer's `split_rounds` handles it,
    /// the rows apply across rounds (last wins) and nothing is dropped; if not, the
    /// MERGE aborts with the cardinality error and the update is lost.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalescer_splits_duplicate_source_keys_single_statement() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let mut cfg = (*create_test_config(&test_id)).clone();
        cfg.buffer.timefusion_dml_coalesce_secs = 3600; // explicit drains only
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        db.start_dml_coalescer();
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let batch = timefusion::test_utils::test_helpers::json_to_batch(create_test_records(now))?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // One statement, source has TWO rows for name='Bob' → duplicate join keys.
        let sql = "UPDATE otel_logs_and_spans
                     SET duration = u.d
                     FROM (VALUES ('Bob', 500), ('Bob', 999)) AS u(name, d)
                     WHERE project_id = 'test_project'
                       AND otel_logs_and_spans.name = u.name";
        ctx.sql(sql).await?.collect().await?;
        db.dml_coalescer().expect("coalescer enabled").drain(&db).await;

        // split_rounds must apply the dup-key rows across rounds (last wins → 999),
        // NOT abort the MERGE and drop the update (which would leave Bob at 200).
        assert_eq!(
            duration_by_name(&ctx, "Bob").await?,
            999,
            "duplicate source keys in ONE statement must be split into rounds and applied, not dropped by a MERGE cardinality abort"
        );
        Ok(())
    }

    /// Two-join-key variant matching monoscope's enrichment UPDATE-2, which joins on
    /// (context___span_id, context___trace_id). Source has two rows for the same
    /// composite key (a span carrying two hash tags). If split_rounds keys on BOTH
    /// columns it splits them into rounds; a gap here (e.g. under-keyed split) would
    /// let both rows hit one target row → the prod MERGE cardinality abort.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalescer_splits_duplicate_composite_source_keys() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let test_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let mut cfg = (*create_test_config(&test_id)).clone();
        cfg.buffer.timefusion_dml_coalesce_secs = 3600;
        let db = Arc::new(Database::with_config(Arc::new(cfg)).await?);
        db.start_dml_coalescer();
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        // One target span identified by (context___span_id, context___trace_id).
        let now = chrono::Utc::now();
        let rec = serde_json::json!({
            "id": "sp1", "name": "orig", "project_id": "test_project",
            "timestamp": now.timestamp_micros(), "date": now.date_naive().to_string(),
            "context___span_id": "S1", "context___trace_id": "T1",
            "hashes": [], "summary": []
        });
        let batch = timefusion::test_utils::test_helpers::json_to_batch(vec![rec])?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Source: two rows for the SAME (span_id, trace_id) — the multi-tag shape.
        let sql = "UPDATE otel_logs_and_spans o
                     SET name = u.nm
                     FROM (VALUES ('S1','T1','a'), ('S1','T1','b')) AS u(sid, tid, nm)
                     WHERE o.project_id = 'test_project'
                       AND o.context___span_id = u.sid
                       AND o.context___trace_id = u.tid";
        ctx.sql(sql).await?.collect().await?;
        db.dml_coalescer().expect("coalescer enabled").drain(&db).await;

        let name = ctx.sql("SELECT name FROM otel_logs_and_spans WHERE project_id='test_project' AND id='sp1'").await?.collect().await?;
        let got = get_str(name[0].column(name[0].schema().index_of("name")?).as_ref(), 0);
        assert_eq!(got, "b", "composite dup-key source must split into rounds and apply (last wins), not be dropped by a cardinality abort");
        Ok(())
    }

    /// With a buffered layer, the mem leg updates buffer-resident rows
    /// synchronously and the flush persists the POST-DML values to Delta —
    /// the invariant that makes both the watermark clamp and coalescer
    /// deferral sound. Exercises update-before-flush, then verifies Delta
    /// (buffer bypassed) holds the updated row.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_update_from_buffered_rows_persisted_by_flush() -> Result<()> {
        timefusion::test_utils::init_test_logging();
        let cfg = timefusion::test_utils::test_helpers::TestConfigBuilder::new("dml_wm").build();
        // SAFETY: same #[serial]-guarded process-global env dance as
        // buffer_consistency_test.rs.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
        let layer = Arc::new(timefusion::test_utils::test_helpers::test_layer(Arc::clone(&cfg))?);
        let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(Arc::clone(&layer)));
        let mut ctx = db.clone().create_session_context();
        db.setup_session_context(&mut ctx)?;

        let now = chrono::Utc::now();
        let batch = timefusion::test_utils::test_helpers::json_to_batch(create_test_records(now))?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true, None).await?;

        // Rows are buffer-resident. The mem leg applies; the Delta leg has
        // nothing committed to touch (and a time window above the watermark
        // is clamped away entirely).
        let df = ctx
            .sql(
                "UPDATE otel_logs_and_spans
                   SET duration = u.d
                   FROM (VALUES ('Bob', 4242)) AS u(name, d)
                   WHERE project_id = 'test_project'
                     AND otel_logs_and_spans.name = u.name",
            )
            .await?;
        let result = df.collect().await?;
        let rows = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(rows, 1, "mem leg updated the buffered row");
        assert_eq!(duration_by_name(&ctx, "Bob").await?, 4242, "overlay read sees the update immediately");

        // Flush → Delta must hold the POST-update value.
        layer.flush_all_now().await?;
        let delta = db.query_delta_only("SELECT duration FROM otel_logs_and_spans WHERE project_id = 'test_project' AND name = 'Bob'").await?;
        assert_eq!(delta[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 4242, "flush persisted the post-DML value");
        Ok(())
    }
}
