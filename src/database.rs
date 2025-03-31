use std::{collections::HashMap, sync::Arc};
use anyhow::Result;
use chrono::Utc;
use datafusion::execution::context::SessionContext;
use delta_kernel::schema::StructField;
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use log::warn;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use tokio::sync::RwLock;

use crate::persistent_queue::OtelLogsAndSpans;

type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);
pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

pub struct Database {
    pub session_context: SessionContext,
    project_configs:     ProjectConfigs,
}

impl Database {
    pub async fn new(storage_uri: &str) -> Result<Self> {
        let session_context = SessionContext::new();
        let project_configs = Arc::new(RwLock::new(HashMap::new()));
        let db = Self { session_context, project_configs };
        // Register a default project.
        db.register_project("default", storage_uri, None, None, None).await?;
        Ok(db)
    }

    /// Inserts records into the appropriate Delta tables.
    /// Records are grouped first by project, then by span (using record.id as the span identifier)
    /// before insertion.
    pub async fn insert_records(&self, records: &Vec<OtelLogsAndSpans>) -> Result<()> {
        let mut groups: HashMap<String, HashMap<String, Vec<OtelLogsAndSpans>>> = HashMap::new();
        for record in records {
            groups.entry(record.project_id.clone()).or_default().entry(record.id.clone()).or_default().push(record.clone());
        }
        for (project_id, span_groups) in groups {
            let (_conn_str, _options, table_ref) = {
                let configs = self.project_configs.read().await;
                configs.get(&project_id).ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", project_id))?.clone()
            };
            let mut table = table_ref.write().await;
            for (_span_id, span_records) in span_groups {
                let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(
                    TracingOptions::default(),
                )?;
                let batch = serde_arrow::to_record_batch(&fields, &span_records)?;
                let ops = DeltaOps(table.clone());
                *table = ops.write(vec![batch]).await?;
            }
        }
        Ok(())
    }

    /// Registers a project by creating (or loading) its Delta table and registering it in the session context
    /// under a unique name based on the project ID.
    pub async fn register_project(
        &self, project_id: &str, conn_str: &str, access_key: Option<&str>, secret_key: Option<&str>, endpoint: Option<&str>,
    ) -> Result<()> {
        let mut storage_options = StorageOptions::default();
        if let Some(key) = access_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_ACCESS_KEY_ID".to_string(), key.to_string());
        }
        if let Some(key) = secret_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_SECRET_ACCESS_KEY".to_string(), key.to_string());
        }
        if let Some(ep) = endpoint.filter(|e| !e.is_empty()) {
            storage_options.0.insert("AWS_ENDPOINT".to_string(), ep.to_string());
        }
        storage_options.0.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        let table = match DeltaTableBuilder::from_uri(&conn_str).with_storage_options(storage_options.0.clone()).with_allow_http(true).load().await {
                Ok(table) => table,
                Err(err) => {
                    warn!("Table doesn't exist. Creating new table. Err: {:?}", err);
                    let tracing_options = TracingOptions::default();
                    let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(tracing_options)?;
                    warn!("Creating new table for project '{}'. Original error: {:?}", project_id, err);
                    let vec_refs: Vec<StructField> = fields.iter().map(|arc_field| arc_field.as_ref().try_into().unwrap()).collect();
                    let delta_ops = DeltaOps::try_from_uri(&conn_str).await?;
                    delta_ops.create().with_columns(vec_refs).with_partition_columns(vec!["project_id".to_string(), "timestamp".to_string()]).with_storage_options(storage_options.0.clone()).await?
                }
            };

        let table_name = format!("otel_logs_and_spans_{}", project_id);
        self.session_context.register_table(&table_name, Arc::new(table.clone()))?;
        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str.to_string(), storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Timelike};
    use datafusion::arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use datafusion::prelude::SessionContext;
    use tempfile::tempdir;
    use super::*;

    // Helper: Refresh the session context with the latest tables.
    async fn refresh_session_context(db: &Database) -> Result<SessionContext> {
        let new_ctx = SessionContext::new();
        let configs = db.project_configs.read().await;
        for (project_id, (_, _, table_lock)) in configs.iter() {
            let table_name = format!("otel_logs_and_spans_{}", project_id);
            new_ctx.register_table(&table_name, Arc::new(table_lock.read().await.clone()))?;
        }
        Ok(new_ctx)
    }

    #[tokio::test]
    async fn test_database_query() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempdir()?;
        let storage_uri = temp_dir.path().to_str().unwrap();
        let db = Database::new(storage_uri).await?;
        db.register_project("test_project", storage_uri, None, None, None).await?;

        let ts1 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 10, 0).unwrap();
        let span1 = OtelLogsAndSpans {
            project_id: "test_project".to_string(),
            timestamp: ts1,
            observed_timestamp: ts1,
            id: "span1".to_string(),
            name: "test_span_1".to_string(),
            context___trace_id: "trace1".to_string(),
            context___span_id: "span1".to_string(),
            start_time: ts1,
            duration: 100_000_000,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        let span2 = OtelLogsAndSpans {
            project_id: "test_project".to_string(),
            timestamp: ts2,
            observed_timestamp: ts2,
            id: "span2".to_string(),
            name: "test_span_2".to_string(),
            context___trace_id: "trace2".to_string(),
            context___span_id: "span2".to_string(),
            start_time: ts2,
            duration: 200_000_000,
            status_code: Some("ERROR".to_string()),
            level: Some("ERROR".to_string()),
            status_message: Some("Error occurred".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![span1, span2]).await?;
        let new_ctx = refresh_session_context(&db).await?;
        let table_name = "otel_logs_and_spans_test_project";

        let count_df = new_ctx.sql(&format!("SELECT COUNT(*) FROM {}", table_name)).await?;
        let count_batches = count_df.collect().await?;
        let count_array = count_batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(count_array.value(0) > 0, "Expected at least one row inserted");

        let df = new_ctx.sql(&format!("SELECT name, status_code, level FROM {} ORDER BY name", table_name)).await?;
        let batches = df.collect().await?;
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 2);
        let name_col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let status_code_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let level_col = batches[0].column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "test_span_1");
        assert_eq!(status_code_col.value(0), "OK");
        assert_eq!(level_col.value(0), "INFO");
        assert_eq!(name_col.value(1), "test_span_2");
        assert_eq!(status_code_col.value(1), "ERROR");
        assert_eq!(level_col.value(1), "ERROR");

        let df = new_ctx.sql(&format!(
            "SELECT name, level, status_code, status_message FROM {} WHERE project_id = 'test_project' and level = 'ERROR'",
            table_name
        )).await?;
        let batches = df.collect().await?;
        let filtered_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(filtered_rows, 1);
        let name_col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let level_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let status_code_col = batches[0].column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let status_message_col = batches[0].column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "test_span_2");
        assert_eq!(level_col.value(0), "ERROR");
        assert_eq!(status_code_col.value(0), "ERROR");
        assert_eq!(status_message_col.value(0), "Error occurred");

        Ok(())
    }

    #[tokio::test]
    async fn test_nanosecond_precision() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempdir()?;
        let storage_uri = temp_dir.path().to_str().unwrap();
        let db = Database::new(storage_uri).await?;
        db.register_project("ns_project", storage_uri, None, None, None).await?;

        // Create a timestamp with non-zero nanosecond precision.
        let base = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let ts = base.with_nanosecond(123456789).unwrap();

        let record = OtelLogsAndSpans {
            project_id: "ns_project".to_string(),
            timestamp: ts,
            observed_timestamp: ts,
            id: "span_ns".to_string(),
            name: "nanosecond_test".to_string(),
            context___trace_id: "trace_ns".to_string(),
            context___span_id: "span_ns".to_string(),
            start_time: ts,
            duration: 1000,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };

        db.insert_records(&vec![record]).await?;
        let new_ctx = refresh_session_context(&db).await?;
        let table_name = "otel_logs_and_spans_ns_project";

        // Query the timestamp column and check its raw integer value.
        let df = new_ctx.sql(&format!("SELECT timestamp FROM {}", table_name)).await?;
        let batches = df.collect().await?;
        assert!(!batches.is_empty(), "No batches returned for nanosecond test");
        let column = batches[0].column(0);
        let retrieved_ts_nanos = if let Some(ts_array) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
            ts_array.value(0)
        } else if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
            int64_array.value(0)
        } else {
            panic!("Column 'timestamp' is not of an expected type. Schema: {:?}", batches[0].schema());
        };
        let expected = ts.timestamp_nanos_opt().unwrap();
        assert_eq!(retrieved_ts_nanos, expected, "Nanosecond precision mismatch");
        Ok(())
    }

    #[tokio::test]
    async fn test_grouping_by_span() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let temp_dir = tempdir()?;
        let storage_uri = temp_dir.path().to_str().unwrap();
        let db = Database::new(storage_uri).await?;
        db.register_project("group_project", storage_uri, None, None, None).await?;

        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
        let record1 = OtelLogsAndSpans {
            project_id: "group_project".to_string(),
            timestamp,
            observed_timestamp: timestamp,
            id: "group_span".to_string(),
            name: "group_test_1".to_string(),
            context___trace_id: "trace_group".to_string(),
            context___span_id: "group_span".to_string(),
            start_time: timestamp,
            duration: 500,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        let record2 = OtelLogsAndSpans {
            project_id: "group_project".to_string(),
            timestamp,
            observed_timestamp: timestamp,
            id: "group_span".to_string(),
            name: "group_test_2".to_string(),
            context___trace_id: "trace_group".to_string(),
            context___span_id: "group_span".to_string(),
            start_time: timestamp,
            duration: 600,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record1, record2]).await?;
        let new_ctx = refresh_session_context(&db).await?;
        let table_name = "otel_logs_and_spans_group_project";
        let df = new_ctx.sql(&format!("SELECT name FROM {} WHERE id = 'group_span'", table_name)).await?;
        let batches = df.collect().await?;
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total_rows, 2, "Grouping by span failed: expected 2 rows");
        Ok(())
    }
}
