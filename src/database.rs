use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    execution::context::SessionContext,
};
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use tokio::sync::RwLock;
type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);

pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

pub struct Database {
    pub ctx:         SessionContext,
    project_configs: ProjectConfigs,
}

impl Database {
    pub async fn new(storage_uri: &str) -> Result<Self> {
        let ctx = SessionContext::new();
        let mut project_configs = HashMap::new();

        let default_options = StorageOptions::default();
        let table = DeltaTableBuilder::from_uri(storage_uri)
            .with_allow_http(true)
            .with_storage_options(default_options.0.clone())
            .build()?;
        ctx.register_table("otel_logs_and_spans", Arc::new(table.clone()))?;
        project_configs.insert("default".to_string(), (storage_uri.to_string(), default_options, Arc::new(RwLock::new(table))));

        Ok(Self {
            ctx,
            project_configs: Arc::new(RwLock::new(project_configs)),
        })
    }

    /// Returns a reference to the SessionContext for querying.
    pub fn get_session_context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn query(&self, project_id: &str, sql: &str) -> Result<DataFrame> {
        let configs = self.project_configs.read().await;
        if !configs.contains_key(project_id) {
            return Err(anyhow::anyhow!("Project ID '{}' not found", project_id));
        }
        let adjusted_sql = sql.replace("otel_logs_and_spans", &format!("otel_logs_and_spans_{}", project_id));
        self.ctx
            .sql(&adjusted_sql)
            .await
            .map_err(|e| anyhow::anyhow!("SQL query failed for project '{}': {:?}", project_id, e))
    }

    fn event_schema() -> Schema {
        Schema::new(vec![
            Field::new("traceId", DataType::Utf8, false),
            Field::new("spanId", DataType::Utf8, false),
            Field::new("startTimeUnixNano", DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))), false),
            Field::new("endTimeUnixNano", DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))), true),
            Field::new("name", DataType::Utf8, false),
        ])
    }

    pub async fn insert_record(
        &self,
        project_id: &str,
        record: &crate::persistent_queue::IngestRecord,
    ) -> Result<()> {
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get(project_id).ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", project_id))?.clone()
        };

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some(&record.context___trace_id[..])])),
            Arc::new(StringArray::from(vec![Some(&record.context___span_id[..])])),
            Arc::new(TimestampNanosecondArray::from(vec![record.timestamp])),
            Arc::new(TimestampNanosecondArray::from(vec![record.start_time])),
            Arc::new(TimestampNanosecondArray::from(vec![record.end_time.unwrap_or(0)])),
            Arc::new(StringArray::from(vec![Some(&record.name[..])])),
        ];

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());
        *table = ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_project(
        &self,
        project_id: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
    ) -> Result<()> {
        let conn_str = format!("s3://{}/otel_logs_and_spans_{}", bucket, project_id);
        let mut storage_options = StorageOptions::default();
        storage_options.0.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
        storage_options.0.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
        storage_options.0.insert("AWS_ENDPOINT".to_string(), endpoint.to_string());
        storage_options.0.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        let table = DeltaTableBuilder::from_uri(&conn_str)
            .with_storage_options(storage_options.0.clone())
            .with_allow_http(true)
            .build()?;

        self.ctx.register_table(&format!("otel_logs_and_spans_{}", project_id), Arc::new(table.clone()))?;

        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str, storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}
