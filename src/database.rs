use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{ArrayRef, StringArray, TimestampNanosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    execution::context::SessionContext,
    prelude::*,
};
use deltalake::{DeltaOps, DeltaTableBuilder};

pub type ProjectConfigs =
    Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;

pub struct Database {
    pub ctx: SessionContext,
    project_configs: ProjectConfigs,
}

impl Database {
    pub async fn new(storage_uri: &str) -> Result<Self> {
        let ctx = SessionContext::new();
        let mut project_configs = HashMap::new();

        // Initialize Delta Table with S3 URI
        let table = DeltaTableBuilder::from_uri(storage_uri)
            .with_allow_http(true)
            .build()?;
        ctx.register_table("otel_logs_and_spans", Arc::new(table.clone()))?;
        project_configs.insert(
            "otel_logs_and_spans".to_string(),
            (storage_uri.to_string(), Arc::new(RwLock::new(table))),
        );

        Ok(Self {
            ctx,
            project_configs: Arc::new(RwLock::new(project_configs)),
        })
    }

    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("SQL query failed: {:?}", e))
    }

    fn event_schema() -> Schema {
        Schema::new(vec![
            Field::new("traceId", DataType::Utf8, false),
            Field::new("spanId", DataType::Utf8, false),
            Field::new(
                "startTimeUnixNano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "endTimeUnixNano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                true,
            ),
            Field::new("name", DataType::Utf8, false),
        ])
    }

    pub async fn insert_record(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        let (_conn_str, table_ref) = {
            let configs = self.project_configs.read().await;
            configs
                .get("otel_logs_and_spans")
                .ok_or_else(|| anyhow::anyhow!("Table 'otel_logs_and_spans' not found"))?
                .clone()
        };

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some(&record.traceId[..])])),
            Arc::new(StringArray::from(vec![Some(&record.spanId[..])])),
            Arc::new(TimestampNanosecondArray::from(vec![record.startTimeUnixNano])),
            Arc::new(TimestampNanosecondArray::from(vec![
                record.endTimeUnixNano.unwrap_or(0)
            ])),
            Arc::new(StringArray::from(vec![Some(&record.name[..])])),
        ];

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());
        *table = ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_table(&self, conn_str: &str) -> Result<()> {
        let table = DeltaTableBuilder::from_uri(conn_str)
            .with_allow_http(true)
            .build()?;
        let mut configs = self.project_configs.write().await;
        configs.insert(
            "otel_logs_and_spans".to_string(),
            (conn_str.to_string(), Arc::new(RwLock::new(table))),
        );
        Ok(())
    }
}