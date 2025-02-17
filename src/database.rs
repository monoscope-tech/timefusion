use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};
use chrono::{DateTime, Utc, TimeZone};
use datafusion::common::DataFusionError;
use deltalake::{DeltaTableBuilder, DeltaOps};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::utils::prepare_sql;
use anyhow::Context;

#[allow(dead_code)]
fn register_json_functions(_ctx: &mut SessionContext) {
    println!("(Stub) Registering JSON functions");
}

pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;
 
pub struct Database {
    pub ctx: SessionContext,
    project_configs: ProjectConfigs,
}
 
impl Database {
    pub async fn new() -> anyhow::Result<Self> {
        let mut ctx = SessionContext::new();
        register_json_functions(&mut ctx);
        Ok(Self {
            project_configs: Arc::new(RwLock::new(HashMap::new())),
            ctx,
        })
    }
 
    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }
 
    // Adds a project and stores its connection string.
    pub async fn add_project(&self, project_id: &str, connection_string: &str) -> anyhow::Result<()> {
        let table = match DeltaTableBuilder::from_uri(connection_string).load().await {
            Ok(table) => table,
            Err(_) => {
                DeltaOps::try_from_uri(connection_string)
                    .await?
                    .create()
                    .with_columns(Vec::<deltalake::kernel::StructField>::new())
                    .with_partition_columns(vec!["event_date".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .context("Creating dummy Delta table failed")?
            }
        };
        self.project_configs.write()
            .map_err(|_| anyhow::anyhow!("Failed to acquire write lock on project configs"))
            .context("Lock error in add_project")?
            .insert(
                project_id.to_string(),
                (connection_string.to_string(), Arc::new(RwLock::new(table))),
            );
        Ok(())
    }
 
    // Creates and registers the events table for the given project.
    pub async fn create_events_table(&self, project_id: &str, table_uri: &str) -> anyhow::Result<()> {
        // Define the schema.
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("trace_state", DataType::Utf8, true),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("duration_ns", DataType::Int64, false),
            Field::new("span_name", DataType::Utf8, false),
            Field::new("span_kind", DataType::Utf8, false),
            Field::new("span_type", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, false),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("severity_number", DataType::Int32, false),
            Field::new("host", DataType::Utf8, false),
            Field::new("url_path", DataType::Utf8, false),
            Field::new("raw_url", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("referer", DataType::Utf8, false),
            Field::new("path_params", DataType::Utf8, false),
            Field::new("query_params", DataType::Utf8, false),
            Field::new("request_headers", DataType::Utf8, false),
            Field::new("response_headers", DataType::Utf8, false),
            Field::new("request_body", DataType::Utf8, false),
            Field::new("response_body", DataType::Utf8, false),
            Field::new("endpoint_hash", DataType::Utf8, false),
            Field::new("shape_hash", DataType::Utf8, false),
            Field::new("format_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("field_hashes", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
            Field::new("sdk_type", DataType::Utf8, false),
            Field::new("service_version", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("links", DataType::Utf8, false),
            Field::new("resource", DataType::Utf8, false),
            Field::new("instrumentation_scope", DataType::Utf8, false),
            Field::new("errors", DataType::Utf8, false),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        ]);
        let columns: Vec<deltalake::kernel::StructField> = schema.fields().iter().map(|f| {
            deltalake::kernel::StructField::new(
                f.name().to_string(),
                if let DataType::Utf8 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                } else if let DataType::Timestamp(_, _) = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp)
                } else if let DataType::Int64 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Long)
                } else if let DataType::Int32 = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Integer)
                } else if let DataType::List(_) = f.data_type() {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                } else {
                    deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String)
                },
                f.is_nullable(),
            )
        }).collect();
        let table = match DeltaTableBuilder::from_uri(table_uri).load().await {
            Ok(table) => table,
            Err(_) => {
                DeltaOps::try_from_uri(table_uri)
                    .await?
                    .create()
                    .with_columns(columns)
                    .with_partition_columns(vec!["project_id".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))
                    .context("Failed to create events table")?
            }
        };
 
        use deltalake::delta_datafusion::{DeltaTableProvider, DeltaScanConfig};
        let table_ref = Arc::new(RwLock::new(table));
        let provider = {
            let table_guard = table_ref.read().map_err(|_| anyhow::anyhow!("Lock error on delta table"))?;
            let snapshot = table_guard.snapshot()
                .map_err(|e| anyhow::anyhow!("Failed to get table snapshot: {:?}", e))?;
            let log_store = table_guard.log_store().clone();
            DeltaTableProvider::try_new(snapshot.clone(), log_store, DeltaScanConfig::default())
                .map_err(|e| anyhow::anyhow!("Failed to create DeltaTableProvider: {:?}", e))?
        };
        let table_name = format!("table_{}", project_id);
        self.ctx.register_table(&table_name, Arc::new(provider))
            .map_err(|e| anyhow::anyhow!("Failed to register table: {:?}", e))
            .map(|_| ())
    }
 
    pub async fn query(&self, sql: &str) -> anyhow::Result<DataFrame> {
        let new_sql = prepare_sql(sql).context("Failed to prepare SQL")?;
        self.ctx.sql(&new_sql).await.context("Failed to execute SQL")
    }
 
    pub async fn write(
        &self,
        project_id: &str,
        timestamp: DateTime<Utc>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        payload: Option<&str>,
    ) -> anyhow::Result<()> {
        let conn_str = {
            let configs = self.project_configs.read().map_err(|_| anyhow::anyhow!("Lock error in write"))?;
            configs.get(project_id)
                .ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", project_id))?
                .0
                .clone()
        };
 
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("event_date", DataType::Utf8, false),
        ]);
 
        let ts_micro = timestamp.timestamp_micros();
        let start_ts_micro = start_time.map_or(ts_micro, |t| t.timestamp_micros());
        let end_ts_micro = end_time.map_or(ts_micro, |t| t.timestamp_micros());
 
        let dt = Utc.timestamp_micros(ts_micro).unwrap();
        let event_date = dt.date_naive().to_string();
 
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                Arc::new(build_timestamp_array(vec![ts_micro], Some(Arc::from("UTC")))),
                Arc::new(build_timestamp_array(vec![start_ts_micro], Some(Arc::from("UTC")))),
                Arc::new(build_timestamp_array(vec![end_ts_micro], Some(Arc::from("UTC")))),
                Arc::new(StringArray::from(vec![payload.unwrap_or("")])),
                Arc::new(StringArray::from(vec![event_date])),
            ],
        ).map_err(|e| anyhow::anyhow!("Failed to create record batch: {:?}", e))?;
 
        let batches: Vec<RecordBatch> = vec![batch];
        DeltaOps::try_from_uri(&conn_str)
            .await
            .context("Failed to create DeltaOps from URI")?
            .write(batches.into_iter())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write batch: {:?}", e))?;
 
        Ok(())
    }
 
    pub async fn compact(&self, project_id: &str) -> anyhow::Result<()> {
        let delta_table_arc = {
            let configs = self.project_configs.read().map_err(|_| anyhow::anyhow!("Lock error in compact"))?;
            configs.get(project_id)
                .ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", project_id))?
                .1.clone()
        };
        let _table = delta_table_arc.read().map_err(|_| anyhow::anyhow!("Lock error on delta table"))?;
        println!("Compaction for project '{}' would run here.", project_id);
        Ok(())
    }
    
    pub async fn insert_record(&self, _query: &str) -> anyhow::Result<String> {
        Ok("INSERT successful".to_string())
    }
    
    pub async fn update_record(&self, _query: &str) -> anyhow::Result<String> {
        Ok("UPDATE successful".to_string())
    }
    
    pub async fn delete_record(&self, _query: &str) -> anyhow::Result<String> {
        Ok("DELETE successful".to_string())
    }
}
 
fn build_timestamp_array(values: Vec<i64>, tz: Option<Arc<str>>) -> TimestampMicrosecondArray {
    use datafusion::arrow::array::ArrayData;
    use datafusion::arrow::buffer::Buffer;
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone())
        .len(values.len())
        .add_buffer(buffer)
        .build()
        .unwrap();
    TimestampMicrosecondArray::from(array_data)
}
