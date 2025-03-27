use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
            TimestampNanosecondArray,
        },
        buffer::Buffer,
        datatypes::{DataType, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    execution::context::SessionContext,
    prelude::*,
};
use deltalake::{DeltaOps, DeltaTableBuilder};

use crate::fields::define_telemetry_fields;

pub type ProjectConfigs =
    Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;
    
pub struct Database {
    pub ctx: SessionContext,
    project_configs: ProjectConfigs,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let ctx = SessionContext::new();
        let mut project_configs = HashMap::new();
        let table = DeltaTableBuilder::from_uri("memory://otel_logs_and_spans")
            .with_allow_http(true)
            .build()?;
        project_configs.insert(
            "otel_logs_and_spans".to_string(),
            (
                "memory://otel_logs_and_spans".to_string(),
                Arc::new(RwLock::new(table)),
            ),
        );
        Ok(Self {
            ctx,
            project_configs: Arc::new(RwLock::new(project_configs)),
        })
    }

    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }

    pub async fn query(&self, sql: &str) -> Result<datafusion::prelude::DataFrame> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|e| anyhow::anyhow!("SQL query failed: {:?}", e))
    }

    fn event_schema() -> Schema {
        let mut fields = Vec::new();
        macro_rules! add_field {
            ($name:ident, $data_type:expr, $nullable:expr, $rust_type:ty) => {
                fields.push(datafusion::arrow::datatypes::Field::new(
                    stringify!($name),
                    $data_type,
                    $nullable,
                ));
            };
        }
        define_telemetry_fields!(add_field);
        Schema::new(fields)
    }

    pub async fn write(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        // Use the async lock here:
        let (_conn_str, table_ref) = {
            let configs = self.project_configs.read().await;
            configs
                .get("otel_logs_and_spans")
                .ok_or_else(|| anyhow::anyhow!("Table not found"))?
                .clone()
        };

        let mut arrays: Vec<ArrayRef> = Vec::new();

        macro_rules! add_array {
            // UTF8: convert a String or Option<String> into Option<&str>
            ($name:ident, DataType::Utf8, $nullable:expr, $rust_type:ty) => {{
                let value: Option<&str> = if $nullable {
                    record.$name.as_ref().map(|s| s.as_str())
                } else {
                    Some(&record.$name[..])
                };
                arrays.push(Arc::new(StringArray::from(vec![value])));
            }};
            ($name:ident, DataType::Int32, $nullable:expr, $rust_type:ty) => {{
                arrays.push(Arc::new(Int32Array::from(vec![record.$name])));
            }};
            ($name:ident, DataType::Int64, $nullable:expr, $rust_type:ty) => {{
                arrays.push(Arc::new(Int64Array::from(vec![record.$name])));
            }};
            ($name:ident, DataType::Boolean, $nullable:expr, $rust_type:ty) => {{
                arrays.push(Arc::new(BooleanArray::from(vec![record.$name])));
            }};
            ($name:ident, DataType::Float64, $nullable:expr, $rust_type:ty) => {{
                arrays.push(Arc::new(Float64Array::from(vec![record.$name])));
            }};
            ($name:ident, $data_type:expr, $nullable:expr, $rust_type:ty) => {{
                unimplemented!("Unsupported data type for {}", stringify!($name))
            }};
        }
        // Special handling for timestamps.
        macro_rules! add_timestamp_array {
            ($name:ident) => {{
                if stringify!($name) == "startTimeUnixNano" {
                    arrays.push(Arc::new(build_timestamp_array(
                        vec![record.startTimeUnixNano],
                        Some(Arc::from("UTC")),
                    )))
                } else if stringify!($name) == "endTimeUnixNano" {
                    arrays.push(Arc::new(build_timestamp_array(
                        vec![record.endTimeUnixNano.unwrap_or(0)],
                        Some(Arc::from("UTC")),
                    )))
                }
            }};
        }

        macro_rules! add_field_to_array {
            ($name:ident, $data_type:expr, $nullable:expr, $rust_type:ty) => {
                if stringify!($name) == "startTimeUnixNano" || stringify!($name) == "endTimeUnixNano" {
                    add_timestamp_array!($name);
                } else {
                    add_array!($name, $data_type, $nullable, $rust_type);
                }
            };
        }
        define_telemetry_fields!(add_field_to_array);

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        // Acquire the write lock using async
        let table = table_ref.write().await;
        // Clone the DeltaTable so that we don’t hold the write guard across an await.
        let ops = DeltaOps(table.clone());
        ops.write(vec![batch]).await?;
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

    pub async fn insert_record(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        self.write(record).await
    }
}

fn build_timestamp_array(values: Vec<i64>, tz: Option<Arc<str>>) -> TimestampNanosecondArray {
    use datafusion::arrow::array::ArrayData;
    let data_type = DataType::Timestamp(TimeUnit::Nanosecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone())
        .len(values.len())
        .add_buffer(buffer)
        .build()
        .unwrap();
    TimestampNanosecondArray::from(array_data)
}
