use std::sync::{Arc as StdArc, Arc, RwLock};
use std::collections::HashMap;

use anyhow::Result;
use datafusion::{
    arrow::{
        array::TimestampNanosecondArray,
        datatypes::{DataType, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    execution::context::SessionContext,
    prelude::*,
};
use deltalake::{DeltaOps, DeltaTableBuilder};

use crate::fields::define_telemetry_fields; 

pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;

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
            ("memory://otel_logs_and_spans".to_string(), Arc::new(RwLock::new(table))),
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
        self.ctx.sql(sql).await.map_err(|e| anyhow::anyhow!("SQL query failed: {:?}", e))
    }

    fn event_schema() -> Schema {
        let mut fields = Vec::new();
        macro_rules! add_field {
            ($name:ident, $data_type:expr, $nullable:expr, $rust_type:ty) => {
                fields.push(datafusion::arrow::datatypes::Field::new(stringify!($name), $data_type, $nullable));
            };
        }
        define_telemetry_fields!(add_field);
        Schema::new(fields)
    }

    pub async fn write(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        let (conn_str, table_ref) = {
            let configs = self.project_configs.read().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
            configs.get("otel_logs_and_spans").ok_or_else(|| anyhow::anyhow!("Table not found"))?.clone()
        };

        let mut arrays = Vec::new();
        macro_rules! add_array {
            ($name:ident, $data_type:expr, $nullable:expr, $rust_type:ty) => {
                match stringify!($name) {
                    "start_time_unix_nano" => arrays.push(Arc::new(build_timestamp_array(vec![record.start_time_unix_nano], Some(StdArc::from("UTC"))))),
                    "end_time_unix_nano" => arrays.push(Arc::new(build_timestamp_array(vec![record.end_time_unix_nano.unwrap_or(0)], Some(StdArc::from("UTC"))))),
                    _ => {
                        let value = &record.$name;
                        match $data_type {
                            DataType::Utf8 => arrays.push(Arc::new(datafusion::arrow::array::StringArray::from(vec![value.clone().unwrap_or_default()]))),
                            DataType::Int32 => arrays.push(Arc::new(datafusion::arrow::array::Int32Array::from(vec![*value]))),
                            DataType::Int64 => arrays.push(Arc::new(datafusion::arrow::array::Int64Array::from(vec![*value]))),
                            DataType::Boolean => arrays.push(Arc::new(datafusion::arrow::array::BooleanArray::from(vec![*value]))),
                            DataType::Float64 => arrays.push(Arc::new(datafusion::arrow::array::Float64Array::from(vec![*value]))),
                            _ => unimplemented!("Unsupported data type for {}", stringify!($name)),
                        }
                    }
                }
            };
        }
        define_telemetry_fields!(add_array);

        let schema = Self::event_schema();
        let batch = RecordBatch::try_new(Arc::new(schema), arrays)?;

        let mut table = table_ref.write().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
        let ops = DeltaOps(*table);
        ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_table(&self, conn_str: &str) -> Result<()> {
        let table = DeltaTableBuilder::from_uri(conn_str)
            .with_allow_http(true)
            .build()?;
        let mut configs = self.project_configs.write().map_err(|e| anyhow::anyhow!("Lock error: {:?}", e))?;
        configs.insert("otel_logs_and_spans".to_string(), (conn_str.to_string(), Arc::new(RwLock::new(table))));
        Ok(())
    }

    pub async fn insert_record(&self, record: &crate::persistent_queue::IngestRecord) -> Result<()> {
        self.write(record).await
    }
}

fn build_timestamp_array(values: Vec<i64>, tz: Option<StdArc<str>>) -> TimestampNanosecondArray {
    use datafusion::arrow::{array::ArrayData, buffer::Buffer};
    let data_type = DataType::Timestamp(TimeUnit::Nanosecond, tz);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone()).len(values.len()).add_buffer(buffer).build().unwrap();
    TimestampNanosecondArray::from(array_data)
}