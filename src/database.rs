use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::array::{StringArray, TimestampMicrosecondArray};
use chrono::{DateTime, Utc};
use datafusion::common::DataFusionError;
use deltalake::{DeltaTableBuilder, DeltaOps};
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::kernel::StructField;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::utils::prepare_sql;

pub type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<deltalake::DeltaTable>>)>>>;
 
pub struct Database {
    project_configs: ProjectConfigs,
    pub ctx: SessionContext,
}

impl Database {
    pub async fn new() -> Result<Self, DataFusionError> {
        Ok(Self {
            project_configs: Arc::new(RwLock::new(HashMap::new())),
            ctx: SessionContext::new(),
        })
    }

    // Expose a cloneable session context.
    pub fn get_session_context(&self) -> SessionContext {
        self.ctx.clone()
    }
 
    pub async fn add_project(&self, project_id: &str, connection_string: &str) -> Result<(), DataFusionError> {
        let table_path = if connection_string.starts_with("s3://")
            || connection_string.starts_with("s3a://")
            || connection_string.starts_with("s3n://")
        {
            connection_string.to_string()
        } else {
            format!("{}/{}", connection_string.trim_end_matches('/'), project_id)
        };
 
        // Try loading the table. If it fails with "TableAlreadyExists", load it again.
        let table = match DeltaTableBuilder::from_uri(&table_path).load().await {
            Ok(table) => table,
            Err(e) => {
                if e.to_string().contains("TableAlreadyExists") {
                    // Table exists; load it.
                    DeltaTableBuilder::from_uri(&table_path).load().await?
                } else {
                    // Otherwise, try creating it.
                    let struct_fields = vec![
                        StructField::new("project_id".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String), false),
                        StructField::new("timestamp".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp), false),
                        StructField::new("start_time".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp), true),
                        StructField::new("end_time".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::Timestamp), true),
                        StructField::new("payload".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String), true),
                        StructField::new("event_date".to_string(), deltalake::kernel::DataType::Primitive(deltalake::kernel::PrimitiveType::String), false),
                    ];
                    DeltaOps::try_from_uri(&table_path)
                        .await?
                        .create()
                        .with_columns(struct_fields)
                        .with_partition_columns(vec!["event_date".to_string()])
                        .await
                        .map_err(|e| DataFusionError::External(e.into()))?
                }
            }
        };
 
        self.project_configs.write().unwrap().insert(
            project_id.to_string(),
            (connection_string.to_string(), Arc::new(RwLock::new(table))),
        );
 
        // Register the table with DataFusion.
        let configs = self.project_configs.read().unwrap();
        let (_conn_str, delta_table) = configs.get(project_id).unwrap().clone();
        let table_ref = delta_table.read().unwrap();
        let snapshot = table_ref
            .snapshot()
            .map_err(|e| DataFusionError::External(e.into()))?;
        let provider = DeltaTableProvider::try_new(
            snapshot.clone(),
            table_ref.log_store().clone(),
            DeltaScanConfig::default(),
        )
        .map_err(|e| DataFusionError::External(e.into()))?;
        let unique_table_name = format!("table_{}", project_id);
        self.ctx.register_table(&unique_table_name, Arc::new(provider))
            .map_err(|e| DataFusionError::External(e.into()))?;
 
        Ok(())
    }
 
    pub async fn query(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        let new_sql = prepare_sql(sql)?;
        self.ctx.sql(&new_sql).await
    }
 
    pub async fn write(
        &self,
        project_id: &str,
        timestamp: DateTime<Utc>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        payload: Option<&str>,
    ) -> Result<(), DataFusionError> {
        let conn_str = {
            let configs = self.project_configs.read().unwrap();
            configs.get(project_id).unwrap().0.clone()
        };
 
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), false),
            Field::new("start_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("end_time", DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))), true),
            Field::new("payload", DataType::Utf8, true),
            Field::new("event_date", DataType::Utf8, false),
        ]);
 
        let event_date = timestamp.format("%Y-%m-%d").to_string();
        let ts_micro = timestamp.timestamp_micros();
        let start_ts_micro = start_time.map_or(ts_micro, |t| t.timestamp_micros());
        let end_ts_micro = end_time.map_or(ts_micro, |t| t.timestamp_micros());
 
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
        ).map_err(|e| DataFusionError::External(e.into()))?;
 
        let batches: Vec<RecordBatch> = vec![batch];
        DeltaOps::try_from_uri(&conn_str)
            .await?
            .write(batches.into_iter())
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
 
        Ok(())
    }
 
    pub async fn compact(&self, project_id: &str) -> Result<(), DataFusionError> {
        let delta_table_arc = {
            let configs = self.project_configs.read().unwrap();
            configs.get(project_id)
                .ok_or_else(|| DataFusionError::External(format!("Project ID '{}' not found", project_id).into()))?
                .1.clone()
        };
        let _table = delta_table_arc.read().unwrap();
        println!("Compaction for project '{}' would run here.", project_id);
        Ok(())
    }
    
    // Dummy DML methods for PGWire requests.
    pub async fn insert_record(&self, _query: &str) -> Result<String, DataFusionError> {
        Ok("INSERT successful".to_string())
    }
    
    pub async fn update_record(&self, _query: &str) -> Result<String, DataFusionError> {
        Ok("UPDATE successful".to_string())
    }
    
    pub async fn delete_record(&self, _query: &str) -> Result<String, DataFusionError> {
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
