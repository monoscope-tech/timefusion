//src/main.rs

use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{
    array::{StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema},
};
use deltalake::arrow::datatypes::TimeUnit;
use chrono::{DateTime, Utc};
use datafusion::common::DataFusionError;
use datafusion::prelude::*;
use deltalake::{kernel::StructField, DeltaOps, DeltaTable};
use deltalake::{
    DeltaTableBuilder,
    delta_datafusion::{DeltaScanConfig, DeltaTableProvider},
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<DeltaTable>>)>>>;
 
// Helper function to build a TimestampMicrosecondArray with microsecond precision.
fn build_timestamp_array(values: Vec<i64>) -> TimestampMicrosecondArray {
    use deltalake::arrow::array::ArrayData;
    use deltalake::arrow::buffer::Buffer;
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, None);
    let buffer = Buffer::from_slice_ref(&values);
    let array_data = ArrayData::builder(data_type.clone())
        .len(values.len())
        .add_buffer(buffer)
        .build()
        .unwrap();
    TimestampMicrosecondArray::from(array_data)
}
 
struct Database {
    project_configs: ProjectConfigs,
    ctx: SessionContext,
}
 
impl Database {
    async fn new() -> Result<Self, DataFusionError> {
        Ok(Self {
            project_configs: Arc::new(RwLock::new(HashMap::new())),
            ctx: SessionContext::new(),
        })
    }
 
    async fn add_project(
        &self,
        project_id: &str,
        s3_connection_string: &str,
    ) -> Result<(), DataFusionError> {
        let table_path = format!(
            "{}/{}",
            s3_connection_string.trim_end_matches('/'),
            project_id
        );
 
        let table = match DeltaTableBuilder::from_uri(&table_path).load().await {
            Ok(table) => table,
            Err(_) => {
                let struct_fields = vec![
                    StructField::new(
                        "project_id".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        false,
                    ),
                    StructField::new(
                        "timestamp".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        false,
                    ),
                    StructField::new(
                        "start_time".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        true,
                    ),
                    StructField::new(
                        "end_time".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::Timestamp,
                        ),
                        true,
                    ),
                    StructField::new(
                        "payload".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        true,
                    ),
                ];
                // Create a new Delta table with the specified schema.
                DeltaOps::new_in_memory()
                    .create()
                    .with_columns(struct_fields)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
        };
 
        // Insert the project configuration.
        self.project_configs.write().unwrap().insert(
            project_id.to_string(),
            (
                s3_connection_string.to_string(),
                Arc::new(RwLock::new(table)),
            ),
        );
 
        Ok(())
    }
 
    async fn query(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        let project_id = extract_project_id_from_sql(sql)?;
        // Bind the read guard so that it lives long enough.
        let configs = self.project_configs.read().unwrap();
        // Clone the Arc to extend the lifetime beyond the guard.
        let (.., delta_table) = configs
            .get(&project_id)
            .ok_or_else(|| {
                DataFusionError::External(format!("Project ID '{}' not found", project_id).into())
            })?
            .clone();
        let table = delta_table.read().unwrap();
        let snapshot = table
            .snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let provider = DeltaTableProvider::try_new(
            snapshot.clone(),
            table.log_store().clone(),
            DeltaScanConfig::default(),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        self.ctx
            .register_table("table", Arc::new(provider))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        self.ctx.sql(sql).await
    }
 
    async fn write(
        &self,
        project_id: &str,
        timestamp: DateTime<Utc>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        payload: Option<&str>,
    ) -> Result<(), DataFusionError> {
        // Bind the read guard to a variable.
        let configs = self.project_configs.read().unwrap();
        // Clone the Arc so that it lives beyond the guard.
        let (.., _delta_table) = configs
            .get(project_id)
            .expect("no project_id in sql query")
            .clone();
 
        // Define the record batch schema with timestamps having microsecond precision.
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                Arc::new(build_timestamp_array(vec![timestamp.timestamp_micros()])),
                Arc::new(build_timestamp_array(vec![start_time.map_or(timestamp.timestamp_micros(), |t| t.timestamp_micros())])),
                Arc::new(build_timestamp_array(vec![end_time.map_or(timestamp.timestamp_micros(), |t| t.timestamp_micros())])),
                Arc::new(StringArray::from(vec![payload.unwrap_or("")])),
            ],
        ).map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        let batches: Vec<RecordBatch> = vec![batch];
        DeltaOps::new_in_memory()
            .write(batches.into_iter())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        Ok(())
    }
}
 
fn extract_project_id_from_sql(sql: &str) -> Result<String, DataFusionError> {
    // Very simple extraction; use a proper SQL parser in production.
    sql.to_lowercase()
        .find("where project_id = '")
        .map(|start| {
            let end = sql[start + 20..].find("'").unwrap();
            sql[start + 20..start + 20 + end].to_string()
        })
        .ok_or_else(|| DataFusionError::External("Project ID not found in SQL".to_string().into()))
}
 
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let db = Database::new().await?;
    db.add_project("project_123", "file:///tmp/delta_123")
        .await?;
    db.add_project("project_456", "file:///tmp/delta_456")
        .await?;
 
    let now = Utc::now();
    db.write("project_123", now, Some(now), None, Some("data1"))
        .await?;
    db.write("project_456", now, None, Some(now), Some("data2"))
        .await?;
 
    // Note: Enclose the table name in double quotes to avoid SQL parser conflicts.
    db.query("SELECT * FROM \"table\" WHERE project_id = 'project_123'")
        .await?
        .show()
        .await?;
    db.query("SELECT * FROM \"table\" WHERE project_id = 'project_456'")
        .await?
        .show()
        .await?;
 
    Ok(())
}
