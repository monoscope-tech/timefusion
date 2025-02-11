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
use std::env;

// Define a type alias for shared project configuration.
type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<DeltaTable>>)>>>;
 
// Updated helper function to build a TimestampMicrosecondArray that accepts an optional timezone.
fn build_timestamp_array(values: Vec<i64>, tz: Option<Arc<str>>) -> TimestampMicrosecondArray {
    use deltalake::arrow::array::ArrayData;
    use deltalake::arrow::buffer::Buffer;
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, tz);
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
        connection_string: &str,
    ) -> Result<(), DataFusionError> {
        // Determine the table URI: if connection_string starts with an S3 scheme, use it;
        // otherwise, append the project_id.
        let table_path = if connection_string.starts_with("s3://")
            || connection_string.starts_with("s3a://")
            || connection_string.starts_with("s3n://")
        {
            connection_string.to_string()
        } else {
            format!("{}/{}", connection_string.trim_end_matches('/'), project_id)
        };
 
        // Try to load the Delta table; if it doesn't exist, create it.
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
                    // Partition column "event_date"
                    StructField::new(
                        "event_date".to_string(),
                        deltalake::kernel::DataType::Primitive(
                            deltalake::kernel::PrimitiveType::String,
                        ),
                        false,
                    ),
                ];
                DeltaOps::try_from_uri(&table_path)
                    .await?
                    .create()
                    .with_columns(struct_fields)
                    .with_partition_columns(vec!["event_date".to_string()])
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
            }
        };
 
        self.project_configs.write().unwrap().insert(
            project_id.to_string(),
            (connection_string.to_string(), Arc::new(RwLock::new(table))),
        );
 
        Ok(())
    }
 
    async fn query(&self, sql: &str) -> Result<DataFrame, DataFusionError> {
        // Extract project ID from the SQL (expects "WHERE project_id = '<id>'").
        let project_id = extract_project_id_from_sql(sql)?;
        let configs = self.project_configs.read().unwrap();
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
 
        // Register a unique table name for DataFusion.
        let unique_table_name = format!("table_{}", project_id);
        self.ctx
            .register_table(&unique_table_name, Arc::new(provider))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        let new_sql = sql.replace("\"table\"", &format!("\"{}\"", unique_table_name));
        self.ctx.sql(&new_sql).await
    }
 
    async fn write(
        &self,
        project_id: &str,
        timestamp: DateTime<Utc>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        payload: Option<&str>,
    ) -> Result<(), DataFusionError> {
        // Retrieve connection string from the project configuration.
        let configs = self.project_configs.read().unwrap();
        let (conn_str, _) = configs
            .get(project_id)
            .expect("no project_id in sql query")
            .clone();
 
        // Define the Arrow schema with timestamp fields that include an explicit timezone.
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("payload", DataType::Utf8, true),
            Field::new("event_date", DataType::Utf8, false),
        ]);
 
        // Compute the partition column "event_date" as "YYYY-MM-DD".
        let event_date = timestamp.format("%Y-%m-%d").to_string();
 
        // Build the record batch.
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                // Supply timezone "UTC" to the helper function.
                Arc::new(build_timestamp_array(vec![timestamp.timestamp_micros()], Some("UTC".into()))),
                Arc::new(build_timestamp_array(
                    vec![start_time.map_or(timestamp.timestamp_micros(), |t| t.timestamp_micros())],
                    Some("UTC".into())
                )),
                Arc::new(build_timestamp_array(
                    vec![end_time.map_or(timestamp.timestamp_micros(), |t| t.timestamp_micros())],
                    Some("UTC".into())
                )),
                Arc::new(StringArray::from(vec![payload.unwrap_or("")])),
                Arc::new(StringArray::from(vec![event_date])),
            ],
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        let batches: Vec<RecordBatch> = vec![batch];
        // Write using DeltaOps::try_from_uri with the connection string.
        DeltaOps::try_from_uri(&conn_str)
            .await?
            .write(batches.into_iter())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
 
        Ok(())
    }
 
    // Stub for periodic compaction.
    async fn compact(&self, project_id: &str) -> Result<(), DataFusionError> {
        let configs = self.project_configs.read().unwrap();
        let (_, delta_table_arc) = configs.get(project_id)
            .ok_or_else(|| DataFusionError::External(format!("Project ID '{}' not found", project_id).into()))?
            .clone();
        let _table = delta_table_arc.read().unwrap();
        println!("Compaction for project '{}' would run here.", project_id);
        Ok(())
    }
}
 
// Helper to extract the project ID from a SQL string (expects "WHERE project_id = '<id>'").
fn extract_project_id_from_sql(sql: &str) -> Result<String, DataFusionError> {
    sql.to_lowercase()
        .find("where project_id = '")
        .map(|start| {
            let idx = start + "where project_id = '".len();
            let end = sql[idx..].find('\'').unwrap();
            sql[idx..idx + end].to_string()
        })
        .ok_or_else(|| DataFusionError::External("Project ID not found in SQL".to_string().into()))
}
 
#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    // Register S3 handlers so that arbitrary S3 URIs are recognized.
    deltalake::aws::register_handlers(None);
 
    // Retrieve the S3 bucket name from the environment.
    let bucket = env::var("S3_BUCKET_NAME")
        .expect("S3_BUCKET_NAME environment variable not set");
    // Build the table URI, e.g., s3://<bucket>/delta_table.
    let s3_uri = format!("s3://{}/delta_table", bucket);
    
    let db = Database::new().await?;
    
    // For now, support a single telemetry/events table.
    db.add_project("events", &s3_uri).await?;
    
    let now = Utc::now();
    db.write("events", now, Some(now), None, Some("data1")).await?;
    
    // Execute a query; the placeholder "table" is replaced with the unique table name.
    db.query("SELECT * FROM \"table\" WHERE project_id = 'events'")
        .await?
        .show()
        .await?;
    
    // Optionally run the compaction stub.
    db.compact("events").await?;
    
    Ok(())
}
