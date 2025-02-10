use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{
    array::{StringArray, TimestampNanosecondArray},
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
use datafusion::datasource::TableProvider;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type ProjectConfigs = Arc<RwLock<HashMap<String, (String, Arc<RwLock<DeltaTable>>)>>>;

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
                // DeltaOps::try_from_uri(&table_path).await.unwrap()
                //     .create()
                //     .with_columns(schema.fields)
                //     .await?

                DeltaOps::new_in_memory()
                    .create()
                    .with_columns(struct_fields)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                // panic!("Dd")
            }
        };

        // Now insert a tuple of three elements (connection string, object store, delta table)
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
        let (.., delta_table) = self
            .project_configs
            .read()
            .unwrap()
            .get(&project_id)
            .ok_or_else(|| {
                DataFusionError::External(format!("Project ID '{}' not found", project_id).into())
            })?;
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
        let (.., delta_table) = self
            .project_configs
            .read()
            .unwrap()
            .get(project_id)
            .expect("no project_id in sql query");

        // Wrap the schema in an Arc since RecordBatch::try_new expects a SchemaRef.
        let schema = Schema::new(vec![
            Field::new("project_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("payload", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    timestamp.timestamp_nanos_opt().unwrap(),
                ])),
                // For illustration, we use the same timestamp for the optional times.
                Arc::new(TimestampNanosecondArray::from(vec![
                    start_time.map_or(timestamp.timestamp_nanos_opt().unwrap(), |t| {
                        t.timestamp_nanos_opt().unwrap()
                    }),
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    end_time.map_or(timestamp.timestamp_nanos_opt().unwrap(), |t| {
                        t.timestamp_nanos_opt().unwrap()
                    }),
                ])),
                Arc::new(StringArray::from(vec![payload.unwrap_or("")])),
            ],
        ).map_err(|e| DataFusionError::External(Box::new(e)))?;

        // let mut table = delta_table.write().unwrap();
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

    db.query("SELECT * FROM table WHERE project_id = 'project_123'")
        .await?
        .show()
        .await?;
    db.query("SELECT * FROM table WHERE project_id = 'project_456'")
        .await?
        .show()
        .await?;

    Ok(())
}
