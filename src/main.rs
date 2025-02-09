use arrow::{array::{StringArray, TimestampNanosecondArray}, datatypes::{DataType, Field, Schema}};
use arrow::record_batch::RecordBatch;
use arrow_schema::TimeUnit;
use datafusion::arrow::array::{StringBuilder, TimestampNanosecondBuilder};
use datafusion::common::DataFusionError;
use datafusion::prelude::*;
use deltalake::DeltaTable;
use deltalake::ObjectStore;
use deltalake::delta_datafusion::DeltaTableProvider;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

type ProjectConfigs =
    Arc<RwLock<HashMap<String, (String, Arc<dyn ObjectStore>, Arc<RwLock<DeltaTable>>)>>>;

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

        // Attempt to open the Delta table at the given path.
        // If it does not exist, create it with a predefined schema.
        let delta_table = match DeltaTable::open(&table_path).await {
            Ok(table) => table,
            Err(_) => {
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
                DeltaTable::create_table(&table_path, schema).await?
            }
        };

        // Store the mapping of project_id to (connection string, delta table) for later queries.
        self.project_configs.write().unwrap().insert(
            project_id.to_string(),
            (
                s3_connection_string.to_string(),
                Arc::new(RwLock::new(delta_table)),
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
                DataFusionError::External(format!("Project ID '{}' not found", project_id))
            })?;

        self.ctx
            .register_table("table", DeltaTableProvider::new(delta_table.clone()))?;
        self.ctx.sql(sql).await
    }

    async fn write(
        &self,
        project_id: &str,
        timestamp: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        payload: Option<&str>,
    ) -> Result<(), DataFusionError> {
        let (.., delta_table) = self
            .project_configs
            .read()
            .unwrap()
            .get(project_id).expect("no project_id in sql query");

        // delta_table.read().unwrap().schema().unwrap()    
        let mut batch = RecordBatch::try_new(
            delta_table.read().unwrap().schema().unwrap().clone(),
            vec![
                Arc::new(StringArray::from(vec![project_id])),
                Arc::new(TimestampNanosecondArray::from(vec![timestamp])),
                Arc::new(TimestampNanosecondArray::from(vec![timestamp])),
                Arc::new(TimestampNanosecondArray::from(vec![timestamp])),
                Arc::new(StringArray::from(vec![project_id])),
            ],
        )?;

        delta_table
            .write()
            .unwrap()
            .update_and_commit_batch(batch, ParquetWriteOptions::default())
            .await?;
        Ok(())
    }
}

fn extract_project_id_from_sql(sql: &str) -> Result<String, DataFusionError> {
    // Simplified example, use a proper SQL parser for production.
    sql.to_lowercase()
        .find("where project_id = '")
        .map(|start| {
            let end = sql[start + 20..].find("'").unwrap();
            sql[start + 20..start + 20 + end].to_string()
        })
        .ok_or_else(|| DataFusionError::External("Project ID not found in SQL".to_string()))
}

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let db = Database::new().await?;
    db.add_project("project_123", "file:///tmp/delta_123")
        .await?;
    db.add_project("project_456", "file:///tmp/delta_456")
        .await?;

    let now = Utc::now();
    db.write("project_123", now, Some(now), None, Some("data1")).await?;
    db.write("project_456", now, None, Some(now), Some("data2")).await?;

    db.query("SELECT * FROM table WHERE project_id = 'project_123'").await?.show().await?;
    db.query("SELECT * FROM table WHERE project_id = 'project_456'").await?.show().await?;

    Ok(())
}
