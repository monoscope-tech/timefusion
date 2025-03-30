use std::{collections::HashMap, env, sync::Arc};

use anyhow::Result;
use datafusion::execution::context::SessionContext;
use delta_kernel::schema::StructField;
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use log::{info, warn};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use tokio::sync::RwLock;
use url::Url;

use crate::persistent_queue::OtelLogsAndSpans;

type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);

pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

pub struct Database {
    pub session_context: SessionContext,
    project_configs:     ProjectConfigs,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET environment variable not set");
        let aws_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
        let storage_uri = format!("s3://{}/?endpoint={}", bucket, aws_endpoint);
        info!("Storage URI configured: {}", storage_uri);
        unsafe {
            env::set_var("AWS_ENDPOINT", &aws_endpoint);
        }
        let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        let session_context = SessionContext::new();
        let project_configs = HashMap::new();

        let db = Self {
            session_context,
            project_configs: Arc::new(RwLock::new(project_configs)),
        };

        db.register_project("default", &storage_uri, None, None, None).await?;

        Ok(db)
    }

    pub async fn delete_all_data(&self) -> Result<()> {
        Ok(())
    }

    pub async fn insert_records(&self, records: &Vec<crate::persistent_queue::OtelLogsAndSpans>) -> Result<()> {
        // TODO: insert records doesn't need to accept a project_id as they can be read from the
        // record.
        // Records should be grouped by span, and separated into groups then inserted into the
        // correct table.
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get("default").ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", "default"))?.clone()
        };

        let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(TracingOptions::default())?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;

        // Debug logging to understand what's in the batch
        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());

        // Use the mode parameter to ensure data is written correctly with partitioning
        *table = ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_project(
        &self, project_id: &str, conn_str: &str, access_key: Option<&str>, secret_key: Option<&str>, endpoint: Option<&str>,
    ) -> Result<()> {
        let mut storage_options = StorageOptions::default();

        if let Some(key) = access_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_ACCESS_KEY_ID".to_string(), key.to_string());
        }

        if let Some(key) = secret_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_SECRET_ACCESS_KEY".to_string(), key.to_string());
        }

        if let Some(ep) = endpoint.filter(|e| !e.is_empty()) {
            storage_options.0.insert("AWS_ENDPOINT".to_string(), ep.to_string());
        }

        storage_options.0.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        let table = match DeltaTableBuilder::from_uri(&conn_str)
            .with_storage_options(storage_options.0.clone())
            .with_allow_http(true)
            .load()
            .await
        {
            Ok(table) => table,
            Err(err) => {
                warn!("table doesn't exist. creating new table. err: {:?}", err);

                let tracing_options = TracingOptions::default();

                let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(tracing_options)?;
                let vec_refs: Vec<StructField> = fields.iter().map(|arc_field| arc_field.as_ref().try_into().unwrap()).collect();

                // Create the table with project_id partitioning only for now
                // Timestamp partitioning is likely causing issues with nanosecond precision
                let delta_ops = DeltaOps::try_from_uri(&conn_str).await?;
                delta_ops
                    .create()
                    .with_columns(vec_refs)
                    .with_partition_columns(vec!["project_id".to_string(), "timestamp".to_string()])
                    .with_storage_options(storage_options.0.clone())
                    .await?
            }
        };

        self.session_context.register_table("otel_logs_and_spans", Arc::new(table.clone()))?;

        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str.to_string(), storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use datafusion::arrow::array::{Array, StringArray};
    use dotenv::dotenv;

    use super::*;

    #[tokio::test]
    async fn test_database_query() -> Result<()> {
        env_logger::builder()
            .is_test(true) // Makes output more compact for tests
            .init();

        dotenv().ok();
        log::info!("Running database query test");

        // Create a new database with default project
        let db = Database::new().await?;
        let ctx = db.session_context.clone();
        log::info!("Database initialized");

        // Create fixed timestamps for testing
        let timestamp1 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 10, 0).unwrap();

        // Create sample records
        let span1 = OtelLogsAndSpans {
            project_id: "test_project".to_string(),
            timestamp: timestamp1,
            observed_timestamp: timestamp1,
            id: "span1".to_string(),
            name: "test_span_1".to_string(),
            context___trace_id: "trace1".to_string(),
            context___span_id: "span1".to_string(),
            start_time: timestamp1,
            duration: 100_000_000,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };

        let span2 = OtelLogsAndSpans {
            project_id: "test_project".to_string(),
            timestamp: timestamp2,
            observed_timestamp: timestamp2,
            id: "span2".to_string(),
            name: "test_span_2".to_string(),
            context___trace_id: "trace2".to_string(),
            context___span_id: "span2".to_string(),
            start_time: timestamp2,
            duration: 200_000_000,
            status_code: Some("ERROR".to_string()),
            level: Some("ERROR".to_string()),
            status_message: Some("Error occurred".to_string()),
            ..Default::default()
        };

        let records = vec![span1, span2];
        let count_df = ctx.sql("DELETE FROM otel_logs_and_spans").await?;

        log::info!("insert record");
        // Insert records
        db.insert_records(&records).await?;
        log::info!("after insert record");

        // Use the session context to run an SQL query

        // First, check what data exists in the table
        let count_df = ctx.sql("SELECT * FROM otel_logs_and_spans").await?;
        let count_batches = count_df.collect().await?;
        assert_eq!(count_batches.len(), 2);

        // let df = ctx.sql("SELECT name, status_code, level FROM otel_logs_and_spans ORDER BY name").await?;
        // // Get the results as batches
        // let batches = df.collect().await?;
        // println!(
        //     "Query returned {} batches with schema: {:?}",
        //     batches.len(),
        //     batches.first().map(|b| b.schema())
        // );
        //
        // // Assert the schema
        // assert_eq!(batches[0].schema().field(0).name(), "name");
        // assert_eq!(batches[0].schema().field(1).name(), "status_code");
        // assert_eq!(batches[0].schema().field(2).name(), "level");
        //
        // // Verify the number of records
        // let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        // assert_eq!(total_rows, 2);
        //
        // // Convert columns to StringArray for easier access
        // let name_col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        // let status_code_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        // let level_col = batches[0].column(2).as_any().downcast_ref::<StringArray>().unwrap();
        //
        // // Check values in the first row
        // assert_eq!(name_col.value(0), "test_span_1");
        // assert_eq!(status_code_col.value(0), "OK");
        // assert_eq!(level_col.value(0), "INFO");
        //
        // // Check values in the second row
        // assert_eq!(name_col.value(1), "test_span_2");
        // assert_eq!(status_code_col.value(1), "ERROR");
        // assert_eq!(level_col.value(1), "ERROR");
        //
        // // Run a more complex query with filtering
        // let df = ctx.sql("SELECT name, level, status_code, status_message FROM otel_logs_and_spans WHERE project_id = 'test_project' and timestamp > '2020-12-01' and level = 'ERROR'").await?;
        // let batches = df.collect().await?;
        //
        // // Verify the filtered result - should only have 1 row
        // let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        // assert_eq!(total_rows, 1);
        //
        // // Convert columns to StringArray for easier access
        // let name_col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        // let level_col = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        // let status_code_col = batches[0].column(2).as_any().downcast_ref::<StringArray>().unwrap();
        // let status_message_col = batches[0].column(3).as_any().downcast_ref::<StringArray>().unwrap();
        //
        // assert_eq!(name_col.value(0), "test_span_2");
        // assert_eq!(level_col.value(0), "ERROR");
        // assert_eq!(status_code_col.value(0), "ERROR");
        // assert_eq!(status_message_col.value(0), "Error occurred");

        Ok(())
    }
}
