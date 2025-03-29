use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use arrow_schema::Schema;
use datafusion::execution::context::SessionContext;
use delta_kernel::schema::StructField;
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use log::warn;
use serde_arrow::schema::{SchemaLike, TracingOptions};
use tokio::sync::RwLock;

use crate::persistent_queue::OtelLogsAndSpans;

type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);

pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

pub struct Database {
    pub session_context: SessionContext,
    project_configs:     ProjectConfigs,
}

impl Database {
    pub async fn new(storage_uri: &str) -> Result<Self> {
        let session_context = SessionContext::new();
        let mut project_configs = HashMap::new();

        let default_options = StorageOptions::default();
        let table = DeltaTableBuilder::from_uri(storage_uri).with_allow_http(true).with_storage_options(default_options.0.clone()).build()?;
        session_context.register_table("otel_logs_and_spans", Arc::new(table.clone()))?;
        project_configs.insert("default".to_string(), (storage_uri.to_string(), default_options, Arc::new(RwLock::new(table))));

        Ok(Self {
            session_context,
            project_configs: Arc::new(RwLock::new(project_configs)),
        })
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

        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());
        *table = ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_project(&self, project_id: &str, bucket: &str, access_key: &str, secret_key: &str, endpoint: &str) -> Result<()> {
        let conn_str = format!("s3://{}/otel_logs_and_spans", bucket);
        let mut storage_options = StorageOptions::default();
        storage_options.0.insert("AWS_ACCESS_KEY_ID".to_string(), access_key.to_string());
        storage_options.0.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key.to_string());
        storage_options.0.insert("AWS_ENDPOINT".to_string(), endpoint.to_string());
        storage_options.0.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        let table = match DeltaTableBuilder::from_uri(&conn_str)
            .with_storage_options(storage_options.0.clone())
            .with_allow_http(true)
            .load()
            .await
        {
            Ok(table) => table,
            Err(err) => {
                warn!("table doesn't exist. creating new ond. err: {}", err);

                let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(TracingOptions::default())?;
                let vec_refs: Vec<StructField> = fields.iter().map(|arc_field| arc_field.as_ref().try_into().unwrap()).collect();

                // Create the table with partitioning for project_id and timestamp
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
        configs.insert(project_id.to_string(), (conn_str, storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}
