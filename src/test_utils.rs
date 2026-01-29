/// Initialize tracing for tests. Call at start of test functions.
/// Uses try_init() so multiple calls are safe.
pub fn init_test_logging() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_test_writer()
        .try_init();
}

pub mod test_helpers {
    use crate::config::AppConfig;
    use crate::schema_loader::get_default_schema;
    use arrow_json::ReaderBuilder;
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BufferMode {
        Enabled,
        FlushImmediately,
    }

    pub struct TestConfigBuilder {
        test_name: String,
        buffer_mode: BufferMode,
    }

    impl TestConfigBuilder {
        pub fn new(test_name: &str) -> Self {
            Self {
                test_name: test_name.to_string(),
                buffer_mode: BufferMode::Enabled,
            }
        }

        pub fn with_buffer_mode(mut self, mode: BufferMode) -> Self {
            self.buffer_mode = mode;
            self
        }

        pub fn build(self) -> Arc<AppConfig> {
            let uuid = uuid::Uuid::new_v4().to_string()[..8].to_string();
            let mut cfg = AppConfig::default();
            cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
            cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
            cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
            cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
            cfg.aws.aws_default_region = Some("us-east-1".to_string());
            cfg.aws.aws_allow_http = Some("true".to_string());
            cfg.core.timefusion_table_prefix = format!("test-{}-{}", self.test_name, uuid);
            cfg.core.walrus_data_dir = PathBuf::from(format!("/tmp/walrus-{}-{}", self.test_name, uuid));
            cfg.cache.timefusion_foyer_disabled = true;
            cfg.buffer.timefusion_flush_immediately = self.buffer_mode == BufferMode::FlushImmediately;
            Arc::new(cfg)
        }
    }

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        let target_schema = get_default_schema().schema_ref();

        // Create a schema for reading JSON with Utf8 (which arrow-json produces)
        let json_read_schema = Arc::new(Schema::new(
            target_schema
                .fields()
                .iter()
                .map(|f| {
                    let data_type = match f.data_type() {
                        DataType::Utf8View => DataType::Utf8,
                        DataType::List(inner) if inner.data_type() == &DataType::Utf8View => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        other => other.clone(),
                    };
                    Field::new(f.name(), data_type, f.is_nullable())
                })
                .collect::<Vec<_>>(),
        ));

        let json_data = records.into_iter().map(|v| v.to_string()).collect::<Vec<_>>().join("\n");

        let batch = ReaderBuilder::new(json_read_schema)
            .build(std::io::Cursor::new(json_data.as_bytes()))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))??;

        // Cast columns to target schema types (Utf8 -> Utf8View)
        let columns: Vec<Arc<dyn datafusion::arrow::array::Array>> = batch
            .columns()
            .iter()
            .zip(target_schema.fields())
            .map(|(col, field)| {
                if col.data_type() != field.data_type() {
                    cast(col, field.data_type()).unwrap_or_else(|_| col.clone())
                } else {
                    col.clone()
                }
            })
            .collect();

        Ok(RecordBatch::try_new(target_schema, columns)?)
    }

    pub fn create_default_record() -> HashMap<String, Value> {
        get_default_schema()
            .fields
            .iter()
            .map(|field| {
                let value = if field.data_type == "List(Utf8)" { json!([]) } else { Value::Null };
                (field.name.clone(), value)
            })
            .collect()
    }

    pub fn test_span(id: &str, name: &str, project_id: &str) -> Value {
        json!({
            "timestamp": chrono::Utc::now().timestamp_micros(),
            "id": id,
            "name": name,
            "project_id": project_id,
            "date": chrono::Utc::now().date_naive().to_string(),
            "hashes": [],
            "summary": vec![format!("Test span: {}", name)]
        })
    }
}
