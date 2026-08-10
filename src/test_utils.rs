/// Initialize tracing for tests. Call at start of test functions.
/// Uses try_init() so multiple calls are safe.
pub fn init_test_logging() {
    use tracing_subscriber::{EnvFilter, filter::LevelFilter};
    let _ = tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into())).with_test_writer().try_init();
}

pub mod test_helpers {
    use std::{path::PathBuf, sync::Arc};

    use arrow_json::ReaderBuilder;
    use datafusion::arrow::{
        compute::cast,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use serde_json::{Value, json};

    use crate::{config::AppConfig, schema_loader::get_default_schema};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum BufferMode {
        Enabled,
        FlushImmediately,
    }

    pub struct TestConfigBuilder {
        test_name: String,
        buffer_mode: BufferMode,
        rollups: bool,
    }

    impl TestConfigBuilder {
        pub fn new(test_name: &str) -> Self {
            Self { test_name: test_name.to_string(), buffer_mode: BufferMode::Enabled, rollups: false }
        }

        pub fn with_buffer_mode(mut self, mode: BufferMode) -> Self {
            self.buffer_mode = mode;
            self
        }

        /// Turn on the default-off rollup build and read gates.
        pub fn with_rollups(mut self) -> Self {
            self.rollups = true;
            self
        }

        pub fn build(self) -> Arc<AppConfig> {
            let id = format!("{}-{}", self.test_name, &uuid::Uuid::new_v4().to_string()[..8]);
            let mut cfg = minio_base_config(&id, &format!("/tmp/timefusion-{id}"));
            cfg.buffer.timefusion_flush_immediately = self.buffer_mode == BufferMode::FlushImmediately;
            cfg.maintenance.timefusion_rollup_enabled = self.rollups;
            cfg.maintenance.timefusion_rollup_read_enabled = self.rollups;
            Arc::new(cfg)
        }
    }

    /// Shared MinIO + foyer-disabled config keyed by an explicit table id / data dir.
    fn minio_base_config(table_id: &str, data_dir: &str) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
        cfg.core.timefusion_table_prefix = format!("test-{table_id}");
        cfg.core.timefusion_data_dir = PathBuf::from(data_dir);
        cfg.cache.timefusion_foyer_disabled = true;
        // Dwell off: suite tests assert "sealed => next tick flushes"; the
        // dwell gate has its own dedicated unit tests.
        cfg.buffer.timefusion_flush_dwell_secs = 0;
        cfg
    }

    /// MinIO-backed config with an explicit table id and data dir. Shared by the
    /// integration tests that manage their own per-test id/path.
    pub fn minio_test_config(table_id: &str, data_dir: &str) -> Arc<AppConfig> {
        Arc::new(minio_base_config(table_id, data_dir))
    }

    /// Physical row count from the Delta log's `num_records` stats, summed over
    /// all active files. Bypasses the routed scan path entirely — unlike a
    /// `query_delta_only` COUNT, it is NOT collapsed by the read-side `DedupExec`,
    /// so it reflects on-disk duplicates (what the dedup *sweep* tests assert).
    pub async fn delta_physical_row_count(table_ref: &tokio::sync::RwLock<deltalake::DeltaTable>) -> anyhow::Result<i64> {
        use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
        let guard = table_ref.read().await;
        let batch = guard.snapshot()?.add_actions_table(true)?;
        let arr = batch.column_by_name("num_records").ok_or_else(|| anyhow::anyhow!("add_actions_table missing num_records"))?.as_primitive::<Int64Type>();
        Ok(arr.iter().flatten().sum())
    }

    /// Build a BufferedWriteLayer for tests/benches without repeating the registry boilerplate.
    pub fn test_layer(cfg: Arc<AppConfig>) -> anyhow::Result<crate::buffered_write_layer::BufferedWriteLayer> {
        crate::buffered_write_layer::BufferedWriteLayer::with_config(cfg, crate::functions::function_registry()?)
    }

    /// Collect a string column out of a layer query result as `Vec<String>`,
    /// casting through Utf8 so Utf8View/Utf8 storage both work. Nulls are
    /// skipped, so a shorter-than-expected result means null cells — assert on
    /// `.len()` when that matters.
    pub fn query_col_strings(layer: &crate::buffered_write_layer::BufferedWriteLayer, project: &str, table: &str, col: &str) -> Vec<String> {
        use datafusion::arrow::array::AsArray;
        layer
            .query(project, table, &[])
            .unwrap()
            .iter()
            .flat_map(|b| {
                let arr = cast(b.column_by_name(col).unwrap(), &DataType::Utf8).unwrap();
                arr.as_string::<i32>().iter().flatten().map(str::to_string).collect::<Vec<_>>()
            })
            .collect()
    }

    pub fn json_to_batch(records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        json_to_batch_for(&get_default_schema().table_name, records)
    }

    /// `json_to_batch` against any registered table's schema.
    pub fn json_to_batch_for(table: &str, records: Vec<Value>) -> anyhow::Result<RecordBatch> {
        let target_schema = crate::schema_loader::get_schema(table).ok_or_else(|| anyhow::anyhow!("unknown table `{table}`"))?.schema_ref();

        // arrow-json only produces Utf8, so read into a Utf8-flavoured mirror of the target schema and cast back.
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

        let json_data = records.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n");

        let batch = ReaderBuilder::new(json_read_schema)
            .build(std::io::Cursor::new(json_data.as_bytes()))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("Failed to read batch"))??;

        let columns =
            batch.columns().iter().zip(target_schema.fields()).map(|(col, field)| cast(col, field.data_type()).unwrap_or_else(|_| col.clone())).collect();

        Ok(RecordBatch::try_new(target_schema, columns)?)
    }

    pub fn test_span(id: &str, name: &str, project_id: &str) -> Value {
        test_span_ts(id, name, project_id, chrono::Utc::now().timestamp_micros())
    }

    /// Like `test_span` but with an explicit timestamp, for tests that need
    /// rows to land in a specific MemBuffer bucket.
    pub fn test_span_ts(id: &str, name: &str, project_id: &str, ts_micros: i64) -> Value {
        let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros).unwrap_or_else(chrono::Utc::now).date_naive().to_string();
        json!({
            "timestamp": ts_micros,
            "id": id,
            "name": name,
            "project_id": project_id,
            "date": date,
            "hashes": [],
            "summary": [format!("Test span: {name}")]
        })
    }

    /// Read a string cell from any String/LargeString/StringView array; panics on other types.
    pub fn array_get_str(arr: &dyn datafusion::arrow::array::Array, idx: usize) -> String {
        use datafusion::arrow::array::AsArray;
        match arr.data_type() {
            DataType::Utf8View => arr.as_string_view().value(idx),
            DataType::Utf8 => arr.as_string::<i32>().value(idx),
            DataType::LargeUtf8 => arr.as_string::<i64>().value(idx),
            dt => panic!("expected string array but got {dt:?}"),
        }
        .to_string()
    }
}
