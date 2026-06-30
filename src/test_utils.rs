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
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

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
            let id = format!("{}-{}", self.test_name, &uuid::Uuid::new_v4().to_string()[..8]);
            let mut cfg = minio_base_config(&id, &format!("/tmp/timefusion-{id}"));
            cfg.buffer.timefusion_flush_immediately = self.buffer_mode == BufferMode::FlushImmediately;
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
        cfg
    }

    /// MinIO-backed config with an explicit table id and data dir. Shared by the
    /// integration tests that manage their own per-test id/path.
    pub fn minio_test_config(table_id: &str, data_dir: &str) -> Arc<AppConfig> {
        Arc::new(minio_base_config(table_id, data_dir))
    }

    /// Point walrus-rust at the test's data dir, restoring the prior value on
    /// drop — including on panic, so a failed `#[serial]` test can't leak the
    /// var into the next one. SAFETY: `set_var` is racy if another thread
    /// reads the env concurrently; callers must hold `#[serial]` — enforced
    /// at runtime by the held-flag assert below, so a forgotten `#[serial]`
    /// fails loudly instead of racing silently.
    pub fn walrus_env_guard(dir: &std::path::Path) -> impl Drop {
        use std::sync::atomic::{AtomicBool, Ordering};
        static HELD: AtomicBool = AtomicBool::new(false);
        // prev read BEFORE taking the flag, and the guard constructed BEFORE
        // set_var: nothing fallible runs while HELD is set but unguarded, so
        // no panic path can leave HELD stuck and poison later callers with a
        // misleading "already held" assert.
        let prev = std::env::var_os("WALRUS_DATA_DIR");
        assert!(
            !HELD.swap(true, Ordering::Acquire),
            "walrus_env_guard already held by another test — add #[serial] to the caller"
        );
        let guard = scopeguard::guard(prev, |prev| {
            match prev {
                Some(v) => unsafe { std::env::set_var("WALRUS_DATA_DIR", v) },
                None => unsafe { std::env::remove_var("WALRUS_DATA_DIR") },
            }
            HELD.store(false, Ordering::Release);
        });
        unsafe { std::env::set_var("WALRUS_DATA_DIR", dir) };
        guard
    }

    /// Physical row count from the Delta log's `num_records` stats, summed over
    /// all active files. Bypasses the routed scan path entirely — unlike a
    /// `query_delta_only` COUNT, it is NOT collapsed by the read-side `DedupExec`,
    /// so it reflects on-disk duplicates (what the dedup *sweep* tests assert).
    pub async fn delta_physical_row_count(table_ref: &Arc<tokio::sync::RwLock<deltalake::DeltaTable>>) -> anyhow::Result<i64> {
        use datafusion::arrow::{
            array::{Array, AsArray},
            datatypes::Int64Type,
        };
        let guard = table_ref.read().await;
        let batch = guard.snapshot()?.add_actions_table(true)?;
        let arr = batch
            .column_by_name("num_records")
            .ok_or_else(|| anyhow::anyhow!("add_actions_table missing num_records"))?
            .as_primitive::<Int64Type>();
        Ok((0..arr.len()).filter(|&i| !arr.is_null(i)).map(|i| arr.value(i)).sum())
    }

    /// Build a BufferedWriteLayer for tests/benches without repeating the registry boilerplate.
    pub fn test_layer(cfg: Arc<AppConfig>) -> anyhow::Result<crate::buffered_write_layer::BufferedWriteLayer> {
        crate::buffered_write_layer::BufferedWriteLayer::with_config(cfg, crate::functions::function_registry()?)
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
        test_span_ts(id, name, project_id, chrono::Utc::now().timestamp_micros())
    }

    /// Like `test_span` but with an explicit timestamp, for tests that need
    /// rows to land in a specific MemBuffer bucket.
    pub fn test_span_ts(id: &str, name: &str, project_id: &str, ts_micros: i64) -> Value {
        let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts_micros)
            .unwrap_or_else(chrono::Utc::now)
            .date_naive()
            .to_string();
        json!({
            "timestamp": ts_micros,
            "id": id,
            "name": name,
            "project_id": project_id,
            "date": date,
            "hashes": [],
            "summary": vec![format!("Test span: {}", name)]
        })
    }

    /// Read a string cell from any String/LargeString/StringView array; panics on other types.
    pub fn array_get_str(arr: &dyn datafusion::arrow::array::Array, idx: usize) -> String {
        use datafusion::arrow::array::{LargeStringArray, StringArray, StringViewArray};
        let any = arr.as_any();
        if let Some(a) = any.downcast_ref::<StringViewArray>() {
            a.value(idx).to_string()
        } else if let Some(a) = any.downcast_ref::<StringArray>() {
            a.value(idx).to_string()
        } else if let Some(a) = any.downcast_ref::<LargeStringArray>() {
            a.value(idx).to_string()
        } else {
            panic!("Expected string array but got {:?}", arr.data_type());
        }
    }
}
