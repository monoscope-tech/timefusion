use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Load each sub-config separately to avoid #[serde(flatten)] issues with envy
    // See: https://github.com/softprops/envy/issues/26
    Ok(AppConfig {
        aws: envy::from_env()?,
        core: envy::from_env()?,
        buffer: envy::from_env()?,
        cache: envy::from_env()?,
        parquet: envy::from_env()?,
        maintenance: envy::from_env()?,
        memory: envy::from_env()?,
        telemetry: envy::from_env()?,
    })
}

/// Initialize global config from environment (for production use).
pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let config = load_config_from_env()?;
    let _ = CONFIG.set(config);
    Ok(CONFIG.get().unwrap())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}

// Macro to generate const default functions for serde
macro_rules! const_default {
    ($name:ident: bool = $val:expr) => { fn $name() -> bool { $val } };
    ($name:ident: u64 = $val:expr) => { fn $name() -> u64 { $val } };
    ($name:ident: u16 = $val:expr) => { fn $name() -> u16 { $val } };
    ($name:ident: i32 = $val:expr) => { fn $name() -> i32 { $val } };
    ($name:ident: i64 = $val:expr) => { fn $name() -> i64 { $val } };
    ($name:ident: usize = $val:expr) => { fn $name() -> usize { $val } };
    ($name:ident: f64 = $val:expr) => { fn $name() -> f64 { $val } };
    ($name:ident: String = $val:expr) => { fn $name() -> String { $val.into() } };
    ($name:ident: PathBuf = $val:expr) => { fn $name() -> PathBuf { PathBuf::from($val) } };
}

// All default value functions using the macro
const_default!(d_true: bool = true);
const_default!(d_s3_endpoint: String = "https://s3.amazonaws.com");
const_default!(d_wal_dir: PathBuf = "/var/lib/timefusion/wal");
const_default!(d_pgwire_port: u16 = 5432);
const_default!(d_table_prefix: String = "timefusion");
const_default!(d_batch_queue_capacity: usize = 100_000_000);
const_default!(d_flush_interval: u64 = 600);
const_default!(d_retention_mins: u64 = 70);
const_default!(d_eviction_interval: u64 = 60);
const_default!(d_buffer_max_memory: usize = 4096);
const_default!(d_shutdown_timeout: u64 = 5);
const_default!(d_wal_corruption_threshold: usize = 10);
const_default!(d_foyer_memory_mb: usize = 512);
const_default!(d_foyer_disk_gb: usize = 100);
const_default!(d_foyer_ttl: u64 = 604_800); // 7 days
const_default!(d_cache_dir: PathBuf = "/tmp/timefusion_cache");
const_default!(d_foyer_shards: usize = 8);
const_default!(d_foyer_file_size_mb: usize = 32);
const_default!(d_foyer_stats: String = "true");
const_default!(d_metadata_size_hint: usize = 1_048_576);
const_default!(d_metadata_memory_mb: usize = 512);
const_default!(d_metadata_disk_gb: usize = 5);
const_default!(d_metadata_shards: usize = 4);
const_default!(d_page_rows: usize = 20_000);
const_default!(d_zstd_level: i32 = 3);
const_default!(d_row_group_size: usize = 134_217_728); // 128MB
const_default!(d_checkpoint_interval: u64 = 10);
const_default!(d_optimize_target: i64 = 128 * 1024 * 1024);
const_default!(d_stats_cache_size: usize = 50);
const_default!(d_vacuum_retention: u64 = 72);
const_default!(d_light_schedule: String = "0 */5 * * * *");
const_default!(d_optimize_schedule: String = "0 */30 * * * *");
const_default!(d_vacuum_schedule: String = "0 0 2 * * *");
const_default!(d_mem_gb: usize = 8);
const_default!(d_mem_fraction: f64 = 0.9);
const_default!(d_otlp_endpoint: String = "http://localhost:4317");
const_default!(d_service_name: String = "timefusion");
fn d_service_version() -> String { env!("CARGO_PKG_VERSION").into() }

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub aws: AwsConfig,
    #[serde(flatten)]
    pub core: CoreConfig,
    #[serde(flatten)]
    pub buffer: BufferConfig,
    #[serde(flatten)]
    pub cache: CacheConfig,
    #[serde(flatten)]
    pub parquet: ParquetConfig,
    #[serde(flatten)]
    pub maintenance: MaintenanceConfig,
    #[serde(flatten)]
    pub memory: MemoryConfig,
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region: Option<String>,
    #[serde(default = "d_s3_endpoint")]
    pub aws_s3_endpoint: String,
    #[serde(default)]
    pub aws_s3_bucket: Option<String>,
    #[serde(default)]
    pub aws_allow_http: Option<String>,
    #[serde(flatten)]
    pub dynamodb: DynamoDbConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DynamoDbConfig {
    #[serde(default)]
    pub aws_s3_locking_provider: Option<String>,
    #[serde(default)]
    pub delta_dynamo_table_name: Option<String>,
    #[serde(default)]
    pub aws_access_key_id_dynamodb: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key_dynamodb: Option<String>,
    #[serde(default)]
    pub aws_region_dynamodb: Option<String>,
    #[serde(default)]
    pub aws_endpoint_url_dynamodb: Option<String>,
}

impl AwsConfig {
    pub fn is_dynamodb_locking_enabled(&self) -> bool {
        self.dynamodb.aws_s3_locking_provider.as_deref() == Some("dynamodb")
    }

    pub fn build_storage_options(&self, endpoint_override: Option<&str>) -> HashMap<String, String> {
        let mut opts = HashMap::new();
        if let Some(ref key) = self.aws_access_key_id {
            opts.insert("aws_access_key_id".into(), key.clone());
        }
        if let Some(ref secret) = self.aws_secret_access_key {
            opts.insert("aws_secret_access_key".into(), secret.clone());
        }
        if let Some(ref region) = self.aws_default_region {
            opts.insert("aws_region".into(), region.clone());
        }
        opts.insert("aws_endpoint".into(), endpoint_override.unwrap_or(&self.aws_s3_endpoint).to_string());

        if self.is_dynamodb_locking_enabled() {
            opts.insert("aws_s3_locking_provider".into(), "dynamodb".into());
            if let Some(ref t) = self.dynamodb.delta_dynamo_table_name {
                opts.insert("delta_dynamo_table_name".into(), t.clone());
            }
            if let Some(ref k) = self.dynamodb.aws_access_key_id_dynamodb {
                opts.insert("aws_access_key_id_dynamodb".into(), k.clone());
            }
            if let Some(ref s) = self.dynamodb.aws_secret_access_key_dynamodb {
                opts.insert("aws_secret_access_key_dynamodb".into(), s.clone());
            }
            if let Some(ref r) = self.dynamodb.aws_region_dynamodb {
                opts.insert("aws_region_dynamodb".into(), r.clone());
            }
            if let Some(ref e) = self.dynamodb.aws_endpoint_url_dynamodb {
                opts.insert("aws_endpoint_url_dynamodb".into(), e.clone());
            }
        }
        opts
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde(default = "d_wal_dir")]
    pub walrus_data_dir: PathBuf,
    #[serde(default = "d_pgwire_port")]
    pub pgwire_port: u16,
    #[serde(default = "d_table_prefix")]
    pub timefusion_table_prefix: String,
    #[serde(default)]
    pub timefusion_config_database_url: Option<String>,
    #[serde(default)]
    pub enable_batch_queue: bool,
    #[serde(default = "d_batch_queue_capacity")]
    pub timefusion_batch_queue_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "d_flush_interval")]
    pub timefusion_flush_interval_secs: u64,
    #[serde(default = "d_retention_mins")]
    pub timefusion_buffer_retention_mins: u64,
    #[serde(default = "d_eviction_interval")]
    pub timefusion_eviction_interval_secs: u64,
    #[serde(default = "d_buffer_max_memory")]
    pub timefusion_buffer_max_memory_mb: usize,
    #[serde(default = "d_shutdown_timeout")]
    pub timefusion_shutdown_timeout_secs: u64,
    #[serde(default = "d_wal_corruption_threshold")]
    pub timefusion_wal_corruption_threshold: usize,
}

impl BufferConfig {
    pub fn flush_interval_secs(&self) -> u64 { self.timefusion_flush_interval_secs.max(1) }
    pub fn retention_mins(&self) -> u64 { self.timefusion_buffer_retention_mins.max(1) }
    pub fn eviction_interval_secs(&self) -> u64 { self.timefusion_eviction_interval_secs.max(1) }
    pub fn max_memory_mb(&self) -> usize { self.timefusion_buffer_max_memory_mb.max(64) }
    pub fn wal_corruption_threshold(&self) -> usize { self.timefusion_wal_corruption_threshold }

    pub fn compute_shutdown_timeout(&self, current_memory_mb: usize) -> Duration {
        let secs = self.timefusion_shutdown_timeout_secs.max(1) + (current_memory_mb / 100) as u64;
        Duration::from_secs(secs.min(300))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "d_foyer_memory_mb")]
    pub timefusion_foyer_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb: Option<usize>,
    #[serde(default = "d_foyer_disk_gb")]
    pub timefusion_foyer_disk_gb: usize,
    #[serde(default = "d_foyer_ttl")]
    pub timefusion_foyer_ttl_seconds: u64,
    #[serde(default = "d_cache_dir")]
    pub timefusion_foyer_cache_dir: PathBuf,
    #[serde(default = "d_foyer_shards")]
    pub timefusion_foyer_shards: usize,
    #[serde(default = "d_foyer_file_size_mb")]
    pub timefusion_foyer_file_size_mb: usize,
    #[serde(default = "d_foyer_stats")]
    pub timefusion_foyer_stats: String,
    #[serde(default = "d_metadata_size_hint")]
    pub timefusion_parquet_metadata_size_hint: usize,
    #[serde(default = "d_metadata_memory_mb")]
    pub timefusion_foyer_metadata_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb: Option<usize>,
    #[serde(default = "d_metadata_disk_gb")]
    pub timefusion_foyer_metadata_disk_gb: usize,
    #[serde(default = "d_metadata_shards")]
    pub timefusion_foyer_metadata_shards: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
}

impl CacheConfig {
    pub fn is_disabled(&self) -> bool { self.timefusion_foyer_disabled }
    pub fn ttl(&self) -> Duration { Duration::from_secs(self.timefusion_foyer_ttl_seconds) }
    pub fn stats_enabled(&self) -> bool { self.timefusion_foyer_stats.eq_ignore_ascii_case("true") }
    pub fn memory_size_bytes(&self) -> usize { self.timefusion_foyer_memory_mb * 1024 * 1024 }
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map_or(self.timefusion_foyer_disk_gb * 1024 * 1024 * 1024, |mb| mb * 1024 * 1024)
    }
    pub fn file_size_bytes(&self) -> usize { self.timefusion_foyer_file_size_mb * 1024 * 1024 }
    pub fn metadata_memory_size_bytes(&self) -> usize { self.timefusion_foyer_metadata_memory_mb * 1024 * 1024 }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb.map_or(self.timefusion_foyer_metadata_disk_gb * 1024 * 1024 * 1024, |mb| mb * 1024 * 1024)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "d_page_rows")]
    pub timefusion_page_row_count_limit: usize,
    #[serde(default = "d_zstd_level")]
    pub timefusion_zstd_compression_level: i32,
    #[serde(default = "d_row_group_size")]
    pub timefusion_max_row_group_size: usize,
    #[serde(default = "d_checkpoint_interval")]
    pub timefusion_checkpoint_interval: u64,
    #[serde(default = "d_optimize_target")]
    pub timefusion_optimize_target_size: i64,
    #[serde(default = "d_stats_cache_size")]
    pub timefusion_stats_cache_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    #[serde(default = "d_vacuum_retention")]
    pub timefusion_vacuum_retention_hours: u64,
    #[serde(default = "d_light_schedule")]
    pub timefusion_light_optimize_schedule: String,
    #[serde(default = "d_optimize_schedule")]
    pub timefusion_optimize_schedule: String,
    #[serde(default = "d_vacuum_schedule")]
    pub timefusion_vacuum_schedule: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "d_mem_gb")]
    pub timefusion_memory_limit_gb: usize,
    #[serde(default = "d_mem_fraction")]
    pub timefusion_memory_fraction: f64,
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default = "d_true")]
    pub timefusion_tracing_record_metrics: bool,
}

impl MemoryConfig {
    pub fn memory_limit_bytes(&self) -> usize { self.timefusion_memory_limit_gb * 1024 * 1024 * 1024 }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "d_otlp_endpoint")]
    pub otel_exporter_otlp_endpoint: String,
    #[serde(default = "d_service_name")]
    pub otel_service_name: String,
    #[serde(default = "d_service_version")]
    pub otel_service_version: String,
    #[serde(default)]
    pub log_format: Option<String>,
}

impl TelemetryConfig {
    pub fn is_json_logging(&self) -> bool { self.log_format.as_deref() == Some("json") }
}

impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>())
            .expect("Default config should always succeed with serde defaults")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.core.pgwire_port, 5432);
        assert_eq!(config.buffer.timefusion_flush_interval_secs, 600);
        assert_eq!(config.cache.timefusion_foyer_memory_mb, 512);
    }

    #[test]
    fn test_buffer_min_enforcement() {
        let mut config = AppConfig::default();
        config.buffer.timefusion_buffer_max_memory_mb = 10;
        assert_eq!(config.buffer.max_memory_mb(), 64);
    }

    #[test]
    fn test_cache_size_calculations() {
        let mut config = AppConfig::default();
        config.cache.timefusion_foyer_memory_mb = 256;
        config.cache.timefusion_foyer_disk_mb = Some(1024);
        assert_eq!(config.cache.memory_size_bytes(), 256 * 1024 * 1024);
        assert_eq!(config.cache.disk_size_bytes(), 1024 * 1024 * 1024);
    }
}
