use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let _ = CONFIG.set(envy::from_env()?);
    Ok(CONFIG.get().unwrap())
}

pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized")
}

fn default_true() -> bool {
    true
}
fn default_true_string() -> String {
    "true".into()
}

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

// ============================================================================
// AWS / S3 Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region: Option<String>,
    #[serde(default = "default_s3_endpoint")]
    pub aws_s3_endpoint: String,
    #[serde(default)]
    pub aws_s3_bucket: Option<String>,
    #[serde(default)]
    pub aws_allow_http: Option<String>,
    #[serde(flatten)]
    pub dynamodb: DynamoDbConfig,
}

fn default_s3_endpoint() -> String {
    "https://s3.amazonaws.com".into()
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

// ============================================================================
// Core Application Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde(default = "default_wal_dir")]
    pub walrus_data_dir: PathBuf,
    #[serde(default = "default_pgwire_port")]
    pub pgwire_port: u16,
    #[serde(default = "default_table_prefix")]
    pub timefusion_table_prefix: String,
    #[serde(default)]
    pub timefusion_config_database_url: Option<String>,
    #[serde(default)]
    pub enable_batch_queue: bool,
    #[serde(default = "default_batch_queue_capacity")]
    pub timefusion_batch_queue_capacity: usize,
}

fn default_wal_dir() -> PathBuf {
    PathBuf::from("/var/lib/timefusion/wal")
}
fn default_pgwire_port() -> u16 {
    5432
}
fn default_table_prefix() -> String {
    "timefusion".into()
}
fn default_batch_queue_capacity() -> usize {
    100_000_000
}

// ============================================================================
// Buffer / WAL Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "default_flush_interval")]
    pub timefusion_flush_interval_secs: u64,
    #[serde(default = "default_retention_mins")]
    pub timefusion_buffer_retention_mins: u64,
    #[serde(default = "default_eviction_interval")]
    pub timefusion_eviction_interval_secs: u64,
    #[serde(default = "default_buffer_max_memory")]
    pub timefusion_buffer_max_memory_mb: usize,
    #[serde(default = "default_shutdown_timeout")]
    pub timefusion_shutdown_timeout_secs: u64,
    #[serde(default = "default_wal_corruption_threshold")]
    pub timefusion_wal_corruption_threshold: usize,
}

fn default_flush_interval() -> u64 {
    600
}
fn default_retention_mins() -> u64 {
    70
}
fn default_eviction_interval() -> u64 {
    60
}
fn default_buffer_max_memory() -> usize {
    4096
}
fn default_shutdown_timeout() -> u64 {
    5
}
fn default_wal_corruption_threshold() -> usize {
    100
}

impl BufferConfig {
    pub fn flush_interval_secs(&self) -> u64 {
        self.timefusion_flush_interval_secs.max(1)
    }
    pub fn retention_mins(&self) -> u64 {
        self.timefusion_buffer_retention_mins.max(1)
    }
    pub fn eviction_interval_secs(&self) -> u64 {
        self.timefusion_eviction_interval_secs.max(1)
    }
    pub fn max_memory_mb(&self) -> usize {
        self.timefusion_buffer_max_memory_mb.max(64)
    }
    pub fn wal_corruption_threshold(&self) -> usize {
        self.timefusion_wal_corruption_threshold
    }

    pub fn compute_shutdown_timeout(&self, current_memory_mb: usize) -> Duration {
        let secs = self.timefusion_shutdown_timeout_secs.max(1) + (current_memory_mb / 100) as u64;
        Duration::from_secs(secs.min(300))
    }
}

// ============================================================================
// Foyer Cache Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_512")]
    pub timefusion_foyer_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb: Option<usize>,
    #[serde(default = "default_100")]
    pub timefusion_foyer_disk_gb: usize,
    #[serde(default = "default_ttl")]
    pub timefusion_foyer_ttl_seconds: u64,
    #[serde(default = "default_cache_dir")]
    pub timefusion_foyer_cache_dir: PathBuf,
    #[serde(default = "default_8")]
    pub timefusion_foyer_shards: usize,
    #[serde(default = "default_32")]
    pub timefusion_foyer_file_size_mb: usize,
    #[serde(default = "default_true_string")]
    pub timefusion_foyer_stats: String,
    #[serde(default = "default_1mb")]
    pub timefusion_parquet_metadata_size_hint: usize,
    #[serde(default = "default_512")]
    pub timefusion_foyer_metadata_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb: Option<usize>,
    #[serde(default = "default_5")]
    pub timefusion_foyer_metadata_disk_gb: usize,
    #[serde(default = "default_4")]
    pub timefusion_foyer_metadata_shards: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
}

fn default_512() -> usize {
    512
}
fn default_100() -> usize {
    100
}
fn default_ttl() -> u64 {
    604_800
} // 7 days
fn default_cache_dir() -> PathBuf {
    PathBuf::from("/tmp/timefusion_cache")
}
fn default_8() -> usize {
    8
}
fn default_32() -> usize {
    32
}
fn default_1mb() -> usize {
    1_048_576
}
fn default_5() -> usize {
    5
}
fn default_4() -> usize {
    4
}

impl CacheConfig {
    pub fn is_disabled(&self) -> bool {
        self.timefusion_foyer_disabled
    }
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_foyer_ttl_seconds)
    }
    pub fn stats_enabled(&self) -> bool {
        self.timefusion_foyer_stats.to_lowercase() == "true"
    }

    pub fn memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_memory_mb * 1024 * 1024
    }
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map(|mb| mb * 1024 * 1024).unwrap_or(self.timefusion_foyer_disk_gb * 1024 * 1024 * 1024)
    }
    pub fn file_size_bytes(&self) -> usize {
        self.timefusion_foyer_file_size_mb * 1024 * 1024
    }
    pub fn metadata_memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_memory_mb * 1024 * 1024
    }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(self.timefusion_foyer_metadata_disk_gb * 1024 * 1024 * 1024)
    }
}

// ============================================================================
// Parquet / Writer Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "default_page_rows")]
    pub timefusion_page_row_count_limit: usize,
    #[serde(default = "default_zstd")]
    pub timefusion_zstd_compression_level: i32,
    #[serde(default = "default_row_group")]
    pub timefusion_max_row_group_size: usize,
    #[serde(default = "default_10")]
    pub timefusion_checkpoint_interval: u64,
    #[serde(default = "default_target_size")]
    pub timefusion_optimize_target_size: i64,
    #[serde(default = "default_50")]
    pub timefusion_stats_cache_size: usize,
}

fn default_page_rows() -> usize {
    20_000
}
fn default_zstd() -> i32 {
    3
}
fn default_row_group() -> usize {
    134_217_728
} // 128MB
fn default_10() -> u64 {
    10
}
fn default_target_size() -> i64 {
    128 * 1024 * 1024
}
fn default_50() -> usize {
    50
}

// ============================================================================
// Maintenance / Scheduler Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    #[serde(default = "default_vacuum_retention")]
    pub timefusion_vacuum_retention_hours: u64,
    #[serde(default = "default_light_schedule")]
    pub timefusion_light_optimize_schedule: String,
    #[serde(default = "default_optimize_schedule")]
    pub timefusion_optimize_schedule: String,
    #[serde(default = "default_vacuum_schedule")]
    pub timefusion_vacuum_schedule: String,
}

fn default_vacuum_retention() -> u64 {
    72
}
fn default_light_schedule() -> String {
    "0 */5 * * * *".into()
}
fn default_optimize_schedule() -> String {
    "0 */30 * * * *".into()
}
fn default_vacuum_schedule() -> String {
    "0 0 2 * * *".into()
}

// ============================================================================
// DataFusion Memory Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "default_mem_gb")]
    pub timefusion_memory_limit_gb: usize,
    #[serde(default = "default_fraction")]
    pub timefusion_memory_fraction: f64,
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default = "default_true")]
    pub timefusion_tracing_record_metrics: bool,
}

fn default_mem_gb() -> usize {
    8
}
fn default_fraction() -> f64 {
    0.9
}

impl MemoryConfig {
    pub fn memory_limit_bytes(&self) -> usize {
        self.timefusion_memory_limit_gb * 1024 * 1024 * 1024
    }
}

// ============================================================================
// Telemetry / OpenTelemetry Configuration
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "default_otlp")]
    pub otel_exporter_otlp_endpoint: String,
    #[serde(default = "default_service")]
    pub otel_service_name: String,
    #[serde(default = "default_version")]
    pub otel_service_version: String,
    #[serde(default)]
    pub log_format: Option<String>,
}

fn default_otlp() -> String {
    "http://localhost:4317".into()
}
fn default_service() -> String {
    "timefusion".into()
}
fn default_version() -> String {
    env!("CARGO_PKG_VERSION").into()
}

impl TelemetryConfig {
    pub fn is_json_logging(&self) -> bool {
        self.log_format.as_deref() == Some("json")
    }
}

// ============================================================================
// Test support - just use AppConfig::default() and mutate fields directly
// ============================================================================

#[cfg(test)]
impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>()).unwrap_or_else(|_| {
            // Fallback with manual defaults if envy fails
            Self {
                aws: AwsConfig::default(),
                core: CoreConfig {
                    walrus_data_dir: default_wal_dir(),
                    pgwire_port: default_pgwire_port(),
                    timefusion_table_prefix: default_table_prefix(),
                    timefusion_config_database_url: None,
                    enable_batch_queue: false,
                    timefusion_batch_queue_capacity: default_batch_queue_capacity(),
                },
                buffer: BufferConfig {
                    timefusion_flush_interval_secs: default_flush_interval(),
                    timefusion_buffer_retention_mins: default_retention_mins(),
                    timefusion_eviction_interval_secs: default_eviction_interval(),
                    timefusion_buffer_max_memory_mb: default_buffer_max_memory(),
                    timefusion_shutdown_timeout_secs: default_shutdown_timeout(),
                    timefusion_wal_corruption_threshold: default_wal_corruption_threshold(),
                },
                cache: CacheConfig {
                    timefusion_foyer_memory_mb: default_512(),
                    timefusion_foyer_disk_mb: None,
                    timefusion_foyer_disk_gb: default_100(),
                    timefusion_foyer_ttl_seconds: default_ttl(),
                    timefusion_foyer_cache_dir: default_cache_dir(),
                    timefusion_foyer_shards: default_8(),
                    timefusion_foyer_file_size_mb: default_32(),
                    timefusion_foyer_stats: default_true_string(),
                    timefusion_parquet_metadata_size_hint: default_1mb(),
                    timefusion_foyer_metadata_memory_mb: default_512(),
                    timefusion_foyer_metadata_disk_mb: None,
                    timefusion_foyer_metadata_disk_gb: default_5(),
                    timefusion_foyer_metadata_shards: default_4(),
                    timefusion_foyer_disabled: false,
                },
                parquet: ParquetConfig {
                    timefusion_page_row_count_limit: default_page_rows(),
                    timefusion_zstd_compression_level: default_zstd(),
                    timefusion_max_row_group_size: default_row_group(),
                    timefusion_checkpoint_interval: default_10(),
                    timefusion_optimize_target_size: default_target_size(),
                    timefusion_stats_cache_size: default_50(),
                },
                maintenance: MaintenanceConfig {
                    timefusion_vacuum_retention_hours: default_vacuum_retention(),
                    timefusion_light_optimize_schedule: default_light_schedule(),
                    timefusion_optimize_schedule: default_optimize_schedule(),
                    timefusion_vacuum_schedule: default_vacuum_schedule(),
                },
                memory: MemoryConfig {
                    timefusion_memory_limit_gb: default_mem_gb(),
                    timefusion_memory_fraction: default_fraction(),
                    timefusion_sort_spill_reservation_bytes: None,
                    timefusion_tracing_record_metrics: true,
                },
                telemetry: TelemetryConfig {
                    otel_exporter_otlp_endpoint: default_otlp(),
                    otel_service_name: default_service(),
                    otel_service_version: default_version(),
                    log_format: None,
                },
            }
        })
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
        assert_eq!(config.buffer.max_memory_mb(), 64); // min enforced
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
