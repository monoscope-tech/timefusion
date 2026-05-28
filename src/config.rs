use std::{collections::HashMap, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Load each sub-config separately to avoid #[serde(flatten)] issues with envy
    // See: https://github.com/softprops/envy/issues/26
    Ok(AppConfig {
        aws:         envy::from_env()?,
        core:        envy::from_env()?,
        buffer:      envy::from_env()?,
        cache:       envy::from_env()?,
        parquet:     envy::from_env()?,
        maintenance: envy::from_env()?,
        memory:      envy::from_env()?,
        telemetry:   envy::from_env()?,
        tantivy:     envy::from_env()?,
    })
}

/// Initialize global config from environment (for production use).
pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let mut config = load_config_from_env()?;
    crate::autotune::apply(&mut config);
    let _ = CONFIG.set(config);
    Ok(CONFIG.get().unwrap())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}

// Macro to generate const default functions for serde
macro_rules! const_default {
    ($name:ident: bool = $val:expr) => {
        fn $name() -> bool {
            $val
        }
    };
    ($name:ident: u64 = $val:expr) => {
        fn $name() -> u64 {
            $val
        }
    };
    ($name:ident: u16 = $val:expr) => {
        fn $name() -> u16 {
            $val
        }
    };
    ($name:ident: u32 = $val:expr) => {
        fn $name() -> u32 {
            $val
        }
    };
    ($name:ident: i32 = $val:expr) => {
        fn $name() -> i32 {
            $val
        }
    };
    ($name:ident: i64 = $val:expr) => {
        fn $name() -> i64 {
            $val
        }
    };
    ($name:ident: usize = $val:expr) => {
        fn $name() -> usize {
            $val
        }
    };
    ($name:ident: f64 = $val:expr) => {
        fn $name() -> f64 {
            $val
        }
    };
    ($name:ident: String = $val:expr) => {
        fn $name() -> String {
            $val.into()
        }
    };
    ($name:ident: PathBuf = $val:expr) => {
        fn $name() -> PathBuf {
            PathBuf::from($val)
        }
    };
}

// All default value functions using the macro
const_default!(d_true: bool = true);
const_default!(d_s3_endpoint: String = "https://s3.amazonaws.com");
const_default!(d_data_dir: PathBuf = "./data");
const_default!(d_pgwire_port: u16 = 5432);
const_default!(d_grpc_port: u16 = 50051);
const_default!(d_table_prefix: String = "timefusion");
const_default!(d_batch_queue_capacity: usize = 100_000_000);
const_default!(d_pgwire_user: String = "postgres");
const_default!(d_flush_interval: u64 = 600);
const_default!(d_retention_mins: u64 = 70);
const_default!(d_eviction_interval: u64 = 60);
const_default!(d_buffer_max_memory: usize = 4096);
const_default!(d_wal_shards_per_topic: usize = 4);
const_default!(d_shutdown_timeout: u64 = 5);
const_default!(d_wal_corruption_threshold: usize = 10);
const_default!(d_flush_parallelism: usize = 4);
const_default!(d_wal_fsync_ms: u64 = 200);
// MemBuffer bucket window (seconds). Smaller windows free RAM sooner because
// the previous bucket becomes flushable sooner; larger windows amortize into
// fewer/larger Delta commits. Default 600s (10 min) matches the historical
// hardcoded value; high-throughput tenants benefit from 60–120s.
const_default!(d_bucket_duration_secs: u64 = 600);
// Memory pressure threshold (0–100) at which the flush task is woken
// independently of the periodic flush timer. Triggers an early
// `flush_completed_buckets` so MemBuffer drains before reservation reaches
// the hard limit. 0 disables pressure-triggered flushes.
const_default!(d_pressure_flush_pct: u32 = 75);
// Durability mode for the WAL. One of:
//   "ms"        — async fsync every `wal_fsync_ms` (default; ~200ms loss window)
//   "sync_each" — fsync after every entry (zero data-loss window, ~1ms per write)
//   "none"      — never fsync (test/throwaway data only)
const_default!(d_wal_fsync_mode: String = "ms");
const_default!(d_wal_max_files: usize = 200);
const_default!(d_foyer_memory_mb: usize = 512);
const_default!(d_foyer_disk_gb: usize = 100);
const_default!(d_foyer_ttl: u64 = 604_800); // 7 days
const_default!(d_foyer_shards: usize = 8);
const_default!(d_foyer_file_size_mb: usize = 32);
const_default!(d_foyer_stats: String = "true");
const_default!(d_metadata_size_hint: usize = 1_048_576);
const_default!(d_metadata_memory_mb: usize = 512);
const_default!(d_metadata_disk_gb: usize = 5);
const_default!(d_metadata_shards: usize = 4);
const_default!(d_page_rows: usize = 20_000);
const_default!(d_zstd_level: i32 = 3);
// Tiered compression by partition age. Hot writes prioritize ingest latency;
// older data is rewritten at progressively higher levels by `recompress_tier`.
const_default!(d_zstd_level_warm: i32 = 9);
const_default!(d_zstd_level_cool: i32 = 15);
const_default!(d_zstd_level_cold: i32 = 19);
const_default!(d_warm_cutoff_days: u64 = 1);
const_default!(d_cool_cutoff_days: u64 = 7);
const_default!(d_cold_cutoff_days: u64 = 30);
const_default!(d_recompress_schedule: String = "0 0 3 * * *");
const_default!(d_row_group_size: usize = 134_217_728); // 128MB
const_default!(d_checkpoint_interval: u64 = 10);
const_default!(d_optimize_target: i64 = 128 * 1024 * 1024);
const_default!(d_stats_cache_size: usize = 50);
const_default!(d_vacuum_retention: u64 = 72);
const_default!(d_optimize_window_hours: u64 = 48);
const_default!(d_compact_min_files: usize = 5);
const_default!(d_light_optimize_target: i64 = 16 * 1024 * 1024);
const_default!(d_light_schedule: String = "0 */5 * * * *");
const_default!(d_optimize_schedule: String = "0 */30 * * * *");
const_default!(d_vacuum_schedule: String = "0 0 2 * * *");
const_default!(d_mem_gb: usize = 8);
const_default!(d_mem_fraction: f64 = 0.9);
const_default!(d_otlp_endpoint: String = "http://localhost:4317");
const_default!(d_service_name: String = "timefusion");
fn d_service_version() -> String {
    env!("CARGO_PKG_VERSION").into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub aws:         AwsConfig,
    #[serde(flatten)]
    pub core:        CoreConfig,
    #[serde(flatten)]
    pub buffer:      BufferConfig,
    #[serde(flatten)]
    pub cache:       CacheConfig,
    #[serde(flatten)]
    pub parquet:     ParquetConfig,
    #[serde(flatten)]
    pub maintenance: MaintenanceConfig,
    #[serde(flatten)]
    pub memory:      MemoryConfig,
    #[serde(flatten)]
    pub telemetry:   TelemetryConfig,
    #[serde(flatten)]
    pub tantivy:     TantivyConfig,
}

const_default!(d_tantivy_max_index_mb: u64 = 64);
const_default!(d_tantivy_cache_disk_gb: u64 = 4);
const_default!(d_tantivy_zstd_level: i32 = 19);
const_default!(d_tantivy_min_files: usize = 2);
const_default!(d_tantivy_prefilter_max_hits: usize = 100_000);
const_default!(d_tantivy_prefilter_min_selectivity_pct: u32 = 50);

/// Tantivy sidecar-index configuration. Indexing is always-on for any
/// table whose YAML schema declares `tantivy.indexed: true` on at least
/// one field. There is no override knob — schema is the single source of
/// truth.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde(default = "d_tantivy_max_index_mb")]
    pub timefusion_tantivy_max_index_size_mb:             u64,
    #[serde(default = "d_tantivy_cache_disk_gb")]
    pub timefusion_tantivy_cache_disk_gb:                 u64,
    #[serde(default = "d_tantivy_zstd_level")]
    pub timefusion_tantivy_compression_level:             i32,
    #[serde(default = "d_tantivy_min_files")]
    pub timefusion_tantivy_min_files_for_pushdown:        usize,
    /// If a tantivy prefilter would produce more than this many hits, skip
    /// the `id IN (...)` pushdown entirely — the IN-list itself becomes the
    /// bottleneck above this point. Default 100k.
    #[serde(default = "d_tantivy_prefilter_max_hits")]
    pub timefusion_tantivy_prefilter_max_hits:            usize,
    /// If a tantivy prefilter selects more than this percentage of the
    /// indexed rows, the pushdown isn't worth the round-trip; skip it and
    /// let Delta scan with the original predicate. Default 50 (%).
    #[serde(default = "d_tantivy_prefilter_min_selectivity_pct")]
    pub timefusion_tantivy_prefilter_min_selectivity_pct: u32,
}

impl TantivyConfig {
    /// Tables to index: schemas with `tantivy.indexed: true` on any field.
    /// Computed once from the static registry on first access (the registry
    /// is compiled-in YAML, so there's nothing to invalidate).
    fn indexed_set() -> &'static std::collections::HashSet<String> {
        static SET: std::sync::OnceLock<std::collections::HashSet<String>> = std::sync::OnceLock::new();
        SET.get_or_init(|| {
            crate::schema_loader::registry()
                .list_tables()
                .into_iter()
                .filter(|name| {
                    crate::schema_loader::registry()
                        .get(name)
                        .is_some_and(|s| s.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)))
                })
                .collect()
        })
    }
    pub fn indexed_tables(&self) -> Vec<String> {
        let mut v: Vec<String> = Self::indexed_set().iter().cloned().collect();
        v.sort();
        v
    }
    pub fn is_table_indexed(&self, table: &str) -> bool {
        Self::indexed_set().contains(table)
    }
    pub fn compression_level(&self) -> i32 {
        self.timefusion_tantivy_compression_level
    }
    pub fn prefilter_max_hits(&self) -> usize {
        self.timefusion_tantivy_prefilter_max_hits.max(1)
    }
    pub fn prefilter_min_selectivity_pct(&self) -> u32 {
        self.timefusion_tantivy_prefilter_min_selectivity_pct.min(100)
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id:     Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region:    Option<String>,
    #[serde(default = "d_s3_endpoint")]
    pub aws_s3_endpoint:       String,
    #[serde(default)]
    pub aws_s3_bucket:         Option<String>,
    #[serde(default)]
    pub aws_allow_http:        Option<String>,
    #[serde(flatten)]
    pub dynamodb:              DynamoDbConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DynamoDbConfig {
    #[serde(default)]
    pub aws_s3_locking_provider:        Option<String>,
    #[serde(default)]
    pub delta_dynamo_table_name:        Option<String>,
    #[serde(default)]
    pub aws_access_key_id_dynamodb:     Option<String>,
    #[serde(default)]
    pub aws_secret_access_key_dynamodb: Option<String>,
    #[serde(default)]
    pub aws_region_dynamodb:            Option<String>,
    #[serde(default)]
    pub aws_endpoint_url_dynamodb:      Option<String>,
}

impl AwsConfig {
    pub fn is_dynamodb_locking_enabled(&self) -> bool {
        self.dynamodb.aws_s3_locking_provider.as_deref() == Some("dynamodb")
    }

    pub fn build_storage_options(&self, endpoint_override: Option<&str>) -> HashMap<String, String> {
        macro_rules! insert_opt {
            ($opts:expr, $key:expr, $val:expr) => {
                if let Some(ref v) = $val {
                    $opts.insert($key.into(), v.clone());
                }
            };
        }

        let mut opts = HashMap::new();
        insert_opt!(opts, "AWS_ACCESS_KEY_ID", self.aws_access_key_id);
        insert_opt!(opts, "AWS_SECRET_ACCESS_KEY", self.aws_secret_access_key);
        insert_opt!(opts, "AWS_REGION", self.aws_default_region);
        insert_opt!(opts, "AWS_ALLOW_HTTP", self.aws_allow_http);
        opts.insert("AWS_ENDPOINT_URL".into(), endpoint_override.unwrap_or(&self.aws_s3_endpoint).to_string());

        if self.is_dynamodb_locking_enabled() {
            opts.insert("AWS_S3_LOCKING_PROVIDER".into(), "dynamodb".into());
            insert_opt!(opts, "DELTA_DYNAMO_TABLE_NAME", self.dynamodb.delta_dynamo_table_name);
            insert_opt!(opts, "AWS_ACCESS_KEY_ID_DYNAMODB", self.dynamodb.aws_access_key_id_dynamodb);
            insert_opt!(opts, "AWS_SECRET_ACCESS_KEY_DYNAMODB", self.dynamodb.aws_secret_access_key_dynamodb);
            insert_opt!(opts, "AWS_REGION_DYNAMODB", self.dynamodb.aws_region_dynamodb);
            insert_opt!(opts, "AWS_ENDPOINT_URL_DYNAMODB", self.dynamodb.aws_endpoint_url_dynamodb);
        }
        opts
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde(default = "d_data_dir")]
    pub timefusion_data_dir:             PathBuf,
    #[serde(default = "d_pgwire_port")]
    pub pgwire_port:                     u16,
    #[serde(default = "d_table_prefix")]
    pub timefusion_table_prefix:         String,
    #[serde(default)]
    pub timefusion_config_database_url:  Option<String>,
    #[serde(default = "d_true")]
    pub enable_batch_queue:              bool,
    #[serde(default = "d_batch_queue_capacity")]
    pub timefusion_batch_queue_capacity: usize,
    #[serde(default = "d_pgwire_user")]
    pub pgwire_user:                     String,
    #[serde(default)]
    pub pgwire_password:                 Option<String>,
    #[serde(default = "d_grpc_port")]
    pub grpc_port:                       u16,
    #[serde(default)]
    pub grpc_token:                      Option<String>,
}

impl CoreConfig {
    pub fn wal_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("wal")
    }
    pub fn cache_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("cache")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "d_flush_interval")]
    pub timefusion_flush_interval_secs:      u64,
    #[serde(default = "d_retention_mins")]
    pub timefusion_buffer_retention_mins:    u64,
    #[serde(default = "d_eviction_interval")]
    pub timefusion_eviction_interval_secs:   u64,
    #[serde(default = "d_buffer_max_memory")]
    pub timefusion_buffer_max_memory_mb:     usize,
    #[serde(default = "d_shutdown_timeout")]
    pub timefusion_shutdown_timeout_secs:    u64,
    #[serde(default = "d_wal_corruption_threshold")]
    pub timefusion_wal_corruption_threshold: usize,
    #[serde(default = "d_flush_parallelism")]
    pub timefusion_flush_parallelism:        usize,
    #[serde(default)]
    pub timefusion_flush_immediately:        bool,
    #[serde(default = "d_wal_fsync_ms")]
    pub timefusion_wal_fsync_ms:             u64,
    #[serde(default = "d_wal_fsync_mode")]
    pub timefusion_wal_fsync_mode:           String,
    #[serde(default = "d_wal_max_files")]
    pub timefusion_wal_max_file_count:       usize,
    #[serde(default = "d_bucket_duration_secs")]
    pub timefusion_bucket_duration_secs:     u64,
    #[serde(default = "d_pressure_flush_pct")]
    pub timefusion_pressure_flush_pct:       u32,
    /// WAL shards per (project, table) topic. Higher = more append parallelism
    /// at the cost of O(shards) recovery memory and more file handles.
    #[serde(default = "d_wal_shards_per_topic")]
    pub timefusion_wal_shards_per_topic:     usize,
}

/// WAL durability mode. See `d_wal_fsync_mode` for the env-var encoding.
#[derive(Debug, Clone, Copy)]
pub enum WalFsyncMode {
    Milliseconds(u64),
    SyncEach,
    None,
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
    pub fn wal_shards_per_topic(&self) -> usize {
        self.timefusion_wal_shards_per_topic.max(1)
    }
    pub fn wal_corruption_threshold(&self) -> usize {
        self.timefusion_wal_corruption_threshold
    }
    pub fn flush_parallelism(&self) -> usize {
        self.timefusion_flush_parallelism.max(1)
    }
    pub fn flush_immediately(&self) -> bool {
        self.timefusion_flush_immediately
    }
    pub fn wal_fsync_ms(&self) -> u64 {
        self.timefusion_wal_fsync_ms.max(1)
    }
    pub fn wal_fsync_mode(&self) -> WalFsyncMode {
        match self.timefusion_wal_fsync_mode.to_ascii_lowercase().as_str() {
            "sync_each" | "synceach" | "each" => WalFsyncMode::SyncEach,
            "none" | "off" | "disabled" => WalFsyncMode::None,
            _ => WalFsyncMode::Milliseconds(self.wal_fsync_ms()),
        }
    }
    pub fn wal_max_file_count(&self) -> usize {
        self.timefusion_wal_max_file_count
    }
    pub fn bucket_duration_secs(&self) -> u64 {
        self.timefusion_bucket_duration_secs.max(1)
    }
    pub fn pressure_flush_pct(&self) -> u32 {
        self.timefusion_pressure_flush_pct.min(100)
    }

    pub fn compute_shutdown_timeout(&self, current_memory_mb: usize) -> Duration {
        Duration::from_secs((self.timefusion_shutdown_timeout_secs.max(1) + (current_memory_mb / 100) as u64).min(300))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "d_foyer_memory_mb")]
    pub timefusion_foyer_memory_mb:            usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb:              Option<usize>,
    #[serde(default = "d_foyer_disk_gb")]
    pub timefusion_foyer_disk_gb:              usize,
    #[serde(default = "d_foyer_ttl")]
    pub timefusion_foyer_ttl_seconds:          u64,
    #[serde(default = "d_foyer_shards")]
    pub timefusion_foyer_shards:               usize,
    #[serde(default = "d_foyer_file_size_mb")]
    pub timefusion_foyer_file_size_mb:         usize,
    #[serde(default = "d_foyer_stats")]
    pub timefusion_foyer_stats:                String,
    #[serde(default = "d_metadata_size_hint")]
    pub timefusion_parquet_metadata_size_hint: usize,
    #[serde(default = "d_metadata_memory_mb")]
    pub timefusion_foyer_metadata_memory_mb:   usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb:     Option<usize>,
    #[serde(default = "d_metadata_disk_gb")]
    pub timefusion_foyer_metadata_disk_gb:     usize,
    #[serde(default = "d_metadata_shards")]
    pub timefusion_foyer_metadata_shards:      usize,
    #[serde(default)]
    pub timefusion_foyer_disabled:             bool,
}

impl CacheConfig {
    pub fn is_disabled(&self) -> bool {
        self.timefusion_foyer_disabled
    }
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_foyer_ttl_seconds)
    }
    pub fn stats_enabled(&self) -> bool {
        self.timefusion_foyer_stats.eq_ignore_ascii_case("true")
    }
    pub fn memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_memory_mb * 1024 * 1024
    }
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map_or(self.timefusion_foyer_disk_gb * 1024 * 1024 * 1024, |mb| mb * 1024 * 1024)
    }
    pub fn file_size_bytes(&self) -> usize {
        self.timefusion_foyer_file_size_mb * 1024 * 1024
    }
    pub fn metadata_memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_memory_mb * 1024 * 1024
    }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb
            .map_or(self.timefusion_foyer_metadata_disk_gb * 1024 * 1024 * 1024, |mb| mb * 1024 * 1024)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "d_page_rows")]
    pub timefusion_page_row_count_limit:   usize,
    /// ZSTD level for hot writes (flush + today's light optimize). Default 3.
    /// Aliased by the legacy env name; lower = faster ingest.
    #[serde(default = "d_zstd_level", alias = "timefusion_zstd_level_hot")]
    pub timefusion_zstd_compression_level: i32,
    #[serde(default = "d_zstd_level_warm")]
    pub timefusion_zstd_level_warm:        i32,
    #[serde(default = "d_zstd_level_cool")]
    pub timefusion_zstd_level_cool:        i32,
    #[serde(default = "d_zstd_level_cold")]
    pub timefusion_zstd_level_cold:        i32,
    #[serde(default = "d_warm_cutoff_days")]
    pub timefusion_warm_cutoff_days:       u64,
    #[serde(default = "d_cool_cutoff_days")]
    pub timefusion_cool_cutoff_days:       u64,
    #[serde(default = "d_cold_cutoff_days")]
    pub timefusion_cold_cutoff_days:       u64,
    #[serde(default = "d_row_group_size")]
    pub timefusion_max_row_group_size:     usize,
    #[serde(default = "d_checkpoint_interval")]
    pub timefusion_checkpoint_interval:    u64,
    #[serde(default = "d_optimize_target")]
    pub timefusion_optimize_target_size:   i64,
    #[serde(default = "d_stats_cache_size")]
    pub timefusion_stats_cache_size:       usize,
    #[serde(default)]
    pub timefusion_bloom_filter_disabled:  bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    #[serde(default = "d_vacuum_retention")]
    pub timefusion_vacuum_retention_hours:     u64,
    #[serde(default = "d_optimize_window_hours")]
    pub timefusion_optimize_window_hours:      u64,
    #[serde(default = "d_compact_min_files")]
    pub timefusion_compact_min_files:          usize,
    #[serde(default = "d_light_optimize_target")]
    pub timefusion_light_optimize_target_size: i64,
    #[serde(default = "d_light_schedule")]
    pub timefusion_light_optimize_schedule:    String,
    #[serde(default = "d_optimize_schedule")]
    pub timefusion_optimize_schedule:          String,
    #[serde(default = "d_vacuum_schedule")]
    pub timefusion_vacuum_schedule:            String,
    #[serde(default = "d_recompress_schedule")]
    pub timefusion_recompress_schedule:        String,
}

/// Which DataFusion `MemoryPool` to back the runtime with.
///
/// - `Greedy` (default): all consumers share the full pool; first-come,
///   first-served. Right for write-heavy workloads where INSERTs dominate
///   and per-statement memory needs vary widely (e.g. one batch is 50 MB
///   of Arrow, another is 5 MB). FairSpillPool would slice the pool into
///   per-consumer quotas (`pool / num_consumers`) and reject any consumer
///   whose batch exceeded its slot — bit us in prod on 2026-05-28 when
///   ~30 concurrent INSERTs each got a ~76 MB slot and every 700-row
///   batch hit `Memory limit exceeded`.
/// - `FairSpill`: slot-per-consumer fairness. Better for ad-hoc query
///   workloads with many concurrent users where one large query
///   shouldn't starve the others. Not the right default for ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPoolKind {
    Greedy,
    FairSpill,
}

fn d_memory_pool() -> MemoryPoolKind {
    MemoryPoolKind::Greedy
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "d_mem_gb")]
    pub timefusion_memory_limit_gb:              usize,
    #[serde(default = "d_mem_fraction")]
    pub timefusion_memory_fraction:              f64,
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default = "d_memory_pool")]
    pub timefusion_memory_pool:                  MemoryPoolKind,
    #[serde(default = "d_true")]
    pub timefusion_tracing_record_metrics:       bool,
}

impl MemoryConfig {
    pub fn memory_limit_bytes(&self) -> usize {
        self.timefusion_memory_limit_gb * 1024 * 1024 * 1024
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "d_otlp_endpoint")]
    pub otel_exporter_otlp_endpoint: String,
    #[serde(default = "d_service_name")]
    pub otel_service_name:           String,
    #[serde(default = "d_service_version")]
    pub otel_service_version:        String,
    #[serde(default)]
    pub log_format:                  Option<String>,
}

impl TelemetryConfig {
    pub fn is_json_logging(&self) -> bool {
        self.log_format.as_deref() == Some("json")
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>()).expect("Default config should always succeed with serde defaults")
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
