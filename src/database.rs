use crate::schema_loader::{get_default_schema, get_schema};
use crate::object_store_cache::{FoyerObjectStoreCache, FoyerCacheConfig, SharedFoyerCache};
use crate::statistics::DeltaStatisticsExtractor;
use anyhow::Result;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, AsArray};
use datafusion::common::not_impl_err;
use datafusion::common::stats::Precision;
use datafusion::common::{SchemaExt, Statistics};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
// Removed unused imports
use datafusion::physical_plan::DisplayAs;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{dml::InsertOp, BinaryExpr},
    physical_plan::{DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_functions_json;
use delta_kernel::arrow::record_batch::RecordBatch;
use deltalake::checkpoints;
use deltalake::datafusion::parquet::file::properties::WriterProperties;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::fmt;
use std::{any::Any, collections::HashMap, env, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use url::Url;

// Changed to support multiple tables per project: (project_id, table_name) -> DeltaTable
pub type ProjectConfigs = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

// Helper function to extract project_id from a batch
pub fn extract_project_id(batch: &RecordBatch) -> Option<String> {
    batch.schema().fields().iter().position(|f| f.name() == "project_id").and_then(|idx| {
        let column = batch.column(idx);
        let string_array = column.as_string::<i32>();
        if string_array.len() > 0 && !string_array.is_null(0) {
            Some(string_array.value(0).to_string())
        } else {
            None
        }
    })
}

// Constants for optimization and vacuum operations
const DEFAULT_VACUUM_RETENTION_HOURS: u64 = 336; // 2 weeks
const DEFAULT_CHECKPOINT_INTERVAL: i64 = 20;
const DEFAULT_OPTIMIZE_TARGET_SIZE: i64 = 536870912; // 512MB
const DEFAULT_PAGE_ROW_COUNT_LIMIT: usize = 20000;
const ZSTD_COMPRESSION_LEVEL: i32 = 6; // Balance between compression ratio and speed

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct StorageConfig {
    project_id: String,
    table_name: String,
    s3_bucket: String,
    s3_prefix: String,
    s3_region: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    s3_endpoint: Option<String>,
}


#[derive(Debug)]
pub struct Database {
    project_configs: ProjectConfigs,
    batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
    // PostgreSQL pool for configuration (optional)
    config_pool: Option<PgPool>,
    // Cached storage configurations
    storage_configs: Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    // Default S3 settings for unconfigured mode
    default_s3_bucket: Option<String>,
    default_s3_prefix: Option<String>,
    default_s3_endpoint: Option<String>,
    // Object store cache (optional)
    object_store_cache: Option<Arc<SharedFoyerCache>>,
    // Statistics extractor for Delta Lake tables
    statistics_extractor: Arc<DeltaStatisticsExtractor>,
    // Track last written versions for read-after-write consistency
    // Map of (project_id, table_name) -> last_written_version
    last_written_versions: Arc<RwLock<HashMap<(String, String), i64>>>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            project_configs: Arc::clone(&self.project_configs),
            batch_queue: self.batch_queue.clone(),
            maintenance_shutdown: Arc::clone(&self.maintenance_shutdown),
            config_pool: self.config_pool.clone(),
            storage_configs: Arc::clone(&self.storage_configs),
            default_s3_bucket: self.default_s3_bucket.clone(),
            default_s3_prefix: self.default_s3_prefix.clone(),
            default_s3_endpoint: self.default_s3_endpoint.clone(),
            object_store_cache: self.object_store_cache.clone(),
            statistics_extractor: Arc::clone(&self.statistics_extractor),
            last_written_versions: Arc::clone(&self.last_written_versions),
        }
    }
}

impl Database {
    /// Creates standard writer properties used across different operations
    fn create_writer_properties() -> WriterProperties {
        use deltalake::datafusion::parquet::basic::{Compression, ZstdLevel};
        use deltalake::datafusion::parquet::file::properties::EnabledStatistics;

        // Get configurable values from environment
        let page_row_count_limit = env::var("TIMEFUSION_PAGE_ROW_COUNT_LIMIT")
            .unwrap_or_else(|_| DEFAULT_PAGE_ROW_COUNT_LIMIT.to_string())
            .parse::<usize>()
            .unwrap_or(DEFAULT_PAGE_ROW_COUNT_LIMIT);

        // Get compression level from environment (default to ZSTD_COMPRESSION_LEVEL constant)
        let compression_level = env::var("TIMEFUSION_ZSTD_COMPRESSION_LEVEL")
            .unwrap_or_else(|_| ZSTD_COMPRESSION_LEVEL.to_string())
            .parse::<i32>()
            .unwrap_or(ZSTD_COMPRESSION_LEVEL);

        // Get max row group size from environment (default to 128MB)
        let max_row_group_size = env::var("TIMEFUSION_MAX_ROW_GROUP_SIZE")
            .unwrap_or_else(|_| "134217728".to_string())
            .parse::<usize>()
            .unwrap_or(134217728); // 128MB

        WriterProperties::builder()
            // Use ZSTD compression with high level for maximum compression ratio
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(compression_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()),
            ))
            // Set max row group size for better compression and query performance
            .set_max_row_group_size(max_row_group_size)
            // Enable dictionary encoding for better compression of repetitive values
            .set_dictionary_enabled(true)
            // Dictionary page size - 8MB allows larger dictionaries for better compression
            .set_dictionary_page_size_limit(8388608) // 8MB
            // Enable statistics for better query optimization
            .set_statistics_enabled(EnabledStatistics::Page)
            // Set page row count limit for better compression
            .set_data_page_row_count_limit(page_row_count_limit)
            .build()
    }

    /// Updates a DeltaTable and handles errors consistently
    async fn update_table(&self, table: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> Result<()> {
        // Try to update with retries for eventual consistency
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;
        
        loop {
            let mut table_write = table.write().await;
            match table_write.update().await {
                Ok(_) => {
                    if let Some(version) = table_write.version() {
                        debug!("Updated table for {}/{} to version {}", project_id, table_name, version);
                        // Update our version tracking to reflect what we just loaded
                        let mut versions = self.last_written_versions.write().await;
                        versions.insert((project_id.to_string(), table_name.to_string()), version);
                    }
                    return Ok(());
                }
                Err(e) => {
                    // Release the lock before retrying
                    drop(table_write);
                    
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        error!("Failed to update table for {}/{} after {} retries: {}", project_id, table_name, MAX_RETRIES, e);
                        return Err(anyhow::anyhow!("Failed to update table: {}", e));
                    }
                    
                    debug!("Failed to update table for {}/{} (attempt {}/{}): {}, retrying...", project_id, table_name, retries, MAX_RETRIES, e);
                    // Exponential backoff with jitter
                    let delay = 100 * retries as u64 + (retries as u64 * 50);
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// Load storage configurations from PostgreSQL
    async fn load_storage_configs(pool: &PgPool) -> Result<HashMap<(String, String), StorageConfig>> {
        // Ensure table exists
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS timefusion_projects (
                project_id VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                s3_bucket VARCHAR(255) NOT NULL,
                s3_prefix VARCHAR(500) NOT NULL,
                s3_region VARCHAR(100) NOT NULL,
                s3_access_key_id VARCHAR(500) NOT NULL,
                s3_secret_access_key VARCHAR(500) NOT NULL,
                s3_endpoint VARCHAR(500),
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (project_id, table_name)
            )
            "#,
        )
        .execute(pool)
        .await?;

        let configs: Vec<StorageConfig> = sqlx::query_as(
            "SELECT project_id, table_name, s3_bucket, s3_prefix, s3_region, 
             s3_access_key_id, s3_secret_access_key, s3_endpoint 
             FROM timefusion_projects WHERE is_active = true",
        )
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for config in configs {
            info!("Loaded config: {}/{}", config.project_id, config.table_name);
            map.insert((config.project_id.clone(), config.table_name.clone()), config);
        }
        Ok(map)
    }

    pub async fn new() -> Result<Self> {
        let aws_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
        let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        // Store default S3 settings for unconfigured mode
        let default_s3_bucket = env::var("AWS_S3_BUCKET").ok();
        let default_s3_prefix = env::var("TIMEFUSION_TABLE_PREFIX").unwrap_or_else(|_| "timefusion".to_string());
        let default_s3_endpoint = Some(aws_endpoint.clone());

        // Try to connect to config database if URL is provided
        let (config_pool, storage_configs) = if let Ok(db_url) = env::var("TIMEFUSION_CONFIG_DATABASE_URL") {
            let pool = PgPoolOptions::new().max_connections(2).connect(&db_url).await.ok();

            if let Some(ref p) = pool {
                let configs = Self::load_storage_configs(p).await.unwrap_or_default();
                (pool, configs)
            } else {
                info!("Could not connect to config database, using default mode");
                (None, HashMap::new())
            }
        } else {
            (None, HashMap::new())
        };

        let project_configs = HashMap::new();
        
        // Initialize object store cache BEFORE creating any tables
        // This ensures all tables benefit from caching
        let object_store_cache = {
            let config = FoyerCacheConfig::from_env();
            info!("Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, TTL: {}s)",
                config.memory_size_bytes / 1024 / 1024,
                config.disk_size_bytes / 1024 / 1024 / 1024,
                config.ttl.as_secs()
            );
            
            match SharedFoyerCache::new(config).await {
                Ok(cache) => {
                    info!("Shared Foyer cache initialized successfully for all tables");
                    Some(Arc::new(cache))
                }
                Err(e) => {
                    error!("Failed to initialize shared Foyer cache: {}. Continuing without cache.", e);
                    None
                }
            }
        };
        
        // Initialize statistics extractor with configurable cache size
        let stats_cache_size = env::var("TIMEFUSION_STATS_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(50);
        let statistics_extractor = Arc::new(DeltaStatisticsExtractor::new(stats_cache_size, 300));
        
        let db = Self {
            project_configs: Arc::new(RwLock::new(project_configs)),
            batch_queue: None,
            maintenance_shutdown: Arc::new(CancellationToken::new()),
            config_pool,
            storage_configs: Arc::new(RwLock::new(storage_configs)),
            default_s3_bucket: default_s3_bucket.clone(),
            default_s3_prefix: Some(default_s3_prefix.clone()),
            default_s3_endpoint,
            object_store_cache,
            statistics_extractor,
            last_written_versions: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize default project with otel_logs_and_spans table if AWS_S3_BUCKET is set
        if let Some(ref bucket) = default_s3_bucket {
            let storage_uri = format!(
                "s3://{}/{}/projects/default/otel_logs_and_spans/?endpoint={}",
                bucket, default_s3_prefix, aws_endpoint
            );
            info!("Default project storage URI: {}", storage_uri);

            // Initialize table for default project with cache support
            // Populate storage options with AWS credentials from environment
            let mut storage_options = HashMap::new();
            if let Ok(access_key) = env::var("AWS_ACCESS_KEY_ID") {
                storage_options.insert("aws_access_key_id".to_string(), access_key);
            }
            if let Ok(secret_key) = env::var("AWS_SECRET_ACCESS_KEY") {
                storage_options.insert("aws_secret_access_key".to_string(), secret_key);
            }
            if let Ok(region) = env::var("AWS_DEFAULT_REGION") {
                storage_options.insert("aws_region".to_string(), region);
            }
            storage_options.insert("aws_endpoint".to_string(), aws_endpoint.clone());
            
            // Create the cached object store for the default table
            let table = if let Some(ref shared_cache) = db.object_store_cache {
                // Create base S3 object store
                let base_store = db.create_object_store(&storage_uri, &storage_options).await?;
                
                // Wrap with the shared Foyer cache
                let cached_store = Arc::new(FoyerObjectStoreCache::new_with_shared_cache(base_store, shared_cache)) as Arc<dyn object_store::ObjectStore>;
                
                info!("Default table will use Foyer cache for all object store operations");
                
                // Load or create table with cached object store
                match DeltaTableBuilder::from_uri(&storage_uri)
                    .with_storage_backend(cached_store.clone(), Url::parse(&storage_uri)?)
                    .with_storage_options(storage_options.clone())
                    .with_allow_http(true)
                    .load()
                    .await
                {
                    Ok(table) => {
                        let version = table.version().unwrap_or(0);
                        let checkpoint_interval = env::var("TIMEFUSION_CHECKPOINT_INTERVAL")
                            .unwrap_or_else(|_| DEFAULT_CHECKPOINT_INTERVAL.to_string())
                            .parse::<i64>()
                            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL);

                        if version > 0 && version % checkpoint_interval == 0 {
                            info!("Checkpointing table for default project at initial load, version {}", version);
                            checkpoints::create_checkpoint(&table, None).await?;
                        }
                        table
                    }
                    Err(err) => {
                        log::warn!("Table doesn't exist for default project. Creating new table. err: {:?}", err);

                        let schema = get_schema("otel_logs_and_spans").unwrap_or_else(get_default_schema);
                        
                        // Create table with cached object store
                        // Note: DeltaOps doesn't support custom object stores during create, but subsequent operations will use cache
                        let delta_ops = DeltaOps::try_from_uri(&storage_uri).await?;
                        let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));

                        let _new_table = delta_ops
                            .create()
                            .with_columns(schema.columns().unwrap_or_default())
                            .with_partition_columns(schema.partitions.clone())
                            .with_storage_options(storage_options.clone())
                            .with_commit_properties(commit_properties)
                            .await?;
                        
                        // After creation, reload with cached object store for future operations
                        DeltaTableBuilder::from_uri(&storage_uri)
                            .with_storage_backend(cached_store.clone(), Url::parse(&storage_uri)?)
                            .with_storage_options(storage_options.clone())
                            .with_allow_http(true)
                            .load()
                            .await?
                    }
                }
            } else {
                // No cache available, fall back to non-cached table
                log::warn!("Foyer cache not available, using non-cached object store for default table");
                match DeltaTableBuilder::from_uri(&storage_uri)
                    .with_storage_options(storage_options.clone())
                    .with_allow_http(true)
                    .load()
                    .await
                {
                    Ok(table) => {
                        let version = table.version().unwrap_or(0);
                        let checkpoint_interval = env::var("TIMEFUSION_CHECKPOINT_INTERVAL")
                            .unwrap_or_else(|_| DEFAULT_CHECKPOINT_INTERVAL.to_string())
                            .parse::<i64>()
                            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL);

                        if version > 0 && version % checkpoint_interval == 0 {
                            info!("Checkpointing table for default project at initial load, version {}", version);
                            checkpoints::create_checkpoint(&table, None).await?;
                        }
                        table
                    }
                    Err(err) => {
                        log::warn!("Table doesn't exist for default project. Creating new table. err: {:?}", err);

                        let schema = get_schema("otel_logs_and_spans").unwrap_or_else(get_default_schema);
                        let delta_ops = DeltaOps::try_from_uri(&storage_uri).await?;
                        let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));

                        delta_ops
                            .create()
                            .with_columns(schema.columns().unwrap_or_default())
                            .with_partition_columns(schema.partitions.clone())
                            .with_storage_options(storage_options.clone())
                            .with_commit_properties(commit_properties)
                            .await?
                    }
                }
            };

            let mut configs = db.project_configs.write().await;
            configs.insert(("default".to_string(), "otel_logs_and_spans".to_string()), Arc::new(RwLock::new(table)));
            info!("Initialized default project table at: {}", storage_uri);
        }

        // Cache is already initialized above, no need to call with_object_store_cache()
        Ok(db)
    }

    /// Set the batch queue to use for insert operations
    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::batch_queue::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }
    
    /// Enable object store cache with foyer (deprecated - cache is now initialized in new())
    /// This method is kept for backward compatibility but is now a no-op
    pub async fn with_object_store_cache(self) -> Result<Self> {
        // Cache is now initialized in new(), so this is a no-op
        Ok(self)
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        use tokio_cron_scheduler::{Job, JobScheduler};

        let scheduler = JobScheduler::new().await?;
        let db = Arc::new(self.clone());

        // Optimize job - every hour
        let optimize_job = Job::new_async("0 0 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Running scheduled optimize on all tables");
                    for ((project_id, table_name), table) in db.project_configs.read().await.iter() {
                        if let Err(e) = db.optimize_table(table, None).await {
                            error!("Optimize failed for project '{}' table '{}': {}", project_id, table_name, e);
                        }
                    }
                })
            }
        })?;

        scheduler.add(optimize_job).await?;

        // Vacuum job - daily at 3AM
        let vacuum_job = Job::new_async("0 0 3 * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Running scheduled vacuum on all tables");
                    let retention_hours = env::var("TIMEFUSION_VACUUM_RETENTION_HOURS")
                        .unwrap_or_else(|_| DEFAULT_VACUUM_RETENTION_HOURS.to_string())
                        .parse::<u64>()
                        .unwrap_or(DEFAULT_VACUUM_RETENTION_HOURS);

                    for ((project_id, table_name), table) in db.project_configs.read().await.iter() {
                        info!("Vacuuming project '{}' table '{}' (retention: {}h)", project_id, table_name, retention_hours);
                        db.vacuum_table(table, retention_hours).await;
                    }
                })
            }
        })?;

        scheduler.add(vacuum_job).await?;
        
        // Cache stats job - every 5 minutes
        let cache_stats_job = Job::new_async("0 */5 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    // Log Foyer cache stats if available
                    if let Some(ref cache) = db.object_store_cache {
                        cache.log_stats().await;
                    }
                    
                    // Log statistics cache stats
                    let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
                    info!("Statistics cache: {}/{} entries used", used, capacity);
                })
            }
        })?;
        
        scheduler.add(cache_stats_job).await?;
        
        // Statistics refresh job - every 15 minutes
        let stats_refresh_job = Job::new_async("0 */15 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Refreshing Delta Lake statistics cache");
                    db.statistics_extractor.clear_cache().await;
                    
                    // Pre-warm cache for active tables
                    for ((project_id, table_name), table) in db.project_configs.read().await.iter() {
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        
                        // Always refresh statistics after clearing cache
                        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
                        let schema = schema_def.schema_ref();
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, project_id, table_name, &schema).await {
                            error!("Failed to refresh statistics for {}:{}: {}", project_id, table_name, e);
                        } else {
                            debug!("Refreshed statistics for {}:{} (version {})", project_id, table_name, current_version);
                        }
                    }
                })
            }
        })?;
        
        scheduler.add(stats_refresh_job).await?;

        // Start the scheduler
        scheduler.start().await?;

        // Handle shutdown
        let shutdown = self.maintenance_shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            info!("Shutting down maintenance scheduler");
            // Note: scheduler will be dropped when this task ends
        });

        Ok(self)
    }

    /// Create and configure a SessionContext with DataFusion settings
    pub fn create_session_context(&self) -> SessionContext {
        use datafusion::config::ConfigOptions;
        use datafusion::execution::context::SessionContext;

        let mut options = ConfigOptions::new();
        let _ = options.set("datafusion.sql_parser.enable_information_schema", "true");
        
        // Enable Parquet statistics for better query optimization with Delta Lake
        // These settings ensure DataFusion uses file and column statistics for pruning
        let _ = options.set("datafusion.execution.parquet.enable_statistics", "true");
        let _ = options.set("datafusion.execution.parquet.pushdown_filters", "true");
        let _ = options.set("datafusion.execution.parquet.enable_page_index", "true");
        let _ = options.set("datafusion.execution.parquet.pruning", "true");
        let _ = options.set("datafusion.execution.parquet.skip_metadata", "false");
        
        // Enable general statistics collection for query optimization
        let _ = options.set("datafusion.execution.collect_statistics", "true");
        
        // Enable bloom filter pruning if available in Parquet files
        let _ = options.set("datafusion.execution.parquet.bloom_filter_on_read", "true");
        
        // Time-series optimized settings
        // Larger batch size for better throughput with time-series data
        let _ = options.set("datafusion.execution.batch_size", "8192");
        
        // Optimize for sorted data (timestamps are typically sorted)
        let _ = options.set("datafusion.optimizer.prefer_existing_sort", "true");
        
        // Enable repartition for better parallel aggregations
        let _ = options.set("datafusion.optimizer.repartition_aggregations", "true");
        
        // Disable round-robin repartitioning to maintain sort order
        let _ = options.set("datafusion.optimizer.enable_round_robin_repartition", "false");
        
        // Enable filter and limit pushdown optimizations
        let _ = options.set("datafusion.optimizer.filter_null_join_keys", "true");
        let _ = options.set("datafusion.optimizer.skip_failed_rules", "false");
        
        // Memory management for large time-series queries
        let _ = options.set("datafusion.execution.coalesce_batches", "true");
        let _ = options.set("datafusion.execution.coalesce_target_batch_size", "8192");
        
        // Enable all optimizer rules for maximum optimization
        let _ = options.set("datafusion.optimizer.max_passes", "5");
        
        SessionContext::new_with_config(options.into())
    }

    /// Setup the session context with tables and register DataFusion tables
    pub fn setup_session_context(&self, ctx: &mut SessionContext) -> DFResult<()> {
        use crate::schema_loader::registry;

        // Get batch queue from the app state if available
        let batch_queue = self.batch_queue.as_ref().map(Arc::clone);

        // Register a routing table for each schema in the registry
        let registry = registry();
        for table_name in registry.list_tables() {
            if let Some(schema) = registry.get(&table_name) {
                let routing_table = ProjectRoutingTable::new(
                    "default".to_string(),
                    Arc::new(self.clone()),
                    schema.schema_ref(),
                    batch_queue.clone(),
                    table_name.clone(),
                );

                ctx.register_table(&table_name, Arc::new(routing_table))?;
                info!("Registered ProjectRoutingTable for table '{}' with SessionContext", table_name);
            }
        }

        self.register_pg_settings_table(ctx)?;
        self.register_set_config_udf(ctx);
        self.register_json_functions(ctx);

        Ok(())
    }

    /// Register PostgreSQL settings table for compatibility
    pub fn register_pg_settings_table(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("setting", DataType::Utf8, false),
        ]));

        let names = vec![
            "TimeZone".to_string(),
            "client_encoding".to_string(),
            "datestyle".to_string(),
            "client_min_messages".to_string(),
            // Add more PostgreSQL settings that clients might try to set
            "lc_monetary".to_string(),
            "lc_numeric".to_string(),
            "lc_time".to_string(),
            "standard_conforming_strings".to_string(),
            "application_name".to_string(),
            "search_path".to_string(),
        ];

        let settings = vec![
            "UTC".to_string(),
            "UTF8".to_string(),
            "ISO, MDY".to_string(),
            "notice".to_string(),
            // Default values for the additional settings
            "C".to_string(),
            "C".to_string(),
            "C".to_string(),
            "on".to_string(),
            "TimeFusion".to_string(),
            "public".to_string(),
        ];

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(names)), Arc::new(StringArray::from(settings))])?;

        ctx.register_batch("pg_settings", batch)?;
        Ok(())
    }

    /// Register set_config UDF for PostgreSQL compatibility
    pub fn register_set_config_udf(&self, ctx: &SessionContext) {
        use datafusion::arrow::array::{StringArray, StringBuilder};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility};

        let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let param_value_array = match &args[1] {
                ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>().expect("set_config second arg must be a StringArray"),
                _ => panic!("set_config second arg must be an array"),
            };

            let mut builder = StringBuilder::new();
            for i in 0..param_value_array.len() {
                if param_value_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(param_value_array.value(i));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        });

        let set_config_udf = create_udf(
            "set_config",
            vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
            DataType::Utf8,
            Volatility::Volatile,
            set_config_fn,
        );

        ctx.register_udf(set_config_udf);
    }

    /// Register JSON functions from datafusion-functions-json
    pub fn register_json_functions(&self, ctx: &mut SessionContext) {
        datafusion_functions_json::register_all(ctx).expect("Failed to register JSON functions");
        info!("Registered JSON functions with SessionContext");
    }

    pub async fn resolve_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // First check if table already exists
        {
            let project_configs = self.project_configs.read().await;
            if let Some(table) = project_configs.get(&(project_id.to_string(), table_name.to_string())) {
                // Check if we have a recent write that might not be visible yet
                let last_written_version = {
                    let versions = self.last_written_versions.read().await;
                    versions.get(&(project_id.to_string(), table_name.to_string())).cloned()
                };
                
                // Check current version without holding the lock too long
                let current_version = table.read().await.version();
                
                // Only update if we don't have a recent write or if the table version is behind
                let should_update = match (current_version, last_written_version) {
                    (Some(current), Some(last)) => {
                        let needs_update = current < last;
                        debug!("Version check for {}/{}: current={}, last_written={}, needs_update={}", 
                               project_id, table_name, current, last, needs_update);
                        needs_update
                    }
                    (None, Some(last)) => {
                        debug!("No current version for {}/{}, but last_written={}, will skip update", project_id, table_name, last);
                        // If we have a last written version but no current version, it means
                        // we just wrote to a new table and it hasn't been loaded yet
                        false
                    }
                    (Some(current), None) => {
                        debug!("Current version {} for {}/{}, no last written, will update", current, project_id, table_name);
                        true
                    }
                    (None, None) => {
                        debug!("No version info for {}/{}, will update", project_id, table_name);
                        true
                    }
                };
                
                if should_update {
                    self.update_table(table, project_id, table_name)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to update table: {}", e)))?;
                } else {
                    debug!("Skipping update for {}/{} - using cached version", project_id, table_name);
                }
                
                return Ok(Arc::clone(table));
            }
        }

        // Table doesn't exist, try to create it
        self.get_or_create_table(project_id, table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create table: {}", e)))
    }

    pub async fn get_or_create_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Check if table already exists before trying to create
        {
            let configs = self.project_configs.read().await;
            if let Some(table) = configs.get(&(project_id.to_string(), table_name.to_string())) {
                return Ok(Arc::clone(table));
            }
        }
        // Try to reload configs from database if we have a pool (lazy loading)
        if let Some(ref pool) = self.config_pool {
            if let Ok(new_configs) = Self::load_storage_configs(pool).await {
                let mut configs = self.storage_configs.write().await;
                *configs = new_configs;
            }
        }

        // Check if we have specific config for this project
        let configs = self.storage_configs.read().await;
        let (storage_uri, storage_options) = if let Some(config) = configs.get(&(project_id.to_string(), table_name.to_string())) {
            // Use project-specific S3 settings
            let storage_uri = format!(
                "s3://{}/{}/?endpoint={}",
                config.s3_bucket,
                config.s3_prefix,
                config
                    .s3_endpoint
                    .as_ref()
                    .unwrap_or(&self.default_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string()))
            );

            let mut storage_options = HashMap::new();
            storage_options.insert("aws_access_key_id".to_string(), config.s3_access_key_id.clone());
            storage_options.insert("aws_secret_access_key".to_string(), config.s3_secret_access_key.clone());
            storage_options.insert("aws_region".to_string(), config.s3_region.clone());
            if let Some(ref endpoint) = config.s3_endpoint {
                storage_options.insert("aws_endpoint".to_string(), endpoint.clone());
            }

            (storage_uri, storage_options)
        } else if let Some(ref bucket) = self.default_s3_bucket {
            // No specific config, use default bucket with environment credentials
            let prefix = self.default_s3_prefix.as_ref().unwrap();
            let endpoint = self.default_s3_endpoint.as_ref().unwrap();
            let storage_uri = format!("s3://{}/{}/projects/{}/{}/?endpoint={}", bucket, prefix, project_id, table_name, endpoint);
            
            // Populate storage options with AWS credentials from environment
            let mut storage_options = HashMap::new();
            if let Ok(access_key) = env::var("AWS_ACCESS_KEY_ID") {
                storage_options.insert("aws_access_key_id".to_string(), access_key);
            }
            if let Ok(secret_key) = env::var("AWS_SECRET_ACCESS_KEY") {
                storage_options.insert("aws_secret_access_key".to_string(), secret_key);
            }
            if let Ok(region) = env::var("AWS_DEFAULT_REGION") {
                storage_options.insert("aws_region".to_string(), region);
            }
            if let Some(ref endpoint) = self.default_s3_endpoint {
                storage_options.insert("aws_endpoint".to_string(), endpoint.clone());
            }
            
            (storage_uri, storage_options)
        } else {
            return Err(anyhow::anyhow!(
                "No configuration for project '{}' table '{}' and no default S3 bucket set",
                project_id,
                table_name
            ));
        };

        info!(
            "Creating or loading table for project '{}' table '{}' at: {}",
            project_id, table_name, storage_uri
        );

        // Hold a write lock during table creation to prevent concurrent creation
        let mut configs = self.project_configs.write().await;

        // Double-check after acquiring write lock
        if let Some(table) = configs.get(&(project_id.to_string(), table_name.to_string())) {
            return Ok(Arc::clone(table));
        }

        // Create the base S3 object store
        let base_store = self.create_object_store(&storage_uri, &storage_options).await?;
        
        // Wrap with the shared Foyer cache
        let cached_store = if let Some(ref shared_cache) = self.object_store_cache {
            // Create a new wrapper around the base store using our shared cache
            // This allows the same cache to be used across all tables
            Arc::new(FoyerObjectStoreCache::new_with_shared_cache(base_store, shared_cache)) as Arc<dyn object_store::ObjectStore>
        } else {
            return Err(anyhow::anyhow!("Shared Foyer cache not initialized"));
        };
        
        // Try to load or create the table with the cached object store
        let table = match DeltaTableBuilder::from_uri(&storage_uri)
            .with_storage_backend(cached_store.clone(), Url::parse(&storage_uri)?)
            .with_storage_options(storage_options.clone())
            .with_allow_http(true)
            .load()
            .await
        {
            Ok(table) => {
                info!("Loaded existing table for project '{}' table '{}'", project_id, table_name);
                table
            }
            Err(load_err) => {
                info!(
                    "Table doesn't exist for project '{}' table '{}', creating new table. err: {:?}",
                    project_id, table_name, load_err
                );

                let schema = get_schema(table_name).unwrap_or_else(get_default_schema);

                // Try to create the table with retry logic for concurrent creation
                let mut create_attempts = 0;
                loop {
                    create_attempts += 1;

                    let delta_ops = DeltaOps::try_from_uri(&storage_uri).await?;
                    let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));

                    match delta_ops
                        .create()
                        .with_columns(schema.columns().unwrap_or_default())
                        .with_partition_columns(schema.partitions.clone())
                        .with_storage_options(storage_options.clone())
                        .with_commit_properties(commit_properties)
                        .await
                    {
                        Ok(table) => break table,
                        Err(create_err) => {
                            let err_str = create_err.to_string();
                            if (err_str.contains("already exists") || err_str.contains("version 0")) && create_attempts < 3 {
                                // Table was created by another process, try to load it
                                debug!("Table creation conflict, attempting to load existing table (attempt {})", create_attempts);
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                                // Try to load the table that was just created
                                match DeltaTableBuilder::from_uri(&storage_uri)
                                    .with_storage_backend(cached_store.clone(), Url::parse(&storage_uri)?)
                                    .with_storage_options(storage_options.clone())
                                    .with_allow_http(true)
                                    .load()
                                    .await
                                {
                                    Ok(table) => break table,
                                    Err(reload_err) => {
                                        debug!("Failed to load table after creation conflict: {:?}", reload_err);
                                        continue;
                                    }
                                }
                            } else {
                                return Err(anyhow::anyhow!("Failed to create table: {}", create_err));
                            }
                        }
                    }
                }
            }
        };

        let table_arc = Arc::new(RwLock::new(table));

        // Store in cache (we already have the write lock)
        configs.insert((project_id.to_string(), table_name.to_string()), Arc::clone(&table_arc));

        Ok(table_arc)
    }

    /// Create an object store for the given URI and storage options
    async fn create_object_store(
        &self,
        storage_uri: &str,
        storage_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        use object_store::aws::AmazonS3Builder;
        
        // Parse the S3 URI to extract bucket and prefix
        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;
        
        // Build S3 configuration
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket);
        
        // Apply storage options
        if let Some(access_key) = storage_options.get("aws_access_key_id") {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = storage_options.get("aws_secret_access_key") {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = storage_options.get("aws_region") {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("aws_endpoint") {
            builder = builder.with_endpoint(endpoint);
            // If endpoint is HTTP, allow HTTP connections
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        
        // Use environment variables as fallback
        if storage_options.get("aws_access_key_id").is_none() {
            if let Ok(access_key) = env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_access_key_id(access_key);
            }
        }
        if storage_options.get("aws_secret_access_key").is_none() {
            if let Ok(secret_key) = env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_secret_access_key(secret_key);
            }
        }
        if storage_options.get("aws_region").is_none() {
            if let Ok(region) = env::var("AWS_DEFAULT_REGION") {
                builder = builder.with_region(region);
            }
        }
        
        // Check if we need to use environment variable for endpoint and allow HTTP
        if storage_options.get("aws_endpoint").is_none() {
            if let Ok(endpoint) = env::var("AWS_S3_ENDPOINT") {
                builder = builder.with_endpoint(&endpoint);
                if endpoint.starts_with("http://") {
                    builder = builder.with_allow_http(true);
                }
            }
        }
        
        let store = builder.build()?;
        Ok(Arc::new(store))
    }

    pub async fn insert_records_batch(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool) -> Result<()> {
        let enable_queue = env::var("ENABLE_BATCH_QUEUE").unwrap_or_else(|_| "false".to_string()) == "true";

        if !skip_queue && enable_queue && self.batch_queue.is_some() {
            let queue = self.batch_queue.as_ref().unwrap();
            for batch in batches {
                if let Err(e) = queue.queue(batch) {
                    return Err(anyhow::anyhow!("Queue error: {}", e));
                }
            }
            return Ok(());
        }

        // Extract project_id from first batch if not provided
        let project_id = if project_id.is_empty() && !batches.is_empty() {
            extract_project_id(&batches[0]).unwrap_or_else(|| "default".to_string())
        } else if project_id.is_empty() {
            "default".to_string()
        } else {
            project_id.to_string()
        };

        // Use provided table_name or default to otel_logs_and_spans
        let table_name = if table_name.is_empty() { "otel_logs_and_spans".to_string() } else { table_name.to_string() };

        // Get or create the table
        let table_ref = self.get_or_create_table(&project_id, &table_name).await?;

        // Get the appropriate schema for this table
        let schema = get_schema(&table_name).unwrap_or_else(get_default_schema);

        let writer_properties = Self::create_writer_properties();

        // Retry logic for concurrent writes
        let max_retries = 5;
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < max_retries {
            // Hold the write lock for the entire operation to prevent concurrent conflicts
            let mut table = table_ref.write().await;

            // Update the table to get the latest version before writing
            if let Err(e) = table.update().await {
                debug!("Failed to update table before write (attempt {}): {}", retry_count + 1, e);
            }

            let write_op = DeltaOps(table.clone())
                .write(batches.clone())
                .with_partition_columns(schema.partitions.clone())
                .with_writer_properties(writer_properties.clone());

            match write_op.await {
                Ok(new_table) => {
                    // Track the version we just wrote
                    if let Some(version) = new_table.version() {
                        // Store the last written version for read-after-write consistency
                        let mut versions = self.last_written_versions.write().await;
                        versions.insert((project_id.clone(), table_name.clone()), version);
                        debug!("Stored last written version for {}/{}: {}", project_id, table_name, version);
                    } else {
                        debug!("WARNING: No version available after write for {}/{}", project_id, table_name);
                    }
                    
                    *table = new_table;
                    
                    // Invalidate statistics cache after successful write
                    drop(table); // Release write lock before async operation
                    self.statistics_extractor.invalidate(&project_id, &table_name).await;
                    debug!("Invalidated statistics cache after write to {}/{}", project_id, table_name);
                    
                    return Ok(());
                }
                Err(e) => {
                    let error_str = e.to_string();
                    if error_str.contains("already exists") || error_str.contains("conflict") || error_str.contains("version") {
                        // This is a version conflict, retry
                        retry_count += 1;
                        last_error = Some(e);
                        debug!("Delta write conflict detected, retrying... (attempt {}/{})", retry_count, max_retries);

                        // Short backoff before retry
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * retry_count as u64)).await;

                        // Drop the lock and try to reload the table
                        drop(table);

                        // Force a table reload on conflict
                        if let Err(reload_err) = table_ref.write().await.update().await {
                            debug!("Failed to reload table after conflict: {}", reload_err);
                        }
                    } else {
                        // Non-retryable error
                        return Err(anyhow::anyhow!("Delta write failed: {}", e));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Delta write failed after {} retries: {}",
            max_retries,
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Optimize the Delta table using Z-ordering on timestamp and id columns
    /// This improves query performance for time-based queries
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, _target_size: Option<i64>) -> Result<()> {
        // Log the start of the optimization operation
        let start_time = std::time::Instant::now();
        info!("Starting Delta table optimization with Z-ordering");

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Get configurable target size
        let target_size = env::var("TIMEFUSION_OPTIMIZE_TARGET_SIZE")
            .unwrap_or_else(|_| DEFAULT_OPTIMIZE_TARGET_SIZE.to_string())
            .parse::<i64>()
            .unwrap_or(DEFAULT_OPTIMIZE_TARGET_SIZE);

        // Run optimize operation with Z-order on the timestamp and id columns
        let writer_properties = Self::create_writer_properties();

        // Note: Z-order functionality is achieved through sorting_columns in writer_properties
        let optimize_result = DeltaOps(table_clone)
            .optimize()
            .with_type(deltalake::operations::optimize::OptimizeType::ZOrder(
                get_default_schema().z_order_columns.clone(),
            ))
            .with_target_size(target_size)
            .with_writer_properties(writer_properties)
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                let duration = start_time.elapsed();
                info!(
                    "Optimization completed in {:?}: {} files removed, {} files added, {} partitions optimized, {} total files considered, {} files skipped",
                    duration,
                    metrics.num_files_removed,
                    metrics.num_files_added,
                    metrics.partitions_optimized,
                    metrics.total_considered_files,
                    metrics.total_files_skipped
                );

                // Log performance metrics for monitoring
                if metrics.num_files_removed > 0 {
                    let compression_ratio = metrics.num_files_removed as f64 / metrics.num_files_added as f64;
                    info!("Optimization compression ratio: {:.2}x", compression_ratio);
                }

                // Update the table reference with the optimized version
                let mut table = table_ref.write().await;
                *table = new_table;

                Ok(())
            }
            Err(e) => {
                error!("Optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Table optimization failed: {}", e))
            }
        }
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    async fn vacuum_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) {
        // Log the start of the vacuum operation
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match DeltaOps(table_clone)
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            .await
        {
            Ok((_, metrics)) => {
                let duration = start_time.elapsed();
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed in {:?}, deleted {} files", duration, files_deleted);

                // Log file sizes for monitoring storage savings
                if !metrics.files_deleted.is_empty() {
                    let _total_size: u64 = metrics
                        .files_deleted
                        .iter()
                        .filter_map(|_path| {
                            // Extract size from path if available
                            // This is a simplified approach - in production you might want to query actual file sizes
                            None::<u64>
                        })
                        .sum();
                    debug!("Vacuum operation details: {:?}", metrics.files_deleted);
                }

                // Update the table reference with the vacuumed version
                let mut table = table_ref.write().await;
                if let Ok(()) = table.update().await {
                    info!("Table updated after vacuum");
                } else {
                    error!("Failed to update table after vacuum");
                }
            }
            Err(e) => error!("Vacuum operation failed: {}", e),
        }
    }
    
    /// Get table statistics using the statistics extractor
    pub async fn get_table_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        // Get the schema for this table
        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
        let schema = schema_def.schema_ref();
        self.statistics_extractor.extract_statistics(table, project_id, table_name, &schema).await
    }
    
    /// Clear the statistics cache
    pub async fn clear_statistics_cache(&self) {
        self.statistics_extractor.clear_cache().await
    }
    
    /// Invalidate statistics for a specific table
    pub async fn invalidate_table_statistics(&self, project_id: &str, table_name: &str) {
        self.statistics_extractor.invalidate(project_id, table_name).await
    }
    
    /// Gracefully shutdown the database, including cache and maintenance tasks
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down TimeFusion database...");
        
        // Cancel maintenance tasks
        self.maintenance_shutdown.cancel();
        
        // Shutdown batch queue if present
        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            queue.shutdown().await;
        }
        
        // Log final cache stats and shutdown cache
        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown().await?;
        }
        
        // Close PostgreSQL connection pool if present
        if let Some(ref pool) = self.config_pool {
            pool.close().await;
        }
        
        info!("Database shutdown complete");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    table_name: String,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>, table_name: String,
    ) -> Self {
        Self {
            default_project,
            database,
            schema,
            _batch_queue: batch_queue,
            table_name,
        }
    }

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        for filter in filters {
            if let Some(project_id) = self.extract_project_id(filter) {
                return Some(project_id);
            }
        }
        None
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[allow(clippy::only_used_in_recursion)]
    fn extract_project_id(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(value)), None)) = (left.as_ref(), right.as_ref()) {
                    if col.name == "project_id" {
                        return Some(value.clone());
                    }
                }
                if let (Expr::Literal(ScalarValue::Utf8(Some(value)), None), Expr::Column(col)) = (left.as_ref(), right.as_ref()) {
                    if col.name == "project_id" {
                        return Some(value.clone());
                    }
                }
                None
            }
            Expr::Not(inner) => self.extract_project_id(inner),
            _ => None,
        }
    }

    /// Determines if a filter can be pushed down exactly to Delta Lake
    fn is_exact_pushdown_filter(expr: &Expr) -> bool {
        match expr {
            // AND expressions are exact if all parts are exact (check this first)
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
                Self::is_exact_pushdown_filter(left) && Self::is_exact_pushdown_filter(right)
            }
            // Simple column comparisons are exact
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let is_column_literal = matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Column(_), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(_))
                );
                
                let is_supported_op = matches!(
                    op,
                    Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | 
                    Operator::Gt | Operator::GtEq
                );
                
                if is_column_literal && is_supported_op {
                    // Check if it's a partition column or indexed column
                    if let Expr::Column(col) = left.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                }
                false
            }
            // IS NULL/IS NOT NULL are exact
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                matches!(inner.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            // IN lists are exact for pushdown columns
            Expr::InList(in_list) => {
                matches!(in_list.expr.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            _ => false,
        }
    }

    /// Checks if a column supports exact pushdown (partitions, sorted columns, indexed columns)
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(
            column_name,
            "project_id" | "date" | "timestamp" | "id" | "level" | "status_code" | 
            "resource___service___name" | "name" | "duration"
        )
    }
    
    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::optimizers::time_range_partition_pruner;
        
        let mut optimized_filters = Vec::new();
        let mut has_date_filter = false;
        
        // First, check if we already have a date filter to avoid duplicates
        for filter in filters {
            if Self::is_date_filter(filter) {
                has_date_filter = true;
            }
            optimized_filters.push(filter.clone());
        }
        
        // Only add date filters if we don't already have one
        if !has_date_filter {
            for filter in filters {
                // Check if this is a timestamp filter that needs a date filter added
                if let Some(date_filter) = time_range_partition_pruner::timestamp_to_date_filter(filter) {
                    optimized_filters.push(date_filter);
                    debug!("Added date partition filter for timestamp query optimization");
                }
            }
        }
        
        // Check if project_id filter is present
        if !self.has_project_id_in_filters(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }
        
        Ok(optimized_filters)
    }
    
    /// Check if an expression is a date filter
    fn is_date_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                matches!(left.as_ref(), Expr::Column(col) if col.name == "date")
            }
            _ => false,
        }
    }
    
    /// Check if filters contain a project_id filter
    fn has_project_id_in_filters(&self, filters: &[Expr]) -> bool {
        use crate::optimizers::ProjectIdPushdown;
        ProjectIdPushdown::has_project_id_filter(filters)
    }
    
    /// Get actual statistics from Delta Lake metadata
    async fn get_delta_statistics(&self) -> Result<Statistics> {
        // Get the Delta table for the default project or first available
        let project_id = self.extract_project_id_from_filters(&[])
            .unwrap_or_else(|| self.default_project.clone());
        
        // Try to get the table
        match self.database.resolve_table(&project_id, &self.table_name).await {
            Ok(table_ref) => {
                let table = table_ref.read().await;
                self.database.statistics_extractor
                    .extract_statistics(&table, &project_id, &self.table_name, &self.schema)
                    .await
            }
            Err(e) => {
                debug!("Failed to resolve table for statistics: {}", e);
                Err(anyhow::anyhow!("Failed to get table for statistics"))
            }
        }
    }
}

// Needed by DataSink
impl DisplayAs for ProjectRoutingTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProjectRoutingTable ")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "ProjectRoutingTable ")
            }
        }
    }
}

#[async_trait]
impl DataSink for ProjectRoutingTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let mut total_row_count = 0;
        let mut project_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        // Collect and group batches by project_id
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let project_id = extract_project_id(&batch).unwrap_or_else(|| self.default_project.clone());
            project_batches.entry(project_id).or_default().push(batch);
        }

        if project_batches.is_empty() {
            return Ok(0);
        }

        // Insert batches for each project
        for (project_id, batches) in project_batches {
            let batch_count = batches.len();
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!(
                "write_all: inserting {} batches with {} total rows for project {}",
                batch_count, row_count, project_id
            );

            self.database
                .insert_records_batch(&project_id, &self.table_name, batches, false)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Insert error for project {} table {}: {}", project_id, self.table_name, e)))?;
        }

        debug!("write_all: completed insertion of {} total rows", total_row_count);
        Ok(total_row_count as u64)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl TableProvider for ProjectRoutingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        match self.schema().logically_equivalent_names_and_types(&input.schema()) {
            Ok(_) => debug!("insert_into; Schema validation passed"),
            Err(e) => {
                error!("Schema validation failed: {}", e);
                return Err(e);
            }
        }

        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }

        // Create sink executor but with additional logging
        let sink = DataSinkExec::new(input, Arc::new(self.clone()), None);

        Ok(Arc::new(sink))
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Analyze each filter to determine if it can be pushed down exactly
        Ok(filter
            .iter()
            .map(|f| {
                if Self::is_exact_pushdown_filter(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Apply our custom optimizations to the filters
        let optimized_filters = self.apply_time_series_optimizations(filters)?;
        
        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(&optimized_filters).unwrap_or_else(|| self.default_project.clone());

        // Execute query and create plan with optimized filters
        let delta_table = self.database.resolve_table(&project_id, &self.table_name).await?;
        let table = delta_table.read().await;
        let plan = table.scan(state, projection, &optimized_filters, limit).await?;
        
        Ok(plan)
    }
    fn statistics(&self) -> Option<Statistics> {
        // Use tokio's block_in_place to run async code in sync context
        // This is safe here as statistics are cached and the operation is fast
        tokio::task::block_in_place(|| {
            let runtime = tokio::runtime::Handle::current();
            runtime.block_on(async {
                // Try to get statistics from Delta Lake
                match self.get_delta_statistics().await {
                    Ok(stats) => Some(stats),
                    Err(e) => {
                        debug!("Failed to get Delta Lake statistics: {}", e);
                        // Fall back to conservative estimates
                        Some(Statistics {
                            num_rows: Precision::Inexact(1_000_000),
                            total_byte_size: Precision::Inexact(100_000_000),
                            column_statistics: vec![],
                        })
                    }
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::test_helpers::*;
    use serial_test::serial;

    async fn setup_test_database() -> Result<(Database, SessionContext)> {
        dotenv::dotenv().ok();
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }
        let db = Database::new().await?;
        let mut ctx = db.create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db.setup_session_context(&mut ctx)?;
        Ok((db, ctx))
    }

    #[serial]
    #[tokio::test]
    async fn test_insert_and_query() -> Result<()> {
        let (db, ctx) = setup_test_database().await?;

        // Test basic insert
        let batch = json_to_batch(vec![test_span("test1", "span1", "project1")])?;
        db.insert_records_batch("project1", "otel_logs_and_spans", vec![batch], true).await?;

        // Verify count
        let result = ctx.sql("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = 'project1'").await?.collect().await?;
        use datafusion::arrow::array::AsArray;
        let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(count, 1);

        // Test field selection
        let result = ctx.sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'project1'").await?.collect().await?;
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(0).as_string::<i32>().value(0), "test1");
        assert_eq!(result[0].column(1).as_string::<i32>().value(0), "span1");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_multiple_projects() -> Result<()> {
        let (db, ctx) = setup_test_database().await?;

        // Insert data for multiple projects
        for project in ["project1", "project2", "project3"] {
            let batch = json_to_batch(vec![test_span(&format!("id_{}", project), &format!("span_{}", project), project)])?;
            db.insert_records_batch(project, "otel_logs_and_spans", vec![batch], true).await?;
        }

        // Verify project isolation
        use datafusion::arrow::array::AsArray;
        for project in ["project1", "project2", "project3"] {
            let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}'", project);
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(result[0].column(0).as_string::<i32>().value(0), format!("id_{}", project));
        }

        // Verify total count - need to check across all projects
        let mut total_count = 0;
        for project in ["project1", "project2", "project3"] {
            let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
            let result = ctx.sql(&sql).await?.collect().await?;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            total_count += count;
        }
        assert_eq!(total_count, 3);

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_filtering() -> Result<()> {
        let (db, ctx) = setup_test_database().await?;
        use chrono::Utc;
        use datafusion::arrow::array::AsArray;
        use serde_json::json;

        let now = Utc::now();
        let records = vec![
            json!({
                "timestamp": now.timestamp_micros(),
                "id": "span1",
                "name": "test_span_1",
                "project_id": "test_project",
                "level": "INFO",
                "status_code": "OK",
                "duration": 100_000_000,
                "date": now.date_naive().to_string(),
                "hashes": []
            }),
            json!({
                "timestamp": (now + chrono::Duration::minutes(10)).timestamp_micros(),
                "id": "span2",
                "name": "test_span_2",
                "project_id": "test_project",
                "level": "ERROR",
                "status_code": "ERROR",
                "status_message": "Error occurred",
                "duration": 200_000_000,
                "date": now.date_naive().to_string(),
                "hashes": []
            }),
        ];

        let batch = json_to_batch(records)?;
        db.insert_records_batch("test_project", "otel_logs_and_spans", vec![batch], true).await?;

        // Test filtering by level
        let result = ctx
            .sql("SELECT id FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'")
            .await?
            .collect()
            .await?;
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(0).as_string::<i32>().value(0), "span2");

        // Test filtering by duration
        let result = ctx
            .sql("SELECT id FROM otel_logs_and_spans WHERE project_id = 'test_project' AND duration > 150000000")
            .await?
            .collect()
            .await?;
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(0).as_string::<i32>().value(0), "span2");

        // Test compound filtering
        let result = ctx
            .sql("SELECT id, status_message FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'")
            .await?
            .collect()
            .await?;
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(1).as_string::<i32>().value(0), "Error occurred");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_sql_insert() -> Result<()> {
        let (db, ctx) = setup_test_database().await?;
        use datafusion::arrow::array::AsArray;

        // Insert via API first
        let batch = json_to_batch(vec![test_span("id1", "name1", "default")])?;
        db.insert_records_batch("default", "otel_logs_and_spans", vec![batch], true).await?;

        // Insert via SQL
        let sql = "INSERT INTO otel_logs_and_spans (
                   project_id, date, timestamp, id, hashes, name, level, status_code
                 ) VALUES (
                   'project2', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 
                   'sql_id', ARRAY[], 'sql_name', 'INFO', 'OK'
                 )";
        let result = ctx.sql(sql).await?.collect().await?;
        assert_eq!(result[0].num_rows(), 1);

        // Verify both records exist - need to check both projects
        let mut total_count = 0;
        for project in ["default", "project2"] {
            let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
            let result = ctx.sql(&sql).await?.collect().await?;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            total_count += count;
        }
        assert_eq!(total_count, 2);

        // Verify SQL-inserted record
        let result = ctx
            .sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'project2' AND id = 'sql_id'")
            .await?
            .collect()
            .await?;
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(1).as_string::<i32>().value(0), "sql_name");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_multi_row_sql_insert() -> Result<()> {
        let (_db, ctx) = setup_test_database().await?;
        use datafusion::arrow::array::AsArray;

        // Test multi-row INSERT
        let sql = "INSERT INTO otel_logs_and_spans (
                   project_id, date, timestamp, id, hashes, name, level, status_code
                 ) VALUES 
                 ('project1', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'id1', ARRAY[], 'name1', 'INFO', 'OK'),
                 ('project1', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T11:00:00Z', 'id2', ARRAY[], 'name2', 'INFO', 'OK'),
                 ('project1', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T12:00:00Z', 'id3', ARRAY[], 'name3', 'ERROR', 'ERROR')";

        // Multi-row INSERT returns a count of rows inserted
        let result = ctx.sql(sql).await?.collect().await?;
        let inserted_count = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
        assert_eq!(inserted_count, 3);

        // Verify all 3 records exist
        let sql = "SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = 'project1'";
        let result = ctx.sql(sql).await?.collect().await?;
        let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
        assert_eq!(count, 3);

        // Verify individual records
        let result = ctx.sql("SELECT id, name FROM otel_logs_and_spans WHERE project_id = 'project1' ORDER BY id").await?.collect().await?;
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].column(0).as_string::<i32>().value(0), "id1");
        assert_eq!(result[0].column(0).as_string::<i32>().value(1), "id2");
        assert_eq!(result[0].column(0).as_string::<i32>().value(2), "id3");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_timestamp_operations() -> Result<()> {
        let (db, ctx) = setup_test_database().await?;
        use chrono::Utc;
        use datafusion::arrow::array::AsArray;
        use serde_json::json;

        let base_time = chrono::DateTime::parse_from_rfc3339("2023-01-01T10:00:00Z").unwrap().with_timezone(&Utc);
        let records = vec![
            json!({
                "timestamp": base_time.timestamp_micros(),
                "id": "early",
                "name": "early_span",
                "project_id": "test",
                "date": base_time.date_naive().to_string(),
                "hashes": []
            }),
            json!({
                "timestamp": (base_time + chrono::Duration::hours(2)).timestamp_micros(),
                "id": "late",
                "name": "late_span",
                "project_id": "test",
                "date": base_time.date_naive().to_string(),
                "hashes": []
            }),
        ];

        let batch = json_to_batch(records)?;
        db.insert_records_batch("test", "otel_logs_and_spans", vec![batch], true).await?;

        // First check if any records were inserted - need to specify project_id
        let all_records = ctx.sql("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'test'").await?.collect().await?;
        assert!(!all_records.is_empty(), "No records found in table");

        // Test timestamp filtering - need to include project_id
        let result = ctx
            .sql("SELECT id FROM otel_logs_and_spans WHERE project_id = 'test' AND timestamp > '2023-01-01T11:00:00Z'")
            .await?
            .collect()
            .await?;
        assert!(!result.is_empty(), "Query returned no results");
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].column(0).as_string::<i32>().value(0), "late");

        // Test timestamp formatting - need to include project_id
        let result = ctx
            .sql("SELECT id, to_char(timestamp, '%Y-%m-%d %H:%M') as ts FROM otel_logs_and_spans WHERE project_id = 'test' ORDER BY timestamp")
            .await?
            .collect()
            .await?;
        assert_eq!(result[0].num_rows(), 2);
        assert_eq!(result[0].column(1).as_string::<i32>().value(0), "2023-01-01 10:00");
        assert_eq!(result[0].column(1).as_string::<i32>().value(1), "2023-01-01 12:00");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_concurrent_writes_same_project() -> Result<()> {
        dotenv::dotenv().ok();
        // Use same test environment as other tests
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }

        let db = Database::new().await?;
        let db = Arc::new(db);
        let project_id = format!("concurrent_test_{}", uuid::Uuid::new_v4());

        // Create 10 concurrent write tasks
        let tasks = (0..10).map(|i| {
            let db = Arc::clone(&db);
            let project = project_id.clone();

            tokio::spawn(async move {
                let batch_id = format!("batch_{}", i);
                let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project)])?;

                // Attempt to write
                db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true).await.map(|_| batch_id)
            })
        });

        // Wait for all tasks to complete
        let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
            .collect();

        // All writes should succeed
        let successful_writes: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;

        assert_eq!(successful_writes.len(), 10, "All 10 concurrent writes should succeed");

        // Verify all records were written
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // Give time for Delta to commit

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_concurrent_table_creation() -> Result<()> {
        dotenv::dotenv().ok();
        // Use same test environment as other tests
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }

        let db = Database::new().await?;
        let db = Arc::new(db);

        // Create multiple projects concurrently - each will try to create its own table
        let tasks = (0..5).map(|i| {
            let db = Arc::clone(&db);
            let project_id = format!("project_create_test_{}", i);

            tokio::spawn(async move {
                let batch_id = format!("init_batch_{}", i);
                let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;

                // First write to a project creates the table
                db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await.map(|_| project_id)
            })
        });

        // Wait for all tasks to complete
        let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
            .collect();

        // All table creations should succeed
        let created_projects: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;

        assert_eq!(created_projects.len(), 5, "All 5 projects should be created successfully");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_batch_queue_under_load() -> Result<()> {
        use crate::batch_queue::BatchQueue;

        dotenv::dotenv().ok();
        // Use same test environment as other tests
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }

        let db = Arc::new(Database::new().await?);
        let queue = BatchQueue::new(Arc::clone(&db), 100, 50); // 100ms interval, 50 rows max

        let project_id = format!("queue_test_{}", uuid::Uuid::new_v4());

        // Queue many batches rapidly
        for i in 0..100 {
            let batch_id = format!("queued_batch_{}", i);
            let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;

            // Queue should handle this gracefully
            match queue.queue(batch) {
                Ok(_) => {}
                Err(e) if e.to_string().contains("Queue full") => {
                    // Expected when queue is at capacity
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        // Give queue time to process
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        // Queue shutdown
        queue.shutdown().await;

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_concurrent_mixed_operations() -> Result<()> {
        dotenv::dotenv().ok();
        // Use same test environment as other tests
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
        }

        let db = Database::new().await?;
        let db = Arc::new(db);

        // Mix of different operations happening concurrently
        let project_id = format!("mixed_ops_{}", uuid::Uuid::new_v4());

        let write_tasks = (0..3).map(|i| {
            let db = Arc::clone(&db);
            let project = project_id.clone();

            tokio::spawn(async move {
                for j in 0..5 {
                    let batch_id = format!("writer_{}_batch_{}", i, j);
                    let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project)]).expect("Failed to create test batch");

                    if let Err(e) = db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true).await {
                        eprintln!("Write failed: {}", e);
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                }
            })
        });

        // Run optimize while writes are happening
        let optimize_task = {
            let db = Arc::clone(&db);
            let project = project_id.clone();

            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await; // Let some writes happen first

                // Get the table and optimize it
                if let Ok(table_ref) = db.get_or_create_table(&project, "otel_logs_and_spans").await {
                    let _ = db.optimize_table(&table_ref, Some(1024 * 1024)).await;
                }
            })
        };

        // Wait for all operations to complete
        futures::future::join_all(write_tasks).await;
        optimize_task.await?;

        Ok(())
    }
}
