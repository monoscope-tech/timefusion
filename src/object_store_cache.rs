use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use futures::stream::BoxStream;
use object_store::{
    path::Path, Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult,
};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use foyer::{
    BlockEngineBuilder, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, 
    HybridCachePolicy, IoEngineBuilder, PsyncIoEngineBuilder
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Cache entry with metadata and TTL
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
    #[serde(with = "object_meta_serde")]
    meta: ObjectMeta,
    timestamp_millis: u64,
}

impl CacheValue {
    fn new(data: Vec<u8>, meta: ObjectMeta) -> Self {
        Self {
            data,
            meta,
            timestamp_millis: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        let age_millis = current_millis().saturating_sub(self.timestamp_millis);
        age_millis > ttl.as_millis() as u64
    }
}

fn current_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

mod object_meta_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct SerializedMeta {
        location: String,
        last_modified: i64,
        size: u64,
        e_tag: Option<String>,
        version: Option<String>,
    }

    pub fn serialize<S>(meta: &ObjectMeta, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializedMeta {
            location: meta.location.to_string(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ObjectMeta, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = SerializedMeta::deserialize(deserializer)?;
        Ok(ObjectMeta {
            location: Path::from(s.location),
            last_modified: DateTime::<Utc>::from_timestamp_millis(s.last_modified).unwrap_or(Utc::now()),
            size: s.size,
            e_tag: s.e_tag,
            version: s.version,
        })
    }
}

/// Configuration for the foyer-based object store cache
#[derive(Debug, Clone)]
pub struct FoyerCacheConfig {
    pub memory_size_bytes: usize,
    pub disk_size_bytes: usize,
    pub ttl: Duration,
    pub cache_dir: PathBuf,
    pub shards: usize,
    pub file_size_bytes: usize,
    pub enable_stats: bool,
    /// Size hint for reading parquet metadata from the end of files
    pub parquet_metadata_size_hint: usize,
    /// Memory size for metadata cache in bytes
    pub metadata_memory_size_bytes: usize,
    /// Disk size for metadata cache in bytes
    pub metadata_disk_size_bytes: usize,
    /// Number of shards for metadata cache
    pub metadata_shards: usize,
}

impl Default for FoyerCacheConfig {
    fn default() -> Self {
        Self {
            memory_size_bytes: 536_870_912,    // 512MB
            disk_size_bytes: 107_374_182_400,  // 100GB
            ttl: Duration::from_secs(604_800), // 7 days
            cache_dir: PathBuf::from("/tmp/timefusion_cache"),
            shards: 8,
            file_size_bytes: 16_777_216, // 16MB - good for Parquet files
            enable_stats: true,
            parquet_metadata_size_hint: 1_048_576,      // 1MB - typical size for parquet metadata
            metadata_memory_size_bytes: 536_870_912,    // 512MB
            metadata_disk_size_bytes: 5_368_709_120,    // 5GB
            metadata_shards: 4,                         // Fewer shards for metadata cache
        }
    }
}

impl FoyerCacheConfig {
    /// Create cache config from environment variables
    pub fn from_env() -> Self {
        fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }

        Self {
            memory_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_MEMORY_MB", 512) * 1024 * 1024,
            disk_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_DISK_GB", 100) * 1024 * 1024 * 1024,
            ttl: Duration::from_secs(parse_env("TIMEFUSION_FOYER_TTL_SECONDS", 604800)),
            cache_dir: PathBuf::from(parse_env("TIMEFUSION_FOYER_CACHE_DIR", "/tmp/timefusion_cache".to_string())),
            shards: parse_env("TIMEFUSION_FOYER_SHARDS", 8),
            file_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_FILE_SIZE_MB", 32) * 1024 * 1024,
            enable_stats: parse_env("TIMEFUSION_FOYER_STATS", "true".to_string()).to_lowercase() == "true",
            parquet_metadata_size_hint: parse_env("TIMEFUSION_PARQUET_METADATA_SIZE_HINT", 1_048_576),
            metadata_memory_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_METADATA_MEMORY_MB", 512) * 1024 * 1024,
            metadata_disk_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_METADATA_DISK_GB", 5) * 1024 * 1024 * 1024,
            metadata_shards: parse_env("TIMEFUSION_FOYER_METADATA_SHARDS", 4),
        }
    }

    /// Create a test configuration with sensible defaults for testing
    /// The name parameter is used to create unique cache directories
    pub fn test_config(name: &str) -> Self {
        Self {
            memory_size_bytes: 10 * 1024 * 1024, // 10MB
            disk_size_bytes: 50 * 1024 * 1024,   // 50MB
            ttl: Duration::from_secs(300),
            cache_dir: PathBuf::from(format!("/tmp/test_foyer_{}", name)),
            shards: 2,
            file_size_bytes: 1024 * 1024, // 1MB
            enable_stats: true,
            parquet_metadata_size_hint: 1_048_576,        // 1MB
            metadata_memory_size_bytes: 10 * 1024 * 1024, // 10MB for tests
            metadata_disk_size_bytes: 50 * 1024 * 1024,   // 50MB for tests
            metadata_shards: 2,
        }
    }

    /// Create a test config with specific overrides
    pub fn test_config_with(name: &str, f: impl FnOnce(&mut Self)) -> Self {
        let mut config = Self::test_config(name);
        f(&mut config);
        config
    }
}

/// Statistics for cache operations
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub ttl_expirations: u64,
    pub inner_gets: u64,
    pub inner_puts: u64,
}

/// Combined statistics for both caches
#[derive(Debug, Default, Clone)]
pub struct CombinedCacheStats {
    pub main: CacheStats,
    pub metadata: CacheStats,
}

impl CacheStats {
    fn log(&self) {
        let hit_rate = if self.hits + self.misses > 0 {
            (self.hits as f64 / (self.hits + self.misses) as f64) * 100.0
        } else {
            0.0
        };
        info!(
            "Foyer cache stats - Hit rate: {:.2}%, Hits: {}, Misses: {}, TTL expirations: {}, Inner gets: {}, Inner puts: {}",
            hit_rate, self.hits, self.misses, self.ttl_expirations, self.inner_gets, self.inner_puts
        );
    }
}

type FoyerCache = Arc<HybridCache<String, CacheValue>>;
type StatsRef = Arc<RwLock<CacheStats>>;

/// Shared Foyer cache that can be used across multiple object stores
#[derive(Debug)]
pub struct SharedFoyerCache {
    cache: FoyerCache,
    metadata_cache: FoyerCache,
    stats: StatsRef,
    metadata_stats: StatsRef,
    config: FoyerCacheConfig,
}

impl SharedFoyerCache {
    /// Create a new shared Foyer cache
    pub async fn new(config: FoyerCacheConfig) -> anyhow::Result<Self> {
        info!(
            "Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, ttl: {}s, parquet_metadata_hint: {}KB)",
            config.memory_size_bytes / 1024 / 1024,
            config.disk_size_bytes / 1024 / 1024 / 1024,
            config.ttl.as_secs(),
            config.parquet_metadata_size_hint / 1024
        );

        info!(
            "Initializing metadata cache (memory: {}MB, disk: {}GB, ttl: {}s)",
            config.metadata_memory_size_bytes / 1024 / 1024,
            config.metadata_disk_size_bytes / 1024 / 1024 / 1024,
            config.ttl.as_secs()
        );

        std::fs::create_dir_all(&config.cache_dir)?;
        let metadata_cache_dir = config.cache_dir.join("metadata");
        std::fs::create_dir_all(&metadata_cache_dir)?;

        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.memory_size_bytes)
            .with_shards(config.shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_io_engine(PsyncIoEngineBuilder::new().build().await?)
            .with_engine_config(
                BlockEngineBuilder::new(
                    FsDeviceBuilder::new(&config.cache_dir)
                        .with_capacity(config.disk_size_bytes)
                        .build()?,
                )
                .with_block_size(config.file_size_bytes),
            )
            .build()
            .await?;

        let metadata_cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.metadata_memory_size_bytes)
            .with_shards(config.metadata_shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_io_engine(PsyncIoEngineBuilder::new().build().await?)
            .with_engine_config(
                BlockEngineBuilder::new(
                    FsDeviceBuilder::new(&metadata_cache_dir)
                        .with_capacity(config.metadata_disk_size_bytes)
                        .build()?,
                )
                .with_block_size(config.file_size_bytes),
            )
            .build()
            .await?;

        Ok(Self {
            cache: Arc::new(cache),
            metadata_cache: Arc::new(metadata_cache),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            metadata_stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
        })
    }

    pub async fn get_stats(&self) -> CombinedCacheStats {
        CombinedCacheStats {
            main: self.stats.read().await.clone(),
            metadata: self.metadata_stats.read().await.clone(),
        }
    }

    pub async fn log_stats(&self) {
        info!("Main cache stats:");
        self.stats.read().await.log();
        info!("Metadata cache stats:");
        self.metadata_stats.read().await.log();
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down Foyer cache...");
        self.log_stats().await;
        Ok(())
    }

    /// Invalidate checkpoint cache for a given table URI
    pub fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        // Extract table path from URI (remove s3:// or other prefixes)
        let table_path = if let Some(idx) = table_uri.find("://") { &table_uri[idx + 3..] } else { table_uri };

        // Remove any trailing slashes
        let table_path = table_path.trim_end_matches('/');

        let last_checkpoint_key = format!("{}/_delta_log/_last_checkpoint", table_path);
        info!("Invalidating _last_checkpoint cache for table: {}", table_path);
        self.cache.remove(&last_checkpoint_key);
    }
}

/// Foyer-based hybrid cache implementation for object store
pub struct FoyerObjectStoreCache {
    inner: Arc<dyn ObjectStore>,
    cache: FoyerCache,
    metadata_cache: FoyerCache,
    stats: StatsRef,
    metadata_stats: StatsRef,
    config: FoyerCacheConfig,
    refreshing: Arc<DashSet<String>>,
}

impl FoyerObjectStoreCache {
    pub fn new_with_shared_cache(inner: Arc<dyn ObjectStore>, shared_cache: &SharedFoyerCache) -> Self {
        Self {
            inner,
            cache: shared_cache.cache.clone(),
            metadata_cache: shared_cache.metadata_cache.clone(),
            stats: shared_cache.stats.clone(),
            metadata_stats: shared_cache.metadata_stats.clone(),
            config: shared_cache.config.clone(),
            refreshing: Arc::new(DashSet::new()),
        }
    }

    /// Check if a path is the mutable _last_checkpoint file
    fn is_last_checkpoint(location: &Path) -> bool {
        location.as_ref().contains("_delta_log/_last_checkpoint")
    }

    /// Get the appropriate TTL for a file based on its type
    fn get_ttl_for_path(&self, _location: &Path) -> Duration {
        self.config.ttl
    }

    /// Explicitly invalidate checkpoint cache for a given table
    pub async fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        // Extract table path from URI (remove s3:// or other prefixes)
        let table_path = if let Some(idx) = table_uri.find("://") { &table_uri[idx + 3..] } else { table_uri };

        // Remove any trailing slashes
        let table_path = table_path.trim_end_matches('/');

        let last_checkpoint_path = format!("{}/_delta_log/_last_checkpoint", table_path);
        let cache_key = last_checkpoint_path.clone();
        info!("Explicitly invalidating and refreshing _last_checkpoint cache for table: {}", table_path);

        // Remove from cache first
        self.cache.remove(&cache_key);

        // Immediately fetch and cache the new version
        let location = Path::from(last_checkpoint_path);
        if let Ok(get_result) = self.inner.get(&location).await {
            use futures::TryStreamExt;
            let data = match get_result.payload {
                GetResultPayload::Stream(s) => {
                    if let Ok(chunks) = s.try_collect::<Vec<Bytes>>().await {
                        chunks.concat()
                    } else {
                        vec![]
                    }
                }
                GetResultPayload::File(mut file, _) => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    if file.read_to_end(&mut buf).is_ok() {
                        buf
                    } else {
                        vec![]
                    }
                }
            };
            if !data.is_empty() {
                self.cache.insert(cache_key, CacheValue::new(data, get_result.meta));
                debug!("Proactively refreshed _last_checkpoint cache after invalidation");
            }
        }
    }

    pub async fn new(inner: Arc<dyn ObjectStore>, config: FoyerCacheConfig) -> anyhow::Result<Self> {
        let shared_cache = SharedFoyerCache::new(config).await?;
        Ok(Self::new_with_shared_cache(inner, &shared_cache))
    }

    async fn update_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut CacheStats),
    {
        f(&mut *self.stats.write().await);
    }

    async fn update_metadata_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut CacheStats),
    {
        f(&mut *self.metadata_stats.write().await);
    }

    fn make_cache_key(location: &Path) -> String {
        location.to_string()
    }

    fn make_range_cache_key(location: &Path, range: &Range<u64>) -> String {
        format!("{}#range:{}-{}", location, range.start, range.end)
    }

    /// Invalidate all metadata cache entries for a given file
    async fn invalidate_metadata_cache(&self, location: &Path) {
        // We can't enumerate all possible range keys, but we can at least
        // invalidate the most common metadata ranges
        let file_meta = match self.inner.head(location).await {
            Ok(meta) => meta,
            Err(_) => return,
        };

        let file_size = file_meta.size;
        let metadata_size_hint = self.config.parquet_metadata_size_hint as u64;

        // Invalidate common metadata ranges
        for offset in [8, 1024, 4096, 8192, metadata_size_hint] {
            if offset < file_size {
                let start = file_size.saturating_sub(offset);
                let range = start..file_size;
                let cache_key = Self::make_range_cache_key(location, &range);
                self.metadata_cache.remove(&cache_key);
            }
        }

        debug!("Invalidated metadata cache entries for: {}", location);
    }

    fn make_get_result(data: Bytes, meta: ObjectMeta) -> GetResult {
        let data_len = data.len() as u64;
        GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(data) }))),
            meta,
            attributes: Attributes::new(),
            range: 0..data_len,
        }
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down foyer hybrid cache");
        self.cache.close().await?;
        self.metadata_cache.close().await?;
        Ok(())
    }

    pub async fn get_stats(&self) -> CombinedCacheStats {
        CombinedCacheStats {
            main: self.stats.read().await.clone(),
            metadata: self.metadata_stats.read().await.clone(),
        }
    }

    pub async fn reset_stats(&self) {
        *self.stats.write().await = CacheStats::default();
        *self.metadata_stats.write().await = CacheStats::default();
    }
}

#[async_trait]
impl ObjectStore for FoyerObjectStoreCache {
    async fn put(&self, location: &Path, payload: PutPayload) -> ObjectStoreResult<PutResult> {
        self.update_stats(|s| s.inner_puts += 1).await;

        let payload_size = payload.content_length();
        let is_parquet = location.as_ref().ends_with(".parquet");
        
        debug!(
            "S3 PUT request starting: {} (size: {} bytes, parquet: {})",
            location,
            payload_size,
            is_parquet
        );

        // Write to S3 first without removing from cache (to avoid cache stampede)
        let start_time = std::time::Instant::now();
        let result = self.inner.put(location, payload).await?;
        let duration = start_time.elapsed();
        
        debug!(
            "S3 PUT request completed: {} (size: {} bytes, duration: {}ms, parquet: {})",
            location,
            payload_size,
            duration.as_millis(),
            is_parquet
        );

        // After successful write, update the cache with the new data
        self.update_stats(|s| s.inner_gets += 1).await;
        if let Ok(get_result) = self.inner.get(location).await {
            use futures::TryStreamExt;
            let data = match get_result.payload {
                GetResultPayload::Stream(s) => {
                    if let Ok(chunks) = s.try_collect::<Vec<Bytes>>().await {
                        chunks.concat()
                    } else {
                        vec![]
                    }
                }
                GetResultPayload::File(mut file, _) => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    if file.read_to_end(&mut buf).is_ok() {
                        buf
                    } else {
                        vec![]
                    }
                }
            };
            if !data.is_empty() {
                let cache_key = Self::make_cache_key(location);
                let size = get_result.meta.size;
                // This will atomically replace the old entry (if any) with the new one
                self.cache.insert(cache_key, CacheValue::new(data, get_result.meta));
                debug!("Updated cache after write: {} (size: {} bytes)", location, size);
            }
        }

        // Invalidate metadata cache entries for this file
        if location.as_ref().ends_with(".parquet") {
            self.invalidate_metadata_cache(location).await;
        }

        Ok(result)
    }

    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.update_stats(|s| s.inner_puts += 1).await;

        // Write to S3 first without removing from cache (to avoid cache stampede)
        let result = self.inner.put_opts(location, payload, opts).await?;

        // After successful write, update the cache with the new data
        if let Ok(get_result) = self.inner.get(location).await {
            use futures::TryStreamExt;
            let data = match get_result.payload {
                GetResultPayload::Stream(s) => {
                    if let Ok(chunks) = s.try_collect::<Vec<Bytes>>().await {
                        chunks.concat()
                    } else {
                        vec![]
                    }
                }
                GetResultPayload::File(mut file, _) => {
                    use std::io::Read;
                    let mut buf = Vec::new();
                    if file.read_to_end(&mut buf).is_ok() {
                        buf
                    } else {
                        vec![]
                    }
                }
            };
            if !data.is_empty() {
                let cache_key = Self::make_cache_key(location);
                let size = get_result.meta.size;
                // This will atomically replace the old entry (if any) with the new one
                self.cache.insert(cache_key, CacheValue::new(data, get_result.meta));
                debug!("Updated cache after write: {} (size: {} bytes)", location, size);
            }
        }

        // Invalidate metadata cache entries for this file
        if location.as_ref().ends_with(".parquet") {
            self.invalidate_metadata_cache(location).await;
        }

        Ok(result)
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let cache_key = Self::make_cache_key(location);

        // Try cache first
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();

            // Use appropriate TTL based on file type
            let ttl = self.get_ttl_for_path(location);

            // Special handling for _last_checkpoint: stale-while-revalidate
            if Self::is_last_checkpoint(location) && !value.is_expired(ttl) {
                self.update_stats(|s| s.hits += 1).await;

                // Check if older than 5 seconds
                let age_millis = current_millis().saturating_sub(value.timestamp_millis);
                if age_millis > 5000 {
                    // Trigger background refresh if not already refreshing
                    if self.refreshing.insert(cache_key.clone()) {
                        let inner = self.inner.clone();
                        let cache = self.cache.clone();
                        let refreshing = self.refreshing.clone();
                        let location = location.clone();
                        let key = cache_key.clone();

                        tokio::spawn(async move {
                            debug!("Background refresh for _last_checkpoint: {}", location);
                            if let Ok(result) = inner.get(&location).await {
                                // Collect payload for caching
                                use futures::TryStreamExt;
                                let data = match result.payload {
                                    GetResultPayload::Stream(s) => {
                                        if let Ok(chunks) = s.try_collect::<Vec<Bytes>>().await {
                                            chunks.concat()
                                        } else {
                                            vec![]
                                        }
                                    }
                                    GetResultPayload::File(mut file, _) => {
                                        use std::io::Read;
                                        let mut buf = Vec::new();
                                        if file.read_to_end(&mut buf).is_ok() {
                                            buf
                                        } else {
                                            vec![]
                                        }
                                    }
                                };
                                if !data.is_empty() {
                                    cache.insert(key.clone(), CacheValue::new(data, result.meta));
                                }
                            }
                            refreshing.remove(&key);
                        });
                    }

                    debug!("Foyer cache HIT (stale-while-revalidate) for: {} (age: {}ms)", location, age_millis);
                } else {
                    debug!("Foyer cache HIT (fresh) for: {} (age: {}ms)", location, age_millis);
                }

                // Always return cached value immediately
                return Ok(Self::make_get_result(Bytes::from(value.data.clone()), value.meta.clone()));
            }

            // Regular cache expiration check for non-checkpoint files
            if value.is_expired(ttl) {
                self.update_stats(|s| s.ttl_expirations += 1).await;
                self.cache.remove(&cache_key);
                info!(
                    "Foyer cache EXPIRED for: {} (TTL: {}s, age: {}ms)",
                    location,
                    ttl.as_secs(),
                    current_millis().saturating_sub(value.timestamp_millis)
                );
            } else {
                self.update_stats(|s| s.hits += 1).await;
                let is_parquet = location.as_ref().ends_with(".parquet");
                debug!(
                    "Foyer cache HIT for: {} (avoiding S3 access, parquet={}, TTL={}s, age={}ms, size={} bytes)",
                    location,
                    is_parquet,
                    ttl.as_secs(),
                    current_millis().saturating_sub(value.timestamp_millis),
                    value.data.len()
                );
                return Ok(Self::make_get_result(Bytes::from(value.data.clone()), value.meta.clone()));
            }
        }

        // Cache miss - fetch from inner store
        self.update_stats(|s| {
            s.misses += 1;
            s.inner_gets += 1;
        })
        .await;
        let is_parquet = location.as_ref().ends_with(".parquet");
        let ttl = self.get_ttl_for_path(location);
        debug!(
            "Foyer cache MISS for: {} (fetching from S3, parquet={}, TTL={}s)",
            location,
            is_parquet,
            ttl.as_secs()
        );

        let start_time = std::time::Instant::now();
        let result = self.inner.get(location).await?;
        let duration = start_time.elapsed();
        
        debug!(
            "S3 GET request: {} (size: {} bytes, duration: {}ms, parquet: {})",
            location,
            result.meta.size,
            duration.as_millis(),
            is_parquet
        );

        // Collect payload for caching
        use futures::TryStreamExt;
        let data = match result.payload {
            GetResultPayload::Stream(s) => {
                let chunks: Vec<Bytes> = s.try_collect().await?;
                chunks.concat()
            }
            GetResultPayload::File(mut file, _) => {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).map_err(|e| object_store::Error::Generic {
                    store: "cache",
                    source: Box::new(e),
                })?;
                buf
            }
        };

        self.cache.insert(cache_key, CacheValue::new(data.clone(), result.meta.clone()));
        Ok(Self::make_get_result(Bytes::from(data), result.meta))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // Bypass cache for complex requests
        if options.range.is_some()
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
        {
            return self.inner.get_opts(location, options).await;
        }
        self.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        let is_parquet = location.as_ref().ends_with(".parquet");

        // First check if we have the full file cached
        let full_cache_key = Self::make_cache_key(location);
        if let Ok(Some(entry)) = self.cache.get(&full_cache_key).await {
            let value = entry.value();
            let ttl = self.get_ttl_for_path(location);
            if !value.is_expired(ttl) && range.end <= value.data.len() as u64 {
                self.update_stats(|s| s.hits += 1).await;
                debug!(
                    "Foyer cache HIT (full file) for range: {} (range: {}..{}, size: {} bytes, parquet={}, age={}ms)",
                    location,
                    range.start,
                    range.end,
                    range.end - range.start,
                    is_parquet,
                    current_millis().saturating_sub(value.timestamp_millis)
                );
                return Ok(Bytes::from(value.data[range.start as usize..range.end as usize].to_vec()));
            }
        }

        // For Parquet files, implement smart caching based on the range
        if is_parquet {
            // First get the file size to determine if this is a metadata request
            let file_meta = match self.inner.head(location).await {
                Ok(meta) => meta,
                Err(e) => {
                    debug!("Failed to get metadata for {}: {}", location, e);
                    return Err(e);
                }
            };

            let file_size = file_meta.size;
            let metadata_size_hint = self.config.parquet_metadata_size_hint as u64;

            // Check if this is likely a metadata request (reading from near the end of the file)
            let is_metadata_request = range.start >= file_size.saturating_sub(metadata_size_hint);

            if is_metadata_request {
                // For metadata requests, use the metadata cache
                let range_cache_key = Self::make_range_cache_key(location, &range);

                // Check if we have this specific range cached in the metadata cache
                if let Ok(Some(entry)) = self.metadata_cache.get(&range_cache_key).await {
                    let value = entry.value();
                    let ttl = self.config.ttl;  // Use unified TTL
                    if !value.is_expired(ttl) {
                        self.update_metadata_stats(|s| s.hits += 1).await;
                        debug!(
                            "Metadata cache HIT for: {} (range: {}..{}, size: {} bytes, age={}ms)",
                            location,
                            range.start,
                            range.end,
                            value.data.len(),
                            current_millis().saturating_sub(value.timestamp_millis)
                        );
                        return Ok(Bytes::from(value.data.clone()));
                    }
                }

                // Cache miss for metadata range - fetch just the range
                self.update_metadata_stats(|s| {
                    s.misses += 1;
                    s.inner_gets += 1;
                })
                .await;
                debug!(
                    "Metadata cache MISS for Parquet: {} (range: {}..{}, file_size: {})",
                    location, range.start, range.end, file_size
                );

                let start_time = std::time::Instant::now();
                let data = self.inner.get_range(location, range.clone()).await?;
                let duration = start_time.elapsed();
                
                debug!(
                    "S3 GET_RANGE request (metadata): {} (range: {}..{}, size: {} bytes, duration: {}ms)",
                    location,
                    range.start,
                    range.end,
                    data.len(),
                    duration.as_millis()
                );

                // Cache the metadata range in the metadata cache
                let range_meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: file_meta.last_modified,
                    size: data.len() as u64,
                    e_tag: file_meta.e_tag.clone(),
                    version: file_meta.version.clone(),
                };
                self.metadata_cache.insert(range_cache_key, CacheValue::new(data.to_vec(), range_meta));

                return Ok(data);
            } else {
                // For data requests, try to cache the full file
                debug!(
                    "Foyer cache MISS for Parquet data: {} (range: {}..{}, fetching full file)",
                    location, range.start, range.end
                );

                // Try to fetch and cache the full file
                if let Ok(result) = self.get(location).await {
                    // The file is now cached, extract the range
                    if range.end <= result.meta.size {
                        let data = match result.payload {
                            GetResultPayload::Stream(s) => {
                                use futures::TryStreamExt;
                                let chunks: Vec<Bytes> = s.try_collect().await?;
                                let full_data = chunks.concat();
                                Bytes::from(full_data[range.start as usize..range.end as usize].to_vec())
                            }
                            GetResultPayload::File(mut file, _) => {
                                use std::io::{Read, Seek, SeekFrom};
                                file.seek(SeekFrom::Start(range.start)).map_err(|e| object_store::Error::Generic {
                                    store: "cache",
                                    source: Box::new(e),
                                })?;
                                let mut buf = vec![0; (range.end - range.start) as usize];
                                file.read_exact(&mut buf).map_err(|e| object_store::Error::Generic {
                                    store: "cache",
                                    source: Box::new(e),
                                })?;
                                Bytes::from(buf)
                            }
                        };
                        return Ok(data);
                    }
                }
            }
        }

        // Fallback to regular range request for non-parquet files
        self.update_stats(|s| {
            s.misses += 1;
            s.inner_gets += 1;
        })
        .await;
        debug!(
            "get_range request for: {} (range: {}..{}, parquet={})",
            location, range.start, range.end, is_parquet
        );
        
        let start_time = std::time::Instant::now();
        let result = self.inner.get_range(location, range.clone()).await?;
        let duration = start_time.elapsed();
        
        debug!(
            "S3 GET_RANGE request: {} (range: {}..{}, size: {} bytes, duration: {}ms, parquet: {})",
            location,
            range.start,
            range.end,
            range.end - range.start,
            duration.as_millis(),
            is_parquet
        );
        
        Ok(result)
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let cache_key = Self::make_cache_key(location);

        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            let ttl = self.get_ttl_for_path(location);
            if !value.is_expired(ttl) {
                return Ok(value.meta.clone());
            }
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.update_stats(|s| s.inner_puts += 1).await;
        let cache_key = Self::make_cache_key(location);
        self.cache.remove(&cache_key);
        
        // Delete from inner store
        self.inner.delete(location).await?;

        // Invalidate metadata cache entries for this file
        if location.as_ref().ends_with(".parquet") {
            self.invalidate_metadata_cache(location).await;
        }

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await?;
        self.cache.remove(&Self::make_cache_key(to));

        // Invalidate metadata cache entries for the destination file
        if to.as_ref().ends_with(".parquet") {
            self.invalidate_metadata_cache(to).await;
        }

        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.cache.remove(&Self::make_cache_key(to));

        // Invalidate metadata cache entries for the destination file
        if to.as_ref().ends_with(".parquet") {
            self.invalidate_metadata_cache(to).await;
        }

        Ok(())
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
}

impl std::fmt::Display for FoyerObjectStoreCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FoyerHybridCachedObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for FoyerObjectStoreCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FoyerHybridCachedObjectStore {{ inner: {} }}", self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_basic_operations() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner, FoyerCacheConfig::test_config("basic_ops")).await?;
        cache.reset_stats().await;

        let path = Path::from("test/file.parquet");
        let data = Bytes::from("test data");

        cache.put(&path, PutPayload::from(data.clone())).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_puts, 1);
        assert_eq!(stats.main.inner_gets, 1); // We fetch after write to cache it

        // First get - cache hit (since we cache on write)
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // No additional fetch needed
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // Still just the one from write
        assert_eq!(stats.main.hits, 2); // Two cache hits total
        assert_eq!(stats.main.misses, 0);

        cache.delete(&path).await?;
        
        // Give cache time to process deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // After deletion, get should fail
        let get_result = cache.get(&path).await;
        assert!(get_result.is_err(), "Expected error after delete, got: {:?}", get_result);

        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_prevents_s3_access() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("s3_bypass", |c| {
            c.memory_size_bytes = 10 * 1024 * 1024;
            c.disk_size_bytes = 100 * 1024 * 1024;
            c.ttl = Duration::from_secs(300);
        });

        let cache = FoyerObjectStoreCache::new(inner, config).await?;
        cache.reset_stats().await;

        let files = vec![
            ("table/part-001.parquet", vec![b'a'; 1024]),
            ("table/part-002.parquet", vec![b'b'; 2048]),
            ("table/part-003.parquet", vec![b'c'; 4096]),
        ];

        // Write all files
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            cache.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;
        }

        // First read - cache hit (since we cache on write)
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let bytes: Vec<Bytes> = match result.payload {
                GetResultPayload::Stream(s) => s.try_collect().await?,
                _ => panic!("Expected stream"),
            };
            assert_eq!(bytes[0].len(), data.len());
        }

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 3); // From the writes
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 3);

        // Second read - cache hit
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let bytes: Vec<Bytes> = match result.payload {
                GetResultPayload::Stream(s) => s.try_collect().await?,
                _ => panic!("Expected stream"),
            };
            assert_eq!(bytes[0].len(), data.len());
        }

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 3); // No new inner gets
        assert_eq!(stats.main.hits, 6); // Total 6 hits (3 per read)

        info!("Cache successfully prevented {} S3 accesses", stats.main.hits);

        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_expiration() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("ttl_{}", std::process::id());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.ttl = Duration::from_millis(100);
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner, config).await?;

        let path = Path::from("test/ttl_file.parquet");
        let data = Bytes::from("test data");

        cache.put(&path, PutPayload::from(data.clone())).await?;
        let _ = cache.get(&path).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = cache.get(&path).await?;

        let stats = cache.get_stats().await;
        info!("TTL test - main cache hits: {}, misses: {}", stats.main.hits, stats.main.misses);

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_large_file_disk_cache() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("disk", |c| {
            c.memory_size_bytes = 1024; // Very small memory
        });

        let cache = FoyerObjectStoreCache::new(inner, config).await?;
        cache.reset_stats().await;

        let large_data = Bytes::from(vec![b'x'; 10 * 1024]); // 10KB
        let path = Path::from("test/large_file.parquet");

        cache.put(&path, PutPayload::from(large_data.clone())).await?;

        // First get - cache hit (since we cache on write)
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // From the write
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // Still just from the write
        assert_eq!(stats.main.hits, 2); // Two cache hits total

        info!("Large file test - main cache hits: {}, misses: {}", stats.main.hits, stats.main.misses);
        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_metadata_optimization() -> anyhow::Result<()> {
        // Use a unique test name to avoid cache conflicts
        let test_id = format!("parquet_metadata_{}", std::process::id());

        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.parquet_metadata_size_hint = 1024; // 1KB for testing
            c.ttl = Duration::from_secs(300);
        });

        // Ensure cache directory is cleaned up first
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;

        // Create a test parquet file (10KB)
        let file_size = 10 * 1024;
        let parquet_data = vec![b'x'; file_size];
        let path = Path::from("test/file.parquet");

        // Put the file directly in the inner store to avoid caching
        inner.put(&path, PutPayload::from(Bytes::from(parquet_data.clone()))).await?;

        // Reset stats to start fresh
        cache.reset_stats().await;

        // Test 1: Request metadata (last 1KB) - should cache only the range
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let metadata = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(metadata.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.inner_gets, 1); // One get_range call for metadata
        assert_eq!(stats.metadata.misses, 1);
        assert_eq!(stats.metadata.hits, 0);

        // Test 2: Request same metadata range again - should hit range cache
        let metadata2 = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(metadata2.len(), 1024);
        assert_eq!(metadata, metadata2);

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.inner_gets, 1); // No additional inner get
        assert_eq!(stats.metadata.hits, 1); // Cache hit on range
        assert_eq!(stats.metadata.misses, 1);

        // Test 3: Request data from beginning - should fetch and cache full file
        let data_range = 0..1024;
        let data = cache.get_range(&path, data_range.clone()).await?;
        assert_eq!(data.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // One get for full file
        assert_eq!(stats.main.misses, 1);
        assert_eq!(stats.metadata.hits, 1); // Still have metadata cache hit

        // Test 4: Request any range now - should hit full file cache
        let another_range = 2048..3072;
        let another_data = cache.get_range(&path, another_range).await?;
        assert_eq!(another_data.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // No additional inner get
        assert_eq!(stats.main.hits, 1); // Cache hit on full file

        info!("Parquet metadata optimization test passed");
        info!("Main cache - hits: {}, misses: {}", stats.main.hits, stats.main.misses);
        info!("Metadata cache - hits: {}, misses: {}", stats.metadata.hits, stats.metadata.misses);
        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_separation() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("metadata_separation_{}", std::process::id());

        // Use in-memory store for testing
        let inner = Arc::new(InMemory::new());

        // Configure cache with small limits to test separation
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.memory_size_bytes = 10 * 1024 * 1024; // 10MB
            c.disk_size_bytes = 50 * 1024 * 1024; // 50MB
            c.metadata_memory_size_bytes = 5 * 1024 * 1024; // 5MB
            c.metadata_disk_size_bytes = 20 * 1024 * 1024; // 20MB
            c.parquet_metadata_size_hint = 1024; // 1KB
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Create a parquet file
        let path = Path::from("test.parquet");
        let file_size = 1024 * 1024; // 1MB
        let data = vec![b'a'; file_size];
        inner.put(&path, PutPayload::from(Bytes::from(data))).await?;

        // Test 1: Read metadata range (should use metadata cache)
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let result = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(result.len(), 1024, "Should get correct range size");

        let stats = cache.get_stats().await;
        info!(
            "After first get_range - metadata.misses: {}, metadata.hits: {}, main.misses: {}, main.hits: {}",
            stats.metadata.misses, stats.metadata.hits, stats.main.misses, stats.main.hits
        );
        assert_eq!(stats.metadata.misses, 1, "Should have 1 metadata cache miss");
        assert_eq!(stats.metadata.hits, 0, "Should have 0 metadata cache hits");
        assert_eq!(stats.main.hits, 0, "Should have 0 main cache hits");

        // Test 2: Read same metadata range again (should hit metadata cache)
        let _ = cache.get_range(&path, metadata_range.clone()).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 1, "Should have 1 metadata cache hit");
        assert_eq!(stats.metadata.misses, 1, "Should still have 1 metadata cache miss");

        // Test 3: Read data range (should use main cache)
        let data_range = 0..1024;
        let _ = cache.get_range(&path, data_range).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.misses, 1, "Should have 1 main cache miss");

        // Test 4: Read full file (should use main cache)
        let _ = cache.get(&path).await?;

        let stats = cache.get_stats().await;
        assert!(stats.main.hits > 0 || stats.main.misses > 0, "Main cache should be used for full file");

        info!("Main cache stats: hits={}, misses={}", stats.main.hits, stats.main.misses);
        info!("Metadata cache stats: hits={}, misses={}", stats.metadata.hits, stats.metadata.misses);

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_invalidation() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("metadata_invalidation_{}", std::process::id());

        let inner = Arc::new(InMemory::new());

        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.parquet_metadata_size_hint = 1024;
            c.metadata_memory_size_bytes = 5 * 1024 * 1024;
            c.metadata_disk_size_bytes = 20 * 1024 * 1024;
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Create a parquet file directly in inner store (to avoid main cache)
        let path = Path::from("test.parquet");
        let file_size = 10 * 1024; // 10KB
        let data = vec![b'a'; file_size];
        inner.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;

        // Read metadata range - should use metadata cache
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let result = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(result.len(), 1024, "Should get correct range size");

        let stats = cache.get_stats().await;
        info!(
            "After first get_range - metadata.misses: {}, metadata.hits: {}, main.misses: {}, main.hits: {}",
            stats.metadata.misses, stats.metadata.hits, stats.main.misses, stats.main.hits
        );
        assert_eq!(stats.metadata.misses, 1, "Should have metadata cache miss");
        assert_eq!(stats.metadata.hits, 0, "Should have no metadata cache hits yet");

        // Read again - should hit metadata cache
        let _ = cache.get_range(&path, metadata_range.clone()).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 1, "Should hit metadata cache");

        // Update the file via cache - should invalidate metadata cache
        let new_data = vec![b'b'; file_size];
        cache.put(&path, PutPayload::from(Bytes::from(new_data))).await?;

        // Read metadata again - should be served from main cache now (file was cached on put)
        let _ = cache.get_range(&path, metadata_range).await?;
        let stats = cache.get_stats().await;
        // The range will be served from the main cache since put() caches the full file
        assert_eq!(stats.main.hits, 1, "Should hit main cache after put");

        info!("Metadata cache invalidation test passed");
        info!(
            "Final stats - Main: hits={}, misses={}, Metadata: hits={}, misses={}",
            stats.main.hits, stats.main.misses, stats.metadata.hits, stats.metadata.misses
        );

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }
}
