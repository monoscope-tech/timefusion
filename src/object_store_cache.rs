use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
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

use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, LargeEngineOptions};
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
    /// Separate TTL for Delta metadata files (_delta_log/*)
    pub delta_metadata_ttl: Option<Duration>,
    /// Whether to cache Delta checkpoint files
    pub cache_delta_checkpoints: bool,
    /// Specific TTL for checkpoint files (when cache_delta_checkpoints is true)
    pub checkpoint_ttl: Option<Duration>,
}

impl Default for FoyerCacheConfig {
    fn default() -> Self {
        Self {
            memory_size_bytes: 268_435_456,  // 256MB
            disk_size_bytes: 10_737_418_240, // 10GB
            ttl: Duration::from_secs(300),   // 5 minutes
            cache_dir: PathBuf::from("/tmp/timefusion_cache"),
            shards: 8,
            file_size_bytes: 16_777_216, // 16MB - good for Parquet files
            enable_stats: true,
            delta_metadata_ttl: Some(Duration::from_secs(5)), // Short TTL for metadata
            cache_delta_checkpoints: false,                   // Disable caching for checkpoint files by default
            checkpoint_ttl: Some(Duration::from_secs(1)),     // Very short TTL for checkpoints if cached
        }
    }
}

impl FoyerCacheConfig {
    /// Create cache config from environment variables
    pub fn from_env() -> Self {
        fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }

        let delta_metadata_ttl_secs = parse_env("TIMEFUSION_FOYER_DELTA_METADATA_TTL_SECONDS", 5);
        let checkpoint_ttl_secs = parse_env("TIMEFUSION_CHECKPOINT_CACHE_TTL_SECONDS", 1);

        Self {
            memory_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_MEMORY_MB", 256) * 1024 * 1024,
            disk_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_DISK_GB", 10) * 1024 * 1024 * 1024,
            ttl: Duration::from_secs(parse_env("TIMEFUSION_FOYER_TTL_SECONDS", 300)),
            cache_dir: PathBuf::from(parse_env("TIMEFUSION_FOYER_CACHE_DIR", "/tmp/timefusion_cache".to_string())),
            shards: parse_env("TIMEFUSION_FOYER_SHARDS", 8),
            file_size_bytes: parse_env::<usize>("TIMEFUSION_FOYER_FILE_SIZE_MB", 16) * 1024 * 1024,
            enable_stats: parse_env("TIMEFUSION_FOYER_STATS", "true".to_string()).to_lowercase() == "true",
            delta_metadata_ttl: if delta_metadata_ttl_secs > 0 { Some(Duration::from_secs(delta_metadata_ttl_secs)) } else { None },
            cache_delta_checkpoints: parse_env("TIMEFUSION_FOYER_CACHE_DELTA_CHECKPOINTS", "false".to_string()).to_lowercase() == "true",
            checkpoint_ttl: if checkpoint_ttl_secs > 0 { Some(Duration::from_secs(checkpoint_ttl_secs)) } else { None },
        }
    }

    /// Create a test configuration with sensible defaults for testing
    /// The name parameter is used to create unique cache directories
    pub fn test_config(name: &str) -> Self {
        Self {
            memory_size_bytes: 10 * 1024 * 1024,  // 10MB
            disk_size_bytes: 50 * 1024 * 1024,    // 50MB
            ttl: Duration::from_secs(300),
            cache_dir: PathBuf::from(format!("/tmp/test_foyer_{}", name)),
            shards: 2,
            file_size_bytes: 1024 * 1024,  // 1MB
            enable_stats: true,
            delta_metadata_ttl: Some(Duration::from_secs(5)),
            cache_delta_checkpoints: false,  // Default to false for tests
            checkpoint_ttl: Some(Duration::from_secs(1)),
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
    stats: StatsRef,
    config: FoyerCacheConfig,
}

impl SharedFoyerCache {
    /// Create a new shared Foyer cache
    pub async fn new(config: FoyerCacheConfig) -> anyhow::Result<Self> {
        info!(
            "Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, ttl: {}s)",
            config.memory_size_bytes / 1024 / 1024,
            config.disk_size_bytes / 1024 / 1024 / 1024,
            config.ttl.as_secs()
        );

        std::fs::create_dir_all(&config.cache_dir)?;

        let cache = HybridCacheBuilder::new()
            .memory(config.memory_size_bytes)
            .with_shards(config.shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage(Engine::Large(LargeEngineOptions::default()))
            .with_device_options(
                DirectFsDeviceOptions::new(&config.cache_dir)
                    .with_capacity(config.disk_size_bytes)
                    .with_file_size(config.file_size_bytes),
            )
            .build()
            .await?;

        Ok(Self {
            cache: Arc::new(cache),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
        })
    }

    pub async fn get_stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    pub async fn log_stats(&self) {
        self.stats.read().await.log();
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

        let last_checkpoint_key = format!("{}_delta_log/_last_checkpoint", table_path);
        info!("Invalidating _last_checkpoint cache for table: {}", table_path);
        self.cache.remove(&last_checkpoint_key);
    }
}

/// Foyer-based hybrid cache implementation for object store
pub struct FoyerObjectStoreCache {
    inner: Arc<dyn ObjectStore>,
    cache: FoyerCache,
    stats: StatsRef,
    config: FoyerCacheConfig,
}

impl FoyerObjectStoreCache {
    pub fn new_with_shared_cache(inner: Arc<dyn ObjectStore>, shared_cache: &SharedFoyerCache) -> Self {
        Self {
            inner,
            cache: shared_cache.cache.clone(),
            stats: shared_cache.stats.clone(),
            config: shared_cache.config.clone(),
        }
    }

    /// Check if a path is a Delta Lake metadata file
    fn is_delta_metadata(location: &Path) -> bool {
        location.as_ref().contains("_delta_log/")
    }

    /// Check if a path is a Delta Lake checkpoint file
    fn is_delta_checkpoint(location: &Path) -> bool {
        let path_str = location.as_ref();
        path_str.contains("_delta_log/") && (path_str.contains("_last_checkpoint") || path_str.contains(".checkpoint."))
    }

    /// Check if a path is the mutable _last_checkpoint file
    fn is_last_checkpoint(location: &Path) -> bool {
        location.as_ref().contains("_delta_log/_last_checkpoint")
    }

    /// Get the appropriate TTL for a file based on its type
    fn get_ttl_for_path(&self, location: &Path) -> Duration {
        if Self::is_last_checkpoint(location) {
            // Use very short TTL for _last_checkpoint file (mutable pointer)
            Duration::from_secs(5)
        } else if Self::is_delta_checkpoint(location) {
            // Use configured TTL for immutable checkpoint files
            self.config.checkpoint_ttl.unwrap_or(Duration::from_secs(1))
        } else if Self::is_delta_metadata(location) {
            // Use shorter TTL for Delta metadata files
            self.config.delta_metadata_ttl.unwrap_or(self.config.ttl)
        } else {
            self.config.ttl
        }
    }

    /// Check if a file should be cached
    fn should_cache(&self, location: &Path) -> bool {
        // Don't cache checkpoint files if disabled
        if !self.config.cache_delta_checkpoints && Self::is_delta_checkpoint(location) {
            return false;
        }
        true
    }

    /// Invalidate related cache entries when writing to Delta log
    async fn invalidate_related_delta_entries(&self, location: &Path) {
        let path_str = location.as_ref();

        // Always invalidate checkpoint cache if aggressive invalidation is enabled
        let aggressive_invalidation =
            std::env::var("TIMEFUSION_AGGRESSIVE_CHECKPOINT_INVALIDATION").unwrap_or_else(|_| "true".to_string()).to_lowercase() == "true";

        // If writing any file to _delta_log, invalidate checkpoint files
        if path_str.contains("_delta_log/") {
            // Extract the table path (everything before _delta_log/)
            if let Some(delta_log_idx) = path_str.find("_delta_log/") {
                let table_path = &path_str[..delta_log_idx];

                // Always invalidate _last_checkpoint for any delta log write in aggressive mode
                if aggressive_invalidation || path_str.ends_with(".json") {
                    let last_checkpoint_path = format!("{}_delta_log/_last_checkpoint", table_path);
                    info!(
                        "Invalidating _last_checkpoint cache for table: {} (aggressive={})",
                        table_path, aggressive_invalidation
                    );
                    self.cache.remove(&last_checkpoint_path);
                }

                // Also invalidate any checkpoint.parquet files to ensure consistency
                // Note: Foyer doesn't support pattern-based removal, so we can't easily remove all checkpoint files
                // This is a limitation we'll document
            }
        }
    }

    /// Explicitly invalidate checkpoint cache for a given table
    pub async fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        // Extract table path from URI (remove s3:// or other prefixes)
        let table_path = if let Some(idx) = table_uri.find("://") { &table_uri[idx + 3..] } else { table_uri };

        // Remove any trailing slashes
        let table_path = table_path.trim_end_matches('/');

        let last_checkpoint_path = format!("{}_delta_log/_last_checkpoint", table_path);
        info!("Explicitly invalidating _last_checkpoint cache for table: {}", table_path);
        self.cache.remove(&last_checkpoint_path);

        // TODO: In the future, we could track and invalidate specific checkpoint.parquet files
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

    fn make_cache_key(location: &Path) -> String {
        location.to_string()
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
        Ok(())
    }

    pub async fn get_stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }

    #[cfg(test)]
    pub async fn reset_stats(&self) {
        *self.stats.write().await = CacheStats::default();
    }
}

#[async_trait]
impl ObjectStore for FoyerObjectStoreCache {
    async fn put(&self, location: &Path, payload: PutPayload) -> ObjectStoreResult<PutResult> {
        self.update_stats(|s| s.inner_puts += 1).await;
        let result = self.inner.put(location, payload).await?;

        // Remove the written file from cache
        self.cache.remove(&Self::make_cache_key(location));

        // Invalidate related Delta entries if writing to _delta_log
        self.invalidate_related_delta_entries(location).await;

        Ok(result)
    }

    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.update_stats(|s| s.inner_puts += 1).await;
        let result = self.inner.put_opts(location, payload, opts).await?;

        // Remove the written file from cache
        self.cache.remove(&Self::make_cache_key(location));

        // Invalidate related Delta entries if writing to _delta_log
        self.invalidate_related_delta_entries(location).await;

        Ok(result)
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        // Check if we should cache this file
        if !self.should_cache(location) {
            self.update_stats(|s| {
                s.misses += 1;
                s.inner_gets += 1;
            })
            .await;
            info!("Bypassing cache for Delta checkpoint file: {}", location);
            return self.inner.get(location).await;
        }

        let cache_key = Self::make_cache_key(location);

        // Try cache first
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();

            // Use appropriate TTL based on file type
            let ttl = self.get_ttl_for_path(location);
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
                let is_delta = Self::is_delta_metadata(location);
                let is_checkpoint = Self::is_delta_checkpoint(location);
                let is_parquet = location.as_ref().ends_with(".parquet");
                debug!(
                    "Foyer cache HIT for: {} (avoiding S3 access, delta={}, checkpoint={}, parquet={}, TTL={}s, age={}ms)",
                    location,
                    is_delta,
                    is_checkpoint,
                    is_parquet,
                    ttl.as_secs(),
                    current_millis().saturating_sub(value.timestamp_millis)
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
        let is_delta = Self::is_delta_metadata(location);
        let is_checkpoint = Self::is_delta_checkpoint(location);
        let is_parquet = location.as_ref().ends_with(".parquet");
        let ttl = self.get_ttl_for_path(location);
        info!(
            "Foyer cache MISS for: {} (fetching from S3, delta={}, checkpoint={}, parquet={}, will_cache={}, TTL={}s)",
            location,
            is_delta,
            is_checkpoint,
            is_parquet,
            self.should_cache(location),
            ttl.as_secs()
        );

        let result = self.inner.get(location).await?;

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

        // Only cache if we should cache this file type
        if self.should_cache(location) {
            self.cache.insert(cache_key, CacheValue::new(data.clone(), result.meta.clone()));
        }
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
        
        // Check if we should cache this file
        if !self.should_cache(location) {
            self.update_stats(|s| {
                s.misses += 1;
                s.inner_gets += 1;
            })
            .await;
            return self.inner.get_range(location, range).await;
        }

        let cache_key = Self::make_cache_key(location);

        // Check if we have the full file cached
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            let ttl = self.get_ttl_for_path(location);
            if !value.is_expired(ttl) && range.end <= value.data.len() as u64 {
                self.update_stats(|s| s.hits += 1).await;
                debug!(
                    "Foyer cache HIT for range: {} (range: {}..{}, parquet={}, age={}ms)",
                    location, range.start, range.end, is_parquet,
                    current_millis().saturating_sub(value.timestamp_millis)
                );
                return Ok(Bytes::from(value.data[range.start as usize..range.end as usize].to_vec()));
            }
        }

        // For Parquet files, cache the entire file on first access
        if is_parquet {
            info!(
                "Foyer cache MISS for Parquet: {} (range: {}..{}, fetching full file)",
                location, range.start, range.end
            );
            
            // Try to fetch and cache the full file
            if let Ok(result) = self.get(location).await {
                // The file is now cached, extract the range
                if range.end <= result.meta.size as u64 {
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

        // Fallback to regular range request
        self.update_stats(|s| {
            s.misses += 1;
            s.inner_gets += 1;
        })
        .await;
        debug!(
            "get_range request for: {} (range: {}..{}, parquet={})",
            location, range.start, range.end, is_parquet
        );
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        // Check if we should cache this file
        if !self.should_cache(location) {
            return self.inner.head(location).await;
        }

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
        self.inner.delete(location).await?;
        self.cache.remove(&Self::make_cache_key(location));
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
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.cache.remove(&Self::make_cache_key(to));
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
        assert_eq!(stats.inner_puts, 1);

        // First get - cache miss
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        cache.delete(&path).await?;
        assert!(cache.get(&path).await.is_err());

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

        // First read - cache miss
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
        assert_eq!(stats.inner_gets, 3);
        assert_eq!(stats.misses, 3);

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
        assert_eq!(stats.inner_gets, 3); // No new inner gets
        assert_eq!(stats.hits, 3);

        info!("Cache successfully prevented {} S3 accesses", stats.hits);
        stats.log();

        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_expiration() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("ttl", |c| {
            c.ttl = Duration::from_millis(100);
        });

        let cache = FoyerObjectStoreCache::new(inner, config).await?;

        let path = Path::from("test/ttl_file.parquet");
        let data = Bytes::from("test data");

        cache.put(&path, PutPayload::from(data.clone())).await?;
        let _ = cache.get(&path).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = cache.get(&path).await?;

        let stats = cache.get_stats().await;
        stats.log();

        cache.shutdown().await?;
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

        // First get - cache miss
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1);
        assert_eq!(stats.hits, 1);

        stats.log();
        cache.shutdown().await?;
        Ok(())
    }
}

