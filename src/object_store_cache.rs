use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use object_store::{
    path::Path, Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as ObjectStoreResult,
};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};

use foyer::{
    DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder, LargeEngineOptions,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Cache entry with metadata and TTL
/// We store raw bytes and metadata separately to enable serialization
#[derive(Debug, Clone)]
struct CacheValue {
    data: Bytes,
    meta: ObjectMeta,
    timestamp_millis: u64,
}

/// Configuration for the foyer-based object store cache
#[derive(Debug, Clone)]
pub struct FoyerCacheConfig {
    /// Memory cache size in bytes
    pub memory_size_bytes: usize,
    /// Disk cache size in bytes
    pub disk_size_bytes: usize,
    /// Time-to-live for cache entries
    pub ttl: Duration,
    /// Directory for disk cache
    pub cache_dir: PathBuf,
    /// Number of shards for better concurrency
    pub shards: usize,
    /// File size for disk cache files
    pub file_size_bytes: usize,
    /// Whether to enable cache statistics logging
    pub enable_stats: bool,
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
        }
    }
}

impl FoyerCacheConfig {
    /// Create cache config from environment variables
    pub fn from_env() -> Self {
        let memory_size_mb = std::env::var("TIMEFUSION_FOYER_MEMORY_MB")
            .unwrap_or_else(|_| "256".to_string())
            .parse::<usize>()
            .unwrap_or(256);

        let disk_size_gb = std::env::var("TIMEFUSION_FOYER_DISK_GB")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<usize>()
            .unwrap_or(10);

        let ttl_seconds = std::env::var("TIMEFUSION_FOYER_TTL_SECONDS")
            .unwrap_or_else(|_| "300".to_string())
            .parse::<u64>()
            .unwrap_or(300);

        let cache_dir = std::env::var("TIMEFUSION_FOYER_CACHE_DIR")
            .unwrap_or_else(|_| "/tmp/timefusion_cache".to_string());

        let shards = std::env::var("TIMEFUSION_FOYER_SHARDS")
            .unwrap_or_else(|_| "8".to_string())
            .parse::<usize>()
            .unwrap_or(8);

        let file_size_mb = std::env::var("TIMEFUSION_FOYER_FILE_SIZE_MB")
            .unwrap_or_else(|_| "16".to_string())
            .parse::<usize>()
            .unwrap_or(16);

        let enable_stats = std::env::var("TIMEFUSION_FOYER_STATS")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase() == "true";

        Self {
            memory_size_bytes: memory_size_mb * 1024 * 1024,
            disk_size_bytes: disk_size_gb * 1024 * 1024 * 1024,
            ttl: Duration::from_secs(ttl_seconds),
            cache_dir: PathBuf::from(cache_dir),
            shards,
            file_size_bytes: file_size_mb * 1024 * 1024,
            enable_stats,
        }
    }
}

/// Statistics for cache operations
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub ttl_expirations: u64,
    pub inner_gets: u64,  // Track actual fetches from inner store
    pub inner_puts: u64,  // Track actual writes to inner store
}

/// Wrapper for cache value that implements foyer's required traits
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableCacheValue {
    data: Vec<u8>,  // Vec<u8> for serialization
    meta_location: String,
    meta_last_modified: i64,
    meta_size: u64,
    meta_e_tag: Option<String>,
    meta_version: Option<String>,
    timestamp_millis: u64,
}

impl From<CacheValue> for SerializableCacheValue {
    fn from(value: CacheValue) -> Self {
        Self {
            data: value.data.to_vec(),
            meta_location: value.meta.location.to_string(),
            meta_last_modified: value.meta.last_modified.timestamp_millis(),
            meta_size: value.meta.size,
            meta_e_tag: value.meta.e_tag.clone(),
            meta_version: value.meta.version.clone(),
            timestamp_millis: value.timestamp_millis,
        }
    }
}

impl SerializableCacheValue {
    fn to_cache_value(&self) -> CacheValue {
        CacheValue {
            data: Bytes::from(self.data.clone()),
            meta: ObjectMeta {
                location: Path::from(self.meta_location.clone()),
                last_modified: DateTime::<Utc>::from_timestamp_millis(self.meta_last_modified)
                    .unwrap_or(Utc::now()),
                size: self.meta_size,
                e_tag: self.meta_e_tag.clone(),
                version: self.meta_version.clone(),
            },
            timestamp_millis: self.timestamp_millis,
        }
    }
}

/// Foyer-based hybrid cache implementation for object store
/// Uses both memory and disk tiers for caching Parquet files
pub struct FoyerObjectStoreCache {
    inner: Arc<dyn ObjectStore>,
    cache: HybridCache<String, SerializableCacheValue>,
    stats: Arc<RwLock<CacheStats>>,
    config: FoyerCacheConfig,
}

impl FoyerObjectStoreCache {
    /// Create a new foyer-based hybrid cached object store
    pub async fn new(inner: Arc<dyn ObjectStore>, config: FoyerCacheConfig) -> anyhow::Result<Self> {
        info!(
            "Initializing foyer hybrid cache (memory: {}MB, disk: {}GB, ttl: {}s)",
            config.memory_size_bytes / 1024 / 1024,
            config.disk_size_bytes / 1024 / 1024 / 1024,
            config.ttl.as_secs()
        );

        // Create cache directory if it doesn't exist
        std::fs::create_dir_all(&config.cache_dir)?;

        // Build the hybrid cache with both memory and disk tiers
        let cache: HybridCache<String, SerializableCacheValue> = HybridCacheBuilder::new()
            .memory(config.memory_size_bytes)
            .with_shards(config.shards)
            .with_weighter(|_key: &String, value: &SerializableCacheValue| value.data.len())
            .storage(Engine::Large(LargeEngineOptions::default())) // Optimized for large Parquet files
            .with_device_options(
                DirectFsDeviceOptions::new(&config.cache_dir)
                    .with_capacity(config.disk_size_bytes)
                    .with_file_size(config.file_size_bytes)
            )
            .build()
            .await?;

        Ok(Self {
            inner,
            cache,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
        })
    }

    /// Check if cache entry is expired
    fn is_expired(&self, entry: &SerializableCacheValue) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let age_millis = now.saturating_sub(entry.timestamp_millis);
        age_millis > self.config.ttl.as_millis() as u64
    }

    /// Create cache key from path
    fn make_cache_key(location: &Path) -> String {
        location.to_string()
    }

    /// Log cache statistics periodically
    pub async fn log_stats(&self) {
        if !self.config.enable_stats {
            return;
        }

        let stats = self.stats.read().await;
        let total_requests = stats.hits + stats.misses;
        if total_requests > 0 {
            let hit_rate = (stats.hits as f64 / total_requests as f64) * 100.0;
            info!(
                "Foyer hybrid cache stats - Hit rate: {:.1}%, Hits: {}, Misses: {}, TTL expirations: {}, Inner gets: {}, Inner puts: {}",
                hit_rate, stats.hits, stats.misses, stats.ttl_expirations, stats.inner_gets, stats.inner_puts
            );
        }
    }
    
    /// Get current cache statistics (test helper)
    #[cfg(test)]
    pub async fn get_stats(&self) -> CacheStats {
        self.stats.read().await.clone()
    }
    
    /// Reset cache statistics (test helper)
    #[cfg(test)]
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.write().await;
        *stats = CacheStats::default();
    }

    /// Gracefully shutdown the cache
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down foyer hybrid cache");
        self.cache.close().await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FoyerObjectStoreCache {
    async fn put(&self, location: &Path, payload: PutPayload) -> ObjectStoreResult<PutResult> {
        // Write through to underlying store
        let mut stats = self.stats.write().await;
        stats.inner_puts += 1;
        drop(stats);
        
        let result = self.inner.put(location, payload).await?;

        // Invalidate cache entry if it exists
        let cache_key = Self::make_cache_key(location);
        self.cache.remove(&cache_key);
        debug!("Invalidated cache entry after PUT: {}", location);

        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // Write through to underlying store
        let result = self.inner.put_opts(location, payload, opts).await?;

        // Invalidate cache entry
        let cache_key = Self::make_cache_key(location);
        self.cache.remove(&cache_key);

        Ok(result)
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let cache_key = Self::make_cache_key(location);
        
        // First, try to get from cache
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            
            // Check if entry is expired
            if self.is_expired(value) {
                // Entry expired - remove it
                let mut stats = self.stats.write().await;
                stats.ttl_expirations += 1;
                drop(stats);
                
                self.cache.remove(&cache_key);
                debug!("Removed expired cache entry: {}", location);
            } else {
                // Cache hit!
                let mut stats = self.stats.write().await;
                stats.hits += 1;
                drop(stats);
                
                debug!("Cache hit for: {}", location);
                
                let cache_value = value.to_cache_value();
                let data = cache_value.data.clone();
                let meta = cache_value.meta.clone();
                let data_len = data.len() as u64;
                
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                        async move { Ok(data) },
                    ))),
                    meta,
                    attributes: Attributes::new(),
                    range: 0..data_len,
                });
            }
        }
        
        // Cache miss - fetch from inner store
        let mut stats = self.stats.write().await;
        stats.misses += 1;
        stats.inner_gets += 1;
        drop(stats);
        
        debug!("Cache miss for: {}", location);
        
        // Fetch from underlying store
        let result = self.inner.get(location).await?;
        
        // Collect the payload for caching
        use futures::TryStreamExt;
        let stream = match result.payload {
            GetResultPayload::Stream(s) => s,
            GetResultPayload::File(mut file, _) => {
                // Read file and create stream
                use std::io::Read;
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).map_err(|e| object_store::Error::Generic {
                    store: "cache",
                    source: Box::new(e),
                })?;
                Box::pin(futures::stream::once(async move { Ok(Bytes::from(bytes)) }))
            }
        };
        
        let bytes_vec: Vec<Bytes> = stream.try_collect().await?;
        let total_len: usize = bytes_vec.iter().map(|b| b.len()).sum();
        let mut data = Vec::with_capacity(total_len);
        for chunk in bytes_vec {
            data.extend_from_slice(&chunk);
        }
        
        let cache_value = CacheValue {
            data: Bytes::from(data.clone()),
            meta: result.meta.clone(),
            timestamp_millis: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };
        
        // Insert into cache for next time
        let serializable_value = SerializableCacheValue::from(cache_value.clone());
        self.cache.insert(cache_key, serializable_value);
        
        let data_len = data.len() as u64;
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(Bytes::from(data)) },
            ))),
            meta: result.meta,
            attributes: Attributes::new(),
            range: 0..data_len,
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // For ranged requests or conditional gets, bypass cache
        if options.range.is_some() || options.if_match.is_some() || 
           options.if_none_match.is_some() || options.if_modified_since.is_some() ||
           options.if_unmodified_since.is_some() {
            return self.inner.get_opts(location, options).await;
        }

        // Use regular get for full object requests
        self.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        let cache_key = Self::make_cache_key(location);

        // Try to get from cache first
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            if !self.is_expired(value) && range.end <= value.data.len() as u64 {
                let mut stats = self.stats.write().await;
                stats.hits += 1;
                drop(stats);

                debug!("Cache hit for range request: {} [{:?}]", location, range);

                let cache_value = value.to_cache_value();
                return Ok(cache_value.data.slice(range.start as usize..range.end as usize));
            }
        }

        // Cache miss or partial - fetch from underlying store
        let mut stats = self.stats.write().await;
        stats.misses += 1;
        stats.inner_gets += 1;
        drop(stats);

        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let cache_key = Self::make_cache_key(location);

        // Check cache for metadata
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            if !self.is_expired(value) {
                return Ok(value.to_cache_value().meta);
            }
        }

        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        // Delete from underlying store
        let mut stats = self.stats.write().await;
        stats.inner_puts += 1;  // Count delete as a write operation
        drop(stats);
        
        self.inner.delete(location).await?;

        // Remove from cache
        let cache_key = Self::make_cache_key(location);
        self.cache.remove(&cache_key);

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        // Delegate to inner store - no caching for list operations
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        // Delegate to inner store - no caching for list operations
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.copy(from, to).await?;
        
        // Invalidate destination cache
        let cache_key = Self::make_cache_key(to);
        self.cache.remove(&cache_key);
        
        Ok(result)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.copy_if_not_exists(from, to).await?;
        
        // Invalidate destination cache
        let cache_key = Self::make_cache_key(to);
        self.cache.remove(&cache_key);
        
        Ok(result)
    }

    async fn put_multipart(&self, location: &Path) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
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
        let config = FoyerCacheConfig {
            memory_size_bytes: 1024 * 1024, // 1MB
            disk_size_bytes: 10 * 1024 * 1024, // 10MB
            ttl: Duration::from_secs(5),
            cache_dir: PathBuf::from("/tmp/test_foyer_hybrid_cache"),
            shards: 2,
            file_size_bytes: 1024 * 1024, // 1MB
            enable_stats: true,
        };
        
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;
        
        // Test put and get
        let path = Path::from("test/file.parquet");
        let data = Bytes::from("test data");
        
        cache.put(&path, PutPayload::from(data.clone())).await?;
        
        // Verify put incremented inner_puts
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_puts, 1, "First put should write to inner store");
        
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let stream = match result.payload {
            GetResultPayload::Stream(s) => s,
            _ => panic!("Expected stream"),
        };
        let bytes: Vec<Bytes> = stream.try_collect().await?;
        assert_eq!(bytes[0], data);
        
        // First get should fetch from inner store
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1, "First get should fetch from inner store");
        assert_eq!(stats.misses, 1, "First get should be a cache miss");
        assert_eq!(stats.hits, 0, "First get should not be a hit");
        
        // Second get should hit cache (from memory or disk)
        let result2 = cache.get(&path).await?;
        let stream2 = match result2.payload {
            GetResultPayload::Stream(s) => s,
            _ => panic!("Expected stream"),
        };
        let bytes2: Vec<Bytes> = stream2.try_collect().await?;
        assert_eq!(bytes2[0], data);
        
        // Verify second get didn't fetch from inner store
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1, "Second get should use cache, not inner store");
        assert_eq!(stats.hits, 1, "Second get should be a cache hit");
        assert_eq!(stats.misses, 1, "Still only one miss");
        
        // Test delete
        cache.delete(&path).await?;
        
        // Should fail after delete
        assert!(cache.get(&path).await.is_err());
        
        // Cleanup
        cache.shutdown().await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_prevents_s3_access() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig {
            memory_size_bytes: 10 * 1024 * 1024, // 10MB
            disk_size_bytes: 100 * 1024 * 1024, // 100MB
            ttl: Duration::from_secs(300),
            cache_dir: PathBuf::from("/tmp/test_foyer_s3_bypass"),
            shards: 4,
            file_size_bytes: 1024 * 1024,
            enable_stats: true,
        };
        
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;
        
        // Simulate multiple Parquet files
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
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_puts, 3, "Should have 3 writes to inner store");
        
        // First read of all files - should fetch from inner store
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let stream = match result.payload {
                GetResultPayload::Stream(s) => s,
                _ => panic!("Expected stream"),
            };
            let bytes: Vec<Bytes> = stream.try_collect().await?;
            assert_eq!(bytes[0].len(), data.len());
        }
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 3, "First reads should fetch from inner store");
        assert_eq!(stats.misses, 3, "First reads should all be cache misses");
        
        // Second read of all files - should use cache
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let stream = match result.payload {
                GetResultPayload::Stream(s) => s,
                _ => panic!("Expected stream"),
            };
            let bytes: Vec<Bytes> = stream.try_collect().await?;
            assert_eq!(bytes[0].len(), data.len());
        }
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 3, "Second reads should NOT fetch from inner store");
        assert_eq!(stats.hits, 3, "Second reads should all be cache hits");
        
        // Third read - still cached
        for (path_str, _) in &files {
            let path = Path::from(*path_str);
            let _ = cache.get(&path).await?;
        }
        
        let final_stats = cache.get_stats().await;
        assert_eq!(final_stats.inner_gets, 3, "Third reads should still use cache");
        assert_eq!(final_stats.hits, 6, "Should have 6 total cache hits");
        
        info!("Cache successfully prevented {} S3 accesses", final_stats.hits);
        cache.log_stats().await;
        
        // Cleanup
        cache.shutdown().await?;
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_ttl_expiration() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig {
            memory_size_bytes: 1024 * 1024,
            disk_size_bytes: 10 * 1024 * 1024,
            ttl: Duration::from_millis(100), // Very short TTL
            cache_dir: PathBuf::from("/tmp/test_foyer_ttl"),
            shards: 2,
            file_size_bytes: 1024 * 1024,
            enable_stats: true,
        };
        
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        
        let path = Path::from("test/ttl_file.parquet");
        let data = Bytes::from("test data");
        
        cache.put(&path, PutPayload::from(data.clone())).await?;
        
        // First get should work
        let _ = cache.get(&path).await?;
        
        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Should fetch from underlying store again (expired entry)
        let _ = cache.get(&path).await?;
        
        // Check stats to see if TTL expiration was detected
        cache.log_stats().await;
        
        // Cleanup
        cache.shutdown().await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_large_file_disk_cache() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig {
            memory_size_bytes: 1024, // Very small memory (1KB)
            disk_size_bytes: 10 * 1024 * 1024, // 10MB disk
            ttl: Duration::from_secs(60),
            cache_dir: PathBuf::from("/tmp/test_foyer_disk"),
            shards: 2,
            file_size_bytes: 1024 * 1024,
            enable_stats: true,
        };
        
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;
        
        // Create a large file that won't fit in memory cache
        let large_data = Bytes::from(vec![b'x'; 10 * 1024]); // 10KB
        let path = Path::from("test/large_file.parquet");
        
        cache.put(&path, PutPayload::from(large_data.clone())).await?;
        
        // First get - will be stored in disk cache since it's too large for memory
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let stream = match result.payload {
            GetResultPayload::Stream(s) => s,
            _ => panic!("Expected stream"),
        };
        let bytes: Vec<Bytes> = stream.try_collect().await?;
        assert_eq!(bytes[0].len(), large_data.len());
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1, "First get should fetch from inner store");
        
        // Second get should hit cache (from disk since too large for memory)
        let result2 = cache.get(&path).await?;
        let stream2 = match result2.payload {
            GetResultPayload::Stream(s) => s,
            _ => panic!("Expected stream"),
        };
        let bytes2: Vec<Bytes> = stream2.try_collect().await?;
        assert_eq!(bytes2[0].len(), large_data.len());
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.inner_gets, 1, "Second get should use cache, not inner store");
        assert_eq!(stats.hits, 1, "Second get should be a cache hit");
        
        cache.log_stats().await;
        
        // Cleanup
        cache.shutdown().await?;
        
        Ok(())
    }
}