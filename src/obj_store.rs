use std::fmt::{Debug, Display};
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use foyer::{DirectFsDeviceOptions, Engine, HybridCache, HybridCacheBuilder};
use futures::stream::{self, StreamExt, TryStreamExt, BoxStream, Once};
use object_store::{
    path::Path, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn, error};
use tokio::sync::RwLock;
use std::collections::HashMap;
use async_stream::try_stream;
use async_stream::stream;

/// Configuration for the Delta cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaCacheConfig {
    /// Memory cache capacity in bytes
    pub memory_capacity: usize,
    /// Disk cache capacity in bytes  
    pub disk_capacity: usize,
    /// Disk cache directory path
    pub disk_cache_dir: String,
    /// TTL for cached objects in seconds
    pub ttl_seconds: u64,
    /// Whether to cache transaction logs
    pub cache_transaction_logs: bool,
    /// Whether to cache parquet metadata
    pub cache_parquet_metadata: bool,
    /// Whether to cache checkpoint files
    pub cache_checkpoints: bool,
    /// Maximum object size to cache (in bytes)
    pub max_object_size: usize,
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Cache warming on startup
    pub enable_cache_warming: bool,
    /// Compression level for cached data (0-9, 0=no compression)
    pub compression_level: u8,
}

impl Default for DeltaCacheConfig {
    fn default() -> Self {
        Self {
            memory_capacity: 256 * 1024 * 1024, // 256MB
            disk_capacity: 1024 * 1024 * 1024,   // 1GB
            disk_cache_dir: "/tmp/delta_cache".to_string(),
            ttl_seconds: 3600, // 1 hour
            cache_transaction_logs: true,
            cache_parquet_metadata: true,
            cache_checkpoints: true,
            max_object_size: 10 * 1024 * 1024, // 10MB max
            enable_metrics: true,
            enable_cache_warming: false,
            compression_level: 3, // Light compression by default
        }
    }
}

/// Cache metrics for monitoring
#[derive(Debug, Default, Clone)]
pub struct CacheMetrics {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub errors: u64,
    pub total_requests: u64,
    pub cache_size_bytes: u64,
}

impl CacheMetrics {
    pub fn hit_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.hits as f64 / self.total_requests as f64
        }
    }
}

/// cached object with metadata and compression
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedObject {
    data: Vec<u8>,
    cached_at: u64,
    original_size: usize,
    is_compressed: bool,
    etag: Option<String>,
    last_modified: Option<i64>,
}

impl CachedObject {
    fn new(data: Vec<u8>, meta: &ObjectMeta, compression_level: u8) -> Self {
        let original_size = data.len();
        let (final_data, is_compressed) = if compression_level > 0 && data.len() > 1024 {
            match Self::compress(&data, compression_level) {
                Ok(compressed) if compressed.len() < data.len() => (compressed, true),
                _ => (data, false),
            }
        } else {
            (data, false)
        };

        Self {
            data: final_data,
            cached_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            original_size,
            is_compressed,
            etag: meta.e_tag.clone(),
            last_modified: Some(meta.last_modified.timestamp()),
        }
    }

    fn get_data(&self) -> Result<Vec<u8>, std::io::Error> {
        if self.is_compressed {
            Self::decompress(&self.data)
        } else {
            Ok(self.data.clone())
        }
    }

    fn compress(data: &[u8], level: u8) -> Result<Vec<u8>, std::io::Error> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level as u32));
        encoder.write_all(data)?;
        encoder.finish()
    }

    fn decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    fn is_valid(&self, ttl_seconds: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        (now - self.cached_at) < ttl_seconds
    }

    fn matches_meta(&self, meta: &ObjectMeta) -> bool {
        // Check ETag if available
        if let (Some(cached_etag), Some(meta_etag)) = (&self.etag, &meta.e_tag) {
            return cached_etag == meta_etag;
        }
        
        // Fallback to last modified time
        if let (Some(cached_modified),meta_modified) = (self.last_modified, meta.last_modified) {
            return cached_modified == meta_modified.timestamp();
        }
        
        // If no metadata available, assume valid (will rely on TTL)
        true
    }
}

/// Delta-optimized object store cache wrapper
#[derive(Debug)]
pub struct DeltaCachedObjectStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<HybridCache<String, CachedObject>>,
    config: DeltaCacheConfig,
    metrics: Arc<RwLock<CacheMetrics>>,
    // Track frequently accessed paths for cache warming
    access_patterns: Arc<RwLock<HashMap<String, u64>>>,
}

impl DeltaCachedObjectStore {
    /// Create a new cached object store
    pub async fn new(
        inner: Arc<dyn ObjectStore>,
        config: DeltaCacheConfig,
    ) -> ObjectStoreResult<Self> {
        // Build the hybrid cache
        let cache = HybridCacheBuilder::new()
            .memory(config.memory_capacity)
            .storage(Engine::Large)
            .with_device_options(DirectFsDeviceOptions::new(&config.disk_cache_dir)
                .with_capacity(config.disk_capacity))
            .build()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "DeltaCache",
                source: Box::new(std::io::Error::other(e)),
            })?;

        info!(
            "Initialized Delta cache: memory={}MB, disk={}GB, dir={}, compression={}",
            config.memory_capacity / (1024 * 1024),
            config.disk_capacity / (1024 * 1024 * 1024),
            config.disk_cache_dir,
            if config.compression_level > 0 { "enabled" } else { "disabled" }
        );
 
        let store = Self {
            inner,
            cache: Arc::new(cache),
            config,
            metrics: Arc::new(RwLock::new(CacheMetrics::default())),
            access_patterns: Arc::new(RwLock::new(HashMap::new())),
        };

        // Optionally warm the cache
        if store.config.enable_cache_warming {
            tokio::spawn({
                let store_clone = store.clone();
                async move {
                    if let Err(e) = store_clone.warm_cache().await {
                        warn!("Cache warming failed: {}", e);
                    }
                }
            });
        }

        Ok(store)
    }

    /// Get current cache metrics
    pub async fn metrics(&self) -> CacheMetrics {
        self.metrics.read().await.clone()
    }

    /// Warm the cache by pre-loading frequently accessed files
    async fn warm_cache(&self) -> ObjectStoreResult<()> {
        info!("Starting cache warming...");
        
        // Focus on Delta log directory
        let delta_log_prefix = Path::from("_delta_log");
        let mut stream = self.inner.list(Some(&delta_log_prefix));
        let mut warmed_count = 0;

        while let Some(meta_result) = futures::StreamExt::next(&mut stream).await {
            let meta = meta_result?;
            
            // Only warm small, frequently accessed files
            if meta.size <= (1024 * 1024) && self.should_cache(&meta.location) {
                match self.inner.get(&meta.location).await {
                    Ok(result) => {
                        if let Ok(bytes) = result.bytes().await {
                            let cache_key = self.make_cache_key(&meta.location, None);
                            let cached_obj = CachedObject::new(
                                bytes.to_vec(), 
                                &meta, 
                                self.config.compression_level
                            );
                            
                            if self.cache.insert(cache_key, cached_obj).get_data().is_ok() {
                                warmed_count += 1;
                            }
                        }
                    }
                    Err(e) => debug!("Failed to warm cache for {}: {}", meta.location, e),
                }
            }
        }

        info!("Cache warming completed: {} files preloaded", warmed_count);
        Ok(())
    }

    /// Enhanced path caching logic with more granular control
    fn should_cache(&self, path: &Path) -> bool {
        let path_str = path.as_ref();
        
        // Always cache Delta log directory contents
        if path_str.contains("_delta_log/") {
            // Transaction logs
            if path_str.ends_with(".json") && self.config.cache_transaction_logs {
                return true;
            }
            // Checkpoint files
            if path_str.ends_with(".checkpoint.parquet") && self.config.cache_checkpoints {
                return true;
            }
            // Other Delta log files (like .crc files)
            return true;
        }
        
        // Parquet metadata files
        if self.config.cache_parquet_metadata
            && (path_str.ends_with(".parquet") 
                || path_str.contains("_metadata")
                || path_str.ends_with("_common_metadata")) {
                return true;
            }
        
        false
    }

    /// Create cache key from path and optional range
    fn make_cache_key(&self, path: &Path, range: Option<&GetRange>) -> String {
        match range {
            Some(GetRange::Bounded(r)) => format!("{}:{}:{}", path.as_ref(), r.start, r.end),
            Some(GetRange::Offset(offset)) => format!("{}:{}:", path.as_ref(), offset),
            Some(GetRange::Suffix(suffix)) => format!("{}:suffix:{}", path.as_ref(), suffix),
            None => path.as_ref().to_string(),
        }
    }

    /// Update access patterns for analytics
    async fn record_access(&self, path: &str) {
        let mut patterns = self.access_patterns.write().await;
        *patterns.entry(path.to_string()).or_insert(0) += 1;
    }

    /// Update metrics
    async fn update_metrics<F>(&self, update_fn: F) 
    where
        F: FnOnce(&mut CacheMetrics),
    {
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            update_fn(&mut metrics);
        }
    }

    /// Enhanced cache retrieval with metadata validation
    async fn get_with_cache(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.update_metrics(|m| m.total_requests += 1).await;
        
        // Check if we should cache this path
        if !self.should_cache(location) {
            debug!("Path not cacheable, delegating: {}", location);
            return self.inner.get_opts(location, options).await;
        }

        // Record access pattern
        self.record_access(location.as_ref()).await;

        // For range requests, bypass cache for now (could be enhanced later)
        if options.range.is_some() {
            debug!("Range request, bypassing cache: {}", location);
            return self.inner.get_opts(location, options).await;
        }

        let cache_key = self.make_cache_key(location, None);
        
        // Try to get from cache first
        if let Ok(Some(cached_entry)) = self.cache.get(&cache_key).await {
            let cached_obj = cached_entry.value();
            
            // Check if cache entry is still valid
            if cached_obj.is_valid(self.config.ttl_seconds) {
                // Get fresh metadata for validation
                match self.inner.head(location).await {
                    Ok(meta) => {
                        // Validate cache against metadata
                        if cached_obj.matches_meta(&meta) {
                            debug!("Cache hit for: {}", location);
                            self.update_metrics(|m| m.hits += 1).await;
                            
                            // Decompress if needed
                            match cached_obj.get_data() {
                                Ok(data) => {
                                    let bytes = Bytes::from(data);
                                    return Ok(GetResult {
                                        payload: GetResultPayload::Stream(
                                            Box::pin(futures::stream::once(async { Ok(bytes) }))
                                        ),
                                        meta: meta.clone(),
                                        range: 0..meta.size,
                                        attributes: Default::default(),
                                    });
                                }
                                Err(e) => {
                                    error!("Failed to decompress cached data for {}: {}", location, e);
                                    // Fall through to cache miss
                                }
                            }
                        } else {
                            debug!("Cache invalidated due to metadata mismatch: {}", location);
                            // Remove stale entry
                            let _ = self.cache.remove(&cache_key);
                        }
                    }
                    Err(e) => {
                        debug!("Failed to get metadata for cache validation: {} - {}", location, e);
                        // Use cached data anyway if metadata lookup fails
                        if let Ok(data) = cached_obj.get_data() {
                            self.update_metrics(|m| m.hits += 1).await;
                            let bytes = Bytes::from(data);
                            return Ok(GetResult {
                                payload: GetResultPayload::Stream(
                                    Box::pin(futures::stream::once(async { Ok(bytes) }))
                                ),
                                meta: ObjectMeta {
                                    location: location.clone(),
                                    last_modified:  DateTime::<chrono::Utc>::MIN_UTC,
                                    size: cached_obj.original_size ,
                                    e_tag: cached_obj.etag.clone(),
                                    version: None,
                                },
                                range: 0..cached_obj.original_size ,
                                attributes: Default::default(),
                            });
                        }
                    }
                }
            } else {
                debug!("Cache entry expired: {}", location);
                self.cache.remove(&cache_key);
            }
        }

        // Cache miss - fetch from underlying store
        debug!("Cache miss, fetching: {}", location);
        self.update_metrics(|m| m.misses += 1).await;
        
        let result = self.inner.get_opts(location, options.clone()).await?;
        let meta = result.meta.clone();
        
        // Only cache if object size is within limits
        if meta.size <= self.config.max_object_size  {
            // Read the entire payload for caching
            let bytes = result.bytes().await?;
            
            // Create cached object with compression
            let cached_obj = CachedObject::new(
                bytes.to_vec(), 
                &meta, 
                self.config.compression_level
            );
            
            // Insert into cache asynchronously
            let cache_clone = self.cache.clone();
            let key_clone = cache_key.clone();
            tokio::spawn(async move {
                if let Err(e) = cache_clone.insert(key_clone, cached_obj).get_data() {
                    debug!("Failed to insert into cache: {}", e);
                }
            });
            
            debug!("Cached object: {} (size: {} bytes)", location, bytes.len());
            
            // Return the data
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    Box::pin(futures::stream::once(async { Ok(bytes) }))
                ),
                meta: meta.clone(),
                range: 0..meta.size,
                attributes: Default::default(),
            })
        } else {
            warn!("Object too large to cache: {} ({} bytes)", location, meta.size);
            Ok(result)
        }
    }

    /// Get access patterns for analytics
    pub async fn get_access_patterns(&self) -> HashMap<String, u64> {
        self.access_patterns.read().await.clone()
    }

    /// Clear access patterns
    pub async fn clear_access_patterns(&self) {
        self.access_patterns.write().await.clear();
    }
}

// Clone implementation for the store
impl Clone for DeltaCachedObjectStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            cache: self.cache.clone(),
            config: self.config.clone(),
            metrics: self.metrics.clone(),
            access_patterns: self.access_patterns.clone(),
        }
    }
}

#[async_trait]
impl ObjectStore for DeltaCachedObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> ObjectStoreResult<PutResult> {
        let result = self.inner.put(location, payload).await?;
        
        // Invalidate cache for this path on writes
        if self.should_cache(location) {
            let cache_key = self.make_cache_key(location, None);
            let _ = self.cache.remove(&cache_key);
            debug!("Invalidated cache for: {}", location);
        }
        
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let result = self.inner.put_opts(location, payload, opts).await?;
        
        // Invalidate cache for this path on writes
        if self.should_cache(location) {
            let cache_key = self.make_cache_key(location, None);
            let _ = self.cache.remove(&cache_key);
            debug!("Invalidated cache for: {}", location);
        }
        
        Ok(result)
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        self.get_with_cache(location, GetOptions::default()).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.get_with_cache(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> ObjectStoreResult<Bytes> {
        let options = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..Default::default()
        };
        let result = self.get_with_cache(location, options).await?;
        result.bytes().await
    }

    async fn head(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        // For metadata requests, always fetch fresh data
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.delete(location).await;
        
        // Invalidate cache on delete
        if self.should_cache(location) {
            let cache_key = self.make_cache_key(location, None);
            let _ = self.cache.remove(&cache_key);
            debug!("Invalidated cache on delete: {}", location);
        }
        
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.map(|p| p.to_owned());
        Box::pin(stream! {
            let mut stream = inner.list(prefix.as_ref());
            use futures::StreamExt;
            while let Some(item) = stream.next().await {
                yield item;
            }
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.copy(from, to).await;
        
        // Invalidate cache for destination
        if self.should_cache(to) {
            let cache_key = self.make_cache_key(to, None);
            let _ = self.cache.remove(&cache_key);
            debug!("Invalidated cache on copy destination: {}", to);
        }
        
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        let result = self.inner.copy_if_not_exists(from, to).await;
        
        // Invalidate cache for destination
        if self.should_cache(to) {
            let cache_key = self.make_cache_key(to, None);
            let _ = self.cache.remove(&cache_key);
            debug!("Invalidated cache on conditional copy: {}", to);
        }
        
        result
    }
}

impl std::fmt::Display for DeltaCachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaCached({})", self.inner)
    }
}

/// Enhanced builder with more configuration options
pub struct DeltaCacheBuilder {
    config: DeltaCacheConfig,
}

impl DeltaCacheBuilder {
    pub fn new() -> Self {
        Self {
            config: DeltaCacheConfig::default(),
        }
    }

    pub fn with_memory_capacity(mut self, capacity: usize) -> Self {
        self.config.memory_capacity = capacity;
        self
    }

    pub fn with_disk_capacity(mut self, capacity: usize) -> Self {
        self.config.disk_capacity = capacity;
        self
    }

    pub fn with_disk_path<P: Into<String>>(mut self, path: P) -> Self {
        self.config.disk_cache_dir = path.into();
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl_seconds = ttl.as_secs();
        self
    }

    pub fn with_max_object_size(mut self, size: usize) -> Self {
        self.config.max_object_size = size;
        self
    }

    pub fn with_compression(mut self, level: u8) -> Self {
        self.config.compression_level = level.min(9);
        self
    }

    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.config.enable_metrics = enable;
        self
    }

    pub fn enable_cache_warming(mut self, enable: bool) -> Self {
        self.config.enable_cache_warming = enable;
        self
    }

    pub fn cache_transaction_logs(mut self, enable: bool) -> Self {
        self.config.cache_transaction_logs = enable;
        self
    }

    pub fn cache_parquet_metadata(mut self, enable: bool) -> Self {
        self.config.cache_parquet_metadata = enable;
        self
    }

    pub fn cache_checkpoints(mut self, enable: bool) -> Self {
        self.config.cache_checkpoints = enable;
        self
    }

    pub async fn build(
        self,
        inner: Arc<dyn ObjectStore>,
    ) -> ObjectStoreResult<Arc<DeltaCachedObjectStore>> {
        let cached_store = DeltaCachedObjectStore::new(inner, self.config).await?;
        Ok(Arc::new(cached_store))
    }
}

impl Default for DeltaCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}