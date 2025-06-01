use std::{path::PathBuf, sync::Arc};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::time::{Duration, Instant};
use std::fs::remove_dir_all;
use std::io;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use foyer::{Cache, CacheBuilder};
use object_store::{ObjectStore, path::Path, PutPayload};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use metrics::{counter, describe_counter, describe_histogram, gauge, histogram, Counter, Gauge};

/// Constants for cache configuration
pub const DEFAULT_MIN_FETCH_SIZE: u64 = 1024 * 1024; // 1 MiB
pub const DEFAULT_CACHE_CAPACITY: u64 = 1024 * 1024 * 1024; // 1 GiB
pub const DEFAULT_CACHE_ENTRY_TTL: Duration = Duration::from_secs(3 * 60);

/// Cache key that includes both path and range information
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct CacheKey {
    path: Path,
    range: Range<usize>,
}

impl Debug for CacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:?}", self.path, self.range)
    }
}

impl CacheKey {
    fn as_filename(&self) -> String {
        format!(
            "{}-{}-{}",
            self.path.to_string().replace('/', "_"),
            self.range.start,
            self.range.end
        )
    }
}

/// Metrics for the object store cache
#[derive(Clone)]
pub struct ObjectStoreCacheMetrics {
    cache_hits: Counter,
    cache_misses: Counter,
    cache_evictions: Counter,
    cache_size: Gauge,
    cache_capacity: Gauge,
    cache_read_latency: Counter,
    cache_write_latency: Counter,
}

impl ObjectStoreCacheMetrics {
    pub fn new() -> Self {
        describe_counter!("object_store_cache_hits", "Number of cache hits");
        describe_counter!("object_store_cache_misses", "Number of cache misses");
        describe_counter!("object_store_cache_evictions", "Number of cache evictions");
        describe_gauge!("object_store_cache_size", "Current cache size in bytes");
        describe_gauge!("object_store_cache_capacity", "Cache capacity in bytes");
        describe_histogram!("object_store_cache_read_latency", "Cache read latency in seconds");
        describe_histogram!("object_store_cache_write_latency", "Cache write latency in seconds");

        Self {
            cache_hits: counter!("object_store_cache_hits"),
            cache_misses: counter!("object_store_cache_misses"),
            cache_evictions: counter!("object_store_cache_evictions"),
            cache_size: gauge!("object_store_cache_size"),
            cache_capacity: gauge!("object_store_cache_capacity"),
            cache_read_latency: counter!("object_store_cache_read_latency"),
            cache_write_latency: counter!("object_store_cache_write_latency"),
        }
    }
}

/// A hybrid cache implementation for object store using foyer
pub struct ObjectStoreCache {
    cache: Arc<RwLock<Cache<CacheKey, Bytes>>>,
    object_store: Arc<dyn ObjectStore>,
    min_fetch_size: u64,
    max_cache_size: u64,
    base_path: PathBuf,
    metrics: ObjectStoreCacheMetrics,
}

impl ObjectStoreCache {
    /// Create a new ObjectStoreCache instance
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        base_path: PathBuf,
        min_fetch_size: u64,
        max_cache_size: u64,
        ttl: Duration,
    ) -> Self {
        let metrics = ObjectStoreCacheMetrics::new();
        metrics.cache_capacity.set(max_cache_size as f64);
        metrics.cache_size.set(0.0);

        let cache = CacheBuilder::new(max_cache_size)
            .time_to_live(ttl)
            .build();

        Self {
            cache: Arc::new(RwLock::new(cache)),
            object_store,
            min_fetch_size,
            max_cache_size,
            base_path,
            metrics,
        }
    }

    /// Get a range of data from the cache or object store
    pub async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> Result<Bytes> {
        debug!("{location}-{range:?} get_range");
        
        // Expand the range to the next min_fetch_size (+ alignment)
        let start_chunk = range.start / self.min_fetch_size as usize;
        let end_chunk = (range.end - 1) / self.min_fetch_size as usize;

        let mut result = BytesMut::with_capacity(
            (end_chunk.saturating_sub(start_chunk) + 1) * self.min_fetch_size as usize,
        );

        for chunk in start_chunk..=end_chunk {
            let chunk_range = (chunk * self.min_fetch_size as usize)
                ..((chunk + 1) * self.min_fetch_size as usize);

            let key = CacheKey {
                path: location.to_owned(),
                range: chunk_range.clone(),
            };

            let chunk_data = match self.cache.read().await.get(&key) {
                Some(data) => {
                    debug!("Cache hit for {key:?}");
                    self.metrics.cache_hits.increment(1);
                    data.clone()
                }
                None => {
                    debug!("Cache miss for {key:?}, fetching from object store");
                    self.metrics.cache_misses.increment(1);
                    let start = Instant::now();
                    let data = self.object_store.get_range(location, chunk_range.clone()).await?;
                    self.metrics.cache_read_latency.increment(start.elapsed().as_secs_f64());
                    self.cache.write().await.insert(key, data.clone());
                    data
                }
            };

            result.extend_from_slice(&chunk_data);
        }

        // Trim the result to match the requested range
        let offset = range.start - start_chunk * self.min_fetch_size as usize;
        result.advance(offset);
        result.truncate(range.end - range.start);

        debug!("{location}-{range:?} return");
        Ok(result.into())
    }

    /// Put data into both cache and object store
    pub async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        // Store in object store
        self.object_store.put(location, bytes.clone().into()).await?;
        
        // Store in cache
        let key = CacheKey {
            path: location.to_owned(),
            range: 0..bytes.len(),
        };
        self.cache.write().await.insert(key, bytes);
        
        Ok(())
    }

    /// Remove data from both cache and object store
    pub async fn remove(&self, location: &Path) -> Result<()> {
        // Remove from object store
        self.object_store.delete(location).await?;
        
        // Remove from cache
        let key = CacheKey {
            path: location.to_owned(),
            range: 0..usize::MAX,
        };
        self.cache.write().await.remove(&key);
        
        Ok(())
    }
}

impl Debug for ObjectStoreCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreCache")
            .field("min_fetch_size", &self.min_fetch_size)
            .field("max_cache_size", &self.max_cache_size)
            .field("base_path", &self.base_path)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_object_store_cache() -> Result<()> {
        let temp_dir = tempdir()?;
        let object_store = Arc::new(InMemory::new());
        let cache = ObjectStoreCache::new(
            object_store,
            temp_dir.path().to_path_buf(),
            DEFAULT_MIN_FETCH_SIZE,
            DEFAULT_CACHE_CAPACITY,
            DEFAULT_CACHE_ENTRY_TTL,
        );

        let path = Path::from("test.txt");
        let data = Bytes::from("test data");

        // Test put and get_range
        cache.put(&path, data.clone()).await?;
        let retrieved = cache.get_range(&path, 0..data.len()).await?;
        assert_eq!(retrieved, data);

        // Test remove
        cache.remove(&path).await?;
        assert!(cache.get_range(&path, 0..data.len()).await.is_err());

        Ok(())
    }
}

