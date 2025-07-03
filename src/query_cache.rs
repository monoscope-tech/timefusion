use datafusion::arrow::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache key for query results
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryCacheKey {
    pub query_hash: u64,
    pub project_id: String,
    pub schema_version: u32,
}

impl QueryCacheKey {
    pub fn new(query: &str, project_id: &str, schema_version: u32) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        query.hash(&mut hasher);
        let query_hash = hasher.finish();

        Self {
            query_hash,
            project_id: project_id.to_string(),
            schema_version,
        }
    }
}

/// Cached query result with metadata
#[derive(Debug, Clone)]
pub struct CachedQueryResult {
    pub result: Vec<RecordBatch>,
    pub created_at: Instant,
    pub access_count: u64,
    pub last_accessed: Instant,
    pub execution_time_ms: u64,
    pub row_count: usize,
}

impl CachedQueryResult {
    pub fn new(result: Vec<RecordBatch>, execution_time_ms: u64) -> Self {
        let row_count = result.iter().map(|batch| batch.num_rows()).sum();
        let now = Instant::now();
        
        Self {
            result,
            created_at: now,
            access_count: 0,
            last_accessed: now,
            execution_time_ms,
            row_count,
        }
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }

    pub fn access(&mut self) {
        self.access_count += 1;
        self.last_accessed = Instant::now();
    }
}

/// Query cache configuration
#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    pub max_entries: usize,
    pub ttl: Duration,
    pub max_result_size_mb: usize,
    pub enable_cache: bool,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(300), // 5 minutes
            max_result_size_mb: 50, // 50MB per result
            enable_cache: true,
        }
    }
}

/// Statistics for cache performance monitoring
#[derive(Debug, Clone, Serialize)]
pub struct QueryCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub total_entries: usize,
    pub total_size_mb: f64,
    pub hit_rate: f64,
    pub average_execution_time_ms: f64,
}

impl QueryCacheStats {
    pub fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            total_entries: 0,
            total_size_mb: 0.0,
            hit_rate: 0.0,
            average_execution_time_ms: 0.0,
        }
    }

    pub fn record_hit(&mut self) {
        self.hits += 1;
        self.update_hit_rate();
    }

    pub fn record_miss(&mut self) {
        self.misses += 1;
        self.update_hit_rate();
    }

    pub fn record_eviction(&mut self) {
        self.evictions += 1;
    }

    fn update_hit_rate(&mut self) {
        let total = self.hits + self.misses;
        self.hit_rate = if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        };
    }
}

/// Thread-safe query result cache
#[derive(Debug)]
pub struct QueryCache {
    cache: Arc<RwLock<HashMap<QueryCacheKey, CachedQueryResult>>>,
    config: QueryCacheConfig,
    stats: Arc<RwLock<QueryCacheStats>>,
}

impl QueryCache {
    pub fn new(config: QueryCacheConfig) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(RwLock::new(QueryCacheStats::new())),
        }
    }

    pub fn new_with_defaults() -> Self {
        Self::new(QueryCacheConfig::default())
    }

    /// Get cached result if available and not expired
    pub async fn get(&self, key: &QueryCacheKey) -> Option<Vec<RecordBatch>> {
        if !self.config.enable_cache {
            return None;
        }

        let mut cache = self.cache.write().await;
        let mut stats = self.stats.write().await;

        if let Some(cached) = cache.get_mut(key) {
            if cached.is_expired(self.config.ttl) {
                cache.remove(key);
                stats.record_miss();
                None
            } else {
                cached.access();
                stats.record_hit();
                Some(cached.result.clone())
            }
        } else {
            stats.record_miss();
            None
        }
    }

    /// Store query result in cache
    pub async fn put(&self, key: QueryCacheKey, result: Vec<RecordBatch>, execution_time_ms: u64) {
        if !self.config.enable_cache {
            return;
        }

        // Check if result size is within limits
        let result_size_mb = self.estimate_size_mb(&result);
        if result_size_mb > self.config.max_result_size_mb as f64 {
            tracing::debug!("Query result too large to cache: {:.2}MB", result_size_mb);
            return;
        }

        let mut cache = self.cache.write().await;
        let mut stats = self.stats.write().await;

        // Evict expired entries first
        self.evict_expired(&mut cache, &mut stats).await;

        // If still at capacity, evict LRU entries
        while cache.len() >= self.config.max_entries {
            self.evict_lru(&mut cache, &mut stats).await;
        }

        let cached_result = CachedQueryResult::new(result, execution_time_ms);
        cache.insert(key, cached_result);

        // Update stats
        stats.total_entries = cache.len();
        stats.total_size_mb = self.calculate_total_size_mb(&cache).await;
    }

    /// Clear all cached entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        let mut stats = self.stats.write().await;
        
        cache.clear();
        stats.total_entries = 0;
        stats.total_size_mb = 0.0;
    }

    /// Get current cache statistics
    pub async fn get_stats(&self) -> QueryCacheStats {
        let cache = self.cache.read().await;
        let mut stats = self.stats.read().await.clone();
        
        stats.total_entries = cache.len();
        stats.total_size_mb = self.calculate_total_size_mb(&cache).await;
        
        // Calculate average execution time
        if !cache.is_empty() {
            let total_execution_time: u64 = cache.values().map(|v| v.execution_time_ms).sum();
            stats.average_execution_time_ms = total_execution_time as f64 / cache.len() as f64;
        }

        stats
    }

    /// Evict expired entries from cache
    async fn evict_expired(&self, cache: &mut HashMap<QueryCacheKey, CachedQueryResult>, stats: &mut QueryCacheStats) {
        let expired_keys: Vec<_> = cache
            .iter()
            .filter(|(_, v)| v.is_expired(self.config.ttl))
            .map(|(k, _)| k.clone())
            .collect();

        for key in expired_keys {
            cache.remove(&key);
            stats.record_eviction();
        }
    }

    /// Evict least recently used entry
    async fn evict_lru(&self, cache: &mut HashMap<QueryCacheKey, CachedQueryResult>, stats: &mut QueryCacheStats) {
        if let Some((lru_key, _)) = cache
            .iter()
            .min_by_key(|(_, v)| v.last_accessed)
            .map(|(k, v)| (k.clone(), v.clone()))
        {
            cache.remove(&lru_key);
            stats.record_eviction();
        }
    }

    /// Estimate the memory size of a result set in MB
    fn estimate_size_mb(&self, batches: &[RecordBatch]) -> f64 {
        let total_bytes: usize = batches
            .iter()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|col| col.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();
        
        total_bytes as f64 / (1024.0 * 1024.0)
    }

    /// Calculate total cache size in MB
    async fn calculate_total_size_mb(&self, cache: &HashMap<QueryCacheKey, CachedQueryResult>) -> f64 {
        cache
            .values()
            .map(|cached| self.estimate_size_mb(&cached.result))
            .sum()
    }
}

/// Convenience function to create cache key from query string
pub fn create_cache_key(query: &str, project_id: &str) -> QueryCacheKey {
    QueryCacheKey::new(query, project_id, 1) // Default schema version
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        
        let id_array = Int32Array::from(vec![1, 2, 3]);
        
        RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let cache = QueryCache::new_with_defaults();
        let key = QueryCacheKey::new("SELECT * FROM test", "project1", 1);
        let batch = create_test_batch();
        
        // Initially empty
        assert!(cache.get(&key).await.is_none());
        
        // Put and get
        cache.put(key.clone(), vec![batch.clone()], 100).await;
        let result = cache.get(&key).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_cache_expiration() {
        let config = QueryCacheConfig {
            ttl: Duration::from_millis(10),
            ..Default::default()
        };
        let cache = QueryCache::new(config);
        let key = QueryCacheKey::new("SELECT * FROM test", "project1", 1);
        let batch = create_test_batch();
        
        cache.put(key.clone(), vec![batch], 100).await;
        
        // Should be available immediately
        assert!(cache.get(&key).await.is_some());
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(20)).await;
        
        // Should be expired
        assert!(cache.get(&key).await.is_none());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let cache = QueryCache::new_with_defaults();
        let key = QueryCacheKey::new("SELECT * FROM test", "project1", 1);
        let batch = create_test_batch();
        
        // Record miss
        cache.get(&key).await;
        
        // Record hit
        cache.put(key.clone(), vec![batch], 100).await;
        cache.get(&key).await;
        
        let stats = cache.get_stats().await;
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hit_rate, 0.5);
    }
}
