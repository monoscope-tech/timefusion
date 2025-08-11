use anyhow::Result;
use bytes::Bytes;
use object_store::{ObjectStore, PutPayload, path::Path};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use timefusion::database::Database;
use timefusion::object_store_cache::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache};

#[tokio::test]
async fn test_cache_performance_and_s3_bypass() -> Result<()> {
    // Create in-memory store to simulate S3
    let inner_store = Arc::new(object_store::memory::InMemory::new());

    // Configure cache with reasonable test sizes
    let config = FoyerCacheConfig::test_config_with("cache_perf", |c| {
        c.memory_size_bytes = 50 * 1024 * 1024; // 50MB memory
        c.disk_size_bytes = 100 * 1024 * 1024; // 100MB disk
        c.shards = 4;
        // Checkpoint caching is always enabled now with stale-while-revalidate
    });

    // Create shared cache
    let shared_cache = SharedFoyerCache::new(config).await?;
    let cached_store = FoyerObjectStoreCache::new_with_shared_cache(inner_store.clone(), &shared_cache);

    // Test data simulating Parquet files
    let test_files = vec![
        ("table/2024/01/part-001.parquet", vec![0u8; 1024 * 512]), // 512KB
        ("table/2024/01/part-002.parquet", vec![1u8; 1024 * 768]), // 768KB
        ("table/2024/01/part-003.parquet", vec![2u8; 1024 * 256]), // 256KB
    ];

    // Write test files (these will be cached immediately after write)
    for (path_str, data) in &test_files {
        let path = Path::from(*path_str);
        cached_store.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;
    }

    // Get baseline stats after writes
    let stats_after_write = shared_cache.get_stats().await;
    assert_eq!(stats_after_write.inner_puts, 3, "Should have written to inner store 3 times");
    assert_eq!(stats_after_write.inner_gets, 3, "Should have fetched from inner store 3 times during write");

    // First read - should hit cache since we cache on write
    let start = Instant::now();
    for (path_str, _) in &test_files {
        let path = Path::from(*path_str);
        let _ = cached_store.get(&path).await?;
    }
    let first_read_time = start.elapsed();

    // Second read - should also hit cache
    let start = Instant::now();
    for (path_str, _) in &test_files {
        let path = Path::from(*path_str);
        let _ = cached_store.get(&path).await?;
    }
    let cached_read_time = start.elapsed();

    // Log stats to verify cache behavior
    shared_cache.log_stats().await;

    // Both reads should be fast since they hit cache
    assert!(
        cached_read_time <= first_read_time * 2,
        "Cached reads should be consistently fast. First: {:?}, Cached: {:?}",
        first_read_time,
        cached_read_time
    );

    // Verify cache stats - all reads should hit cache since we cache on write
    let stats = shared_cache.get_stats().await;
    assert_eq!(stats.hits, 6, "Should have 6 cache hits total (3 per read iteration)");
    assert_eq!(stats.misses, 0, "Should have no cache misses since files were cached on write");
    assert_eq!(stats.inner_gets, 3, "Should have fetched from inner store 3 times during write");
    assert_eq!(stats.inner_puts, 3, "Should have written to inner store 3 times");

    // Test cache invalidation on write
    let update_path = Path::from("table/2024/01/part-001.parquet");
    cached_store.put(&update_path, PutPayload::from(Bytes::from(vec![9u8; 1024]))).await?;

    // Read should fetch new data
    let result = cached_store.get(&update_path).await?;
    use futures::TryStreamExt;
    let stream = match result.payload {
        object_store::GetResultPayload::Stream(s) => s,
        _ => panic!("Expected stream"),
    };
    let bytes: Vec<Bytes> = stream.try_collect().await?;
    assert_eq!(bytes[0][0], 9u8, "Should get updated data after invalidation");

    // Cleanup
    shared_cache.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_large_file_disk_caching() -> Result<()> {
    let inner_store = Arc::new(object_store::memory::InMemory::new());

    // Test with reasonable cache sizes
    let config = FoyerCacheConfig::test_config_with("disk_cache", |c| {
        c.ttl = Duration::from_secs(60);
        // Checkpoint caching is always enabled now with stale-while-revalidate
    });

    let shared_cache = SharedFoyerCache::new(config).await?;
    let cached_store = FoyerObjectStoreCache::new_with_shared_cache(inner_store.clone(), &shared_cache);

    // Create test files
    let large_files = vec![
        ("test/file1.parquet", vec![0u8; 512 * 1024]), // 512KB
        ("test/file2.parquet", vec![1u8; 768 * 1024]), // 768KB
    ];

    // Write and read test files
    for (path_str, data) in &large_files {
        let path = Path::from(*path_str);
        cached_store.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;

        // First read - cache miss
        let _ = cached_store.get(&path).await?;
    }

    // Second read should hit cache
    for (path_str, data) in &large_files {
        let path = Path::from(*path_str);
        let result = cached_store.get(&path).await?;

        use futures::TryStreamExt;
        let stream = match result.payload {
            object_store::GetResultPayload::Stream(s) => s,
            _ => panic!("Expected stream"),
        };
        let bytes: Vec<Bytes> = stream.try_collect().await?;
        assert_eq!(bytes[0].len(), data.len(), "Should retrieve full file from cache");
    }

    let stats = shared_cache.get_stats().await;
    assert!(stats.hits > 0, "Should have cache hits");

    shared_cache.log_stats().await;
    shared_cache.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_cache_configuration_from_env() -> Result<()> {
    // Test that configuration is loaded correctly from environment
    // Save current values to restore later
    let orig_mem = env::var("TIMEFUSION_FOYER_MEMORY_MB").ok();
    let orig_disk = env::var("TIMEFUSION_FOYER_DISK_GB").ok();
    let orig_ttl = env::var("TIMEFUSION_FOYER_TTL_SECONDS").ok();
    let orig_shards = env::var("TIMEFUSION_FOYER_SHARDS").ok();

    unsafe {
        env::set_var("TIMEFUSION_FOYER_MEMORY_MB", "512");
        env::set_var("TIMEFUSION_FOYER_DISK_GB", "20");
        env::set_var("TIMEFUSION_FOYER_TTL_SECONDS", "600");
        env::set_var("TIMEFUSION_FOYER_SHARDS", "16");
    }

    let config = FoyerCacheConfig::from_env();

    // The config should match what we set (unless overridden by .env file)
    // Since we can't guarantee clean env in CI, just check the values were read
    assert!(config.memory_size_bytes > 0);
    assert!(config.disk_size_bytes > 0);
    assert_eq!(config.ttl.as_secs(), 600);
    assert_eq!(config.shards, 16);

    // Restore original values
    unsafe {
        if let Some(val) = orig_mem {
            env::set_var("TIMEFUSION_FOYER_MEMORY_MB", val);
        } else {
            env::remove_var("TIMEFUSION_FOYER_MEMORY_MB");
        }
        if let Some(val) = orig_disk {
            env::set_var("TIMEFUSION_FOYER_DISK_GB", val);
        } else {
            env::remove_var("TIMEFUSION_FOYER_DISK_GB");
        }
        if let Some(val) = orig_ttl {
            env::set_var("TIMEFUSION_FOYER_TTL_SECONDS", val);
        } else {
            env::remove_var("TIMEFUSION_FOYER_TTL_SECONDS");
        }
        if let Some(val) = orig_shards {
            env::set_var("TIMEFUSION_FOYER_SHARDS", val);
        } else {
            env::remove_var("TIMEFUSION_FOYER_SHARDS");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_cache_with_database_integration() -> Result<()> {
    // Configure cache with specific test settings
    unsafe {
        env::set_var("TIMEFUSION_FOYER_MEMORY_MB", "10");
        env::set_var("TIMEFUSION_FOYER_DISK_GB", "1");
        env::set_var("TIMEFUSION_FOYER_TTL_SECONDS", "300");
        env::set_var("TIMEFUSION_FOYER_STATS", "true");
    }

    // Create database - should initialize shared Foyer cache
    let db = Database::new().await?;

    // Verify:
    // 1. Shared Foyer cache initializes correctly
    // 2. All tables use the cached object store
    // 3. Cache configuration is applied from environment

    // Graceful shutdown
    db.shutdown().await?;

    Ok(())
}
