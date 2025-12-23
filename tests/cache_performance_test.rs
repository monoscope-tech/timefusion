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
    assert_eq!(stats_after_write.main.inner_puts, 3, "Should have written to inner store 3 times");
    assert_eq!(
        stats_after_write.main.inner_gets, 3,
        "Should have fetched from inner store 3 times during write"
    );

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
    assert_eq!(stats.main.hits, 6, "Should have 6 cache hits total (3 per read iteration)");
    assert_eq!(stats.main.misses, 0, "Should have no cache misses since files were cached on write");
    assert_eq!(stats.main.inner_gets, 3, "Should have fetched from inner store 3 times during write");
    assert_eq!(stats.main.inner_puts, 3, "Should have written to inner store 3 times");

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
    assert!(stats.main.hits > 0, "Should have cache hits");

    shared_cache.log_stats().await;
    shared_cache.shutdown().await?;

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

#[tokio::test]
async fn test_parquet_metadata_cache_performance() -> Result<()> {
    // Use in-memory store for testing
    let inner = Arc::new(object_store::memory::InMemory::new());

    // Configure cache with metadata optimization
    let config = FoyerCacheConfig {
        memory_size_bytes: 50 * 1024 * 1024, // 50MB
        disk_size_bytes: 100 * 1024 * 1024,  // 100MB
        ttl: std::time::Duration::from_secs(300),
        cache_dir: std::path::PathBuf::from("/tmp/test_parquet_metadata_perf"),
        shards: 4,
        file_size_bytes: 4 * 1024 * 1024, // 4MB
        enable_stats: true,
        parquet_metadata_size_hint: 1_048_576,        // 1MB
        metadata_memory_size_bytes: 20 * 1024 * 1024, // 20MB
        metadata_disk_size_bytes: 50 * 1024 * 1024,   // 50MB
        metadata_shards: 2,
    };

    // Clean up cache directory
    let cache_dir = config.cache_dir.clone();
    let _ = std::fs::remove_dir_all(&cache_dir);

    let cache = Arc::new(FoyerObjectStoreCache::new(inner.clone(), config).await?);

    // Create multiple large parquet files (simulating real scenario)
    let file_count = 10;
    let file_size = 50 * 1024 * 1024; // 50MB each
    let metadata_size = 1024 * 1024; // 1MB metadata

    println!("Creating {} parquet files of {}MB each...", file_count, file_size / 1024 / 1024);

    for i in 0..file_count {
        let path = Path::from(format!("data/part-{:04}.parquet", i));
        let data = vec![b'x'; file_size];
        inner.put(&path, PutPayload::from(Bytes::from(data))).await?;
    }

    // Get initial stats
    let initial_stats = cache.get_stats().await;

    // Test 1: Read metadata from all files (cold cache)
    println!("\nTest 1: Reading metadata with cold cache...");
    let start = Instant::now();

    for i in 0..file_count {
        let path = Path::from(format!("data/part-{:04}.parquet", i));
        let metadata_range = (file_size - metadata_size) as u64..file_size as u64;
        let _ = cache.get_range(&path, metadata_range).await?;
    }

    let cold_duration = start.elapsed();
    let cold_stats = cache.get_stats().await;

    println!("Cold cache duration: {:?}", cold_duration);
    println!(
        "Cold cache stats: metadata_hits={}, metadata_misses={}, metadata_inner_gets={}",
        cold_stats.metadata.hits - initial_stats.metadata.hits,
        cold_stats.metadata.misses - initial_stats.metadata.misses,
        cold_stats.metadata.inner_gets - initial_stats.metadata.inner_gets
    );

    // Test 2: Read metadata again (warm cache)
    println!("\nTest 2: Reading metadata with warm cache...");
    let start = Instant::now();

    for i in 0..file_count {
        let path = Path::from(format!("data/part-{:04}.parquet", i));
        let metadata_range = (file_size - metadata_size) as u64..file_size as u64;
        let _ = cache.get_range(&path, metadata_range).await?;
    }

    let warm_duration = start.elapsed();
    let final_stats = cache.get_stats().await;

    println!("Warm cache duration: {:?}", warm_duration);
    println!(
        "Final stats: metadata_hits={}, metadata_misses={}, metadata_inner_gets={}",
        final_stats.metadata.hits, final_stats.metadata.misses, final_stats.metadata.inner_gets
    );

    // Calculate speedup
    let speedup = cold_duration.as_secs_f64() / warm_duration.as_secs_f64();
    println!("\nSpeedup: {:.2}x", speedup);

    // Calculate data savings
    let cold_inner_gets = cold_stats.metadata.inner_gets - initial_stats.metadata.inner_gets;
    let data_fetched = cold_inner_gets as usize * metadata_size;
    let data_saved = file_count * file_size - data_fetched;
    println!(
        "Data fetched: {}MB (instead of {}MB)",
        data_fetched / 1024 / 1024,
        file_count * file_size / 1024 / 1024
    );
    println!(
        "Data saved: {}MB ({:.1}% reduction)",
        data_saved / 1024 / 1024,
        (data_saved as f64 / (file_count * file_size) as f64) * 100.0
    );

    // Verify correctness
    assert_eq!(final_stats.metadata.hits - cold_stats.metadata.hits, file_count as u64);
    assert_eq!(final_stats.metadata.inner_gets, cold_stats.metadata.inner_gets); // No new fetches

    // Clean up
    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cache_dir);

    Ok(())
}
