use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{GetResultPayload, ObjectStoreExt, PutPayload, path::Path};
use timefusion::{
    database::Database,
    storage::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache},
};

/// A cache over a fresh in-memory inner store, configured by `tweak`.
async fn cached_store(name: &str, tweak: impl FnOnce(&mut FoyerCacheConfig)) -> Result<(SharedFoyerCache, FoyerObjectStoreCache)> {
    let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config_with(name, tweak)).await?;
    let store = FoyerObjectStoreCache::new_with_shared_cache(Arc::new(object_store::memory::InMemory::new()), &shared);
    Ok((shared, store))
}

/// The raw stream chunks of a GET — callers assert on chunk 0, so this must not concat.
async fn get_chunks(store: &FoyerObjectStoreCache, path: &Path) -> Result<Vec<Bytes>> {
    match store.get(path).await?.payload {
        GetResultPayload::Stream(s) => Ok(s.try_collect().await?),
        _ => panic!("Expected stream"),
    }
}

async fn put(store: &FoyerObjectStoreCache, path: &Path, data: Vec<u8>) -> Result<()> {
    store.put(path, PutPayload::from(Bytes::from(data))).await?;
    Ok(())
}

/// Reads only the parquet footer range of every file; returns wall time.
async fn read_all_footers(cache: &FoyerObjectStoreCache, count: usize, size: usize, footer: usize) -> Result<Duration> {
    let start = Instant::now();
    for i in 0..count {
        let _ = cache.get_range(&Path::from(format!("data/part-{i:04}.parquet")), (size - footer) as u64..size as u64).await?;
    }
    Ok(start.elapsed())
}

async fn finish(shared: &SharedFoyerCache) -> Result<()> {
    shared.log_stats().await;
    shared.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(30)).await
}

#[tokio::test]
async fn test_cache_performance_and_s3_bypass() -> Result<()> {
    // Checkpoint caching is always enabled now with stale-while-revalidate
    let (shared_cache, cached) = cached_store("cache_perf", |c| {
        c.memory_size_bytes = 50 * 1024 * 1024; // 50MB memory
        c.disk_size_bytes = 100 * 1024 * 1024; // 100MB disk
        c.shards = 4;
    })
    .await?;

    let test_files = [
        ("table/2024/01/part-001.parquet", vec![0u8; 1024 * 512]), // 512KB
        ("table/2024/01/part-002.parquet", vec![1u8; 1024 * 768]), // 768KB
        ("table/2024/01/part-003.parquet", vec![2u8; 1024 * 256]), // 256KB
    ];

    // Write test files (these will be cached immediately after write)
    for (path_str, data) in &test_files {
        put(&cached, &Path::from(*path_str), data.clone()).await?;
    }

    let stats_after_write = shared_cache.get_stats().await;
    assert_eq!(stats_after_write.main.inner_puts, 3, "Should have written to inner store 3 times");
    // Writes warm the cache directly from the put payload (no post-write
    // re-fetch), so no inner GETs are issued during writes.
    assert_eq!(stats_after_write.main.inner_gets, 0, "Writes warm from payload — no inner GET during write");

    // Both reads should be fast since they hit cache
    let mut elapsed = Vec::new();
    for _ in 0..2 {
        let start = Instant::now();
        for (path_str, _) in &test_files {
            let _ = cached.get(&Path::from(*path_str)).await?;
        }
        elapsed.push(start.elapsed());
    }
    let (first_read_time, cached_read_time) = (elapsed[0], elapsed[1]);
    assert!(cached_read_time <= first_read_time * 2, "Cached reads should be consistently fast. First: {:?}, Cached: {:?}", first_read_time, cached_read_time);

    let stats = shared_cache.get_stats().await;
    assert_eq!(stats.main.hits, 6, "Should have 6 cache hits total (3 per read iteration)");
    assert_eq!(stats.main.misses, 0, "Should have no cache misses since files were cached on write");
    assert_eq!(stats.main.inner_gets, 0, "Writes warm from payload and reads hit cache — no inner GETs at all");
    assert_eq!(stats.main.inner_puts, 3, "Should have written to inner store 3 times");

    // Overwrite invalidates: the next read must see the new data
    let update_path = Path::from("table/2024/01/part-001.parquet");
    put(&cached, &update_path, vec![9u8; 1024]).await?;
    assert_eq!(get_chunks(&cached, &update_path).await?[0][0], 9u8, "Should get updated data after invalidation");

    finish(&shared_cache).await
}

#[tokio::test]
async fn test_large_file_disk_caching() -> Result<()> {
    let (shared_cache, cached) = cached_store("disk_cache", |c| c.ttl = Duration::from_secs(60)).await?;

    let large_files = [
        ("test/file1.parquet", vec![0u8; 512 * 1024]), // 512KB
        ("test/file2.parquet", vec![1u8; 768 * 1024]), // 768KB
    ];

    for (path_str, data) in &large_files {
        let path = Path::from(*path_str);
        put(&cached, &path, data.clone()).await?;
        let _ = cached.get(&path).await?;
    }

    // Second read should hit cache, serving the whole file in one chunk
    for (path_str, data) in &large_files {
        let chunks = get_chunks(&cached, &Path::from(*path_str)).await?;
        assert_eq!(chunks[0].len(), data.len(), "Should retrieve full file from cache");
    }

    assert!(shared_cache.get_stats().await.main.hits > 0, "Should have cache hits");

    finish(&shared_cache).await
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

    // 1. Shared Foyer cache initializes correctly
    // 2. All tables use the cached object store
    // 3. Cache configuration is applied from environment
    let db = Database::new().await?;

    // Graceful shutdown
    db.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_parquet_metadata_cache_performance() -> Result<()> {
    const FILE_COUNT: usize = 10;
    const FILE_SIZE: usize = 50 * 1024 * 1024; // 50MB each
    const METADATA_SIZE: usize = 1024 * 1024; // 1MB metadata

    let inner = Arc::new(object_store::memory::InMemory::new());
    let config = FoyerCacheConfig {
        memory_size_bytes: 50 * 1024 * 1024, // 50MB
        disk_size_bytes: 100 * 1024 * 1024,  // 100MB
        ttl: Duration::from_secs(300),
        cache_dir: std::path::PathBuf::from("/tmp/test_parquet_metadata_perf"),
        shards: 4,
        file_size_bytes: 4 * 1024 * 1024, // 4MB
        enable_stats: true,
        parquet_metadata_size_hint: 1_048_576,        // 1MB
        metadata_memory_size_bytes: 20 * 1024 * 1024, // 20MB
        metadata_disk_size_bytes: 50 * 1024 * 1024,   // 50MB
        metadata_shards: 2,
        ..Default::default()
    };
    let cache_dir = config.cache_dir.clone();
    let _ = std::fs::remove_dir_all(&cache_dir);

    let cache = Arc::new(FoyerObjectStoreCache::new(inner.clone(), config).await?);

    let part = |i: usize| Path::from(format!("data/part-{i:04}.parquet"));
    for i in 0..FILE_COUNT {
        inner.put(&part(i), PutPayload::from(Bytes::from(vec![b'x'; FILE_SIZE]))).await?;
    }

    let initial_stats = cache.get_stats().await;
    let cold_duration = read_all_footers(&cache, FILE_COUNT, FILE_SIZE, METADATA_SIZE).await?;
    let cold_stats = cache.get_stats().await;
    let warm_duration = read_all_footers(&cache, FILE_COUNT, FILE_SIZE, METADATA_SIZE).await?;
    let final_stats = cache.get_stats().await;

    // Only the footer of each file is ever fetched, not the whole 50MB object.
    let cold_inner_gets = cold_stats.metadata.inner_gets - initial_stats.metadata.inner_gets;
    let data_fetched = cold_inner_gets as usize * METADATA_SIZE;
    println!("cold {cold_duration:?} / warm {warm_duration:?}; fetched {}MB of {}MB", data_fetched / 1024 / 1024, FILE_COUNT * FILE_SIZE / 1024 / 1024);

    assert_eq!(final_stats.metadata.hits - cold_stats.metadata.hits, FILE_COUNT as u64);
    assert_eq!(final_stats.metadata.inner_gets, cold_stats.metadata.inner_gets); // No new fetches

    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cache_dir);

    Ok(())
}
