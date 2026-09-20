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

/// Raw stream chunks of a GET — must not concat; callers assert on chunk 0.
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

fn part(i: usize) -> Path {
    Path::from(format!("data/part-{i:04}.parquet"))
}

/// GETs every path in order (whole object, or only `range`) and returns the wall time.
async fn read_all(store: &FoyerObjectStoreCache, paths: impl IntoIterator<Item = Path>, range: Option<std::ops::Range<u64>>) -> Result<Duration> {
    let start = Instant::now();
    for p in paths {
        match &range {
            Some(r) => drop(store.get_range(&p, r.clone()).await?),
            None => drop(store.get(&p).await?),
        }
    }
    Ok(start.elapsed())
}

async fn finish(shared: &SharedFoyerCache) -> Result<()> {
    shared.log_stats();
    shared.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(30)).await
}

#[tokio::test]
async fn test_cache_performance_and_s3_bypass() -> Result<()> {
    let (shared_cache, cached) = cached_store("cache_perf", |c| {
        c.memory_size_bytes = 50 * 1024 * 1024;
        c.disk_size_bytes = 100 * 1024 * 1024;
        c.shards = 4;
    })
    .await?;

    let test_files = [
        ("table/2024/01/part-001.parquet", vec![0u8; 1024 * 512]),
        ("table/2024/01/part-002.parquet", vec![1u8; 1024 * 768]),
        ("table/2024/01/part-003.parquet", vec![2u8; 1024 * 256]),
    ];

    for (path_str, data) in &test_files {
        put(&cached, &Path::from(*path_str), data.clone()).await?;
    }

    let stats_after_write = shared_cache.get_stats();
    assert_eq!(stats_after_write.main.inner_puts, 3, "Should have written to inner store 3 times");
    assert_eq!(stats_after_write.main.inner_gets, 0, "Writes warm from payload — no inner GET during write");

    let paths = || test_files.iter().map(|(p, _)| Path::from(*p));
    let first_read_time = read_all(&cached, paths(), None).await?;
    let cached_read_time = read_all(&cached, paths(), None).await?;
    assert!(cached_read_time <= first_read_time * 2, "Cached reads should be consistently fast. First: {first_read_time:?}, Cached: {cached_read_time:?}");

    let stats = shared_cache.get_stats();
    assert_eq!(stats.main.hits, 6, "Should have 6 cache hits total (3 per read iteration)");
    assert_eq!(stats.main.misses, 0, "Should have no cache misses since files were cached on write");
    assert_eq!(stats.main.inner_gets, 0, "Writes warm from payload and reads hit cache — no inner GETs at all");
    assert_eq!(stats.main.inner_puts, 3, "Should have written to inner store 3 times");

    let update_path = Path::from("table/2024/01/part-001.parquet");
    put(&cached, &update_path, vec![9u8; 1024]).await?;
    assert_eq!(get_chunks(&cached, &update_path).await?[0][0], 9u8, "Should get updated data after invalidation");

    finish(&shared_cache).await
}

#[tokio::test]
async fn test_large_file_disk_caching() -> Result<()> {
    let (shared_cache, cached) = cached_store("disk_cache", |c| c.ttl = Duration::from_secs(60)).await?;

    let large_files = [("test/file1.parquet", vec![0u8; 512 * 1024]), ("test/file2.parquet", vec![1u8; 768 * 1024])];

    for (path_str, data) in &large_files {
        let path = Path::from(*path_str);
        put(&cached, &path, data.clone()).await?;
        let _ = cached.get(&path).await?;
    }

    // A cache hit serves the whole file as one chunk.
    for (path_str, data) in &large_files {
        let chunks = get_chunks(&cached, &Path::from(*path_str)).await?;
        assert_eq!(chunks[0].len(), data.len(), "Should retrieve full file from cache");
    }

    assert!(shared_cache.get_stats().main.hits > 0, "Should have cache hits");

    finish(&shared_cache).await
}

#[tokio::test]
async fn test_cache_with_database_integration() -> Result<()> {
    unsafe {
        env::set_var("TIMEFUSION_FOYER_MEMORY_MB", "10");
        env::set_var("TIMEFUSION_FOYER_DISK_GB", "1");
        env::set_var("TIMEFUSION_FOYER_TTL_SECONDS", "300");
        env::set_var("TIMEFUSION_FOYER_STATS", "true");
    }

    // Database::new() must wire every table through the shared Foyer cache built from these env vars.
    let db = Database::new().await?;

    db.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_parquet_metadata_cache_performance() -> Result<()> {
    const FILE_COUNT: usize = 10;
    const FILE_SIZE: usize = 50 * 1024 * 1024;
    const METADATA_SIZE: usize = 1024 * 1024;

    let inner = Arc::new(object_store::memory::InMemory::new());
    let config = FoyerCacheConfig {
        memory_size_bytes: 50 * 1024 * 1024,
        disk_size_bytes: 100 * 1024 * 1024,
        ttl: Duration::from_secs(300),
        cache_dir: std::path::PathBuf::from("/tmp/test_parquet_metadata_perf"),
        shards: 4,
        file_size_bytes: 4 * 1024 * 1024,
        metadata_memory_size_bytes: 20 * 1024 * 1024,
        metadata_disk_size_bytes: 50 * 1024 * 1024,
        metadata_shards: 2,
        ..Default::default()
    };
    let cache_dir = config.cache_dir.clone();
    let _ = std::fs::remove_dir_all(&cache_dir);

    let cache = Arc::new(FoyerObjectStoreCache::new(inner.clone(), config).await?);

    for i in 0..FILE_COUNT {
        inner.put(&part(i), PutPayload::from(Bytes::from(vec![b'x'; FILE_SIZE]))).await?;
    }

    let footer = (FILE_SIZE - METADATA_SIZE) as u64..FILE_SIZE as u64;
    let initial_stats = cache.get_stats();
    let cold_duration = read_all(&cache, (0..FILE_COUNT).map(part), Some(footer.clone())).await?;
    let cold_stats = cache.get_stats();
    let warm_duration = read_all(&cache, (0..FILE_COUNT).map(part), Some(footer)).await?;
    let final_stats = cache.get_stats();

    // Only each file's footer is fetched, never the whole object.
    let cold_inner_gets = cold_stats.metadata.inner_gets - initial_stats.metadata.inner_gets;
    let data_fetched = cold_inner_gets as usize * METADATA_SIZE;
    println!("cold {cold_duration:?} / warm {warm_duration:?}; fetched {}MB of {}MB", data_fetched / 1024 / 1024, FILE_COUNT * FILE_SIZE / 1024 / 1024);

    assert_eq!(final_stats.metadata.hits - cold_stats.metadata.hits, FILE_COUNT as u64);
    assert_eq!(final_stats.metadata.inner_gets, cold_stats.metadata.inner_gets);

    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all(&cache_dir);

    Ok(())
}
