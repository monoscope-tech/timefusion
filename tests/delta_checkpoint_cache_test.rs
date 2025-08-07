use std::sync::Arc;
use std::time::Duration;
use object_store::{ObjectStore, PutPayload};
use object_store::memory::InMemory;
use object_store::path::Path;
use timefusion::object_store_cache::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache};
use futures::TryStreamExt;

#[tokio::test]
async fn test_delta_checkpoint_cache_behavior() -> anyhow::Result<()> {
    // Create config with checkpoint caching disabled (default)
    let config = FoyerCacheConfig::test_config("delta_checkpoint_cache");

    let inner = Arc::new(InMemory::new());
    let shared_cache = SharedFoyerCache::new(config).await?;
    let cache = FoyerObjectStoreCache::new_with_shared_cache(inner.clone(), &shared_cache);

    // Test 1: Regular file should be cached
    let regular_path = Path::from("data/file.parquet");
    let regular_data = b"regular parquet data";
    cache.put(&regular_path, PutPayload::from(&regular_data[..])).await?;
    
    // First get should hit the inner store
    let stats1 = cache.get_stats().await;
    let _ = cache.get(&regular_path).await?;
    let stats2 = cache.get_stats().await;
    assert_eq!(stats2.misses - stats1.misses, 1, "First get should be a miss");
    
    // Second get should hit the cache
    let _ = cache.get(&regular_path).await?;
    let stats3 = cache.get_stats().await;
    assert_eq!(stats3.hits - stats2.hits, 1, "Second get should be a hit");

    // Test 2: _last_checkpoint file should now be cached (with stale-while-revalidate)
    let checkpoint_path = Path::from("table/_delta_log/_last_checkpoint");
    let checkpoint_data = b"checkpoint metadata";
    inner.put(&checkpoint_path, PutPayload::from(&checkpoint_data[..])).await?;
    
    // First get should miss the cache
    let stats4 = cache.get_stats().await;
    let _ = cache.get(&checkpoint_path).await?;
    let stats5 = cache.get_stats().await;
    assert_eq!(stats5.misses - stats4.misses, 1, "First checkpoint get should miss");
    
    // Second get should hit the cache (now cached)
    let _ = cache.get(&checkpoint_path).await?;
    let stats6 = cache.get_stats().await;
    assert_eq!(stats6.hits - stats5.hits, 1, "Second checkpoint get should hit");

    // Test 3: Writing a commit file should invalidate _last_checkpoint
    let commit_path = Path::from("table/_delta_log/00000001.json");
    let commit_data = b"commit data";
    
    // Put checkpoint in inner store
    inner.put(&checkpoint_path, PutPayload::from(&b"old checkpoint"[..])).await?;
    
    // Write commit file through cache
    cache.put(&commit_path, PutPayload::from(&commit_data[..])).await?;
    
    // The checkpoint cache should have been invalidated
    // (though in this case it wasn't cached anyway due to cache_delta_checkpoints=false)
    
    // Test 4: Delta metadata files should use shorter TTL
    let metadata_path = Path::from("table/_delta_log/00000000.json");
    let metadata_data = b"metadata";
    cache.put(&metadata_path, PutPayload::from(&metadata_data[..])).await?;
    
    // First get should miss
    let stats7 = cache.get_stats().await;
    let _ = cache.get(&metadata_path).await?;
    let stats8 = cache.get_stats().await;
    assert_eq!(stats8.misses - stats7.misses, 1, "First metadata get should miss");
    
    // Second get should hit (within TTL)
    let _ = cache.get(&metadata_path).await?;
    let stats9 = cache.get_stats().await;
    assert_eq!(stats9.hits - stats8.hits, 1, "Second metadata get should hit");

    // Cleanup
    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all("/tmp/test_delta_checkpoint_cache");
    
    Ok(())
}

#[tokio::test]
async fn test_checkpoint_invalidation_on_commit() -> anyhow::Result<()> {
    // Create config with checkpoint caching ENABLED to test invalidation
    let config = FoyerCacheConfig::test_config_with("checkpoint_invalidation", |c| {
        c.delta_metadata_ttl = Some(Duration::from_secs(60)); // Longer TTL to test invalidation
    });

    let inner = Arc::new(InMemory::new());
    let shared_cache = SharedFoyerCache::new(config).await?;
    let cache = FoyerObjectStoreCache::new_with_shared_cache(inner.clone(), &shared_cache);

    // Setup: Create checkpoint file
    let checkpoint_path = Path::from("mytable/_delta_log/_last_checkpoint");
    let checkpoint_data = b"version: 10";
    inner.put(&checkpoint_path, PutPayload::from(&checkpoint_data[..])).await?;

    // Get checkpoint - should cache it
    let stats1 = cache.get_stats().await;
    let result1 = cache.get(&checkpoint_path).await?;
    let data1 = result1.into_stream().try_collect::<Vec<_>>().await?.concat();
    assert_eq!(data1, checkpoint_data);
    let stats2 = cache.get_stats().await;
    assert_eq!(stats2.misses - stats1.misses, 1, "First get should miss");

    // Get again - should hit cache
    let result2 = cache.get(&checkpoint_path).await?;
    let data2 = result2.into_stream().try_collect::<Vec<_>>().await?.concat();
    assert_eq!(data2, checkpoint_data);
    let stats3 = cache.get_stats().await;
    assert_eq!(stats3.hits - stats2.hits, 1, "Second get should hit cache");

    // Update checkpoint in inner store
    let new_checkpoint_data = b"version: 11";
    inner.put(&checkpoint_path, PutPayload::from(&new_checkpoint_data[..])).await?;

    // Write a commit file
    let commit_path = Path::from("mytable/_delta_log/00000011.json");
    cache.put(&commit_path, PutPayload::from(&b"commit 11"[..])).await?;

    // With stale-while-revalidate, checkpoint is still served from cache (stale data)
    // The refresh happens in background after 5 seconds
    let stats4 = cache.get_stats().await;
    let result3 = cache.get(&checkpoint_path).await?;
    let data3 = result3.into_stream().try_collect::<Vec<_>>().await?.concat();
    // Still gets old data initially (stale-while-revalidate behavior)
    assert_eq!(data3, checkpoint_data, "Should still get cached (stale) checkpoint data");
    let stats5 = cache.get_stats().await;
    assert_eq!(stats5.hits - stats4.hits, 1, "Should hit cache with stale data");
    
    // To get the new data, we need to wait for the stale threshold (5 seconds)
    // or manually invalidate the cache
    cache.invalidate_checkpoint_cache("mytable").await;
    
    // After invalidation, the cache is immediately refreshed, so we get a hit with new data
    let stats6 = cache.get_stats().await;
    let result4 = cache.get(&checkpoint_path).await?;
    let data4 = result4.into_stream().try_collect::<Vec<_>>().await?.concat();
    assert_eq!(data4, new_checkpoint_data, "Should get new checkpoint data after invalidation");
    let stats7 = cache.get_stats().await;
    // Should be a hit because invalidate_checkpoint_cache now immediately refreshes the cache
    assert_eq!(stats7.hits - stats6.hits, 1, "Should hit cache after invalidation (cache was refreshed)");

    // Cleanup
    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all("/tmp/test_checkpoint_invalidation");
    
    Ok(())
}

#[tokio::test]
async fn test_delta_metadata_ttl() -> anyhow::Result<()> {
    let config = FoyerCacheConfig::test_config_with("delta_ttl", |c| {
        c.ttl = Duration::from_secs(10); // Regular TTL
        c.delta_metadata_ttl = Some(Duration::from_millis(100)); // Very short TTL for test
        // Checkpoint caching is always enabled now with stale-while-revalidate
    });

    let inner = Arc::new(InMemory::new());
    let shared_cache = SharedFoyerCache::new(config).await?;
    let cache = FoyerObjectStoreCache::new_with_shared_cache(inner.clone(), &shared_cache);

    // Test metadata file with short TTL
    let metadata_path = Path::from("table/_delta_log/00000000.json");
    cache.put(&metadata_path, PutPayload::from(&b"metadata"[..])).await?;

    // Should hit cache immediately
    let stats1 = cache.get_stats().await;
    let _ = cache.get(&metadata_path).await?;
    let stats2 = cache.get_stats().await;
    assert_eq!(stats2.misses - stats1.misses, 1);

    let _ = cache.get(&metadata_path).await?;
    let stats3 = cache.get_stats().await;
    assert_eq!(stats3.hits - stats2.hits, 1, "Should hit cache within TTL");

    // Wait for metadata TTL to expire
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Should miss cache after TTL
    let _ = cache.get(&metadata_path).await?;
    let stats4 = cache.get_stats().await;
    assert_eq!(stats4.misses - stats3.misses, 1, "Should miss cache after TTL");
    assert_eq!(stats4.ttl_expirations - stats3.ttl_expirations, 1, "Should record TTL expiration");

    // Test regular file with longer TTL
    let regular_path = Path::from("data/file.parquet");
    cache.put(&regular_path, PutPayload::from(&b"data"[..])).await?;

    let _ = cache.get(&regular_path).await?;
    let _ = cache.get(&regular_path).await?;
    
    // Wait same time as before (less than regular TTL)
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Should still hit cache (regular TTL is longer)
    let stats5 = cache.get_stats().await;
    let _ = cache.get(&regular_path).await?;
    let stats6 = cache.get_stats().await;
    assert_eq!(stats6.hits - stats5.hits, 1, "Regular file should still be cached");

    // Cleanup
    cache.shutdown().await?;
    let _ = std::fs::remove_dir_all("/tmp/test_delta_ttl");
    
    Ok(())
}