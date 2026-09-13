use std::{path::PathBuf, sync::Arc, time::Duration};

use futures::TryStreamExt;
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};
use serial_test::serial;
use test_case::test_case;
use timefusion::storage::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache};

/// Removes the cache dir on drop. The path is unique per (name, process), so every case needs its own name.
struct CacheDirGuard(PathBuf);

impl Drop for CacheDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A cache over a fresh `InMemory` store; the returned shared handle and guard must be kept alive for the whole test.
async fn env(name: &str, tweak: impl FnOnce(&mut FoyerCacheConfig)) -> anyhow::Result<(Arc<InMemory>, FoyerObjectStoreCache, SharedFoyerCache, CacheDirGuard)> {
    let config = FoyerCacheConfig::test_config_with(name, tweak);
    let guard = CacheDirGuard(config.cache_dir.clone());
    let inner = Arc::new(InMemory::new());
    let shared = SharedFoyerCache::new(config).await?;
    let cache = FoyerObjectStoreCache::new_with_shared_cache(inner.clone(), &shared);
    Ok((inner, cache, shared, guard))
}

/// Counter deltas produced by exactly one `get`.
#[derive(Debug, PartialEq, Eq)]
struct Delta {
    hits: u64,
    misses: u64,
    ttl_expirations: u64,
}

/// Reads `path` through the cache, returning its bytes and the counter deltas that read caused.
async fn get_counted(cache: &FoyerObjectStoreCache, path: &Path) -> anyhow::Result<(Vec<u8>, Delta)> {
    let before = cache.get_stats().await.main;
    let bytes = cache.get(path).await?.into_stream().try_collect::<Vec<_>>().await?.concat();
    let after = cache.get_stats().await.main;
    let delta = Delta { hits: after.hits - before.hits, misses: after.misses - before.misses, ttl_expirations: after.ttl_expirations - before.ttl_expirations };
    Ok((bytes, delta))
}

/// A put through the cache also caches, so the first get hits; an inner-store-only object misses once, then hits.
#[test_case("data/file.parquet", "regular parquet data", true ; "regular parquet cached by put")]
#[test_case("table/_delta_log/00000000.json", "metadata", true ; "delta metadata cached by put")]
#[test_case("table/_delta_log/00000001.json", "commit data", true ; "delta commit cached by put")]
#[test_case("table/_delta_log/_last_checkpoint", "checkpoint metadata", false ; "checkpoint only in inner store")]
#[tokio::test]
#[serial]
async fn test_delta_checkpoint_cache_behavior(path: &'static str, body: &'static str, written_through_cache: bool) -> anyhow::Result<()> {
    let (inner, cache, _shared, _guard) = env(&format!("behavior_{}", path.replace(['/', '.'], "_")), |_| {}).await?;
    let p = Path::from(path);
    let payload = || PutPayload::from(body.as_bytes());
    if written_through_cache {
        cache.put(&p, payload()).await?;
    } else {
        inner.put(&p, payload()).await?;
    }

    let (bytes, first) = get_counted(&cache, &p).await?;
    assert_eq!(bytes, body.as_bytes(), "get returns the stored bytes");
    assert_eq!(
        (first.hits, first.misses),
        if written_through_cache { (1, 0) } else { (0, 1) },
        "first get: put-through data is already cached, inner-only data misses"
    );

    let (bytes, second) = get_counted(&cache, &p).await?;
    assert_eq!(bytes, body.as_bytes(), "second get returns the stored bytes");
    assert_eq!((second.hits, second.misses), (1, 0), "second get hits the cache");

    cache.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_checkpoint_invalidation_on_commit() -> anyhow::Result<()> {
    // Longer TTL so expiry cannot be mistaken for invalidation.
    let (inner, cache, _shared, _guard) = env("checkpoint_invalidation", |c| c.ttl = Duration::from_secs(60)).await?;

    let table = "test_invalidation_table";
    let checkpoint_path = Path::from(format!("{table}/_delta_log/_last_checkpoint"));
    let (old, new) = ("version: 10", "version: 11");
    inner.put(&checkpoint_path, PutPayload::from(old.as_bytes())).await?;

    let (data, d) = get_counted(&cache, &checkpoint_path).await?;
    assert_eq!(data, old.as_bytes(), "first get loads from the inner store");
    assert_eq!(d.misses, 1, "First get should miss");

    let (data, d) = get_counted(&cache, &checkpoint_path).await?;
    assert_eq!(data, old.as_bytes(), "second get returns the same bytes");
    assert_eq!(d.hits, 1, "Second get should hit cache");

    inner.put(&checkpoint_path, PutPayload::from(new.as_bytes())).await?;
    cache.put(&Path::from(format!("{table}/_delta_log/00000011.json")), PutPayload::from(&b"commit 11"[..])).await?;

    // Stale-while-revalidate: the checkpoint is still served stale until the background refresh.
    let (data, d) = get_counted(&cache, &checkpoint_path).await?;
    assert_eq!(data, old.as_bytes(), "Should still get cached (stale) checkpoint data");
    assert_eq!(d.hits, 1, "Should hit cache with stale data");

    cache.invalidate_checkpoint_cache(table).await;

    let (data, d) = get_counted(&cache, &checkpoint_path).await?;
    assert_eq!(data, new.as_bytes(), "Should get new checkpoint data after invalidation");
    // A hit because invalidate_checkpoint_cache immediately refreshes the cache.
    assert_eq!(d.hits, 1, "Should hit cache after invalidation (cache was refreshed)");

    cache.shutdown().await?;
    Ok(())
}

/// The same TTL applies to every path kind: Delta log metadata is not privileged over regular data.
#[test_case("table/_delta_log/00000000.json", "metadata" ; "delta metadata expires")]
#[test_case("data/file.parquet", "data" ; "regular file expires on the same TTL")]
#[tokio::test]
#[serial]
async fn test_delta_metadata_ttl(path: &'static str, body: &'static str) -> anyhow::Result<()> {
    // 2s TTL, not ~100ms: a loaded runner can stall the put→get window past a tight TTL and turn the expected hit into a miss.
    let (_inner, cache, _shared, _guard) = env(&format!("ttl_{}", path.replace(['/', '.'], "_")), |c| c.ttl = Duration::from_millis(2000)).await?;

    let p = Path::from(path);
    cache.put(&p, PutPayload::from(body.as_bytes())).await?;

    let (_, first) = get_counted(&cache, &p).await?;
    assert_eq!(first.hits, 1, "First get should hit (cached by put)");
    let (_, second) = get_counted(&cache, &p).await?;
    assert_eq!(second.hits, 1, "Should hit cache within TTL");

    tokio::time::sleep(Duration::from_millis(2500)).await;

    let (bytes, expired) = get_counted(&cache, &p).await?;
    assert_eq!(bytes, body.as_bytes(), "post-expiry get still returns the object from the inner store");
    assert_eq!(expired.misses, 1, "Should miss cache after TTL");
    assert_eq!(expired.ttl_expirations, 1, "Should record TTL expiration");

    cache.shutdown().await?;
    Ok(())
}
