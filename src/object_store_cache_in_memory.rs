use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use foyer::{Cache, CacheBuilder};
use object_store::{ObjectStore, path::Path};
use tokio::sync::RwLock;

/// A hybrid cache implementation for object store using foyer
pub struct ObjectStoreCache {
    cache:        Arc<RwLock<Cache<Path, Bytes>>>,
    object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCache {
    /// Create a new ObjectStoreCache instance (in-memory cache)
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        capacity: usize,
    ) -> Result<Self> {
        let cache = CacheBuilder::new(capacity).build();
        Ok(Self {
            cache: Arc::new(RwLock::new(cache)),
            object_store,
        })
    }

    /// Get an object from the cache or object store
    pub async fn get(&self, path: &Path) -> Result<Bytes> {
        // Try to get from cache first
        if let Some(entry) = self.cache.read().await.get(path) {
            return Ok(entry.value().clone());
        }

        // If not in cache, get from object store
        let bytes = self.object_store.get(path).await?.bytes().await?;

        // Store in cache
        self.cache.write().await.insert(path.clone(), bytes.clone());

        Ok(bytes)
    }

    /// Put an object into both cache and object store
    pub async fn put(&self, path: &Path, bytes: Bytes) -> Result<()> {
        // Store in object store
        self.object_store.put(path, bytes.clone().into()).await?;

        // Store in cache
        self.cache.write().await.insert(path.clone(), bytes);

        Ok(())
    }

    /// Remove an object from both cache and object store
    pub async fn remove(&self, path: &Path) -> Result<()> {
        // Remove from object store
        self.object_store.delete(path).await?;

        // Remove from cache
        self.cache.write().await.remove(path);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn test_object_store_cache() -> Result<()> {
        let object_store = Arc::new(InMemory::new());
        let cache = ObjectStoreCache::new(
            object_store,
            1024 * 1024, // 1MB cache
        )?;

        let path = Path::from("test.txt");
        let data = Bytes::from("test data");

        // Test put and get
        cache.put(&path, data.clone()).await?;
        let retrieved = cache.get(&path).await?;
        assert_eq!(retrieved, data);

        // Test remove
        cache.remove(&path).await?;
        assert!(cache.get(&path).await.is_err());

        Ok(())
    }
}
