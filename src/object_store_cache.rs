use std::{
    ops::Range,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashSet;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder, HybridCachePolicy, HybridCacheProperties, Location, PsyncIoEngineConfig,
};
use futures::stream::BoxStream;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult, path::Path,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
};
use tracing::{Instrument, debug, field::Empty, info, instrument};

/// Cache entry with metadata and TTL
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    #[serde(with = "serde_bytes")]
    data:             Vec<u8>,
    #[serde(with = "object_meta_serde")]
    meta:             ObjectMeta,
    timestamp_millis: u64,
}

impl CacheValue {
    fn new(data: Vec<u8>, meta: ObjectMeta) -> Self {
        Self {
            data,
            meta,
            timestamp_millis: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        let age_millis = current_millis().saturating_sub(self.timestamp_millis);
        age_millis > ttl.as_millis() as u64
    }
}

fn current_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

mod object_meta_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct SerializedMeta {
        location:      String,
        last_modified: i64,
        size:          u64,
        e_tag:         Option<String>,
        version:       Option<String>,
    }

    pub fn serialize<S>(meta: &ObjectMeta, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializedMeta {
            location:      meta.location.to_string(),
            last_modified: meta.last_modified.timestamp_millis(),
            size:          meta.size,
            e_tag:         meta.e_tag.clone(),
            version:       meta.version.clone(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ObjectMeta, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = SerializedMeta::deserialize(deserializer)?;
        Ok(ObjectMeta {
            location:      Path::from(s.location),
            last_modified: DateTime::<Utc>::from_timestamp_millis(s.last_modified).unwrap_or(Utc::now()),
            size:          s.size,
            e_tag:         s.e_tag,
            version:       s.version,
        })
    }
}

/// Configuration for the foyer-based object store cache
#[derive(Debug, Clone)]
pub struct FoyerCacheConfig {
    pub memory_size_bytes:          usize,
    pub disk_size_bytes:            usize,
    pub ttl:                        Duration,
    pub cache_dir:                  PathBuf,
    pub shards:                     usize,
    pub file_size_bytes:            usize,
    pub enable_stats:               bool,
    /// Size hint for reading parquet metadata from the end of files
    pub parquet_metadata_size_hint: usize,
    /// Memory size for metadata cache in bytes
    pub metadata_memory_size_bytes: usize,
    /// Disk size for metadata cache in bytes
    pub metadata_disk_size_bytes:   usize,
    /// Number of shards for metadata cache
    pub metadata_shards:            usize,
    /// Optional extra cap on bytes buffered to warm the cache inline from a
    /// multipart write (see `CachingMultipartUpload`). Always bounded by
    /// `block_size_bytes`; 0 = bound only by the block size.
    pub warm_inline_max_bytes:      usize,
    /// Disk block size for the main data cache — foyer's eviction unit and the
    /// hard cap on the largest entry that can persist to disk. Must be >= the
    /// largest file we want cached (compaction target size).
    pub block_size_bytes:           usize,
    /// Entries larger than this are inserted disk-only (`Location::OnDisk`) so
    /// they don't evict the hot L1 working set. 0 = always use L1.
    pub l1_max_entry_bytes:         usize,
    /// Don't admit writes whose `date=` partition is older than this many days.
    /// 0 = no age limit.
    pub cache_recent_days:          usize,
}

impl Default for FoyerCacheConfig {
    fn default() -> Self {
        Self {
            memory_size_bytes:          134_217_728,                 // 128MB
            disk_size_bytes:            107_374_182_400,             // 100GB
            ttl:                        Duration::from_secs(86_400), // 24h
            cache_dir:                  PathBuf::from("/tmp/timefusion_cache"),
            shards:                     8,
            file_size_bytes:            16_777_216, // 16MB - good for Parquet files
            enable_stats:               true,
            parquet_metadata_size_hint: 1_048_576,   // 1MB - typical size for parquet metadata
            metadata_memory_size_bytes: 67_108_864,  // 64MB
            metadata_disk_size_bytes:   536_870_912, // 512MB
            metadata_shards:            4,           // Fewer shards for metadata cache
            warm_inline_max_bytes:      0,           // bound by block size
            block_size_bytes:           268_435_456, // 256MB — fits 128MB compaction outputs
            l1_max_entry_bytes:         16_777_216,  // 16MB
            cache_recent_days:          8,
        }
    }
}

impl FoyerCacheConfig {
    pub fn from_app_config(cfg: &crate::config::AppConfig) -> Self {
        // The disk block size caps the largest file that can be cached locally,
        // and compaction writes files at ~the optimize target size. Floor the
        // block at 2x that target so the two stay in lockstep automatically —
        // an operator can raise timefusion_optimize_target_size without
        // silently losing the ability to cache the bigger outputs. The
        // configured block size acts as a lower bound / explicit override.
        let optimize_target = cfg.parquet.timefusion_optimize_target_size.max(0) as usize;
        let block_size_bytes = cfg.cache.block_size_bytes().max(optimize_target.saturating_mul(2));
        let disk_size_bytes = cfg.cache.disk_size_bytes();
        if block_size_bytes > disk_size_bytes {
            tracing::warn!(
                "Foyer disk block size ({}MB) exceeds disk capacity ({}MB) — large files won't persist to disk. Raise timefusion_foyer_disk_gb or lower the optimize target.",
                block_size_bytes / 1024 / 1024,
                disk_size_bytes / 1024 / 1024
            );
        }
        Self {
            memory_size_bytes: cfg.cache.memory_size_bytes(),
            disk_size_bytes,
            ttl: cfg.cache.ttl(),
            cache_dir: cfg.core.cache_dir(),
            shards: cfg.cache.timefusion_foyer_shards,
            file_size_bytes: cfg.cache.file_size_bytes(),
            enable_stats: cfg.cache.stats_enabled(),
            parquet_metadata_size_hint: cfg.cache.timefusion_parquet_metadata_size_hint,
            metadata_memory_size_bytes: cfg.cache.metadata_memory_size_bytes(),
            metadata_disk_size_bytes: cfg.cache.metadata_disk_size_bytes(),
            metadata_shards: cfg.cache.timefusion_foyer_metadata_shards,
            warm_inline_max_bytes: cfg.cache.warm_inline_max_bytes(),
            block_size_bytes,
            l1_max_entry_bytes: cfg.cache.l1_max_entry_bytes(),
            cache_recent_days: cfg.cache.timefusion_cache_recent_days,
        }
    }

    /// Create a test configuration with sensible defaults for testing
    /// The name parameter is used to create unique cache directories
    pub fn test_config(name: &str) -> Self {
        Self {
            memory_size_bytes:          10 * 1024 * 1024, // 10MB
            disk_size_bytes:            50 * 1024 * 1024, // 50MB
            ttl:                        Duration::from_secs(300),
            cache_dir:                  PathBuf::from(format!("/tmp/test_foyer_{}", name)),
            shards:                     2,
            file_size_bytes:            1024 * 1024, // 1MB
            enable_stats:               true,
            parquet_metadata_size_hint: 1_048_576,        // 1MB
            metadata_memory_size_bytes: 10 * 1024 * 1024, // 10MB for tests
            metadata_disk_size_bytes:   50 * 1024 * 1024, // 50MB for tests
            metadata_shards:            2,
            warm_inline_max_bytes:      0,               // bound by block size
            block_size_bytes:           4 * 1024 * 1024, // 4MB — must be <= test disk size
            l1_max_entry_bytes:         1024 * 1024,     // 1MB
            cache_recent_days:          0,               // no age limit in tests (avoid date flakiness)
        }
    }

    /// Create a test config with specific overrides
    pub fn test_config_with(name: &str, f: impl FnOnce(&mut Self)) -> Self {
        let mut config = Self::test_config(name);
        f(&mut config);
        config
    }
}

/// Statistics for cache operations
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits:            u64,
    pub misses:          u64,
    pub ttl_expirations: u64,
    pub inner_gets:      u64,
    pub inner_puts:      u64,
}

/// Combined statistics for both caches
#[derive(Debug, Default, Clone)]
pub struct CombinedCacheStats {
    pub main:     CacheStats,
    pub metadata: CacheStats,
}

impl CacheStats {
    fn log(&self) {
        let hit_rate = if self.hits + self.misses > 0 {
            (self.hits as f64 / (self.hits + self.misses) as f64) * 100.0
        } else {
            0.0
        };
        info!(
            "Foyer cache stats - Hit rate: {:.2}%, Hits: {}, Misses: {}, TTL expirations: {}, Inner gets: {}, Inner puts: {}",
            hit_rate, self.hits, self.misses, self.ttl_expirations, self.inner_gets, self.inner_puts
        );
    }
}

type FoyerCache = Arc<HybridCache<String, CacheValue>>;
type StatsRef = Arc<RwLock<CacheStats>>;

/// Floor for the foyer disk block (region) size. Matches the legacy default
/// (`timefusion_foyer_file_size_mb`), small enough that even a modest disk
/// budget yields several regions.
const MIN_DISK_BLOCK_BYTES: usize = 4 * 1024 * 1024;

/// Cap a desired foyer disk block (region) size to the device. Foyer carves the
/// device into block-sized regions, so a block >= the device leaves zero usable
/// regions and every disk insert stalls (a 256MB block on a 50MB device wedged
/// CI). Keep several regions by capping at a quarter of the device, floored at
/// the legacy 4MB granularity and never above the device itself. Shared by both
/// cache builders so neither can silently wedge on a small disk.
fn capped_block_size(desired: usize, disk_size: usize) -> usize {
    desired.min(disk_size / 4).max(MIN_DISK_BLOCK_BYTES).min(disk_size)
}

/// Shared Foyer cache that can be used across multiple object stores
#[derive(Debug)]
pub struct SharedFoyerCache {
    cache:          FoyerCache,
    metadata_cache: FoyerCache,
    stats:          StatsRef,
    metadata_stats: StatsRef,
    config:         FoyerCacheConfig,
}

impl SharedFoyerCache {
    /// Create a new shared Foyer cache
    pub async fn new(config: FoyerCacheConfig) -> anyhow::Result<Self> {
        info!(
            "Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, block: {}MB, ttl: {}s, parquet_metadata_hint: {}KB)",
            config.memory_size_bytes / 1024 / 1024,
            config.disk_size_bytes / 1024 / 1024 / 1024,
            config.block_size_bytes / 1024 / 1024,
            config.ttl.as_secs(),
            config.parquet_metadata_size_hint / 1024
        );

        info!(
            "Initializing metadata cache (memory: {}MB, disk: {}GB, ttl: {}s)",
            config.metadata_memory_size_bytes / 1024 / 1024,
            config.metadata_disk_size_bytes / 1024 / 1024 / 1024,
            config.ttl.as_secs()
        );

        std::fs::create_dir_all(&config.cache_dir)?;
        let metadata_cache_dir = config.cache_dir.join("metadata");
        std::fs::create_dir_all(&metadata_cache_dir)?;

        // The main data cache wants a block big enough to hold full compaction
        // outputs (128MB) so they persist, capped to the device so it can't wedge.
        let data_block_size = capped_block_size(config.block_size_bytes, config.disk_size_bytes);

        let cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.memory_size_bytes)
            .with_shards(config.shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(FsDeviceBuilder::new(&config.cache_dir).with_capacity(config.disk_size_bytes).build()?).with_block_size(data_block_size),
            )
            .build()
            .await?;

        let metadata_cache = HybridCacheBuilder::new()
            .with_policy(HybridCachePolicy::WriteOnInsertion)
            .memory(config.metadata_memory_size_bytes)
            .with_shards(config.metadata_shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(FsDeviceBuilder::new(&metadata_cache_dir).with_capacity(config.metadata_disk_size_bytes).build()?)
                    .with_block_size(capped_block_size(config.file_size_bytes, config.metadata_disk_size_bytes)),
            )
            .build()
            .await?;

        Ok(Self {
            cache: Arc::new(cache),
            metadata_cache: Arc::new(metadata_cache),
            stats: Arc::new(RwLock::new(CacheStats::default())),
            metadata_stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
        })
    }

    pub async fn get_stats(&self) -> CombinedCacheStats {
        CombinedCacheStats {
            main:     self.stats.read().await.clone(),
            metadata: self.metadata_stats.read().await.clone(),
        }
    }

    pub async fn log_stats(&self) {
        info!("Main cache stats:");
        self.stats.read().await.log();
        info!("Metadata cache stats:");
        self.metadata_stats.read().await.log();
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down Foyer cache...");
        self.log_stats().await;

        // Close the underlying caches
        info!("Closing Foyer caches...");
        self.cache.close().await?;
        self.metadata_cache.close().await?;

        Ok(())
    }

    /// Invalidate checkpoint cache for a given table URI
    pub fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        let table_path = table_path_from_uri(table_uri);
        let last_checkpoint_key = format!("{}/_delta_log/_last_checkpoint", table_path);
        info!("Invalidating _last_checkpoint cache for table: {}", table_path);
        self.cache.remove(&last_checkpoint_key);
    }

    /// Best-effort eviction of a main (full-file) cache entry by its key — the
    /// relativized object path, matching `make_cache_key`. Used to proactively
    /// drop the (now dead) full-file bytes of a file a compaction tombstoned,
    /// instead of waiting for VACUUM / TTL / LRU to reclaim them.
    pub fn evict_data_entry(&self, key: &str) {
        self.cache.remove(key);
    }
}

/// Strip the `scheme://` prefix and trailing slashes from a table URI, yielding
/// the bare table path used to build `_delta_log` cache keys.
fn table_path_from_uri(table_uri: &str) -> &str {
    let table_path = table_uri.find("://").map(|idx| &table_uri[idx + 3..]).unwrap_or(table_uri);
    table_path.trim_end_matches('/')
}

/// Whether a cached object is a Parquet data file (vs. Delta log / checkpoint
/// metadata), which governs TTL and metadata-cache behavior.
fn is_parquet_file(location: &Path) -> bool {
    location.as_ref().ends_with(".parquet")
}

/// Best-effort: warm the Parquet footer of `location` into the cache by issuing
/// a ranged GET of the last `metadata_size_hint` bytes through `store`. When
/// `store` is a [`FoyerObjectStoreCache`], that ranged GET lands in the
/// metadata cache, so subsequent query planning (footer parse, row-group
/// stats, schema, pruning) pays zero S3 round-trips. The single `head` resolves
/// the file size needed to address the tail.
///
/// Strictly best-effort: every error is swallowed and reported via the return
/// value. Warming must never affect correctness or a caller's commit. Returns
/// `true` if the footer range was fetched.
pub async fn warm_footer(store: &dyn ObjectStore, location: &Path, metadata_size_hint: u64) -> bool {
    // Single suffix GET: the response carries the resolved absolute range + total
    // size, so a `FoyerObjectStoreCache` caches the footer under the same
    // absolute key a later bounded footer read requests — one round-trip, no
    // separate HEAD. Falls back to HEAD + bounded GET for stores that don't
    // support suffix ranges.
    let opts = GetOptions {
        range: Some(GetRange::Suffix(metadata_size_hint.max(1))),
        ..Default::default()
    };
    match store.get_opts(location, opts).await {
        Ok(result) => result.bytes().await.is_ok(),
        Err(_) => warm_footer_via_head(store, location, metadata_size_hint).await,
    }
}

/// HEAD + bounded-GET fallback for [`warm_footer`] when the store doesn't
/// support suffix ranges. Two round-trips, but always correct.
async fn warm_footer_via_head(store: &dyn ObjectStore, location: &Path, metadata_size_hint: u64) -> bool {
    let size = match store.head(location).await {
        Ok(meta) => meta.size,
        Err(_) => return false,
    };
    if size == 0 {
        return false;
    }
    let start = size.saturating_sub(metadata_size_hint.max(1));
    let opts = GetOptions {
        range: Some(GetRange::Bounded(start..size)),
        ..Default::default()
    };
    store.get_opts(location, opts).await.is_ok()
}

/// Best-effort: warm the full contents of `location` into the cache via a plain
/// GET through `store`. For a [`FoyerObjectStoreCache`] this populates the main
/// (full-file) cache so ranged data reads — DataFusion row-group scans — hit
/// Foyer instead of S3. Errors are swallowed; see [`warm_footer`].
pub async fn warm_full(store: &dyn ObjectStore, location: &Path) -> bool {
    // Explicitly drain the body: `GetResult`'s payload is a stream, and a
    // generic store may not have read it yet by the time `get_opts` returns.
    // (`FoyerObjectStoreCache` populates the cache eagerly inside `get_opts`,
    // but consuming the bytes keeps this correct for any inner store and is
    // a no-op cost there.)
    match store.get_opts(location, GetOptions::default()).await {
        Ok(result) => result.bytes().await.is_ok(),
        Err(_) => false,
    }
}

/// Parse the `date=YYYY-MM-DD` partition segment from `s` and return whether it
/// is on or after `cutoff`. Strings without a parseable date segment (Delta log,
/// checkpoints) are always within the window; a `None` cutoff means no age limit.
///
/// Shared by the cache-admission window here and the compaction-warm recency
/// filter in `database.rs` so the two parsers can't drift.
pub fn date_partition_within(s: &str, cutoff: Option<chrono::NaiveDate>) -> bool {
    let Some(cutoff) = cutoff else { return true };
    match s.find("date=").and_then(|i| s.get(i + 5..i + 15)).and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok()) {
        Some(date) => date >= cutoff,
        None => true,
    }
}

/// Whether `location` should be admitted to the cache given the recent-days
/// window. Paths without a `date=YYYY-MM-DD` segment (Delta log, checkpoints)
/// are always admitted. 0 days = no age limit.
///
/// Keeps cold-tier rewrites (recompress of week+-old partitions) out of the
/// cache so recent data stays local and old data is served from S3.
fn is_within_recent_window(location: &Path, recent_days: usize) -> bool {
    if recent_days == 0 {
        return true;
    }
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(recent_days as i64);
    date_partition_within(location.as_ref(), Some(cutoff))
}

/// Insert into the main full-file cache, steering large entries to disk-only
/// (`Location::OnDisk`, which makes them phantom in L1) so warming a 128MB
/// compaction output doesn't evict the hot small-entry working set from memory.
/// Small entries keep the default L1+disk placement for fastest repeat reads.
fn insert_main(cache: &FoyerCache, key: String, value: CacheValue, l1_max_entry_bytes: usize) {
    if l1_max_entry_bytes > 0 && value.data.len() > l1_max_entry_bytes {
        cache.insert_with_properties(key, value, HybridCacheProperties::default().with_location(Location::OnDisk));
    } else {
        cache.insert(key, value);
    }
}

/// Synthesize an `ObjectMeta` for a just-written object from its `PutResult`
/// (e_tag/version) and known size — lets the write path warm the cache without
/// a post-write GET just to learn the metadata.
fn put_result_meta(location: Path, size: u64, result: &PutResult) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: Utc::now(),
        size,
        e_tag: result.e_tag.clone(),
        version: result.version.clone(),
    }
}

/// Foyer-based hybrid cache implementation for object store
#[derive(derive_more::Display, derive_more::Debug)]
#[display("FoyerHybridCachedObjectStore({})", inner)]
#[debug("FoyerHybridCachedObjectStore {{ inner: {} }}", inner)]
pub struct FoyerObjectStoreCache {
    inner:            Arc<dyn ObjectStore>,
    cache:            FoyerCache,
    metadata_cache:   FoyerCache,
    stats:            StatsRef,
    metadata_stats:   StatsRef,
    config:           FoyerCacheConfig,
    refreshing:       Arc<DashSet<String>>,
    background_tasks: Arc<Mutex<JoinSet<()>>>,
}

impl FoyerObjectStoreCache {
    pub fn new_with_shared_cache(inner: Arc<dyn ObjectStore>, shared_cache: &SharedFoyerCache) -> Self {
        Self {
            inner,
            cache: shared_cache.cache.clone(),
            metadata_cache: shared_cache.metadata_cache.clone(),
            stats: shared_cache.stats.clone(),
            metadata_stats: shared_cache.metadata_stats.clone(),
            config: shared_cache.config.clone(),
            refreshing: Arc::new(DashSet::new()),
            background_tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    /// Check if a path is the mutable _last_checkpoint file
    fn is_last_checkpoint(location: &Path) -> bool {
        location.as_ref().contains("_delta_log/_last_checkpoint")
    }

    /// Get the appropriate TTL for a file based on its type
    fn get_ttl_for_path(&self, _location: &Path) -> Duration {
        self.config.ttl
    }

    /// Explicitly invalidate checkpoint cache for a given table
    pub async fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        let table_path = table_path_from_uri(table_uri);
        let last_checkpoint_path = format!("{}/_delta_log/_last_checkpoint", table_path);
        let cache_key = last_checkpoint_path.clone();
        info!("Explicitly invalidating and refreshing _last_checkpoint cache for table: {}", table_path);

        // Remove from cache first
        self.cache.remove(&cache_key);

        // Immediately fetch and cache the new version
        let location = Path::from(last_checkpoint_path);
        if let Ok(get_result) = self.inner.get(&location).await {
            let (data, meta) = Self::collect_payload(get_result).await;
            if !data.is_empty() {
                self.cache.insert(cache_key, CacheValue::new(data, meta));
                debug!("Proactively refreshed _last_checkpoint cache after invalidation");
            }
        }
    }

    pub async fn new(inner: Arc<dyn ObjectStore>, config: FoyerCacheConfig) -> anyhow::Result<Self> {
        let shared_cache = SharedFoyerCache::new(config).await?;
        Ok(Self::new_with_shared_cache(inner, &shared_cache))
    }

    async fn update_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut CacheStats),
    {
        f(&mut *self.stats.write().await);
    }

    async fn update_metadata_stats<F>(&self, f: F)
    where
        F: FnOnce(&mut CacheStats),
    {
        f(&mut *self.metadata_stats.write().await);
    }

    fn make_cache_key(location: &Path) -> String {
        location.to_string()
    }

    fn make_range_cache_key(location: &Path, range: &Range<u64>) -> String {
        format!("{}#range:{}-{}", location, range.start, range.end)
    }

    /// Key for a path's `ObjectMeta` in the metadata cache, kept distinct from
    /// range keys (`#range:`). Delta data files are immutable, so a path's
    /// size/etag is stable for the cache TTL — caching it lets footer reads skip
    /// the per-read HEAD.
    fn make_meta_cache_key(location: &Path) -> String {
        format!("{}#meta", location)
    }

    /// Invalidate all metadata cache entries for a given file
    async fn invalidate_metadata_cache(&self, location: &Path) {
        // We can't enumerate all possible range keys, but we can at least
        // invalidate the most common metadata ranges
        let file_meta = match self.inner.head(location).await {
            Ok(meta) => meta,
            Err(_) => return,
        };

        let file_size = file_meta.size;
        let metadata_size_hint = self.config.parquet_metadata_size_hint as u64;

        // Invalidate common metadata ranges
        for offset in [8, 1024, 4096, 8192, metadata_size_hint] {
            if offset < file_size {
                let start = file_size.saturating_sub(offset);
                let range = start..file_size;
                let cache_key = Self::make_range_cache_key(location, &range);
                self.metadata_cache.remove(&cache_key);
            }
        }

        debug!("Invalidated metadata cache entries for: {}", location);
    }

    fn make_get_result(data: Bytes, meta: ObjectMeta) -> GetResult {
        let data_len = data.len() as u64;
        GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(data) }))),
            meta,
            attributes: Attributes::new(),
            range: 0..data_len,
        }
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("Shutting down foyer hybrid cache");

        // Cancel all background refresh tasks
        let mut tasks = self.background_tasks.lock().await;
        debug!("Cancelling {} background refresh tasks", tasks.len());
        tasks.abort_all();
        // Wait for all tasks to complete or be cancelled
        while tasks.join_next().await.is_some() {}

        // Clear the refreshing set
        self.refreshing.clear();

        // Note: We don't close the caches here because they're shared
        // and owned by SharedFoyerCache
        Ok(())
    }

    pub async fn get_stats(&self) -> CombinedCacheStats {
        CombinedCacheStats {
            main:     self.stats.read().await.clone(),
            metadata: self.metadata_stats.read().await.clone(),
        }
    }

    pub async fn reset_stats(&self) {
        *self.stats.write().await = CacheStats::default();
        *self.metadata_stats.write().await = CacheStats::default();
    }
}

impl FoyerObjectStoreCache {
    /// Collect a GetResult payload into a Vec<u8>
    async fn collect_payload(result: GetResult) -> (Vec<u8>, ObjectMeta) {
        use futures::TryStreamExt;
        let meta = result.meta.clone();
        let data = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect::<Vec<Bytes>>().await.map(|c| c.concat()).unwrap_or_default(),
            GetResultPayload::File(mut file, _) => {
                use std::io::Read;
                let mut buf = Vec::new();
                if file.read_to_end(&mut buf).is_ok() { buf } else { vec![] }
            }
        };
        (data, meta)
    }

    #[instrument(
        name = "foyer_cache.get",
        skip_all,
        fields(
            location = %location,
            cache_hit = Empty,
            is_checkpoint = Self::is_last_checkpoint(location),
        )
    )]
    async fn get_cached(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let span = tracing::Span::current();
        let cache_key = Self::make_cache_key(location);

        // Try cache first
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();

            // Use appropriate TTL based on file type
            let ttl = self.get_ttl_for_path(location);

            // Special handling for _last_checkpoint: stale-while-revalidate
            if Self::is_last_checkpoint(location) && !value.is_expired(ttl) {
                self.update_stats(|s| s.hits += 1).await;
                span.record("cache_hit", true);

                // Check if older than 5 seconds
                let age_millis = current_millis().saturating_sub(value.timestamp_millis);
                if age_millis > 5000 {
                    // Trigger background refresh if not already refreshing
                    if self.refreshing.insert(cache_key.clone()) {
                        let inner = self.inner.clone();
                        let cache = self.cache.clone();
                        let refreshing = self.refreshing.clone();
                        let location = location.clone();
                        let key = cache_key.clone();

                        let tasks = self.background_tasks.clone();
                        let handle = tokio::spawn(async move {
                            debug!("Background refresh for _last_checkpoint: {}", location);
                            if let Ok(result) = inner.get(&location).await {
                                let (data, meta) = FoyerObjectStoreCache::collect_payload(result).await;
                                if !data.is_empty() {
                                    cache.insert(key.clone(), CacheValue::new(data, meta));
                                }
                            }
                            refreshing.remove(&key);
                        });

                        // Track the background task
                        if let Ok(mut tasks_guard) = tasks.try_lock() {
                            tasks_guard.spawn(async move {
                                let _ = handle.await;
                            });
                        }
                    }

                    debug!("Foyer cache HIT (stale-while-revalidate) for: {} (age: {}ms)", location, age_millis);
                } else {
                    debug!("Foyer cache HIT (fresh) for: {} (age: {}ms)", location, age_millis);
                }

                // Always return cached value immediately
                return Ok(Self::make_get_result(Bytes::from(value.data.clone()), value.meta.clone()));
            }

            // Regular cache expiration check for non-checkpoint files
            if value.is_expired(ttl) {
                self.update_stats(|s| s.ttl_expirations += 1).await;
                self.cache.remove(&cache_key);
                debug!(
                    "Foyer cache EXPIRED for: {} (TTL: {}s, age: {}ms)",
                    location,
                    ttl.as_secs(),
                    current_millis().saturating_sub(value.timestamp_millis)
                );
            } else {
                self.update_stats(|s| s.hits += 1).await;
                span.record("cache_hit", true);
                let is_parquet = is_parquet_file(location);
                debug!(
                    "Foyer cache HIT for: {} (avoiding S3 access, parquet={}, TTL={}s, age={}ms, size={} bytes)",
                    location,
                    is_parquet,
                    ttl.as_secs(),
                    current_millis().saturating_sub(value.timestamp_millis),
                    value.data.len()
                );
                let result = Self::make_get_result(Bytes::from(value.data.clone()), value.meta.clone());
                self.maybe_touch(&self.cache, &cache_key, entry.clone(), self.config.l1_max_entry_bytes);
                return Ok(result);
            }
        }

        // Cache miss - fetch from inner store
        span.record("cache_hit", false);
        self.update_stats(|s| {
            s.misses += 1;
            s.inner_gets += 1;
        })
        .await;
        let is_parquet = is_parquet_file(location);
        let ttl = self.get_ttl_for_path(location);
        debug!(
            "Foyer cache MISS for: {} (fetching from S3, parquet={}, TTL={}s)",
            location,
            is_parquet,
            ttl.as_secs()
        );

        let start_time = std::time::Instant::now();
        let inner_span = tracing::trace_span!(parent: &span, "s3.get", location = %location);
        let result = self.inner.get(location).instrument(inner_span).await?;
        let duration = start_time.elapsed();

        debug!(
            "S3 GET request: {} (size: {} bytes, duration: {}ms, parquet: {})",
            location,
            result.meta.size,
            duration.as_millis(),
            is_parquet
        );

        // Collect payload for caching
        use futures::TryStreamExt;
        let data = match result.payload {
            GetResultPayload::Stream(s) => {
                let chunks: Vec<Bytes> = s.try_collect().await?;
                chunks.concat()
            }
            GetResultPayload::File(mut file, _) => {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).map_err(|e| object_store::Error::Generic {
                    store:  "cache",
                    source: Box::new(e),
                })?;
                buf
            }
        };

        self.insert_main_value(location, CacheValue::new(data.clone(), result.meta.clone()));
        Ok(Self::make_get_result(Bytes::from(data), result.meta))
    }

    #[instrument(
        name = "foyer_cache.get_range",
        skip_all,
        fields(
            location = %location,
            range.start = range.start,
            range.end = range.end,
            range.size = range.end - range.start,
            is_parquet = is_parquet_file(location),
            cache_hit = Empty,
            is_metadata = Empty,
        )
    )]
    async fn get_range_cached(&self, location: &Path, range: Range<u64>) -> ObjectStoreResult<Bytes> {
        let span = tracing::Span::current();
        let is_parquet = is_parquet_file(location);

        // First check if we have the full file cached
        let full_cache_key = Self::make_cache_key(location);
        if let Ok(Some(entry)) = self.cache.get(&full_cache_key).await {
            let value = entry.value();
            let ttl = self.get_ttl_for_path(location);
            if !value.is_expired(ttl) && range.end <= value.data.len() as u64 {
                self.update_stats(|s| s.hits += 1).await;
                span.record("cache_hit", true);
                debug!(
                    "Foyer cache HIT (full file) for range: {} (range: {}..{}, size: {} bytes, parquet={}, age={}ms)",
                    location,
                    range.start,
                    range.end,
                    range.end - range.start,
                    is_parquet,
                    current_millis().saturating_sub(value.timestamp_millis)
                );
                let sliced = Bytes::from(value.data[range.start as usize..range.end as usize].to_vec());
                self.maybe_touch(&self.cache, &full_cache_key, entry.clone(), self.config.l1_max_entry_bytes);
                return Ok(sliced);
            }
        }

        // For Parquet files, implement smart caching based on the range
        if is_parquet {
            // Probe the metadata range cache *before* any HEAD: its key is just
            // (location, range), so a steady-state footer read served from cache
            // pays zero S3 round-trips. Data ranges aren't stored here and fall
            // through to the size-based classification below.
            let range_cache_key = Self::make_range_cache_key(location, &range);
            if let Ok(Some(entry)) = self.metadata_cache.get(&range_cache_key).await {
                let value = entry.value();
                if !value.is_expired(self.config.ttl) {
                    self.update_metadata_stats(|s| s.hits += 1).await;
                    span.record("cache_hit", true);
                    span.record("is_metadata", true);
                    debug!(
                        "Metadata cache HIT for: {} (range: {}..{}, size: {} bytes, age={}ms)",
                        location,
                        range.start,
                        range.end,
                        value.data.len(),
                        current_millis().saturating_sub(value.timestamp_millis)
                    );
                    let sliced = Bytes::from(value.data.clone());
                    // l1_max=0: metadata entries are tiny, always keep in L1.
                    self.maybe_touch(&self.metadata_cache, &range_cache_key, entry.clone(), 0);
                    return Ok(sliced);
                }
            }

            // Range-cache miss: we need the file size to classify the request and
            // to stamp the cached range's meta. Use the cached ObjectMeta
            // (immutable Delta files) so this HEAD is paid at most once per file.
            let file_meta = match self.head_cached(location).await {
                Ok(meta) => meta,
                Err(e) => {
                    debug!("Failed to get metadata for {}: {}", location, e);
                    return Err(e);
                }
            };

            let file_size = file_meta.size;
            let metadata_size_hint = self.config.parquet_metadata_size_hint as u64;

            // Containment probe against the two ranges `warm_footer` can have
            // populated: the suffix tail (size-hint..size) and — for files
            // smaller than the hint — the whole file (0..size). The reader's
            // own footer reads (8-byte tail, then the parsed metadata range)
            // never equal those keys exactly, so the exact-key probe above
            // misses and a pre-warmed cold partition still paid 1-2 S3 RTTs
            // of metadata latency (300 ms+ observed against OVH).
            let warm_start = file_size.saturating_sub(metadata_size_hint);
            // When warm_start == 0 the two candidate ranges coincide; probe once.
            // candidate=0 also means files smaller than the hint are fully
            // cached here by warm_footer — any in-bounds read (including data
            // pages) is intentionally served from the metadata cache.
            // Probe the suffix key first: for files larger than the hint only
            // (warm_start..size) exists, so leading with it saves an always-miss
            // (0..size) lookup on the common footer-read path.
            let candidates: &[u64] = if warm_start == 0 { &[0] } else { &[warm_start, 0] };
            for &candidate in candidates {
                if candidate <= range.start && range.end <= file_size {
                    let key = Self::make_range_cache_key(location, &(candidate..file_size));
                    if let Ok(Some(entry)) = self.metadata_cache.get(&key).await {
                        let value = entry.value();
                        let (s, e) = ((range.start - candidate) as usize, (range.end - candidate) as usize);
                        if !value.is_expired(self.config.ttl) && e <= value.data.len() {
                            self.update_metadata_stats(|st| st.hits += 1).await;
                            span.record("cache_hit", true);
                            span.record("is_metadata", true);
                            // Distinct from the exact-key HIT log above so cache-key
                            // alignment is diagnosable on a new deployment.
                            debug!(
                                "Metadata cache HIT (containment {}..{}) for: {} (range: {}..{})",
                                candidate, file_size, location, range.start, range.end
                            );
                            let sliced = Bytes::copy_from_slice(&value.data[s..e]);
                            self.maybe_touch(&self.metadata_cache, &key, entry.clone(), 0);
                            return Ok(sliced);
                        }
                    }
                }
            }

            // Check if this is likely a metadata request (reading from near the end of the file)
            let is_metadata_request = range.start >= file_size.saturating_sub(metadata_size_hint);
            span.record("is_metadata", is_metadata_request);

            if is_metadata_request {
                // Cache miss for metadata range - fetch just the range
                span.record("cache_hit", false);
                self.update_metadata_stats(|s| {
                    s.misses += 1;
                    s.inner_gets += 1;
                })
                .await;
                debug!(
                    "Metadata cache MISS for Parquet: {} (range: {}..{}, file_size: {})",
                    location, range.start, range.end, file_size
                );

                let start_time = std::time::Instant::now();
                let inner_span = tracing::trace_span!(parent: &span, "s3.get_range",
                    location = %location,
                    range.start = range.start,
                    range.end = range.end,
                    is_metadata = true
                );
                let data = self.inner.get_range(location, range.clone()).instrument(inner_span).await?;
                let duration = start_time.elapsed();

                debug!(
                    "S3 GET_RANGE request (metadata): {} (range: {}..{}, size: {} bytes, duration: {}ms)",
                    location,
                    range.start,
                    range.end,
                    data.len(),
                    duration.as_millis()
                );

                // Cache the metadata range in the metadata cache
                let range_meta = ObjectMeta {
                    location:      location.clone(),
                    last_modified: file_meta.last_modified,
                    size:          data.len() as u64,
                    e_tag:         file_meta.e_tag.clone(),
                    version:       file_meta.version.clone(),
                };
                self.metadata_cache.insert(range_cache_key, CacheValue::new(data.to_vec(), range_meta));

                return Ok(data);
            } else {
                // For data requests, try to cache the full file
                debug!(
                    "Foyer cache MISS for Parquet data: {} (range: {}..{}, fetching full file)",
                    location, range.start, range.end
                );

                // Try to fetch and cache the full file
                if let Ok(result) = self.get_cached(location).await {
                    // The file is now cached, extract the range
                    if range.end <= result.meta.size {
                        let data = match result.payload {
                            GetResultPayload::Stream(s) => {
                                use futures::TryStreamExt;
                                let chunks: Vec<Bytes> = s.try_collect().await?;
                                let full_data = chunks.concat();
                                Bytes::from(full_data[range.start as usize..range.end as usize].to_vec())
                            }
                            GetResultPayload::File(mut file, _) => {
                                use std::io::{Read, Seek, SeekFrom};
                                file.seek(SeekFrom::Start(range.start)).map_err(|e| object_store::Error::Generic {
                                    store:  "cache",
                                    source: Box::new(e),
                                })?;
                                let mut buf = vec![0; (range.end - range.start) as usize];
                                file.read_exact(&mut buf).map_err(|e| object_store::Error::Generic {
                                    store:  "cache",
                                    source: Box::new(e),
                                })?;
                                Bytes::from(buf)
                            }
                        };
                        return Ok(data);
                    }
                }
            }
        }

        // Fallback to regular range request for non-parquet files
        span.record("cache_hit", false);
        self.update_stats(|s| {
            s.misses += 1;
            s.inner_gets += 1;
        })
        .await;
        debug!(
            "get_range request for: {} (range: {}..{}, parquet={})",
            location, range.start, range.end, is_parquet
        );

        let start_time = std::time::Instant::now();
        let inner_span = tracing::trace_span!(parent: &span, "s3.get_range",
            location = %location,
            range.start = range.start,
            range.end = range.end
        );
        let result = self.inner.get_range(location, range.clone()).instrument(inner_span).await?;
        let duration = start_time.elapsed();

        debug!(
            "S3 GET_RANGE request: {} (range: {}..{}, size: {} bytes, duration: {}ms, parquet: {})",
            location,
            range.start,
            range.end,
            range.end - range.start,
            duration.as_millis(),
            is_parquet
        );

        Ok(result)
    }

    /// Resolve a path's `ObjectMeta` from cache only (no S3). Checks the
    /// full-file cache, then — for immutable parquet data files — the dedicated
    /// meta cache. Returns `None` if neither has a live entry.
    async fn cached_meta(&self, location: &Path) -> Option<ObjectMeta> {
        if let Ok(Some(entry)) = self.cache.get(&Self::make_cache_key(location)).await {
            let value = entry.value();
            if !value.is_expired(self.get_ttl_for_path(location)) {
                return Some(value.meta.clone());
            }
        }
        if is_parquet_file(location)
            && let Ok(Some(entry)) = self.metadata_cache.get(&Self::make_meta_cache_key(location)).await
        {
            let value = entry.value();
            if !value.is_expired(self.config.ttl) {
                return Some(value.meta.clone());
            }
        }
        None
    }

    #[instrument(
        name = "foyer_cache.head",
        skip_all,
        fields(
            location = %location,
            cache_hit = Empty,
        )
    )]
    async fn head_cached(&self, location: &Path) -> ObjectStoreResult<ObjectMeta> {
        let span = tracing::Span::current();
        if let Some(meta) = self.cached_meta(location).await {
            span.record("cache_hit", true);
            return Ok(meta);
        }

        span.record("cache_hit", false);
        let inner_span = tracing::trace_span!(parent: &span, "s3.head", location = %location);
        let meta = self.inner.head(location).instrument(inner_span).await?;
        // Cache immutable parquet meta so later footer reads skip the HEAD. Skip
        // mutable paths (Delta log / _last_checkpoint can be rewritten in place).
        if is_parquet_file(location) {
            self.metadata_cache.insert(Self::make_meta_cache_key(location), CacheValue::new(Vec::new(), meta.clone()));
        }
        Ok(meta)
    }

    /// Core put logic: writes to inner store, then caches the new data
    async fn put_cached(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.update_stats(|s| s.inner_puts += 1).await;
        let payload_size = payload.content_length();
        let is_parquet = is_parquet_file(location);

        // Keep a cheap (Arc-backed) handle to the payload so we can warm the
        // cache from the bytes we already hold — no need to re-download what we
        // just wrote to S3.
        let payload_for_cache = payload.clone();

        debug!("S3 PUT request starting: {} (size: {} bytes, parquet: {})", location, payload_size, is_parquet);
        let start_time = std::time::Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await?;
        debug!(
            "S3 PUT request completed: {} (size: {} bytes, duration: {}ms, parquet: {})",
            location,
            payload_size,
            start_time.elapsed().as_millis(),
            is_parquet
        );

        // Warm the cache directly from the just-written bytes — a range-agnostic
        // full-file entry, so any subsequent ranged read is served by a slice.
        // ObjectMeta is reconstructed from the PutResult (e_tag/version) and the
        // known payload size; no post-write GET. insert_main_value applies the
        // recent-days window and large-entry disk steering.
        if payload_size > 0 {
            let mut data = Vec::with_capacity(payload_size);
            for chunk in payload_for_cache.iter() {
                data.extend_from_slice(chunk);
            }
            let meta = put_result_meta(location.clone(), payload_size as u64, &result);
            self.insert_main_value(location, CacheValue::new(data, meta));
            debug!("Warmed cache from write payload: {} (size: {} bytes)", location, payload_size);
        }

        // Overwrites land a fresh full-file entry (checked first on read), but
        // stale per-range metadata entries from a previous version of this key
        // must still be dropped.
        if is_parquet {
            self.invalidate_metadata_cache(location).await;
        }
        Ok(result)
    }

    /// Invalidate cache for delete/copy destination
    async fn invalidate_for_delete(&self, location: &Path) {
        self.cache.remove(&Self::make_cache_key(location));
        if is_parquet_file(location) {
            self.invalidate_metadata_cache(location).await;
        }
    }

    /// Admit a full-file entry to the main cache, honoring the recent-days
    /// window (cold/old partitions are skipped → served from S3) and steering
    /// large entries to disk-only so they don't evict the L1 hot set.
    fn insert_main_value(&self, location: &Path, value: CacheValue) {
        if !is_within_recent_window(location, self.config.cache_recent_days) {
            return;
        }
        insert_main(&self.cache, Self::make_cache_key(location), value, self.config.l1_max_entry_bytes);
    }

    /// Sliding-TTL refresh: keep an entry at most `ttl` past its *last query*
    /// rather than its insertion. On a hit, once an entry is more than halfway
    /// to expiry, re-insert it with a fresh timestamp so frequently-queried
    /// data survives indefinitely while cold data still ages out after `ttl`.
    ///
    /// Throttled (the halfway gate + one in-flight refresh per key) so a hot
    /// entry is rewritten at most once per `ttl/2`, and run in the background
    /// off a cheap `entry` clone so the read never blocks on the re-insert (the
    /// data clone happens in the spawned task, not on the query path).
    fn maybe_touch(&self, cache: &FoyerCache, key: &str, entry: foyer::HybridCacheEntry<String, CacheValue>, l1_max_entry_bytes: usize) {
        let age = current_millis().saturating_sub(entry.value().timestamp_millis);
        // `as_millis()` is u128; clamp before the u64 cast so an absurdly large
        // configured TTL can't silently truncate into a tiny value.
        let ttl_millis = self.config.ttl.as_millis().min(u64::MAX as u128) as u64;
        if age.saturating_mul(2) <= ttl_millis {
            return; // still fresh enough — don't churn the cache
        }
        if !self.refreshing.insert(key.to_string()) {
            return; // a refresh is already in flight for this key
        }
        let cache = cache.clone();
        let refreshing = self.refreshing.clone();
        let key = key.to_string();
        let handle = tokio::spawn(async move {
            let v = entry.value();
            insert_main(&cache, key.clone(), CacheValue::new(v.data.clone(), v.meta.clone()), l1_max_entry_bytes);
            refreshing.remove(&key);
        });
        // Best-effort join registration: if the lock is contended we drop the
        // handle and the refresh task simply detaches — it still runs to
        // completion, it just won't be awaited by `background_tasks` on
        // shutdown. So `background_tasks` is not an exhaustive registry of
        // in-flight refreshes; don't assume it joins every one.
        if let Ok(mut tasks) = self.background_tasks.try_lock() {
            tasks.spawn(async move {
                let _ = handle.await;
            });
        }
    }
}

/// Wraps an inner [`MultipartUpload`] to tee written bytes into a bounded
/// buffer, so the completed file can be inserted into the cache directly — we
/// never re-download a file we just streamed to S3. If the upload grows past
/// `max_warm_bytes` the buffer is dropped and the rest streams through
/// un-captured (large compaction outputs fall back to the selective
/// post-commit warm path); this bounds both transient memory and L1 cache
/// pressure. Strictly best-effort: failure to capture never affects the write.
struct CachingMultipartUpload {
    inner:              Box<dyn MultipartUpload>,
    location:           Path,
    cache:              FoyerCache,
    /// `None` once the cap was exceeded (capture abandoned for this upload).
    buffer:             Option<Vec<u8>>,
    max_warm_bytes:     usize,
    l1_max_entry_bytes: usize,
}

impl std::fmt::Debug for CachingMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingMultipartUpload").field("location", &self.location).finish()
    }
}

#[async_trait]
impl MultipartUpload for CachingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        if let Some(buf) = self.buffer.as_mut() {
            if buf.len().saturating_add(data.content_length()) > self.max_warm_bytes {
                // Too big to warm without risking memory / L1 eviction — give
                // up capturing for this upload.
                self.buffer = None;
            } else {
                for chunk in data.iter() {
                    buf.extend_from_slice(chunk);
                }
            }
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let result = self.inner.complete().await?;
        if let Some(buf) = self.buffer.take()
            && !buf.is_empty()
        {
            let size = buf.len() as u64;
            let meta = put_result_meta(self.location.clone(), size, &result);
            // Use the same key derivation as the read path so a multipart-warmed
            // entry is found by a later GET even if `make_cache_key` ever does
            // more than `location.to_string()`.
            insert_main(
                &self.cache,
                FoyerObjectStoreCache::make_cache_key(&self.location),
                CacheValue::new(buf, meta),
                self.l1_max_entry_bytes,
            );
            debug!("Warmed cache from multipart write: {} (size: {} bytes)", self.location, size);
        }
        Ok(result)
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        self.buffer = None;
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for FoyerObjectStoreCache {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.put_cached(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        // Skip capture for cold-partition rewrites (e.g. tier recompress of
        // week+-old data) — recent data stays local, old data is served from S3.
        if !is_within_recent_window(location, self.config.cache_recent_days) {
            return Ok(inner);
        }
        // Parquet writers (flush + compaction outputs) stream large files via
        // multipart. Tee the written bytes into a bounded buffer so the
        // completed file warms the cache directly — no re-download of what we
        // just uploaded. Cap the buffer at the disk block size (the largest
        // entry foyer can persist), optionally tightened by warm_inline_max_bytes.
        let mut cap = self.config.block_size_bytes;
        if self.config.warm_inline_max_bytes > 0 {
            cap = cap.min(self.config.warm_inline_max_bytes);
        }
        Ok(Box::new(CachingMultipartUpload {
            inner,
            location: location.clone(),
            cache: self.cache.clone(),
            buffer: Some(Vec::new()),
            max_warm_bytes: cap,
            l1_max_entry_bytes: self.config.l1_max_entry_bytes,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // Handle range requests via the dedicated range cache path
        if let Some(GetRange::Bounded(ref r)) = options.range
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
        {
            let range = r.clone();
            let bytes = self.get_range_cached(location, range.clone()).await?;
            let meta = self.head_cached(location).await.unwrap_or(ObjectMeta {
                location:      location.clone(),
                last_modified: Utc::now(),
                size:          range.end,
                e_tag:         None,
                version:       None,
            });
            let data_len = bytes.len() as u64;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes) }))),
                meta,
                attributes: Attributes::new(),
                range: range.start..range.start + data_len,
            });
        }
        // Suffix range (footer warm + any suffix reader): resolve to an absolute
        // range so it shares cache keys with bounded footer reads. If we already
        // know the size (cached meta), reuse the bounded path for free; otherwise
        // a single suffix GET to the inner store learns the absolute range + size
        // from the response — one round-trip, no separate HEAD.
        if let Some(GetRange::Suffix(n)) = options.range
            && options.if_match.is_none()
            && options.if_none_match.is_none()
            && options.if_modified_since.is_none()
            && options.if_unmodified_since.is_none()
        {
            let n = n.max(1);
            if let Some(meta) = self.cached_meta(location).await {
                let range = meta.size.saturating_sub(n)..meta.size;
                let bytes = self.get_range_cached(location, range.clone()).await?;
                let data_len = bytes.len() as u64;
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes) }))),
                    meta,
                    attributes: Attributes::new(),
                    range: range.start..range.start + data_len,
                });
            }
            let result = self
                .inner
                .get_opts(
                    location,
                    GetOptions {
                        range: Some(GetRange::Suffix(n)),
                        ..Default::default()
                    },
                )
                .await?;
            let meta = result.meta.clone();
            let abs_range = result.range.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            self.update_metadata_stats(|s| {
                s.misses += 1;
                s.inner_gets += 1;
            })
            .await;
            // Populate both the footer-range cache (under the absolute key bounded
            // reads use) and the immutable-meta cache, so the next footer read is
            // a pure cache hit.
            if is_parquet_file(location) {
                let range_meta = ObjectMeta {
                    location:      location.clone(),
                    last_modified: meta.last_modified,
                    size:          bytes.len() as u64,
                    e_tag:         meta.e_tag.clone(),
                    version:       meta.version.clone(),
                };
                self.metadata_cache
                    .insert(Self::make_range_cache_key(location, &abs_range), CacheValue::new(bytes.to_vec(), range_meta));
                self.metadata_cache.insert(Self::make_meta_cache_key(location), CacheValue::new(Vec::new(), meta.clone()));
            }
            let data_len = bytes.len() as u64;
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(bytes) }))),
                meta,
                attributes,
                range: abs_range.start..abs_range.start + data_len,
            });
        }
        // Bypass cache for complex (conditional / non-bounded) requests
        if options.range.is_some()
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.head
        {
            return self.inner.get_opts(location, options).await;
        }
        self.get_cached(location).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, ObjectStoreResult<Path>>) -> BoxStream<'static, ObjectStoreResult<Path>> {
        use futures::StreamExt;
        let cache = self.cache.clone();
        let metadata_cache = self.metadata_cache.clone();
        let inner_stream = self.inner.delete_stream(locations);
        inner_stream
            .inspect(move |res| {
                if let Ok(path) = res {
                    cache.remove(&path.to_string());
                    if is_parquet_file(path) {
                        // Best-effort: we can't enumerate metadata keys without head;
                        // remove the most common ones by reusing the same heuristic offsets.
                        let _ = &metadata_cache;
                    }
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.invalidate_for_delete(to).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    #[tokio::test]
    async fn test_basic_operations() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner, FoyerCacheConfig::test_config("basic_ops")).await?;
        cache.reset_stats().await;

        let path = Path::from("test/file.parquet");
        let data = Bytes::from("test data");

        cache.put(&path, PutPayload::from(data.clone())).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_puts, 1);
        assert_eq!(stats.main.inner_gets, 0); // Cached directly from the write payload — no re-fetch

        // First get - cache hit (since we cache on write)
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // No fetch needed - cached from write payload
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0], data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Still no fetch - served from cache
        assert_eq!(stats.main.hits, 2); // Two cache hits total
        assert_eq!(stats.main.misses, 0);

        cache.delete(&path).await?;

        // Give cache time to process deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // After deletion, get should fail
        let get_result = cache.get(&path).await;
        assert!(get_result.is_err(), "Expected error after delete, got: {:?}", get_result);

        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_cache_prevents_s3_access() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("s3_bypass", |c| {
            c.memory_size_bytes = 10 * 1024 * 1024;
            c.disk_size_bytes = 100 * 1024 * 1024;
            c.ttl = Duration::from_secs(300);
        });

        let cache = FoyerObjectStoreCache::new(inner, config).await?;
        cache.reset_stats().await;

        let files = vec![
            ("table/part-001.parquet", vec![b'a'; 1024]),
            ("table/part-002.parquet", vec![b'b'; 2048]),
            ("table/part-003.parquet", vec![b'c'; 4096]),
        ];

        // Write all files
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            cache.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;
        }

        // First read - cache hit (since we cache on write)
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let bytes: Vec<Bytes> = match result.payload {
                GetResultPayload::Stream(s) => s.try_collect().await?,
                _ => panic!("Expected stream"),
            };
            assert_eq!(bytes[0].len(), data.len());
        }

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Cached from write payloads — no re-fetch
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 3);

        // Second read - cache hit
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            let result = cache.get(&path).await?;
            use futures::TryStreamExt;
            let bytes: Vec<Bytes> = match result.payload {
                GetResultPayload::Stream(s) => s.try_collect().await?,
                _ => panic!("Expected stream"),
            };
            assert_eq!(bytes[0].len(), data.len());
        }

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // No inner gets at all
        assert_eq!(stats.main.hits, 6); // Total 6 hits (3 per read)

        info!("Cache successfully prevented {} S3 accesses", stats.main.hits);

        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_ttl_expiration() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("ttl_{}", std::process::id());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.ttl = Duration::from_millis(100);
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner, config).await?;

        let path = Path::from("test/ttl_file.parquet");
        let data = Bytes::from("test data");

        cache.put(&path, PutPayload::from(data.clone())).await?;
        let _ = cache.get(&path).await?;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = cache.get(&path).await?;

        let stats = cache.get_stats().await;
        info!("TTL test - main cache hits: {}, misses: {}", stats.main.hits, stats.main.misses);

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_large_file_disk_cache() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("disk", |c| {
            c.memory_size_bytes = 1024; // Very small memory
        });

        let cache = FoyerObjectStoreCache::new(inner, config).await?;
        cache.reset_stats().await;

        let large_data = Bytes::from(vec![b'x'; 10 * 1024]); // 10KB
        let path = Path::from("test/large_file.parquet");

        cache.put(&path, PutPayload::from(large_data.clone())).await?;

        // First get - cache hit (since we cache on write)
        let result = cache.get(&path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Cached from write payload — no re-fetch
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        let result2 = cache.get(&path).await?;
        let bytes2: Vec<Bytes> = match result2.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes2[0].len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Still no fetch - served from cache
        assert_eq!(stats.main.hits, 2); // Two cache hits total

        info!("Large file test - main cache hits: {}, misses: {}", stats.main.hits, stats.main.misses);
        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_metadata_optimization() -> anyhow::Result<()> {
        // Use a unique test name to avoid cache conflicts
        let test_id = format!("parquet_metadata_{}", std::process::id());

        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.parquet_metadata_size_hint = 1024; // 1KB for testing
            c.ttl = Duration::from_secs(300);
        });

        // Ensure cache directory is cleaned up first
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;

        // Create a test parquet file (10KB)
        let file_size = 10 * 1024;
        let parquet_data = vec![b'x'; file_size];
        let path = Path::from("test/file.parquet");

        // Put the file directly in the inner store to avoid caching
        inner.put(&path, PutPayload::from(Bytes::from(parquet_data.clone()))).await?;

        // Reset stats to start fresh
        cache.reset_stats().await;

        // Test 1: Request metadata (last 1KB) - should cache only the range
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let metadata = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(metadata.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.inner_gets, 1); // One get_range call for metadata
        assert_eq!(stats.metadata.misses, 1);
        assert_eq!(stats.metadata.hits, 0);

        // Test 2: Request same metadata range again - should hit range cache
        let metadata2 = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(metadata2.len(), 1024);
        assert_eq!(metadata, metadata2);

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.inner_gets, 1); // No additional inner get
        assert_eq!(stats.metadata.hits, 1); // Cache hit on range
        assert_eq!(stats.metadata.misses, 1);

        // Test 3: Request data from beginning - should fetch and cache full file
        let data_range = 0..1024;
        let data = cache.get_range(&path, data_range.clone()).await?;
        assert_eq!(data.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // One get for full file
        assert_eq!(stats.main.misses, 1);
        assert_eq!(stats.metadata.hits, 1); // Still have metadata cache hit

        // Test 4: Request any range now - should hit full file cache
        let another_range = 2048..3072;
        let another_data = cache.get_range(&path, another_range).await?;
        assert_eq!(another_data.len(), 1024);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 1); // No additional inner get
        assert_eq!(stats.main.hits, 1); // Cache hit on full file

        info!("Parquet metadata optimization test passed");
        info!("Main cache - hits: {}, misses: {}", stats.main.hits, stats.main.misses);
        info!("Metadata cache - hits: {}, misses: {}", stats.metadata.hits, stats.metadata.misses);
        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_separation() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("metadata_separation_{}", std::process::id());

        // Use in-memory store for testing
        let inner = Arc::new(InMemory::new());

        // Configure cache with small limits to test separation
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.memory_size_bytes = 10 * 1024 * 1024; // 10MB
            c.disk_size_bytes = 50 * 1024 * 1024; // 50MB
            c.metadata_memory_size_bytes = 5 * 1024 * 1024; // 5MB
            c.metadata_disk_size_bytes = 20 * 1024 * 1024; // 20MB
            c.parquet_metadata_size_hint = 1024; // 1KB
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Create a parquet file
        let path = Path::from("test.parquet");
        let file_size = 1024 * 1024; // 1MB
        let data = vec![b'a'; file_size];
        inner.put(&path, PutPayload::from(Bytes::from(data))).await?;

        // Test 1: Read metadata range (should use metadata cache)
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let result = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(result.len(), 1024, "Should get correct range size");

        let stats = cache.get_stats().await;
        info!(
            "After first get_range - metadata.misses: {}, metadata.hits: {}, main.misses: {}, main.hits: {}",
            stats.metadata.misses, stats.metadata.hits, stats.main.misses, stats.main.hits
        );
        assert_eq!(stats.metadata.misses, 1, "Should have 1 metadata cache miss");
        assert_eq!(stats.metadata.hits, 0, "Should have 0 metadata cache hits");
        assert_eq!(stats.main.hits, 0, "Should have 0 main cache hits");

        // Test 2: Read same metadata range again (should hit metadata cache)
        let _ = cache.get_range(&path, metadata_range.clone()).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 1, "Should have 1 metadata cache hit");
        assert_eq!(stats.metadata.misses, 1, "Should still have 1 metadata cache miss");

        // Test 3: Read data range (should use main cache)
        let data_range = 0..1024;
        let _ = cache.get_range(&path, data_range).await?;

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.misses, 1, "Should have 1 main cache miss");

        // Test 4: Read full file (should use main cache)
        let _ = cache.get(&path).await?;

        let stats = cache.get_stats().await;
        assert!(stats.main.hits > 0 || stats.main.misses > 0, "Main cache should be used for full file");

        info!("Main cache stats: hits={}, misses={}", stats.main.hits, stats.main.misses);
        info!("Metadata cache stats: hits={}, misses={}", stats.metadata.hits, stats.metadata.misses);

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_invalidation() -> anyhow::Result<()> {
        // Use a unique test name to avoid conflicts
        let test_id = format!("metadata_invalidation_{}", std::process::id());

        let inner = Arc::new(InMemory::new());

        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.parquet_metadata_size_hint = 1024;
            c.metadata_memory_size_bytes = 5 * 1024 * 1024;
            c.metadata_disk_size_bytes = 20 * 1024 * 1024;
        });

        // Clean up any existing cache directory
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Create a parquet file directly in inner store (to avoid main cache)
        let path = Path::from("test.parquet");
        let file_size = 10 * 1024; // 10KB
        let data = vec![b'a'; file_size];
        inner.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;

        // Read metadata range - should use metadata cache
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let result = cache.get_range(&path, metadata_range.clone()).await?;
        assert_eq!(result.len(), 1024, "Should get correct range size");

        let stats = cache.get_stats().await;
        info!(
            "After first get_range - metadata.misses: {}, metadata.hits: {}, main.misses: {}, main.hits: {}",
            stats.metadata.misses, stats.metadata.hits, stats.main.misses, stats.main.hits
        );
        assert_eq!(stats.metadata.misses, 1, "Should have metadata cache miss");
        assert_eq!(stats.metadata.hits, 0, "Should have no metadata cache hits yet");

        // Read again - should hit metadata cache
        let _ = cache.get_range(&path, metadata_range.clone()).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 1, "Should hit metadata cache");

        // Update the file via cache - should invalidate metadata cache
        let new_data = vec![b'b'; file_size];
        cache.put(&path, PutPayload::from(Bytes::from(new_data))).await?;

        // Read metadata again - should be served from main cache now (file was cached on put)
        let _ = cache.get_range(&path, metadata_range).await?;
        let stats = cache.get_stats().await;
        // The range will be served from the main cache since put() caches the full file
        assert_eq!(stats.main.hits, 1, "Should hit main cache after put");

        info!("Metadata cache invalidation test passed");
        info!(
            "Final stats - Main: hits={}, misses={}, Metadata: hits={}, misses={}",
            stats.main.hits, stats.main.misses, stats.metadata.hits, stats.metadata.misses
        );

        cache.shutdown().await?;

        // Clean up cache directory after test
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_capture_warms_cache() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let test_id = format!("mpu_capture_{}", std::process::id());
        let inner = Arc::new(InMemory::new());
        // Tighten the inline cap to 1MB (below the 4MB block size) so we can
        // exercise both the captured and skipped paths.
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.warm_inline_max_bytes = 1024 * 1024;
        });
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Small multipart write (under the cap) → captured into the cache on
        // complete, with no re-download.
        let small_path = Path::from("table/date=2026-06-05/small.parquet");
        let small_data = Bytes::from(vec![b'a'; 256 * 1024]);
        let mut upload = cache.put_multipart(&small_path).await?;
        upload.put_part(small_data.clone().into()).await?;
        upload.complete().await?;

        // A read is served entirely from cache — the multipart write warmed it.
        let result = cache.get(&small_path).await?;
        use futures::TryStreamExt;
        let bytes: Vec<Bytes> = match result.payload {
            GetResultPayload::Stream(s) => s.try_collect().await?,
            _ => panic!("Expected stream"),
        };
        assert_eq!(bytes.concat().len(), small_data.len());
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "small multipart write should warm the cache");
        assert_eq!(stats.main.misses, 0, "no S3 read needed after multipart capture");

        // Large multipart write (over the cap) → capture abandoned, streams
        // through, so the first read is a genuine miss.
        cache.reset_stats().await;
        let big_path = Path::from("table/date=2026-06-05/big.parquet");
        let big_chunk = Bytes::from(vec![b'b'; 768 * 1024]);
        let mut upload = cache.put_multipart(&big_path).await?;
        upload.put_part(big_chunk.clone().into()).await?; // 768KB
        upload.put_part(big_chunk.clone().into()).await?; // 1.5MB total > 1MB cap
        upload.complete().await?;

        let _ = cache.get(&big_path).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.misses, 1, "over-cap multipart write should not be cached inline");

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[test]
    fn test_block_size_tracks_optimize_target() {
        use crate::config::AppConfig;
        let mut cfg = AppConfig::default();

        // Defaults: 2x the 128MB target == the 256MB configured floor.
        assert_eq!(FoyerCacheConfig::from_app_config(&cfg).block_size_bytes, 256 * 1024 * 1024);

        // Raise the optimize target past the floor → block auto-tracks to 2x,
        // so big outputs stay cacheable without touching the cache config.
        cfg.parquet.timefusion_optimize_target_size = 512 * 1024 * 1024;
        assert_eq!(
            FoyerCacheConfig::from_app_config(&cfg).block_size_bytes,
            1024 * 1024 * 1024,
            "block size should track 2x the optimize target"
        );

        // With a small target, the configured block size is the floor.
        cfg.parquet.timefusion_optimize_target_size = 16 * 1024 * 1024;
        cfg.cache.timefusion_foyer_block_size_mb = 256;
        assert_eq!(
            FoyerCacheConfig::from_app_config(&cfg).block_size_bytes,
            256 * 1024 * 1024,
            "configured block size acts as a floor"
        );
    }

    #[tokio::test]
    async fn test_sliding_ttl_refresh_on_query() -> anyhow::Result<()> {
        let test_id = format!("sliding_ttl_{}", std::process::id());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.ttl = Duration::from_millis(1000);
        });
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);
        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner, config).await?;

        let path = Path::from("table/part-hot.parquet");
        cache.put(&path, PutPayload::from(Bytes::from(vec![b'h'; 4096]))).await?;

        // Query past the halfway point (ttl/2 = 500ms) → triggers a sliding-TTL
        // refresh that re-stamps the entry to "now".
        tokio::time::sleep(Duration::from_millis(600)).await;
        let _ = cache.get(&path).await?; // hit + background touch
        tokio::time::sleep(Duration::from_millis(200)).await; // let the re-insert land

        // Now ~1200ms since the original insert (> base TTL) but well within the
        // refreshed window — a non-sliding TTL would have expired this entry.
        tokio::time::sleep(Duration::from_millis(400)).await;
        cache.reset_stats().await;
        let _ = cache.get(&path).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "queried entry should survive past base TTL via sliding refresh");
        assert_eq!(stats.main.misses, 0);

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_evict_data_entry_removes_cached_file() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config("evict_entry")).await?;
        let cache = FoyerObjectStoreCache::new_with_shared_cache(inner, &shared);
        cache.reset_stats().await;

        let path = Path::from("table/date=2026-06-05/part.parquet");
        cache.put(&path, PutPayload::from(Bytes::from(vec![b'a'; 4096]))).await?;
        let _ = cache.get(&path).await?;
        assert_eq!(cache.get_stats().await.main.hits, 1, "freshly written file should be cached");
        assert!(
            shared.cache.memory().contains(&path.to_string()),
            "freshly read file should be in the in-memory cache"
        );

        // Proactive eviction (what the compaction path does for tombstoned
        // files) drops the entry from the in-memory cache immediately. foyer's
        // HybridCache::remove deletes the on-disk copy asynchronously, so we
        // assert on the memory layer for a deterministic result; the dead bytes
        // are reclaimed from disk shortly after rather than waiting for VACUUM.
        shared.evict_data_entry(path.as_ref());
        assert!(
            !shared.cache.memory().contains(&path.to_string()),
            "evicted entry should be dropped from the in-memory cache"
        );

        cache.shutdown().await?;
        Ok(())
    }

    #[test]
    fn test_is_within_recent_window() {
        let today = Utc::now().date_naive();
        let recent = Path::from(format!("t/date={}/part.parquet", today));
        let old = Path::from(format!("t/date={}/part.parquet", today - chrono::Duration::days(30)));

        // Recent partitions are admitted; week+-old ones are skipped.
        assert!(is_within_recent_window(&recent, 8));
        assert!(!is_within_recent_window(&old, 8));
        // 0 = no age limit → everything admitted.
        assert!(is_within_recent_window(&old, 0));
        // No date= segment (Delta log, checkpoints) → always admitted.
        assert!(is_within_recent_window(&Path::from("t/_delta_log/00001.json"), 8));
    }

    #[tokio::test]
    async fn test_recent_window_skips_old_partition_writes() -> anyhow::Result<()> {
        let test_id = format!("recent_window_{}", std::process::id());
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.cache_recent_days = 8; // enforce the window in this test
        });
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        let today = Utc::now().date_naive();
        let recent = Path::from(format!("t/date={}/part.parquet", today));
        let old = Path::from(format!("t/date={}/part.parquet", today - chrono::Duration::days(30)));
        let data = Bytes::from(vec![b'a'; 4096]);

        // Recent write is admitted → served from cache (no S3 read).
        cache.put(&recent, PutPayload::from(data.clone())).await?;
        let _ = cache.get(&recent).await?;
        assert_eq!(cache.get_stats().await.main.hits, 1, "recent write should be cached");

        // Old-partition write is NOT admitted → read falls through to S3 (miss).
        cache.reset_stats().await;
        cache.put(&old, PutPayload::from(data.clone())).await?;
        let _ = cache.get(&old).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 0, "old-partition write should not be cached");
        assert_eq!(stats.main.misses, 1, "old partition served from S3");

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_warm_footer_primes_metadata_cache() -> anyhow::Result<()> {
        let test_id = format!("warm_footer_{}", std::process::id());
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with(&test_id, |c| {
            c.parquet_metadata_size_hint = 1024; // 1KB footer
        });
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;

        // Write the file straight to the inner store so nothing is cached yet —
        // this simulates a multipart compaction output that bypassed put_cached.
        let file_size = 10 * 1024;
        let path = Path::from("table/date=2026-06-05/part-0.parquet");
        inner.put(&path, PutPayload::from(Bytes::from(vec![b'x'; file_size]))).await?;
        cache.reset_stats().await;

        // Warm the footer. The ranged GET should populate the metadata cache.
        assert!(warm_footer(&cache, &path, 1024).await);

        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.misses, 1, "warm should fetch the footer once");
        assert_eq!(stats.metadata.hits, 0);

        // A subsequent read of the same footer range is now a metadata HIT —
        // i.e. query planning pays zero S3 round-trips post-warm.
        let metadata_range = (file_size - 1024) as u64..file_size as u64;
        let _ = cache.get_range(&path, metadata_range).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 1, "footer read should hit after warm");

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    #[tokio::test]
    async fn test_warm_full_primes_main_cache() -> anyhow::Result<()> {
        let test_id = format!("warm_full_{}", std::process::id());
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config(&test_id);
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);

        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;

        let file_size = 8 * 1024;
        let path = Path::from("table/date=2026-06-05/part-1.parquet");
        inner.put(&path, PutPayload::from(Bytes::from(vec![b'y'; file_size]))).await?;
        cache.reset_stats().await;

        // Warm the full file into the main cache. The warm itself incurs one
        // miss (the fetch from the inner store); reset so we isolate the
        // post-warm read behavior.
        assert!(warm_full(&cache, &path).await);
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.misses, 1, "warm should fetch the full file once");
        cache.reset_stats().await;

        // Any subsequent data read is served from the full-file cache (main HIT),
        // never falling back to S3.
        let _ = cache.get_range(&path, 0..1024).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "data read should hit main cache after full warm");
        assert_eq!(stats.main.misses, 0);

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }

    /// Guards key consistency across the three cache paths that derive a key
    /// independently: the multipart-write warm (`complete()`), the read path
    /// (`make_cache_key`), and the compaction eviction path (`evict_data_entry`
    /// on a relativized object path). If any of them diverged, a multipart-warmed
    /// entry would either never be read back or never be evicted.
    #[tokio::test]
    async fn test_multipart_warm_read_and_evict_key_consistency() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let inner = Arc::new(InMemory::new());
        let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config("mpu_key_consistency")).await?;
        let cache = FoyerObjectStoreCache::new_with_shared_cache(inner, &shared);
        cache.reset_stats().await;

        // Warm via the multipart-write path.
        let path = Path::from("table/date=2026-06-05/part.parquet");
        let data = Bytes::from(vec![b'z'; 64 * 1024]);
        let mut upload = cache.put_multipart(&path).await?;
        upload.put_part(data.clone().into()).await?;
        upload.complete().await?;

        // Read path: a plain GET must find the entry the multipart write warmed —
        // i.e. `complete()` inserted under the same key the read derives. A key
        // mismatch would surface here as a miss + an S3 fetch.
        let _ = cache.get(&path).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "multipart-warmed entry must be found by a plain GET (warm/read key match)");
        assert_eq!(stats.main.misses, 0, "no S3 read needed after multipart capture");

        // Eviction path: the compaction hook evicts by the relativized object
        // path. It must target the same key warming/reads use, or tombstoned
        // files would linger. Assert at the in-memory layer — foyer removes the
        // on-disk copy asynchronously, so the memory layer is the deterministic
        // signal (mirrors test_evict_data_entry_removes_cached_file).
        assert!(
            shared.cache.memory().contains(&path.to_string()),
            "warmed entry should be in the in-memory cache"
        );
        shared.evict_data_entry(path.as_ref());
        assert!(
            !shared.cache.memory().contains(&path.to_string()),
            "evict must drop the same key warming/reads use"
        );

        cache.shutdown().await?;
        Ok(())
    }

    /// Wraps an `InMemory` store and counts S3-equivalent round-trips, so tests
    /// can assert that warming + reads issue the expected number of HEADs/GETs.
    /// `head()` is an extension method that routes through `get_opts(head:true)`,
    /// so we count it there.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<InMemory>,
        heads: Arc<std::sync::atomic::AtomicUsize>,
        gets:  Arc<std::sync::atomic::AtomicUsize>,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
            use std::sync::atomic::Ordering;
            if options.head {
                self.heads.fetch_add(1, Ordering::Relaxed);
            } else {
                self.gets.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(&self, locations: BoxStream<'static, ObjectStoreResult<Path>>) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Locks in both performance wins: a suffix-based footer warm is a single GET
    /// (no HEAD), and a later footer read of a warmed file is a pure cache hit —
    /// zero S3 round-trips (no HEAD to classify, no GET).
    #[tokio::test]
    async fn test_warm_footer_eliminates_read_path_heads() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mem = Arc::new(InMemory::new());
        let file_size = 10 * 1024usize;
        let path = Path::from("table/date=2026-06-05/part-heads.parquet");
        mem.put(&path, PutPayload::from(Bytes::from(vec![b'x'; file_size]))).await?;

        let heads = Arc::new(AtomicUsize::new(0));
        let gets = Arc::new(AtomicUsize::new(0));
        let counting = Arc::new(CountingStore {
            inner: mem.clone(),
            heads: heads.clone(),
            gets:  gets.clone(),
        });

        let config = FoyerCacheConfig::test_config_with("warm_footer_heads", |c| {
            c.parquet_metadata_size_hint = 1024;
        });
        let cache_dir = config.cache_dir.clone();
        let _ = std::fs::remove_dir_all(&cache_dir);
        let cache = FoyerObjectStoreCache::new(counting, config).await?;
        cache.reset_stats().await;

        // Footer warm: a single suffix GET, no HEAD.
        assert!(warm_footer(&cache, &path, 1024).await);
        assert_eq!(heads.load(Ordering::Relaxed), 0, "suffix warm must not issue a HEAD");
        assert_eq!(gets.load(Ordering::Relaxed), 1, "suffix warm is a single GET");

        // A later footer read of the warmed file is served entirely from cache:
        // no HEAD to classify the range, no GET for the bytes.
        let footer = (file_size - 1024) as u64..file_size as u64;
        let bytes = cache.get_range(&path, footer).await?;
        assert_eq!(bytes.len(), 1024);
        assert_eq!(heads.load(Ordering::Relaxed), 0, "warmed footer read must not HEAD");
        assert_eq!(gets.load(Ordering::Relaxed), 1, "warmed footer read must not GET (still just the warm)");
        assert_eq!(cache.get_stats().await.metadata.hits, 1, "footer served from the metadata cache");

        cache.shutdown().await?;
        let _ = std::fs::remove_dir_all(&cache_dir);
        Ok(())
    }
}
