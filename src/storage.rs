use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::{DashMap, DashSet};
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
use tracing::{Instrument, debug, field::Empty, info, instrument, warn};

/// Align large Parquet data reads so sliding time predicates reuse the same
/// cache entry even when page/coalescing boundaries move slightly. At most two
/// edge blocks are extra, bounding amplification to <2 MiB per request.
const PARQUET_RANGE_ALIGNMENT_BYTES: u64 = 1024 * 1024;

/// Cache entry with metadata and TTL
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheValue {
    /// `Bytes`, not `Vec<u8>`: served slices refcount the BUFFER, never the
    /// `HybridCacheEntry` (2bb5e85 pinned entries via `Bytes::from_owner` and
    /// stalled foyer admission under scan load — 191 failed flushes; see
    /// 5926f66). Eviction always proceeds; a live slice only delays freeing
    /// the bytes until its reader drops.
    data: Bytes,
    #[serde(with = "object_meta_serde")]
    meta: ObjectMeta,
    timestamp_millis: u64,
}

impl CacheValue {
    fn new(data: impl Into<Bytes>, meta: ObjectMeta) -> Self {
        Self { data: data.into(), meta, timestamp_millis: current_millis() }
    }

    fn age_millis(&self) -> u64 {
        current_millis().saturating_sub(self.timestamp_millis)
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.age_millis() > ttl.as_millis() as u64
    }
}

fn current_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn allocated_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.file_name().is_some_and(|name| name == "metadata") {
                return 0;
            }
            let Ok(meta) = entry.metadata() else {
                return 0;
            };
            if meta.is_dir() {
                return allocated_bytes(&path);
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                meta.blocks().saturating_mul(512)
            }
            #[cfg(not(unix))]
            {
                meta.len()
            }
        })
        .sum()
}

mod object_meta_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct SerializedMeta {
        location: String,
        last_modified: i64,
        size: u64,
        e_tag: Option<String>,
        version: Option<String>,
    }

    pub fn serialize<S>(meta: &ObjectMeta, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SerializedMeta {
            location: meta.location.to_string(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ObjectMeta, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = SerializedMeta::deserialize(deserializer)?;
        Ok(ObjectMeta {
            location: Path::from(s.location),
            last_modified: DateTime::<Utc>::from_timestamp_millis(s.last_modified).unwrap_or(Utc::now()),
            size: s.size,
            e_tag: s.e_tag,
            version: s.version,
        })
    }
}

/// Configuration for the foyer-based object store cache
#[derive(Debug, Clone, educe::Educe)]
#[educe(Default)]
pub struct FoyerCacheConfig {
    #[educe(Default = 134_217_728)] // 128MB
    pub memory_size_bytes: usize,
    #[educe(Default = 107_374_182_400)] // 100GB
    pub disk_size_bytes: usize,
    #[educe(Default(expression = Duration::from_secs(86_400)))] // 24h
    pub ttl: Duration,
    #[educe(Default(expression = PathBuf::from("/tmp/timefusion_cache")))]
    pub cache_dir: PathBuf,
    #[educe(Default = 8)]
    pub shards: usize,
    #[educe(Default = 16_777_216)] // 16MB - good for Parquet files
    pub file_size_bytes: usize,
    #[educe(Default = true)]
    pub enable_stats: bool,
    /// Size hint for reading parquet metadata from the end of files
    #[educe(Default = 1_048_576)] // 1MB - typical size for parquet metadata
    pub parquet_metadata_size_hint: usize,
    /// Memory size for metadata cache in bytes
    #[educe(Default = 67_108_864)] // 64MB
    pub metadata_memory_size_bytes: usize,
    /// Disk size for metadata cache in bytes
    #[educe(Default = 536_870_912)] // 512MB
    pub metadata_disk_size_bytes: usize,
    /// Number of shards for metadata cache — fewer than the data cache needs.
    #[educe(Default = 4)]
    pub metadata_shards: usize,
    /// Optional extra cap on bytes buffered to warm the cache inline from a
    /// multipart write (see `CachingMultipartUpload`). Always bounded by
    /// `block_size_bytes`; 0 = bound only by the block size (the default).
    pub warm_inline_max_bytes: usize,
    /// Per-upload cap on bytes teed into heap by `CachingMultipartUpload`.
    /// Sized for flush outputs; big compaction outputs skip the tee and are
    /// warmed post-commit via the read path. 0 = bounded only by the block size.
    #[educe(Default = 33_554_432)] // 32MB — flush-sized files only
    pub write_capture_max_bytes: usize,
    /// Process-wide budget for in-flight write-capture buffers. 0 = unbudgeted.
    #[educe(Default = 268_435_456)] // 256MB process-wide (8 x the per-upload cap)
    pub write_capture_budget_bytes: usize,
    /// Disk block size for the main data cache — foyer's eviction unit and the
    /// hard cap on the largest entry that can persist to disk. Must be >= the
    /// largest file we want cached (compaction target size).
    #[educe(Default = 268_435_456)] // 256MB — fits 128MB compaction outputs
    pub block_size_bytes: usize,
    /// Entries larger than this are inserted disk-only (`Location::OnDisk`) so
    /// they don't evict the hot L1 working set. 0 = always use L1.
    #[educe(Default = 16_777_216)] // 16MB
    pub l1_max_entry_bytes: usize,
    /// Don't admit writes whose `date=` partition is older than this many days.
    /// 0 = no age limit.
    #[educe(Default = 8)]
    pub cache_recent_days: usize,
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
        // Hard floor of 2GiB: the fleet holds compacted files up to ~1.5GB
        // (historical consolidation outputs), and foyer's max entry is
        // `block_size - blob_index_size` — with the old 512MB floor every file
        // above it silently never persisted to the disk tier, re-fetched from
        // R2 on each scan.
        let block_size_bytes = cfg.cache.block_size_bytes().max(optimize_target.saturating_mul(2)).max(2 << 30);
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
            write_capture_max_bytes: cfg.cache.write_capture_max_bytes(),
            write_capture_budget_bytes: cfg.cache.write_capture_budget_bytes(),
            block_size_bytes,
            l1_max_entry_bytes: cfg.cache.l1_max_entry_bytes(),
            cache_recent_days: cfg.cache.timefusion_cache_recent_days,
        }
    }

    /// Create a test configuration with sensible defaults for testing
    /// The name parameter is used to create unique cache directories
    pub fn test_config(name: &str) -> Self {
        Self {
            memory_size_bytes: 10 * 1024 * 1024, // 10MB
            disk_size_bytes: 50 * 1024 * 1024,   // 50MB
            ttl: Duration::from_secs(300),
            // Per-process dir: foyer's disk tier outlives the run, so a fixed
            // path leaks entries into the NEXT `cargo test` and any absence
            // assertion then passes only on a clean /tmp.
            cache_dir: PathBuf::from(format!("/tmp/test_foyer_{}_{}", name, std::process::id())),
            shards: 2,
            file_size_bytes: 1024 * 1024, // 1MB
            enable_stats: true,
            parquet_metadata_size_hint: 1_048_576,        // 1MB
            metadata_memory_size_bytes: 10 * 1024 * 1024, // 10MB for tests
            metadata_disk_size_bytes: 50 * 1024 * 1024,   // 50MB for tests
            metadata_shards: 2,
            warm_inline_max_bytes: 0,          // bound by block size
            write_capture_max_bytes: 0,        // bound by block size in tests
            write_capture_budget_bytes: 0,     // unbudgeted in tests
            block_size_bytes: 4 * 1024 * 1024, // 4MB — must be <= test disk size
            l1_max_entry_bytes: 1024 * 1024,   // 1MB
            cache_recent_days: 0,              // no age limit in tests (avoid date flakiness)
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
    pub hits: u64,
    pub misses: u64,
    pub range_hits: u64,
    pub range_misses: u64,
    pub bytes_served: u64,
    pub inner_bytes_read: u64,
    pub range_bytes_read: u64,
    pub ttl_expirations: u64,
    pub inner_gets: u64,
    pub inner_puts: u64,
}

/// Combined statistics for both caches
#[derive(Debug, Default, Clone)]
pub struct CombinedCacheStats {
    pub main: CacheStats,
    pub metadata: CacheStats,
}

#[derive(Debug, Default, Clone)]
pub struct FoyerRuntimeStats {
    pub stats: CombinedCacheStats,
    pub memory_size_bytes: usize,
    pub disk_size_bytes: usize,
    pub ttl_seconds: u64,
    pub l1_max_entry_bytes: usize,
    pub block_size_bytes: usize,
    pub cache_recent_days: usize,
    pub cache_dir: PathBuf,
    pub metadata_memory_size_bytes: usize,
    pub metadata_disk_size_bytes: usize,
    /// Main-cache L1 payload bytes currently resident in memory.
    pub l1_used_bytes: usize,
    /// Physical bytes allocated by the main cache device, excluding metadata.
    pub l2_used_bytes: u64,
    /// Entries currently tracked by the main cache's L1 index.
    pub entry_count: usize,
    /// Main-cache L1 capacity evictions since process start.
    pub evictions: u64,
}

impl CacheStats {
    fn log(&self) {
        let hit_rate = if self.hits + self.misses > 0 { (self.hits as f64 / (self.hits + self.misses) as f64) * 100.0 } else { 0.0 };
        info!(
            "Foyer cache stats - Hit rate: {:.2}%, Hits: {}, Misses: {}, Range hits: {}, Range misses: {}, Bytes served: {}, Inner bytes read: {}, Range bytes read: {}, TTL expirations: {}, Inner gets: {}, Inner puts: {}",
            hit_rate,
            self.hits,
            self.misses,
            self.range_hits,
            self.range_misses,
            self.bytes_served,
            self.inner_bytes_read,
            self.range_bytes_read,
            self.ttl_expirations,
            self.inner_gets,
            self.inner_puts
        );
    }
}

type FoyerCache = Arc<HybridCache<String, CacheValue>>;
type CacheEntry = foyer::HybridCacheEntry<String, CacheValue>;
type StatsRef = Arc<RwLock<CacheStats>>;

async fn bump(stats: &StatsRef, f: impl FnOnce(&mut CacheStats)) {
    f(&mut *stats.write().await);
}

/// A cache hit served from `stats`' tier.
async fn record_hit(stats: &StatsRef, bytes_served: u64) {
    bump(stats, |s| {
        s.hits += 1;
        s.bytes_served += bytes_served;
    })
    .await;
}

/// A miss on `stats`' tier that triggers an inner-store fetch.
async fn record_miss_with_fetch(stats: &StatsRef) {
    bump(stats, |s| {
        s.misses += 1;
        s.inner_gets += 1;
    })
    .await;
}

async fn record_range_hit(stats: &StatsRef, bytes_served: u64) {
    bump(stats, |s| {
        s.hits += 1;
        s.range_hits += 1;
        s.bytes_served += bytes_served;
    })
    .await;
}

async fn record_range_miss(stats: &StatsRef, bytes_read: u64) {
    bump(stats, |s| {
        s.range_misses += 1;
        s.range_bytes_read += bytes_read;
    })
    .await;
}

async fn combined_stats(main: &StatsRef, metadata: &StatsRef) -> CombinedCacheStats {
    CombinedCacheStats { main: main.read().await.clone(), metadata: metadata.read().await.clone() }
}

/// Lock-free snapshot: a contended lock yields default counters rather than
/// blocking a diagnostics caller.
fn try_combined_stats(main: &StatsRef, metadata: &StatsRef) -> CombinedCacheStats {
    let snap = |s: &StatsRef| s.try_read().map(|g| g.clone()).unwrap_or_default();
    CombinedCacheStats { main: snap(main), metadata: snap(metadata) }
}

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

/// Dedicated runtime for foyer's internal fetch/IO tasks, shared by every
/// cache instance in the process (2 threads, lives for the process).
///
/// Why not the caller's runtime: `RawCache::get_or_fetch_inner` holds its
/// inflight-manager mutex across `Spawner::spawn`. On a live runtime that's
/// fine, but on a runtime that is shutting down tokio cancels the spawned
/// task INLINE — `RawFetch::drop` then re-locks the same non-reentrant mutex
/// on the same thread and deadlocks. Any in-flight cache get racing runtime
/// teardown (test end, prod stop-grace) could hang forever; the e2e restart
/// tests hit it deterministically (3×600s timeouts, 2026-08-03). A dedicated
/// runtime never dies under foyer, so the inline-cancel path can't trigger.
fn foyer_spawner() -> foyer::Spawner {
    static SPAWNER: std::sync::OnceLock<foyer::Spawner> = std::sync::OnceLock::new();
    SPAWNER
        .get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("foyer").enable_all().build().expect("build foyer runtime");
            foyer::Spawner::from(rt)
        })
        .clone()
}

/// Build one hybrid (memory + disk) cache tier. The data and metadata caches
/// differ only in their sizes and in the eviction listener, so they share this.
async fn build_hybrid_cache(
    dir: &std::path::Path, memory_bytes: usize, shards: usize, disk_bytes: usize, block_size: usize,
    listener: Option<Arc<dyn foyer::EventListener<Key = String, Value = CacheValue>>>,
) -> anyhow::Result<FoyerCache> {
    let builder = HybridCacheBuilder::new().with_policy(HybridCachePolicy::WriteOnInsertion);
    let builder = listener.into_iter().fold(builder, |b, l| b.with_event_listener(l));
    Ok(Arc::new(
        builder
            .memory(memory_bytes)
            .with_shards(shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_spawner(foyer_spawner())
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(FsDeviceBuilder::new(dir).with_capacity(disk_bytes).build()?).with_block_size(block_size))
            .build()
            .await?,
    ))
}

/// Shared Foyer cache that can be used across multiple object stores
#[derive(Debug)]
pub struct SharedFoyerCache {
    cache: FoyerCache,
    metadata_cache: FoyerCache,
    stats: StatsRef,
    metadata_stats: StatsRef,
    config: FoyerCacheConfig,
    evictions: Arc<AtomicU64>,
}

struct EvictionCounter(Arc<AtomicU64>);

impl foyer::EventListener for EvictionCounter {
    type Key = String;
    type Value = CacheValue;

    fn on_leave(&self, event: foyer::Event, _: &Self::Key, _: &Self::Value) {
        if event == foyer::Event::Evict {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
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
        // A sub-5-minute TTL is almost certainly a debug leftover (a real one
        // shipped in .env once): every idle partition re-cold-starts after
        // expiry, silently erasing the entire warm-path win.
        if config.ttl < std::time::Duration::from_secs(300) {
            warn!(
                "Foyer TTL is only {}s — cached footers expire between queries; set TIMEFUSION_FOYER_TTL_SECONDS higher unless debugging",
                config.ttl.as_secs()
            );
        }

        std::fs::create_dir_all(&config.cache_dir)?;
        let metadata_cache_dir = config.cache_dir.join("metadata");
        std::fs::create_dir_all(&metadata_cache_dir)?;

        // The main data cache wants a block big enough to hold full compaction
        // outputs (128MB) so they persist, capped to the device so it can't wedge.
        let data_block_size = capped_block_size(config.block_size_bytes, config.disk_size_bytes);
        let evictions = Arc::new(AtomicU64::new(0));

        let cache = build_hybrid_cache(
            &config.cache_dir,
            config.memory_size_bytes,
            config.shards,
            config.disk_size_bytes,
            data_block_size,
            Some(Arc::new(EvictionCounter(evictions.clone()))),
        )
        .await?;
        let metadata_cache = build_hybrid_cache(
            &metadata_cache_dir,
            config.metadata_memory_size_bytes,
            config.metadata_shards,
            config.metadata_disk_size_bytes,
            capped_block_size(config.file_size_bytes, config.metadata_disk_size_bytes),
            None,
        )
        .await?;

        Ok(Self {
            cache,
            metadata_cache,
            stats: Arc::new(RwLock::new(CacheStats::default())),
            metadata_stats: Arc::new(RwLock::new(CacheStats::default())),
            config,
            evictions,
        })
    }

    pub async fn get_stats(&self) -> CombinedCacheStats {
        combined_stats(&self.stats, &self.metadata_stats).await
    }

    pub fn try_get_stats(&self) -> CombinedCacheStats {
        try_combined_stats(&self.stats, &self.metadata_stats)
    }

    pub fn runtime_stats(&self) -> FoyerRuntimeStats {
        FoyerRuntimeStats {
            stats: self.try_get_stats(),
            memory_size_bytes: self.config.memory_size_bytes,
            disk_size_bytes: self.config.disk_size_bytes,
            ttl_seconds: self.config.ttl.as_secs(),
            l1_max_entry_bytes: self.config.l1_max_entry_bytes,
            block_size_bytes: self.config.block_size_bytes,
            cache_recent_days: self.config.cache_recent_days,
            cache_dir: self.config.cache_dir.clone(),
            metadata_memory_size_bytes: self.config.metadata_memory_size_bytes,
            metadata_disk_size_bytes: self.config.metadata_disk_size_bytes,
            l1_used_bytes: self.cache.memory().usage(),
            // A recursive `read_dir` + `metadata()` over the whole L2 cache
            // directory — tens of thousands of files at the configured disk
            // size — and `timefusion_stats` is a pgwire query, so this runs on
            // a runtime worker every time anything reads the stats table.
            l2_used_bytes: crate::support::without_blocking_the_worker(|| allocated_bytes(&self.config.cache_dir)),
            entry_count: self.cache.memory().entries(),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    pub async fn log_stats(&self) {
        info!("Main cache stats:");
        self.stats.read().await.log();
        info!("Metadata cache stats:");
        self.metadata_stats.read().await.log();
    }

    /// Close the caches, bounded by `deadline`. Foyer's `close()` flushes
    /// in-memory entries to disk; on a large cache / slow disk this can run for
    /// minutes (prod 2026-07-13: 377MB → 8.5min), and because it blocks process
    /// exit it also blocks `wal.lock` release, stalling the incoming instance of
    /// a redeploy. The disk cache is a rebuildable READ cache, so abandoning an
    /// in-progress flush loses only warmth, never durable data — prioritize
    /// releasing the WAL lock over cache completeness.
    /// How long Foyer's close may take before it is abandoned. Small on purpose:
    /// see the note in `shutdown_by`.
    const CLOSE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> anyhow::Result<()> {
        info!("Shutting down Foyer cache...");
        self.log_stats().await;

        // Foyer close gets a SMALL slice of the grace, not all of it.
        //
        // An unclean kill leaves the disk cache in a state where `close()` never
        // completes. Bounded only by `deadline`, the next shutdown then spends
        // the ENTIRE stop grace here (measured 2026-08-02: 70.003s against a 70s
        // grace) and leaves nothing for the WAL cursor snapshot — so the process
        // is SIGKILLed, `clean_shutdown=false`, and the next boot pays a full
        // blocking WAL replay, which can overrun and kill uncleanly again. One
        // dirty kill otherwise makes every later restart slow and dirty.
        //
        // Everything at stake here is rebuildable cache warmth, so it is the
        // cheapest thing in shutdown to abandon. See issue #82, where the same
        // close overran "for minutes" in prod and stalled the wal.lock release.
        let close_deadline = deadline.min(tokio::time::Instant::now() + Self::CLOSE_BUDGET);
        info!("Closing Foyer caches...");
        match tokio::time::timeout_at(close_deadline, async {
            self.cache.close().await?;
            self.metadata_cache.close().await
        })
        .await
        {
            Ok(res) => res?,
            Err(_) => warn!(
                "Foyer close exceeded its {:?} budget — abandoning disk-cache flush so the rest of shutdown keeps its share of the grace (cache warmth lost, no data loss)",
                Self::CLOSE_BUDGET
            ),
        }
        Ok(())
    }

    /// Invalidate checkpoint cache for a given table URI
    pub fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        let key = last_checkpoint_key(table_uri);
        info!("Invalidating _last_checkpoint cache for table: {}", key);
        self.cache.remove(&key);
    }

    /// Best-effort eviction of a main (full-file) cache entry by its key — the
    /// relativized object path, matching `make_cache_key`. Used to proactively
    /// drop the (now dead) full-file bytes of a file a compaction tombstoned,
    /// instead of waiting for VACUUM / TTL / LRU to reclaim them.
    pub fn evict_data_entry(&self, key: &str) {
        self.cache.remove(key);
    }

    /// Non-populating existence probe on the main cache, keyed like
    /// `evict_data_entry` (the object-store-relative path). Checks L1 plus the
    /// disk bloom filter, so it costs no IO and never promotes an entry — which
    /// is what makes the pre-drain confirm free for already-captured files.
    /// May false-positive on a hash collision (foyer's contract): the cost is
    /// one un-warmed file, i.e. a later cache miss.
    pub fn contains_data(&self, key: &str) -> bool {
        self.cache.contains(key)
    }
}

/// Strip the `scheme://` prefix and trailing slashes from a table URI, yielding
/// the bare table path used to build `_delta_log` cache keys.
fn table_path_from_uri(table_uri: &str) -> &str {
    let table_path = table_uri.find("://").map(|idx| &table_uri[idx + 3..]).unwrap_or(table_uri);
    table_path.trim_end_matches('/')
}

/// Cache key (and object path) of a table's mutable `_last_checkpoint` file.
fn last_checkpoint_key(table_uri: &str) -> String {
    format!("{}/_delta_log/_last_checkpoint", table_path_from_uri(table_uri))
}

/// Whether a GET carries no precondition — the only shape the cache can serve,
/// since a cached body says nothing about the current etag/mtime.
fn is_unconditional(o: &GetOptions) -> bool {
    o.if_match.is_none() && o.if_none_match.is_none() && o.if_modified_since.is_none() && o.if_unmodified_since.is_none()
}

/// Whether a cached object is a Parquet data file (vs. Delta log / checkpoint
/// metadata), which governs TTL and metadata-cache behavior.
fn is_parquet_file(location: &Path) -> bool {
    location.as_ref().ends_with(".parquet")
}

/// Best-effort: warm the Parquet header and footer of `location` into the cache.
/// The header probe is deliberately metadata too: a cold `0..8` Parquet magic
/// read must not be classified as data and trigger a full-object fallback.
pub async fn warm_parquet_metadata(store: &dyn ObjectStore, location: &Path, metadata_size_hint: u64) -> bool {
    let header = store.get_opts(location, GetOptions { range: Some(GetRange::Bounded(0..8)), ..Default::default() }).await.is_ok();
    warm_footer(store, location, metadata_size_hint).await && header
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
    let opts = GetOptions { range: Some(GetRange::Suffix(metadata_size_hint.max(1))), ..Default::default() };
    match store.get_opts(location, opts).await {
        Ok(result) => result.bytes().await.is_ok(),
        Err(_) => warm_footer_via_head(store, location, metadata_size_hint).await,
    }
}

/// HEAD + bounded-GET fallback for [`warm_footer`] when the store doesn't
/// support suffix ranges. Two round-trips, but always correct.
async fn warm_footer_via_head(store: &dyn ObjectStore, location: &Path, metadata_size_hint: u64) -> bool {
    let Ok(ObjectMeta { size, .. }) = store.head(location).await else { return false };
    if size == 0 {
        return false;
    }
    let start = size.saturating_sub(metadata_size_hint.max(1));
    let opts = GetOptions { range: Some(GetRange::Bounded(start..size)), ..Default::default() };
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

tokio::task_local! {
    /// Set for the duration of a scan that must not pollute the cache.
    static SCAN_BYPASS: bool;
}

/// Run `fut` with cache POPULATION suppressed (lookups still hit normally) when
/// `bypass` is set — scan-resistant admission, so a wide historical scan can't
/// evict the hot tail it will never re-read (ClickHouse's big-scan bypass).
///
/// Task-local, so it covers everything awaited inside `fut` but NOT work the
/// inner store hands to a separate task. That's the intended blast radius: the
/// gated scan's own fetches are what read GBs of cold parquet.
pub fn scan_bypass_scope<F: std::future::Future>(bypass: bool, fut: F) -> impl std::future::Future<Output = F::Output> {
    SCAN_BYPASS.scope(bypass, fut)
}

/// Cap on [`FoyerObjectStoreCache::repeat_sighting`]'s key set. ~100k keys of
/// path-length strings is a few MB — small against a 4 GB L1, and large enough
/// that a whole dashboard's working set fits without a reset mid-refresh.
const BYPASS_SEEN_MAX: usize = 100_000;

pub fn bypass_active() -> bool {
    SCAN_BYPASS.try_with(|b| *b).unwrap_or(false)
}

/// [`warm_full`] that skips the GET when the cache already holds the object —
/// on the flush path most files were captured during their own upload, so the
/// Influx-oracle confirm costs one free existence probe and only fetches the
/// gap write-capture left (over its per-upload cap or the process budget).
/// Returns `true` iff bytes were actually fetched.
/// `cache_key` is the key inserts actually used — bucket-relative, NOT the
/// table-relative `location` the (prefixed) `store` GETs by. Probing with
/// `location` made this always-miss in prod (every confirm re-downloaded its
/// own upload).
pub async fn warm_full_if_absent(store: &dyn ObjectStore, shared: &SharedFoyerCache, location: &Path, cache_key: &str) -> bool {
    !shared.contains_data(cache_key) && warm_full(store, location).await
}

/// Parse the `date=YYYY-MM-DD` Hive partition segment from `s`. `None` for
/// strings without a parseable segment (Delta log, checkpoints, undated paths).
/// Single source of truth for the magic `date=` offset/format, reused by the
/// recency window here and the prefilter coverage gate in `database.rs`.
pub fn date_partition_of(s: &str) -> Option<chrono::NaiveDate> {
    s.find("date=").and_then(|i| s.get(i + 5..i + 15)).and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok())
}

/// Parse the `date=YYYY-MM-DD` partition segment from `s` and return whether it
/// is on or after `cutoff`. Strings without a parseable date segment (Delta log,
/// checkpoints) are always within the window; a `None` cutoff means no age limit.
///
/// Shared by the cache-admission window here and the compaction-warm recency
/// filter in `database.rs` so the two parsers can't drift.
pub fn date_partition_within(s: &str, cutoff: Option<chrono::NaiveDate>) -> bool {
    let Some(cutoff) = cutoff else { return true };
    date_partition_of(s).is_none_or(|date| date >= cutoff)
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
    ObjectMeta { location, last_modified: Utc::now(), size, e_tag: result.e_tag.clone(), version: result.version.clone() }
}

/// Foyer-based hybrid cache implementation for object store
#[derive(Clone, derive_more::Display, derive_more::Debug)]
#[display("FoyerHybridCachedObjectStore({})", inner)]
#[debug("FoyerHybridCachedObjectStore {{ inner: {} }}", inner)]
pub struct FoyerObjectStoreCache {
    inner: Arc<dyn ObjectStore>,
    cache: FoyerCache,
    metadata_cache: FoyerCache,
    stats: StatsRef,
    metadata_stats: StatsRef,
    config: FoyerCacheConfig,
    refreshing: Arc<DashSet<String>>,
    main_fetch_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    background_tasks: Arc<Mutex<JoinSet<()>>>,
    /// Keys a bypassed (wide) scan has asked to admit once already. See
    /// [`Self::repeat_sighting`].
    bypass_seen: Arc<DashSet<String>>,
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
            main_fetch_locks: Arc::new(DashMap::new()),
            bypass_seen: Arc::new(DashSet::new()),
            background_tasks: Arc::new(Mutex::new(JoinSet::new())),
        }
    }

    /// Check if a path is the mutable _last_checkpoint file
    fn is_last_checkpoint(location: &Path) -> bool {
        location.as_ref().contains("_delta_log/_last_checkpoint")
    }

    /// Explicitly invalidate checkpoint cache for a given table
    pub async fn invalidate_checkpoint_cache(&self, table_uri: &str) {
        let key = last_checkpoint_key(table_uri);
        info!("Explicitly invalidating and refreshing _last_checkpoint cache for table: {}", key);
        self.cache.remove(&key);

        // Immediately fetch and cache the new version.
        if let Ok(get_result) = self.inner.get(&Path::from(key.as_str())).await {
            let (data, meta) = Self::collect_payload(get_result).await;
            if !data.is_empty() {
                self.cache.insert(key, CacheValue::new(data, meta));
                debug!("Proactively refreshed _last_checkpoint cache after invalidation");
            }
        }
    }

    pub async fn new(inner: Arc<dyn ObjectStore>, config: FoyerCacheConfig) -> anyhow::Result<Self> {
        let shared_cache = SharedFoyerCache::new(config).await?;
        Ok(Self::new_with_shared_cache(inner, &shared_cache))
    }

    /// Record a metadata-tier hit and stamp the span fields that always
    /// accompany one.
    async fn record_meta_hit(&self, span: &tracing::Span, bytes_served: u64) {
        record_hit(&self.metadata_stats, bytes_served).await;
        span.record("cache_hit", true);
        span.record("is_metadata", true);
    }

    /// Spawn a background (best-effort) task, registering it for the shutdown
    /// join when `background_tasks` isn't contended. A contended lock only means
    /// the task detaches — it still runs, it just isn't awaited on shutdown, so
    /// `background_tasks` is not an exhaustive registry.
    fn spawn_tracked(&self, fut: impl std::future::Future<Output = ()> + Send + 'static) {
        let handle = tokio::spawn(fut);
        if let Ok(mut tasks) = self.background_tasks.try_lock() {
            tasks.spawn(async move {
                let _ = handle.await;
            });
        }
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
        // Range keys can't be enumerated, so drop the tail ranges a reader is
        // most likely to have cached; the rest ages out with the TTL.
        let Ok(file_meta) = self.inner.head(location).await else { return };
        let file_size = file_meta.size;
        [8, 1024, 4096, 8192, self.config.parquet_metadata_size_hint as u64]
            .into_iter()
            .filter(|&offset| offset < file_size)
            .for_each(|offset| self.metadata_cache.remove(&Self::make_range_cache_key(location, &(file_size - offset..file_size))));

        debug!("Invalidated metadata cache entries for: {}", location);
    }

    /// One-shot `GetResult` over already-materialized bytes, reporting the
    /// absolute range they occupy in the object (`start` = 0 for a full read).
    fn make_get_result_at(data: Bytes, meta: ObjectMeta, attributes: Attributes, start: u64) -> GetResult {
        let data_len = data.len() as u64;
        GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(async move { Ok(data) }))),
            meta,
            attributes,
            range: start..start + data_len,
        }
    }

    fn make_get_result(data: Bytes, meta: ObjectMeta) -> GetResult {
        Self::make_get_result_at(data, meta, Attributes::new(), 0)
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
        combined_stats(&self.stats, &self.metadata_stats).await
    }

    pub fn try_get_stats(&self) -> CombinedCacheStats {
        try_combined_stats(&self.stats, &self.metadata_stats)
    }

    pub async fn reset_stats(&self) {
        *self.stats.write().await = CacheStats::default();
        *self.metadata_stats.write().await = CacheStats::default();
    }
}

impl FoyerObjectStoreCache {
    /// Read a payload body into bytes, propagating IO/stream errors.
    async fn read_payload(payload: GetResultPayload) -> ObjectStoreResult<Vec<u8>> {
        use futures::TryStreamExt;
        match payload {
            GetResultPayload::Stream(s) => Ok(s.try_collect::<Vec<Bytes>>().await?.concat()),
            GetResultPayload::File(mut file, _) => {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).map_err(|e| object_store::Error::Generic { store: "cache", source: Box::new(e) })?;
                Ok(buf)
            }
        }
    }

    /// Best-effort [`Self::read_payload`] for warm paths: a failed read yields
    /// empty bytes, which every caller treats as "nothing to cache".
    async fn collect_payload(result: GetResult) -> (Vec<u8>, ObjectMeta) {
        let meta = result.meta.clone();
        (Self::read_payload(result.payload).await.unwrap_or_default(), meta)
    }

    /// Serve a live main-cache entry: record the hit (stats + span) and
    /// materialize the body.
    async fn serve_hit(&self, span: &tracing::Span, value: &CacheValue) -> GetResult {
        record_hit(&self.stats, value.data.len() as u64).await;
        span.record("cache_hit", true);
        Self::make_get_result(value.data.clone(), value.meta.clone())
    }

    #[instrument(
        name = "foyer_cache.get",
        skip_all,
        fields(
            location = %location,
            cache_hit = Empty,
            cache_fetch_leader = Empty,
            cache_waited_for_inflight = Empty,
            cache_entry_bytes = Empty,
            cache_admission = Empty,
            is_checkpoint = Self::is_last_checkpoint(location),
        )
    )]
    async fn get_cached(&self, location: &Path) -> ObjectStoreResult<GetResult> {
        let span = tracing::Span::current();
        let cache_key = Self::make_cache_key(location);

        let ttl = self.config.ttl;

        // Try cache first
        if let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();

            // Special handling for _last_checkpoint: stale-while-revalidate
            if Self::is_last_checkpoint(location) && !value.is_expired(ttl) {
                let result = self.serve_hit(&span, value).await;
                let age_millis = value.age_millis();
                // Stale (>5s) checkpoints are served immediately and refreshed
                // behind the request; one refresh in flight per key.
                if age_millis > 5000 && self.refreshing.insert(cache_key.clone()) {
                    let (inner, cache, refreshing, location, key) =
                        (self.inner.clone(), self.cache.clone(), self.refreshing.clone(), location.clone(), cache_key.clone());
                    self.spawn_tracked(async move {
                        debug!("Background refresh for _last_checkpoint: {}", location);
                        if let Ok(result) = inner.get(&location).await {
                            let (data, meta) = Self::collect_payload(result).await;
                            if !data.is_empty() {
                                cache.insert(key.clone(), CacheValue::new(data, meta));
                            }
                        }
                        refreshing.remove(&key);
                    });
                }
                debug!("Foyer cache HIT (_last_checkpoint) for: {} (age: {}ms)", location, age_millis);
                return Ok(result); // the cached value is always served immediately
            }

            // Regular cache expiration check for non-checkpoint files
            if value.is_expired(ttl) {
                bump(&self.stats, |s| s.ttl_expirations += 1).await;
                self.cache.remove(&cache_key);
                debug!("Foyer cache EXPIRED for: {} (TTL: {}s, age: {}ms)", location, ttl.as_secs(), value.age_millis());
            } else {
                let result = self.serve_hit(&span, value).await;
                debug!(
                    "Foyer cache HIT for: {} (avoiding S3 access, parquet={}, TTL={}s, age={}ms, size={} bytes)",
                    location,
                    is_parquet_file(location),
                    ttl.as_secs(),
                    value.age_millis(),
                    value.data.len()
                );
                self.maybe_touch(&self.cache, &cache_key, entry.clone(), self.config.l1_max_entry_bytes);
                return Ok(result);
            }
        }

        let fetch_lock = self.main_fetch_locks.entry(cache_key.clone()).or_insert_with(|| Arc::new(Mutex::new(()))).clone();
        let waited_for_inflight = fetch_lock.try_lock().is_err();
        let fetch_guard = fetch_lock.lock().await;
        span.record("cache_waited_for_inflight", waited_for_inflight);

        // A concurrent leader may have populated the object while we waited.
        if waited_for_inflight && let Ok(Some(entry)) = self.cache.get(&cache_key).await {
            let value = entry.value();
            if !value.is_expired(ttl) {
                let result = self.serve_hit(&span, value).await;
                span.record("cache_entry_bytes", value.data.len() as i64);
                return Ok(result);
            }
        }

        // Cache miss - fetch from inner store
        span.record("cache_hit", false);
        span.record("cache_fetch_leader", true);
        record_miss_with_fetch(&self.stats).await;
        let is_parquet = is_parquet_file(location);
        debug!("Foyer cache MISS for: {} (fetching from S3, parquet={}, TTL={}s)", location, is_parquet, ttl.as_secs());

        let fetch_result = async {
            let start_time = std::time::Instant::now();
            let inner_span = tracing::trace_span!(parent: &span, "s3.get", location = %location);
            let result = self.inner.get(location).instrument(inner_span).await?;
            let duration = start_time.elapsed();

            debug!("S3 GET request: {} (size: {} bytes, duration: {}ms, parquet: {})", location, result.meta.size, duration.as_millis(), is_parquet);

            let data = Self::read_payload(result.payload).await?;

            bump(&self.stats, |s| s.inner_bytes_read += data.len() as u64).await;
            span.record("cache_entry_bytes", data.len() as i64);
            span.record("cache_admission", if data.len() > self.config.l1_max_entry_bytes { "disk" } else { "memory" });
            let data = Bytes::from(data);
            self.insert_main_value(location, CacheValue::new(data.clone(), result.meta.clone()));
            Ok(Self::make_get_result(data, result.meta))
        }
        .await;

        drop(fetch_guard);
        self.main_fetch_locks.remove_if(&cache_key, |_, lock| Arc::ptr_eq(lock, &fetch_lock));
        fetch_result
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
        let mut range_cache_key = Self::make_range_cache_key(location, &range);
        let mut fetch_range = range.clone();
        let mut response_slice = None;
        let mut range_meta = None;

        let full_cache_key = Self::make_cache_key(location);
        if let Ok(Some(entry)) = self.cache.get(&full_cache_key).await {
            let value = entry.value();
            if !value.is_expired(self.config.ttl) && range.end <= value.data.len() as u64 {
                record_range_hit(&self.stats, range.end - range.start).await;
                span.record("cache_hit", true);
                debug!(
                    "Foyer cache HIT (full file) for range: {} (range: {}..{}, size: {} bytes, parquet={}, age={}ms)",
                    location,
                    range.start,
                    range.end,
                    range.end - range.start,
                    is_parquet,
                    value.age_millis()
                );
                // Zero-copy, second attempt. 2bb5e85 pinned the cache ENTRY
                // (`Bytes::from_owner(entry)`) and stalled foyer admission
                // under scan load; this slice shares only the `Bytes` buffer,
                // so eviction and accounting proceed regardless — a live slice
                // at worst delays freeing one evicted file body until its
                // reader drops.
                let sliced = value.data.slice(range.start as usize..range.end as usize);
                self.maybe_touch(&self.cache, &full_cache_key, entry.clone(), self.config.l1_max_entry_bytes);
                return Ok(sliced);
            }
        }

        // Full-file coverage is preferred, but cache the exact coalesced data
        // ranges DataFusion repeatedly requests as a resilient second line.
        // This avoids making every dashboard refresh wait on R2 when upload or
        // post-commit full warming has not converged yet. Range entries live in
        // the main tier (not the small metadata tier) and remain subject to the
        // recent-window and wide-scan admission policies.
        if is_parquet && let Ok(Some(entry)) = self.cache.get(&range_cache_key).await {
            let value = entry.value();
            if !value.is_expired(self.config.ttl) {
                record_range_hit(&self.stats, value.data.len() as u64).await;
                span.record("cache_hit", true);
                let data = value.data.clone();
                self.maybe_touch(&self.cache, &range_cache_key, entry.clone(), self.config.l1_max_entry_bytes);
                return Ok(data);
            }
            bump(&self.stats, |s| s.ttl_expirations += 1).await;
            self.cache.remove(&range_cache_key);
        }

        // For Parquet files, implement smart caching based on the range
        if is_parquet {
            // Probe the metadata range cache *before* any HEAD: its key is just
            // (location, range), so a steady-state footer read served from cache
            // pays zero S3 round-trips. Data ranges aren't stored here and fall
            // through to the size-based classification below.
            if let Ok(Some(entry)) = self.metadata_cache.get(&range_cache_key).await {
                let value = entry.value();
                if !value.is_expired(self.config.ttl) {
                    self.record_meta_hit(&span, value.data.len() as u64).await;
                    debug!(
                        "Metadata cache HIT for: {} (range: {}..{}, size: {} bytes, age={}ms)",
                        location,
                        range.start,
                        range.end,
                        value.data.len(),
                        value.age_millis()
                    );
                    let sliced = value.data.clone();
                    // l1_max=0: metadata entries are tiny, always keep in L1.
                    self.maybe_touch(&self.metadata_cache, &range_cache_key, entry.clone(), 0);
                    return Ok(sliced);
                }
            }

            // Range-cache miss: we need the file size to classify the request and
            // to stamp the cached range's meta. Use the cached ObjectMeta
            // (immutable Delta files) so this HEAD is paid at most once per file.
            let file_meta = self.head_cached(location).await.inspect_err(|e| debug!("Failed to get metadata for {}: {}", location, e))?;
            range_meta = Some(file_meta.clone());

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
                            self.record_meta_hit(&span, range.end - range.start).await;
                            // Distinct from the exact-key HIT log above so cache-key
                            // alignment is diagnosable on a new deployment.
                            debug!("Metadata cache HIT (containment {}..{}) for: {} (range: {}..{})", candidate, file_size, location, range.start, range.end);
                            let sliced = value.data.slice(s..e);
                            self.maybe_touch(&self.metadata_cache, &key, entry.clone(), 0);
                            return Ok(sliced);
                        }
                    }
                }
            }

            // Check if this is likely a metadata request (reading from near the end of the file)
            // A file no bigger than the metadata hint is FULL-FILE class, not
            // metadata: for such a file every range satisfies the footer
            // proximity test, so the old test routed all its data pages down
            // the per-(file,range) metadata path — one R2 GET per coalesced
            // range per query, forever, and the main tier read as unused
            // (prod 2026-08-03: hits=0/misses=0 with 1703 warmed entries while
            // a sea of sub-1MB flush files paid per-range GETs). Falling
            // through caches the whole body in the main tier with ONE GET.
            let is_metadata_request = range.end <= 8 || (file_size > metadata_size_hint && range.start >= file_size.saturating_sub(metadata_size_hint));
            span.record("is_metadata", is_metadata_request);

            if is_metadata_request {
                // Cache miss for metadata range - fetch just the range
                span.record("cache_hit", false);
                record_miss_with_fetch(&self.metadata_stats).await;
                debug!("Metadata cache MISS for Parquet: {} (range: {}..{}, file_size: {})", location, range.start, range.end, file_size);

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

                bump(&self.metadata_stats, |s| s.inner_bytes_read += data.len() as u64).await;
                self.admit_range(location, range_cache_key, &data, &file_meta);

                return Ok(data);
            }

            // A small file (≤ the L1 entry cap) is cheap enough to cache whole
            // inline. Large files are warmed only by upload capture and the
            // post-commit/restart warmer. Never start a full-object download
            // from a query miss: in production one 1h count needed ~5MB of
            // selected parquet bytes but query-triggered detached warms read
            // 1.33GB from object storage (266x amplification) and competed
            // with the foreground range requests. A large miss therefore
            // falls through to the exact range fetch below.
            if file_meta.size <= self.config.l1_max_entry_bytes as u64 {
                debug!("Foyer cache MISS for Parquet data: {} (range: {}..{}, fetching full file)", location, range.start, range.end);
                if let Ok(result) = self.get_cached(location).await {
                    let full = Self::read_payload(result.payload).await?;
                    if range.end <= full.len() as u64 {
                        return Ok(Bytes::from(full).slice(range.start as usize..range.end as usize));
                    }
                }
            } else if file_size > PARQUET_RANGE_ALIGNMENT_BYTES {
                // Exact DataFusion ranges depend on the sliding predicate and
                // page-index result. Normalize their edges so the next refresh
                // addresses the same cache entry instead of downloading a
                // nearly-identical multi-MB range again.
                let aligned_start = (range.start / PARQUET_RANGE_ALIGNMENT_BYTES) * PARQUET_RANGE_ALIGNMENT_BYTES;
                let aligned_end = range.end.div_ceil(PARQUET_RANGE_ALIGNMENT_BYTES).saturating_mul(PARQUET_RANGE_ALIGNMENT_BYTES).min(file_size);
                let aligned = aligned_start..aligned_end;
                let aligned_key = Self::make_range_cache_key(location, &aligned);
                if aligned != range
                    && let Ok(Some(entry)) = self.cache.get(&aligned_key).await
                {
                    let value = entry.value();
                    let start = (range.start - aligned.start) as usize;
                    let end = start + (range.end - range.start) as usize;
                    if !value.is_expired(self.config.ttl) && end <= value.data.len() {
                        record_range_hit(&self.stats, range.end - range.start).await;
                        span.record("cache_hit", true);
                        let data = value.data.slice(start..end);
                        self.maybe_touch(&self.cache, &aligned_key, entry.clone(), self.config.l1_max_entry_bytes);
                        return Ok(data);
                    }
                }
                response_slice = Some(((range.start - aligned.start) as usize, (range.end - aligned.start) as usize));
                range_cache_key = aligned_key;
                fetch_range = aligned;
            }
        }

        // Fallback to regular range request for non-parquet files
        span.record("cache_hit", false);
        record_miss_with_fetch(&self.stats).await;
        debug!("get_range request for: {} (range: {}..{}, parquet={})", location, range.start, range.end, is_parquet);

        let start_time = std::time::Instant::now();
        let inner_span = tracing::trace_span!(parent: &span, "s3.get_range",
            location = %location,
            range.start = fetch_range.start,
            range.end = fetch_range.end
        );
        let result = self.inner.get_range(location, fetch_range.clone()).instrument(inner_span).await?;
        let duration = start_time.elapsed();

        debug!(
            "S3 GET_RANGE request: {} (range: {}..{}, size: {} bytes, duration: {}ms, parquet: {})",
            location,
            fetch_range.start,
            fetch_range.end,
            fetch_range.end - fetch_range.start,
            duration.as_millis(),
            is_parquet
        );

        record_range_miss(&self.stats, result.len() as u64).await;
        bump(&self.stats, |s| s.inner_bytes_read += result.len() as u64).await;
        if let Some(meta) = range_meta.as_ref() {
            self.admit_data_range(location, range_cache_key, &result, meta);
        }
        Ok(match response_slice {
            Some((start, end)) => result.slice(start..end),
            None => result,
        })
    }

    /// Resolve a path's `ObjectMeta` from cache only (no S3). Checks the
    /// full-file cache, then — for immutable parquet data files — the dedicated
    /// meta cache. Returns `None` if neither has a live entry.
    async fn cached_meta(&self, location: &Path) -> Option<ObjectMeta> {
        let live = |e: Option<CacheEntry>| e.filter(|e| !e.value().is_expired(self.config.ttl)).map(|e| e.value().meta.clone());
        if let Some(meta) = live(self.cache.get(&Self::make_cache_key(location)).await.ok().flatten()) {
            return Some(meta);
        }
        is_parquet_file(location).then_some(())?;
        live(self.metadata_cache.get(&Self::make_meta_cache_key(location)).await.ok().flatten())
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
            self.admit_meta(location, meta.clone());
        }
        Ok(meta)
    }

    /// Core put logic: writes to inner store, then caches the new data
    async fn put_cached(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        bump(&self.stats, |s| s.inner_puts += 1).await;
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
            let data = payload_for_cache.iter().fold(Vec::with_capacity(payload_size), |mut acc, chunk| {
                acc.extend_from_slice(chunk);
                acc
            });
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
        self.admit(&self.cache, Self::make_cache_key(location), value, self.config.l1_max_entry_bytes);
    }

    /// Single funnel for every cache population, so `scan_bypass_scope` can
    /// suppress all of them in one place. `l1_max_entry_bytes = 0` (metadata
    /// entries) keeps the default L1+disk placement.
    fn admit(&self, cache: &FoyerCache, key: String, value: CacheValue, l1_max_entry_bytes: usize) {
        if bypass_active() && !self.repeat_sighting(&key) {
            crate::observability::record_cache_insert_bypassed();
            return;
        }
        insert_main(cache, key, value, l1_max_entry_bytes);
    }

    /// Has a bypassed scan already tried to admit `key` once?
    ///
    /// The bypass exists so a ONE-OFF wide scan cannot evict the hot set on its
    /// way through — a real concern with L2 full (118.5 of 120 GB, 12.3k
    /// evictions, prod 2026-08-09). But a dashboard panel is the opposite of a
    /// one-off: it is the most repeated query in the system, and under a blanket
    /// bypass it can never warm its own working set. Measured on prod, the same
    /// 3d panel ran 24.4 s cold and plateaued at ~11-15 s warm purely on the
    /// caches the bypass does NOT touch (parquet metadata, provider) — the data
    /// blocks never stuck.
    ///
    /// First sighting records and still declines; the second admits. So an
    /// ad-hoc scan that touches a key once pays nothing, and a panel that
    /// refreshes converges. `DashSet` insert returns false when already present,
    /// which is exactly the second-sighting test.
    ///
    /// Bounded by clearing wholesale at [`BYPASS_SEEN_MAX`] rather than tracking
    /// per-entry recency: this is a hint, and a rare full reset costs one extra
    /// cold pass, not correctness.
    fn repeat_sighting(&self, key: &str) -> bool {
        if self.bypass_seen.len() >= BYPASS_SEEN_MAX {
            self.bypass_seen.clear();
        }
        !self.bypass_seen.insert(key.to_string())
    }

    /// Cache a fetched byte range under `key`, stamped with the file's identity
    /// (etag/version/mtime) but the *range's* length as its size.
    fn admit_range(&self, location: &Path, key: String, data: &[u8], file: &ObjectMeta) {
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: file.last_modified,
            size: data.len() as u64,
            e_tag: file.e_tag.clone(),
            version: file.version.clone(),
        };
        self.admit(&self.metadata_cache, key, CacheValue::new(data.to_vec(), meta), 0);
    }

    /// Admit an exact parquet data range to the main cache. This is the
    /// fallback for files whose full-file post-commit warm has not landed.
    fn admit_data_range(&self, location: &Path, key: String, data: &[u8], file: &ObjectMeta) {
        if !is_within_recent_window(location, self.config.cache_recent_days) {
            return;
        }
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: file.last_modified,
            size: data.len() as u64,
            e_tag: file.e_tag.clone(),
            version: file.version.clone(),
        };
        self.admit(&self.cache, key, CacheValue::new(data.to_vec(), meta), self.config.l1_max_entry_bytes);
    }

    /// Cache a path's `ObjectMeta` (body-less entry) so later reads skip the HEAD.
    fn admit_meta(&self, location: &Path, meta: ObjectMeta) {
        self.admit(&self.metadata_cache, Self::make_meta_cache_key(location), CacheValue::new(Vec::new(), meta), 0);
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
    fn maybe_touch(&self, cache: &FoyerCache, key: &str, entry: CacheEntry, l1_max_entry_bytes: usize) {
        // `as_millis()` is u128; clamp before the u64 cast so an absurdly large
        // configured TTL can't silently truncate into a tiny value.
        let ttl_millis = self.config.ttl.as_millis().min(u64::MAX as u128) as u64;
        // Still fresh enough (or already refreshing) — don't churn the cache.
        if entry.value().age_millis().saturating_mul(2) <= ttl_millis || !self.refreshing.insert(key.to_string()) {
            return;
        }
        let (cache, refreshing, key) = (cache.clone(), self.refreshing.clone(), key.to_string());
        self.spawn_tracked(async move {
            let v = entry.value();
            insert_main(&cache, key.clone(), CacheValue::new(v.data.clone(), v.meta.clone()), l1_max_entry_bytes);
            refreshing.remove(&key);
        });
    }
}

/// Wraps an inner [`MultipartUpload`] to tee written bytes into a bounded
/// buffer, so the completed file can be inserted into the cache directly — we
/// never re-download a file we just streamed to S3. If the upload grows past
/// `max_warm_bytes` the buffer is dropped and the rest streams through
/// un-captured (large compaction outputs fall back to the selective
/// post-commit warm path); this bounds both transient memory and L1 cache
/// pressure. Strictly best-effort: failure to capture never affects the write.
#[derive(derive_more::Debug)]
#[debug("CachingMultipartUpload {{ location: {} }}", location)]
struct CachingMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    location: Path,
    cache: FoyerCache,
    /// `None` once the cap was exceeded (capture abandoned for this upload).
    buffer: Option<Vec<u8>>,
    max_warm_bytes: usize,
    l1_max_entry_bytes: usize,
    /// Holds this upload's slice of the process-wide capture budget; dropping
    /// it (abandon, complete, abort, or panic) returns the bytes.
    reservation: Option<CaptureReservation>,
}

/// Bytes currently reserved by in-flight write captures. Each capturing upload
/// reserves its full per-upload cap up front — the buffer's final size isn't
/// known until `complete()`, so reserving the worst case is what makes the
/// bound real rather than advisory.
static WRITE_CAPTURE_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

/// RAII claim on `WRITE_CAPTURE_INFLIGHT`.
struct CaptureReservation(usize);

impl CaptureReservation {
    /// Reserve `bytes` if that keeps the process under `budget` (0 = unbudgeted).
    /// Never blocks: over budget simply means this upload doesn't capture.
    fn acquire(bytes: usize, budget: usize) -> Option<Self> {
        if budget == 0 {
            return Some(Self(0));
        }
        WRITE_CAPTURE_INFLIGHT
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| (cur.saturating_add(bytes) <= budget).then(|| cur + bytes))
            .ok()
            .map(|_| Self(bytes))
    }
}

impl Drop for CaptureReservation {
    fn drop(&mut self) {
        if self.0 > 0 {
            WRITE_CAPTURE_INFLIGHT.fetch_sub(self.0, Ordering::AcqRel);
        }
    }
}

#[async_trait]
impl MultipartUpload for CachingMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        if let Some(buf) = self.buffer.as_mut() {
            if buf.len().saturating_add(data.content_length()) > self.max_warm_bytes {
                // Too big to warm without risking memory / L1 eviction — give
                // up capturing for this upload and return its budget slice.
                self.buffer = None;
                self.reservation = None;
                crate::observability::record_write_capture_skipped();
            } else {
                data.iter().for_each(|chunk| buf.extend_from_slice(chunk));
            }
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        let result = self.inner.complete().await?;
        let _reservation = self.reservation.take(); // released once this scope ends
        if let Some(buf) = self.buffer.take()
            && !buf.is_empty()
        {
            let size = buf.len() as u64;
            let meta = put_result_meta(self.location.clone(), size, &result);
            // Use the same key derivation as the read path so a multipart-warmed
            // entry is found by a later GET even if `make_cache_key` ever does
            // more than `location.to_string()`.
            insert_main(&self.cache, FoyerObjectStoreCache::make_cache_key(&self.location), CacheValue::new(buf, meta), self.l1_max_entry_bytes);
            debug!("Warmed cache from multipart write: {} (size: {} bytes)", self.location, size);
        }
        Ok(result)
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        self.buffer = None;
        self.reservation = None;
        self.inner.abort().await
    }
}

// DIAG (commit-throughput profiling): wraps a list stream to log total items +
// duration when it finishes, surfacing slow `_delta_log` scans during the ~40s
// Delta commit. Remove once the commit bottleneck is confirmed/fixed.
struct TimedListStream {
    inner: BoxStream<'static, ObjectStoreResult<ObjectMeta>>,
    started: std::time::Instant,
    count: usize,
    prefix: String,
    done: bool,
}
impl TimedListStream {
    fn new(inner: BoxStream<'static, ObjectStoreResult<ObjectMeta>>, prefix: String) -> Self {
        Self { inner, started: std::time::Instant::now(), count: 0, prefix, done: false }
    }
}
impl futures::Stream for TimedListStream {
    type Item = ObjectStoreResult<ObjectMeta>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let p = this.inner.as_mut().poll_next(cx);
        match &p {
            std::task::Poll::Ready(Some(_)) => this.count += 1,
            std::task::Poll::Ready(None) if !this.done => {
                this.done = true;
                let ms = this.started.elapsed().as_millis();
                if ms > 1500 {
                    tracing::info!("slow_list prefix={} items={} ms={}", this.prefix, this.count, ms);
                }
            }
            _ => {}
        }
        p
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
        // `write_capture_max_bytes` tightens this to flush-file scale so a
        // 256MB compaction output never sits in heap: it skips the tee here and
        // is warmed post-commit through the read path (warm_cache_for_uris).
        // A cap above the budget would make EVERY reservation fail (each
        // acquires its full cap up front), silently disabling capture — most
        // easily hit with `write_capture_max_mb=0`, whose documented meaning is
        // "bounded only by the block size", not "off". Clamp instead.
        let cap = [self.config.warm_inline_max_bytes, self.config.write_capture_max_bytes, self.config.write_capture_budget_bytes]
            .into_iter()
            .filter(|&c| c > 0)
            .fold(self.config.block_size_bytes, usize::min);
        // Best-effort claim on the process-wide capture budget. Denied = this
        // upload streams through un-teed; the upload itself never waits or fails.
        let reservation = CaptureReservation::acquire(cap, self.config.write_capture_budget_bytes);
        if reservation.is_none() {
            crate::observability::record_write_capture_skipped();
        }
        Ok(Box::new(CachingMultipartUpload {
            inner,
            location: location.clone(),
            cache: self.cache.clone(),
            buffer: reservation.is_some().then(Vec::new),
            max_warm_bytes: cap,
            l1_max_entry_bytes: self.config.l1_max_entry_bytes,
            reservation,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        // Handle range requests via the dedicated range cache path
        if let Some(GetRange::Bounded(ref r)) = options.range
            && is_unconditional(&options)
        {
            let range = r.clone();
            let bytes = self.get_range_cached(location, range.clone()).await?;
            let meta = self.head_cached(location).await.unwrap_or_else(|_| ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size: range.end,
                e_tag: None,
                version: None,
            });
            return Ok(Self::make_get_result_at(bytes, meta, Attributes::new(), range.start));
        }
        // Suffix range (footer warm + any suffix reader): resolve to an absolute
        // range so it shares cache keys with bounded footer reads. If we already
        // know the size (cached meta), reuse the bounded path for free; otherwise
        // a single suffix GET to the inner store learns the absolute range + size
        // from the response — one round-trip, no separate HEAD.
        if let Some(GetRange::Suffix(n)) = options.range
            && is_unconditional(&options)
        {
            let n = n.max(1);
            if let Some(meta) = self.cached_meta(location).await {
                let range = meta.size.saturating_sub(n)..meta.size;
                let bytes = self.get_range_cached(location, range.clone()).await?;
                return Ok(Self::make_get_result_at(bytes, meta, Attributes::new(), range.start));
            }
            let result = self.inner.get_opts(location, GetOptions { range: Some(GetRange::Suffix(n)), ..Default::default() }).await?;
            let meta = result.meta.clone();
            let abs_range = result.range.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            record_miss_with_fetch(&self.metadata_stats).await;
            // Populate both the footer-range cache (under the absolute key bounded
            // reads use) and the immutable-meta cache, so the next footer read is
            // a pure cache hit.
            if is_parquet_file(location) {
                self.admit_range(location, Self::make_range_cache_key(location, &abs_range), &bytes, &meta);
                self.admit_meta(location, meta.clone());
            }
            return Ok(Self::make_get_result_at(bytes, meta, attributes, abs_range.start));
        }
        // Bypass cache for complex (conditional / non-bounded) requests
        if options.range.is_some() || options.head || !is_unconditional(&options) {
            return self.inner.get_opts(location, options).await;
        }
        self.get_cached(location).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, ObjectStoreResult<Path>>) -> BoxStream<'static, ObjectStoreResult<Path>> {
        use futures::StreamExt;
        let cache = self.cache.clone();
        // Only the full-file entry is dropped here: per-range metadata keys can't
        // be enumerated without a HEAD, so they age out with the TTL.
        self.inner
            .delete_stream(locations)
            .inspect(move |res| {
                if let Ok(path) = res {
                    cache.remove(&path.to_string());
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        Box::pin(TimedListStream::new(self.inner.list(prefix), prefix.map(|p| p.to_string()).unwrap_or_default()))
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        Box::pin(TimedListStream::new(
            self.inner.list_with_offset(prefix, offset),
            format!("{}@>{}", prefix.map(|p| p.to_string()).unwrap_or_default(), offset),
        ))
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

/// Whether a path belongs to the COMMIT-LOG request class: the small
/// control-plane objects Delta's transaction protocol reads and writes
/// (`_delta_log/NNN.json`, `_last_checkpoint`, and log LISTs).
///
/// Checkpoint **parquet** under `_delta_log/` is deliberately NOT log class: a
/// checkpoint for a 26k-file table is a multi-MB bulk transfer whose duration
/// scales with the table, exactly like a data file — and it already has its own
/// bound (`CHECKPOINT_OP_TIMEOUT`). Only the sub-second control-plane traffic
/// gets the short client.
pub fn is_commit_log_path(location: &Path) -> bool {
    let p = location.as_ref();
    p.contains("_delta_log/") && !p.ends_with(".parquet")
}

/// Routes object-store requests to one of two S3 clients by REQUEST CLASS: the
/// commit log gets a client with a short request timeout, everything else the
/// long-timeout data client.
///
/// This is the primary defence against the 2026-07-30 stall class — a hung R2
/// request pinning a per-table commit lock. Bounding it here rather than with an
/// outer `tokio::time::timeout` is what makes it safe: at this layer a
/// timed-out commit is an ordinary commit error that TimeFusion's landed-probe
/// already knows how to classify, whereas an outer timeout abandons a future
/// mid-flight and manufactures an unconfirmed landing every time it fires.
///
/// Sits BELOW the foyer cache and the instrumentation wrapper (see
/// `create_delta_table_internal`), so cache semantics, metrics and cache keys
/// are byte-for-byte unchanged — only the HTTP client underneath differs.
#[derive(Debug, derive_more::Display)]
#[display("RequestClassRouter(log={}, data={})", log, data)]
pub struct RequestClassRouter {
    log: Arc<dyn ObjectStore>,
    data: Arc<dyn ObjectStore>,
}

impl RequestClassRouter {
    pub fn new(log: Arc<dyn ObjectStore>, data: Arc<dyn ObjectStore>) -> Self {
        Self { log, data }
    }

    fn route(&self, location: &Path) -> &Arc<dyn ObjectStore> {
        if is_commit_log_path(location) { &self.log } else { &self.data }
    }

    /// Prefix-based routing for LIST. A `None` prefix (or a table-root prefix)
    /// enumerates data files, so only an explicit `_delta_log` prefix is log
    /// class — a whole-table LIST must keep the generous data bound.
    fn route_prefix(&self, prefix: Option<&Path>) -> &Arc<dyn ObjectStore> {
        match prefix {
            Some(p) if p.as_ref().contains("_delta_log") => &self.log,
            _ => &self.data,
        }
    }
}

#[async_trait]
impl ObjectStore for RequestClassRouter {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.route(location).put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        // Multipart is bulk by definition — never log class, whatever the path.
        self.data.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.route(location).get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> ObjectStoreResult<Vec<Bytes>> {
        self.route(location).get_ranges(location, ranges).await
    }

    fn delete_stream(&self, locations: BoxStream<'static, ObjectStoreResult<Path>>) -> BoxStream<'static, ObjectStoreResult<Path>> {
        // A mixed-class stream can't be split without buffering it; log cleanup
        // (the only bulk-delete caller that touches `_delta_log`) is off the
        // commit lock and already bounded by CHECKPOINT_OP_TIMEOUT.
        self.data.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.route_prefix(prefix).list(prefix)
    }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.route_prefix(prefix).list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.route_prefix(prefix).list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> ObjectStoreResult<()> {
        // Route on the DESTINATION: `rename_if_not_exists` into `_delta_log` is
        // the commit-entry write on non-conditional-put stores.
        self.route(to).copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;

    /// Removes a test's cache dir when the test ends — including on panic, and
    /// without every test repeating a pre/post `remove_dir_all` pair.
    /// `test_config` already makes the path unique per (test name, process).
    struct CacheDirGuard(PathBuf);

    impl Drop for CacheDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The request-class split is the PRIMARY bound on commit-lock hold time
    /// (prod 2026-07-30), so what counts as commit-log class is load-bearing:
    /// misroute the commit PUT and it silently goes back to the 900s data
    /// client; misroute a parquet part and a legitimate multi-minute upload
    /// starts failing at 30s.
    #[tokio::test]
    async fn commit_log_traffic_routes_to_the_short_timeout_client() -> anyhow::Result<()> {
        let log = Arc::new(InMemory::new());
        let data = Arc::new(InMemory::new());
        let router = RequestClassRouter::new(log.clone(), data.clone());

        let commit = Path::from("tbl/_delta_log/00000000000000000001.json");
        let checkpoint = Path::from("tbl/_delta_log/00000000000000000001.checkpoint.parquet");
        let part = Path::from("tbl/date=2026-07-30/part-0.parquet");
        for p in [&commit, &checkpoint, &part] {
            router.put(p, PutPayload::from_static(b"x")).await?;
        }
        assert!(log.head(&commit).await.is_ok(), "the commit entry must use the short-timeout client");
        assert!(data.head(&commit).await.is_err());
        // A checkpoint is a multi-MB bulk transfer, not control plane.
        assert!(data.head(&checkpoint).await.is_ok(), "checkpoint parquet stays on the data client");
        assert!(data.head(&part).await.is_ok(), "data parquet stays on the data client");
        assert!(log.head(&part).await.is_err());
        // Reads follow the same split, so a commit-log GET can't hang for the
        // data bound either.
        assert_eq!(router.get(&commit).await?.bytes().await?, Bytes::from_static(b"x"));

        // LIST routes by prefix: only an explicit _delta_log prefix is control
        // plane — a whole-table listing enumerates data files.
        assert!(matches!(router.route_prefix(Some(&Path::from("tbl/_delta_log"))), s if Arc::ptr_eq(s, &(log.clone() as Arc<dyn ObjectStore>))));
        assert!(matches!(router.route_prefix(Some(&Path::from("tbl"))), s if Arc::ptr_eq(s, &(data.clone() as Arc<dyn ObjectStore>))));
        assert!(matches!(router.route_prefix(None), s if Arc::ptr_eq(s, &(data.clone() as Arc<dyn ObjectStore>))));
        Ok(())
    }

    // Locks in the containment-probe slice math in get_range_cached: a strict
    // sub-range of the warmed (size-hint..size) footer never equals the warm
    // key, so only the containment probe can serve it without an inner fetch.
    #[tokio::test]
    async fn containment_probe_serves_subrange_of_warmed_footer() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        // The probe computes its candidate key from the CONFIG's size hint, so
        // the warm call below must use the same value (as prod does — both
        // read config.cache.timefusion_parquet_metadata_size_hint).
        let hint = 1024u64;
        let cfg = FoyerCacheConfig::test_config_with("containment_probe", |c| c.parquet_metadata_size_hint = hint as usize);
        let cache = FoyerObjectStoreCache::new(inner.clone(), cfg).await?;
        let path = Path::from("tbl/date=2026-01-01/part.parquet");
        let data = Bytes::from((0..4096u32).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
        // Put via the inner store so nothing is cached from a write payload.
        inner.put(&path, PutPayload::from(data.clone())).await?;

        // file (4096B) > hint → warm key is (3072..4096). Warm through &cache
        // (not &*inner) deliberately: prod warms through the caching layer, so
        // this also covers the suffix-GET path that populates the range key.
        assert!(warm_footer(&cache, &path, hint).await, "footer warm must succeed");

        let before = cache.get_stats().await;
        let r = 3100u64..3500u64;
        let got = cache.get_range(&path, r.clone()).await?;
        assert_eq!(got, data.slice(r.start as usize..r.end as usize), "containment slice math");

        let after = cache.get_stats().await;
        assert_eq!(after.metadata.hits, before.metadata.hits + 1, "served by the containment probe");
        assert_eq!(after.metadata.inner_gets, before.metadata.inner_gets, "no inner fetch after warm");

        cache.shutdown().await?;
        Ok(())
    }

    // Regression for issue #82: shutdown must never block past its deadline.
    // The unbounded foyer close() stalled process exit (and wal.lock release)
    // for ~8.5min on a redeploy. With an already-elapsed deadline, shutdown_by
    // must abandon the flush and return promptly.
    #[tokio::test]
    async fn shutdown_by_respects_elapsed_deadline() -> anyhow::Result<()> {
        let cache = SharedFoyerCache::new(FoyerCacheConfig::test_config("shutdown_deadline")).await?;
        let start = tokio::time::Instant::now();
        cache.shutdown_by(start).await?; // deadline already reached
        assert!(start.elapsed() < Duration::from_secs(5), "shutdown_by must not block past its deadline");
        Ok(())
    }

    #[tokio::test]
    async fn containment_probe_serves_data_reads_on_small_fully_warmed_files() -> anyhow::Result<()> {
        // Files smaller than the size hint are cached WHOLE under (0..size) by
        // warm_footer — the candidate=0 probe then deliberately serves even
        // data-page reads near the file START from the metadata cache. This is
        // the other probe branch (candidate 0), distinct from the suffix-key
        // case covered above.
        let inner = Arc::new(InMemory::new());
        let hint = 1024u64;
        let cfg = FoyerCacheConfig::test_config_with("containment_probe_small", |c| c.parquet_metadata_size_hint = hint as usize);
        let cache = FoyerObjectStoreCache::new(inner.clone(), cfg).await?;
        let path = Path::from("tbl/date=2026-01-01/part-small.parquet");
        let data = Bytes::from((0..512u32).map(|i| (i % 13) as u8).collect::<Vec<u8>>());
        inner.put(&path, PutPayload::from(data.clone())).await?;

        // file (512B) <= hint → warm key is the whole file (0..512)
        assert!(warm_footer(&cache, &path, hint).await, "footer warm must succeed");

        let before = cache.get_stats().await;
        let r = 16u64..96u64; // nowhere near the footer
        let got = cache.get_range(&path, r.clone()).await?;
        assert_eq!(got, data.slice(r.start as usize..r.end as usize), "candidate-0 slice math");

        let after = cache.get_stats().await;
        assert_eq!(after.metadata.hits, before.metadata.hits + 1, "served by the candidate-0 probe");
        assert_eq!(after.metadata.inner_gets, before.metadata.inner_gets, "no inner fetch after warm");

        cache.shutdown().await?;
        Ok(())
    }

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
        assert_eq!(cache.get(&path).await?.bytes().await?, data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // No fetch needed - cached from write payload
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        assert_eq!(cache.get(&path).await?.bytes().await?, data);

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Still no fetch - served from cache
        assert_eq!(stats.main.hits, 2); // Two cache hits total
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.bytes_served, (data.len() * 2) as u64);
        assert_eq!(cache.try_get_stats().main.bytes_served, stats.main.bytes_served);

        cache.delete(&path).await?;

        // Give cache time to process deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // After deletion, get should fail
        let get_result = cache.get(&path).await;
        assert!(get_result.is_err(), "Expected error after delete, got: {:?}", get_result);

        cache.shutdown().await?;
        Ok(())
    }

    /// A cache-hit range read serves the requested slice of the cached object,
    /// zero-copy (a `Bytes::slice` of the entry's buffer).
    #[tokio::test]
    async fn cache_hit_serves_the_requested_range() -> anyhow::Result<()> {
        let cache = SharedFoyerCache::new(FoyerCacheConfig::test_config("hit_range")).await?;
        let store = FoyerObjectStoreCache::new_with_shared_cache(Arc::new(object_store::memory::InMemory::new()), &cache);
        let path = Path::from("test/zc.parquet");
        let body: Vec<u8> = (0u8..=255).collect();
        let meta = ObjectMeta { location: path.clone(), last_modified: Utc::now(), size: body.len() as u64, e_tag: None, version: None };
        cache.cache.insert(FoyerObjectStoreCache::make_cache_key(&path), CacheValue::new(body.clone(), meta));

        assert_eq!(&store.get_range_cached(&path, 10..40).await?[..], &body[10..40], "served range must be the cached bytes");
        assert_eq!(&store.get_range_cached(&path, 0..1).await?[..], &body[0..1], "leading edge");
        assert_eq!(&store.get_range_cached(&path, 255..256).await?[..], &body[255..256], "trailing edge");
        Ok(())
    }

    /// THE test the first zero-copy attempt (2bb5e85) needed: a slice held by a
    /// reader must never pin the cache ENTRY. That attempt handed `Bytes` an
    /// owner holding the `HybridCacheEntry`; entries stopped being reclaimable
    /// under scan load, foyer admission stalled, and the tantivy manifest GET
    /// on the flush path timed out — 191 failed flushes in prod (see 5926f66).
    /// Now `CacheValue.data` is itself `Bytes`, so a served slice refcounts the
    /// BUFFER only: removal and further cache traffic must complete while
    /// slices are held, and the held slices must stay readable afterwards.
    #[tokio::test]
    async fn held_slice_does_not_pin_the_cache_entry() -> anyhow::Result<()> {
        let cache = SharedFoyerCache::new(FoyerCacheConfig::test_config("no_pin")).await?;
        let store = FoyerObjectStoreCache::new_with_shared_cache(Arc::new(object_store::memory::InMemory::new()), &cache);
        let path = Path::from("test/pin.parquet");
        let body: Vec<u8> = (0u8..=255).collect();
        let meta = ObjectMeta { location: path.clone(), last_modified: Utc::now(), size: body.len() as u64, e_tag: None, version: None };
        let key = FoyerObjectStoreCache::make_cache_key(&path);
        cache.cache.insert(key.clone(), CacheValue::new(body.clone(), meta.clone()));

        let held: Vec<Bytes> = futures::future::try_join_all((0..8).map(|i| store.get_range_cached(&path, i * 8..i * 8 + 8))).await?;

        // Removal and a full traffic cycle must not block on the held slices.
        tokio::time::timeout(Duration::from_secs(1), async {
            cache.cache.remove(&key);
            for i in 0..64u8 {
                let p = Path::from(format!("test/churn_{i}.parquet"));
                let m = ObjectMeta { location: p.clone(), last_modified: Utc::now(), size: 256, e_tag: None, version: None };
                cache.cache.insert(FoyerObjectStoreCache::make_cache_key(&p), CacheValue::new(body.clone(), m));
                cache.cache.get(&FoyerObjectStoreCache::make_cache_key(&p)).await.ok();
            }
        })
        .await
        .expect("cache traffic must proceed while slices are held — a hang here is the 2bb5e85 outage");

        for (i, s) in held.iter().enumerate() {
            assert_eq!(&s[..], &body[i * 8..i * 8 + 8], "held slice stays readable after eviction");
        }
        Ok(())
    }

    #[tokio::test]
    async fn runtime_stats_exposes_effective_config_and_occupancy() -> anyhow::Result<()> {
        let config = FoyerCacheConfig::test_config_with("runtime_stats", |c| {
            c.memory_size_bytes = 1024 * 1024;
            c.l1_max_entry_bytes = 64 * 1024;
        });
        let cache = SharedFoyerCache::new(config.clone()).await?;
        let path = Path::from("test/file.parquet");
        let meta = ObjectMeta { location: path, last_modified: Utc::now(), size: 6, e_tag: None, version: None };
        cache.cache.insert("test/file.parquet".into(), CacheValue::new(b"cached".to_vec(), meta));

        let stats = cache.runtime_stats();
        assert_eq!(stats.memory_size_bytes, config.memory_size_bytes);
        assert_eq!(stats.disk_size_bytes, config.disk_size_bytes);
        assert_eq!(stats.ttl_seconds, config.ttl.as_secs());
        assert_eq!(stats.l1_max_entry_bytes, config.l1_max_entry_bytes);
        assert_eq!(stats.cache_dir, config.cache_dir);
        assert!(stats.l1_used_bytes >= 6, "cached bytes must count toward L1 usage");
        assert_eq!(stats.entry_count, 1);

        cache.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(5)).await?;
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

        let files =
            vec![("table/part-001.parquet", vec![b'a'; 1024]), ("table/part-002.parquet", vec![b'b'; 2048]), ("table/part-003.parquet", vec![b'c'; 4096])];

        // Write all files
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            cache.put(&path, PutPayload::from(Bytes::from(data.clone()))).await?;
        }

        // First read - cache hit (since we cache on write)
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            assert_eq!(cache.get(&path).await?.bytes().await?.len(), data.len());
        }

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Cached from write payloads — no re-fetch
        assert_eq!(stats.main.misses, 0);
        assert_eq!(stats.main.hits, 3);

        // Second read - cache hit
        for (path_str, data) in &files {
            let path = Path::from(*path_str);
            assert_eq!(cache.get(&path).await?.bytes().await?.len(), data.len());
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
        let config = FoyerCacheConfig::test_config_with("ttl", |c| {
            c.ttl = Duration::from_millis(100);
        });

        let _dir = CacheDirGuard(config.cache_dir.clone());

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
        assert_eq!(cache.get(&path).await?.bytes().await?.len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Cached from write payload — no re-fetch
        assert_eq!(stats.main.hits, 1);

        // Second get - cache hit
        assert_eq!(cache.get(&path).await?.bytes().await?.len(), large_data.len());

        let stats = cache.get_stats().await;
        assert_eq!(stats.main.inner_gets, 0); // Still no fetch - served from cache
        assert_eq!(stats.main.hits, 2); // Two cache hits total

        info!("Large file test - main cache hits: {}, misses: {}", stats.main.hits, stats.main.misses);
        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_metadata_optimization() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("parquet_metadata", |c| {
            c.parquet_metadata_size_hint = 1024; // 1KB for testing
            c.ttl = Duration::from_secs(300);
        });

        let _dir = CacheDirGuard(config.cache_dir.clone());

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

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_separation() -> anyhow::Result<()> {
        // Use in-memory store for testing
        let inner = Arc::new(InMemory::new());

        // Configure cache with small limits to test separation
        let config = FoyerCacheConfig::test_config_with("metadata_separation", |c| {
            c.memory_size_bytes = 10 * 1024 * 1024; // 10MB
            c.disk_size_bytes = 50 * 1024 * 1024; // 50MB
            c.metadata_memory_size_bytes = 5 * 1024 * 1024; // 5MB
            c.metadata_disk_size_bytes = 20 * 1024 * 1024; // 20MB
            c.parquet_metadata_size_hint = 1024; // 1KB
        });

        let _dir = CacheDirGuard(config.cache_dir.clone());

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

        Ok(())
    }

    #[tokio::test]
    async fn test_metadata_cache_invalidation() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());

        let config = FoyerCacheConfig::test_config_with("metadata_invalidation", |c| {
            c.parquet_metadata_size_hint = 1024;
            c.metadata_memory_size_bytes = 5 * 1024 * 1024;
            c.metadata_disk_size_bytes = 20 * 1024 * 1024;
        });

        let _dir = CacheDirGuard(config.cache_dir.clone());

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

        Ok(())
    }

    #[tokio::test]
    async fn test_multipart_capture_warms_cache() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let inner = Arc::new(InMemory::new());
        // Tighten the inline cap to 1MB (below the 4MB block size) so we can
        // exercise both the captured and skipped paths.
        let config = FoyerCacheConfig::test_config_with("mpu_capture", |c| {
            c.warm_inline_max_bytes = 1024 * 1024;
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());

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
        assert_eq!(cache.get(&small_path).await?.bytes().await?.len(), small_data.len());
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
        Ok(())
    }

    /// The per-upload write-capture cap must bound the tee independently of the
    /// (much larger) block size: over-cap uploads abandon capture but still
    /// upload correct, readable bytes. This is the memory fix — before it, a
    /// 256MB compaction output sat in heap per concurrent upload.
    #[tokio::test]
    async fn test_write_capture_cap_bounds_tee() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let inner = Arc::new(InMemory::new());
        // Block size stays 4MB; only the write-capture cap is tightened.
        let config = FoyerCacheConfig::test_config_with("wcap_cap", |c| {
            c.write_capture_max_bytes = 512 * 1024;
            c.write_capture_budget_bytes = 0; // isolate the per-upload cap
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        // Under the cap → still captured (flush-sized files keep the feature).
        let small_path = Path::from("table/date=2026-06-05/small.parquet");
        let small = Bytes::from(vec![b'a'; 128 * 1024]);
        let mut upload = cache.put_multipart(&small_path).await?;
        upload.put_part(small.clone().into()).await?;
        upload.complete().await?;
        let _ = cache.get(&small_path).await?;
        assert_eq!(cache.get_stats().await.main.hits, 1, "under-cap write should still warm the cache");

        // Over the cap (but well under the 4MB block size) → capture abandoned.
        cache.reset_stats().await;
        let big_path = Path::from("table/date=2026-06-05/big.parquet");
        let chunk = Bytes::from(vec![b'b'; 384 * 1024]);
        let mut upload = cache.put_multipart(&big_path).await?;
        upload.put_part(chunk.clone().into()).await?;
        upload.put_part(chunk.clone().into()).await?; // 768KB > 512KB cap
        upload.complete().await?;

        // The object is fully and correctly uploaded regardless — only the
        // cache tee was sacrificed, so the read is a genuine miss.
        let fetched = inner.get(&big_path).await?.bytes().await?;
        assert_eq!(fetched.len(), 768 * 1024, "upload must be unaffected by capture abandonment");
        let _ = cache.get(&big_path).await?;
        assert_eq!(cache.get_stats().await.main.misses, 1, "over-cap write must not be captured");

        cache.shutdown().await?;
        Ok(())
    }

    /// `write_capture_max_bytes = 0` means "bounded only by the block size", not
    /// "disabled" — but the block size can exceed the budget, and a reservation
    /// asks for its full cap, so without a clamp every acquire is denied and
    /// capture silently stops entirely.
    #[tokio::test]
    async fn test_write_capture_cap_is_clamped_to_budget() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("wcap_clamp", |c| {
            c.write_capture_max_bytes = 0; // bounded by the (4MB) block size
            c.write_capture_budget_bytes = 512 * 1024; // …which exceeds the budget
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        let path = Path::from("table/date=2026-06-05/clamped.parquet");
        let data = Bytes::from(vec![b'd'; 128 * 1024]);
        let mut upload = cache.put_multipart(&path).await?;
        upload.put_part(data.clone().into()).await?;
        upload.complete().await?;
        let _ = cache.get(&path).await?;
        assert_eq!(cache.get_stats().await.main.hits, 1, "capture must stay on when the cap is clamped to the budget");

        cache.shutdown().await?;
        Ok(())
    }

    /// The process-wide budget stops N concurrent captures from stacking. Once
    /// exhausted, further uploads skip capture — but never block and never fail.
    #[tokio::test]
    async fn test_write_capture_budget_skips_without_failing_uploads() -> anyhow::Result<()> {
        use object_store::MultipartUpload;

        let inner = Arc::new(InMemory::new());
        // Budget fits exactly one concurrent capture (1x the per-upload cap).
        let config = FoyerCacheConfig::test_config_with("wcap_budget", |c| {
            c.write_capture_max_bytes = 512 * 1024;
            c.write_capture_budget_bytes = 512 * 1024;
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());
        let cache = FoyerObjectStoreCache::new(inner.clone(), config).await?;
        cache.reset_stats().await;

        let first_path = Path::from("table/date=2026-06-05/first.parquet");
        let second_path = Path::from("table/date=2026-06-05/second.parquet");
        let data = Bytes::from(vec![b'c'; 64 * 1024]);

        // Hold the first upload open so its reservation is still outstanding
        // when the second one asks for budget.
        let mut first = cache.put_multipart(&first_path).await?;
        first.put_part(data.clone().into()).await?;
        let mut second = cache.put_multipart(&second_path).await?; // over budget → no capture
        second.put_part(data.clone().into()).await?;
        second.complete().await?;
        first.complete().await?;

        // Both uploads succeeded with correct bytes.
        assert_eq!(inner.get(&first_path).await?.bytes().await?.len(), data.len());
        assert_eq!(inner.get(&second_path).await?.bytes().await?.len(), data.len());

        // First was captured (hit), second was budget-skipped (miss).
        let _ = cache.get(&first_path).await?;
        let _ = cache.get(&second_path).await?;
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "the in-budget upload should be captured");
        assert_eq!(stats.main.misses, 1, "the over-budget upload should skip capture");

        // Reservations are released once the uploads finish, so capture works
        // again — the budget is a transient bound, not a latch.
        cache.reset_stats().await;
        let third_path = Path::from("table/date=2026-06-05/third.parquet");
        let mut third = cache.put_multipart(&third_path).await?;
        third.put_part(data.clone().into()).await?;
        third.complete().await?;
        let _ = cache.get(&third_path).await?;
        assert_eq!(cache.get_stats().await.main.hits, 1, "budget must be released when an upload completes");

        cache.shutdown().await?;
        Ok(())
    }

    #[test]
    fn test_block_size_tracks_optimize_target() {
        use crate::config::AppConfig;
        let mut cfg = AppConfig::default();

        // The 2GiB hard floor wins over both the configured block size and 2x
        // the optimize target: the fleet holds ~1.5GB files, and foyer's max
        // entry is the block size — anything above it silently never persists.
        assert_eq!(FoyerCacheConfig::from_app_config(&cfg).block_size_bytes, 2 << 30);

        // A target big enough that 2x exceeds the hard floor → block tracks it,
        // so bigger outputs stay cacheable without touching the cache config.
        cfg.parquet.timefusion_optimize_target_size = 2 * 1024 * 1024 * 1024;
        assert_eq!(FoyerCacheConfig::from_app_config(&cfg).block_size_bytes, 4 << 30, "block size should track 2x the optimize target");

        // A small target still floors at 2GiB.
        cfg.parquet.timefusion_optimize_target_size = 16 * 1024 * 1024;
        cfg.cache.timefusion_foyer_block_size_mb = 256;
        assert_eq!(FoyerCacheConfig::from_app_config(&cfg).block_size_bytes, 2 << 30, "the 1.5GB-file floor holds");
    }

    #[tokio::test]
    async fn test_sliding_ttl_refresh_on_query() -> anyhow::Result<()> {
        let config = FoyerCacheConfig::test_config_with("sliding_ttl", |c| {
            c.ttl = Duration::from_millis(1000);
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());
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
        assert!(shared.cache.memory().contains(&path.to_string()), "freshly read file should be in the in-memory cache");

        // Proactive eviction (what the compaction path does for tombstoned
        // files) drops the entry from the in-memory cache immediately. foyer's
        // HybridCache::remove deletes the on-disk copy asynchronously, so we
        // assert on the memory layer for a deterministic result; the dead bytes
        // are reclaimed from disk shortly after rather than waiting for VACUUM.
        shared.evict_data_entry(path.as_ref());
        assert!(!shared.cache.memory().contains(&path.to_string()), "evicted entry should be dropped from the in-memory cache");

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
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("recent_window", |c| {
            c.cache_recent_days = 8; // enforce the window in this test
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());

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
        Ok(())
    }

    #[tokio::test]
    async fn test_warm_footer_primes_metadata_cache() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config_with("warm_footer", |c| {
            c.parquet_metadata_size_hint = 1024; // 1KB footer
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());

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
        Ok(())
    }

    #[tokio::test]
    async fn test_warm_full_primes_main_cache() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let config = FoyerCacheConfig::test_config("warm_full");
        let _dir = CacheDirGuard(config.cache_dir.clone());

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
        Ok(())
    }

    /// Header and footer warming must both use the metadata cache, so a Parquet
    /// reader's leading magic probe cannot fall through to a full data read.
    #[tokio::test]
    async fn warm_parquet_metadata_primes_header_and_footer() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("table/date=2026-06-05/meta.parquet");
        let size = 4096usize;
        inner.put(&path, PutPayload::from(Bytes::from(vec![b'x'; size]))).await?;
        let cache = FoyerObjectStoreCache::new(inner, FoyerCacheConfig::test_config_with("header_footer", |c| c.parquet_metadata_size_hint = 1024)).await?;

        assert!(warm_parquet_metadata(&cache, &path, 1024).await);
        cache.reset_stats().await;
        assert_eq!(cache.get_range(&path, 0..8).await?.len(), 8);
        assert_eq!(cache.get_range(&path, 3500..3600).await?.len(), 100);
        let stats = cache.get_stats().await;
        assert_eq!(stats.metadata.hits, 2);
        assert_eq!(stats.metadata.inner_gets, 0);
        cache.shutdown().await?;
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
        assert!(shared.cache.memory().contains(&path.to_string()), "warmed entry should be in the in-memory cache");
        shared.evict_data_entry(path.as_ref());
        assert!(!shared.cache.memory().contains(&path.to_string()), "evict must drop the same key warming/reads use");

        cache.shutdown().await?;
        Ok(())
    }

    /// Wraps an `InMemory` store and counts S3-equivalent round-trips, so tests
    /// can assert that warming + reads issue the expected number of HEADs/GETs.
    /// `head()` is an extension method that routes through `get_opts(head:true)`,
    /// so we count it there.
    #[derive(Debug, derive_more::Display)]
    #[display("CountingStore")]
    struct CountingStore {
        inner: Arc<InMemory>,
        heads: Arc<std::sync::atomic::AtomicUsize>,
        gets: Arc<std::sync::atomic::AtomicUsize>,
        delay: Duration,
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
            if !options.head && !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
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

    #[tokio::test]
    async fn failed_get_removes_fetch_lock() -> anyhow::Result<()> {
        let cache = FoyerObjectStoreCache::new(Arc::new(InMemory::new()), FoyerCacheConfig::test_config("failed_get_cleanup")).await?;
        assert!(cache.get(&Path::from("table/date=2026-01-01/missing.parquet")).await.is_err());
        assert!(cache.main_fetch_locks.is_empty(), "failed fetch must not retain its lock");
        cache.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_cold_gets_share_one_inner_fetch() -> anyhow::Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let inner = Arc::new(InMemory::new());
        let path = Path::from(format!("table/date={}/part.parquet", Utc::now().date_naive()));
        inner.put(&path, PutPayload::from(Bytes::from(vec![b'x'; 1024]))).await?;
        let gets = Arc::new(AtomicUsize::new(0));
        let cache = FoyerObjectStoreCache::new(
            Arc::new(CountingStore { inner, heads: Arc::new(AtomicUsize::new(0)), gets: gets.clone(), delay: Duration::from_millis(50) }),
            FoyerCacheConfig::test_config("singleflight"),
        )
        .await?;

        let (first, second) = tokio::join!(cache.get(&path), cache.get(&path));
        first?;
        second?;
        assert_eq!(gets.load(Ordering::Relaxed), 1, "concurrent cold reads must share one inner GET");
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.misses, 1);
        assert_eq!(stats.main.hits, 1);
        cache.shutdown().await?;
        Ok(())
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
        let counting = Arc::new(CountingStore { inner: mem.clone(), heads: heads.clone(), gets: gets.clone(), delay: Duration::ZERO });

        let config = FoyerCacheConfig::test_config_with("warm_footer_heads", |c| {
            c.parquet_metadata_size_hint = 1024;
        });
        let _dir = CacheDirGuard(config.cache_dir.clone());
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
        Ok(())
    }

    // P0 scan-resistant admission: a wide historical scan must be able to READ
    // through the cache without POPULATING it, or one 14d query evicts the hot
    // tail the whole design exists to keep local.
    #[tokio::test]
    async fn bypass_scope_suppresses_population_but_not_hits() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner.clone(), FoyerCacheConfig::test_config("scan_bypass")).await?;
        let hot = Path::from("tbl/date=2026-01-02/hot.parquet");
        let cold = Path::from("tbl/date=2020-01-01/cold.parquet");
        cache.put(&hot, PutPayload::from_static(b"hot-bytes")).await?; // cached from the write payload
        inner.put(&cold, PutPayload::from_static(b"cold-bytes")).await?; // only in the inner store
        cache.reset_stats().await;

        let (hot_bytes, cold_bytes) =
            scan_bypass_scope(true, async { (cache.get(&hot).await.unwrap().bytes().await.unwrap(), cache.get(&cold).await.unwrap().bytes().await.unwrap()) })
                .await;
        assert_eq!(hot_bytes, Bytes::from_static(b"hot-bytes"));
        assert_eq!(cold_bytes, Bytes::from_static(b"cold-bytes"));
        let stats = cache.get_stats().await;
        assert_eq!(stats.main.hits, 1, "lookups still hit inside a bypass scope");
        assert_eq!(stats.main.inner_gets, 1, "the miss still fetches");
        assert!(!cache.cache.contains(cold.as_ref()), "a bypassed miss must not populate the cache");
        assert!(cache.cache.contains(hot.as_ref()), "the pre-existing hot entry survives the scan");

        // Same read outside the scope populates normally — the suppression is
        // scoped, not a config kill switch.
        cache.get(&cold).await?.bytes().await?;
        assert!(cache.cache.contains(cold.as_ref()));
        assert!(!bypass_active(), "the scope must not leak past its future");

        cache.shutdown().await?;
        Ok(())
    }

    /// A wide scan is bypassed so a ONE-OFF cannot evict the hot set, but a
    /// dashboard panel is the most REPEATED query in the system and under a
    /// blanket bypass could never warm its own working set: measured on prod
    /// 2026-08-09 the same 3d panel went 24.4 s cold -> ~11-15 s warm purely on
    /// the metadata/provider caches the bypass does not touch, while its data
    /// blocks never stuck. Second sighting admits.
    #[tokio::test]
    async fn a_repeated_bypassed_scan_warms_on_the_second_sighting() -> anyhow::Result<()> {
        let inner = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new(inner.clone(), FoyerCacheConfig::test_config("bypass_repeat")).await?;
        let cold = Path::from("tbl/date=2020-01-01/cold.parquet");
        inner.put(&cold, PutPayload::from_static(b"cold-bytes")).await?;

        scan_bypass_scope(true, async { cache.get(&cold).await.unwrap().bytes().await.unwrap() }).await;
        assert!(!cache.cache.contains(cold.as_ref()), "first sighting still declines — a one-off scan pays nothing");

        scan_bypass_scope(true, async { cache.get(&cold).await.unwrap().bytes().await.unwrap() }).await;
        assert!(cache.cache.contains(cold.as_ref()), "second sighting admits, so a refreshing panel converges");

        assert!(!bypass_active(), "the scope must not leak past its future");
        cache.shutdown().await?;
        Ok(())
    }

    /// A parquet file no bigger than the metadata size hint is FULL-FILE
    /// class: its first data-range read pays ONE inner GET that populates the
    /// main tier, and every later range on it is a main-tier hit with zero
    /// inner traffic. Before the fix every range on such a file satisfied the
    /// footer-proximity test and went down the per-(file,range) metadata path
    /// — one R2 GET per distinct range per query, forever (prod 2026-08-03:
    /// main tier read 0/0 while a sea of sub-1MB flush files paid per-range
    /// GETs).
    #[tokio::test]
    async fn tiny_parquet_file_is_cached_whole_not_per_range() -> anyhow::Result<()> {
        use std::sync::atomic::AtomicUsize;
        let mem = Arc::new(InMemory::new());
        let (heads, gets) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let counting = Arc::new(CountingStore { inner: mem.clone(), heads: heads.clone(), gets: gets.clone(), delay: Duration::ZERO });
        let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config("tiny_full")).await?;
        let cache = FoyerObjectStoreCache::new_with_shared_cache(counting, &shared);

        let path = Path::from("tbl/date=2026-01-02/tiny.parquet");
        let body: Vec<u8> = (0..200u8).collect(); // far below the metadata size hint
        mem.put(&path, PutPayload::from(Bytes::from(body.clone()))).await?;

        assert_eq!(&cache.get_range_cached(&path, 0..64).await?[..], &body[0..64]);
        let after_first = gets.load(Ordering::Relaxed);
        assert_eq!(&cache.get_range_cached(&path, 64..128).await?[..], &body[64..128]);
        assert_eq!(&cache.get_range_cached(&path, 10..190).await?[..], &body[10..190]);
        assert_eq!(gets.load(Ordering::Relaxed), after_first, "later ranges must be main-tier hits, not per-range GETs");
        Ok(())
    }

    /// A data-range miss on a large parquet file is range-only. Full-file cache
    /// convergence belongs to upload capture and the post-commit/restart
    /// warmer; doing it from the query path causes severe bandwidth
    /// amplification and competes with the request it is meant to accelerate.
    #[tokio::test]
    async fn large_file_query_miss_reads_ranges_without_warming_full_file() -> anyhow::Result<()> {
        let config = FoyerCacheConfig::test_config_with("large_warm", |c| c.l1_max_entry_bytes = 64);
        let shared = SharedFoyerCache::new(config).await?;
        let mem = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new_with_shared_cache(mem.clone(), &shared);

        let path = Path::from("tbl/date=2026-01-02/big.parquet");
        let body: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        mem.put(&path, PutPayload::from(Bytes::from(body.clone()))).await?;

        assert_eq!(&cache.get_range_cached(&path, 100..200).await?[..], &body[100..200]);
        assert_eq!(&cache.get_range_cached(&path, 200..300).await?[..], &body[200..300]);
        assert!(!shared.contains_data(path.as_ref()), "query ranges must not trigger a full-file cache population");
        let cold = cache.get_stats().await.main;
        assert_eq!(cold.range_misses, 2);
        assert_eq!(cold.range_bytes_read, 200, "inner bytes must track requested ranges, not the 4096-byte object");
        assert_eq!(cold.inner_bytes_read, 200);

        assert_eq!(&cache.get_range_cached(&path, 100..200).await?[..], &body[100..200]);
        let warm = cache.get_stats().await.main;
        assert_eq!(warm.range_hits, 1, "a repeated coalesced range must hit the main range cache");
        assert_eq!(warm.inner_bytes_read, cold.inner_bytes_read, "a range hit must not touch the inner store");
        Ok(())
    }

    #[tokio::test]
    async fn sliding_large_file_ranges_reuse_aligned_cache_entry() -> anyhow::Result<()> {
        let config = FoyerCacheConfig::test_config_with("aligned_ranges", |c| c.l1_max_entry_bytes = 64);
        let shared = SharedFoyerCache::new(config).await?;
        let mem = Arc::new(InMemory::new());
        let cache = FoyerObjectStoreCache::new_with_shared_cache(mem.clone(), &shared);

        let path = Path::from("tbl/date=2026-01-02/large.parquet");
        let body: Vec<u8> = (0..3 * PARQUET_RANGE_ALIGNMENT_BYTES as usize).map(|i| (i % 251) as u8).collect();
        mem.put(&path, PutPayload::from(Bytes::from(body.clone()))).await?;

        let first = 100..1_500_000;
        let shifted = 200..1_400_000;
        assert_eq!(&cache.get_range_cached(&path, first.clone()).await?[..], &body[first.start as usize..first.end as usize]);
        let cold = cache.get_stats().await.main;
        assert_eq!(cold.range_misses, 1);
        assert_eq!(cold.inner_bytes_read, 2 * PARQUET_RANGE_ALIGNMENT_BYTES);

        assert_eq!(&cache.get_range_cached(&path, shifted.clone()).await?[..], &body[shifted.start as usize..shifted.end as usize]);
        let warm = cache.get_stats().await.main;
        assert_eq!(warm.range_hits, 1, "a shifted predicate range in the same aligned window must hit");
        assert_eq!(warm.inner_bytes_read, cold.inner_bytes_read);
        Ok(())
    }

    #[tokio::test]
    async fn contains_data_probes_without_populating() -> anyhow::Result<()> {
        let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config("contains_probe")).await?;
        let path = Path::from("tbl/date=2026-01-02/part.parquet");
        assert!(!shared.contains_data(path.as_ref()), "probe is false before insert");
        let meta = ObjectMeta { location: path.clone(), last_modified: Utc::now(), size: 3, e_tag: None, version: None };
        shared.cache.insert(path.to_string(), CacheValue::new(b"abc".to_vec(), meta));
        assert!(shared.contains_data(path.as_ref()));
        assert!(!shared.contains_data("tbl/date=2026-01-02/other.parquet"));
        shared.shutdown_by(tokio::time::Instant::now() + Duration::from_secs(5)).await?;
        Ok(())
    }

    // The oracle's whole point is that files already captured during upload are
    // free to confirm: only the write-capture gap may cost a fetch.
    #[tokio::test]
    async fn warm_full_if_absent_fetches_only_the_uncaptured_files() -> anyhow::Result<()> {
        use std::sync::atomic::AtomicUsize;
        let mem = Arc::new(InMemory::new());
        let (heads, gets) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let counting = Arc::new(CountingStore { inner: mem.clone(), heads: heads.clone(), gets: gets.clone(), delay: Duration::ZERO });
        let shared = SharedFoyerCache::new(FoyerCacheConfig::test_config("confirm_cached")).await?;
        let cache = FoyerObjectStoreCache::new_with_shared_cache(counting, &shared);

        let captured = Path::from("tbl/date=2026-01-02/captured.parquet");
        let skipped = Path::from("tbl/date=2026-01-02/skipped.parquet");
        cache.put(&captured, PutPayload::from_static(b"captured")).await?; // write-capture path
        mem.put(&skipped, PutPayload::from_static(b"skipped")).await?; // capture was skipped (over cap)

        assert!(!warm_full_if_absent(&cache, &shared, &captured, captured.as_ref()).await, "the captured file costs a probe, not a GET");
        assert!(warm_full_if_absent(&cache, &shared, &skipped, skipped.as_ref()).await, "only the uncaptured file is fetched");
        assert_eq!(gets.load(Ordering::Relaxed), 1);
        assert!(shared.contains_data(skipped.as_ref()), "the gap is cached before the caller drains");

        // Idempotent: a second pass is pure probes.
        assert!(!warm_full_if_absent(&cache, &shared, &captured, captured.as_ref()).await);
        assert!(!warm_full_if_absent(&cache, &shared, &skipped, skipped.as_ref()).await);
        assert_eq!(gets.load(Ordering::Relaxed), 1);

        cache.shutdown().await?;
        Ok(())
    }
}

// ===== snapshot_cache =====
// Local persistence of Delta table snapshots so a restart restores the last
// known state from disk and replays only commits made since, instead of
// rebuilding from checkpoint + log tail on S3 (prod boot replay was the
// dominant cold-start cost). Files live next to the WAL metadata under
// `TIMEFUSION_DATA_DIR/.timefusion_meta/delta_snapshots/` and are
// best-effort: any failure to write or read falls back to a full S3 load.
//
// Format: zstd-compressed JSON of `(FORMAT_VERSION, table_url, state)`.
// JSON (not bincode) because delta-rs's snapshot Serialize uses
// `serialize_seq(None)`, which non-self-describing formats reject.

use std::fs;

use deltalake::table::state::DeltaTableState;

/// Bump on incompatible layout changes (ours or delta-rs's snapshot serde);
/// old files then just miss and the table does a full load.
const FORMAT_VERSION: u32 = 1;

/// Snapshot files untouched for this long belong to dropped or long-idle
/// tables (active ones rewrite theirs every flush).
pub const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

/// FROZEN HASH: this names a FILE on disk, so changing the hasher orphans
/// every existing snapshot sidecar (a cold-start cost, not corruption, but a
/// real one). Not part of the XXH3 sweep for that reason.
fn path_for(dir: &std::path::Path, table_url: &str) -> std::path::PathBuf {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    table_url.hash(&mut h);
    dir.join(format!("{:016x}.json.zst", h.finish()))
}

/// Best-effort atomic persist (tmp + rename, same pattern as the WAL cursor
/// snapshot). Failures are logged, never propagated — persistence is an
/// optimization, not a correctness requirement.
pub fn store_snapshot(dir: &std::path::Path, table_url: &str, state: &DeltaTableState) {
    let path = path_for(dir, table_url);
    let write = || -> anyhow::Result<()> {
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut enc = zstd::Encoder::new(fs::File::create(&tmp)?, 3)?;
        serde_json::to_writer(&mut enc, &(FORMAT_VERSION, table_url, state))?;
        enc.finish()?.sync_all()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    };
    match write() {
        Ok(()) => debug!("Persisted delta snapshot for {table_url} to {path:?}"),
        Err(e) => warn!("Failed to persist delta snapshot for {table_url}: {e}"),
    }
}

/// Load a previously persisted snapshot. Any failure — missing file, corrupt
/// or incompatible payload, table-url mismatch (hash collision) — returns
/// `None` and the caller performs a full load.
pub fn load_snapshot(dir: &std::path::Path, table_url: &str) -> Option<DeltaTableState> {
    let path = path_for(dir, table_url);
    let reader = zstd::Decoder::new(fs::File::open(&path).ok()?).ok()?;
    match serde_json::from_reader::<_, (u32, String, DeltaTableState)>(reader) {
        Ok((FORMAT_VERSION, url, state)) if url == table_url => {
            debug!("Restored delta snapshot for {table_url} at version {}", state.version());
            Some(state)
        }
        Ok((version, url, _)) => {
            debug!("Ignoring delta snapshot {path:?}: version {version} / url {url} does not match {FORMAT_VERSION} / {table_url}");
            None
        }
        Err(e) => {
            warn!("Discarding unreadable delta snapshot {path:?}: {e}");
            let _ = fs::remove_file(&path);
            None
        }
    }
}

/// Remove snapshot files not refreshed within `max_age` (active tables
/// rewrite theirs every flush, so stale files belong to dropped or long-idle
/// tables). Bounds disk growth; best-effort.
pub fn prune_stale(dir: &std::path::Path, max_age: Duration) {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > max_age))
        .for_each(|e| {
            debug!("Pruning stale delta snapshot {:?}", e.path());
            let _ = fs::remove_file(e.path());
        });
}

#[cfg(test)]
mod snapshot_cache_tests {
    use std::{collections::HashMap, sync::Arc};

    use deltalake::{DeltaTable, DeltaTableBuilder};
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;
    use crate::schema::get_default_schema;

    fn mem_store(name: &str) -> anyhow::Result<(Arc<InMemory>, Url)> {
        Ok((Arc::new(InMemory::new()), Url::parse(&format!("memory:///{name}"))?))
    }

    fn builder(mem: &Arc<InMemory>, url: &Url) -> anyhow::Result<DeltaTableBuilder> {
        Ok(DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()))
    }

    async fn create_table(mem: &Arc<InMemory>, url: &Url) -> anyhow::Result<DeltaTable> {
        Ok(builder(mem, url)?.build()?.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?)
    }

    /// A metadata commit through the high-level ops API exercises the same
    /// `CommitBuilder` post-commit hook the flush path uses.
    async fn commit_property(table: DeltaTable) -> anyhow::Result<DeltaTable> {
        Ok(table.set_tbl_properties().with_properties(HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())])).await?)
    }

    fn materialized(table: &DeltaTable) -> bool {
        table.state.as_ref().unwrap().has_materialized_files()
    }

    /// Decisive probe: does a delta-rs commit (the `CommitBuilder` post-commit
    /// hook that produces `finalized.snapshot()` in TF's flush path) preserve
    /// the materialized file list? If a commit drops it, every subsequent
    /// post-commit `snapshot.update()` falls back to a full checkpoint replay —
    /// the 2-8s/flush prod cost — even though load/update preserve it.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_preserves_materialized_files() -> anyhow::Result<()> {
        let (mem, url) = mem_store("commit_mat")?;
        let table = create_table(&mem, &url).await?;
        assert!(materialized(&table), "freshly created table is materialized");

        let table = commit_property(table).await?;
        assert_eq!(table.version(), Some(1));
        assert!(materialized(&table), "post-commit state must stay materialized; if false, every flush full-scans");
        Ok(())
    }

    /// Does a full `.load()` (the path TF takes when there's no usable local
    /// snapshot — e.g. a legacy on-disk snapshot that misses) come back
    /// materialized? If not, every post-commit update stays on the full-scan
    /// branch and never self-heals — the suspected prod cause. Also verifies
    /// `ensure_materialized_files` (Tier A) repairs it.
    #[tokio::test(flavor = "multi_thread")]
    async fn full_load_materialization() -> anyhow::Result<()> {
        let (mem, url) = mem_store("full_load")?;
        commit_property(create_table(&mem, &url).await?).await?;

        let mut loaded = builder(&mem, &url)?.load().await?;
        let materialized_on_load = materialized(&loaded);
        // Tier A must leave the state materialized regardless of how it loaded.
        let log_store = loaded.log_store();
        loaded.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        assert!(materialized(&loaded), "ensure_materialized_files must materialize after a full load");
        eprintln!("FULL_LOAD_MATERIALIZED_ON_LOAD={materialized_on_load}");
        Ok(())
    }

    /// Round-trip: persist a snapshot, restore it into a fresh unloaded
    /// handle, and incrementally catch up to a commit made after the persist.
    /// This is exactly the boot path — restore at version V, replay > V.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_roundtrip_and_incremental_catchup() -> anyhow::Result<()> {
        let (mem, url) = mem_store("snap_tbl")?;
        let table = create_table(&mem, &url).await?;
        assert_eq!(table.version(), Some(0));

        let dir = tempfile::tempdir()?;
        store_snapshot(dir.path(), url.as_str(), table.state.as_ref().unwrap());

        // External commit after the persist — restore must catch up to it.
        commit_property(table).await?;

        let state = load_snapshot(dir.path(), url.as_str()).expect("persisted snapshot loads");
        assert_eq!(state.version(), 0);
        let mut restored = builder(&mem, &url)?.build()?;
        restored.state = Some(state);
        restored.update_state().await?;
        assert_eq!(restored.version(), Some(1), "restored snapshot must incrementally reach the latest commit");

        // The fork persists the materialized file list (MaterializedFilesWire),
        // so a restored snapshot must come back materialized and STAY that way
        // across updates — otherwise post-commit updates fall back to a full
        // checkpoint replay (the 2-8s/flush prod cost). Guard the whole chain:
        // ensure (idempotent) → update (incremental) → reconcile rebuild.
        assert!(materialized(&restored), "restored snapshot must come back materialized");
        let log_store = restored.log_store();
        restored.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        restored.update_state().await?;
        assert!(materialized(&restored), "materialization must survive update (stays incremental)");
        restored.state.as_mut().unwrap().rematerialize_files(log_store.as_ref()).await?;
        assert!(materialized(&restored), "rematerialize_files keeps the file list materialized");

        // Wrong table url (or hash collision) must miss, not mis-restore.
        assert!(load_snapshot(dir.path(), "memory:///other_tbl").is_none());
        Ok(())
    }
}

// ===== certification_store =====
// Best-effort durable record of sweep certifications.
//
// `dedup_clean_fp` is process-local, so every restart begins with zero certified
// partitions and the read-side dedup skip starts from cold. TF deploys several
// times a day, which is why the skip was measured firing on 0.2–0.5% of
// Delta-reading scans (`docs/plans/2026-08-11-certification-survival.md`).
//
// What is stored is exactly what `record_certification` decided — the
// fingerprint it proved the partition clean over, never a verdict re-derived at
// a different strictness. Nothing here can widen certification: a loaded entry
// is subject to the same fingerprint-equality check against the live file list
// as an in-memory one, so a stale or corrupted record can only cost a skip, not
// grant a wrong one.

use std::io::ErrorKind;

/// Newest-first cap on what is written. Bounds the file for a process that has
/// certified a very large number of partitions; the tail it drops is the oldest,
/// which is also the likeliest to have been invalidated already.
pub const PERSIST_CAP: usize = 20_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredCertification {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    /// `partition_file_fp` over the file set the certifying pass proved clean.
    pub fp: u64,
    /// Wall-clock ms since the epoch at which it was granted. Wall-clock rather
    /// than a monotonic instant precisely because it has to survive the process:
    /// it exists so dwell measures a certification's real lifetime instead of
    /// restarting at every deploy.
    pub granted_unix_ms: u64,
    /// The file paths the certifying pass proved clean, for the per-FILE skip.
    /// `default` so stores written before this field load as "no per-file
    /// evidence" rather than failing — those certifications still serve the
    /// whole-partition skip through `fp`.
    #[serde(default)]
    pub files: Vec<String>,
    /// Whether the certification may still grant the WHOLE-PARTITION skip, or
    /// only vouch for the files it names.
    ///
    /// Load-bearing across restarts: slice-derived certifications are stale by
    /// construction (they proved one time window, never a day), and prod
    /// restarts on every deploy. Restoring them as non-stale would let a proof
    /// about ten minutes satisfy a day-wide skip. `default` = false so stores
    /// written before this field keep their old meaning: they only ever held
    /// whole-day grants.
    #[serde(default)]
    pub stale: bool,
}

/// Wall-clock ms since the epoch, now.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// How long ago `granted_unix_ms` was, or `None` if it is in the future — which
/// a backwards clock jump or a hand-edited file can produce, and which must not
/// become a nonsense dwell.
pub fn age_since(granted_unix_ms: u64) -> Option<std::time::Duration> {
    now_unix_ms().checked_sub(granted_unix_ms).map(std::time::Duration::from_millis)
}

// ===== dirty_bin_queue =====
// Best-effort durable metadata for sealed-bin dedup scheduling.

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirtyBin {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub bin: i64,
    /// The bin width `bin` was computed at. Carried PER RECORD rather than per
    /// file so a sidecar written across a width change still reads correctly,
    /// and so an old file (no field) means the historical 10 minutes.
    #[serde(default = "default_bin_minutes")]
    pub width_minutes: i64,
}

fn default_bin_minutes() -> i64 {
    crate::database::DEFAULT_BIN_MINUTES
}

/// Every bin at `to_micros` that overlaps bin `bin` of width `from_micros`.
///
/// Both directions and non-multiple widths, because the only safe error is an
/// over-approximation: marking a clean bin dirty costs one probe, while missing
/// a dirty bin leaves duplicates in place forever. Widening maps many old bins
/// onto one new; narrowing fans one old bin out across several.
pub fn remap_bin(bin: i64, from_micros: i64, to_micros: i64) -> std::ops::RangeInclusive<i64> {
    if from_micros == to_micros {
        return bin..=bin;
    }
    let lo = bin.saturating_mul(from_micros);
    let hi = lo.saturating_add(from_micros - 1);
    lo.div_euclid(to_micros)..=hi.div_euclid(to_micros)
}

/// Sidecar files in the WAL meta dir (certifications, dirty bins): best-effort,
/// never load-bearing. A missing, unreadable or corrupt file degrades to "empty"
/// with a warning rather than failing a boot, and a failed store is logged and
/// dropped — losing one costs re-derived work, never correctness.
pub fn load_sidecar<T: serde::de::DeserializeOwned>(data_dir: &std::path::Path, (file, what): (&str, &str)) -> Vec<T> {
    let path = crate::write::wal::meta_path(data_dir, file);
    match fs::read(&path).map(|data| serde_json::from_slice(&data)) {
        Ok(Ok(items)) => items,
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable {what}");
            Vec::new()
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load {what}");
            Vec::new()
        }
    }
}

/// Returns whether the write landed. Deliberately not `#[must_use]`: the hint
/// stores (certifications, dirty bins) are best-effort by design and losing one
/// costs a recomputation, so their call sites are right to ignore this. A store
/// whose contents are an AUTHORITY is not — see `JsonCoverageLedger::persist`.
pub fn store_sidecar<T: Serialize>(data_dir: &std::path::Path, (file, what): (&str, &str), items: &[T]) -> bool {
    use std::io::Write;
    let path = crate::write::wal::meta_path(data_dir, file);
    // The SERIALIZE is the expensive half here, and it sat outside the wrap
    // `write_atomic_with` already has: the coverage ledger persists
    // write-through on every rollup publication and re-encodes every cell it
    // has ever seen, so this is a multi-MB `to_vec` on a runtime worker at
    // maintenance frequency. (Nested `block_in_place` is fine — pinned by
    // `support::tests::nested_helper_calls_are_allowed`.)
    let result = crate::support::without_blocking_the_worker(|| {
        path.parent()
            .map_or(Ok(()), fs::create_dir_all)
            .and_then(|()| serde_json::to_vec(items).map_err(std::io::Error::other))
            // Reuses the WAL's tmp+rename helper, which also cleans up the tmp file on failure.
            .and_then(|bytes| crate::write::wal::write_atomic_with(&path, false, |f| f.write_all(&bytes)))
    });
    if let Err(error) = result {
        warn!(%error, "failed to persist {what}");
        return false;
    }
    true
}

/// Sidecar file names, paired with the label their warnings use.
pub const CERTIFICATIONS: (&str, &str) = ("dedup_certifications.json", "certification store");
pub const SLICE_COVERAGE: (&str, &str) = ("dedup_slice_coverage.json", "slice coverage store");
pub const UNTAGGED_CELLS: (&str, &str) = ("rollup_untagged_cells.json", "untagged tier cell store");

/// A `(source, project, tier table, date)` partition holding tier files with no
/// identity tags, persisted so the DAMAGE RANK survives a restart.
///
/// The repair units themselves are already durable — they live in the task
/// journal — but their priority was not: `claim_next` reads a runtime set that
/// only `recover_rollup_coverage` fills, and that runs ~40 minutes after boot.
/// Prod 2026-08-23 restarted four times in one hour, so the rank was never once
/// active and the queued repairs drained at the slow unprioritised rate.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredUntaggedCell {
    pub source: String,
    pub project_id: String,
    pub table_name: String,
    pub date: String,
}

/// One partition's accumulated clean-slice intervals, persisted write-through so
/// certification evidence survives the restarts that the journal's Complete
/// marks do (a day straddling a restart was otherwise permanently
/// uncertifiable — cert_granted_total=0, diagnosed 2026-08-21).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredSliceCoverage {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub fp: u64,
    pub intervals: Vec<(i64, i64)>,
}
pub const DIRTY_BINS: (&str, &str) = ("dedup_dirty_bins.json", "dirty-bin queue");

pub const ROLLUP_COVERAGE: (&str, &str) = ("rollup_coverage_ledger.json", "rollup coverage ledger");

/// One slice of a rollup tier, and what it was built FROM.
///
/// Today the same facts live in Delta metadata tags on each parquet file
/// (`maintenance_coordinator::TAG_SLICE_START` and friends), which makes the
/// files self-describing and makes coverage cost a full log replay to read. It
/// also means an unrelated `OPTIMIZE` that drops custom tags destroys coverage
/// state, and that two files covering different slices can never be compacted
/// into one — the tier built to make 30d queries fast is itself thousands of
/// small files.
///
/// See `docs/plans/2026-08-24-open-work-after-untagged-convergence.md` §3.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct CoverageEntry {
    pub start_micros: i64,
    /// EXCLUSIVE, like `TimeSlice`, and unlike a file statistic's `hi`.
    pub end_micros: i64,
    pub generation: String,
    pub source_fingerprint: u64,
    /// The witness that this slice is not stale: the source partition's PHYSICAL
    /// `num_records` sum at build time, i.e. exactly `TAG_SOURCE_ROWS`, which is
    /// what `record_readable_coverage` copies in here.
    ///
    /// It is deliberately the same computation the read path re-derives, because
    /// comparing unlike quantities is worse than not comparing at all — see
    /// `rollup::slice_coverage_agrees`. Being physical is also its defect: a
    /// dedup rewrite or a DML delete changes `num_records` without changing what
    /// the rollup aggregated, and ingest anywhere else in the day moves it too,
    /// so correct slices are voided. See
    /// `docs/plans/2026-08-25-rollup-witness-design.md`. `None` means no witness,
    /// which every read must treat as unverifiable rather than fresh.
    pub source_rows: Option<i64>,
    /// The tier files that SERVE this range.
    ///
    /// Without this the ledger can say a range is covered but not what to read
    /// for it, so file selection would still have to come from the tags and the
    /// tags could never be dropped — the ledger would be a supplement rather
    /// than a replacement, and files would stay non-anonymous and therefore
    /// non-compactable, which is the whole point of moving coverage off them.
    ///
    /// It is also what makes an on-demand slice split possible later: a hole
    /// inside a wide covering file cannot be republished today because the file
    /// physically holds the hole's rows and re-tagging does not remove them, so
    /// both copies would be summed. Naming files per range is the prerequisite
    /// for a reader that can prefer one over the other.
    #[serde(default)]
    pub files: Vec<String>,
    /// The measure columns these files actually MATERIALIZED, from
    /// `TAG_MEASURES`. `None` on an entry written before the tag existed, which
    /// the read path treats as unproven rather than as "no measures" — the
    /// distinction `#[serde(default)]` preserves for ledgers already on disk.
    #[serde(default)]
    pub measures: Option<Vec<String>>,
}

/// `(source, project_id, tier table, date)` — the partition a coverage entry
/// belongs to, and the unit retirement and repair both work in.
pub type CoverageCell = (String, String, String, String);

/// Where rollup coverage is recorded and read.
///
/// A trait rather than a concrete store because the sidecar JSON here is the
/// FIRST backend, not the intended one: this state is destined for a real
/// datastore (Postgres, SlateDB) where a point read does not mean loading every
/// cell. Keeping callers on this interface makes that a backend swap.
///
/// **The decision, recorded 2026-08-24:** the real backend is DEFERRED, and JSON
/// is the fallback rather than the plan. Nothing is blocked on it — the tags are
/// still the authority and the ledger is still being verified against them — so
/// the datastore is worth choosing when the ledger becomes load-bearing, not
/// before. What must not happen in the meantime is callers reaching past this
/// trait to `JsonCoverageLedger`, because that is what would turn a backend swap
/// back into a rewrite. Two known JSON limits set the bar the replacement has to
/// clear: `CoverageEntry.files` makes the sidecar grow with the FILE count
/// (~13,800 live tier files in prod, not the cell count), and every change
/// re-serializes every cell — `replace_many` bounds that to one write per tier
/// pass and cannot do better within a single-document store.
///
/// **Ordering is the safety property.** The Delta commit and the ledger write
/// are not one transaction, and the two failure modes are not symmetric:
///
/// - the ledger UNDERSTATES coverage — a wasted rebuild, cheap;
/// - the ledger CLAIMS coverage that is not there — wrong query results.
///
/// So `record` is called only AFTER the commit lands, and a crash in between
/// leaves an understating ledger. Never the reverse order.
pub trait CoverageLedger: Send + Sync {
    fn coverage(&self, cell: &CoverageCell) -> Vec<CoverageEntry>;
    /// Record a slice that is ALREADY COMMITTED. See the ordering note above.
    fn record(&self, cell: &CoverageCell, entry: CoverageEntry);
    /// Drop a cell entirely — its partition aged out, or its coverage is being
    /// rebuilt from scratch.
    /// Replace a cell's coverage wholesale with what a replay just proved.
    ///
    /// `record` alone is APPEND-ONLY, and coverage is not: a slice rebuilt under
    /// a new generation supersedes the old entry rather than joining it, and a
    /// slice whose files were removed leaves nothing behind at all. Re-recording
    /// every hour without replacing would accumulate superseded entries forever
    /// and — worse than the size — keep serving ranges whose files are gone,
    /// which is the ledger's one unacceptable failure: claiming coverage that is
    /// not there.
    fn replace(&self, cell: &CoverageCell, entries: Vec<CoverageEntry>);
    /// Apply a whole tier's worth of replacements as ONE durable write.
    ///
    /// `replace` persists the entire ledger, so calling it per cell costs
    /// O(cells^2) serialization — and the recovery pass that writes this ledger
    /// walks every cell of a tier, hourly, with every file path in every write.
    /// A batch API is not a convenience here, it is the difference between one
    /// write and thousands.
    ///
    /// An empty entry list retires that cell, same as `replace`.
    fn replace_many(&self, cells: Vec<(CoverageCell, Vec<CoverageEntry>)>) {
        for (cell, entries) in cells {
            self.replace(&cell, entries);
        }
    }
    /// Drop a cell entirely — its partition aged out, or its coverage is being
    /// rebuilt from scratch.
    fn retire(&self, cell: &CoverageCell);
    fn cells(&self) -> Vec<CoverageCell>;
}

/// Merge entries of the SAME generation whose slices touch or overlap.
///
/// Without this a cell accumulates one entry per incremental build forever, and
/// the ledger — which is meant to make coverage a cheap read — grows without
/// bound. Generations are kept apart because a differing generation means the
/// slices were built from different source content; merging those would invent
/// a range no single build ever produced.
pub fn merge_coverage(mut entries: Vec<CoverageEntry>) -> Vec<CoverageEntry> {
    entries.sort_by(|a, b| (&a.generation, a.start_micros).cmp(&(&b.generation, b.start_micros)));
    let mut merged: Vec<CoverageEntry> = Vec::with_capacity(entries.len());
    for entry in entries {
        match merged.last_mut() {
            Some(last) if last.generation == entry.generation && entry.start_micros <= last.end_micros => {
                last.end_micros = last.end_micros.max(entry.end_micros);
                // A merged range is only as trustworthy as its weakest part: one
                // witness-less contributor makes the whole span unverifiable,
                // which is the conservative direction (a rebuild, not a false
                // claim of freshness).
                last.source_rows = match (last.source_rows, entry.source_rows) {
                    (Some(a), Some(b)) => Some(a.saturating_add(b)),
                    _ => None,
                };
                // Union, not replace: a merged range is served by every file
                // that served any part of it, and dropping one would make the
                // ledger describe a range it cannot actually produce.
                last.files.extend(entry.files);
                last.files.sort_unstable();
                last.files.dedup();
            }
            _ => merged.push(entry),
        }
    }
    merged
}

/// One `(cell -> entries)` row as it is persisted. Flat on purpose: a tuple key
/// is not a JSON object key, and a flat row is also what a SQL backend wants.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredCoverage {
    pub source: String,
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub entries: Vec<CoverageEntry>,
}

/// The first `CoverageLedger` backend: in memory, written through to the same
/// best-effort JSON sidecar the certification and dirty-bin stores use.
///
/// Write-through rather than periodic, for the reason certifications had to
/// become write-through (2026-08-21): this box restarts constantly, and state
/// that only reaches disk on a clean shutdown is state that never reaches disk.
#[derive(Debug)]
pub struct JsonCoverageLedger {
    data_dir: std::path::PathBuf,
    cells: dashmap::DashMap<CoverageCell, Vec<CoverageEntry>>,
}

impl JsonCoverageLedger {
    pub fn load(data_dir: impl Into<std::path::PathBuf>) -> Self {
        let data_dir = data_dir.into();
        let cells = load_sidecar::<StoredCoverage>(&data_dir, ROLLUP_COVERAGE)
            .into_iter()
            .map(|row| ((row.source, row.project_id, row.table_name, row.date), row.entries))
            .collect();
        Self { data_dir, cells }
    }

    /// Write-through, and COUNTED when it fails.
    ///
    /// The sidecar helper warns and continues, which is correct for a hint and
    /// wrong for an authority: a dropped write means the in-memory ledger and
    /// the disk copy diverge, and only the memory copy knows. Losing one is safe
    /// while the Delta tags remain the authority — the ledger merely understates
    /// coverage and costs a rebuild — so this does NOT fail the recovery pass.
    /// It has to be VISIBLE, though, because a ledger that silently stopped
    /// persisting reads exactly like a ledger with nothing to say:
    /// `coverage_ledger_persist_failures` beside `coverage_ledger_disagreements`
    /// is what says which one it is. Both must read zero before the tags can go.
    fn persist(&self) {
        let rows: Vec<StoredCoverage> = self
            .cells
            .iter()
            .map(|entry| {
                let ((source, project_id, table_name, date), entries) = (entry.key().clone(), entry.value().clone());
                StoredCoverage { source, project_id, table_name, date, entries }
            })
            .collect();
        if !store_sidecar(&self.data_dir, ROLLUP_COVERAGE, &rows) {
            crate::observability::maintenance_stats().coverage_ledger_persist_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// The covered ranges this ledger claims for one `(source, tier table)`,
    /// keyed by project — the shape the ROUTING path needs, as opposed to the
    /// per-cell shape maintenance needs.
    ///
    /// This is the bridge for moving reads off the tags. Routing currently
    /// answers "is this window covered, and is that coverage current?" from
    /// `rollup_slice_coverage`, which is rebuilt from Delta tags. The same
    /// question answered from here is what lets the tags go.
    ///
    /// Ranges are returned MERGED, so this is deliberately coarser than the
    /// per-slice map: two adjacent slices of one generation are one range here
    /// and two entries there. The covered SET is identical, which is the only
    /// thing routing asks.
    pub fn routing_view(&self, source: &str, table_name: &str) -> std::collections::HashMap<String, Vec<CoverageEntry>> {
        let mut by_project: std::collections::HashMap<String, Vec<CoverageEntry>> = std::collections::HashMap::new();
        for cell in self.cells.iter() {
            let (cell_source, project_id, cell_table, _date) = cell.key();
            if cell_source != source || cell_table != table_name {
                continue;
            }
            by_project.entry(project_id.clone()).or_default().extend(cell.value().iter().cloned());
        }
        by_project.into_iter().map(|(project, entries)| (project, merge_coverage(entries))).collect()
    }

    /// Drop cells whose date is older than `keep_from` (a `YYYY-MM-DD` bound),
    /// returning how many went.
    ///
    /// Retirement is not optional: without it the ledger grows for as long as
    /// the process has ever seen a partition, including partitions that aged out
    /// of retention months ago. Dates are compared as strings, which is exactly
    /// right for zero-padded ISO dates and wrong for anything else — the format
    /// is fixed by the Delta partition value.
    pub fn retire_before(&self, keep_from: &str) -> usize {
        let stale: Vec<CoverageCell> = self.cells.iter().filter(|e| e.key().3.as_str() < keep_from).map(|e| e.key().clone()).collect();
        for cell in &stale {
            self.cells.remove(cell);
        }
        if !stale.is_empty() {
            self.persist();
        }
        stale.len()
    }
}

impl CoverageLedger for JsonCoverageLedger {
    fn coverage(&self, cell: &CoverageCell) -> Vec<CoverageEntry> {
        self.cells.get(cell).map(|entries| entries.clone()).unwrap_or_default()
    }

    fn record(&self, cell: &CoverageCell, entry: CoverageEntry) {
        self.cells.entry(cell.clone()).or_default().push(entry);
        if let Some(mut entries) = self.cells.get_mut(cell) {
            let merged = merge_coverage(std::mem::take(&mut *entries));
            *entries = merged;
        }
        self.persist();
    }

    fn replace(&self, cell: &CoverageCell, entries: Vec<CoverageEntry>) {
        if entries.is_empty() {
            self.retire(cell);
            return;
        }
        self.cells.insert(cell.clone(), merge_coverage(entries));
        self.persist();
    }

    /// One write for the whole batch. See the trait's note on why this exists.
    fn replace_many(&self, cells: Vec<(CoverageCell, Vec<CoverageEntry>)>) {
        if cells.is_empty() {
            return;
        }
        for (cell, entries) in cells {
            if entries.is_empty() {
                self.cells.remove(&cell);
            } else {
                self.cells.insert(cell, merge_coverage(entries));
            }
        }
        self.persist();
    }

    fn retire(&self, cell: &CoverageCell) {
        if self.cells.remove(cell).is_some() {
            self.persist();
        }
    }

    fn cells(&self) -> Vec<CoverageCell> {
        self.cells.iter().map(|entry| entry.key().clone()).collect()
    }
}

#[cfg(test)]
mod bin_remap_tests {
    use super::*;

    const MIN: i64 = 60 * 1_000_000;

    /// Re-keying the dirty-bin queue across a width change must never LOSE a
    /// dirty bin; gaining one costs a probe, missing one leaves duplicates in
    /// place forever. So every case asserts the new bins fully COVER the old
    /// bin's time span, in both directions and at a non-multiple width.
    #[test_case::test_case(7, 10, 60, 1..=1; "widen 10->60: six old bins collapse onto one")]
    #[test_case::test_case(6, 10, 60, 1..=1; "widen: the first bin of the hour")]
    #[test_case::test_case(5, 10, 60, 0..=0; "widen: the last bin of hour zero")]
    #[test_case::test_case(1, 60, 10, 6..=11; "narrow 60->10: one old bin fans out to six")]
    #[test_case::test_case(2, 10, 45, 0..=0; "non-multiple, contained")]
    #[test_case::test_case(4, 10, 45, 0..=1; "non-multiple, straddling a boundary")]
    #[test_case::test_case(3, 10, 10, 3..=3; "same width is the identity")]
    #[test_case::test_case(-1, 10, 60, -1..=-1; "negative bins (pre-epoch) stay contained")]
    fn remap_bin_covers_the_old_span(bin: i64, from_min: i64, to_min: i64, expect: std::ops::RangeInclusive<i64>) {
        let (from, to) = (from_min * MIN, to_min * MIN);
        let got = remap_bin(bin, from, to);
        assert_eq!(got, expect, "bin {bin} at {from_min}min -> {to_min}min");
        // The property the table exists to protect, asserted independently of
        // the expected values: the new bins must cover every instant of the old.
        let (lo, hi) = (bin * from, (bin + 1) * from - 1);
        assert!(*got.start() * to <= lo, "first new bin must start at or before the old bin");
        assert!((*got.end() + 1) * to > hi, "last new bin must end at or after the old bin");
    }
}

#[cfg(test)]
mod coverage_ledger_tests {
    use super::*;

    fn entry(start: i64, end: i64, generation: &str, rows: Option<i64>) -> CoverageEntry {
        CoverageEntry {
            start_micros: start,
            end_micros: end,
            generation: generation.to_owned(),
            source_fingerprint: 7,
            source_rows: rows,
            files: vec![format!("{start}-{end}.parquet")],
            measures: None,
        }
    }

    fn cell() -> CoverageCell {
        ("otel_logs_and_spans".to_owned(), "p".to_owned(), "tier".to_owned(), "2026-08-24".to_owned())
    }

    /// A cell gains an entry per incremental build. Left unmerged the ledger
    /// grows without bound, which defeats the entire reason it exists — coverage
    /// as a cheap read instead of a log replay.
    #[test]
    fn touching_slices_of_one_generation_merge() {
        let merged = merge_coverage(vec![entry(0, 10, "g1", Some(3)), entry(10, 20, "g1", Some(4))]);
        assert_eq!(merged.len(), 1, "adjacent slices of one generation are one range");
        assert_eq!((merged[0].start_micros, merged[0].end_micros), (0, 20));
        assert_eq!(merged[0].source_rows, Some(7), "the witness is the sum of what was merged");
        assert_eq!(merged[0].files.len(), 2, "a merged range is served by every file that served any part of it");
    }

    /// Different generations were built from different source content. Merging
    /// them would invent a range no build ever produced, and the ledger is the
    /// authority — an invented range is served as truth.
    #[test]
    fn different_generations_never_merge() {
        let merged = merge_coverage(vec![entry(0, 10, "g1", Some(3)), entry(10, 20, "g2", Some(4))]);
        assert_eq!(merged.len(), 2, "a generation boundary is a real boundary");
    }

    /// The witness is what lets a read trust a slice is not stale, so a merged
    /// range is only as trustworthy as its weakest contributor. Summing `3 +
    /// unknown` into `3` would manufacture a witness for rows nobody counted —
    /// and it would fail in the dangerous direction, claiming freshness.
    #[test]
    fn a_witnessless_slice_poisons_the_witness_of_the_range_it_merges_into() {
        let merged = merge_coverage(vec![entry(0, 10, "g1", Some(3)), entry(10, 20, "g1", None)]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].source_rows, None, "unverifiable beats a manufactured count");
    }

    /// Write-through, not on shutdown. Certifications had to learn this the hard
    /// way (2026-08-21): a box that restarts constantly never reaches a clean
    /// shutdown, so state that only persists then never persists at all.
    #[test]
    fn coverage_survives_a_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ledger = JsonCoverageLedger::load(dir.path());
        ledger.record(&cell(), entry(0, 10, "g1", Some(5)));

        let reloaded = JsonCoverageLedger::load(dir.path());
        assert_eq!(reloaded.coverage(&cell()), vec![entry(0, 10, "g1", Some(5))], "a recorded slice is on disk before the process ends");
    }

    /// `record` is append-only; coverage is not. A slice rebuilt under a new
    /// generation SUPERSEDES the old entry, and re-recording every hour without
    /// replacing would keep the superseded range live forever — serving coverage
    /// whose files are gone, which is the one failure the ledger must never have.
    #[test]
    fn replacing_a_cell_drops_superseded_entries_rather_than_accumulating_them() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ledger = JsonCoverageLedger::load(dir.path());
        ledger.record(&cell(), entry(0, 10, "g1", Some(5)));
        ledger.record(&cell(), entry(20, 30, "g1", Some(5)));
        assert_eq!(ledger.coverage(&cell()).len(), 2, "two disjoint ranges are two entries");

        ledger.replace(&cell(), vec![entry(0, 10, "g2", Some(9))]);

        assert_eq!(ledger.coverage(&cell()), vec![entry(0, 10, "g2", Some(9))], "what the replay proved is ALL the cell holds");
        assert_eq!(JsonCoverageLedger::load(dir.path()).coverage(&cell()).len(), 1, "and the replacement is durable");
    }

    /// `replace` persists the WHOLE ledger, so the recovery pass — which walks
    /// every cell of a tier, hourly, with every file path in every entry — must
    /// not call it per cell. That is O(cells^2) serialization on the boot path.
    #[test]
    fn a_batch_replacement_is_one_durable_write() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ledger = JsonCoverageLedger::load(dir.path());
        let other = ("s".to_owned(), "q".to_owned(), "tier".to_owned(), "2026-08-24".to_owned());
        ledger.record(&cell(), entry(0, 10, "g1", Some(1)));
        ledger.record(&other, entry(0, 10, "g1", Some(1)));

        // One cell replaced, one retired by an empty list, in a single call.
        ledger.replace_many(vec![(cell(), vec![entry(20, 30, "g2", Some(4))]), (other.clone(), Vec::new())]);

        assert_eq!(ledger.coverage(&cell()), vec![entry(20, 30, "g2", Some(4))], "the replacement applied");
        assert!(ledger.coverage(&other).is_empty(), "an empty list retires the cell, same as replace");
        let reloaded = JsonCoverageLedger::load(dir.path());
        assert_eq!(reloaded.coverage(&cell()).len(), 1, "and the batch reached disk");
        assert!(reloaded.coverage(&other).is_empty(), "including the retirement");
    }

    /// A ledger that cannot write must SAY SO. It goes on serving what it holds
    /// in memory — which is the safe behaviour while the tags are the authority —
    /// and the only thing distinguishing that from a healthy ledger is this
    /// counter.
    ///
    /// `data_dir` is a FILE, so creating the `.timefusion_meta` directory under
    /// it fails on every platform regardless of who is running the test. A
    /// permission bit would not: CI runs as root often enough that a chmod-based
    /// test passes for the wrong reason.
    #[test]
    fn a_ledger_that_cannot_reach_disk_counts_the_loss() {
        let dir = tempfile::tempdir().expect("temp dir");
        let blocked = dir.path().join("not-a-directory");
        std::fs::write(&blocked, b"").expect("write file");
        let ledger = JsonCoverageLedger::load(&blocked);
        let before = crate::observability::maintenance_stats().coverage_ledger_persist_failures.load(std::sync::atomic::Ordering::Relaxed);

        ledger.record(&cell(), entry(0, 10, "g1", Some(5)));

        assert!(
            crate::observability::maintenance_stats().coverage_ledger_persist_failures.load(std::sync::atomic::Ordering::Relaxed) > before,
            "a failed persist must be visible in timefusion_stats, not only in a log line"
        );
        assert_eq!(ledger.coverage(&cell()).len(), 1, "and the in-memory ledger still answers — losing the write is not losing the coverage");
    }

    /// Replacing with nothing means the replay found no coverage at all, which
    /// is a retirement — not a cell holding an empty list, which would read back
    /// as "known to have no coverage" and is a different claim.
    #[test]
    fn replacing_with_nothing_retires_the_cell() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ledger = JsonCoverageLedger::load(dir.path());
        ledger.record(&cell(), entry(0, 10, "g1", Some(5)));
        ledger.replace(&cell(), Vec::new());
        assert!(ledger.cells().is_empty(), "the cell is gone, not empty");
    }

    #[test]
    fn retiring_drops_only_dates_before_the_bound() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ledger = JsonCoverageLedger::load(dir.path());
        let old = ("s".to_owned(), "p".to_owned(), "tier".to_owned(), "2026-07-01".to_owned());
        ledger.record(&old, entry(0, 10, "g1", Some(1)));
        ledger.record(&cell(), entry(0, 10, "g1", Some(1)));

        assert_eq!(ledger.retire_before("2026-08-01"), 1, "only the July cell is past retention");
        assert!(ledger.coverage(&old).is_empty(), "the retired cell is gone");
        assert_eq!(ledger.coverage(&cell()).len(), 1, "the in-retention cell is untouched");
        assert!(JsonCoverageLedger::load(dir.path()).coverage(&old).is_empty(), "retirement is durable, not in-memory only");
    }
}
