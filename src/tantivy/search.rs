//! Searches in-window sidecar indexes and unions their hits.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! Missing blobs are downloaded, unpacked, atomically installed, memory-mapped,
//! and retained in a process LRU. Immutable blob paths need only eviction.
//!
//! [`TantivySearchService::reap_disk_cache`] bounds extracted indexes because
//! object-store GC cannot see them and the cache shares disk with the WAL.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime},
};

use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use lru::LruCache;
use object_store::{ObjectStore, path::Path as ObjPath};
use parking_lot::Mutex;
use tantivy::{Index, IndexReader};

use crate::tantivy::{
    MANIFEST_PREFIX, Manifest, SCHEMA_VERSION, load_manifest,
    udf::{PredNode, TextMatchPred},
    upsert_manifest,
};

/// Per-phase timings and counts for one process's tantivy read path, so the
/// prefilter's cost can be attributed from `timefusion_stats` instead of by
/// differencing query wall-clock on a loaded box. Sums and counts only —
/// they divide into means and that is all this needs to answer.
///
/// Every counter here was added because the path had NONE: the pre-existing
/// `index_opens` was incremented and never read by anything, and the OTel
/// counters don't reach pgwire, so the biggest cost centre on the read path
/// could not be attributed in-process at all.
#[derive(Debug, Default)]
pub struct SearchStats {
    pub manifest_loads: AtomicU64,
    pub manifest_load_us: AtomicU64,
    pub manifest_hits: AtomicU64,
    pub blob_fetches: AtomicU64,
    pub blob_fetch_us: AtomicU64,
    pub index_opens: AtomicU64,
    pub index_open_us: AtomicU64,
    pub reader_hits: AtomicU64,
    pub searches: AtomicU64,
    pub search_us: AtomicU64,
    /// Indexes consulted across all queries — the fan-out this path is
    /// dominated by. Divided by `queries`, this is indexes-per-query.
    pub indexes_searched: AtomicU64,
    pub queries: AtomicU64,
    /// Hits actually materialized (doc-store reads) by prefilter searches.
    /// The fat-needle abort must keep this O(max_hits), never O(matches).
    pub hits_materialized: AtomicU64,
    /// Extracted-index dirs seeded by the indexer at publish time, i.e. S3
    /// round trips this process avoided by keeping what it just built.
    pub cache_seeded: AtomicU64,
    pub cache_seed_failures: AtomicU64,
    /// Turning the manifest into the work list: the schema-version scan, the
    /// time-prune, and the `covered_files` union — all of which walk EVERY
    /// entry, not just the in-window ones, and clone their URI strings.
    pub plans: AtomicU64,
    pub plan_us: AtomicU64,
    /// Per-index setup that precedes the search proper: `ensure_cached`'s
    /// `last_used` stamp and segment stat, plus the reader-LRU lookup. Charged
    /// separately because a fully-resident, fully-open index still pays it once
    /// per index per query, and `search_us` deliberately starts after it.
    pub prepares: AtomicU64,
    pub prepare_us: AtomicU64,
    /// WALL time of the whole fan-out — driving every per-index task to
    /// completion and merging their hits. `fanout_us - prepare_us - search_us`
    /// is the result-merge bookkeeping (the hit/coverage/row-selection sets),
    /// which no other counter can see.
    pub fanouts: AtomicU64,
    pub fanout_us: AtomicU64,
}

impl SearchStats {
    fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Ordering::Relaxed);
    }
    /// Record an elapsed duration against a (count, micros) pair.
    fn timed(count: &AtomicU64, micros: &AtomicU64, started: Instant) {
        Self::add(count, 1);
        Self::add(micros, started.elapsed().as_micros() as u64);
    }
}

/// `timed` as an RAII guard, for a phase with early returns. The fan-out exits
/// three ways (`?`, the max_hits abort, the no-usable-index path); charging it
/// only on the happy path would make the abort — the expensive case — invisible.
struct TimedPhase<'a> {
    count: &'a AtomicU64,
    micros: &'a AtomicU64,
    started: Instant,
}

impl<'a> TimedPhase<'a> {
    fn new(count: &'a AtomicU64, micros: &'a AtomicU64) -> Self {
        Self { count, micros, started: Instant::now() }
    }
}

impl Drop for TimedPhase<'_> {
    fn drop(&mut self) {
        SearchStats::timed(self.count, self.micros, self.started);
    }
}

#[derive(Debug)]
pub struct SearchResult {
    pub hits: Vec<Hit>,
    /// Rows covered by queried manifest entries.
    pub indexed_rows: u64,
    /// Files covered by every successful entry, including time-pruned entries.
    pub covered_files: HashSet<String>,
    /// Covered files proven to have no hits and safe to skip.
    pub zero_hit_files: HashSet<String>,
    /// Matching row ordinals for single-file, parquet-ordered indexes.
    pub row_selections: HashMap<String, Vec<u64>>,
    /// Whether an in-window index lacks a queried field, requiring a full scan.
    pub field_coverage_gap: bool,
}

#[derive(Debug)]
pub struct TantivySearchService {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache_root: PathBuf,
    pub config: Arc<TantivyConfig>,
    /// Per-phase attribution for the read path; surfaced via `timefusion_stats`.
    pub stats: SearchStats,
    readers: Mutex<LruCache<PathBuf, (Index, IndexReader)>>,
    /// TTL cache of parsed manifests, keyed (table, project). Per-service
    /// (not global) so distinct object stores never cross-contaminate.
    manifests: DashMap<(String, String), (Instant, Arc<Manifest>)>,
    /// Last time each cache dir was served to a query — the reaper's recency
    /// signal. mmap reads don't reliably move a directory's atime, so the
    /// filesystem cannot be asked what is hot. Dirs absent here (never touched
    /// by this process, i.e. the post-restart case) fall back to dir mtime,
    /// which is their unpack time.
    last_used: DashMap<PathBuf, SystemTime>,
}

/// Outcome of one [`TantivySearchService::reap_disk_cache`] sweep.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReapReport {
    pub dirs_scanned: usize,
    pub bytes_before: u64,
    pub dirs_removed: usize,
    pub bytes_removed: u64,
    pub errors: usize,
}

impl TantivySearchService {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_root: PathBuf, config: Arc<TantivyConfig>) -> Self {
        Self {
            object_store,
            cache_root,
            readers: Mutex::new(LruCache::new(config.reader_cache_entries())),
            config,
            stats: SearchStats::default(),
            manifests: DashMap::new(),
            last_used: DashMap::new(),
        }
    }

    /// Single-predicate convenience used by tests/tools.
    pub async fn search(&self, table: &str, project_id: &str, field: &str, query_str: &str) -> Result<Option<Vec<Hit>>> {
        let node = PredNode::Leaf(TextMatchPred { column: field.to_string(), query: query_str.to_string() });
        Ok(self.search_with_stats(table, project_id, &node, usize::MAX, None).await?.map(|r| r.hits))
    }

    /// Search every usable in-window index with ONE combined boolean query
    /// per index and union the hits (indexes cover disjoint row sets).
    /// Aborts (returns `Ok(None)`) once cumulative hits exceed `max_hits` —
    /// the caller treats the result as "too noisy to push down" and falls
    /// back to full scan.
    ///
    /// `time_range` is the query's `[lo, hi]` timestamp window (micros), if any.
    /// Entries whose `[min,max]_timestamp_micros` can't overlap it are skipped
    /// without downloading their blob — so a `trace_id =` over a 1h window
    /// touches only the indexes covering that window, not every index the
    /// project ever built (the cold-old-data latency cliff). Pruning is sound:
    /// a non-overlapping index only covers rows outside the window, which the
    /// query's own timestamp filter excludes anyway.
    ///
    /// Returns:
    /// - `Ok(None)` — no usable index, or hit cap exceeded.
    /// - `Ok(Some(SearchResult))` — search ran to completion within bounds.
    pub async fn search_with_stats(
        &self, table: &str, project_id: &str, node: &PredNode, max_hits: usize, time_range: Option<(i64, i64)>,
    ) -> Result<Option<SearchResult>> {
        let m = self.load_manifest_cached(table, project_id).await?;
        if m.entries.is_empty() {
            return Ok(None);
        }
        let plan_started = Instant::now();
        let current = || m.entries.iter().filter(|(_, e)| e.schema_version == SCHEMA_VERSION);
        // Coverage ignores the time-prune below: a pruned entry still covers its
        // file, its rows are merely out of window (see SearchResult docs).
        let covered_files: HashSet<String> =
            current().filter(|(_, e)| e.index.is_some() && e.error.is_none()).flat_map(|(_, e)| e.covered_files.iter().cloned()).collect();
        // Time-prune: skip indexes whose timestamp span can't overlap the query
        // window (no blob download). Conservative on unknown bounds.
        // Work item: (file_uuid, blob_path, rows, entry covered_files, ordinals_valid).
        let work: Vec<_> = current()
            .filter_map(|(key, e)| {
                let blob_path = e.index.as_ref().filter(|_| entry_overlaps(e.min_timestamp_micros, e.max_timestamp_micros, time_range))?;
                Some((file_uuid(key).to_string(), blob_path.clone(), e.rows, e.covered_files.clone(), e.ordinals_valid && e.covered_files.len() == 1))
            })
            .collect();

        // One download+open+search task per index, SEARCH_CONCURRENCY-wide.
        // `None` hits = the index lacks a queried field (coverage gap).
        SearchStats::add(&self.stats.queries, 1);
        SearchStats::add(&self.stats.indexes_searched, work.len() as u64);
        SearchStats::timed(&self.stats.plans, &self.stats.plan_us, plan_started);
        let _fanout = TimedPhase::new(&self.stats.fanouts, &self.stats.fanout_us);
        let mut tasks = futures::stream::iter(work.into_iter().map(|(file_uuid, blob_path, rows, entry_covered, ordinals_valid)| async move {
            let prepare_started = Instant::now();
            let dir = self.ensure_cached(table, project_id, &file_uuid, &blob_path).await?;
            let (index, reader) = self.open_cached(&dir).with_context(|| format!("open index {file_uuid}"))?;
            SearchStats::timed(&self.stats.prepares, &self.stats.prepare_us, prepare_started);
            let started = Instant::now();
            let out = match build_node_query(&index, node)? {
                PredsQuery::MissingField => Some((None, rows, entry_covered, ordinals_valid)),
                PredsQuery::Query(q) => {
                    let searcher = reader.searcher();
                    // Count-first: a raw per-index match count over `max_hits`
                    // already forces the abort verdict (the prefilter can no
                    // longer prove completeness), so establish it with the
                    // cheap Count collector instead of materializing hits —
                    // a 4.5M-match needle cost 4-6s of plan time per query
                    // doing doc-store reads it then threw away (prod
                    // 2026-08-22). The limit on the materializing search is a
                    // backstop; `search()` passes usize::MAX, hence saturating.
                    let count = searcher.search(&*q, &tantivy::collector::Count).map_err(|e| anyhow!("count: {e}"))?;
                    if count > max_hits {
                        None
                    } else {
                        let hits = query_with_searcher(&searcher, &*q, Some(max_hits.saturating_add(1)))?;
                        SearchStats::add(&self.stats.hits_materialized, hits.len() as u64);
                        Some((Some(hits), rows, entry_covered, ordinals_valid))
                    }
                }
            };
            SearchStats::timed(&self.stats.searches, &self.stats.search_us, started);
            Ok::<_, anyhow::Error>(out)
        }))
        .buffer_unordered(self.config.search_concurrency());

        // Imperative: seven interdependent accumulators plus an early abort once
        // `max_hits` is exceeded — a fold would only hide the control flow.
        let mut all_hits: Vec<Hit> = Vec::new();
        let mut seen: HashSet<(i64, String)> = HashSet::new();
        let mut any_usable = false;
        let mut indexed_rows: u64 = 0;
        let mut field_coverage_gap = false;
        let mut zero_hit_files: HashSet<String> = HashSet::new();
        // Files covered by a hitting or field-gapped entry can never be
        // pruned, even if another (double-covering) entry saw zero hits.
        let mut unprunable_files: HashSet<String> = HashSet::new();
        let mut row_selections: HashMap<String, Vec<u64>> = HashMap::new();
        // Files where some covering entry can't express its hits as ordinals
        // — a partial selection would UNDER-select, so drop theirs entirely.
        let mut unselectable_files: HashSet<String> = HashSet::new();
        while let Some(res) = tasks.next().await {
            // Per-index overflow: some index alone exceeds `max_hits`.
            let Some((hits, rows, entry_covered, ordinals_valid)) = res? else {
                return Ok(None);
            };
            let Some(hits) = hits else {
                // An in-window index that can't answer a queried field (e.g.
                // built before the column was indexed) is a coverage hole the
                // file-level `covered_files` set can't see — signal the caller
                // to skip the prefilter rather than drop this file's matches.
                field_coverage_gap = true;
                unprunable_files.extend(entry_covered);
                continue;
            };
            indexed_rows = indexed_rows.saturating_add(rows);
            if ordinals_valid && hits.iter().all(|h| h.row_ordinal.is_some()) {
                // covered_files.len()==1 guaranteed by the work-item gate; the
                // empty-hits filter avoids materializing an empty selection.
                if let Some(uri) = entry_covered.first().filter(|_| !hits.is_empty()) {
                    row_selections.entry(uri.clone()).or_default().extend(hits.iter().filter_map(|h| h.row_ordinal));
                }
            } else {
                unselectable_files.extend(entry_covered.iter().cloned());
            }
            if hits.is_empty() {
                zero_hit_files.extend(entry_covered);
            } else {
                unprunable_files.extend(entry_covered);
            }
            for h in hits {
                if seen.insert((h.timestamp_micros, h.id.clone())) {
                    all_hits.push(h);
                    if all_hits.len() > max_hits {
                        return Ok(None);
                    }
                }
            }
            any_usable = true;
        }
        if !any_usable {
            return Ok(None);
        }
        zero_hit_files.retain(|f| !unprunable_files.contains(f));
        row_selections.retain(|f, _| !unselectable_files.contains(f));
        Ok(Some(SearchResult { hits: all_hits, indexed_rows, covered_files, zero_hit_files, row_selections, field_coverage_gap }))
    }

    /// Warm the local disk cache with every blob whose data is at most
    /// `days` old, across all projects of `table`. Turns the cold-window
    /// download cliff after a restart into a background cost. Best-effort:
    /// individual blob failures are skipped.
    pub async fn warm_recent(self: &Arc<Self>, table: &str, days: u32) -> Result<usize> {
        let cutoff = crate::support::now_micros() - i64::from(days) * 86_400_000_000;
        let prefix = ObjPath::from(format!("{}/{table}", MANIFEST_PREFIX));
        let objs: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        // Imperative: every step is awaited IO whose failures are individually skipped.
        let mut warmed = 0usize;
        for meta in objs.iter().filter(|m| m.location.as_ref().ends_with("/manifest.json")) {
            // .../{project}/manifest.json
            let Some(project) = meta.location.as_ref().rsplit('/').nth(1) else {
                continue;
            };
            let Ok(m) = load_manifest(self.object_store.as_ref(), table, project).await else {
                continue;
            };
            // Owned, not borrowed from `m`: the warm tasks below must be
            // 'static to be driven concurrently from the cron.
            let recent: Vec<(String, String)> = m
                .entries
                .iter()
                .filter(|(_, e)| e.schema_version == SCHEMA_VERSION && e.max_timestamp_micros.is_some_and(|mx| mx >= cutoff))
                .filter_map(|(key, e)| Some((file_uuid(key).to_string(), e.index.as_ref()?.clone())))
                .collect();
            // Concurrent, at the same width as a query's fan-out. Sequentially
            // this was one round trip at a time: a cold start after a restart
            // has ~550 blobs to pull for the largest project alone, so serial
            // warming took minutes per project and the "hot window is local"
            // guarantee did not hold until long after the box was serving.
            // Already-resident blobs cost one `has_any_segment` stat each, so a
            // steady-state pass stays cheap at this width.
            warmed += futures::stream::iter(recent.into_iter().map(|(uuid, blob)| {
                let (me, table, project) = (Arc::clone(self), table.to_string(), project.to_string());
                async move { me.ensure_cached(&table, &project, &uuid, &blob).await }
            }))
            .buffer_unordered(self.config.search_concurrency())
            .filter(|r| futures::future::ready(r.is_ok()))
            .count()
            .await;
        }
        Ok(warmed)
    }

    /// TTL-cached manifest read (see `MANIFEST_CACHE_TTL` for the staleness
    /// argument). Removes the per-query S3 GET + JSON parse.
    async fn load_manifest_cached(&self, table: &str, project_id: &str) -> Result<Arc<Manifest>> {
        let key = (table.to_string(), project_id.to_string());
        if let Some(m) = self.manifests.get(&key).filter(|e| e.0.elapsed() < self.config.manifest_ttl()).map(|e| e.1.clone()) {
            SearchStats::add(&self.stats.manifest_hits, 1);
            return Ok(m);
        }
        let started = Instant::now();
        let m = Arc::new(load_manifest(self.object_store.as_ref(), table, project_id).await?);
        SearchStats::timed(&self.stats.manifest_loads, &self.stats.manifest_load_us, started);
        self.manifests.insert(key, (Instant::now(), m.clone()));
        Ok(m)
    }

    /// Fold a just-published entry into the cached manifest instead of dropping
    /// it, so this process sees its own write without paying to reload all of
    /// it. Where there is nothing cached, do nothing — the next read loads it.
    ///
    /// This replaced a `remove()`, and the difference is not cosmetic: with a
    /// remove, prod measured **manifest_hit_pct = 0.0 across 56 loads for 54
    /// queries** — busy projects publish far more often than they are queried,
    /// so every publish threw away the entry the next query needed and the
    /// 300s TTL bought exactly nothing. Updating in place keeps the cache warm
    /// while still never letting our own write go unseen.
    pub fn apply_published_entry(&self, table: &str, project_id: &str, key: &str, entry: crate::tantivy::ManifestEntry) {
        // `entry()` holds the shard lock across the read-modify-write. A plain
        // get-clone-insert would let two concurrent publishes for the same
        // project (a flush overlapping a backfill/compaction build) each start
        // from the same snapshot, so the second write would silently drop the
        // first one's entry — leaving a covered file looking uncovered here
        // until the TTL expired. The S3 manifest is unaffected either way,
        // since `upsert_manifest` serializes per (table, project).
        let dashmap::mapref::entry::Entry::Occupied(mut occupied) = self.manifests.entry((table.to_string(), project_id.to_string())) else {
            // Nothing cached: the next read loads it, including this entry.
            return;
        };
        let (loaded_at, current) = occupied.get();
        // Keep the ORIGINAL load time. Refreshing it here would mean a project
        // that publishes every few minutes never re-reads its manifest at all,
        // so writers we don't observe (the repair CLI) would be invisible
        // forever rather than for at most one TTL.
        let (loaded_at, mut updated) = (*loaded_at, (**current).clone());
        updated.entries.insert(key.to_string(), entry);
        occupied.insert((loaded_at, Arc::new(updated)));
    }

    /// Drop a cached manifest so the next read reloads it from S3. The GC
    /// counterpart of `apply_published_entry`, and deliberately the opposite
    /// treatment: GC deletes blobs, so a manifest cached across it routes the
    /// plan path at objects that are gone for up to a full TTL. That is soft —
    /// the prefilter reads a missing blob as "no usable index" — but it is a
    /// wasted lookup where latency is the whole point.
    ///
    /// Drop rather than install the pruned manifest: a concurrent publish may
    /// have folded in an entry the GC's snapshot predates, and installing would
    /// silently drop it, recreating the covered-file-looks-uncovered bug
    /// `apply_published_entry` exists to avoid. A reload from S3 is
    /// authoritative. Affordable only because GC runs once per project per
    /// hour — on the publish path, which fires constantly, this same `remove`
    /// is what drove the hit rate to 0%.
    pub fn invalidate_manifest(&self, table: &str, project_id: &str) {
        self.manifests.remove(&(table.to_string(), project_id.to_string()));
    }

    /// LRU-cached open, keyed by cache dir — 1:1 with the (immutable) blob
    /// path, so entries are never stale, and the reaper can drop the reader
    /// for a dir it deletes under that same key.
    fn open_cached(&self, dir: &Path) -> Result<(Index, IndexReader)> {
        if let Some(v) = self.readers.lock().get(dir) {
            SearchStats::add(&self.stats.reader_hits, 1);
            return Ok(v.clone());
        }
        let started = Instant::now();
        let index = super::open_index(dir)?;
        let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
        SearchStats::timed(&self.stats.index_opens, &self.stats.index_open_us, started);
        self.readers.lock().put(dir.to_path_buf(), (index.clone(), reader.clone()));
        Ok((index, reader))
    }

    async fn ensure_cached(&self, table: &str, project_id: &str, file_uuid: &str, blob_path: &str) -> Result<PathBuf> {
        let dir = super::local_cache_path(&self.cache_root, table, project_id, file_uuid);
        // Stamped on every hit, not only on miss: recency is what the reaper
        // sorts by, and a dir serving a query every minute must never look as
        // old as its unpack time.
        self.last_used.insert(dir.clone(), SystemTime::now());
        if has_any_segment(&dir) {
            return Ok(dir);
        }
        let started = Instant::now();
        let blob = super::download(self.object_store.as_ref(), &ObjPath::from(blob_path)).await?;
        install_blob_into_cache(&dir, &blob)?;
        SearchStats::timed(&self.stats.blob_fetches, &self.stats.blob_fetch_us, started);
        Ok(dir)
    }

    /// Bound the extracted-index disk tree at `budget_bytes`, evicting
    /// least-recently-used dirs until it fits. Blocking IO — call from
    /// `spawn_blocking`.
    ///
    /// Eviction is pure cache loss, never a correctness risk: every dir is an
    /// immutable extraction of an object-store blob, and the next query that
    /// wants it re-downloads through `ensure_cached`. That is also why this
    /// consults nothing but the filesystem — it does NOT need to know which
    /// parquet files are still live, so a compacted-away file's dir is reaped
    /// by the same rule that reaps a merely cold one: nobody opened it.
    ///
    /// Unlinking a dir whose index is still mmap'd is safe on Unix (existing
    /// mappings stay valid), but the space is not reclaimed until the last
    /// mapping drops — so the reader-LRU entry is dropped with the dir.
    ///
    /// Racing `ensure_cached` is likewise only ever a cache miss: the loser
    /// either re-downloads, or fails `open_index` on a half-deleted dir and
    /// its query falls back to a full scan.
    pub fn reap_disk_cache(&self, budget_bytes: u64) -> ReapReport {
        let root = self.cache_root.join("tantivy_cache");
        let mut entries = collect_index_dirs(&root);
        let mut report = ReapReport { dirs_scanned: entries.len(), bytes_before: entries.iter().map(|e| e.bytes).sum(), ..Default::default() };
        let mut live = report.bytes_before;
        if live <= budget_bytes {
            return report;
        }
        // Coldest first; a `last_used` stamp from this process beats dir mtime.
        entries.sort_by_key(|e| self.last_used.get(&e.dir).map_or(e.mtime, |v| *v));
        for entry in entries {
            if live <= budget_bytes {
                break;
            }
            self.readers.lock().pop(&entry.dir);
            self.last_used.remove(&entry.dir);
            match std::fs::remove_dir_all(&entry.dir) {
                Ok(()) => {
                    live = live.saturating_sub(entry.bytes);
                    report.dirs_removed += 1;
                    report.bytes_removed += entry.bytes;
                    prune_empty_parents(&entry.dir, &root);
                }
                // Already gone, or racing an unpack — either way not ours to
                // account for, and retried on the next sweep.
                Err(_) => report.errors += 1,
            }
        }
        report
    }
}

/// One extracted index directory, as the reaper sees it.
struct CachedDir {
    dir: PathBuf,
    bytes: u64,
    /// Unpack time, the recency fallback for dirs this process never served
    /// (post-restart, or another process's leftovers).
    mtime: SystemTime,
}

/// Walk `root` and return every extracted index directory with its size. A
/// directory is a leaf index iff it holds `meta.json`; anything else is an
/// interior node of the `{table}/{project}/{file_uuid}` tree and is descended
/// into. Crashed-unpack leftovers (`tempfile`'s `.tmpXXXX` dirs) hold a
/// `meta.json` too, so they are collected — with an old mtime and no
/// `last_used` entry they sort to the very front of the eviction order, which
/// is exactly where they belong.
///
/// The list is materialized because eviction has to sort it globally. That is
/// on the order of 100 bytes per index dir, so it stays clear of mattering on
/// a memory-tight box even with a cache in the hundreds of GB.
fn collect_index_dirs(root: &Path) -> Vec<CachedDir> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else { continue };
        // Materialized so the leaf test can be made before descending.
        let children: Vec<_> = rd.flatten().collect();
        if children.iter().any(|c| c.file_name() == "meta.json") {
            let (bytes, mtime) = children
                .iter()
                .filter_map(|c| c.metadata().ok())
                .fold((0u64, SystemTime::UNIX_EPOCH), |(b, t), m| (b + m.len(), t.max(m.modified().unwrap_or(SystemTime::UNIX_EPOCH))));
            out.push(CachedDir { dir, bytes, mtime });
        } else {
            stack.extend(children.iter().filter(|c| c.file_type().is_ok_and(|t| t.is_dir())).map(|c| c.path()));
        }
    }
    out
}

/// Remove now-empty ancestors of a reaped dir, stopping at `root` (kept) or at
/// the first non-empty directory. Without this the `{table}/{project}`
/// skeleton outlives every index it ever held.
fn prune_empty_parents(dir: &Path, root: &Path) {
    let mut cur = dir.parent();
    while let Some(p) = cur.filter(|p| *p != root && p.starts_with(root)) {
        if std::fs::remove_dir(p).is_err() {
            return; // non-empty, or racing a concurrent unpack — leave it
        }
        cur = p.parent();
    }
}

/// Whether a manifest entry's `[min,max]` timestamp span could contain rows in
/// the query's `[lo,hi]` window. Conservative by design: an entry with unknown
/// bounds (`None`, e.g. legacy or a failed-stats build) always overlaps so it's
/// never wrongly pruned, and a `None` query range (no timestamp filter) matches
/// everything. Correctness rests on this never returning `false` for an entry
/// that covers an in-window row.
fn entry_overlaps(min: Option<i64>, max: Option<i64>, range: Option<(i64, i64)>) -> bool {
    range.is_none_or(|(lo, hi)| max.unwrap_or(i64::MAX) >= lo && min.unwrap_or(i64::MIN) <= hi)
}

/// Strips the manifest key's `bucket-` tag to recover the bare file UUID
/// (see `service.rs`'s `bucket_key` format).
fn file_uuid(key: &str) -> &str {
    key.strip_prefix("bucket-").unwrap_or(key)
}

/// Unpack `blob` into a temp dir adjacent to `dir`, then atomically rename it
/// into place. Shared by the reader (after a download) and the indexer (right
/// after publishing a blob it just built) so a freshly built index is never
/// fetched back from S3 to answer the first query that needs it.
///
/// Concurrency-safe by construction: unpack happens in a private temp dir and
/// only the rename is observable, so a query downloading the same blob and an
/// indexer seeding it can race freely — whoever renames second finds the dir
/// present and keeps the winner's copy. Blob paths are immutable, so the two
/// copies are byte-identical anyway.
///
/// Deliberately does NOT stamp `last_used`: a seeded dir's mtime is its unpack
/// time, which the reaper already treats as the recency fallback, so it sorts
/// as newest and is evicted last. That is what the seeding is for.
fn install_blob_into_cache(dir: &Path, blob: &bytes::Bytes) -> Result<()> {
    let parent = dir.parent().ok_or_else(|| anyhow!("cache path has no parent"))?;
    std::fs::create_dir_all(parent).context("mkdir cache parent")?;
    let tmp = tempfile::TempDir::new_in(parent).context("tempdir for unpack")?;
    super::unpack_to_dir(blob, tmp.path())?;
    // Bound in a `let` so the `tmp.path()` borrow ends before the arms move `tmp`.
    let renamed = std::fs::rename(tmp.path(), dir);
    match renamed {
        Ok(()) => drop(tmp.keep()),  // disarm cleanup: the dir now lives at `dir`
        Err(_) if dir.exists() => {} // someone else won the race
        Err(e) => return Err(e).context("rename into cache"),
    }
    Ok(())
}

/// True if `dir` already holds an extracted index (a `seg*` file or `meta.json`).
fn has_any_segment(dir: &Path) -> bool {
    std::fs::read_dir(dir).is_ok_and(|rd| {
        rd.flatten().any(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            name.starts_with("seg") || name == "meta.json"
        })
    })
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::SystemTime};

    use super::{TantivySearchService, collect_index_dirs, entry_overlaps};

    /// Fake extracted index of `bytes` bytes at `<root>/tantivy_cache/<rel>`.
    fn fake_index(root: &std::path::Path, rel: &str, bytes: usize) -> PathBuf {
        let dir = root.join("tantivy_cache").join(rel);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("meta.json"), "{}").unwrap();
        std::fs::write(dir.join("seg0.store"), vec![b'x'; bytes]).unwrap();
        dir
    }

    fn service(root: &std::path::Path) -> TantivySearchService {
        TantivySearchService::new(Arc::new(object_store::memory::InMemory::new()), root.to_path_buf(), Arc::new(crate::config::TantivyConfig::default()))
    }

    #[test]
    fn collect_walks_to_leaves_and_never_into_them() {
        let tmp = tempfile::tempdir().unwrap();
        fake_index(tmp.path(), "tbl/proj-a/0f0f-aaaa", 100);
        fake_index(tmp.path(), "tbl/proj-b/bucket-uuid", 50);
        let dirs = collect_index_dirs(&tmp.path().join("tantivy_cache"));
        assert_eq!(dirs.len(), 2, "one entry per leaf, regardless of nesting depth");
        // meta.json is 2 bytes; a leaf's size is the whole dir, not just the payload.
        let mut sizes: Vec<u64> = dirs.iter().map(|d| d.bytes).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![52, 102]);
    }

    #[test]
    fn reap_evicts_coldest_first_until_under_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let (cold, warm) = (fake_index(tmp.path(), "tbl/p/cold", 1000), fake_index(tmp.path(), "tbl/p/warm", 1000));
        // `warm` was served by a query; `cold` never was.
        svc.last_used.insert(warm.clone(), SystemTime::now());

        let report = svc.reap_disk_cache(1500);
        assert_eq!(report.dirs_scanned, 2);
        assert_eq!(report.dirs_removed, 1, "evicts only as much as the budget demands");
        assert!(!cold.exists(), "the dir no query touched goes first");
        assert!(warm.exists(), "the recently-served dir survives");
        assert!(report.bytes_before - report.bytes_removed <= 1500);
        // Emptied ancestors go with it, but the cache root itself stays.
        assert!(tmp.path().join("tantivy_cache").exists());
        assert!(!tmp.path().join("tantivy_cache/tbl/p/cold").exists());
    }

    #[test]
    fn reap_under_budget_is_a_no_op() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let dir = fake_index(tmp.path(), "tbl/p/only", 1000);
        let report = svc.reap_disk_cache(u64::MAX);
        assert_eq!((report.dirs_removed, report.bytes_removed), (0, 0));
        assert!(dir.exists());
    }

    #[test]
    fn reap_on_a_missing_root_reports_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let report = service(tmp.path()).reap_disk_cache(0);
        assert_eq!((report.dirs_scanned, report.dirs_removed), (0, 0));
    }

    #[test]
    fn reap_drops_the_reader_entry_with_the_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let dir = fake_index(tmp.path(), "tbl/p/x", 1000);
        // A stale reader pinning the mmap would hold the space despite the unlink.
        assert!(svc.reap_disk_cache(0).dirs_removed == 1);
        assert!(svc.readers.lock().peek(&dir).is_none());
        assert!(svc.last_used.get(&dir).is_none());
    }

    #[test]
    fn time_prune_overlap_logic() {
        // overlapping span is kept
        assert!(entry_overlaps(Some(10), Some(20), Some((15, 25))));
        assert!(entry_overlaps(Some(10), Some(20), Some((5, 12))));
        // entirely before / after the window is pruned
        assert!(!entry_overlaps(Some(10), Some(20), Some((21, 30))));
        assert!(!entry_overlaps(Some(40), Some(50), Some((21, 30))));
        // an unknown bound is treated permissively (won't wrongly prune), but a
        // KNOWN bound still prunes correctly even if the other side is unknown.
        assert!(entry_overlaps(None, None, Some((21, 30)))); // both unknown → keep
        assert!(entry_overlaps(Some(10), None, Some((100, 200)))); // max unknown, min below hi → keep
        assert!(!entry_overlaps(None, Some(5), Some((100, 200)))); // known max 5 < lo 100 → safely pruned
        assert!(!entry_overlaps(Some(300), None, Some((100, 200)))); // known min 300 > hi 200 → safely pruned
        // no query range matches everything (today's behavior)
        assert!(entry_overlaps(Some(10), Some(20), None));
    }
}

// ===== reader =====
// Run text/range queries against a built tantivy index and return
// `(timestamp_micros, id)` candidate pairs for downstream Delta filtering.
//
// `build_preds_query` is the single place SQL-side `text_match` predicates
// become a tantivy query (AND of per-field parsed queries) — shared by the
// Delta sidecar search and the MemBuffer bucket index so both interpret
// predicates identically.

use tantivy::{
    Searcher, TantivyDocument, Term,
    collector::TopDocs,
    query::{BooleanQuery, Occur, Query, QueryParser, TermQuery},
    schema::{Field, FieldType, IndexRecordOption, Value},
};

use crate::tantivy::{ID_FIELD, NGRAM3_TOKENIZER, ROW_ORDINAL_FIELD, TS_FIELD};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Hit {
    pub timestamp_micros: i64,
    pub id: String,
    /// Row offset within the covered parquet file, when the index carries the
    /// `_row_ordinal` fast field. Only meaningful for read-back-built indexes
    /// (`ManifestEntry.ordinals_valid`) — see schema.rs.
    pub row_ordinal: Option<u64>,
}

/// Outcome of compiling predicates against a concrete index's schema.
pub enum PredsQuery {
    Query(Box<dyn Query>),
    /// The index predates one of the queried fields (schema evolution) —
    /// it cannot answer the predicate; callers must treat this as a
    /// coverage gap, not an empty result.
    MissingField,
}

/// Compile `preds` (implicitly AND-ed) into one tantivy query for `index`.
pub fn build_preds_query(index: &Index, preds: &[TextMatchPred]) -> Result<PredsQuery> {
    let node = PredNode::from_preds(preds).ok_or_else(|| anyhow!("no predicates"))?;
    build_node_query(index, &node)
}

/// Compile a routable predicate tree into one tantivy query for `index`:
/// And→all-Must, Or→all-Should (matches ≥1). Leaf parsing is conjunctive
/// across tokens — critical for n-gram: "hello" tokenizes into trigrams
/// `hel`,`ell`,`llo` and ALL must match (a single matching trigram doesn't
/// imply substring presence).
pub fn build_node_query(index: &Index, node: &PredNode) -> Result<PredsQuery> {
    let q: Box<dyn Query> = match node {
        PredNode::Leaf(p) => {
            let schema = index.schema();
            let Ok(field) = schema.get_field(&p.column) else {
                return Ok(PredsQuery::MissingField);
            };
            let tokenizer = match schema.get_field_entry(field).field_type() {
                FieldType::Str(o) => o.get_indexing_options().map(|i| i.tokenizer().to_string()),
                _ => None,
            };
            match &tokenizer {
                // On ngram3 a trailing `*` is only a routing marker (prefix ⊆
                // substring), so the literal can always be analyzed. On raw/
                // default it carries real prefix semantics that a conjunction of
                // whole terms would UNDER-match, so those keep the parser.
                Some(tok) if tok.as_str() == NGRAM3_TOKENIZER || !p.query.ends_with('*') => analyzed_conjunction_query(index, field, tok, &p.query)?,
                _ => {
                    let mut qp = QueryParser::for_index(index, vec![field]);
                    qp.set_conjunction_by_default();
                    qp.parse_query(&p.query).map_err(|e| anyhow!("parse query '{}': {e}", p.query))?
                }
            }
        }
        PredNode::And(kids) | PredNode::Or(kids) => {
            let occur = if matches!(node, PredNode::And(_)) { Occur::Must } else { Occur::Should };
            // Collecting into `Result<Option<_>>` short-circuits on both errors
            // and the first unanswerable child.
            let subs = kids
                .iter()
                .map(|k| {
                    build_node_query(index, k).map(|r| match r {
                        PredsQuery::Query(q) => Some((occur, q)),
                        PredsQuery::MissingField => None,
                    })
                })
                .collect::<Result<Option<Vec<_>>>>()?;
            let Some(subs) = subs else { return Ok(PredsQuery::MissingField) };
            Box::new(BooleanQuery::new(subs))
        }
    };
    Ok(PredsQuery::Query(q))
}

/// Compile a routed literal against `field` WITHOUT tantivy's `QueryParser`:
/// run the field's own analyzer over the literal and AND the resulting terms.
///
/// The parser's grammar is fatal here: routed literals are user data, and a
/// whitespace-adjacent `-` (`"accept -header"`) becomes a MustNot clause while
/// bare `AND`/`OR`/`NOT` become operators — and on a raw-tokenized field any
/// whitespace splits one indexed token into two unmatchable ones. Such a query
/// parses "successfully" and returns ZERO hits, so the intersecting
/// `id IN (hits)` prefilter silently drops matching rows.
///
/// Matching semantics vs. the old parser path: `QueryParser` turned a
/// multi-token word into a PhraseQuery (for ngram3, consecutive trigrams). A
/// conjunction of the same terms is a strict SUPERSET of that — always sound
/// for a prefilter, which may only ever over-select. Preserved from the old
/// path: whitespace splits into independently-required words, a word too short
/// to produce a token simply broadens (is dropped), and a literal that yields
/// no token at all errors out into a full scan.
fn analyzed_conjunction_query(index: &Index, field: Field, tokenizer: &str, query: &str) -> Result<Box<dyn Query>> {
    let mut analyzer = index.tokenizers().get(tokenizer).ok_or_else(|| anyhow!("tokenizer {tokenizer} not registered"))?;
    // ngram3 keeps the parser's whitespace-AND behaviour (each word required
    // independently, short words dropped). The other tokenizers must see the
    // WHOLE literal: `default` splits it itself, and `raw` indexes the value as
    // one token, so pre-splitting it would under-match.
    let words: Vec<&str> = if tokenizer == NGRAM3_TOKENIZER { query.split_whitespace().collect() } else { vec![query] };
    let mut seen: HashSet<String> = HashSet::new();
    let clauses: Vec<(Occur, Box<dyn Query>)> = words
        .iter()
        // Plan-time classification appends `*` for `LIKE 'foo%'`; on a 3-gram
        // field a prefix match is a subset of the substring match, so dropping
        // the marker keeps the prefilter a superset. (Non-ngram3 `*` queries
        // never reach here — see the caller.) `process` sinks into a Vec because
        // tantivy hands tokens to an `FnMut` sink, not an iterator.
        .flat_map(|word| {
            let mut terms = Vec::new();
            analyzer.token_stream(word.trim_end_matches('*')).process(&mut |t| terms.push(t.text.clone()));
            terms
        })
        .filter(|t| seen.insert(t.clone()))
        .map(|t| (Occur::Must, Box::new(TermQuery::new(Term::from_field_text(field, &t), IndexRecordOption::Basic)) as Box<dyn Query>))
        .collect();
    if clauses.is_empty() {
        return Err(anyhow!("query '{query}' produces no {tokenizer} terms"));
    }
    Ok(Box::new(BooleanQuery::new(clauses)))
}

/// Run a tantivy `Query` against the index and return hits up to `limit`.
/// `limit = None` returns up to a hard cap (currently 1M) to bound memory.
pub fn query_index(index: &Index, query: &dyn Query, limit: Option<usize>) -> Result<Vec<Hit>> {
    let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
    query_with_searcher(&reader.searcher(), query, limit)
}

/// As `query_index`, but with a caller-provided (cached) searcher.
/// Hit extraction prefers fast fields (`_timestamp` is always FAST; `_id`
/// is FAST on indexes built after 2026-07-05) and falls back to the doc
/// store per segment for older indexes.
pub fn query_with_searcher(searcher: &Searcher, query: &dyn Query, limit: Option<usize>) -> Result<Vec<Hit>> {
    let schema = searcher.schema();
    let ts_field = schema.get_field(TS_FIELD).map_err(|e| anyhow!("missing _timestamp: {e}"))?;
    let id_field = schema.get_field(ID_FIELD).map_err(|e| anyhow!("missing _id: {e}"))?;

    const HIT_CAP: usize = 1_000_000;
    // Clamp even explicit limits: `search()` passes usize::MAX through
    // max_hits, and tantivy's TopDocs multiplies the limit internally —
    // an unclamped huge limit overflows (debug panic).
    let top = searcher.search(query, &TopDocs::with_limit(limit.unwrap_or(HIT_CAP).min(HIT_CAP))).map_err(|e| anyhow!("search: {e}"))?;
    // Per-segment fast-field columns, resolved lazily on first hit in that
    // segment. `None` in the outer Option = not yet resolved; inner `None`
    // = this segment has no fast `_id` (pre-fast-field index) → doc store.
    type FfCols = (tantivy::columnar::Column<i64>, tantivy::columnar::StrColumn, Option<tantivy::columnar::Column<u64>>);
    let mut ff_cols: Vec<Option<Option<FfCols>>> = vec![None; searcher.segment_readers().len()];
    // `id_buf` is reused across hits so the fast path allocates once per hit
    // (the `clone` into the Hit) instead of twice.
    let mut id_buf = String::new();
    top.into_iter()
        .map(|(_score, addr)| {
            let cols = ff_cols[addr.segment_ord as usize].get_or_insert_with(|| {
                let ff = searcher.segment_reader(addr.segment_ord).fast_fields();
                match (ff.i64(TS_FIELD), ff.str(ID_FIELD)) {
                    (Ok(ts), Ok(Some(id))) => Some((ts, id, ff.u64(ROW_ORDINAL_FIELD).ok())),
                    _ => None,
                }
            });
            // `None` = no fast columns for this segment, or this doc carries no
            // value for them (shouldn't happen for required fields) — either way
            // fall back to the doc store.
            let fast = match cols {
                Some((ts_col, id_col, ord_col)) => match (ts_col.first(addr.doc_id), id_col.term_ords(addr.doc_id).next()) {
                    (Some(ts), Some(ord)) => {
                        id_buf.clear();
                        id_col.ord_to_str(ord, &mut id_buf).map_err(|e| anyhow!("fast _id read: {e}"))?.then(|| Hit {
                            timestamp_micros: ts,
                            id: id_buf.clone(),
                            row_ordinal: ord_col.as_ref().and_then(|c| c.first(addr.doc_id)),
                        })
                    }
                    _ => None,
                },
                None => None,
            };
            match fast {
                Some(hit) => Ok(hit),
                None => {
                    let doc: TantivyDocument = searcher.doc(addr).map_err(|e| anyhow!("doc fetch: {e}"))?;
                    Ok(Hit {
                        timestamp_micros: doc.get_first(ts_field).and_then(|v| v.as_i64()).ok_or_else(|| anyhow!("hit missing _timestamp"))?,
                        id: doc.get_first(id_field).and_then(|v| v.as_str()).map(str::to_string).ok_or_else(|| anyhow!("hit missing _id"))?,
                        row_ordinal: None,
                    })
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod reader_tests {
    use tantivy::doc;

    use super::*;
    use crate::{
        schema::{FieldDef, TableSchema, TantivyFieldConfig},
        tantivy::{build_for_table, udf::PredNode},
    };

    /// Index three docs whose text contains tokens that tantivy's `QueryParser`
    /// grammar would swallow (`-x` → MustNot, bare `NOT` → operator). Before
    /// the parser was taken out of the substring path these queries parsed
    /// "successfully" and returned zero hits, so the `id IN (hits)` prefilter
    /// silently dropped the matching rows.
    fn ngram_index() -> Index {
        let table = TableSchema {
            rollups: vec![],
            table_name: "t".into(),
            partitions: vec![],
            sorting_columns: vec![],
            z_order_columns: vec![],
            time_column: None,
            dedup_keys: vec![],
            dedup_tiebreak: None,
            tombstone_column: None,
            version_append: false,
            fields: ["body", "level"]
                .into_iter()
                .map(|name| FieldDef {
                    name: name.into(),
                    data_type: "String".into(),
                    nullable: true,
                    // body → ngram3 (default), level → raw single token.
                    tantivy: Some(TantivyFieldConfig { indexed: true, tokenizer: (name == "level").then(|| "raw".to_string()), flatten: None }),
                    dictionary: None,
                    bloom_filter: false,
                    mutable: false,
                })
                .collect(),
        };
        let built = build_for_table(&table);
        let index = Index::create_in_ram(built.schema.clone());
        crate::tantivy::register_tokenizers(&index);
        let (body, level) = (built.user_fields["body"].field, built.user_fields["level"].field);
        let mut w = index.writer(15_000_000).expect("writer");
        let docs = [("accept -header now", "-alpha"), ("err -1234 code", "ERROR"), ("foo NOT bar baz", "two words"), ("unrelated payload", "INFO")];
        for (i, (text, lvl)) in docs.iter().enumerate() {
            w.add_document(doc!(built.timestamp => i as i64, built.id => format!("id{i}"), body => *text, level => *lvl)).expect("add");
        }
        w.commit().expect("commit");
        index
    }

    fn hit_ids(index: &Index, query: &str) -> Result<Vec<String>> {
        hit_ids_on(index, "body", query)
    }

    fn hit_ids_on(index: &Index, column: &str, query: &str) -> Result<Vec<String>> {
        let node = PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() });
        let PredsQuery::Query(q) = build_node_query(index, &node)? else { panic!("field must exist") };
        let mut ids: Vec<String> = query_index(index, &*q, None)?.into_iter().map(|h| h.id).collect();
        ids.sort();
        Ok(ids)
    }

    #[test]
    fn query_grammar_chars_in_routed_substrings_still_hit() {
        let index = ngram_index();
        // `-` adjacent to whitespace used to become a MustNot clause.
        assert_eq!(hit_ids(&index, "accept -header").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids(&index, "err -1234").unwrap(), vec!["id1"]);
        // Bare `NOT` used to be parsed as an operator.
        assert_eq!(hit_ids(&index, "foo NOT bar").unwrap(), vec!["id2"]);
        // Single words and the full literal still work.
        assert_eq!(hit_ids(&index, "header").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids(&index, "accept -header now").unwrap(), vec!["id0"]);
        // Case-insensitive (LowerCaser in tf_ngram3).
        assert_eq!(hit_ids(&index, "ACCEPT").unwrap(), vec!["id0"]);
        // Prefix marker from `LIKE 'foo%'` routing is dropped (substring ⊇ prefix).
        assert_eq!(hit_ids(&index, "accept*").unwrap(), vec!["id0"]);
        // Still selective: a literal present in no doc returns nothing.
        assert!(hit_ids(&index, "zzzqqq").unwrap().is_empty());
    }

    #[test]
    fn short_tokens_broaden_and_all_short_literals_scan() {
        let index = ngram_index();
        // A <3-char word yields no trigram → dropped (broadens), never empties.
        assert_eq!(hit_ids(&index, "header xy").unwrap(), vec!["id0"]);
        // Nothing left to query → error, which callers treat as "no prefilter".
        assert!(hit_ids(&index, "xy").is_err());
    }

    #[test]
    fn raw_tokenized_terms_match_the_whole_indexed_value() {
        let index = ngram_index();
        assert_eq!(hit_ids_on(&index, "level", "ERROR").unwrap(), vec!["id1"]);
        // A leading `-` used to parse as a lone MustNot clause (∅ hits), and
        // whitespace used to AND-split a value indexed as ONE raw token.
        assert_eq!(hit_ids_on(&index, "level", "-alpha").unwrap(), vec!["id0"]);
        assert_eq!(hit_ids_on(&index, "level", "two words").unwrap(), vec!["id2"]);
        // Raw stays exact/case-sensitive: no partial or case-folded matches.
        assert!(hit_ids_on(&index, "level", "error").unwrap().is_empty());
        assert!(hit_ids_on(&index, "level", "two").unwrap().is_empty());
    }
}

// ===== service =====
// High-level glue: a `TantivyIndexService` that owns the object_store
// handle and produces the `TantivyIndexCallback` used by `BufferedWriteLayer`.
//
// Index keying: a commit that added exactly one parquet file is keyed by that
// file's table-relative path (partition-mirrored blob); anything else falls
// back to a fresh `"bucket-{uuid}"` key. The read-side resolves
// manifest entries by intersecting their `[min_ts, max_ts]` with the query's
// time predicates (or scans the full manifest for full-text predicates).

use std::{collections::BTreeMap, sync::atomic::AtomicI64};

use chrono::Utc;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    config::TantivyConfig,
    tantivy::{ManifestEntry, MergeMode},
    write::TantivyIndexCallback,
};

/// Where the indexed batches came from — fixes both `_row_ordinal` validity
/// and the merge cadence.
#[derive(Debug, Clone, Copy)]
enum IndexSource {
    /// Flush path: batches are indexed BEFORE the Delta writer's sort, so doc
    /// order ≠ parquet row order and ordinals must not drive row selection.
    /// Merging is deferred to keep it off the ingest path.
    Flush,
}

impl IndexSource {
    fn ordinals_and_merge(self) -> (bool, MergeMode) {
        match self {
            Self::Flush => (false, MergeMode::Deferred),
        }
    }
}

/// Owns the object store + tantivy config and produces a callback.
#[derive(Debug)]
pub struct TantivyIndexService {
    pub object_store: Arc<dyn ObjectStore>,
    pub config: Arc<TantivyConfig>,
    /// Max `max_timestamp_micros` across every index this process has
    /// successfully published; `i64::MIN` until the first one. Feeds the
    /// `index_lag_seconds` gauge.
    newest_indexed_micros: AtomicI64,
    /// The reader this process serves queries from, if it has one. Held so a
    /// publish can seed the reader's extracted-index cache and invalidate its
    /// manifest — the indexer and reader are separate services, but in the
    /// server they are two halves of one process and there is no reason for
    /// the reader to re-fetch from S3 what the indexer just wrote.
    ///
    /// `Weak` because the reader also holds no ownership claim on the indexer
    /// and both are `Arc`-held by `Database`; a strong ref here would make the
    /// pair mutually immortal.
    reader: Mutex<Option<std::sync::Weak<TantivySearchService>>>,
}

impl TantivyIndexService {
    pub fn new(object_store: Arc<dyn ObjectStore>, config: Arc<TantivyConfig>) -> Self {
        Self { object_store, config, newest_indexed_micros: AtomicI64::new(i64::MIN), reader: Mutex::new(None) }
    }

    /// Attach the reader whose cache publishes should seed. Without this the
    /// indexer still works — it just uploads and lets the first query download
    /// the blob back, which is the pre-existing behaviour.
    pub fn with_reader(&self, reader: &Arc<TantivySearchService>) {
        *self.reader.lock() = Some(Arc::downgrade(reader));
    }

    fn reader(&self) -> Option<Arc<TantivySearchService>> {
        self.reader.lock().as_ref().and_then(std::sync::Weak::upgrade)
    }

    /// Newest indexed timestamp seen so far (microseconds). `None` until this
    /// process has published an index.
    pub fn newest_indexed_micros(&self) -> Option<i64> {
        Some(self.newest_indexed_micros.load(Ordering::Relaxed)).filter(|&v| v != i64::MIN)
    }

    /// Build the callback to attach via `BufferedWriteLayer::with_tantivy_indexer`.
    pub fn callback(self: Arc<Self>) -> TantivyIndexCallback {
        Arc::new(move |project_id, table_name, batches, added_files| {
            let svc = self.clone();
            Box::pin(async move {
                if batches.is_empty() || !svc.config.is_table_indexed(&table_name) {
                    return Ok(());
                }
                svc.build_and_publish(&project_id, &table_name, batches, added_files).await
            })
        })
    }

    async fn build_and_publish(
        &self, project_id: &str, table_name: &str, batches: Vec<arrow::record_batch::RecordBatch>, added_files: Vec<String>,
    ) -> Result<()> {
        // Partition-mirrored 1:1 path when the commit added exactly one file
        // (the common case: a 10-min bucket lands in one date partition).
        // Multi-file commits keep the legacy one-blob-covers-all shape — rows
        // can't be attributed to files without re-deriving the partition
        // split, and a multi-covered entry is still correct for coverage.
        let (key, path) = match added_files.as_slice() {
            [uri] => parquet_rel_of_uri(uri).map(|rel| (rel.to_string(), super::index_path_for_parquet(table_name, rel))),
            _ => None,
        }
        .unwrap_or_else(|| {
            let uuid = Uuid::new_v4().to_string();
            (format!("bucket-{uuid}"), super::blob_path(table_name, project_id, &uuid))
        });
        self.build_pack_upload(table_name, project_id, &key, path, added_files, batches, IndexSource::Flush).await
    }

    /// Build & publish an index for a single already-committed parquet file,
    /// reading it back from `delta_store` (rooted at the table), keyed by the
    /// table-RELATIVE `parquet_rel` at the deterministic partition-mirrored
    /// path. `parquet_uri` is the same file as it appears in
    /// `get_file_uris()` — recorded in `covered_files` so the coverage gate
    /// and `gc_after_compaction` (both URI-keyed) recognize the entry.
    /// Idempotent. The reused primitive behind compaction-reindex/backfill.
    pub async fn build_index_for_file(
        &self, table_name: &str, project_id: &str, parquet_rel: &str, parquet_uri: &str, delta_store: Arc<dyn ObjectStore>,
    ) -> Result<()> {
        self.build_index_for_file_inner(table_name, project_id, parquet_rel, parquet_uri, delta_store, false).await.map(|_| ())
    }

    /// As `build_index_for_file`, but returns the manifest entry INSTEAD of
    /// writing it, so a caller building many files for one project can commit
    /// them in a single manifest write (see `upsert_manifest_many`).
    ///
    /// The blob is uploaded and the reader cache seeded exactly as usual, so
    /// the only thing deferred is the manifest record. A crash between upload
    /// and batch-commit therefore leaves an orphan blob and an uncovered file —
    /// the next pass rebuilds it, which is idempotent. Batches are bounded for
    /// that reason: the exposure is at most one batch of re-done work.
    pub async fn build_index_for_file_deferred(
        &self, table_name: &str, project_id: &str, parquet_rel: &str, parquet_uri: &str, delta_store: Arc<dyn ObjectStore>,
    ) -> Result<(String, crate::tantivy::ManifestEntry)> {
        self.build_index_for_file_inner(table_name, project_id, parquet_rel, parquet_uri, delta_store, true)
            .await?
            .ok_or_else(|| anyhow!("deferred build returned no manifest entry"))
    }

    async fn build_index_for_file_inner(
        &self, table_name: &str, project_id: &str, parquet_rel: &str, parquet_uri: &str, delta_store: Arc<dyn ObjectStore>, defer: bool,
    ) -> Result<Option<(String, crate::tantivy::ManifestEntry)>> {
        let path = super::index_path_for_parquet(table_name, parquet_rel);
        let table = crate::schema::get_schema(table_name).with_context(|| format!("schema not found for {table_name}"))?;
        let result = super::build_parquet_and_pack(delta_store, parquet_rel, table, self.config.compression_level(), MergeMode::Now).await;
        self.publish_built_index(table_name, project_id, parquet_rel, path, vec![parquet_uri.to_string()], true, result, defer).await
    }

    /// Build+pack `batches`, upload to `blob_path`, and upsert the manifest
    /// entry keyed by `manifest_key`. On build failure records a failed entry
    /// (index=None, error set) and returns the error. Shared by the flush
    /// callback (random bucket key + flat path) and `build_index_for_file`
    /// (parquet-rel key + partition-mirrored path).
    // Still 8 with `&self` even after `(ordinals_valid, merge)` folded into
    // `source`; the rest are independent identifiers, not a cohesive struct.
    #[allow(clippy::too_many_arguments)]
    async fn build_pack_upload(
        &self, table_name: &str, project_id: &str, manifest_key: &str, blob_path: object_store::path::Path, covered_files: Vec<String>,
        batches: Vec<arrow::record_batch::RecordBatch>, source: IndexSource,
    ) -> Result<()> {
        let (ordinals_valid, merge) = source.ordinals_and_merge();
        let svc_table = crate::schema::get_schema(table_name).with_context(|| format!("schema not found for {table_name}"))?;
        let level = self.config.compression_level();
        let pack_result = tokio::task::spawn_blocking(move || {
            let (blob, stats) = super::build_and_pack(svc_table, &batches, level, merge)?;
            // Guard against publishing a corrupt archive (see super::verify_blob).
            super::verify_blob(&blob).context("verify packed blob")?;
            Ok::<_, anyhow::Error>((blob, stats))
        })
        .await
        .context("join build")?;
        self.publish_built_index(table_name, project_id, manifest_key, blob_path, covered_files, ordinals_valid, pack_result, false).await.map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    async fn publish_built_index(
        &self, table_name: &str, project_id: &str, manifest_key: &str, blob_path: object_store::path::Path, covered_files: Vec<String>, ordinals_valid: bool,
        result: Result<(bytes::Bytes, crate::tantivy::IndexBuildStats)>, defer: bool,
    ) -> Result<Option<(String, crate::tantivy::ManifestEntry)>> {
        let (blob, stats) = match result {
            Ok(v) => v,
            Err(e) => {
                let entry = ManifestEntry::failed(format!("build failed: {e}"), covered_files);
                let _ = upsert_manifest(self.object_store.as_ref(), table_name, project_id, manifest_key, entry).await;
                warn!("tantivy build failed for {project_id}/{table_name}: {e}");
                return Err(e);
            }
        };
        // INFO, not debug: the size distribution of what the backfill indexes is
        // the input to every sizing decision around it — the pass cap, the
        // oldest-first reservation, and `max_file_mb` (4096, i.e. 4 GB) were all
        // set without it. At ~4-6 builds/hr this is a handful of lines an hour.
        // `segments` is the cheap proxy for scale: the writer serializes one each
        // time its 64 MB arena fills, so 40+ segments means multi-GB of content.
        tracing::info!(project_id, table_name, rows = stats.rows, index_bytes = blob.len(), segments = stats.segments, event = "tantivy_index_built");
        // S3 first, always: it is the source of truth and the local copy is
        // only ever a cache. Seeding after a failed upload would leave a
        // locally-readable index no manifest entry points at.
        super::upload(self.object_store.as_ref(), &blob_path, blob.clone()).await?;
        self.seed_reader_cache(table_name, project_id, manifest_key, blob).await;
        let entry = ManifestEntry {
            index: Some(blob_path.to_string()),
            rows: stats.rows,
            built_at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            min_timestamp_micros: stats.min_timestamp_micros,
            max_timestamp_micros: stats.max_timestamp_micros,
            error: None,
            covered_files,
            ordinals_valid,
        };
        if !defer {
            upsert_manifest(self.object_store.as_ref(), table_name, project_id, manifest_key, entry.clone()).await?;
        }
        // Our own write must never wait out the manifest TTL, which is what
        // lets that TTL be minutes rather than seconds. Applied for a deferred
        // entry too: the blob IS uploaded, so consulting it is already correct,
        // and the batch commit only makes it durable.
        if let Some(reader) = self.reader() {
            reader.apply_published_entry(table_name, project_id, manifest_key, entry.clone());
        }
        if let Some(ts) = stats.max_timestamp_micros {
            self.newest_indexed_micros.fetch_max(ts, Ordering::Relaxed);
        }
        Ok(defer.then(|| (manifest_key.to_string(), entry)))
    }

    /// Install a just-published blob into the reader's extracted-index cache,
    /// so the first query needing it reads local disk instead of S3.
    ///
    /// Strictly best-effort: every failure path leaves the pre-existing
    /// behaviour (download on first read) intact, so this can never fail a
    /// flush. Unpack is CPU + disk, hence `spawn_blocking`.
    async fn seed_reader_cache(&self, table: &str, project_id: &str, manifest_key: &str, blob: bytes::Bytes) {
        if !self.config.seed_cache_on_publish() {
            return;
        }
        let Some(reader) = self.reader() else { return };
        let dir = super::local_cache_path(&reader.cache_root, table, project_id, file_uuid(manifest_key));
        let installed = tokio::task::spawn_blocking(move || install_blob_into_cache(&dir, &blob)).await;
        match installed {
            Ok(Ok(())) => SearchStats::add(&reader.stats.cache_seeded, 1),
            Ok(Err(e)) => {
                SearchStats::add(&reader.stats.cache_seed_failures, 1);
                debug!("tantivy cache seed failed for {project_id}/{table}: {e:#}");
            }
            Err(e) => {
                SearchStats::add(&reader.stats.cache_seed_failures, 1);
                debug!("tantivy cache seed join failed for {project_id}/{table}: {e}");
            }
        }
    }

    /// Carry existing coverage FORWARD across a compaction instead of
    /// re-indexing its output.
    ///
    /// A rewrite's output holds exactly its inputs' rows under the same ids, so
    /// an index that answered "these ids match" for the inputs still answers it
    /// for the output. Extending `covered_files` therefore buys the same
    /// coverage for a manifest read-modify-write, where re-indexing costs a full
    /// S3 read-back and a build — and a build was measured at ~4/hr on this box,
    /// so this is the difference between keeping up and not.
    ///
    /// **The guard is the whole correctness argument:** every removed file must
    /// already be covered. If even one was not, the output holds rows no index
    /// has seen, and marking it covered would cause a FALSE NEGATIVE — the one
    /// failure mode that is not tolerable, since the read path trusts coverage
    /// to skip files. In that case this returns `false` and the caller rebuilds
    /// exactly as before.
    ///
    /// Stale membership is safe in the other direction: an entry may now list a
    /// file whose rows it also indexed under an older path, which yields false
    /// POSITIVES that the scan filters. `ordinals_valid` is surrendered because
    /// row ordinals are per-file positions and the output's are not the inputs'.
    ///
    /// Returns whether the carry-forward applied.
    pub async fn carry_forward_after_compaction(&self, table: &str, project_id: &str, removed: &[String], added: &[String]) -> Result<bool> {
        if removed.is_empty() || added.is_empty() {
            return Ok(false);
        }
        // Callers disagree on path form: the optimize path passes absolute URIs
        // (`get_file_uris`), the wave path passes Delta-relative `add.path`.
        // Comparing them raw would never match and this would silently refuse
        // every time — implemented and inert. Anchor both on the table-relative
        // form, which `parquet_rel_of_uri` derives from either.
        let rel_of = |u: &str| parquet_rel_of_uri(u).unwrap_or(u).to_string();
        let removed_rel: HashSet<String> = removed.iter().map(|u| rel_of(u)).collect();
        let applied = super::mutate(self.object_store.as_ref(), table, project_id, |m| {
            // EXACTLY the reader's usability predicate, schema_version included.
            // An entry the reader filters out cannot be evidence that an input
            // is covered: it would vouch for a file at carry-forward time and be
            // invisible at query time, which is a false negative.
            let usable = |e: &ManifestEntry| e.index.is_some() && e.error.is_none() && e.schema_version == SCHEMA_VERSION;
            let covered: HashSet<String> = m.entries.values().filter(|e| usable(e)).flat_map(|e| e.covered_files.iter().map(|u| rel_of(u))).collect();
            if !removed_rel.iter().all(|u| covered.contains(u)) {
                return (false, false);
            }
            let removed_set = &removed_rel;
            let mut touched = false;
            for e in m.entries.values_mut().filter(|e| usable(e) && e.covered_files.iter().any(|u| removed_set.contains(&rel_of(u)))) {
                let fresh: Vec<String> = added.iter().filter(|u| !e.covered_files.contains(u)).cloned().collect();
                e.covered_files.extend(fresh);
                e.ordinals_valid = false;
                touched = true;
            }
            (touched, touched)
        })
        .await?;
        if applied {
            metrics::counter!(crate::database::scan_metric_names::TANTIVY_CARRIED_FORWARD).increment(added.len() as u64);
            if let Some(reader) = self.reader() {
                reader.invalidate_manifest(table, project_id);
            }
        }
        Ok(applied)
    }

    /// Targeted compaction GC: drop manifest entries none of whose
    /// `covered_files` are still present in `live_uris`, and prune the departed
    /// files from the entries that survive.
    ///
    /// It used to drop an entry as soon as ANY covered file died, which took
    /// that entry's still-live siblings down with it — a multi-file flush
    /// commit publishes ONE entry, so compacting a single member un-covered
    /// files nothing had touched. That collateral is proportional to the
    /// compaction rate and is a standing contributor to the coverage
    /// divergence measured 2026-08-22, where every rewrite path was reindexing
    /// its own output successfully and coverage still lost ~60 files/hr.
    ///
    /// Keeping the entry is sound because the index is a candidate generator:
    /// hits belonging to the departed file are false positives the scan
    /// filters out, and `zero_hit` pruning only ever gets more conservative.
    /// Row ORDINALS are not sound across the change — they are per-file
    /// positions, and pruning a two-file entry to one would make
    /// `covered_files.len() == 1` re-enable them against the wrong file — so a
    /// pruned entry gives them up.
    ///
    /// `live_uris` should be the current Delta table's `get_file_uris()` set
    /// after the compaction commit. Entries built before per-file tracking
    /// existed (empty `covered_files`) are treated as **stale** and dropped —
    /// they cannot be proven to cover live data, so dropping them is the
    /// correctness-preserving choice; queries fall back to a full scan + UDF
    /// post-filter until the next flush rebuilds.
    pub async fn gc_after_compaction(&self, table: &str, project_id: &str, live_uris: &[String]) -> Result<GcReport> {
        let live: HashSet<&str> = live_uris.iter().map(String::as_str).collect();
        // Under the per-manifest lock: this is a read-modify-write like every
        // other manifest mutation, and doing it outside `mutate` raced concurrent
        // upserts (last writer wins, silently un-covering files).
        let (stale, kept_len, changed) = super::mutate(self.object_store.as_ref(), table, project_id, |m| {
            let (stale, mut kept): (BTreeMap<_, _>, BTreeMap<_, _>) =
                std::mem::take(&mut m.entries).into_iter().partition(|(_, e)| !e.covered_files.iter().any(|u| live.contains(u.as_str())));
            let pruned = kept
                .values_mut()
                .filter(|e| e.covered_files.iter().any(|u| !live.contains(u.as_str())))
                .map(|e| {
                    e.covered_files.retain(|u| live.contains(u.as_str()));
                    e.ordinals_valid = false;
                })
                .count();
            let (kept_len, dirty) = (kept.len(), !stale.is_empty() || pruned > 0);
            m.entries = kept;
            ((stale, kept_len, dirty), dirty)
        })
        .await?;
        let mut report = GcReport { kept: kept_len, entries_removed: stale.len(), ..Default::default() };
        // Effectful: delete stale blobs concurrently (serial deletes made a
        // 100k-blob GC take hours). A blob already gone counts as deleted —
        // re-runs after an interrupted GC must not report errors.
        let results: Vec<bool> = futures::stream::iter(stale.into_values().filter_map(|e| e.index))
            .map(|blob| async move {
                match super::delete(self.object_store.as_ref(), &object_store::path::Path::from(blob.as_str())).await {
                    Ok(()) => true,
                    Err(e) if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::NotFound { .. })) => true,
                    Err(e) => {
                        warn!("gc: failed to delete {blob}: {e}");
                        false
                    }
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        report.blobs_deleted = results.iter().filter(|ok| **ok).count();
        report.blob_delete_errors = results.len() - report.blobs_deleted;
        // A pruned entry changes what the plan path may consult, so the cached
        // manifest must go whenever the stored one did.
        if changed && let Some(reader) = self.reader() {
            reader.invalidate_manifest(table, project_id);
        }
        Ok(report)
    }
}

/// Table-relative parquet path from an absolute add-file URI, anchored at the
/// `project_id=` partition segment (all indexed tables partition by
/// [project_id, date]). `None` for URIs that don't follow the layout —
/// callers fall back to the legacy flat blob path.
pub fn parquet_rel_of_uri(uri: &str) -> Option<&str> {
    let rel = &uri[uri.find("project_id=")?..];
    rel.ends_with(".parquet").then_some(rel)
}

/// project_id encoded in an add-file URI's partition segment.
pub fn project_id_of_uri(uri: &str) -> Option<&str> {
    uri.split_once("project_id=")?.1.split('/').next().filter(|s| !s.is_empty())
}

#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub kept: usize,
    pub entries_removed: usize,
    pub blobs_deleted: usize,
    pub blob_delete_errors: usize,
}
