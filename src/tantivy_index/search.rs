//! Read-side search: given (project_id, table, predicates), open every
//! in-window manifest entry, download/cache the blob if needed, run ONE
//! combined query per index, and return the union of hits.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! On-miss: download blob → unpack to a fresh tempdir → atomically rename
//! into the cache path. Open the index from the cache path with mmap; the
//! opened (Index, IndexReader) pair is kept in a process LRU keyed by the
//! cache dir, which is 1:1 with the blob path (blobs are immutable — new
//! data always lands at a new path — so entries never need invalidation,
//! only eviction).
//!
//! The disk tree is bounded by [`TantivySearchService::reap_disk_cache`],
//! run by the "Tantivy cache reap" cron: nothing else ever deletes from it.
//! `gc_after_compaction` reaps manifest entries and their object-store blobs,
//! but the *extracted* copy under `cache_root` is invisible to it, and
//! compaction rewrites parquet constantly — so every compacted-away file
//! leaves a dir behind that no query will open again. `cache_root` is
//! `timefusion_data_dir`, which also holds the WAL, so unbounded growth ends
//! in failed WAL appends rather than a merely cold cache.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, anyhow};
use dashmap::DashMap;
use futures::{StreamExt, TryStreamExt};
use lru::LruCache;
use object_store::{ObjectStore, path::Path as ObjPath};
use parking_lot::Mutex;
use tantivy::{Index, IndexReader};

use crate::tantivy_index::{
    manifest,
    reader::{Hit, PredsQuery, build_node_query, query_with_searcher},
    store,
    udf::{PredNode, TextMatchPred},
};

/// Open (Index, IndexReader) pairs kept hot across queries.
const READER_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// Staleness bound on the per-service parsed-manifest cache. A stale
/// manifest only under-reports coverage (prefilter skipped, full scan) or
/// points at a deleted blob (download error → full scan) — never wrong rows.
const MANIFEST_CACHE_TTL: Duration = Duration::from_secs(5);
/// Concurrent per-index download+search tasks within one query.
const SEARCH_CONCURRENCY: usize = 8;

#[derive(Debug)]
pub struct SearchResult {
    pub hits: Vec<Hit>,
    /// Sum of `rows` across all manifest entries that were queried. Lets the
    /// caller compute hit_count / indexed_rows for the selectivity cutoff.
    pub indexed_rows: u64,
    /// Union of `covered_files` (parquet URIs) over every **successful**
    /// manifest entry (index present, no build error), regardless of the
    /// time-prune window. The read-side coverage gate intersects this with the
    /// live Delta add-file set: if a live file overlapping the query can't be
    /// found here, the `id IN (hits)` prefilter would silently drop its rows,
    /// so the caller skips the prefilter (full scan). Collected over ALL
    /// successful entries (not just opened ones) because a time-pruned entry
    /// still legitimately covers its file — its rows are simply out of window.
    pub covered_files: HashSet<String>,
    /// Parquet URIs whose covering index was queried and returned ZERO hits,
    /// minus any URI also covered by an entry that hit, was unqueried, or
    /// lacked a field. With the coverage gate passed, these files provably
    /// contain no matching rows — the scan can skip them entirely
    /// (file-level pruning), a strictly stronger cut than the id IN-list.
    pub zero_hit_files: HashSet<String>,
    /// Per-parquet-URI matching row ordinals, for entries that (a) cover
    /// exactly one file and (b) were built in parquet row order
    /// (`ordinals_valid`). Feeds the scan's per-file `ParquetAccessPlan`
    /// (row-selection pushdown) — files absent here simply scan normally
    /// under the id IN-list, so this can only narrow, never drop.
    pub row_selections: HashMap<String, Vec<u64>>,
    /// True if any **in-window** index (one that wasn't time-pruned) lacks a
    /// queried field. `covered_files` is field-independent (an entry covers its
    /// file for all of the current schema's indexed columns), so it can't catch
    /// the schema-evolution case where an older index predates a newly-indexed
    /// column: that index still appears "covered" yet returns no hits for the
    /// new field, and the `id IN (hits)` intersection would drop its file's
    /// matching rows. When set, the caller must skip the prefilter (full scan).
    pub field_coverage_gap: bool,
}

#[derive(Debug)]
pub struct TantivySearchService {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache_root: PathBuf,
    readers: Mutex<LruCache<PathBuf, (Index, IndexReader)>>,
    /// TTL cache of parsed manifests, keyed (table, project). Per-service
    /// (not global) so distinct object stores never cross-contaminate.
    manifests: DashMap<(String, String), (Instant, Arc<manifest::Manifest>)>,
    /// Cold `open_index` calls — observability for the reader cache.
    pub index_opens: AtomicU64,
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
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_root: PathBuf) -> Self {
        Self {
            object_store,
            cache_root,
            readers: Mutex::new(LruCache::new(READER_CACHE_ENTRIES)),
            manifests: DashMap::new(),
            index_opens: AtomicU64::new(0),
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
        let current = || m.entries.iter().filter(|(_, e)| e.schema_version == manifest::SCHEMA_VERSION);
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
        let mut tasks = futures::stream::iter(work.into_iter().map(|(file_uuid, blob_path, rows, entry_covered, ordinals_valid)| async move {
            let dir = self.ensure_cached(table, project_id, &file_uuid, &blob_path).await?;
            let (index, reader) = self.open_cached(&dir).with_context(|| format!("open index {file_uuid}"))?;
            match build_node_query(&index, node)? {
                PredsQuery::MissingField => Ok::<_, anyhow::Error>((None, rows, entry_covered, ordinals_valid)),
                PredsQuery::Query(q) => Ok((Some(query_with_searcher(&reader.searcher(), &*q, None)?), rows, entry_covered, ordinals_valid)),
            }
        }))
        .buffer_unordered(SEARCH_CONCURRENCY);

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
            let (hits, rows, entry_covered, ordinals_valid) = res?;
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
    pub async fn warm_recent(&self, table: &str, days: u32) -> Result<usize> {
        let cutoff = crate::clock::now_micros() - i64::from(days) * 86_400_000_000;
        let prefix = ObjPath::from(format!("{}/{table}", manifest::MANIFEST_PREFIX));
        let objs: Vec<_> = self.object_store.list(Some(&prefix)).try_collect().await?;
        // Imperative: every step is awaited IO whose failures are individually skipped.
        let mut warmed = 0usize;
        for meta in objs.iter().filter(|m| m.location.as_ref().ends_with("/manifest.json")) {
            // .../{project}/manifest.json
            let Some(project) = meta.location.as_ref().rsplit('/').nth(1) else {
                continue;
            };
            let Ok(m) = manifest::load(self.object_store.as_ref(), table, project).await else {
                continue;
            };
            let recent = m
                .entries
                .iter()
                .filter(|(_, e)| e.schema_version == manifest::SCHEMA_VERSION && e.max_timestamp_micros.is_some_and(|mx| mx >= cutoff))
                .filter_map(|(key, e)| Some((file_uuid(key), e.index.as_ref()?)));
            for (uuid, blob) in recent {
                if self.ensure_cached(table, project, uuid, blob).await.is_ok() {
                    warmed += 1;
                }
            }
        }
        Ok(warmed)
    }

    /// TTL-cached manifest read (see `MANIFEST_CACHE_TTL` for the staleness
    /// argument). Removes the per-query S3 GET + JSON parse.
    async fn load_manifest_cached(&self, table: &str, project_id: &str) -> Result<Arc<manifest::Manifest>> {
        let key = (table.to_string(), project_id.to_string());
        if let Some(m) = self.manifests.get(&key).filter(|e| e.0.elapsed() < MANIFEST_CACHE_TTL).map(|e| e.1.clone()) {
            return Ok(m);
        }
        let m = Arc::new(manifest::load(self.object_store.as_ref(), table, project_id).await?);
        self.manifests.insert(key, (Instant::now(), m.clone()));
        Ok(m)
    }

    /// LRU-cached open, keyed by cache dir — 1:1 with the (immutable) blob
    /// path, so entries are never stale, and the reaper can drop the reader
    /// for a dir it deletes under that same key.
    fn open_cached(&self, dir: &Path) -> Result<(Index, IndexReader)> {
        if let Some(v) = self.readers.lock().get(dir) {
            return Ok(v.clone());
        }
        let index = store::open_index(dir)?;
        let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
        self.index_opens.fetch_add(1, Ordering::Relaxed);
        self.readers.lock().put(dir.to_path_buf(), (index.clone(), reader.clone()));
        Ok((index, reader))
    }

    async fn ensure_cached(&self, table: &str, project_id: &str, file_uuid: &str, blob_path: &str) -> Result<PathBuf> {
        let dir = store::local_cache_path(&self.cache_root, table, project_id, file_uuid);
        // Stamped on every hit, not only on miss: recency is what the reaper
        // sorts by, and a dir serving a query every minute must never look as
        // old as its unpack time.
        self.last_used.insert(dir.clone(), SystemTime::now());
        if has_any_segment(&dir) {
            return Ok(dir);
        }
        // Fetch blob and unpack into a temp dir adjacent to the cache, then rename.
        let blob = store::download(self.object_store.as_ref(), &ObjPath::from(blob_path)).await?;
        let parent = dir.parent().ok_or_else(|| anyhow!("cache path has no parent"))?;
        std::fs::create_dir_all(parent).context("mkdir cache parent")?;
        let tmp = tempfile::TempDir::new_in(parent).context("tempdir for unpack")?;
        store::unpack_to_dir(&blob, tmp.path())?;
        // Best-effort rename. If another worker beat us, drop ours and use theirs.
        // Bound in a `let` so the `tmp.path()` borrow ends before the arms move `tmp`.
        let renamed = std::fs::rename(tmp.path(), &dir);
        match renamed {
            Ok(()) => drop(tmp.keep()),  // disarm cleanup: the dir now lives at `dir`
            Err(_) if dir.exists() => {} // someone else won the race
            Err(e) => return Err(e).context("rename into cache"),
        }
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
        TantivySearchService::new(Arc::new(object_store::memory::InMemory::new()), root.to_path_buf())
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
