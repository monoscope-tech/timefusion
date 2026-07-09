//! Read-side search: given (project_id, table, predicates), open every
//! in-window manifest entry, download/cache the blob if needed, run ONE
//! combined query per index, and return the union of hits.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! On-miss: download blob → unpack to a fresh tempdir → atomically rename
//! into the cache path. Open the index from the cache path with mmap; the
//! opened (Index, IndexReader) pair is kept in a process LRU keyed by blob
//! path (blobs are immutable — new data always lands at a new path — so
//! entries never need invalidation, only eviction).

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use futures::StreamExt;
use object_store::ObjectStore;
use tantivy::{Index, IndexReader};

use crate::tantivy_index::{
    manifest,
    reader::{Hit, PredsQuery, build_node_query, query_with_searcher},
    store,
    udf::{PredNode, TextMatchPred},
};

/// Open (Index, IndexReader) pairs kept hot across queries.
const READER_CACHE_ENTRIES: usize = 256;
/// Staleness bound on the per-service parsed-manifest cache. A stale
/// manifest only under-reports coverage (prefilter skipped, full scan) or
/// points at a deleted blob (download error → full scan) — never wrong rows.
const MANIFEST_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5);
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
    pub covered_files: std::collections::HashSet<String>,
    /// Parquet URIs whose covering index was queried and returned ZERO hits,
    /// minus any URI also covered by an entry that hit, was unqueried, or
    /// lacked a field. With the coverage gate passed, these files provably
    /// contain no matching rows — the scan can skip them entirely
    /// (file-level pruning), a strictly stronger cut than the id IN-list.
    pub zero_hit_files: std::collections::HashSet<String>,
    /// Per-parquet-URI matching row ordinals, for entries that (a) cover
    /// exactly one file and (b) were built in parquet row order
    /// (`ordinals_valid`). Feeds the scan's per-file `ParquetAccessPlan`
    /// (row-selection pushdown) — files absent here simply scan normally
    /// under the id IN-list, so this can only narrow, never drop.
    pub row_selections: std::collections::HashMap<String, Vec<u64>>,
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
    readers: parking_lot::Mutex<lru::LruCache<String, (Index, IndexReader)>>,
    /// TTL cache of parsed manifests, keyed (table, project). Per-service
    /// (not global) so distinct object stores never cross-contaminate.
    manifests: dashmap::DashMap<(String, String), (std::time::Instant, Arc<manifest::Manifest>)>,
    /// Cold `open_index` calls — observability for the reader cache.
    pub index_opens: std::sync::atomic::AtomicU64,
}

impl TantivySearchService {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_root: PathBuf) -> Self {
        Self {
            object_store,
            cache_root,
            readers: parking_lot::Mutex::new(lru::LruCache::new(NonZeroUsize::new(READER_CACHE_ENTRIES).expect("nonzero"))),
            manifests: dashmap::DashMap::new(),
            index_opens: std::sync::atomic::AtomicU64::new(0),
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
        let mut covered_files: HashSet<String> = HashSet::new();
        // (file_uuid, blob_path, rows, entry covered_files, ordinals_valid)
        // surviving the time-prune.
        let mut work: Vec<(String, String, u64, Vec<String>, bool)> = Vec::new();
        for (key, entry) in &m.entries {
            if entry.schema_version != manifest::SCHEMA_VERSION {
                continue;
            }
            // Coverage accounting: a successful entry covers its files for the
            // gate regardless of the time-prune window (see SearchResult docs).
            if entry.index.is_some() && entry.error.is_none() {
                covered_files.extend(entry.covered_files.iter().cloned());
            }
            // Time-prune: skip indexes whose timestamp span can't overlap the
            // query window (no blob download). Conservative on unknown bounds.
            if !entry_overlaps(entry.min_timestamp_micros, entry.max_timestamp_micros, time_range) {
                continue;
            }
            let Some(blob_path) = entry.index.as_ref() else {
                continue;
            };
            let file_uuid = key.strip_prefix("bucket-").unwrap_or(key);
            work.push((
                file_uuid.to_string(),
                blob_path.clone(),
                entry.rows,
                entry.covered_files.clone(),
                entry.ordinals_valid && entry.covered_files.len() == 1,
            ));
        }

        // One download+open+search task per index, SEARCH_CONCURRENCY-wide.
        // `None` hits = the index lacks a queried field (coverage gap).
        let mut tasks = futures::stream::iter(work.into_iter().map(|(file_uuid, blob_path, rows, entry_covered, ordinals_valid)| async move {
            let dir = self.ensure_cached(table, project_id, &file_uuid, &blob_path).await?;
            let (index, reader) = self.open_cached(&blob_path, &dir).with_context(|| format!("open index {file_uuid}"))?;
            match build_node_query(&index, node)? {
                PredsQuery::MissingField => Ok::<_, anyhow::Error>((None, rows, entry_covered, ordinals_valid)),
                PredsQuery::Query(q) => Ok((Some(query_with_searcher(&reader.searcher(), &*q, None)?), rows, entry_covered, ordinals_valid)),
            }
        }))
        .buffer_unordered(SEARCH_CONCURRENCY);

        let mut all_hits: Vec<Hit> = Vec::new();
        let mut seen: HashSet<(i64, String)> = HashSet::new();
        let mut usable_entries = 0usize;
        let mut indexed_rows: u64 = 0;
        let mut field_coverage_gap = false;
        let mut zero_hit_files: HashSet<String> = HashSet::new();
        // Files covered by a hitting or field-gapped entry can never be
        // pruned, even if another (double-covering) entry saw zero hits.
        let mut unprunable_files: HashSet<String> = HashSet::new();
        let mut row_selections: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
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
                // covered_files.len()==1 guaranteed by the work-item gate.
                if let Some(uri) = entry_covered.first()
                    && !hits.is_empty()
                {
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
                let dedup_key = (h.timestamp_micros, h.id.clone());
                if seen.insert(dedup_key) {
                    all_hits.push(h);
                    if all_hits.len() > max_hits {
                        return Ok(None);
                    }
                }
            }
            usable_entries += 1;
        }
        if usable_entries == 0 {
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
        use futures::TryStreamExt;
        let cutoff = crate::clock::now_micros() - i64::from(days) * 86_400_000_000;
        let prefix = object_store::path::Path::from(format!("{}/{table}", manifest::MANIFEST_PREFIX));
        let objs: Vec<object_store::ObjectMeta> = self.object_store.list(Some(&prefix)).try_collect().await?;
        let mut warmed = 0usize;
        for meta in objs.iter().filter(|m| m.location.as_ref().ends_with("/manifest.json")) {
            let parts: Vec<&str> = meta.location.as_ref().split('/').collect();
            let Some(project) = parts.len().checked_sub(2).and_then(|i| parts.get(i)) else {
                continue;
            };
            let Ok(m) = manifest::load(self.object_store.as_ref(), table, project).await else {
                continue;
            };
            for (key, e) in &m.entries {
                let Some(blob) = &e.index else { continue };
                if e.schema_version != manifest::SCHEMA_VERSION || e.max_timestamp_micros.is_none_or(|mx| mx < cutoff) {
                    continue;
                }
                let uuid = key.strip_prefix("bucket-").unwrap_or(key);
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
        if let Some(e) = self.manifests.get(&key)
            && e.value().0.elapsed() < MANIFEST_CACHE_TTL
        {
            return Ok(e.value().1.clone());
        }
        let m = Arc::new(manifest::load(self.object_store.as_ref(), table, project_id).await?);
        self.manifests.insert(key, (std::time::Instant::now(), m.clone()));
        Ok(m)
    }

    /// LRU-cached open. Blob paths are immutable so entries are never stale.
    fn open_cached(&self, blob_path: &str, dir: &Path) -> Result<(Index, IndexReader)> {
        if let Some(v) = self.readers.lock().get(blob_path) {
            return Ok(v.clone());
        }
        let index = store::open_index(dir)?;
        let reader = index.reader().map_err(|e| anyhow!("open reader: {e}"))?;
        self.index_opens.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.readers.lock().put(blob_path.to_string(), (index.clone(), reader.clone()));
        Ok((index, reader))
    }

    async fn ensure_cached(&self, table: &str, project_id: &str, file_uuid: &str, blob_path: &str) -> Result<PathBuf> {
        let dir = store::local_cache_path(&self.cache_root, table, project_id, file_uuid);
        if dir.join("meta.json").exists() || has_any_segment(&dir) {
            return Ok(dir);
        }
        // Fetch blob and unpack into a temp dir adjacent to the cache, then rename.
        let blob = store::download(self.object_store.as_ref(), &object_store::path::Path::from(blob_path.to_string())).await?;
        let parent = dir.parent().ok_or_else(|| anyhow!("cache path has no parent"))?;
        std::fs::create_dir_all(parent).context("mkdir cache parent")?;
        let tmp = tempfile::TempDir::new_in(parent).context("tempdir for unpack")?;
        store::unpack_to_dir(&blob, tmp.path())?;
        // Best-effort rename. If another worker beat us, drop ours and use theirs.
        match std::fs::rename(tmp.path(), &dir) {
            Ok(()) => {
                std::mem::forget(tmp);
            }
            Err(_) if dir.exists() => {} // someone else won the race
            Err(e) => return Err(e).context("rename into cache"),
        }
        Ok(dir)
    }
}

/// Whether a manifest entry's `[min,max]` timestamp span could contain rows in
/// the query's `[lo,hi]` window. Conservative by design: an entry with unknown
/// bounds (`None`, e.g. legacy or a failed-stats build) always overlaps so it's
/// never wrongly pruned, and a `None` query range (no timestamp filter) matches
/// everything. Correctness rests on this never returning `false` for an entry
/// that covers an in-window row.
fn entry_overlaps(min: Option<i64>, max: Option<i64>, range: Option<(i64, i64)>) -> bool {
    let Some((lo, hi)) = range else { return true };
    let emin = min.unwrap_or(i64::MIN);
    let emax = max.unwrap_or(i64::MAX);
    emax >= lo && emin <= hi
}

fn has_any_segment(dir: &Path) -> bool {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            if entry.file_name().to_string_lossy().starts_with("seg") || entry.file_name().to_string_lossy() == "meta.json" {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::entry_overlaps;

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
