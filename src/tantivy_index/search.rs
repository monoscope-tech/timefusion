//! Read-side search: given (project_id, table, query string), open every
//! manifest entry, download/cache the blob if needed, run the query, and
//! return all hits combined.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! On-miss: download blob → unpack to a fresh tempdir → atomically rename
//! into the cache path. Open the index from the cache path with mmap.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use object_store::ObjectStore;
use tantivy::query::QueryParser;

use crate::tantivy_index::{
    manifest,
    reader::{Hit, query_index},
    store,
};

#[derive(Debug)]
pub struct SearchResult {
    pub hits: Vec<Hit>,
    /// Sum of `rows` across all manifest entries that contributed (whether
    /// they hit or not). Lets the caller compute hit_count / indexed_rows
    /// for the selectivity cutoff.
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
    /// True if any **in-window** index (one that wasn't time-pruned) lacks the
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
}

impl TantivySearchService {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_root: PathBuf) -> Self {
        Self { object_store, cache_root }
    }

    /// Outcome of a search: hits + cost information for the caller's
    /// selectivity decision. `indexed_rows` is the total row count covered
    /// by the queried indexes, used to compute hit-set selectivity.
    pub async fn search(&self, table: &str, project_id: &str, field: &str, query_str: &str) -> Result<Option<Vec<Hit>>> {
        Ok(self.search_with_stats(table, project_id, field, query_str, usize::MAX, None).await?.map(|r| r.hits))
    }

    /// Bounded variant. Aborts (returns `Ok(None)`) once cumulative hits
    /// across indexes exceed `max_hits` — the caller treats the result as
    /// "too noisy to push down" and falls back to full scan.
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
        &self, table: &str, project_id: &str, field: &str, query_str: &str, max_hits: usize, time_range: Option<(i64, i64)>,
    ) -> Result<Option<SearchResult>> {
        let m = manifest::load(self.object_store.as_ref(), table, project_id).await?;
        if m.entries.is_empty() {
            return Ok(None);
        }
        let mut all_hits: Vec<Hit> = Vec::new();
        let mut seen: HashSet<(i64, String)> = HashSet::new();
        let mut usable_entries = 0usize;
        let mut indexed_rows: u64 = 0;
        let mut covered_files: HashSet<String> = HashSet::new();
        let mut field_coverage_gap = false;
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
            let dir = self.ensure_cached(table, project_id, file_uuid, blob_path).await?;
            let idx = store::open_index(&dir).with_context(|| format!("open index {file_uuid}"))?;
            let schema = idx.schema();
            let Ok(field_obj) = schema.get_field(field) else {
                // An in-window index that can't answer this field (e.g. built
                // before the column was indexed) is a coverage hole the
                // file-level `covered_files` set can't see — signal the caller
                // to skip the prefilter rather than drop this file's matches.
                field_coverage_gap = true;
                continue;
            };
            let mut qp = QueryParser::for_index(&idx, vec![field_obj]);
            // AND multiple tokens together. Critical for n-gram: "hello"
            // tokenizes into trigrams `hel`,`ell`,`llo` and we want ALL to
            // match (a single matching trigram doesn't imply substring
            // presence — only the full sequence does).
            qp.set_conjunction_by_default();
            let q = qp.parse_query(query_str).map_err(|e| anyhow!("parse query: {e}"))?;
            let hits = query_index(&idx, &*q, None)?;
            indexed_rows = indexed_rows.saturating_add(entry.rows);
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
        Ok(Some(SearchResult {
            hits: all_hits,
            indexed_rows,
            covered_files,
            field_coverage_gap,
        }))
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
