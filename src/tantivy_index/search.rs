//! Read-side search: given (project_id, table, query string), open every
//! manifest entry, download/cache the blob if needed, run the query, and
//! return all hits combined.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! On-miss: download blob → unpack to a fresh tempdir → atomically rename
//! into the cache path. Open the index from the cache path with mmap.

use anyhow::{Context, Result, anyhow};
use object_store::ObjectStore;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::query::QueryParser;

use crate::tantivy_index::manifest;
use crate::tantivy_index::reader::{Hit, query_index};
use crate::tantivy_index::store;

#[derive(Debug)]
pub struct TantivySearchService {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache_root: PathBuf,
}

impl TantivySearchService {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_root: PathBuf) -> Self {
        Self { object_store, cache_root }
    }

    /// Run `text:<query>` across all usable index entries for a project/table.
    ///
    /// Returns:
    /// - `Ok(None)` — no usable index exists (manifest empty, all entries
    ///   marked failed, or none indexes the requested field). The caller
    ///   must fall back to a full scan + UDF post-filter; the tantivy result
    ///   doesn't authoritatively cover the data.
    /// - `Ok(Some(hits))` — at least one usable index was queried; `hits`
    ///   is the union of `(timestamp, id)` matches across all of them.
    ///   `Some(vec![])` means "indexes ran and matched zero rows" — the
    ///   caller may use that as an authoritative prefilter for the files
    ///   those indexes cover, *but* it does not cover any rows still in
    ///   MemBuffer or in newly-written Delta files that haven't flushed.
    pub async fn search(&self, table: &str, project_id: &str, field: &str, query_str: &str) -> Result<Option<Vec<Hit>>> {
        let m = manifest::load(self.object_store.as_ref(), table, project_id).await?;
        if m.entries.is_empty() {
            return Ok(None);
        }
        let mut all_hits: Vec<Hit> = Vec::new();
        let mut seen: HashSet<(i64, String)> = HashSet::new();
        let mut usable_entries = 0usize;
        for (key, entry) in &m.entries {
            if entry.schema_version != manifest::SCHEMA_VERSION {
                // Skip entries built with an incompatible tantivy schema version.
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
                // Field not in this index — skip it.
                continue;
            };
            let qp = QueryParser::for_index(&idx, vec![field_obj]);
            let q = qp.parse_query(query_str).map_err(|e| anyhow!("parse query: {e}"))?;
            let hits = query_index(&idx, &*q, None)?;
            for h in hits {
                let key = (h.timestamp_micros, h.id.clone());
                if seen.insert(key) {
                    all_hits.push(h);
                }
            }
            usable_entries += 1;
        }
        if usable_entries == 0 {
            // Manifest had entries but none indexed the requested field or
            // matched our schema version — can't authoritatively prefilter.
            return Ok(None);
        }
        Ok(Some(all_hits))
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
