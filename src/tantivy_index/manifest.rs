//! Per-(table, project_id) manifest mapping parquet file URI → tantivy
//! index blob URI. Tracks build status so the read-side can fall back to a
//! full scan when an index is missing or marked failed.
//!
//! Manifest is JSON, persisted to object storage via temp+rename. We use
//! `ObjectStore::put` (PUT-overwrite) — collisions are resolved by a coarse
//! in-process lock (DashMap entry per (table, project_id)) plus an etag
//! check on read. Good enough for low-frequency manifest writes; if multiple
//! writers race, last-writer-wins (entries are idempotent upserts).

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use serde::{Deserialize, Serialize};

use crate::mem_buffer::TableKey;

pub const MANIFEST_PREFIX: &str = "index_manifests";
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub entries: BTreeMap<String, ManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Object-store path to the index tar.zst, or `None` if build failed.
    pub index: Option<String>,
    pub rows: u64,
    pub built_at: DateTime<Utc>,
    pub schema_version: u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Set when build failed; `index` will be None.
    pub error: Option<String>,
    /// Parquet file URIs that this index covers. Populated from the Delta
    /// write commit's add-actions. Used by `gc_after_compaction` to detect
    /// stale entries: when any of these URIs is no longer live (i.e. it was
    /// compacted away), the entry no longer authoritatively covers its rows
    /// and can be dropped. Older entries built before this field existed
    /// will deserialize to an empty Vec.
    #[serde(default)]
    pub covered_files: Vec<String>,
    /// True when the index's `_row_ordinal` fast field equals parquet row
    /// order — i.e. the index was built by reading the committed file back
    /// (compaction reindex / backfill). Flush-path indexes see batches
    /// BEFORE the writer's sort, so their ordinals must not drive row
    /// selection. Old entries deserialize to false.
    #[serde(default)]
    pub ordinals_valid: bool,
}

impl Default for Manifest {
    fn default() -> Self {
        Self { version: SCHEMA_VERSION, entries: BTreeMap::new() }
    }
}

/// Object-store path of the manifest for a given table/project.
pub fn manifest_path(table: &str, project_id: &str) -> ObjPath {
    ObjPath::from(format!("{MANIFEST_PREFIX}/{table}/{project_id}/manifest.json"))
}

pub async fn load(store: &dyn ObjectStore, table: &str, project_id: &str) -> Result<Manifest> {
    match store.get(&manifest_path(table, project_id)).await {
        Ok(result) => serde_json::from_slice(&result.bytes().await.context("read manifest bytes")?).context("parse manifest json"),
        Err(object_store::Error::NotFound { .. }) => Ok(Manifest::default()),
        Err(e) => Err(e).context("load manifest"),
    }
}

pub async fn save(store: &dyn ObjectStore, table: &str, project_id: &str, manifest: &Manifest) -> Result<()> {
    let body = serde_json::to_vec_pretty(manifest).context("serialize manifest")?;
    store.put(&manifest_path(table, project_id), body.into()).await.context("put manifest").map(drop)
}

type ManifestLocks = dashmap::DashMap<TableKey, Arc<tokio::sync::Mutex<()>>>;

/// Load the manifest, apply `f`, and save it back. The shared load/save
/// skeleton behind `upsert` and `remove_many`. Serialized per
/// (table, project_id) — concurrent bucket flushes upserting the same
/// manifest would otherwise interleave load/save and drop each other's
/// entries (last-writer-wins), silently un-covering files and disabling
/// the prefilter via the coverage gate.
async fn mutate<F: FnOnce(&mut Manifest)>(store: &dyn ObjectStore, table: &str, project_id: &str, f: F) -> Result<()> {
    static LOCKS: std::sync::OnceLock<ManifestLocks> = std::sync::OnceLock::new();
    let lock = LOCKS.get_or_init(Default::default).entry((table.into(), project_id.into())).or_default().clone();
    let _guard = lock.lock().await;
    let mut m = load(store, table, project_id).await?;
    f(&mut m);
    save(store, table, project_id, &m).await
}

/// Idempotent upsert: load, mutate, save.
impl ManifestEntry {
    /// Entry recorded when the index build itself failed: no index, no rows,
    /// but the covered files are still tracked so GC can reap it later.
    pub fn failed(error: String, covered_files: Vec<String>) -> Self {
        Self {
            index: None,
            rows: 0,
            built_at: Utc::now(),
            schema_version: SCHEMA_VERSION,
            min_timestamp_micros: None,
            max_timestamp_micros: None,
            error: Some(error),
            covered_files,
            ordinals_valid: false,
        }
    }
}

pub async fn upsert(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_key: &str, entry: ManifestEntry) -> Result<()> {
    mutate(store, table, project_id, |m| {
        m.entries.insert(parquet_key.to_string(), entry);
    })
    .await
}

/// Remove entries by parquet key (used during compaction GC).
pub async fn remove_many(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_keys: &[String]) -> Result<()> {
    if parquet_keys.is_empty() {
        return Ok(());
    }
    mutate(store, table, project_id, |m| {
        for k in parquet_keys {
            m.entries.remove(k);
        }
    })
    .await
}
