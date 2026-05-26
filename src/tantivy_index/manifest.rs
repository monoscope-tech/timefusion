//! Per-(table, project_id) manifest mapping parquet file URI → tantivy
//! index blob URI. Tracks build status so the read-side can fall back to a
//! full scan when an index is missing or marked failed.
//!
//! Manifest is JSON, persisted to object storage via temp+rename. We use
//! `ObjectStore::put` (PUT-overwrite) — collisions are resolved by a coarse
//! in-process lock (DashMap entry per (table, project_id)) plus an etag
//! check on read. Good enough for low-frequency manifest writes; if multiple
//! writers race, last-writer-wins (entries are idempotent upserts).

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use serde::{Deserialize, Serialize};

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
    pub index:                Option<String>,
    pub rows:                 u64,
    pub built_at:             DateTime<Utc>,
    pub schema_version:       u32,
    pub min_timestamp_micros: Option<i64>,
    pub max_timestamp_micros: Option<i64>,
    /// Set when build failed; `index` will be None.
    pub error:                Option<String>,
    /// Parquet file URIs that this index covers. Populated from the Delta
    /// write commit's add-actions. Used by `gc_after_compaction` to detect
    /// stale entries: when any of these URIs is no longer live (i.e. it was
    /// compacted away), the entry no longer authoritatively covers its rows
    /// and can be dropped. Older entries built before this field existed
    /// will deserialize to an empty Vec.
    #[serde(default)]
    pub covered_files:        Vec<String>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            version: SCHEMA_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

/// Object-store path of the manifest for a given table/project.
pub fn manifest_path(table: &str, project_id: &str) -> ObjPath {
    ObjPath::from(format!("{MANIFEST_PREFIX}/{table}/{project_id}/manifest.json"))
}

pub async fn load(store: &dyn ObjectStore, table: &str, project_id: &str) -> Result<Manifest> {
    let p = manifest_path(table, project_id);
    match store.get(&p).await {
        Ok(result) => {
            let bytes = result.bytes().await.context("read manifest bytes")?;
            let m: Manifest = serde_json::from_slice(&bytes).context("parse manifest json")?;
            Ok(m)
        }
        Err(object_store::Error::NotFound { .. }) => Ok(Manifest::default()),
        Err(e) => Err(e).context("load manifest"),
    }
}

pub async fn save(store: &dyn ObjectStore, table: &str, project_id: &str, manifest: &Manifest) -> Result<()> {
    let p = manifest_path(table, project_id);
    let body = serde_json::to_vec_pretty(manifest).context("serialize manifest")?;
    store.put(&p, body.into()).await.context("put manifest")?;
    Ok(())
}

/// Idempotent upsert: load, mutate, save.
pub async fn upsert(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_key: &str, entry: ManifestEntry) -> Result<()> {
    let mut m = load(store, table, project_id).await?;
    m.entries.insert(parquet_key.to_string(), entry);
    save(store, table, project_id, &m).await
}

/// Remove entries by parquet key (used during compaction GC).
pub async fn remove_many(store: &dyn ObjectStore, table: &str, project_id: &str, parquet_keys: &[String]) -> Result<()> {
    if parquet_keys.is_empty() {
        return Ok(());
    }
    let mut m = load(store, table, project_id).await?;
    for k in parquet_keys {
        m.entries.remove(k);
    }
    save(store, table, project_id, &m).await
}
