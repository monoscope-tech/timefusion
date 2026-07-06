//! High-level glue: a `TantivyIndexService` that owns the object_store
//! handle and produces the `TantivyIndexCallback` used by `BufferedWriteLayer`.
//!
//! Index keying: each flushed bucket produces one index, identified by a
//! fresh UUID. The manifest entry maps `bucket_key` → index blob URI.
//! `bucket_key` = `"bucket-{min_ts_micros}-{uuid}"`. The read-side resolves
//! manifest entries by intersecting their `[min_ts, max_ts]` with the query's
//! time predicates (or scans the full manifest for full-text predicates).

use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use anyhow::{Context, Result};
use chrono::Utc;
use object_store::ObjectStore;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    buffered_write_layer::TantivyIndexCallback,
    config::TantivyConfig,
    schema_loader,
    tantivy_index::{
        manifest::{self, ManifestEntry},
        store,
    },
};

/// Owns the object store + tantivy config and produces a callback.
#[derive(Debug)]
pub struct TantivyIndexService {
    pub object_store:      Arc<dyn ObjectStore>,
    pub config:            Arc<TantivyConfig>,
    /// Max `max_timestamp_micros` across every index this process has
    /// successfully published. Feeds the `index_lag_seconds` gauge. Loaded
    /// from manifests on first observation (lazy) and updated after each
    /// successful build_and_publish.
    newest_indexed_micros: AtomicI64,
}

impl TantivyIndexService {
    pub fn new(object_store: Arc<dyn ObjectStore>, config: Arc<TantivyConfig>) -> Self {
        Self {
            object_store,
            config,
            newest_indexed_micros: AtomicI64::new(i64::MIN),
        }
    }

    /// Newest indexed timestamp seen so far (microseconds). `None` if the
    /// service has never published or warm-loaded any index.
    pub fn newest_indexed_micros(&self) -> Option<i64> {
        let v = self.newest_indexed_micros.load(Ordering::Relaxed);
        if v == i64::MIN { None } else { Some(v) }
    }

    fn observe_newest(&self, ts_micros: Option<i64>) {
        if let Some(ts) = ts_micros {
            let mut cur = self.newest_indexed_micros.load(Ordering::Relaxed);
            while ts > cur {
                match self.newest_indexed_micros.compare_exchange_weak(cur, ts, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(v) => cur = v,
                }
            }
        }
    }

    /// Build the callback to attach via `BufferedWriteLayer::with_tantivy_indexer`.
    pub fn callback(self: Arc<Self>) -> TantivyIndexCallback {
        Arc::new(move |project_id, table_name, batches, added_files| {
            let svc = self.clone();
            Box::pin(async move {
                if !svc.config.is_table_indexed(&table_name) {
                    return Ok(());
                }
                if batches.is_empty() {
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
        if added_files.len() == 1
            && let Some(rel) = parquet_rel_of_uri(&added_files[0])
        {
            let rel = rel.to_string();
            let path = store::index_path_for_parquet(table_name, &rel);
            // ordinals_valid=false: flush batches are indexed BEFORE the
            // Delta writer's sort, so doc order ≠ parquet row order.
            return self.build_pack_upload(table_name, project_id, &rel, path, added_files, batches, false).await;
        }
        let bucket_uuid = Uuid::new_v4().to_string();
        let path = store::blob_path(table_name, project_id, &bucket_uuid);
        self.build_pack_upload(table_name, project_id, &bucket_key(&bucket_uuid), path, added_files, batches, false).await
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
        let batches = store::read_parquet_batches(delta_store, parquet_rel).await?;
        if batches.is_empty() {
            return Ok(());
        }
        let path = store::index_path_for_parquet(table_name, parquet_rel);
        // ordinals_valid=true: batches came from the committed parquet in row
        // order, so `_row_ordinal` is a valid parquet row index.
        self.build_pack_upload(table_name, project_id, parquet_rel, path, vec![parquet_uri.to_string()], batches, true).await
    }

    /// Build+pack `batches`, upload to `blob_path`, and upsert the manifest
    /// entry keyed by `manifest_key`. On build failure records a failed entry
    /// (index=None, error set) and returns the error. Shared by the flush
    /// callback (random bucket key + flat path) and `build_index_for_file`
    /// (parquet-rel key + partition-mirrored path).
    async fn build_pack_upload(
        &self, table_name: &str, project_id: &str, manifest_key: &str, blob_path: object_store::path::Path, covered_files: Vec<String>,
        batches: Vec<arrow::record_batch::RecordBatch>, ordinals_valid: bool,
    ) -> Result<()> {
        let svc_table = schema_loader::get_schema(table_name).with_context(|| format!("schema not found for {table_name}"))?.clone();
        let level = self.config.compression_level();
        let pack_result = tokio::task::spawn_blocking(move || store::build_and_pack(&svc_table, &batches, level)).await.context("join build")?;
        let (blob, stats) = match pack_result {
            Ok(v) => v,
            Err(e) => {
                let entry = ManifestEntry {
                    index:                None,
                    rows:                 0,
                    built_at:             Utc::now(),
                    schema_version:       manifest::SCHEMA_VERSION,
                    min_timestamp_micros: None,
                    max_timestamp_micros: None,
                    error:                Some(format!("build failed: {e}")),
                    covered_files:        covered_files.clone(),
                    ordinals_valid:       false,
                };
                let _ = manifest::upsert(self.object_store.as_ref(), table_name, project_id, manifest_key, entry).await;
                warn!("tantivy build failed for {project_id}/{table_name}: {e}");
                return Err(e);
            }
        };
        debug!("tantivy index for {project_id}/{table_name} built: rows={} bytes={}", stats.rows, blob.len());
        store::upload(self.object_store.as_ref(), &blob_path, blob).await?;
        let entry = ManifestEntry {
            index: Some(blob_path.to_string()),
            rows: stats.rows,
            built_at: Utc::now(),
            schema_version: manifest::SCHEMA_VERSION,
            min_timestamp_micros: stats.min_timestamp_micros,
            max_timestamp_micros: stats.max_timestamp_micros,
            error: None,
            covered_files,
            ordinals_valid,
        };
        manifest::upsert(self.object_store.as_ref(), table_name, project_id, manifest_key, entry).await?;
        self.observe_newest(stats.max_timestamp_micros);
        Ok(())
    }
}

fn bucket_key(uuid: &str) -> String {
    format!("bucket-{uuid}")
}

/// Table-relative parquet path from an absolute add-file URI, anchored at the
/// `project_id=` partition segment (all indexed tables partition by
/// [project_id, date]). `None` for URIs that don't follow the layout —
/// callers fall back to the legacy flat blob path.
pub fn parquet_rel_of_uri(uri: &str) -> Option<&str> {
    let idx = uri.find("project_id=")?;
    let rel = &uri[idx..];
    rel.ends_with(".parquet").then_some(rel)
}

/// project_id encoded in an add-file URI's partition segment.
pub fn project_id_of_uri(uri: &str) -> Option<&str> {
    let rel = &uri[uri.find("project_id=")? + "project_id=".len()..];
    rel.split('/').next().filter(|s| !s.is_empty())
}

impl TantivyIndexService {
    /// Targeted compaction GC: drop manifest entries whose `covered_files`
    /// reference any parquet URI no longer present in `live_uris`. Entries
    /// whose covered files are fully alive are preserved (their index still
    /// authoritatively covers live rows).
    ///
    /// `live_uris` should be the current Delta table's `get_file_uris()` set
    /// after the compaction commit. Entries built before per-file tracking
    /// existed (empty `covered_files`) are treated as **stale** and dropped —
    /// they cannot be proven to cover live data, so dropping them is the
    /// correctness-preserving choice; queries fall back to a full scan + UDF
    /// post-filter until the next flush rebuilds.
    pub async fn gc_after_compaction(&self, table: &str, project_id: &str, live_uris: &[String]) -> Result<GcReport> {
        use std::collections::HashSet;
        let live: HashSet<&str> = live_uris.iter().map(|s| s.as_str()).collect();
        let mut m = manifest::load(self.object_store.as_ref(), table, project_id).await?;
        let mut report = GcReport::default();
        let keys: Vec<String> = m.entries.keys().cloned().collect();
        for key in keys {
            let entry = m.entries.get(&key).cloned().unwrap();
            let stale = entry.covered_files.is_empty() || entry.covered_files.iter().any(|u| !live.contains(u.as_str()));
            if !stale {
                report.kept += 1;
                continue;
            }
            if let Some(blob) = &entry.index {
                let path = object_store::path::Path::from(blob.clone());
                match store::delete(self.object_store.as_ref(), &path).await {
                    Ok(()) => report.blobs_deleted += 1,
                    Err(e) => {
                        warn!("gc: failed to delete {blob}: {e}");
                        report.blob_delete_errors += 1;
                    }
                }
            }
            m.entries.remove(&key);
            report.entries_removed += 1;
        }
        if report.entries_removed > 0 {
            manifest::save(self.object_store.as_ref(), table, project_id, &m).await?;
        }
        Ok(report)
    }
}

#[derive(Debug, Default, Clone)]
pub struct GcReport {
    pub kept:               usize,
    pub entries_removed:    usize,
    pub blobs_deleted:      usize,
    pub blob_delete_errors: usize,
}
