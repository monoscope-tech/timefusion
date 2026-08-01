//! High-level glue: a `TantivyIndexService` that owns the object_store
//! handle and produces the `TantivyIndexCallback` used by `BufferedWriteLayer`.
//!
//! Index keying: a commit that added exactly one parquet file is keyed by that
//! file's table-relative path (partition-mirrored blob); anything else falls
//! back to a fresh `"bucket-{uuid}"` key. The read-side resolves
//! manifest entries by intersecting their `[min_ts, max_ts]` with the query's
//! time predicates (or scans the full manifest for full-text predicates).

use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
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
        builder::MergeMode,
        manifest::{self, ManifestEntry},
        store,
    },
};

/// Where the indexed batches came from — fixes both `_row_ordinal` validity
/// and the merge cadence.
#[derive(Debug, Clone, Copy)]
enum IndexSource {
    /// Flush path: batches are indexed BEFORE the Delta writer's sort, so doc
    /// order ≠ parquet row order and ordinals must not drive row selection.
    /// Merging is deferred to keep it off the ingest path.
    Flush,
    /// Read back from the committed parquet in row order (post-optimize
    /// reindex, startup backfill, post-WAL-recovery reindex) — ordinals are
    /// valid parquet row indexes, and since every such caller is already a
    /// maintenance task, this rebuild IS the merge the flush path deferred.
    Committed,
}

impl IndexSource {
    fn ordinals_and_merge(self) -> (bool, MergeMode) {
        match self {
            Self::Flush => (false, MergeMode::Deferred),
            Self::Committed => (true, MergeMode::Now),
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
}

impl TantivyIndexService {
    pub fn new(object_store: Arc<dyn ObjectStore>, config: Arc<TantivyConfig>) -> Self {
        Self { object_store, config, newest_indexed_micros: AtomicI64::new(i64::MIN) }
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
            [uri] => parquet_rel_of_uri(uri).map(|rel| (rel.to_string(), store::index_path_for_parquet(table_name, rel))),
            _ => None,
        }
        .unwrap_or_else(|| {
            let uuid = Uuid::new_v4().to_string();
            (format!("bucket-{uuid}"), store::blob_path(table_name, project_id, &uuid))
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
        let batches = store::read_parquet_batches(delta_store, parquet_rel).await?;
        if batches.is_empty() {
            return Ok(());
        }
        let path = store::index_path_for_parquet(table_name, parquet_rel);
        self.build_pack_upload(table_name, project_id, parquet_rel, path, vec![parquet_uri.to_string()], batches, IndexSource::Committed).await
    }

    /// Build+pack `batches`, upload to `blob_path`, and upsert the manifest
    /// entry keyed by `manifest_key`. On build failure records a failed entry
    /// (index=None, error set) and returns the error. Shared by the flush
    /// callback (random bucket key + flat path) and `build_index_for_file`
    /// (parquet-rel key + partition-mirrored path).
    async fn build_pack_upload(
        &self, table_name: &str, project_id: &str, manifest_key: &str, blob_path: object_store::path::Path, covered_files: Vec<String>,
        batches: Vec<arrow::record_batch::RecordBatch>, source: IndexSource,
    ) -> Result<()> {
        let (ordinals_valid, merge) = source.ordinals_and_merge();
        let svc_table = schema_loader::get_schema(table_name).with_context(|| format!("schema not found for {table_name}"))?;
        let level = self.config.compression_level();
        let pack_result = tokio::task::spawn_blocking(move || {
            let (blob, stats) = store::build_and_pack(svc_table, &batches, level, merge)?;
            // Guard against publishing a corrupt archive (see store::verify_blob).
            store::verify_blob(&blob).context("verify packed blob")?;
            Ok::<_, anyhow::Error>((blob, stats))
        })
        .await
        .context("join build")?;
        let (blob, stats) = match pack_result {
            Ok(v) => v,
            Err(e) => {
                let entry = ManifestEntry::failed(format!("build failed: {e}"), covered_files);
                let _ = manifest::upsert(self.object_store.as_ref(), table_name, project_id, manifest_key, entry).await;
                warn!("tantivy build failed for {project_id}/{table_name}: {e}");
                return Err(e);
            }
        };
        debug!("tantivy index for {project_id}/{table_name} built: rows={} bytes={} segments={}", stats.rows, blob.len(), stats.segments);
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
        if let Some(ts) = stats.max_timestamp_micros {
            self.newest_indexed_micros.fetch_max(ts, Ordering::Relaxed);
        }
        Ok(())
    }

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
        let live: HashSet<&str> = live_uris.iter().map(String::as_str).collect();
        let mut m = manifest::load(self.object_store.as_ref(), table, project_id).await?;
        let (stale, kept): (BTreeMap<_, _>, BTreeMap<_, _>) = std::mem::take(&mut m.entries)
            .into_iter()
            .partition(|(_, e)| e.covered_files.is_empty() || e.covered_files.iter().any(|u| !live.contains(u.as_str())));
        let mut report = GcReport { kept: kept.len(), entries_removed: stale.len(), ..Default::default() };
        m.entries = kept;
        // Effectful: one delete per stale blob, each awaited.
        for blob in stale.into_values().filter_map(|e| e.index) {
            match store::delete(self.object_store.as_ref(), &object_store::path::Path::from(blob.as_str())).await {
                Ok(()) => report.blobs_deleted += 1,
                Err(e) => {
                    warn!("gc: failed to delete {blob}: {e}");
                    report.blob_delete_errors += 1;
                }
            }
        }
        if report.entries_removed > 0 {
            manifest::save(self.object_store.as_ref(), table, project_id, &m).await?;
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
