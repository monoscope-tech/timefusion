//! Pack/unpack tantivy indexes for object-store transport.
//!
//! Cold form: a single `tar.zst` blob per parquet file.
//! Warm form: an extracted directory (used to mmap-open via tantivy::Index).
//!
//! Path conventions (rooted under whatever prefix the caller chose):
//!   indexes/{table}/v1/{project_id}/{file_uuid}.tantivy.tar.zst
//!
//! `pack_index` serializes the in-memory `Index` to bytes; `unpack_to_dir`
//! is the inverse. Upload/download are thin wrappers around `ObjectStore`.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use tantivy::{Index, directory::MmapDirectory};

use crate::{
    schema_loader::TableSchema,
    tantivy_index::{
        builder::{IndexBuildStats, MergeMode, index_to_writer},
        schema::{BuiltSchema, build_for_table, register_tokenizers},
    },
};

pub const INDEX_PREFIX: &str = "indexes";
pub const INDEX_VERSION: &str = "v1";
pub const BLOB_SUFFIX: &str = ".tantivy.tar.zst";

/// Object-store path for a given parquet file's index blob.
pub fn blob_path(table: &str, project_id: &str, file_uuid: &str) -> ObjPath {
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{project_id}/{file_uuid}{BLOB_SUFFIX}"))
}

/// Partition-mirrored index blob path derived from a parquet file's path
/// relative to its Delta table root, e.g.
///   project_id=<uuid>/date=<d>/part-<id>-c000.zstd.parquet
/// → indexes/{table}/v1/project_id=<uuid>/date=<d>/part-<id>-c000.zstd.tantivy.tar.zst
///
/// A pure suffix swap under the version prefix, so the mapping is 1:1 with the
/// parquet tree and reversible (`index_to_parquet_rel` is the inverse):
/// "does every live parquet have an index?" / "are there orphan blobs?" reduce
/// to a list + diff against the Delta add-file set.
pub fn index_path_for_parquet(table: &str, parquet_rel: &str) -> ObjPath {
    let stem = parquet_rel.strip_suffix(".parquet").unwrap_or(parquet_rel);
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{stem}{BLOB_SUFFIX}"))
}

/// Inverse of `index_path_for_parquet`: recover the table-relative parquet
/// path from an index blob path, or `None` if it isn't a partition-mirrored
/// blob for `table`. Used by reconcile to detect orphan blobs (no live parquet).
pub fn index_to_parquet_rel(table: &str, blob_path: &str) -> Option<String> {
    let prefix = format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/");
    let stem = blob_path.strip_prefix(&prefix)?.strip_suffix(BLOB_SUFFIX)?;
    Some(format!("{stem}.parquet"))
}

/// Read a parquet file (path relative to the Delta table root) back into Arrow
/// RecordBatches via the object store. Powers indexing a file that's already
/// committed (post-optimize reindex, reconcile, backfill) — unlike the flush
/// path it has no live in-memory batches to consume.
pub async fn read_parquet_batches(store: Arc<dyn ObjectStore>, parquet_rel: &str) -> Result<Vec<RecordBatch>> {
    use deltalake::datafusion::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
    use futures::TryStreamExt;
    let path = ObjPath::from(parquet_rel);
    let meta = store.head(&path).await.with_context(|| format!("head {parquet_rel}"))?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader).await.context("parquet stream builder")?.build().context("build parquet stream")?;
    stream.try_collect::<Vec<_>>().await.context("collect parquet batches")
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot, then
/// pack it into a `tar.zst` blob. Avoids any RAM→disk copy.
pub fn build_and_pack(table: &TableSchema, batches: &[RecordBatch], level: i32, merge: MergeMode) -> Result<(Bytes, IndexBuildStats)> {
    let tmp = tempfile::tempdir().context("build_and_pack: tempdir")?;
    let (_built, stats) = build_to_dir(table, batches, tmp.path(), merge)?;
    Ok((pack_dir(tmp.path(), level)?, stats))
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot.
pub fn build_to_dir(table: &TableSchema, batches: &[RecordBatch], dir: &Path, merge: MergeMode) -> Result<(BuiltSchema, IndexBuildStats)> {
    let built = build_for_table(table);
    let mmap_dir = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::create(mmap_dir, built.schema.clone(), Default::default()).map_err(|e| anyhow!("create disk index: {e}"))?;
    register_tokenizers(&index);
    let stats = index_to_writer(&built, &index, batches, merge)?;
    Ok((built, stats))
}

/// Tar+zstd a directory into a Bytes buffer.
pub fn pack_dir(dir: &Path, level: i32) -> Result<Bytes> {
    let tar_buf = {
        let mut tar = tar::Builder::new(Vec::new());
        tar.append_dir_all(".", dir).context("tar append")?;
        tar.into_inner().context("tar finish")?
    };
    zstd::encode_all(&tar_buf[..], level).map(Bytes::from).context("zstd encode")
}

/// Unpack a tar.zst blob into a fresh directory under `dest`.
pub fn unpack_to_dir(blob: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest).context("mkdir dest")?;
    let tar_bytes = zstd::decode_all(blob).context("zstd decode")?;
    tar::Archive::new(&tar_bytes[..]).unpack(dest).context("tar unpack")
}

/// Round-trip a freshly packed blob (unpack + open) before publishing it, so a
/// structurally-corrupt archive is never uploaded. Blob paths are immutable and
/// reader-cached, so a poison blob would otherwise fail every future read until
/// a manual reindex.
pub fn verify_blob(blob: &[u8]) -> Result<()> {
    let tmp = tempfile::tempdir().context("verify: tempdir")?;
    unpack_to_dir(blob, tmp.path())?;
    open_index(tmp.path()).map(drop)
}

/// Open an unpacked tantivy index for querying.
pub fn open_index(dir: &Path) -> Result<Index> {
    let mm = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::open(mm).map_err(|e| anyhow!("open index: {e}"))?;
    // Tokenizer registry is per-Index, not persisted, so the reader must
    // re-register exactly the same chains the writer used. Mismatch ⇒ silent
    // miss (tantivy looks up by name and falls back to default).
    register_tokenizers(&index);
    Ok(index)
}

pub async fn upload(store: &dyn ObjectStore, path: &ObjPath, blob: Bytes) -> Result<()> {
    store.put(path, blob.into()).await.with_context(|| format!("upload {path}")).map(drop)
}

pub async fn download(store: &dyn ObjectStore, path: &ObjPath) -> Result<Bytes> {
    let result = store.get(path).await.with_context(|| format!("get {path}"))?;
    result.bytes().await.with_context(|| format!("read {path}"))
}

pub async fn delete(store: &dyn ObjectStore, path: &ObjPath) -> Result<()> {
    store.delete(path).await.with_context(|| format!("delete {path}")).map(drop)
}

/// Local cache directory for a (project_id, table, file_uuid).
pub fn local_cache_path(root: &Path, table: &str, project_id: &str, file_uuid: &str) -> PathBuf {
    root.join("tantivy_cache").join(table).join(project_id).join(file_uuid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_index_path_is_partition_mirrored_and_reversible() {
        let table = "otel_logs_and_spans";
        let rel = "project_id=abc-123/date=2026-06-30/part-00000-deadbeef-c000.zstd.parquet";
        let blob = index_path_for_parquet(table, rel).to_string();
        assert_eq!(blob, "indexes/otel_logs_and_spans/v1/project_id=abc-123/date=2026-06-30/part-00000-deadbeef-c000.zstd.tantivy.tar.zst");
        // inverse recovers the exact parquet rel path
        assert_eq!(index_to_parquet_rel(table, &blob).as_deref(), Some(rel));
        // a blob for a different table / a non-blob path is not ours
        assert_eq!(index_to_parquet_rel("other_table", &blob), None);
        assert_eq!(index_to_parquet_rel(table, "indexes/otel_logs_and_spans/v1/foo.txt"), None);
    }
}
