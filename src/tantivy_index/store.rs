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

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjPath};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use tantivy::Index;

pub const INDEX_PREFIX: &str = "indexes";
pub const INDEX_VERSION: &str = "v1";
pub const BLOB_SUFFIX: &str = ".tantivy.tar.zst";

/// Object-store path for a given parquet file's index blob.
pub fn blob_path(table: &str, project_id: &str, file_uuid: &str) -> ObjPath {
    ObjPath::from(format!("{INDEX_PREFIX}/{table}/{INDEX_VERSION}/{project_id}/{file_uuid}{BLOB_SUFFIX}"))
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot, then
/// pack it into a `tar.zst` blob. Avoids any RAM→disk copy.
pub fn build_and_pack(
    table: &crate::schema_loader::TableSchema,
    batches: &[arrow::record_batch::RecordBatch],
    level: i32,
) -> Result<(Bytes, crate::tantivy_index::builder::IndexBuildStats)> {
    let tmp = tempfile::tempdir().context("build_and_pack: tempdir")?;
    let (_built, stats) = build_to_dir(table, batches, tmp.path())?;
    let bytes = pack_dir(tmp.path(), level)?;
    Ok((bytes, stats))
}

/// Build a tantivy `Index` to a fresh on-disk directory in one shot.
pub fn build_to_dir(
    table: &crate::schema_loader::TableSchema,
    batches: &[arrow::record_batch::RecordBatch],
    dir: &Path,
) -> Result<(crate::tantivy_index::schema::BuiltSchema, crate::tantivy_index::builder::IndexBuildStats)> {
    use tantivy::directory::MmapDirectory;
    let built = crate::tantivy_index::schema::build_for_table(table);
    let mmap_dir = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::create(mmap_dir, built.schema.clone(), Default::default()).map_err(|e| anyhow!("create disk index: {e}"))?;
    crate::tantivy_index::schema::register_tokenizers(&index);
    let stats = crate::tantivy_index::builder::index_to_writer(&built, &index, batches)?;
    Ok((built, stats))
}

/// Tar+zstd a directory into a Bytes buffer.
pub fn pack_dir(dir: &Path, level: i32) -> Result<Bytes> {
    let mut tar_buf: Vec<u8> = Vec::new();
    {
        let mut tar = tar::Builder::new(&mut tar_buf);
        tar.append_dir_all(".", dir).context("tar append")?;
        tar.finish().context("tar finish")?;
    }
    let mut compressed: Vec<u8> = Vec::with_capacity(tar_buf.len() / 4);
    let mut enc = zstd::Encoder::new(&mut compressed, level).context("zstd encoder")?;
    enc.write_all(&tar_buf).context("zstd write")?;
    enc.finish().context("zstd finish")?;
    Ok(Bytes::from(compressed))
}

/// Unpack a tar.zst blob into a fresh directory under `dest`.
pub fn unpack_to_dir(blob: &[u8], dest: &Path) -> Result<()> {
    std::fs::create_dir_all(dest).context("mkdir dest")?;
    let cursor = Cursor::new(blob);
    let mut decoder = zstd::Decoder::new(cursor).context("zstd decoder")?;
    let mut tar_bytes: Vec<u8> = Vec::new();
    decoder.read_to_end(&mut tar_bytes).context("zstd decode")?;
    let mut archive = tar::Archive::new(Cursor::new(tar_bytes));
    archive.unpack(dest).context("tar unpack")?;
    Ok(())
}

/// Open an unpacked tantivy index for querying.
pub fn open_index(dir: &Path) -> Result<Index> {
    use tantivy::directory::MmapDirectory;
    let mm = MmapDirectory::open(dir).map_err(|e| anyhow!("open mmap dir: {e}"))?;
    let index = Index::open(mm).map_err(|e| anyhow!("open index: {e}"))?;
    // Tokenizer registry is per-Index, not persisted, so the reader must
    // re-register exactly the same chains the writer used. Mismatch ⇒ silent
    // miss (tantivy looks up by name and falls back to default).
    crate::tantivy_index::schema::register_tokenizers(&index);
    Ok(index)
}

pub async fn upload(store: &dyn ObjectStore, path: &ObjPath, blob: Bytes) -> Result<()> {
    store.put(path, blob.into()).await.with_context(|| format!("upload {path}"))?;
    Ok(())
}

pub async fn download(store: &dyn ObjectStore, path: &ObjPath) -> Result<Bytes> {
    let result = store.get(path).await.with_context(|| format!("get {path}"))?;
    Ok(result.bytes().await.with_context(|| format!("read {path}"))?)
}

pub async fn delete(store: &dyn ObjectStore, path: &ObjPath) -> Result<()> {
    store.delete(path).await.with_context(|| format!("delete {path}"))?;
    Ok(())
}

/// Local cache directory for a (project_id, table, file_uuid).
pub fn local_cache_path(root: &Path, table: &str, project_id: &str, file_uuid: &str) -> PathBuf {
    root.join("tantivy_cache").join(table).join(project_id).join(file_uuid)
}
