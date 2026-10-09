//! Range-readable indexes: what a search must read, so it can be fetched by byte range
//! instead of installing the whole blob (Quickwit's hotcache).

use std::{
    collections::HashMap,
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tantivy::{
    Directory, HasLen,
    directory::{
        FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, OpenReadError, OpenWriteError},
    },
};

/// Every `(file, byte range)` a wrapped directory served, in order.
pub type ReadLog = Arc<Mutex<Vec<(PathBuf, Range<usize>)>>>;

/// Passes reads through to `inner`, logging each range read.
#[derive(Debug, Clone)]
pub struct RecordingDirectory<D> {
    inner: D,
    pub log: ReadLog,
}

impl<D> RecordingDirectory<D> {
    pub fn new(inner: D) -> Self {
        Self { inner, log: Default::default() }
    }
}

#[derive(Debug)]
struct RecordingHandle {
    path: PathBuf,
    inner: Arc<dyn FileHandle>,
    log: ReadLog,
}

impl HasLen for RecordingHandle {
    fn len(&self) -> usize {
        self.inner.len()
    }
}

#[async_trait::async_trait]
impl FileHandle for RecordingHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.log.lock().push((self.path.clone(), range.clone()));
        self.inner.read_bytes(range)
    }
}

impl<D: Directory + Clone> Directory for RecordingDirectory<D> {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(RecordingHandle { path: path.to_owned(), inner: self.inner.get_file_handle(path)?, log: self.log.clone() }))
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.inner.open_write(path)
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let bytes = self.inner.atomic_read(path)?;
        self.log.lock().push((path.to_owned(), 0..bytes.len()));
        Ok(bytes)
    }
    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.inner.atomic_write(path, data)
    }
    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }
    fn acquire_lock(&self, lock: &tantivy::directory::Lock) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        self.inner.acquire_lock(lock)
    }
    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

// A bundle is `[MAGIC][u32 table len][table json][hot bytes][zstd blocks…]`: every file cut
// into independently compressed `BUNDLE_BLOCK` blocks, so any byte range costs one ranged
// read, plus the bytes opening the index reads (the hotcache) stored raw up front, so one
// GET of the head opens it. A sequential reader streams it like the `tar.zst` it replaces.

const BUNDLE_MAGIC: &[u8; 4] = b"TFB1";
/// Uncompressed bytes per block; a search's postings ranges average ~90 KB.
pub const BUNDLE_BLOCK: usize = 64 << 10;

#[derive(Debug, Serialize, Deserialize)]
struct BundleFile {
    name: String,
    len: u64,
    /// Compressed size of each block, in order.
    blocks: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BundleTable {
    files: Vec<BundleFile>,
    /// `(file, start, len)` ranges stored raw after the table, in order.
    hot: Vec<(String, u64, u64)>,
}

pub fn is_bundle(head: &[u8]) -> bool {
    head.starts_with(BUNDLE_MAGIC)
}

/// The merged byte ranges that opening `dir` and creating a searcher read.
fn open_set(dir: &Path) -> anyhow::Result<Vec<(PathBuf, Range<usize>)>> {
    let recording = RecordingDirectory::new(tantivy::directory::MmapDirectory::open(dir)?);
    let log = recording.log.clone();
    drop(super::open_index_in(recording)?.reader()?.searcher());
    let mut ranges = std::mem::take(&mut *log.lock());
    ranges.sort_by(|(a, x), (b, y)| (a, x.start).cmp(&(b, y.start)));
    Ok(ranges
        .into_iter()
        .coalesce(|(pa, a), (pb, b)| if pa == pb && b.start <= a.end { Ok((pa, a.start..a.end.max(b.end))) } else { Err(((pa, a), (pb, b))) })
        .collect())
}

pub fn pack_bundle(dir: &Path, level: i32) -> anyhow::Result<Bytes> {
    use std::{io::Read, os::unix::fs::FileExt};
    let hot = open_set(dir)?;
    let mut names: Vec<String> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok().filter(|e| e.file_type().is_ok_and(|t| t.is_file())).map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect();
    names.sort();
    let (mut files, mut data, mut block) = (vec![], vec![], vec![0; BUNDLE_BLOCK]);
    for name in names {
        let mut file = std::fs::File::open(dir.join(&name))?;
        let mut entry = BundleFile { name, len: file.metadata()?.len(), blocks: vec![] };
        for start in (0..entry.len).step_by(BUNDLE_BLOCK) {
            let chunk = &mut block[..BUNDLE_BLOCK.min((entry.len - start) as usize)];
            file.read_exact(chunk)?;
            let compressed = zstd::bulk::compress(chunk, level)?;
            entry.blocks.push(u32::try_from(compressed.len())?);
            data.extend_from_slice(&compressed);
        }
        files.push(entry);
    }
    let mut hot_bytes = vec![];
    for (path, range) in &hot {
        let mut buf = vec![0; range.len()];
        std::fs::File::open(dir.join(path))?.read_exact_at(&mut buf, range.start as u64)?;
        hot_bytes.extend_from_slice(&buf);
    }
    let hot = hot.into_iter().map(|(path, r)| (path.to_string_lossy().into_owned(), r.start as u64, r.len() as u64)).collect();
    let table = serde_json::to_vec(&BundleTable { files, hot })?;
    let mut out = Vec::with_capacity(8 + table.len() + hot_bytes.len() + data.len());
    out.extend_from_slice(BUNDLE_MAGIC);
    out.extend_from_slice(&u32::try_from(table.len())?.to_le_bytes());
    out.extend_from_slice(&table);
    out.extend_from_slice(&hot_bytes);
    out.extend_from_slice(&data);
    Ok(out.into())
}

fn read_table(r: &mut impl std::io::Read) -> anyhow::Result<BundleTable> {
    let mut head = [0; 8];
    r.read_exact(&mut head)?;
    anyhow::ensure!(is_bundle(&head), "not a bundle");
    let mut table = vec![0; u32::from_le_bytes(head[4..].try_into()?) as usize];
    r.read_exact(&mut table)?;
    Ok(serde_json::from_slice(&table)?)
}

/// Stream a bundle into `dest` as plain files; memory is one block.
pub fn unpack_bundle(mut r: impl std::io::Read, dest: &Path) -> anyhow::Result<()> {
    use std::io::Write;
    let table = read_table(&mut r)?;
    std::io::copy(&mut std::io::Read::take(&mut r, table.hot.iter().map(|h| h.2).sum()), &mut std::io::sink())?;
    let mut buf = vec![];
    for file in &table.files {
        let mut out = std::io::BufWriter::new(std::fs::File::create(dest.join(&file.name))?);
        for &size in &file.blocks {
            buf.resize(size as usize, 0);
            r.read_exact(&mut buf)?;
            out.write_all(&zstd::bulk::decompress(&buf, BUNDLE_BLOCK)?)?;
        }
        out.flush()?;
    }
    Ok(())
}

/// Where a bundle's bytes come from: the blob in memory, or ranged object-store reads.
pub trait BundleSource: Send + Sync + std::fmt::Debug + 'static {
    fn read(&self, range: Range<u64>) -> io::Result<Bytes>;
}

impl BundleSource for Bytes {
    fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
        let range = usize::try_from(range.start).map_err(io::Error::other)?..usize::try_from(range.end).map_err(io::Error::other)?;
        (range.end <= self.len()).then(|| self.slice(range)).ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "bundle range past end"))
    }
}

#[derive(Debug)]
struct FileLayout {
    len: usize,
    /// Absolute offsets of each block, plus the end of the last.
    blocks: Vec<u64>,
    /// File range -> offset of its raw bytes in `head`.
    hot: Vec<(Range<usize>, usize)>,
}

/// A read-only tantivy directory over a bundle: hot ranges from the head, the rest by
/// decompressing the covering blocks, fetched in one read.
#[derive(Debug, Clone)]
pub struct BundleDirectory {
    head: Bytes,
    files: Arc<HashMap<PathBuf, FileLayout>>,
    source: Arc<dyn BundleSource>,
}

impl BundleDirectory {
    /// `head` must hold at least the table and hot bytes; `source` serves the whole bundle.
    pub fn new(head: Bytes, source: Arc<dyn BundleSource>) -> anyhow::Result<Self> {
        let mut cursor = &head[..];
        let table = read_table(&mut cursor)?;
        let hot_start = head.len() - cursor.len();
        let data_start = hot_start as u64 + table.hot.iter().map(|h| h.2).sum::<u64>();
        anyhow::ensure!(head.len() as u64 >= data_start, "bundle head is truncated");
        let mut hot_at = hot_start;
        let mut hot: HashMap<&str, Vec<(Range<usize>, usize)>> = HashMap::new();
        for (file, start, len) in &table.hot {
            hot.entry(file.as_str()).or_default().push((*start as usize..(start + len) as usize, hot_at));
            hot_at += *len as usize;
        }
        let mut offset = data_start;
        let files = table
            .files
            .iter()
            .map(|f| {
                let blocks = std::iter::once(offset).chain(f.blocks.iter().map(|&size| {
                    offset += u64::from(size);
                    offset
                }));
                (PathBuf::from(&f.name), FileLayout { len: f.len as usize, blocks: blocks.collect(), hot: hot.remove(f.name.as_str()).unwrap_or_default() })
            })
            .collect();
        Ok(Self { head, files: Arc::new(files), source })
    }

    /// The byte length of the whole bundle this head describes.
    pub fn bundle_len(&self) -> u64 {
        self.files.values().filter_map(|f| f.blocks.last().copied()).max().unwrap_or(0)
    }

    pub fn in_memory(blob: Bytes) -> anyhow::Result<Self> {
        Self::new(blob.clone(), Arc::new(blob))
    }

    fn read(&self, file: &FileLayout, range: Range<usize>) -> io::Result<Vec<u8>> {
        if range.is_empty() {
            return Ok(vec![]);
        }
        if let Some((hot, at)) = file.hot.iter().find(|(hot, _)| hot.start <= range.start && range.end <= hot.end) {
            let start = at + range.start - hot.start;
            return Ok(self.head[start..start + range.len()].to_vec());
        }
        let (first, last) = (range.start / BUNDLE_BLOCK, (range.end - 1) / BUNDLE_BLOCK);
        let span = self.source.read(file.blocks[first]..file.blocks[last + 1])?;
        let mut out = Vec::with_capacity((last - first + 1) * BUNDLE_BLOCK);
        for (block, end) in (first..=last).zip(&file.blocks[first + 1..=last + 1]) {
            let compressed = &span[(file.blocks[block] - file.blocks[first]) as usize..(end - file.blocks[first]) as usize];
            out.extend_from_slice(&zstd::bulk::decompress(compressed, BUNDLE_BLOCK)?);
        }
        let skip = range.start - first * BUNDLE_BLOCK;
        Ok(out[skip..skip + range.len()].to_vec())
    }
}

#[derive(Debug)]
struct BundleHandle {
    dir: BundleDirectory,
    path: PathBuf,
}

impl HasLen for BundleHandle {
    fn len(&self) -> usize {
        self.dir.files[&self.path].len
    }
}

#[async_trait::async_trait]
impl FileHandle for BundleHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        Ok(OwnedBytes::new(self.dir.read(&self.dir.files[&self.path], range)?))
    }
}

fn read_only(path: &Path) -> io::Error {
    io::Error::other(format!("bundle directory is read-only: {}", path.display()))
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.files
            .contains_key(path)
            .then(|| Arc::new(BundleHandle { dir: self.clone(), path: path.to_owned() }) as Arc<dyn FileHandle>)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path))
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file = self.files.get(path).ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_owned()))?;
        self.read(file, 0..file.len).map_err(|e| OpenReadError::wrap_io_error(e, path.to_owned()))
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError { io_error: Arc::new(read_only(path)), filepath: path.to_owned() })
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(read_only(path), path.to_owned()))
    }
    fn atomic_write(&self, path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(read_only(path))
    }
    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
    fn acquire_lock(&self, _lock: &tantivy::directory::Lock) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
        Ok(tantivy::directory::DirectoryLock::from(Box::new(())))
    }
    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::tantivy::{
        search::{PredsQuery, build_node_query},
        udf::{PredNode, TextMatchPred},
    };

    /// Per file extension: (reads, bytes read).
    fn summarize(log: &[(PathBuf, Range<usize>)]) -> BTreeMap<String, (usize, usize)> {
        let mut out: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        for (path, range) in log {
            let ext = path.extension().map_or_else(|| path.display().to_string(), |e| e.to_string_lossy().into_owned());
            let e = out.entry(ext).or_default();
            e.0 += 1;
            e.1 += range.len();
        }
        out
    }

    /// What opening an unpacked prod index and running needle queries reads.
    /// `TF_HOTCACHE_INDEX=<unpacked index dir> cargo nextest run --lib --run-ignored only measure_reads`
    #[test]
    #[ignore]
    fn measure_reads_of_a_needle_search() -> anyhow::Result<()> {
        let dir = std::env::var("TF_HOTCACHE_INDEX")?;
        let recording = RecordingDirectory::new(tantivy::directory::MmapDirectory::open(&dir)?);
        let log = recording.log.clone();
        let index = super::super::open_index_in(recording)?;
        let searcher = index.reader()?.searcher();
        println!("open: {:?}", summarize(&log.lock().drain(..).collect::<Vec<_>>()));
        for (column, query) in [("body", "timeout"), ("summary", "connection refused"), ("attributes", "shipment"), ("name", "GET"), ("level", "ERROR")] {
            let PredsQuery::Query(q) = build_node_query(&index, &PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() }))? else {
                println!("{column}: missing field");
                continue;
            };
            let hits = searcher.search(&*q, &tantivy::collector::Count)?;
            println!("{column}={query:?} hits={hits}: {:?}", summarize(&log.lock().drain(..).collect::<Vec<_>>()));
        }
        Ok(())
    }

    #[derive(Debug)]
    struct Counted(Bytes, Mutex<(usize, u64)>);
    impl BundleSource for Counted {
        fn read(&self, range: Range<u64>) -> io::Result<Bytes> {
            let mut c = self.1.lock();
            (c.0, c.1) = (c.0 + 1, c.1 + range.end - range.start);
            self.0.read(range)
        }
    }

    /// Bundle size and per-query object reads for an unpacked prod index.
    /// `TF_HOTCACHE_INDEX=<dir> cargo nextest run --lib --run-ignored only measure_bundle --no-capture`
    #[test]
    #[ignore]
    fn measure_bundle_reads() -> anyhow::Result<()> {
        let dir = std::env::var("TF_HOTCACHE_INDEX")?;
        let started = std::time::Instant::now();
        let blob = pack_bundle(Path::new(&dir), 3)?;
        let source = Arc::new(Counted(blob.clone(), Default::default()));
        let directory = BundleDirectory::new(blob.clone(), source.clone())?;
        println!(
            "bundle {} MB in {:?}, head {} KB",
            blob.len() >> 20,
            started.elapsed(),
            (directory.files.values().flat_map(|f| f.hot.iter().map(|h| h.0.len())).sum::<usize>()) >> 10
        );
        let index = super::super::open_index_in(directory)?;
        let searcher = index.reader()?.searcher();
        println!("open: {:?}", std::mem::take(&mut *source.1.lock()));
        for (column, query) in [("body", "timeout"), ("summary", "connection refused"), ("attributes", "shipment"), ("name", "GET"), ("level", "ERROR")] {
            let PredsQuery::Query(q) = build_node_query(&index, &PredNode::Leaf(TextMatchPred { column: column.into(), query: query.into() }))? else {
                continue;
            };
            let started = std::time::Instant::now();
            let hits = searcher.search(&*q, &tantivy::collector::Count)?;
            let (reads, bytes) = std::mem::take(&mut *source.1.lock());
            println!("{column}={query:?} hits={hits} reads={reads} bytes={} KB in {:?}", bytes >> 10, started.elapsed());
        }
        Ok(())
    }
}
