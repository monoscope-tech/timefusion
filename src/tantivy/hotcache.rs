//! Range-readable indexes: what a search must read, so it can be fetched by byte range
//! instead of installing the whole blob (Quickwit's hotcache).

use std::{
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::Mutex;
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
}
