#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::{HashMap, hash_map::Entry},
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use memmap2::MmapMut;

use crate::wal::config::{FsyncSchedule, USE_FD_BACKEND};

#[derive(Debug)]
pub(crate) struct FdBackend {
    file: std::fs::File,
    len: usize,
}

impl FdBackend {
    fn new(path: &str, use_o_sync: bool) -> std::io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        #[cfg(unix)]
        if use_o_sync {
            opts.custom_flags(libc::O_SYNC);
        }

        let file = opts.open(path)?;
        let metadata = file.metadata()?;
        let len = metadata.len() as usize;

        Ok(Self { file, len })
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        use std::os::unix::fs::FileExt;
        // pwrite doesn't move the file cursor
        let _ = self.file.write_at(data, offset as u64);
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        use std::os::unix::fs::FileExt;
        // pread doesn't move the file cursor
        let _ = self.file.read_at(dest, offset as u64);
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn file(&self) -> &std::fs::File {
        &self.file
    }
}

#[derive(Debug)]
pub(crate) enum StorageImpl {
    Mmap(MmapMut),
    Fd(FdBackend),
}

impl StorageImpl {
    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset <= mmap.len());
                debug_assert!(mmap.len() - offset >= data.len());
                unsafe {
                    let ptr = mmap.as_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
                }
            }
            StorageImpl::Fd(fd) => fd.write(offset, data),
        }
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset + dest.len() <= mmap.len());
                let src = &mmap[offset..offset + dest.len()];
                dest.copy_from_slice(src);
            }
            StorageImpl::Fd(fd) => fd.read(offset, dest),
        }
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => mmap.flush(),
            StorageImpl::Fd(fd) => fd.flush(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            StorageImpl::Mmap(mmap) => mmap.len(),
            StorageImpl::Fd(fd) => fd.len(),
        }
    }

    pub(crate) fn as_fd(&self) -> Option<&FdBackend> {
        if let StorageImpl::Fd(fd) = self { Some(fd) } else { None }
    }
}

static GLOBAL_FSYNC_SCHEDULE: OnceLock<FsyncSchedule> = OnceLock::new();

fn should_use_o_sync() -> bool {
    GLOBAL_FSYNC_SCHEDULE.get().map(|s| matches!(s, FsyncSchedule::SyncEach)).unwrap_or(false)
}

fn create_storage_impl(path: &str) -> std::io::Result<StorageImpl> {
    if USE_FD_BACKEND.load(Ordering::Relaxed) {
        let use_o_sync = should_use_o_sync();
        Ok(StorageImpl::Fd(FdBackend::new(path, use_o_sync)?))
    } else {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        // SAFETY: `file` is opened read/write and lives for the duration of this
        // mapping; `memmap2` upholds aliasing invariants for `MmapMut`.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(StorageImpl::Mmap(mmap))
    }
}

#[derive(Debug)]
pub(crate) struct SharedMmap {
    storage: StorageImpl,
    last_touched_at: AtomicU64,
}

// SAFETY: `SharedMmap` provides interior mutability only via methods that
// enforce bounds and perform atomic timestamp updates; the underlying
// storage supports concurrent reads and explicit flushes.
unsafe impl Sync for SharedMmap {}
// SAFETY: The struct holds storage that is safe to move between threads;
// timestamps are atomics, so sending is sound.
unsafe impl Send for SharedMmap {}

impl SharedMmap {
    pub(crate) fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let storage = create_storage_impl(path)?;

        let now_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(|_| std::time::Duration::from_secs(0)).as_millis() as u64;
        Ok(Arc::new(Self { storage, last_touched_at: AtomicU64::new(now_ms) }))
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) {
        // Bounds check before raw copy to maintain memory safety
        debug_assert!(offset <= self.storage.len());
        debug_assert!(self.storage.len() - offset >= data.len());

        self.storage.write(offset, data);

        let now_ms = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(|_| std::time::Duration::from_secs(0)).as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) {
        debug_assert!(offset + dest.len() <= self.storage.len());
        self.storage.read(offset, dest);
    }

    pub(crate) fn len(&self) -> usize {
        self.storage.len()
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.storage.flush()
    }

    pub(crate) fn storage(&self) -> &StorageImpl {
        &self.storage
    }
}

/// The one open handle per WAL file, shared by every block in it. Blocks look
/// their file up here per access instead of owning a handle, so dropping an
/// entry ([`release_file`]) is what finally closes a deleted file.
pub(crate) struct SharedMmapKeeper;

impl SharedMmapKeeper {
    fn map() -> &'static RwLock<HashMap<PathBuf, Arc<SharedMmap>>> {
        static MAP: OnceLock<RwLock<HashMap<PathBuf, Arc<SharedMmap>>>> = OnceLock::new();
        MAP.get_or_init(Default::default)
    }

    fn poisoned() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper lock poisoned")
    }

    /// The cached handle, never opening one: fsync must not resurrect a file
    /// that was released.
    pub(crate) fn cached(path: &str) -> Option<Arc<SharedMmap>> {
        Self::map().read().ok()?.get(Path::new(path)).cloned()
    }

    pub(crate) fn get_mmap_arc(path: &str) -> std::io::Result<Arc<SharedMmap>> {
        if let Some(existing) = Self::map().read().map_err(|_| Self::poisoned())?.get(Path::new(path)) {
            return Ok(existing.clone());
        }
        Ok(match Self::map().write().map_err(|_| Self::poisoned())?.entry(PathBuf::from(path)) {
            Entry::Occupied(existing) => existing.get().clone(),
            Entry::Vacant(slot) => slot.insert(SharedMmap::new(path)?).clone(),
        })
    }
}

/// Close walrus's handle on a WAL file. Call AFTER unlinking it: a reader
/// racing the release then fails to re-open the missing path instead of
/// caching a fresh handle to a file that is about to disappear.
pub fn release_file(path: &Path) {
    if let Ok(mut map) = SharedMmapKeeper::map().write() {
        map.remove(path);
    }
}

pub(crate) fn set_fsync_schedule(schedule: FsyncSchedule) {
    let _ = GLOBAL_FSYNC_SCHEDULE.set(schedule);
}

pub(crate) fn fsync_schedule() -> Option<FsyncSchedule> {
    GLOBAL_FSYNC_SCHEDULE.get().copied()
}
