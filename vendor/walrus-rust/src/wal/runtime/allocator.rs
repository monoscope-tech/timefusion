use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, RwLock,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
};

use super::DELETION_TX;
use crate::wal::{
    block::Block,
    config::{DEFAULT_BLOCK_SIZE, MAX_ALLOC, MAX_FILE_SIZE, debug_print},
    paths::WalPathManager,
    storage::{SharedMmap, SharedMmapKeeper},
};

pub(super) struct BlockAllocator {
    /// A plain `Mutex`, not a hand-rolled spinlock: the critical section is not
    /// reliably tiny — a file rollover creates and mmaps a file inside it — and
    /// the fallible steps use `?`, so a spinlock released only on the success
    /// path stays LOCKED FOREVER after the first file-creation or mmap error,
    /// burning a core per waiter. The guard releases on every exit, error and
    /// unwind included.
    next_block: Mutex<Block>,
    paths: Arc<WalPathManager>,
}

impl BlockAllocator {
    pub(super) fn new(paths: Arc<WalPathManager>) -> std::io::Result<Self> {
        let file1 = paths.create_new_file()?;
        let mmap: Arc<SharedMmap> = SharedMmapKeeper::get_mmap_arc(&file1)?;
        debug_print!("[alloc] init: created file={}, max_file_size={}B, block_size={}B", file1, MAX_FILE_SIZE, DEFAULT_BLOCK_SIZE);
        Ok(BlockAllocator { next_block: Mutex::new(Block { id: 1, offset: 0, limit: DEFAULT_BLOCK_SIZE, file_path: file1, mmap, used: 0 }), paths })
    }

    /// Allocator state, recovering from poisoning.
    ///
    /// Every fallible step below assigns only AFTER its call returns, so a panic
    /// cannot tear the state mid-update; the worst an error leaves behind is a
    /// rolled-over `file_path` with the old `offset`, which the next call
    /// re-detects as needing rollover and repeats. Refusing to allocate forever
    /// is strictly worse than continuing from that.
    fn state(&self) -> MutexGuard<'_, Block> {
        self.next_block.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// SAFETY: Caller must ensure the returned `Block` is treated as uniquely
    /// owned by a single writer until it is sealed — two writers sharing one
    /// block would race on its mmap. The allocator itself hands out disjoint
    /// (id, offset) ranges under the state mutex.
    pub(super) unsafe fn get_next_available_block(&self) -> std::io::Result<Block> {
        let mut guard = self.state();
        let data = &mut *guard;
        let prev_block_file_path = data.file_path.clone();
        if data.offset >= MAX_FILE_SIZE {
            // mark previous file as fully allocated before switching
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            data.file_path = self.paths.create_new_file()?;
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path)?;
            data.offset = 0;
            data.used = 0;
            debug_print!("[alloc] rolled over to new file: {}", data.file_path);
        }

        // set the cur block as locked
        BlockStateTracker::register_block(data.id as usize, &data.file_path);
        FileStateTracker::register_file_if_absent(&data.file_path);
        FileStateTracker::add_block_to_file_state(&data.file_path);
        FileStateTracker::set_block_locked(data.id as usize);
        let ret = data.clone();
        data.offset += DEFAULT_BLOCK_SIZE;
        data.id += 1;
        drop(guard);
        debug_print!("[alloc] handout: block_id={}, file={}, offset={}, limit={}", ret.id, ret.file_path, ret.offset, ret.limit);
        Ok(ret)
    }

    /// SAFETY: Caller must ensure the resulting `Block` remains uniquely used
    /// by one writer and not read concurrently while being written. The state
    /// mutex only guarantees the handed-out ranges are disjoint.
    pub(super) unsafe fn alloc_block(&self, want_bytes: u64) -> std::io::Result<Block> {
        if want_bytes == 0 || want_bytes > MAX_ALLOC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid allocation size, a single entry can't be more than 1gb"));
        }
        let alloc_units = (want_bytes + DEFAULT_BLOCK_SIZE - 1) / DEFAULT_BLOCK_SIZE;
        let alloc_size = alloc_units * DEFAULT_BLOCK_SIZE;
        debug_print!("[alloc] alloc_block: want_bytes={}, units={}, size={}", want_bytes, alloc_units, alloc_size);

        let mut guard = self.state();
        let data = &mut *guard;
        if data.offset + alloc_size > MAX_FILE_SIZE {
            let prev_block_file_path = data.file_path.clone();
            data.file_path = self.paths.create_new_file()?;
            data.mmap = SharedMmapKeeper::get_mmap_arc(&data.file_path)?;
            data.offset = 0;
            // mark the previous file fully allocated now
            FileStateTracker::set_fully_allocated(prev_block_file_path);
            debug_print!("[alloc] file rollover for sized alloc -> {}", data.file_path);
        }
        let ret = Block { id: data.id, file_path: data.file_path.clone(), offset: data.offset, limit: alloc_size, mmap: data.mmap.clone(), used: 0 };
        // register the new block before handing it out
        BlockStateTracker::register_block(ret.id as usize, &ret.file_path);
        FileStateTracker::register_file_if_absent(&ret.file_path);
        FileStateTracker::add_block_to_file_state(&ret.file_path);
        FileStateTracker::set_block_locked(ret.id as usize);
        data.offset += alloc_size;
        data.id += 1;
        drop(guard);
        debug_print!("[alloc] handout(sized): block_id={}, file={}, offset={}, limit={}", ret.id, ret.file_path, ret.offset, ret.limit);
        Ok(ret)
    }
}

pub(super) fn flush_check(file_path: String) {
    // readiness check fast path; hook actual reclamation later
    if let Some((locked, checkpointed, total, fully_allocated)) = FileStateTracker::get_state_snapshot(&file_path) {
        let ready_to_delete = fully_allocated && locked == 0 && total > 0 && checkpointed >= total;
        if ready_to_delete {
            if let Some(tx) = DELETION_TX.get() {
                let _ = tx.send(file_path);
            }
        }
    }
}

struct BlockState {
    is_checkpointed: AtomicBool,
    file_path: String,
}

pub(super) struct BlockStateTracker {}

impl BlockStateTracker {
    fn map() -> &'static RwLock<HashMap<usize, BlockState>> {
        static MAP: OnceLock<RwLock<HashMap<usize, BlockState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    pub(super) fn register_block(block_id: usize, file_path: &str) {
        let map = Self::map();
        if let Ok(mut w) = map.write() {
            w.entry(block_id).or_insert_with(|| BlockState { is_checkpointed: AtomicBool::new(false), file_path: file_path.to_string() });
        }
    }

    pub(super) fn get_file_path_for_block(block_id: usize) -> Option<String> {
        let map = Self::map();
        let r = map.read().ok()?;
        r.get(&block_id).map(|b| b.file_path.clone())
    }

    pub(super) fn set_checkpointed_true(block_id: usize) {
        // Idempotent: only increment the file's checkpoint counter on the
        // false→true transition. Prior to this, repeated calls would
        // double-increment `checkpoint_block_ctr` (cursor rebases across
        // chain resets, or the new fast-forward path in
        // `set_persisted_read_position`), potentially overshooting `total`
        // without ever clearing — and on the other side of the comparison,
        // legitimately-checkpointed blocks could fail the `>= total` check
        // if a parallel duplicate consumed the increment "budget" earlier.
        let (path_opt, transitioned) = {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(b) = r.get(&block_id) {
                    let prev = b.is_checkpointed.swap(true, Ordering::AcqRel);
                    (Some(b.file_path.clone()), !prev)
                } else {
                    (None, false)
                }
            } else {
                (None, false)
            }
        };

        if let Some(path) = path_opt {
            if transitioned {
                FileStateTracker::inc_checkpoint_for_file(&path);
            }
            // Deliberate: call flush_check even when no transition happened.
            // It acts as a retry for files whose previous checkpoint observation
            // raced `set_fully_allocated` and didn't reclaim — replays of the
            // same block_id can still close out the file. Tradeoff: a slightly
            // hotter DELETION_TX channel (one extra send per duplicate call)
            // in exchange for correctness against racing observers.
            flush_check(path);
        }
    }
}

struct FileState {
    locked_block_ctr: AtomicU16,
    checkpoint_block_ctr: AtomicU16,
    total_blocks: AtomicU16,
    is_fully_allocated: AtomicBool,
}

pub(super) struct FileStateTracker {}

impl FileStateTracker {
    fn map() -> &'static RwLock<HashMap<String, FileState>> {
        static MAP: OnceLock<RwLock<HashMap<String, FileState>>> = OnceLock::new();
        MAP.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Snapshot of all tracked file paths — lets a reclaim sweep re-run
    /// `flush_check` across every file, closing the window where a
    /// checkpoint-time eligibility check raced `set_fully_allocated` /
    /// block-unlock and nothing ever retried (the in-line retry relies on a
    /// FUTURE `set_checkpointed_true` for the same file, which never comes
    /// after a final cursor fast-forward).
    pub(super) fn all_paths() -> Vec<String> {
        Self::map().read().map(|r| r.keys().cloned().collect()).unwrap_or_default()
    }

    pub(super) fn register_file_if_absent(file_path: &str) {
        let map = Self::map();
        let mut w = map.write().expect("file state map write lock poisoned");
        w.entry(file_path.to_string()).or_insert_with(|| FileState {
            locked_block_ctr: AtomicU16::new(0),
            checkpoint_block_ctr: AtomicU16::new(0),
            total_blocks: AtomicU16::new(0),
            is_fully_allocated: AtomicBool::new(false),
        });
    }

    pub(super) fn add_block_to_file_state(file_path: &str) {
        Self::register_file_if_absent(file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.total_blocks.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub(super) fn set_fully_allocated(file_path: String) {
        Self::register_file_if_absent(&file_path);
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(&file_path) {
                st.is_fully_allocated.store(true, Ordering::Release);
            }
        }
        flush_check(file_path);
    }

    pub(super) fn set_block_locked(block_id: usize) {
        if let Some(path) = BlockStateTracker::get_file_path_for_block(block_id) {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(st) = r.get(&path) {
                    st.locked_block_ctr.fetch_add(1, Ordering::AcqRel);
                }
            }
        }
    }

    pub(super) fn set_block_unlocked(block_id: usize) {
        if let Some(path) = BlockStateTracker::get_file_path_for_block(block_id) {
            let map = Self::map();
            if let Ok(r) = map.read() {
                if let Some(st) = r.get(&path) {
                    st.locked_block_ctr.fetch_sub(1, Ordering::AcqRel);
                }
            }
            flush_check(path);
        }
    }

    pub(super) fn inc_checkpoint_for_file(file_path: &str) {
        let map = Self::map();
        if let Ok(r) = map.read() {
            if let Some(st) = r.get(file_path) {
                st.checkpoint_block_ctr.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    pub(super) fn get_state_snapshot(file_path: &str) -> Option<(u16, u16, u16, bool)> {
        let map = Self::map();
        let r = map.read().ok()?;
        let st = r.get(file_path)?;
        let locked = st.locked_block_ctr.load(Ordering::Acquire);
        let checkpointed = st.checkpoint_block_ctr.load(Ordering::Acquire);
        let total = st.total_blocks.load(Ordering::Acquire);
        let fully = st.is_fully_allocated.load(Ordering::Acquire);
        Some((locked, checkpointed, total, fully))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::config::BLOCKS_PER_FILE;

    /// A failed file rollover must leave the allocator USABLE.
    ///
    /// The state lock was a hand-rolled spinlock released only on the success
    /// path, so the `?` on `create_new_file` returned while still holding it and
    /// every later allocation spun forever on a live core. The second attempt
    /// below is the assertion: it must come back (with an error), not hang.
    ///
    /// Reinstate the spinlock and this fails on the `recv_timeout`.
    #[test]
    fn a_failed_rollover_releases_the_state_lock() {
        let root = std::env::temp_dir().join(format!("walrus-rollover-wedge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let allocator = Arc::new(BlockAllocator::new(Arc::new(WalPathManager::under(root.clone()))).expect("initial file"));

        // Make every later `create_new_file` fail for ANY uid: `ensure_root`'s
        // `create_dir_all` cannot succeed while the root path is a regular file.
        // (A permissions-based injection would be a no-op for root, which is how
        // CI runs.)
        std::fs::remove_dir_all(&root).unwrap();
        std::fs::write(&root, b"not a directory").unwrap();

        // The first BLOCKS_PER_FILE hand-outs consume the initial file; the next
        // needs a new one and fails.
        let failed = (0..=BLOCKS_PER_FILE).any(|_| unsafe { allocator.get_next_available_block() }.is_err());
        assert!(failed, "rollover should fail while the WAL root is a regular file");

        let (tx, rx) = std::sync::mpsc::channel();
        let probe = Arc::clone(&allocator);
        std::thread::spawn(move || {
            let _ = tx.send(unsafe { probe.get_next_available_block() }.is_err());
        });
        let still_answers = rx.recv_timeout(std::time::Duration::from_secs(5)).expect("allocator wedged: the failed rollover never released the state lock");
        assert!(still_answers, "the root is still a file, so this must error rather than hand out a block");

        let _ = std::fs::remove_file(&root);
    }
}
