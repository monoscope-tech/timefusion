//! Public position type + APIs added for TimeFusion's zero-replay-shutdown
//! work. Lets callers snapshot the current write tail per topic and later
//! set the persisted-read cursor directly to that position, atomically.
//!
//! Walrus's internal cursor format `(cur_block_idx, cur_block_offset)` uses a
//! TAIL_FLAG bit to distinguish "chain index" from "active tail block id".
//! `WalPosition` always carries the persistent block id; on read, walrus's
//! existing fold logic in `read_next` rebases tail-form positions to
//! chain-index form if the target block has since been sealed.

use std::io;

use super::{
    Walrus,
    allocator::{BlockStateTracker, FileStateTracker},
};

const TAIL_FLAG: u64 = 1u64 << 63;

/// A position in the WAL for a single topic — `block_id` is the persistent
/// block identifier, `offset` is bytes consumed within that block.
///
/// `(0, 0)` is the sentinel meaning "origin / unread". A position obtained
/// from [`Walrus::current_position`] can be persisted by the caller (e.g.
/// to durable storage alongside downstream data) and later replayed via
/// [`Walrus::set_persisted_read_position`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalPosition {
    pub block_id: u64,
    pub offset:   u64,
}

impl WalPosition {
    pub const ORIGIN: WalPosition = WalPosition { block_id: 0, offset: 0 };

    pub fn is_origin(&self) -> bool {
        self.block_id == 0 && self.offset == 0
    }
}

impl Walrus {
    /// Snapshot the current write tail for `col_name`. Reading up to this
    /// position consumes exactly the entries currently durable for `col_name`.
    ///
    /// Returns [`WalPosition::ORIGIN`] if the column has never been written
    /// in this process. A column that was written in a *previous* process
    /// run but not yet in this one returns the chain tail (last sealed
    /// block's used offset), since the writer is created lazily on append.
    pub fn current_position(&self, col_name: &str) -> io::Result<WalPosition> {
        // Active writer present? Use its tail.
        if let Ok(map) = self.writers.read() {
            if let Some(w) = map.get(col_name) {
                let (block, written) = w.snapshot_block()?;
                return Ok(WalPosition {
                    block_id: block.id,
                    offset:   written,
                });
            }
        }

        // No active writer — column may exist in the recovered chain but
        // hasn't been appended to in this session. Use the last sealed
        // block's tail.
        if let Ok(map) = self.reader.data.read() {
            if let Some(info_arc) = map.get(col_name) {
                if let Ok(info) = info_arc.read() {
                    if let Some(last) = info.chain.last() {
                        return Ok(WalPosition {
                            block_id: last.id,
                            offset:   last.used,
                        });
                    }
                }
            }
        }

        Ok(WalPosition::ORIGIN)
    }

    /// Read the persisted read cursor for `col_name` without consuming.
    /// Returns `None` when no cursor has been persisted yet (column never
    /// read) or when the persisted state can't be mapped back to a public
    /// position (e.g. an internal chain index pointing past current chain).
    /// `Some(WalPosition::ORIGIN)` means "cursor at start of log".
    pub fn persisted_read_position(&self, col_name: &str) -> io::Result<Option<WalPosition>> {
        let idx_guard = self.read_offset_index.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "index lock poisoned"))?;
        let Some(pos) = idx_guard.get(col_name) else {
            return Ok(None);
        };
        if (pos.cur_block_idx & TAIL_FLAG) != 0 {
            let block_id = pos.cur_block_idx & (!TAIL_FLAG);
            return Ok(Some(WalPosition {
                block_id,
                offset: pos.cur_block_offset,
            }));
        }
        // Chain-index form — resolve to a persistent block_id via the reader's chain.
        drop(idx_guard);
        let map = self.reader.data.read().ok();
        let Some(map) = map else {
            return Ok(None);
        };
        let info_arc = match map.get(col_name) {
            Some(a) => a.clone(),
            None => return Ok(Some(WalPosition::ORIGIN)),
        };
        drop(map);
        let info = info_arc.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "col info lock poisoned"))?;
        let idx = self.read_offset_index.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "index lock poisoned"))?;
        let Some(pos) = idx.get(col_name) else {
            return Ok(None);
        };
        let chain_idx = pos.cur_block_idx as usize;
        if chain_idx < info.chain.len() {
            Ok(Some(WalPosition {
                block_id: info.chain[chain_idx].id,
                offset:   pos.cur_block_offset,
            }))
        } else if chain_idx == info.chain.len() && !info.chain.is_empty() {
            // Past the last sealed block; use the last block's tail.
            let last = info.chain.last().unwrap();
            Ok(Some(WalPosition {
                block_id: last.id,
                offset:   last.used,
            }))
        } else {
            Ok(None)
        }
    }

    /// Set the persisted-read cursor for `col_name` to `pos`. Atomic fsync
    /// (via `WalIndex::set`).
    ///
    /// Writes the position in tail-flag form unless we recognise `block_id`
    /// as a sealed block in the in-memory chain, in which case we write the
    /// chain-index form directly to avoid the rebase round-trip on the next
    /// read. Either form is correct; `read_next` handles both.
    ///
    /// Invalidates the in-memory `hydrated_from_index` flag so the next
    /// `read_next` rereads the on-disk index instead of using a stale
    /// in-memory cursor.
    pub fn set_persisted_read_position(&self, col_name: &str, pos: WalPosition) -> io::Result<()> {
        if pos.is_origin() {
            let mut idx_guard = self.read_offset_index.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "index lock poisoned"))?;
            idx_guard.set(col_name.to_string(), 0, 0)?;
            self.invalidate_hydration(col_name);
            return Ok(());
        }

        // Try chain form first.
        let chain_form = self.find_chain_position(col_name, pos);

        let mut idx_guard = self.read_offset_index.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "index lock poisoned"))?;
        match chain_form {
            Some((idx, off)) => idx_guard.set(col_name.to_string(), idx, off)?,
            None => idx_guard.set(col_name.to_string(), pos.block_id | TAIL_FLAG, pos.offset)?,
        }
        drop(idx_guard);

        // Mark every sealed block strictly before `pos` as checkpointed. The
        // normal advance path (`read_next`) does this lazily as the cursor
        // walks past each block; a Delta-derived fast-forward (TimeFusion's
        // `derive_wal_cursor_for_table`) bypasses `read_next` entirely, so
        // without this loop the skipped-over blocks stay uncheckpointed
        // forever and their containing files never become eligible for
        // deletion (`flush_check` requires `checkpointed >= total`).
        //
        // We checkpoint a block when its id is strictly less than the target
        // block_id, OR equal-id with the cursor having consumed past the
        // block's `used` bytes (the same condition `read_next` uses).
        self.checkpoint_blocks_before(col_name, pos);

        self.invalidate_hydration(col_name);
        Ok(())
    }

    /// Test/diagnostic accessor: per-block file accounting for the file
    /// owning `block_id`. Returns `(checkpointed, total)` block counts on
    /// that file — file becomes eligible for deletion when
    /// `checkpointed >= total` (and unlocked, fully allocated). Useful for
    /// regression tests covering the cursor fast-forward → file-reclaim
    /// path; not part of normal runtime use.
    // test-only: kept `pub` so cross-crate regression tests in tests/position.rs
    // can probe file-level checkpoint counts after fast-forward.
    #[doc(hidden)]
    pub fn block_file_checkpoint_state(block_id: u64) -> Option<(u16, u16)> {
        let path = BlockStateTracker::get_file_path_for_block(block_id as usize)?;
        FileStateTracker::get_state_snapshot(&path).map(|(_l, c, t, _f)| (c, t))
    }

    fn checkpoint_blocks_before(&self, col_name: &str, pos: WalPosition) {
        let Ok(map) = self.reader.data.read() else { return };
        let Some(info_arc) = map.get(col_name).cloned() else { return };
        drop(map);
        let Ok(info) = info_arc.read() else { return };
        for block in info.chain.iter() {
            let past_block = block.id < pos.block_id || (block.id == pos.block_id && pos.offset >= block.used);
            if past_block {
                BlockStateTracker::set_checkpointed_true(block.id as usize);
            }
        }
    }

    fn find_chain_position(&self, col_name: &str, pos: WalPosition) -> Option<(u64, u64)> {
        let map = self.reader.data.read().ok()?;
        let info_arc = map.get(col_name)?.clone();
        drop(map);
        let info = info_arc.read().ok()?;
        let (idx, block) = info.chain.iter().enumerate().find(|(_, b)| b.id == pos.block_id)?;
        // Past the block's used? Normalise to next chain index, offset 0.
        Some(if pos.offset >= block.used { (idx as u64 + 1, 0) } else { (idx as u64, pos.offset) })
    }

    /// Resets the in-memory cursor and re-arms `hydrated_from_index` so the next
    /// `read_next` re-reads the on-disk index. Without resetting the cursor we'd
    /// leak state from prior reads that's now inconsistent with the freshly-set
    /// position.
    fn invalidate_hydration(&self, col_name: &str) {
        if let Ok(map) = self.reader.data.read() {
            if let Some(info_arc) = map.get(col_name) {
                if let Ok(mut info) = info_arc.write() {
                    info.hydrated_from_index = false;
                    info.cur_block_idx = 0;
                    info.cur_block_offset = 0;
                    info.tail_block_id = 0;
                    info.tail_offset = 0;
                }
            }
        }
    }
}
