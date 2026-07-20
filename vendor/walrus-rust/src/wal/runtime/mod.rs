use std::sync::{Arc, OnceLock, mpsc};

mod allocator;
mod background;
mod index;
mod position;
mod reader;
mod walrus;
mod walrus_read;
mod walrus_write;
mod writer;

#[allow(unused_imports)]
pub use index::{BlockPos, WalIndex};
pub use position::WalPosition;
pub use walrus::{ReadConsistency, Walrus};

pub(super) static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

/// Set by [`Walrus::request_reclaim_sweep`]; the background worker consumes it
/// on its next tick and runs the deletion sweep immediately instead of waiting
/// for the periodic 1000-tick cadence. Fully-consumed files enqueued during a
/// boot's cursor restore otherwise sit on disk for the whole first period.
pub(super) static SWEEP_NOW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
