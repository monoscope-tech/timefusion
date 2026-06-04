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
