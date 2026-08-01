//! Best-effort durable metadata for sealed-bin dedup scheduling.

use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirtyBin {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub bin: i64,
}

fn path(data_dir: &Path) -> PathBuf {
    crate::wal::meta_path(data_dir, "dedup_dirty_bins.json")
}

pub fn load(data_dir: &Path) -> Vec<DirtyBin> {
    let path = path(data_dir);
    match fs::read(&path).map(|data| serde_json::from_slice(&data)) {
        Ok(Ok(bins)) => bins,
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable dirty-bin queue");
            Vec::new()
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load dirty-bin queue");
            Vec::new()
        }
    }
}

pub fn store(data_dir: &Path, bins: &[DirtyBin]) {
    use std::io::Write;
    let path = path(data_dir);
    let result = path
        .parent()
        .map_or(Ok(()), fs::create_dir_all)
        .and_then(|()| serde_json::to_vec(bins).map_err(std::io::Error::other))
        // Reuses the WAL's tmp+rename helper, which also cleans up the tmp file on failure.
        .and_then(|bytes| crate::wal::write_atomic_with(&path, false, |f| f.write_all(&bytes)));
    if let Err(error) = result {
        warn!(%error, "failed to persist dirty-bin queue");
    }
}
