//! Best-effort durable metadata for sealed-bin dedup scheduling.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirtyBin {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub bin: i64,
}

fn path(data_dir: &Path) -> std::path::PathBuf {
    data_dir.join(".timefusion_meta/dedup_dirty_bins.json")
}

pub fn load(data_dir: &Path) -> Vec<DirtyBin> {
    let path = path(data_dir);
    match fs::read(&path) {
        Ok(data) => match serde_json::from_slice(&data) {
            Ok(bins) => bins,
            Err(error) => {
                warn!(?path, %error, "discarding unreadable dirty-bin queue");
                Vec::new()
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load dirty-bin queue");
            Vec::new()
        }
    }
}

pub fn store(data_dir: &Path, bins: &[DirtyBin]) {
    let path = path(data_dir);
    let result = (|| -> anyhow::Result<()> {
        fs::create_dir_all(path.parent().unwrap())?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, serde_json::to_vec(bins)?)?;
        fs::rename(tmp, path)?;
        Ok(())
    })();
    if let Err(error) = result {
        warn!(%error, "failed to persist dirty-bin queue");
    }
}
