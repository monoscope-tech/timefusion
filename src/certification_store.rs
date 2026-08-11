//! Best-effort durable record of sweep certifications.
//!
//! `dedup_clean_fp` is process-local, so every restart begins with zero certified
//! partitions and the read-side dedup skip starts from cold. TF deploys several
//! times a day, which is why the skip was measured firing on 0.2–0.5% of
//! Delta-reading scans (`docs/plans/2026-08-11-certification-survival.md`).
//!
//! What is stored is exactly what `record_certification` decided — the
//! fingerprint it proved the partition clean over, never a verdict re-derived at
//! a different strictness. Nothing here can widen certification: a loaded entry
//! is subject to the same fingerprint-equality check against the live file list
//! as an in-memory one, so a stale or corrupted record can only cost a skip, not
//! grant a wrong one.

use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::warn;

/// Newest-first cap on what is written. Bounds the file for a process that has
/// certified a very large number of partitions; the tail it drops is the oldest,
/// which is also the likeliest to have been invalidated already.
pub const PERSIST_CAP: usize = 20_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredCertification {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    /// `partition_file_fp` over the file set the certifying pass proved clean.
    pub fp: u64,
    /// Wall-clock ms since the epoch at which it was granted. Wall-clock rather
    /// than a monotonic instant precisely because it has to survive the process:
    /// it exists so dwell measures a certification's real lifetime instead of
    /// restarting at every deploy.
    pub granted_unix_ms: u64,
}

/// Wall-clock ms since the epoch, now.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// How long ago `granted_unix_ms` was, or `None` if it is in the future — which
/// a backwards clock jump or a hand-edited file can produce, and which must not
/// become a nonsense dwell.
pub fn age_since(granted_unix_ms: u64) -> Option<std::time::Duration> {
    now_unix_ms().checked_sub(granted_unix_ms).map(std::time::Duration::from_millis)
}

fn path(data_dir: &Path) -> PathBuf {
    crate::wal::meta_path(data_dir, "dedup_certifications.json")
}

pub fn load(data_dir: &Path) -> Vec<StoredCertification> {
    let path = path(data_dir);
    match fs::read(&path).map(|data| serde_json::from_slice(&data)) {
        Ok(Ok(entries)) => entries,
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable certification store");
            Vec::new()
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load certification store");
            Vec::new()
        }
    }
}

pub fn store(data_dir: &Path, entries: &[StoredCertification]) {
    use std::io::Write;
    let path = path(data_dir);
    let result = path
        .parent()
        .map_or(Ok(()), fs::create_dir_all)
        .and_then(|()| serde_json::to_vec(entries).map_err(std::io::Error::other))
        // Reuses the WAL's tmp+rename helper, which also cleans up the tmp file on failure.
        .and_then(|bytes| crate::wal::write_atomic_with(&path, false, |f| f.write_all(&bytes)));
    if let Err(error) = result {
        warn!(%error, "failed to persist certification store");
    }
}
