//! Crash-safe dirty-range journal for rollup maintenance.
//!
//! This is scheduling state, not the read-side correctness boundary. Missing or
//! unreadable state deliberately loads as empty; an absent dirty entry already
//! means "full rebuild required" to the builder.

use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::warn;

const VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RollupInvalidation {
    pub project_id: String,
    pub source: String,
    pub date: String,
    pub epoch: u64,
    pub dirty_hours: u32,
    pub unknown: bool,
    /// Wall-clock time when this partition first became dirty. A zero value
    /// comes from journals written before this field existed and is treated as
    /// unknown rather than manufacturing an inaccurate age.
    #[serde(default)]
    pub invalidated_unix_ms: u64,
}

#[derive(Deserialize, Serialize)]
struct Snapshot {
    version: u32,
    entries: Vec<RollupInvalidation>,
}

fn path(data_dir: &Path) -> PathBuf {
    crate::write::wal::meta_path(data_dir, "rollup_invalidations.json")
}

pub fn load(data_dir: &Path) -> Vec<RollupInvalidation> {
    let path = path(data_dir);
    let snapshot = match fs::read(&path).map(|data| serde_json::from_slice::<Snapshot>(&data)) {
        Ok(Ok(snapshot)) if snapshot.version == VERSION => snapshot,
        Ok(Ok(snapshot)) => {
            warn!(?path, version = snapshot.version, "discarding unsupported rollup invalidation journal");
            return Vec::new();
        }
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable rollup invalidation journal");
            return Vec::new();
        }
        Err(error) if error.kind() == ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load rollup invalidation journal");
            return Vec::new();
        }
    };
    snapshot.entries
}

/// Atomically and durably replace the journal.
///
/// Invalidation callers propagate this error before acknowledging inbound
/// writes. Clearing after a target commit is best effort: failure only causes a
/// redundant rebuild after restart.
pub fn store(data_dir: &Path, entries: &[RollupInvalidation]) -> std::io::Result<()> {
    let path = path(data_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec(&Snapshot { version: VERSION, entries: entries.to_vec() }).map_err(std::io::Error::other)?;
    crate::write::wal::write_atomic_with(&path, true, |file| file.write_all(&bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_entries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let entries = vec![RollupInvalidation {
            project_id: "p".into(),
            source: "s".into(),
            date: "2026-08-15".into(),
            epoch: 7,
            dirty_hours: 5,
            unknown: false,
            invalidated_unix_ms: 123,
        }];
        store(dir.path(), &entries).expect("store journal");
        assert_eq!(load(dir.path()), entries);
    }

    #[test]
    fn corrupt_state_falls_back_to_full_rebuild_semantics() {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = path(dir.path());
        fs::create_dir_all(target.parent().expect("metadata parent")).expect("create metadata dir");
        fs::write(target, b"not json").expect("write corrupt journal");
        assert!(load(dir.path()).is_empty());
    }

    #[test]
    fn journal_written_before_invalidation_timestamps_remains_readable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = path(dir.path());
        fs::create_dir_all(target.parent().expect("metadata parent")).expect("create metadata dir");
        fs::write(target, br#"{"version":1,"entries":[{"project_id":"p","source":"s","date":"2026-08-15","epoch":2,"dirty_hours":1,"unknown":false}]}"#)
            .expect("write old journal");

        let entries = load(dir.path());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].invalidated_unix_ms, 0);
    }
}
