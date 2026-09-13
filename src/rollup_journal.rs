//! Crash-safe dirty-range journal for rollup maintenance.
//!
//! Scheduling state only. Missing or unreadable state loads as empty, which the
//! builder already reads as "full rebuild required".

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
    /// Wall-clock time when this partition first became dirty; zero means unknown.
    #[serde(default)]
    pub invalidated_unix_ms: u64,
}

// Generic over `entries` so `store` can serialize a borrowed slice without cloning.
#[derive(Deserialize, Serialize)]
struct Snapshot<E = Vec<RollupInvalidation>> {
    version: u32,
    entries: E,
}

fn path(data_dir: &Path) -> PathBuf {
    crate::write::wal::meta_path(data_dir, "rollup_invalidations.json")
}

pub fn load(data_dir: &Path) -> Vec<RollupInvalidation> {
    let path = path(data_dir);
    match fs::read(&path).map(|data| serde_json::from_slice::<Snapshot>(&data)) {
        Ok(Ok(snapshot)) if snapshot.version == VERSION => return snapshot.entries,
        Ok(Ok(snapshot)) => warn!(?path, version = snapshot.version, "discarding unsupported rollup invalidation journal"),
        Ok(Err(error)) => warn!(?path, %error, "discarding unreadable rollup invalidation journal"),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => warn!(?path, %error, "failed to load rollup invalidation journal"),
    }
    Vec::new()
}

/// Atomically and durably replace the journal.
pub fn store(data_dir: &Path, entries: &[RollupInvalidation]) -> std::io::Result<()> {
    store_encoded(data_dir, &encode(entries)?)
}

/// The exact bytes [`store_encoded`] would write, so a caller can compare them
/// against what it last persisted and skip the write.
pub fn encode(entries: &[RollupInvalidation]) -> std::io::Result<Vec<u8>> {
    serde_json::to_vec(&Snapshot { version: VERSION, entries }).map_err(std::io::Error::other)
}

pub fn store_encoded(data_dir: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let path = path(data_dir);
    path.parent().map_or(Ok(()), fs::create_dir_all)?;
    crate::write::wal::write_atomic_with(&path, true, |file| file.write_all(bytes))
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    fn entry(invalidated_unix_ms: u64) -> RollupInvalidation {
        RollupInvalidation {
            project_id: "p".into(),
            source: "s".into(),
            date: "2026-08-15".into(),
            epoch: 7,
            dirty_hours: 5,
            unknown: false,
            invalidated_unix_ms,
        }
    }

    #[test]
    fn round_trips_entries() {
        let dir = tempfile::tempdir().expect("temp dir");
        let entries = vec![entry(123)];
        store(dir.path(), &entries).expect("store journal");
        assert_eq!(load(dir.path()), entries);
    }

    /// Unreadable shapes load as empty; a journal lacking the timestamp field
    /// still loads, with a zero (unknown) invalidation time.
    #[test_case(b"not json" => Vec::<RollupInvalidation>::new() ; "corrupt falls back to full rebuild semantics")]
    #[test_case(br#"{"version":2,"entries":[]}"# => Vec::<RollupInvalidation>::new() ; "unsupported version discarded")]
    #[test_case(br#"{"version":1,"entries":[{"project_id":"p","source":"s","date":"2026-08-15","epoch":7,"dirty_hours":5,"unknown":false}]}"#
        => vec![entry(0)] ; "journal written before invalidation timestamps")]
    fn loads_raw_journal(bytes: &[u8]) -> Vec<RollupInvalidation> {
        let dir = tempfile::tempdir().expect("temp dir");
        store_encoded(dir.path(), bytes).expect("write journal");
        load(dir.path())
    }
}
