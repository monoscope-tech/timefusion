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

// Generic over `entries` so `store` can serialize a borrowed slice without
// cloning into a `Vec` (the default, used for deserializing in `load`).
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
        Ok(Ok(snapshot)) if snapshot.version == VERSION => snapshot.entries,
        Ok(Ok(snapshot)) => {
            warn!(?path, version = snapshot.version, "discarding unsupported rollup invalidation journal");
            Vec::new()
        }
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable rollup invalidation journal");
            Vec::new()
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load rollup invalidation journal");
            Vec::new()
        }
    }
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
    let bytes = serde_json::to_vec(&Snapshot { version: VERSION, entries }).map_err(std::io::Error::other)?;
    crate::write::wal::write_atomic_with(&path, true, |file| file.write_all(&bytes))
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

    /// Every unreadable shape must load as empty — which the builder already reads
    /// as "full rebuild required" — and a pre-timestamp journal must still load,
    /// with an unknown (zero) invalidation time rather than a manufactured one.
    #[test_case(b"not json" => Vec::<RollupInvalidation>::new() ; "corrupt falls back to full rebuild semantics")]
    #[test_case(br#"{"version":2,"entries":[]}"# => Vec::<RollupInvalidation>::new() ; "unsupported version discarded")]
    #[test_case(br#"{"version":1,"entries":[{"project_id":"p","source":"s","date":"2026-08-15","epoch":7,"dirty_hours":5,"unknown":false}]}"#
        => vec![entry(0)] ; "journal written before invalidation timestamps")]
    fn loads_raw_journal(bytes: &[u8]) -> Vec<RollupInvalidation> {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = path(dir.path());
        fs::create_dir_all(target.parent().expect("metadata parent")).expect("create metadata dir");
        fs::write(&target, bytes).expect("write journal");
        load(dir.path())
    }
}
