//! Local persistence of Delta table snapshots so a restart restores the last
//! known state from disk and replays only commits made since, instead of
//! rebuilding from checkpoint + log tail on S3 (prod boot replay was the
//! dominant cold-start cost). Files live next to the WAL metadata under
//! `TIMEFUSION_DATA_DIR/.timefusion_meta/delta_snapshots/` and are
//! best-effort: any failure to write or read falls back to a full S3 load.
//!
//! Format: zstd-compressed JSON of `(FORMAT_VERSION, table_url, state)`.
//! JSON (not bincode) because delta-rs's snapshot Serialize uses
//! `serialize_seq(None)`, which non-self-describing formats reject.

use std::{
    fs,
    path::{Path, PathBuf},
};

use deltalake::table::state::DeltaTableState;
use tracing::{debug, warn};

/// Bump on incompatible layout changes (ours or delta-rs's snapshot serde);
/// old files then just miss and the table does a full load.
const FORMAT_VERSION: u32 = 1;

fn path_for(dir: &Path, table_url: &str) -> PathBuf {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    table_url.hash(&mut h);
    dir.join(format!("{:016x}.json.zst", h.finish()))
}

/// Best-effort atomic persist (tmp + rename, same pattern as the WAL cursor
/// snapshot). Failures are logged, never propagated — persistence is an
/// optimization, not a correctness requirement.
pub fn store(dir: &Path, table_url: &str, state: &DeltaTableState) {
    let path = path_for(dir, table_url);
    let res = (|| -> anyhow::Result<()> {
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut enc = zstd::Encoder::new(fs::File::create(&tmp)?, 3)?;
        serde_json::to_writer(&mut enc, &(FORMAT_VERSION, table_url, state))?;
        enc.finish()?.sync_all()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    })();
    match res {
        Ok(()) => debug!("Persisted delta snapshot for {table_url} to {path:?}"),
        Err(e) => warn!("Failed to persist delta snapshot for {table_url}: {e}"),
    }
}

/// Load a previously persisted snapshot. Any failure — missing file, corrupt
/// or incompatible payload, table-url mismatch (hash collision) — returns
/// `None` and the caller performs a full load.
pub fn load(dir: &Path, table_url: &str) -> Option<DeltaTableState> {
    let path = path_for(dir, table_url);
    let file = fs::File::open(&path).ok()?;
    let parsed: Result<(u32, String, DeltaTableState), _> = serde_json::from_reader(zstd::Decoder::new(file).ok()?);
    match parsed {
        Ok((FORMAT_VERSION, url, state)) if url == table_url => {
            debug!("Restored delta snapshot for {table_url} at version {}", state.version());
            Some(state)
        }
        Ok(_) => None,
        Err(e) => {
            warn!("Discarding unreadable delta snapshot {path:?}: {e}");
            let _ = fs::remove_file(&path);
            None
        }
    }
}

/// Remove snapshot files not refreshed within `max_age` (active tables
/// rewrite theirs every flush, so stale files belong to dropped or long-idle
/// tables). Bounds disk growth; best-effort.
pub fn prune_stale(dir: &Path, max_age: std::time::Duration) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let stale = entry.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > max_age);
        if stale {
            debug!("Pruning stale delta snapshot {:?}", entry.path());
            let _ = fs::remove_file(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use deltalake::DeltaTableBuilder;
    use url::Url;

    use super::*;
    use crate::schema_loader::get_default_schema;

    /// Decisive probe: does a delta-rs commit (the `CommitBuilder` post-commit
    /// hook that produces `finalized.snapshot()` in TF's flush path) preserve
    /// the materialized file list? If a commit drops it, every subsequent
    /// post-commit `snapshot.update()` falls back to a full checkpoint replay —
    /// the 2-8s/flush prod cost — even though load/update preserve it.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_preserves_materialized_files() -> anyhow::Result<()> {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///commit_mat")?;
        let t = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).build()?;
        let mut table = t.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;
        assert!(table.state.as_ref().unwrap().has_materialized_files(), "freshly created table is materialized");

        // A metadata commit through the high-level ops API exercises the same
        // CommitBuilder post-commit hook the flush path uses.
        table = table
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())]))
            .await?;
        assert_eq!(table.version(), Some(1));
        assert!(
            table.state.as_ref().unwrap().has_materialized_files(),
            "post-commit state must stay materialized; if false, every flush full-scans"
        );
        Ok(())
    }

    /// Does a full `.load()` (the path TF takes when there's no usable local
    /// snapshot — e.g. a legacy on-disk snapshot that misses) come back
    /// materialized? If not, every post-commit update stays on the full-scan
    /// branch and never self-heals — the suspected prod cause. Also verifies
    /// `ensure_materialized_files` (Tier A) repairs it.
    #[tokio::test(flavor = "multi_thread")]
    async fn full_load_materialization() -> anyhow::Result<()> {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///full_load")?;
        let t = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).build()?;
        let created = t.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;
        created
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())]))
            .await?;

        let mut loaded = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).load().await?;
        let materialized_on_load = loaded.state.as_ref().unwrap().has_materialized_files();
        // Tier A must leave the state materialized regardless of how it loaded.
        let log_store = loaded.log_store();
        loaded.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        assert!(
            loaded.state.as_ref().unwrap().has_materialized_files(),
            "ensure_materialized_files must materialize after a full load"
        );
        eprintln!("FULL_LOAD_MATERIALIZED_ON_LOAD={materialized_on_load}");
        Ok(())
    }

    /// Round-trip: persist a snapshot, restore it into a fresh unloaded
    /// handle, and incrementally catch up to a commit made after the persist.
    /// This is exactly the boot path — restore at version V, replay > V.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_roundtrip_and_incremental_catchup() -> anyhow::Result<()> {
        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///snap_tbl")?;
        let t = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).build()?;
        let table = t.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;
        assert_eq!(table.version(), Some(0));

        let dir = tempfile::tempdir()?;
        store(dir.path(), url.as_str(), table.state.as_ref().unwrap());

        // External commit after the persist — restore must catch up to it.
        let _v1 = table
            .set_tbl_properties()
            .with_properties(std::collections::HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())]))
            .await?;

        let state = load(dir.path(), url.as_str()).expect("persisted snapshot loads");
        assert_eq!(state.version(), 0);
        let mut restored = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem, url.clone()).build()?;
        restored.state = Some(state);
        restored.update_state().await?;
        assert_eq!(restored.version(), Some(1), "restored snapshot must incrementally reach the latest commit");

        // The fork persists the materialized file list (MaterializedFilesWire),
        // so a restored snapshot must come back materialized and STAY that way
        // across updates — otherwise post-commit updates fall back to a full
        // checkpoint replay (the 2-8s/flush prod cost). Guard the whole chain:
        // ensure (idempotent) → update (incremental) → reconcile rebuild.
        assert!(
            restored.state.as_ref().unwrap().has_materialized_files(),
            "restored snapshot must come back materialized"
        );
        let log_store = restored.log_store();
        restored.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        restored.update_state().await?;
        assert!(
            restored.state.as_ref().unwrap().has_materialized_files(),
            "materialization must survive update (stays incremental)"
        );
        let log_store = restored.log_store();
        restored.state.as_mut().unwrap().rematerialize_files(log_store.as_ref()).await?;
        assert!(
            restored.state.as_ref().unwrap().has_materialized_files(),
            "rematerialize_files keeps the file list materialized"
        );

        // Wrong table url (or hash collision) must miss, not mis-restore.
        assert!(load(dir.path(), "memory:///other_tbl").is_none());
        Ok(())
    }
}
