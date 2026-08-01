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
    time::Duration,
};

use deltalake::table::state::DeltaTableState;
use tracing::{debug, warn};

/// Bump on incompatible layout changes (ours or delta-rs's snapshot serde);
/// old files then just miss and the table does a full load.
const FORMAT_VERSION: u32 = 1;

/// Snapshot files untouched for this long belong to dropped or long-idle
/// tables (active ones rewrite theirs every flush).
pub const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

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
    let write = || -> anyhow::Result<()> {
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut enc = zstd::Encoder::new(fs::File::create(&tmp)?, 3)?;
        serde_json::to_writer(&mut enc, &(FORMAT_VERSION, table_url, state))?;
        enc.finish()?.sync_all()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    };
    match write() {
        Ok(()) => debug!("Persisted delta snapshot for {table_url} to {path:?}"),
        Err(e) => warn!("Failed to persist delta snapshot for {table_url}: {e}"),
    }
}

/// Load a previously persisted snapshot. Any failure — missing file, corrupt
/// or incompatible payload, table-url mismatch (hash collision) — returns
/// `None` and the caller performs a full load.
pub fn load(dir: &Path, table_url: &str) -> Option<DeltaTableState> {
    let path = path_for(dir, table_url);
    let reader = zstd::Decoder::new(fs::File::open(&path).ok()?).ok()?;
    match serde_json::from_reader::<_, (u32, String, DeltaTableState)>(reader) {
        Ok((FORMAT_VERSION, url, state)) if url == table_url => {
            debug!("Restored delta snapshot for {table_url} at version {}", state.version());
            Some(state)
        }
        Ok((version, url, _)) => {
            debug!("Ignoring delta snapshot {path:?}: version {version} / url {url} does not match {FORMAT_VERSION} / {table_url}");
            None
        }
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
pub fn prune_stale(dir: &Path, max_age: Duration) {
    fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.metadata().and_then(|m| m.modified()).ok().and_then(|t| t.elapsed().ok()).is_some_and(|age| age > max_age))
        .for_each(|e| {
            debug!("Pruning stale delta snapshot {:?}", e.path());
            let _ = fs::remove_file(e.path());
        });
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use deltalake::{DeltaTable, DeltaTableBuilder};
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;
    use crate::schema_loader::get_default_schema;

    fn mem_store(name: &str) -> anyhow::Result<(Arc<InMemory>, Url)> {
        Ok((Arc::new(InMemory::new()), Url::parse(&format!("memory:///{name}"))?))
    }

    fn builder(mem: &Arc<InMemory>, url: &Url) -> anyhow::Result<DeltaTableBuilder> {
        Ok(DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()))
    }

    async fn create_table(mem: &Arc<InMemory>, url: &Url) -> anyhow::Result<DeltaTable> {
        Ok(builder(mem, url)?.build()?.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?)
    }

    /// A metadata commit through the high-level ops API exercises the same
    /// `CommitBuilder` post-commit hook the flush path uses.
    async fn commit_property(table: DeltaTable) -> anyhow::Result<DeltaTable> {
        Ok(table.set_tbl_properties().with_properties(HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())])).await?)
    }

    fn materialized(table: &DeltaTable) -> bool {
        table.state.as_ref().unwrap().has_materialized_files()
    }

    /// Decisive probe: does a delta-rs commit (the `CommitBuilder` post-commit
    /// hook that produces `finalized.snapshot()` in TF's flush path) preserve
    /// the materialized file list? If a commit drops it, every subsequent
    /// post-commit `snapshot.update()` falls back to a full checkpoint replay —
    /// the 2-8s/flush prod cost — even though load/update preserve it.
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_preserves_materialized_files() -> anyhow::Result<()> {
        let (mem, url) = mem_store("commit_mat")?;
        let table = create_table(&mem, &url).await?;
        assert!(materialized(&table), "freshly created table is materialized");

        let table = commit_property(table).await?;
        assert_eq!(table.version(), Some(1));
        assert!(materialized(&table), "post-commit state must stay materialized; if false, every flush full-scans");
        Ok(())
    }

    /// Does a full `.load()` (the path TF takes when there's no usable local
    /// snapshot — e.g. a legacy on-disk snapshot that misses) come back
    /// materialized? If not, every post-commit update stays on the full-scan
    /// branch and never self-heals — the suspected prod cause. Also verifies
    /// `ensure_materialized_files` (Tier A) repairs it.
    #[tokio::test(flavor = "multi_thread")]
    async fn full_load_materialization() -> anyhow::Result<()> {
        let (mem, url) = mem_store("full_load")?;
        commit_property(create_table(&mem, &url).await?).await?;

        let mut loaded = builder(&mem, &url)?.load().await?;
        let materialized_on_load = materialized(&loaded);
        // Tier A must leave the state materialized regardless of how it loaded.
        let log_store = loaded.log_store();
        loaded.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        assert!(materialized(&loaded), "ensure_materialized_files must materialize after a full load");
        eprintln!("FULL_LOAD_MATERIALIZED_ON_LOAD={materialized_on_load}");
        Ok(())
    }

    /// Round-trip: persist a snapshot, restore it into a fresh unloaded
    /// handle, and incrementally catch up to a commit made after the persist.
    /// This is exactly the boot path — restore at version V, replay > V.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_roundtrip_and_incremental_catchup() -> anyhow::Result<()> {
        let (mem, url) = mem_store("snap_tbl")?;
        let table = create_table(&mem, &url).await?;
        assert_eq!(table.version(), Some(0));

        let dir = tempfile::tempdir()?;
        store(dir.path(), url.as_str(), table.state.as_ref().unwrap());

        // External commit after the persist — restore must catch up to it.
        commit_property(table).await?;

        let state = load(dir.path(), url.as_str()).expect("persisted snapshot loads");
        assert_eq!(state.version(), 0);
        let mut restored = builder(&mem, &url)?.build()?;
        restored.state = Some(state);
        restored.update_state().await?;
        assert_eq!(restored.version(), Some(1), "restored snapshot must incrementally reach the latest commit");

        // The fork persists the materialized file list (MaterializedFilesWire),
        // so a restored snapshot must come back materialized and STAY that way
        // across updates — otherwise post-commit updates fall back to a full
        // checkpoint replay (the 2-8s/flush prod cost). Guard the whole chain:
        // ensure (idempotent) → update (incremental) → reconcile rebuild.
        assert!(materialized(&restored), "restored snapshot must come back materialized");
        let log_store = restored.log_store();
        restored.state.as_mut().unwrap().ensure_materialized_files(log_store.as_ref()).await?;
        restored.update_state().await?;
        assert!(materialized(&restored), "materialization must survive update (stays incremental)");
        restored.state.as_mut().unwrap().rematerialize_files(log_store.as_ref()).await?;
        assert!(materialized(&restored), "rematerialize_files keeps the file list materialized");

        // Wrong table url (or hash collision) must miss, not mis-restore.
        assert!(load(dir.path(), "memory:///other_tbl").is_none());
        Ok(())
    }
}
