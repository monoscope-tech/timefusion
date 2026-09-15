//! Write path: insert/coalesced-commit machinery, flush-time sort, runtime envs,
//! per-table locks, staged writes, watermark reconciliation.
use super::*;

/// Shared OCC retry budget for the staged single-unit and coalesced commit loops.
const MAX_COMMIT_RETRIES: u32 = 5;

/// How many top memory-pool consumers to name when a pool is exhausted.
const TOP_POOL_CONSUMERS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(5).unwrap();

/// Acquire a per-table commit lock with flush priority: a [`flush_waiter`] is
/// registered across the WAIT only. Maintenance waves stand down while the count
/// is nonzero, so it must fall the moment the lock is held or the future is cancelled.
async fn lock_with_flush_priority<'a>(lock: &'a tokio::sync::Mutex<()>, waiters: &Arc<std::sync::atomic::AtomicUsize>) -> tokio::sync::MutexGuard<'a, ()> {
    let _waiting = flush_waiter(waiters);
    lock.lock().await
}

/// Spawn detached best-effort post-commit work that a maintenance shutdown cancels.
fn spawn_until_shutdown(shutdown: Arc<CancellationToken>, work: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = work => {}
        }
    });
}

/// Which path is driving [`Database::commit_staged_group`].
#[derive(Clone, Copy)]
enum StagedCommitKind {
    /// One project's unit. `warm` is the caller's `watermark.is_some()`: only
    /// the BufferedWriteLayer flush path warms the cache.
    Flush { warm: bool },
    /// One physical table's coalesced group — always a flush, always warms.
    Coalesced,
}

impl StagedCommitKind {
    /// `(refresh, commit)` labels for the commit-lock timeout metric; they must
    /// match the values documented in `observability.rs`.
    fn ops(self) -> (&'static str, &'static str) {
        match self {
            Self::Flush { .. } => ("flush_refresh", "flush_commit"),
            Self::Coalesced => ("coalesced_refresh", "coalesced_commit"),
        }
    }

    /// Subject of the failure messages this path returns.
    fn what(self) -> &'static str {
        match self {
            Self::Flush { .. } => "staged commit",
            Self::Coalesced => "coalesced staged commit",
        }
    }

    fn warm(self) -> bool {
        matches!(self, Self::Flush { warm: true } | Self::Coalesced)
    }
}

/// What a staged commit appends: the already-uploaded parquet's actions, the
/// target's schema (which supplies the operation's `partition_by`), and the
/// commit metadata.
struct StagedCommit<'a> {
    adds: &'a [deltalake::kernel::Action],
    schema: &'a crate::schema::TableSchema,
    properties: CommitProperties,
}

impl Database {
    /// Directory holding locally persisted Delta snapshots.
    pub(crate) fn delta_snapshot_dir(cfg: &AppConfig) -> PathBuf {
        crate::write::wal::meta_path(&cfg.core.timefusion_data_dir, "delta_snapshots")
    }

    /// Whether snapshot refreshes may take the incremental catch-up fast path.
    pub(crate) fn incremental_snapshot(&self) -> bool {
        self.config.maintenance.timefusion_incremental_snapshot
    }

    /// Let the post-commit hook advance the snapshot incrementally instead of
    /// re-materializing the whole active file set. The hook still rebuilds the
    /// kernel snapshot from the log, so schema changes are applied either way.
    fn with_incremental_advance(&self, properties: CommitProperties) -> CommitProperties {
        match self.incremental_snapshot() {
            true => properties.with_incremental_advance(true),
            false => properties,
        }
    }

    /// Returns the process-wide query memory pool and Parquet metadata cache.
    pub(crate) fn shared_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.runtime_env
            .get_or_init(|| {
                let pool_size = self.config.derived.query_pool_bytes();
                use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, TrackConsumersPool};
                let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = match self.config.memory.timefusion_memory_pool {
                    crate::config::MemoryPoolKind::Greedy => Arc::new(TrackConsumersPool::new(GreedyMemoryPool::new(pool_size), TOP_POOL_CONSUMERS)),
                    crate::config::MemoryPoolKind::FairSpill => Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_size), TOP_POOL_CONSUMERS)),
                };
                let meta_cache_bytes = self.config.cache.timefusion_df_metadata_cache_mb * 1024 * 1024;
                // Spill to the data volume: the DiskManager otherwise defaults to
                // `std::env::temp_dir()`, unbounded by our config.
                let disk = self.spill_disk("query_spill", self.config.maintenance.timefusion_query_spill_max_gb);
                Arc::new(build_query_runtime_env(pool, meta_cache_bytes, disk))
            })
            .clone()
    }

    /// Create `subdir` under the data volume, reap dirs orphaned by a previous
    /// process, then cap it — the ceiling must never be left to DataFusion's
    /// 100 GB default (see [`spill_disk_builder`]).
    fn spill_disk(&self, subdir: &str, max_gb: u64) -> datafusion::execution::disk_manager::DiskManagerBuilder {
        let dir = self.config.core.timefusion_data_dir.join(subdir);
        let _ = std::fs::create_dir_all(&dir);
        reap_orphaned_spill_dirs(&dir);
        spill_disk_builder(dir, max_gb)
    }

    /// Dedicated `RuntimeEnv` for maintenance jobs (optimize/dedup/recompress).
    ///
    /// FairSpill so each consumer can reserve its floor and spill rather than being starved;
    /// spills land under the data volume, and the bounded pool errors instead of OOM-killing.
    fn build_spill_runtime_env(&self, pool_size: usize, spill_subdir: &str) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        use datafusion::execution::{
            memory_pool::{FairSpillPool, TrackConsumersPool},
            runtime_env::RuntimeEnvBuilder,
        };
        let disk = self.spill_disk(spill_subdir, self.config.maintenance.timefusion_maintenance_spill_max_gb);
        let pool = Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_size), TOP_POOL_CONSUMERS));
        Arc::new(RuntimeEnvBuilder::new().with_memory_pool(pool).with_disk_manager_builder(disk).build().expect("build maintenance runtime env"))
    }

    /// Light-optimize slice of the maintenance budget. Deferred to the budget
    /// tree — never recompute a share locally, it drifts when the tree moves.
    pub(crate) fn light_optimize_pool_bytes(&self) -> usize {
        self.config.derived.light_share_bytes()
    }

    /// Packing's dedicated slice of the light pool, disjoint from repair's so
    /// an in-flight repair bin cannot starve packing entirely.
    pub(crate) fn pack_pool_bytes(&self) -> usize {
        (self.light_optimize_pool_bytes() / 2).max(1)
    }

    /// Repair's slice: the light share minus packing's reservation.
    pub(crate) fn repair_pool_bytes(&self) -> usize {
        self.light_optimize_pool_bytes() - self.pack_pool_bytes()
    }

    /// Heavy maintenance (dedup, recompress, Z-order). Deferred to the budget
    /// tree, not derived as "pool minus light": a residual definition silently
    /// absorbs any share the tree carves off for another consumer.
    pub(crate) fn heavy_pool_bytes(&self) -> usize {
        self.config.derived.heavy_share_bytes()
    }

    pub(crate) fn maintenance_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.maintenance_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.heavy_pool_bytes(), "maintenance_spill")).clone()
    }

    /// Hot-tail packing env, on packing's reserved slice.
    pub(crate) fn light_optimize_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.light_optimize_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.pack_pool_bytes(), "light_optimize_spill")).clone()
    }

    /// Footer repair env, on its own pool disjoint from packing's.
    pub(crate) fn repair_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.repair_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.repair_pool_bytes(), "repair_spill")).clone()
    }

    pub(crate) fn coordinator_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.coordinator_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.config.derived.coordinator_share_bytes(), "coordinator_spill")).clone()
    }

    /// Sort one flush group, picking the strategy by size.
    ///
    /// Below `timefusion_sort_skip_bytes` the in-process sort wins on latency; above it the
    /// pooled+spilling DataFusion sort is used rather than skipping, because one file without a
    /// `sorting_columns` footer disables the reader's all-or-nothing ordering for the whole
    /// partition. `fallback` decides what happens when even that fails; see [`UnsortedFallback`].
    pub(crate) async fn sort_flush_group(
        &self, schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>, fallback: UnsortedFallback,
    ) -> Result<(FlushBatches, bool)> {
        // An empty group, or a table declaring no sort order, has no footer to
        // lose: `sorted = false` there is not a degradation and must not abort.
        let nothing_to_declare = batches.is_empty() || schema.sorting_columns.is_empty();
        let ceiling = self.config.maintenance.timefusion_sort_skip_bytes;
        let total: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        if total <= ceiling || nothing_to_declare {
            // `usize::MAX`: the size decision is made here, so the in-process
            // helper must not second-guess it and silently skip.
            let (out, sorted) = sort_batches_by_schema(schema, batches, usize::MAX);
            anyhow::ensure!(
                sorted || nothing_to_declare || fallback == UnsortedFallback::Allow,
                "in-process sort degraded to unsorted on a rewrite path; keeping the committed inputs instead"
            );
            return Ok((out, sorted));
        }
        match self.sort_flush_group_spilling(schema, &batches).await {
            Some(sorted) => {
                debug!("flush sort: escalated {} MB group to the spilling DataFusion sort", total / (1 << 20));
                Ok((FlushBatches::Ready(sorted.into_iter()), true))
            }
            None => {
                // A rewrite's inputs are already committed and sorted: aborting
                // costs one compaction cycle, while writing the group unsorted
                // costs the partition's declared ordering permanently.
                anyhow::ensure!(
                    fallback == UnsortedFallback::Allow,
                    "escalated sort of a {} MB rewrite group failed; keeping the committed inputs rather than replacing them with an unsorted file",
                    total / (1 << 20)
                );
                // Ingest has no such choice — the rows exist nowhere else. Counted
                // because one unsorted file disables the reader's ordering for its
                // whole partition, which must never be silent.
                crate::observability::record_flush_sort_unsorted_fallback();
                Ok((FlushBatches::Ready(batches.into_iter()), false))
            }
        }
    }

    /// Flush-path sort pool: bounded and spillable, so an oversized bucket
    /// degrades to disk I/O instead of an unpooled allocation spike.
    fn flush_sort_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.flush_sort_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.config.maintenance.flush_sort_pool_bytes(), "flush_sort_spill")).clone()
    }

    /// Sort an oversized flush group inside a DataFusion plan, so the peak is bounded by a pool
    /// (the in-process path allocates outside every memory pool). Returns `None` on any failure
    /// so the caller writes the original batches unsorted; a flush must never lose rows to a
    /// sort failure.
    async fn sort_flush_group_spilling(&self, schema: &crate::schema::TableSchema, batches: &[RecordBatch]) -> Option<Vec<RecordBatch>> {
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        // Hold a slice of the shared spill pool for the whole sort.
        let _slice = self.flush_sort_gate.acquire().await.ok()?;
        let first = batches.first()?.schema();
        // Schema-diverse buckets (an evolved nullable column) must be unified
        // before MemTable will accept them; give up rather than guess.
        let arrow_schema = match batches.iter().all(|b| b.schema() == first) {
            true => first,
            false => Arc::new(arrow_schema::Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone())).ok()?),
        };
        let unified: Vec<RecordBatch> = batches
            .iter()
            .map(|b| match b.schema() == arrow_schema {
                true => Ok(b.clone()),
                false => deltalake::kernel::schema::cast_record_batch(b, arrow_schema.clone(), true, true),
            })
            .collect::<Result<_, _>>()
            .ok()?;

        let order_by = schema
            .sorting_columns
            .iter()
            .filter(|c| arrow_schema.index_of(&c.name).is_ok())
            .map(|c| {
                format!(
                    "{} {} NULLS {}",
                    crate::rollup::quoted(&c.name),
                    if c.descending { "DESC" } else { "ASC" },
                    if c.nulls_first { "FIRST" } else { "LAST" }
                )
            })
            .join(", ");
        if order_by.is_empty() {
            return None;
        }

        // One partition, deliberately: N>1 fans out to N ExternalSorters plus an
        // UNSPILLABLE SortPreservingMergeExec, which exhausts the pool and drops
        // to the unsorted fallback. `flush_sort_gate`'s per-permit sizing assumes
        // one pool consumer per permit. The small batch size is for the same
        // reason — `ExternalSorterMerge`'s reservation scales with `batch_size`.
        let state = build_delta_write_session_state(1, self.flush_sort_runtime_env(), "256");
        let ctx = SessionContext::new_with_state(state);
        let name = format!("flush_sort_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(&name, Arc::new(MemTable::try_new(arrow_schema, vec![unified]).ok()?)).ok()?;
        let out = ctx.sql(&format!("SELECT * FROM {name} ORDER BY {order_by}")).await.ok()?.collect().await;
        let _ = ctx.deregister_table(&name);
        out.inspect_err(|e| warn!("flush sort: spilling DataFusion sort failed, writing unsorted: {e}")).ok()
    }

    /// Heavy-maintenance session state, built once.
    pub(crate) fn maintenance_session_state(&self) -> datafusion::execution::session_state::SessionState {
        self.maintenance_session_state
            .get_or_init(|| build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()))
            .clone()
    }

    /// Packing session state, built once. Lifts `MAINTENANCE_MAX_PARTITIONS` (sized for heavy
    /// fan-out) and derives the partition count instead: each partition's reservation is
    /// unspillable, so a fixed high number wastes most of a small pool before sorting a row.
    pub(crate) fn light_optimize_session_state(&self) -> datafusion::execution::session_state::SessionState {
        self.light_optimize_session_state
            .get_or_init(|| {
                build_optimize_session_state_tuned(
                    self.config.memory.timefusion_query_partitions,
                    self.light_optimize_runtime_env(),
                    None,
                    Some(UncappedSort { partitions: self.pack_sort_partitions(), reservation_bytes: None }),
                )
            })
            .clone()
    }

    pub(crate) fn pack_sort_partitions(&self) -> usize {
        pack_sort_partitions(self.pack_pool_bytes(), self.config.derived.max_light_optimize_k(), self.config.derived.cores)
    }

    /// Repair session state, cached per parallelism. The batch size is deliberately smaller than
    /// packing's: a repair bin is one whole large file and `SortPreservingMergeExec` allocates per
    /// spill-run per batch, so its ask scales with `batch_size`. `partitions` comes from
    /// `REPAIR_SORT_PARTITION_LADDER` — a bin that exhausted the pool is retried with fewer
    /// partitions, since the unspillable merge operator is per-partition.
    pub(crate) fn repair_session_state(&self, partitions: usize) -> datafusion::execution::session_state::SessionState {
        self.repair_session_states
            .entry(partitions)
            .or_insert_with(|| {
                build_optimize_session_state_tuned(
                    self.config.memory.timefusion_query_partitions,
                    self.repair_runtime_env(),
                    Some("256"),
                    Some(UncappedSort { partitions, reservation_bytes: Some(REPAIR_SORT_RESERVATION_BYTES) }),
                )
            })
            .clone()
    }

    /// Physical Delta log lock key: all default projects sharing a unified table collapse onto one
    /// key (an empty project_id is not valid, so it cannot collide), while custom-storage tables
    /// keep per-project isolation. Shared by `dml_lock` and `commit_lock`.
    pub(crate) async fn table_lock_key(&self, project_id: &str, table_name: &str) -> (String, String) {
        let project_key = if self.has_custom_storage(project_id, table_name).await { project_id.to_string() } else { String::new() };
        (project_key, table_name.to_string())
    }

    pub(crate) async fn dml_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.dml_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Per-physical-table Delta commit lock.
    pub(crate) async fn commit_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.commit_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Waiter count keyed identically to [`Self::commit_lock`]. Flush/ingest commit paths register
    /// a [`flush_waiter`] across their `lock().await`; `commit_wave` stands down while it is nonzero.
    pub(crate) async fn flush_waiters(&self, project_id: &str, table_name: &str) -> Arc<std::sync::atomic::AtomicUsize> {
        self.flush_waiter_counts.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// [`Self::commit_lock`] and [`Self::flush_waiters`] together, resolving
    /// `table_lock_key` (a `has_custom_storage` await) once instead of twice.
    async fn commit_lock_and_waiters(&self, project_id: &str, table_name: &str) -> (Arc<tokio::sync::Mutex<()>>, Arc<std::sync::atomic::AtomicUsize>) {
        let key = self.table_lock_key(project_id, table_name).await;
        (self.commit_locks.entry(key.clone()).or_default().clone(), self.flush_waiter_counts.entry(key).or_default().clone())
    }

    /// Persist `table`'s post-commit snapshot locally (detached) so the next
    /// boot restores it and replays only later commits.
    pub(crate) fn persist_snapshot(&self, table: &DeltaTable) {
        // At most one persist per table per interval. The snapshot is a boot-recovery
        // seed, not a durability requirement, so a skipped persist only costs a few
        // extra replayed commits on the next boot.
        const MIN_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let url = table.table_url().to_string();
        let now = std::time::Instant::now();
        if self.snapshot_persist_gate.get(&url).is_some_and(|last| now.duration_since(*last) < MIN_PERSIST_INTERVAL) {
            return;
        }
        if let Some(state) = table.state.clone() {
            self.snapshot_persist_gate.insert(url.clone(), now);
            let dir = Self::delta_snapshot_dir(&self.config);
            tokio::task::spawn_blocking(move || crate::storage::store_snapshot(&dir, &url, &state));
        }
    }

    /// Materialize a table snapshot's active file list in memory. `reconcile`
    /// rebuilds it from object-store truth; otherwise it materializes once if
    /// not already done. No-op when the table carries no state.
    async fn materialize_snapshot_files(table: &mut DeltaTable, reconcile: bool) -> Result<()> {
        let log_store = table.log_store();
        match table.state.as_mut() {
            Some(state) if reconcile => state.rematerialize_files(log_store.as_ref()).await.map_err(Into::into),
            Some(state) => state.ensure_materialized_files(log_store.as_ref()).await.map_err(Into::into),
            None => Ok(()),
        }
    }

    /// Creates or loads a DeltaTable with proper configuration. Prefers the
    /// locally persisted snapshot (restore at version V + incremental replay
    /// of commits > V) over a full checkpoint + log-tail rebuild from S3;
    /// falls back to the full load on any restore failure.
    pub(crate) async fn create_or_load_delta_table(
        &self, storage_uri: &str, storage_options: HashMap<String, String>, cached_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<DeltaTable> {
        let url = Url::parse(storage_uri)?;
        let builder = || -> Result<DeltaTableBuilder> {
            Ok(DeltaTableBuilder::from_url(url.clone())?
                .with_storage_backend(cached_store.clone(), url.clone())
                .with_storage_options(storage_options.clone())
                .with_allow_http(true))
        };
        // `spawn_blocking`: this zstd-decodes and deserializes a whole
        // `DeltaTableState`, seconds of CPU on large tables, and boot preload runs
        // many concurrently on the coordinator's runtime.
        let (snapshot_dir, url_owned) = (Self::delta_snapshot_dir(&self.config), storage_uri.to_string());
        let loaded = tokio::task::spawn_blocking(move || crate::storage::load_snapshot(&snapshot_dir, &url_owned)).await.unwrap_or(None);
        let restored = match loaded {
            Some(state) => {
                let restored_version = state.version();
                let mut table = builder()?.build()?;
                table.state = Some(state);
                // `update_state()` only probes versions *after* the supplied state, so it
                // returns Ok even for a snapshot ahead of the durable log whose own commit
                // disappeared — such a zombie serves removed files and fails every later
                // commit with InvalidTableVersion. Require its anchor commit to exist.
                match table.log_store().read_commit_entry(restored_version).await {
                    Ok(Some(_)) => table
                        .update_state()
                        .await
                        .inspect_err(|e| warn!("Local snapshot catch-up failed for '{storage_uri}': {e}; falling back to full load"))
                        .ok()
                        .map(|()| {
                            info!("Restored '{storage_uri}' from local snapshot at v{restored_version}, caught up to {:?}", table.version());
                            table
                        }),
                    Ok(None) => {
                        warn!("Local snapshot anchor v{restored_version} is absent for '{storage_uri}'; falling back to durable checkpoint/log load");
                        None
                    }
                    Err(e) => {
                        warn!(
                            "Could not validate local snapshot anchor v{restored_version} for '{storage_uri}': {e}; falling back to durable checkpoint/log load"
                        );
                        None
                    }
                }
            }
            None => None,
        };
        let mut table = match restored {
            Some(t) => t,
            None => builder()?.load().await.map_err(|e| anyhow::anyhow!("Failed to load table: {}", e))?,
        };
        // Correctness, not just perf: with incremental snapshots on, a
        // non-materialized snapshot enumerates an EMPTY file set that the
        // fast-advance post-commit hook would then build on. Fail loud rather
        // than cache a handle serving empty results.
        if self.incremental_snapshot() {
            Self::materialize_snapshot_files(&mut table, false)
                .await
                .map_err(|e| anyhow::anyhow!("Materializing file list for '{storage_uri}' failed: {e}"))?;
        }
        Ok(table)
    }

    /// Casts each of `batches` to `writer`'s table schema (`RecordBatchWriter`, unlike
    /// `WriteBuilder`, does not cast for us) and streams them in, flushing at `max_file_bytes`
    /// so one oversized bucket doesn't land as a single file. On a sorted stream each flushed
    /// piece keeps its own footer and stays time-disjoint.
    async fn stage_batches(
        writer: &mut deltalake::writer::RecordBatchWriter, batches: FlushBatches, max_file_bytes: usize,
    ) -> Result<Vec<deltalake::kernel::Action>, deltalake::DeltaTableError> {
        use deltalake::writer::DeltaWriter;
        let target_schema = writer.arrow_schema();
        let mut staged = Vec::new();
        for b in batches {
            let casted = deltalake::kernel::schema::cast_record_batch(&b?, target_schema.clone(), true, true)?;
            writer.write(casted).await?;
            if writer.buffer_len() >= max_file_bytes {
                staged.extend(writer.flush().await?);
            }
        }
        staged.extend(writer.flush().await?);
        Ok(staged.into_iter().map(deltalake::kernel::Action::Add).collect())
    }

    /// Everything a staged (lock-free parquet upload) Delta write needs, built
    /// once per (project, table) unit.
    ///
    /// `staged_writer` is `None` when the fast path is unavailable — a batch carries a column
    /// the table schema lacks (delta-rs' Default-mode `RecordBatchWriter` cannot evolve schema
    /// on a partitioned table), or the writer could not be built. That unit must take the
    /// locked WriteBuilder merge path.
    async fn prepare_staged_write(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Result<PreparedWrite> {
        // Delta-kernel's `unshredded_variant()` expects Struct{Binary,Binary} on write,
        // but MemBuffer carries Struct{BinaryView,BinaryView}.
        let batches: Vec<RecordBatch> = batches.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;

        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let schema = schema_or_default(table_name);

        let dirty_bins: Vec<(String, i64)> = if schema.dedup_keys.is_empty() {
            Vec::new()
        } else {
            // Dirty-bin granularity is intentionally independent of MemBuffer's bucket duration.
            use crate::database::compact::bin_micros;
            batches
                .iter()
                .filter_map(|batch| batch.column_by_name("timestamp")?.as_any().downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>())
                .flat_map(|timestamps| {
                    timestamps.iter().flatten().filter_map(|timestamp| {
                        chrono::DateTime::from_timestamp_micros(timestamp).map(|time| (time.date_naive().to_string(), timestamp.div_euclid(bin_micros())))
                    })
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };

        // Cluster by the declared sort keys so the parquet SortingColumn footer is
        // honest; declare it only when `sorted`. Ingest rows are committed nowhere
        // else, so an unsorted write beats losing them (`Allow` never errors).
        let (batches, sorted) = self.sort_flush_group(schema, batches, UnsortedFallback::Allow).await?;
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);

        let staging_table = { table_ref.read().await.clone() };
        let stage_store = staging_table.log_store().object_store(None);
        let staged_writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)
            .inspect_err(|e| debug!("RecordBatchWriter::for_table failed, using merge path: {}", e))
            .ok()
            .map(|w| w.with_writer_properties(writer_properties.clone()))
            .filter(|w| {
                let arrow_schema = w.arrow_schema();
                let table_fields: HashSet<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
                !batches.schemas().iter().any(|s| s.fields().iter().any(|f| !table_fields.contains(f.name().as_str())))
            });
        Ok(PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer, sorted })
    }

    /// Identity of one flush unit's batch set; `None` when the replay-decline is
    /// off for it (kill-switch, or a table with no identity).
    fn landed_digest_for(&self, table_name: &str, batches: &[RecordBatch]) -> Option<crate::write::LandedDigest> {
        let gated = self.config.buffer.landed_skip_enabled() && crate::write::landed_identity_applies(table_name);
        gated.then(|| crate::write::landed_digest(batches)).flatten()
    }

    /// Insert batches and return the URIs of files newly added by this commit
    /// (empty for the buffered-layer / batch-queue paths, where the actual Delta
    /// write happens later).
    #[instrument(
        name = "delta.insert_batch",
        skip_all,
        fields(
            table.name = %table_name,
            project_id = %project_id,
            batches.count = batches.len(),
            rows.count = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            use_queue = Empty,
        )
    )]
    pub async fn insert_records_batch(
        &self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool, watermark: Option<&crate::write::DeltaWatermark>,
    ) -> Result<Vec<String>> {
        self.insert_records_batch_bounded(project_id, table_name, batches, skip_queue, watermark, true).await
    }

    /// `bound: false` is for DML re-appends only — see
    /// [`crate::write::BufferedWriteLayer::insert_bounded`].
    pub async fn insert_records_batch_bounded(
        &self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool, watermark: Option<&crate::write::DeltaWatermark>, bound: bool,
    ) -> Result<Vec<String>> {
        let span = tracing::Span::current();
        // Delta-rs' Arrow→Delta schema conversion only accepts the IANA `"UTC"`
        // form, not a `+00:00` offset.
        let batches: Vec<RecordBatch> =
            batches.into_iter().map(normalize_timestamp_tz).map(|batch| batch.and_then(derive_date_partition)).collect::<DFResult<_>>()?;

        // Extract project_id from the first batch if not provided; bucket under
        // "default" (loudly) when neither the caller nor the data carries one.
        let project_id = match (project_id, batches.first()) {
            ("", Some(first)) => extract_project_id(first).unwrap_or_else(|| {
                warn!("insert_records_batch: empty project_id and batch has no project_id column → bucketing under 'default'");
                "default".to_string()
            }),
            ("", None) => {
                warn!("insert_records_batch: empty project_id and no batches → bucketing under 'default'");
                "default".to_string()
            }
            _ => project_id.to_string(),
        };

        let table_name = if table_name.is_empty() { "otel_logs_and_spans" } else { table_name }.to_string();

        if watermark.is_none() {
            self.invalidate_rollup_batches(&project_id, &table_name, &batches)?;
        }

        // Stamp the schema's TF-owned version column: this is the single funnel every
        // *inbound* write passes through, and it runs before the WAL append so the
        // durable record carries the value. A `watermark` marks the one non-inbound
        // caller — a flush of already-stamped buffered rows — which must keep the
        // original value, or a crash-retried flush would disagree with the WAL.
        let batches = if watermark.is_none() { crate::write::stamp_version(&table_name, batches) } else { batches };

        // Buffered layer (WAL → MemBuffer): nothing is written synchronously, so an
        // empty URI list is correct.
        if !skip_queue && let Some(layer) = self.buffered_layer() {
            span.record("use_queue", "buffered_layer");
            layer.insert_bounded(&project_id, &table_name, batches, bound).await?;
            return Ok(Vec::new());
        }

        if !skip_queue
            && self.config.core.enable_batch_queue
            && let Some(ref queue) = self.batch_queue
        {
            span.record("use_queue", true);
            batches.into_iter().try_for_each(|batch| queue.queue(batch).map_err(|e| anyhow::anyhow!("Queue error: {}", e)))?;
            return Ok(Vec::new());
        }

        span.record("use_queue", false);

        // Identity of the batch set this commit carries, so a later boot can decline
        // to re-write it. Must be computed on the batches AS THE FLUSH HANDED THEM
        // OVER — before `prepare_staged_write` coerces or sorts — because that is
        // what the flush side hashes when it checks.
        let landed = watermark.is_some().then(|| self.landed_digest_for(&table_name, &batches)).flatten();

        let PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer, sorted } =
            self.prepare_staged_write(&project_id, &table_name, batches).await?;

        // Base properties (hooks off) when there is no watermark: leaving this unset
        // lets WriteBuilder's own default re-enable the checkpoint hook.
        let commit_properties = self.with_incremental_advance(watermark.map_or_else(base_commit_properties, |w| {
            build_watermark_commit_properties(
                [(project_id.clone(), table_name.clone(), w.clone())],
                landed.map(|d| (project_id.clone(), table_name.clone(), d)),
            )
        }));
        // STAGED COMMIT (fast path): encode parquet + upload to S3 OUTSIDE the per-table
        // commit lock, then serialize only the tiny commit-log append. OCC conflicts
        // re-commit the already-uploaded parquet with no re-encode/re-upload. When a batch
        // carries a column absent from the table schema there is no staged writer, and the
        // locked WriteBuilder merge path below runs instead.
        if let Some(mut writer) = staged_writer {
            let stage_span = tracing::trace_span!(parent: &span, "delta.stage_parquet");
            let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
            let adds = Self::stage_batches(&mut writer, batches, max_file_bytes)
                .instrument(stage_span)
                .await
                .map_err(|e| anyhow::anyhow!("staged parquet flush failed: {}", e))?;
            if adds.is_empty() {
                return Ok(Vec::new());
            }

            return match self
                .commit_staged_group(
                    StagedCommitKind::Flush { warm: watermark.is_some() },
                    &table_ref,
                    &[(project_id.as_str(), dirty_bins.as_slice())],
                    &table_name,
                    StagedCommit { adds: &adds, schema, properties: commit_properties },
                )
                .await
            {
                // Only AFTER the commit lands: a path marked sorted that never
                // committed would be a permanent lie.
                Ok(committed) => {
                    self.mark_written_sorted(schema, sorted, &adds);
                    Ok(committed)
                }
                Err(e) => {
                    if !InconclusiveCommit::marks(&e) {
                        Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                    }
                    Err(e)
                }
            };
        }

        // SCHEMA-EVOLUTION FALLBACK: locked WriteBuilder merge path, holding the commit
        // lock across the whole write so the schema-metadata merge cannot race a
        // concurrent commit. WriteBuilder re-submits the same rows on every OCC retry,
        // so the lazy sort-merge must be materialized once here.
        let batches: Vec<RecordBatch> = batches.collect::<Result<_, _>>()?;
        let (commit_lock, flush_waiters) = self.commit_lock_and_waiters(&project_id, &table_name).await;
        let mut last_error = None;
        for attempt in 1..=MAX_COMMIT_RETRIES {
            if let Err(e) = refresh_table_snapshot(&table_ref, self.incremental_snapshot()).await {
                debug!("Failed to update table state before write (attempt {}): {}", attempt, e);
            }
            let commit_guard = lock_with_flush_priority(&commit_lock, &flush_waiters).await;
            let table = { table_ref.read().await.clone() };
            let pre_uris: HashSet<String> = file_uris(&table);

            let write_span = tracing::trace_span!(parent: &span, "delta.write_operation", retry_attempt = attempt);
            let write_result = async {
                table
                    .clone()
                    .write(batches.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
                    .with_commit_properties(commit_properties.clone())
                    .await
            }
            .instrument(write_span)
            .await;

            match write_result {
                Ok(new_table) => {
                    let added = self
                        .record_committed_write(
                            &table_ref,
                            &[(project_id.as_str(), dirty_bins.as_slice())],
                            &table_name,
                            new_table,
                            &pre_uris,
                            watermark.is_some(),
                        )
                        .await;
                    return Ok(added);
                }
                Err(e) => {
                    if is_occ_conflict_err(&e.to_string()) {
                        last_error = Some(e);
                        debug!("Delta write conflict detected, retrying... (attempt {}/{})", attempt, MAX_COMMIT_RETRIES);
                        // Release the commit lock BEFORE the backoff sleep: holding it
                        // across the sleep serializes every other writer behind us.
                        drop(commit_guard);
                        tokio::time::sleep(occ_backoff(attempt as usize)).await;
                        drop(table); // stale clone — the retry re-clones after the reload
                        if let Err(reload_err) = refresh_table_snapshot(&table_ref, self.incremental_snapshot()).await {
                            debug!("Failed to reload table state after conflict: {}", reload_err);
                        }
                    } else {
                        return Err(anyhow::anyhow!("Delta write failed: {}", e));
                    }
                }
            }
        }

        let last_error = last_error.map_or_else(|| "Unknown error".to_string(), |e| e.to_string());
        Err(anyhow::anyhow!("Delta write failed after {} retries: {}", MAX_COMMIT_RETRIES, last_error))
    }

    /// Cross-project flush commit coalescing: one tick's per-project flush units become a single
    /// Delta commit per physical table. Parquet encode/upload still run in parallel outside the
    /// lock; only the commit-log append is shared. A staging failure excludes only that unit; a
    /// shared commit failure fails every unit in the physical group; schema-evolving units are
    /// committed alone. Returns one result per input unit, in input order.
    pub async fn insert_records_batches_coalesced(&self, units: Vec<CoalescedWriteUnit>) -> Vec<Result<Vec<String>>> {
        use deltalake::kernel::Action;
        use futures::stream::{self, StreamExt};
        let parallelism = self.config.buffer.flush_parallelism();
        let mut results: Vec<Result<Vec<String>>> = units.iter().map(|_| Ok(Vec::new())).collect();
        // Shared by every phase's futures; all three streams are collected in this
        // scope, so a borrow suffices — nothing here is spawned.
        let units = &units;

        // ---- Phase 1: prepare (bounded-concurrent; table resolution + casts).
        let prepared: Vec<(usize, Result<PreparedForPhysicalTable>)> = stream::iter(0..units.len())
            .map(move |i| async move {
                let u = &units[i];
                let prep = self.prepare_staged_write(&u.project_id, &u.table_name, u.batches.clone()).await;
                let key = self.table_lock_key(&u.project_id, &u.table_name).await;
                (i, prep.map(|p| (p, key)))
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // ---- Phase 2: stage parquet OUTSIDE any lock, `flush_parallelism`-wide.
        // Schema-evolution units never reach here: they are split out to the solo
        // (locked WriteBuilder) path so one project's merge can't stall the rest.
        let mut solo: Vec<usize> = Vec::new();
        let mut stageable: Vec<(usize, PreparedWrite, deltalake::writer::RecordBatchWriter, (String, String))> = Vec::new();
        for (i, prep) in prepared {
            match prep {
                Err(e) => results[i] = Err(e),
                Ok((mut p, key)) => match p.staged_writer.take() {
                    Some(writer) => stageable.push((i, p, writer, key)),
                    None => {
                        debug!("coalesced flush: {}/{} needs schema evolution — splitting out of the shared commit", units[i].project_id, units[i].table_name);
                        solo.push(i);
                    }
                },
            }
        }

        let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
        let staged: Vec<(usize, (String, String), Result<StagedUnit>)> = stream::iter(stageable)
            .map(|(i, prep, mut writer, key)| async move {
                let PreparedWrite { table_ref, schema, dirty_bins, batches, stage_store, sorted, .. } = prep;
                let adds: Result<Vec<Action>> =
                    Self::stage_batches(&mut writer, batches, max_file_bytes).await.map_err(|e| anyhow::anyhow!("staged parquet flush failed: {}", e));
                (i, key, adds.map(|adds| StagedUnit { table_ref, schema, dirty_bins, adds, stage_store, sorted }))
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // ---- Phase 3: one commit per PHYSICAL table.
        let mut by_physical: HashMap<(String, String), Vec<(usize, StagedUnit)>> = HashMap::new();
        for (i, key, unit) in staged {
            match unit {
                Err(e) => results[i] = Err(e),
                // Nothing was written (all rows filtered out) — no Add to commit.
                Ok(u) if u.adds.is_empty() => results[i] = Ok(Vec::new()),
                Ok(u) => by_physical.entry(key).or_default().push((i, u)),
            }
        }

        let committed: Vec<Vec<(usize, Result<Vec<String>>)>> = stream::iter(by_physical.into_values())
            .map(move |group| async move {
                let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
                let table_name = units[indices[0]].table_name.clone();
                let projects: Vec<&str> = indices.iter().map(|i| units[*i].project_id.as_str()).collect();
                let table_ref = group[0].1.table_ref.clone();
                let adds: Vec<Action> = group.iter().flat_map(|(_, u)| u.adds.iter().cloned()).collect();
                let watermarks = indices.iter().map(|i| (units[*i].project_id.clone(), units[*i].table_name.clone(), units[*i].watermark.clone()));
                // Per-unit landed identity, scoped to its own topic, so one
                // tenant's identity can never decline another's write.
                let digests: Vec<(String, String, crate::write::LandedDigest)> = indices
                    .iter()
                    .map(|i| &units[*i])
                    .filter_map(|u| self.landed_digest_for(&u.table_name, &u.batches).map(|d| (u.project_id.clone(), u.table_name.clone(), d)))
                    .collect();
                let commit_properties = self.with_incremental_advance(build_watermark_commit_properties(watermarks, digests));
                let per_project: Vec<(&str, &[(String, i64)])> = group.iter().map(|(i, u)| (units[*i].project_id.as_str(), u.dirty_bins.as_slice())).collect();
                let commit = StagedCommit { adds: &adds, schema: group[0].1.schema, properties: commit_properties };
                let outcome = self
                    .commit_staged_group(StagedCommitKind::Coalesced, &table_ref, &per_project, &table_name, commit)
                    .await
                    .map(|added| attribute_added_files(added, &projects));
                match outcome {
                    Ok(per_project_added) => {
                        // Per unit, not per group: one unit degrading to an
                        // unsorted write must not tar or exonerate its neighbours.
                        for (_, unit) in &group {
                            self.mark_written_sorted(unit.schema, unit.sorted, &unit.adds);
                        }
                        indices.into_iter().zip(per_project_added).map(|(i, a)| (i, Ok(a))).collect::<Vec<_>>()
                    }
                    Err(e) => {
                        // Fail EVERY project in the group identically — no partial
                        // settle; the caller requeues each one's buckets.
                        if !InconclusiveCommit::marks(&e) {
                            // Every unit in a physical group stages into the same
                            // store, so one store deletes all.
                            Self::cleanup_orphaned_parquet(&group[0].1.stage_store, &adds).await;
                        }
                        indices.into_iter().map(|i| (i, Err(anyhow::anyhow!("coalesced commit failed for {}: {}", table_name, e)))).collect()
                    }
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        // ---- Phase 4: schema-evolution units, each on its own (locked merge path).
        let solo_results: Vec<(usize, Result<Vec<String>>)> = stream::iter(solo)
            .map(move |i| async move {
                let u = &units[i];
                (i, self.insert_records_batch(&u.project_id, &u.table_name, u.batches.clone(), true, Some(&u.watermark)).await)
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        // Phase-3 and phase-4 outcomes cover disjoint units, so order is irrelevant.
        for (i, r) in committed.into_iter().flatten().chain(solo_results) {
            results[i] = r;
        }
        results
    }

    /// The shared commit-log append for a staged (parquet already uploaded) write: one project's
    /// flush unit, or one physical table's coalesced group spanning several projects.
    ///
    /// Staged parquet is the CALLER's to clean up: on `Err`, delete it unless the error carries
    /// [`InconclusiveCommit`], where landing could not be confirmed and deleting would risk a
    /// dangling Add.
    async fn commit_staged_group(
        &self, kind: StagedCommitKind, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, commit: StagedCommit<'_>,
    ) -> Result<Vec<String>> {
        use deltalake::kernel::transaction::TableReference;
        let StagedCommit { adds, schema, properties } = commit;
        let op = deltalake::protocol::DeltaOperation::Write {
            mode: deltalake::protocol::SaveMode::Append,
            partition_by: (!schema.partitions.is_empty()).then(|| schema.partitions.clone()),
            predicate: None,
        };
        // Any member resolves to the same physical lock (a coalesced group's key IS
        // `table_lock_key`), so serialization is identical on both paths.
        let (commit_lock, flush_waiters) = self.commit_lock_and_waiters(projects[0].0, table_name).await;
        let (refresh_op, commit_op) = kind.ops();
        let mut retry_count = 0u32;
        loop {
            // Refresh UNDER the lock: the per-table commit lock serializes all in-process
            // commits to this log, so refreshing here guarantees we build on the previous
            // committer's version and never self-conflict.
            let commit_guard = lock_with_flush_priority(&commit_lock, &flush_waiters).await;
            let t_refresh = std::time::Instant::now();
            if let Err(e) =
                bounded_commit_await(COMMIT_LOCK_OP_TIMEOUT, refresh_op, table_name, refresh_table_snapshot(table_ref, self.incremental_snapshot())).await
            {
                debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e.message);
            }
            let refresh_ms = t_refresh.elapsed().as_millis();
            let mut new_table = { table_ref.read().await.clone() };
            let t_build = std::time::Instant::now();
            // Bounded: this await holds the per-table commit lock every other
            // committer queues on.
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                commit_op,
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(properties.clone()).with_actions(adds.to_vec()).build(
                    Some(new_table.snapshot()? as &dyn TableReference),
                    new_table.log_store(),
                    op.clone(),
                ),
            )
            .await;
            let build_ms = t_build.elapsed().as_millis();
            match commit_res {
                Ok(finalized) => {
                    // Capture pre-commit URIs before the state swap below makes
                    // `new_table` post-commit; only on success, so failed attempts
                    // don't pay the full-table file-URI walk.
                    let pre_uris: HashSet<String> = file_uris(&new_table);
                    new_table.state = Some(finalized.snapshot());
                    drop(commit_guard);
                    let t_record = std::time::Instant::now();
                    let added = self.record_committed_write(table_ref, projects, table_name, new_table, &pre_uris, kind.warm()).await;
                    match kind {
                        StagedCommitKind::Flush { .. } => info!(
                            "commit_timing project={} table={} refresh_ms={} build_ms={} record_ms={} files={}",
                            projects[0].0,
                            table_name,
                            refresh_ms,
                            build_ms,
                            t_record.elapsed().as_millis(),
                            adds.len()
                        ),
                        StagedCommitKind::Coalesced => {
                            debug!("coalesced commit landed: table={} projects={} files={}", table_name, projects.len(), adds.len())
                        }
                    }
                    return Ok(added);
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    drop(commit_guard);
                    if !timed_out && is_occ_conflict_err(&e) {
                        retry_count += 1;
                        if retry_count >= MAX_COMMIT_RETRIES {
                            return Err(anyhow::anyhow!("{} failed after {} retries: {}", kind.what(), MAX_COMMIT_RETRIES, e));
                        }
                        debug!("{} conflict, retrying ({}/{}): {}", kind.what(), retry_count, MAX_COMMIT_RETRIES, e);
                        tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                        continue;
                    }
                    // Non-OCC error: the commit MAY have landed (the post-commit hook
                    // can fail after N.json is written), so probe before letting the
                    // caller delete parquet a landed commit references.
                    let pre_uris: HashSet<String> = file_uris(&new_table);
                    let (subject, draining, unconfirmed) = match kind {
                        StagedCommitKind::Flush { .. } => (
                            format!("staged commit for {}/{}", projects[0].0, table_name),
                            "draining bucket",
                            "UNCONFIRMED (snapshot read failed) — leaving staged parquet in place to avoid a dangling Add",
                        ),
                        StagedCommitKind::Coalesced => {
                            (format!("coalesced commit for {table_name}"), "draining", "UNCONFIRMED — leaving staged parquet in place")
                        }
                    };
                    return match probe_after_timeout(self.probe_commit_landed_bounded(table_ref, adds).await, timed_out) {
                        CommitProbe::Landed => {
                            warn!("{subject} reported an error but LANDED (post-commit hook failed) — {draining}: {e}");
                            let post = { table_ref.read().await.clone() };
                            Ok(self.record_committed_write(table_ref, projects, table_name, post, &pre_uris, kind.warm()).await)
                        }
                        CommitProbe::NotLanded => Err(anyhow::anyhow!("{} failed: {}", kind.what(), e)),
                        CommitProbe::Inconclusive => {
                            warn!("{subject} errored and landing is {unconfirmed}: {e}");
                            // Marker error: tells the caller not to delete the parquet.
                            Err(anyhow::Error::new(InconclusiveCommit).context(format!("{} failed (landing unconfirmed): {}", kind.what(), e)))
                        }
                    };
                }
            }
        }
    }

    /// Probe whether a staged commit landed despite returning an error: refresh the snapshot and
    /// check every Add is active. `NotLanded` is the only verdict that permits deleting staged
    /// parquet; `Inconclusive` (refresh failed, partial visibility, or probe timeout) preserves it,
    /// because a matching path with a different DV is still a live physical object.
    pub(crate) async fn probe_commit_landed_bounded(&self, table_ref: &Arc<RwLock<DeltaTable>>, adds: &[deltalake::kernel::Action]) -> CommitProbe {
        match tokio::time::timeout(COMMIT_LOCK_OP_TIMEOUT, self.probe_commit_landed(table_ref, adds)).await {
            Ok(probe) => probe,
            Err(_) => {
                crate::observability::record_commit_timeout("landing_probe");
                CommitProbe::Inconclusive
            }
        }
    }

    pub(crate) async fn probe_commit_landed(&self, table_ref: &Arc<RwLock<DeltaTable>>, adds: &[deltalake::kernel::Action]) -> CommitProbe {
        if refresh_table_snapshot(table_ref, self.incremental_snapshot()).await.is_err() {
            return CommitProbe::Inconclusive;
        }
        let guard = table_ref.read().await;
        let Ok(snap) = guard.snapshot() else {
            return CommitProbe::Inconclusive;
        };
        let active = ActiveFiles::from_snapshot(snap);
        if active.adds_live(adds) {
            CommitProbe::Landed
        } else if adds.iter().any(|action| matches!(action, deltalake::kernel::Action::Add(add) if active.0.contains_key(&add.path))) {
            // A partial landing or a later DV update cannot authorize deleting
            // still-referenced parquet, even though the exact Adds differ.
            CommitProbe::Inconclusive
        } else {
            CommitProbe::NotLanded
        }
    }

    /// Best-effort delete of staged-but-uncommitted parquet after a terminal staged-commit
    /// failure. Those objects have no Add/Remove action in the Delta log, so VACUUM never
    /// reclaims them; any path that could not be removed is logged for manual cleanup.
    pub(crate) async fn cleanup_orphaned_parquet(store: &Arc<dyn object_store::ObjectStore>, adds: &[deltalake::kernel::Action]) {
        use object_store::ObjectStoreExt; // dyn-safe `delete` wrapper
        for action in adds {
            if let deltalake::kernel::Action::Add(add) = action
                && let Err(e) = store.delete(&object_store::path::Path::from(add.path.as_str())).await
            {
                warn!("orphaned staged parquet (manual cleanup needed): {} — delete failed: {}", add.path, e);
            }
        }
    }

    /// Shared post-commit bookkeeping for staged and merge write paths: records the version for
    /// read-after-write, swaps the shared handle (version-guarded), warms just-written files,
    /// invalidates stats, and returns the added file URIs. `projects` is every
    /// `(project_id, dirty_bins)` the commit carried.
    #[allow(clippy::too_many_arguments)]
    async fn record_committed_write(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, new_table: DeltaTable, pre_uris: &HashSet<String>,
        warm: bool,
    ) -> Vec<String> {
        // Jitter anchor + logging identity: any member of the physical group is
        // equivalent, since they all commit to the same log.
        let project_id = projects.first().map(|(p, _)| *p).unwrap_or("");
        let committed_version = new_table.version();
        if let Some(version) = committed_version {
            self.last_written_versions.write().await.extend(projects.iter().map(|(project, _)| (table_key(project, table_name), version)));
            debug!("Stored last written version for {}/{} (+{} coalesced): {}", project_id, table_name, projects.len().saturating_sub(1), version);
        } else {
            debug!("WARNING: No version available after write for {}/{}", project_id, table_name);
        }
        let added: Vec<String> = new_table.get_file_uris().map(|it| it.filter(|u| !pre_uris.contains(u)).collect()).unwrap_or_default();
        // Capture the store off the committed handle so the warm task never
        // re-resolves the table.
        let (warm_store, warm_table_uri) = (new_table.log_store().object_store(None), new_table.table_url().to_string());
        self.persist_snapshot(&new_table);
        // Brief write lock for the swap only. Version-guarded: a concurrent
        // maintenance commit may have advanced the shared handle past ours.
        {
            let mut shared = table_ref.write().await;
            if new_table.version() > shared.version() {
                *shared = new_table;
            }
        }
        // Warm freshly-flushed files, which are queried next. Gated on `warm` (only the
        // BufferedWriteLayer flush path sets it): direct inserts from tests and tools must
        // not spawn detached warm tasks whose in-flight connections outlive a short-lived
        // runtime and poison the shared client pool.
        if warm {
            // The MemBuffer prefix drains right after this returns, so confirm the new
            // files are cached BEFORE that handoff. Bounded and best-effort — it can
            // never fail the commit.
            let warm_added = added.clone();
            if self.object_store_cache.is_some() {
                // Header/footer coverage only; full bodies come from the detached
                // path below, so flush durability never depends on the remote store.
                self.warm_cache_for_uris(warm_store.clone(), warm_table_uri.clone(), warm_added.clone(), Some(crate::config::CACHE_CONFIRM_TIMEOUT), false)
                    .await;
            }
            let db = self.clone();
            spawn_until_shutdown(
                self.maintenance_shutdown.clone(),
                async move { db.warm_cache_for_uris(warm_store, warm_table_uri, warm_added, None, true).await },
            );
        }
        for (project, dirty_bins) in projects {
            self.statistics_extractor.invalidate(project, table_name).await;
            for (date, bin) in *dirty_bins {
                self.enqueue_dirty_bin(project, table_name, date, *bin);
            }
        }
        debug!("Invalidated statistics cache after write to {}/{}", project_id, table_name);
        // Periodic reconcile, OFF the flush path: every Nth commit, rebuild the file list
        // from object-store truth in the background to bound incremental-replay drift. Runs
        // on a detached clone so it never touches `added` or the persisted snapshot.
        let reconcile_n = self.config.maintenance.timefusion_snapshot_reconcile_commits;
        if self.incremental_snapshot()
            && reconcile_n > 0
            && committed_version.is_some_and(|v| (v + Self::reconcile_offset(project_id, table_name, reconcile_n)).is_multiple_of(reconcile_n))
        {
            let table_ref = table_ref.clone();
            let (project_id, table_name) = table_key(project_id, table_name);
            spawn_until_shutdown(self.maintenance_shutdown.clone(), async move { Self::reconcile_snapshot(&table_ref, &project_id, &table_name).await });
        }
        added
    }

    /// Stable per-table offset into the reconcile cycle so tables committing in
    /// lockstep don't all hit their `% reconcile_n == 0` boundary together.
    fn reconcile_offset(project_id: &str, table_name: &str, reconcile_n: u64) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = twox_hash::XxHash3_64::default();
        (project_id, table_name).hash(&mut h);
        h.finish() % reconcile_n
    }

    /// Rebuild a table's in-memory file list from object-store truth and swap it
    /// in — but only if no commit advanced the handle while we rebuilt, since a
    /// rebuild is pinned to its version and a stale swap would drop newer files.
    /// Runs detached (off the flush path); never persists (the commit path
    /// already persisted the correct incremental state).
    async fn reconcile_snapshot(table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) {
        let mut fresh = table_ref.read().await.clone();
        if let Err(e) = Self::materialize_snapshot_files(&mut fresh, true).await {
            warn!("Snapshot reconcile failed for {project_id}/{table_name}: {e}");
            return;
        }
        let fresh_version = fresh.version();
        let mut shared = table_ref.write().await;
        if fresh_version == shared.version() {
            *shared = fresh;
            debug!("Reconciled snapshot for {project_id}/{table_name} at v{fresh_version:?}");
        }
    }

    /// Read the latest commit metadata for each WAL topic and fast-forward the walrus cursor to
    /// `max(local, delta)` per shard, closing the crash-mid-flush window where Delta committed
    /// but the watermark advance did not finish.
    ///
    /// MUST run before `recover_from_wal`. Best-effort: failures are logged and skipped, so this
    /// can never make recovery worse than at-least-once.
    pub async fn derive_wal_cursors_from_delta(
        &self, wal: &crate::write::wal::WalManager, layer: Option<&crate::write::BufferedWriteLayer>,
    ) -> anyhow::Result<usize> {
        use futures::stream::{self, StreamExt};

        // Group logical WAL topics by physical Delta log so a dirty boot loads each
        // unified table's snapshot once, not once per project sharing it.
        let custom = self.custom_storage_keys().await;
        let physical: HashMap<(String, String), Vec<(String, String)>> = wal
            .list_topic_pairs()
            .into_iter()
            .map(|(project_id, table_name)| {
                let physical_project = if custom.contains(&(project_id.clone(), table_name.clone())) { project_id.clone() } else { String::new() };
                ((physical_project, table_name.clone()), (project_id, table_name))
            })
            .into_group_map();
        let totals: Vec<usize> = stream::iter(physical.into_values())
            .map(|topics| async move {
                self.derive_wal_cursors_for_physical_table(wal, topics, layer).await.unwrap_or_else(|e| {
                    warn!("WAL cursor derivation failed for a physical table; its shards keep their old floor: {e}");
                    0
                })
            })
            .buffer_unordered(self.config.buffer.delta_scan_concurrency())
            .collect()
            .await;
        Ok(totals.into_iter().sum())
    }

    async fn derive_wal_cursors_for_physical_table(
        &self, wal: &crate::write::wal::WalManager, topics: Vec<(String, String)>, layer: Option<&crate::write::BufferedWriteLayer>,
    ) -> anyhow::Result<usize> {
        let Some((representative_project, representative_table)) = topics.first() else { return Ok(0) };
        // Scan recent commits; replay-derived commits without a watermark
        // contribute nothing so they can't reset the MAX backward.
        let Ok(table_ref) = self.resolve_table(representative_project, representative_table).await else {
            return Ok(0);
        };
        let table = table_ref.read().await;
        let commits: Vec<_> = match table.history(Some(self.config.buffer.delta_scan_depth())).await {
            Ok(it) => it.collect(),
            Err(e) => {
                debug!("derive_wal_cursor: history unavailable for {}/{}: {}", representative_project, representative_table, e);
                return Ok(0);
            }
        };
        drop(table);

        topics
            .into_iter()
            .map(|(project_id, table_name)| {
                // Batch-set identities from the same scan. These feed ONLY the flush-time
                // decline, never the cursor advance below, which stays governed by the
                // conservative watermark.
                if self.config.buffer.landed_skip_enabled()
                    && let Some(layer) = layer
                {
                    let digests: Vec<crate::write::LandedDigest> =
                        commits.iter().flat_map(|ci| parse_landed_digests_from_json(&ci.info, &project_id, &table_name)).collect();
                    if !digests.is_empty() {
                        info!("Loaded {} landed-batch identities for {}.{}", digests.len(), project_id, table_name);
                        layer.note_landed_digests(&project_id, &table_name, digests);
                    }
                }
                let delta_max = max_watermark_across_commits(commits.iter().map(|ci| &ci.info), wal.shards_per_topic(), &project_id, &table_name);
                let advanced = wal.merge_persisted_positions(&project_id, &table_name, &delta_max)?;
                if advanced > 0 {
                    info!("Delta-derived cursor advance: project={}, table={}, shards_advanced={}", project_id, table_name, advanced);
                }
                Ok(advanced)
            })
            .sum()
    }
}

/// Spill `DiskManager` for one maintenance-family env: the explicit on-disk
/// directory plus the configured byte ceiling.
///
/// The ceiling must be set HERE, on the builder. DataFusion's default is 100 GB, which a single
/// large-file sort can exceed — the sort then dies at the cap and the unit requeues forever.
///
/// ```
/// # use timefusion::database::spill_disk_builder;
/// let dm = spill_disk_builder(std::env::temp_dir().join("tf-spill-doctest"), 220).build().unwrap();
/// assert_eq!(dm.max_temp_directory_size(), 220 * 1024 * 1024 * 1024);
/// // DataFusion's default is far below the configured cap.
/// assert!(datafusion::execution::disk_manager::DiskManagerBuilder::default().build().unwrap().max_temp_directory_size() < 110 * 1024 * 1024 * 1024);
/// ```
pub fn spill_disk_builder(spill_dir: std::path::PathBuf, max_gb: u64) -> datafusion::execution::disk_manager::DiskManagerBuilder {
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    DiskManagerBuilder::default()
        .with_mode(DiskManagerMode::Directories(vec![spill_dir]))
        .with_max_temp_directory_size(max_gb.saturating_mul(1024 * 1024 * 1024))
}
