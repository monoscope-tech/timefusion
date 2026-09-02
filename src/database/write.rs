//! Write path: insert/coalesced-commit machinery, flush-time sort, runtime envs,
//! per-table locks, staged writes, watermark reconciliation.
use super::*;

impl Database {
    /// Directory holding locally persisted Delta snapshots (see `snapshot_cache`).
    pub(crate) fn delta_snapshot_dir(cfg: &AppConfig) -> PathBuf {
        crate::write::wal::meta_path(&cfg.core.timefusion_data_dir, "delta_snapshots")
    }

    /// Whether snapshot refreshes may take the incremental catch-up fast path
    /// (see [`refresh_table_snapshot`]) — exposed for the DML path in dml.rs.
    pub(crate) fn incremental_snapshot(&self) -> bool {
        self.config.maintenance.timefusion_incremental_snapshot
    }

    /// Returns the process-wide query memory pool and Parquet metadata cache.
    pub(crate) fn shared_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.runtime_env
            .get_or_init(|| {
                let pool_size = self.config.derived.query_pool_bytes();
                use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, TrackConsumersPool};
                // Name the largest consumers when the pool is exhausted.
                let top = std::num::NonZeroUsize::new(5).unwrap();
                let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = match self.config.memory.timefusion_memory_pool {
                    crate::config::MemoryPoolKind::Greedy => Arc::new(TrackConsumersPool::new(GreedyMemoryPool::new(pool_size), top)),
                    crate::config::MemoryPoolKind::FairSpill => Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_size), top)),
                };
                let meta_cache_bytes = self.config.cache.timefusion_df_metadata_cache_mb * 1024 * 1024;
                Arc::new(build_query_runtime_env(pool, meta_cache_bytes))
            })
            .clone()
    }

    /// Dedicated `RuntimeEnv` for maintenance jobs (optimize/dedup/recompress).
    ///
    /// Uses a FairSpill pool so each consumer can reserve its floor and spill, rather than being
    /// starved by queries on a Greedy pool. Spills land on an explicit on-disk directory under the
    /// data volume, not a RAM-backed `/tmp`. The bounded pool fails as error rather than OOM-killing,
    /// and is sized from the budget left over the query pool.
    fn build_spill_runtime_env(&self, pool_size: usize, spill_subdir: &str) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        use datafusion::execution::{
            disk_manager::{DiskManagerBuilder, DiskManagerMode},
            memory_pool::{FairSpillPool, TrackConsumersPool},
            runtime_env::RuntimeEnvBuilder,
        };
        let spill_dir = self.config.core.timefusion_data_dir.join(spill_subdir);
        let _ = std::fs::create_dir_all(&spill_dir);
        reap_orphaned_spill_dirs(&spill_dir);
        let disk = DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![spill_dir]));
        // Name holders, not merely the allocation victim, on exhaustion.
        let top = std::num::NonZeroUsize::new(5).expect("5 is non-zero");
        let pool = Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_size), top));
        Arc::new(RuntimeEnvBuilder::new().with_memory_pool(pool).with_disk_manager_builder(disk).build().expect("build maintenance runtime env"))
    }

    /// Light-optimize slice of the maintenance budget.
    ///
    /// Deferred to `light_share_bytes` (not recomputed here) — a local formula
    /// previously duplicated it by coincidence and drifted when the heavy share moved.
    pub(crate) fn light_optimize_pool_bytes(&self) -> usize {
        self.config.derived.light_share_bytes()
    }

    /// Packing's dedicated slice of the light pool so repair cannot starve it.
    ///
    /// The two passes used to share one pool and `light_optimize_brake` stopped packing while
    /// any repair bin was in flight. With a 8640s repair budget on a 3600s period, packing
    /// effectively never ran. An even split covers both sides' measured demand: packing needs
    /// ~4.5 GB plus sort reservations for 3 bins; repair needs up to its unspillable merge
    /// reservation ladder. Repair spills either way; the alternative was packing getting nothing
    /// at all.
    pub(crate) fn pack_pool_bytes(&self) -> usize {
        (self.light_optimize_pool_bytes() / 2).max(1)
    }

    /// Repair's slice: the light share minus packing's reservation. Still far
    /// above the unspillable `REPAIR_SORT_PARTITIONS * REPAIR_SORT_RESERVATION_BYTES`
    /// merge share, and the partition ladder handles the rest. A repair sort
    /// peaked 14.3 GB of a 15.4 GB pool, but that is a spilling sort taking what
    /// it is offered, not what it needs — it spills either way.
    pub(crate) fn repair_pool_bytes(&self) -> usize {
        self.light_optimize_pool_bytes() - self.pack_pool_bytes()
    }

    /// Heavy maintenance (dedup, recompress, Z-order).
    ///
    /// Deferred to the budget tree rather than derived as "the pool minus
    /// light". That residual form silently ABSORBED any share the tree carved
    /// off for someone else: adding the coordinator's slice left heavy holding
    /// it too, so coordinator + heavy + light came to 24 GiB of a 16 GiB pool.
    /// A residual definition cannot stay correct across a change to the tree,
    /// which is the same lesson `light_optimize_pool_bytes` already records.
    pub(crate) fn heavy_pool_bytes(&self) -> usize {
        self.config.derived.heavy_share_bytes()
    }

    pub(crate) fn maintenance_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.maintenance_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.heavy_pool_bytes(), "maintenance_spill")).clone()
    }

    /// Hot-tail PACKING: the reserved slice (see field doc).
    pub(crate) fn light_optimize_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.light_optimize_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.pack_pool_bytes(), "light_optimize_spill")).clone()
    }

    /// Footer REPAIR: its own pool, disjoint from packing's (see `pack_pool_bytes`).
    pub(crate) fn repair_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.repair_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.repair_pool_bytes(), "repair_spill")).clone()
    }

    pub(crate) fn coordinator_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.coordinator_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.config.derived.coordinator_share_bytes(), "coordinator_spill")).clone()
    }

    /// Sort one flush group, picking the strategy by size.
    ///
    /// Below `timefusion_sort_skip_bytes` the in-process sort wins on latency. Above it, use the
    /// pooled+spilling DataFusion sort instead of skipping: a skipped sort writes a file with no
    /// `sorting_columns` footer, and one such file disables the reader's all-or-nothing ordering for
    /// every scan touching the partition. Spilling is the better failure mode. `fallback` decides
    /// what happens when even that fails; see [`UnsortedFallback`].
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
            // `sort_batches_by_schema` also degrades on its own (schema merge,
            // row encoding, lexsort), so the guard belongs here, not only on the
            // escalation branch.
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
                // A REWRITE must not buy a transient failure with a permanent
                // one. Its inputs are already committed and already sorted, so
                // aborting costs one compaction cycle; writing the group
                // unsorted costs the partition's declared ordering FOREVER
                // (nothing re-sorts a converged file) and, because
                // `derive_common_ordering` is all-or-nothing, degrades every
                // scan whose window touches that date.
                anyhow::ensure!(
                    fallback == UnsortedFallback::Allow,
                    "escalated sort of a {} MB rewrite group failed; keeping the committed inputs rather than replacing them with an unsorted file",
                    total / (1 << 20)
                );
                // Ingest has no such choice — the rows exist nowhere else, so
                // losing them is not on the table. Counted because one such file
                // disables the reader's ordering for its whole partition — this
                // must never be silent (2026-08-03).
                crate::observability::record_flush_sort_unsorted_fallback();
                Ok((FlushBatches::Ready(batches.into_iter()), false))
            }
        }
    }

    /// Flush-path sort pool (see field doc). Bounded and spillable, so an
    /// oversized bucket degrades to disk I/O instead of an unpooled spike.
    fn flush_sort_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.flush_sort_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.config.maintenance.flush_sort_pool_bytes(), "flush_sort_spill")).clone()
    }

    /// Sort an oversized flush group inside a DataFusion plan.
    ///
    /// The in-process path allocates outside every memory pool, so past a point the only safe
    /// options are skipping the sort or this pooled, spilling sort. Spilling is strictly better
    /// than skipping: the footer stays honest and the peak is bounded by the pool. Returns `None`
    /// on any failure so the caller writes the original batches unsorted; a flush must never lose
    /// rows to a sort failure.
    async fn sort_flush_group_spilling(&self, schema: &crate::schema::TableSchema, batches: &[RecordBatch]) -> Option<Vec<RecordBatch>> {
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        // Hold a slice of the shared spill pool for the whole sort. Queueing
        // here costs latency on an already-oversized group; losing the slice
        // costs the partition's footer ordering on every later scan.
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
            .collect::<Vec<_>>()
            .join(", ");
        if order_by.is_empty() {
            return None;
        }

        // ONE pool consumer per gate permit — `flush_sort_gate`'s 512 MB-per-permit
        // sizing assumes it. N>1 partitions fans out to N ExternalSorters plus an
        // UNSPILLABLE SortPreservingMergeExec, and concurrent escalations starved
        // the FairSpillPool into the unsorted fallback (prod 2026-08-03); a
        // single-partition sort is slower but spills within its fair share, so the
        // footer stays honest. Shared by both `sort_flush_group` callers (flush's
        // `Allow` and the dedup rewrite's `Forbid`) on one 1 GB pool, so this batch
        // size governs a 6.25 GB rewrite group as much as a flush group.
        //
        // SMALL batches, same reason as `repair_session_state` (6ef5ccf):
        // `ExternalSorterMerge`'s ask scales with `batch_size`. Prod 2026-08-08,
        // even with the whole pool to itself, a 6252 MB group at 8192-row batches
        // still failed ("545.4 MB additional, 539.8 MB already allocated, 484.2 MB
        // remain" — the full pool, so contention wasn't the cause; the merge just
        // doubled a half-pool reservation). Shrinking the batch trades scan
        // throughput for the allocation; the alternative — writing the group
        // UNSORTED — poisons the partition's footer ordering for every later scan.
        let state = build_delta_write_session_state(1, self.flush_sort_runtime_env(), "256");
        let ctx = SessionContext::new_with_state(state);
        let name = format!("flush_sort_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(&name, Arc::new(MemTable::try_new(arrow_schema, vec![unified]).ok()?)).ok()?;
        let out = ctx.sql(&format!("SELECT * FROM {name} ORDER BY {order_by}")).await.ok()?.collect().await;
        let _ = ctx.deregister_table(&name);
        match out {
            Ok(sorted) => Some(sorted),
            Err(e) => {
                warn!("flush sort: spilling DataFusion sort failed, writing unsorted: {e}");
                None
            }
        }
    }

    /// Heavy-maintenance session state, built once (see field doc).
    pub(crate) fn maintenance_session_state(&self) -> datafusion::execution::session_state::SessionState {
        self.maintenance_session_state
            .get_or_init(|| build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()))
            .clone()
    }

    /// Packing session state, built once.
    ///
    /// Lifts `MAINTENANCE_MAX_PARTITIONS` because that cap is sized for heavy fan-out, while
    /// packing runs at most `max_light_optimize_k` bins in its own pool. The partition count is
    /// derived: each partition's reservation is unspillable, so a fixed high number can waste most
    /// of a small pool before sorting a single row.
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

    /// Repair batch size: deliberately smaller than packing's.
    ///
    /// A repair bin is one whole large file; `SortPreservingMergeExec` allocates per spill-run
    /// per batch, so its ask scales with `batch_size`. Sharing 2048 with packing made the merge
    /// ask exceed headroom. 256 matches the MaintenanceCli budget profile and lets the sort
    /// admit one batch before spilling. `partitions` comes from
    /// `REPAIR_SORT_PARTITION_LADDER`: a bin that exhausted the pool is retried with fewer
    /// partitions because the unspillable merge operator is per-partition. Cached per
    /// parallelism.
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

    /// Physical Delta log lock key.
    ///
    /// Collapses all default projects sharing a unified table onto one key (empty project_id is
    /// not valid and cannot collide), while custom-storage tables keep per-project isolation. Shared
    /// by `dml_lock` and `commit_lock` so both serialize at physical-log granularity.
    pub(crate) async fn table_lock_key(&self, project_id: &str, table_name: &str) -> (String, String) {
        let project_key = if self.has_custom_storage(project_id, table_name).await { project_id.to_string() } else { String::new() };
        (project_key, table_name.to_string())
    }

    pub(crate) async fn dml_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.dml_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Per-physical-table Delta commit lock (see `commit_locks`).
    pub(crate) async fn commit_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.commit_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Waiter count for the SAME key as [`Self::commit_lock`] (see
    /// `flush_waiter_counts`). Flush/ingest commit paths register a
    /// [`flush_waiter`] on it across their `lock().await`; `commit_wave` reads it
    /// and stands down while it is nonzero.
    pub(crate) async fn flush_waiters(&self, project_id: &str, table_name: &str) -> Arc<std::sync::atomic::AtomicUsize> {
        self.flush_waiter_counts.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Persist `table`'s post-commit snapshot locally (detached) so the next
    /// boot restores it and replays only later commits (see `snapshot_cache`).
    /// Called from every commit path that swaps a fresh table state in.
    pub(crate) fn persist_snapshot(&self, table: &DeltaTable) {
        // Throttle: at most one persist per table per interval. The snapshot is
        // a boot-recovery seed, not a durability requirement, so skipping most
        // commits just replays a few extra commits on next boot (see field docs).
        const MIN_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let url = table.table_url().to_string();
        let now = std::time::Instant::now();
        match self.snapshot_persist_gate.get(&url) {
            Some(last) if now.duration_since(*last) < MIN_PERSIST_INTERVAL => return,
            _ => {}
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
        // `spawn_blocking`, mirroring `persist_snapshot`'s store side: this
        // zstd-decodes and deserializes a whole `DeltaTableState` (22k+ files
        // on the fat prod tables), which is SECONDS of CPU — long work wants
        // its own thread, not a borrowed worker. Boot preload runs many of
        // these concurrently on the same runtime the coordinator is on.
        let (snapshot_dir, url_owned) = (Self::delta_snapshot_dir(&self.config), storage_uri.to_string());
        let loaded = tokio::task::spawn_blocking(move || crate::storage::load_snapshot(&snapshot_dir, &url_owned)).await.unwrap_or(None);
        let restored = match loaded {
            Some(state) => {
                let restored_version = state.version();
                let mut table = builder()?.build()?;
                table.state = Some(state);
                // `update_state()` only probes versions *after* the supplied
                // state. It returns Ok when the local snapshot is ahead of the
                // durable log, even if its own commit disappeared (prod
                // 2026-08-04: local otel_metrics v140816, S3 ended at v140806).
                // Such a zombie snapshot serves removed files and makes every
                // subsequent commit fail with InvalidTableVersion. Require its
                // anchor commit to exist; if log cleanup legitimately removed
                // it behind a newer checkpoint, a full load is also the right
                // path because it starts from that durable checkpoint.
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
        // Materialize the file list once so every post-commit update stays
        // incremental. With incremental snapshots on this is a *correctness*
        // requirement, not just perf: a non-materialized snapshot enumerates an
        // EMPTY file set, and the fast-advance post-commit hook would build on
        // it — so fail loud rather than cache a handle that serves empty results
        // (the caller retries on next access). load()/restore normally arrive
        // materialized, so this no-ops and can only fail on the rare path that
        // actually has to materialize.
        if self.config.maintenance.timefusion_incremental_snapshot {
            Self::materialize_snapshot_files(&mut table, false)
                .await
                .map_err(|e| anyhow::anyhow!("Materializing file list for '{storage_uri}' failed: {e}"))?;
        }
        Ok(table)
    }

    /// Casts each of `batches` to `writer`'s table schema (`RecordBatchWriter`,
    /// unlike `WriteBuilder`, doesn't cast for us — Utf8View→Utf8 etc, missing
    /// columns filled with nulls; safe=true, add_missing=true mirrors
    /// `WriteBuilder`'s own coercion) and streams them in, flushing at
    /// `max_file_bytes` so one oversized bucket (the MemBuffer ceiling is GBs)
    /// doesn't land as a single file — on a sorted stream each flushed piece
    /// keeps its own footer and stays time-disjoint. Shared by the per-project
    /// staged-commit path and the cross-project coalesced flush's staging phase.
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
    /// once per (project, table) unit. Shared by `insert_records_batch` and the
    /// cross-project coalesced flush path so both prepare writes identically.
    ///
    /// `staged_writer` is `None` when the fast path is unavailable — a batch
    /// carries a column the table schema lacks (delta-rs' Default-mode
    /// `RecordBatchWriter` cannot evolve schema on a partitioned table), or the
    /// writer could not be built at all. That unit must take the locked
    /// WriteBuilder merge path.
    async fn prepare_staged_write(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Result<PreparedWrite> {
        // Delta-kernel's `unshredded_variant()` expects Struct{Binary,Binary}
        // on write, but our MemBuffer carries Struct{BinaryView,BinaryView}
        // (matches what the parquet reader natively produces — no per-row
        // casts on read). Cast just-before-write so the Delta commit
        // accepts the schema.
        let batches: Vec<RecordBatch> = batches.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;

        // Get or create the table
        let table_ref = self.get_or_create_table(project_id, table_name).await?;

        // Get the appropriate schema for this table
        let schema = schema_or_default(table_name);

        let dirty_bins: Vec<(String, i64)> = if schema.dedup_keys.is_empty() {
            Vec::new()
        } else {
            // Dirty-bin granularity, intentionally independent of MemBuffer's (configurable,
            // currently 5-min) bucket duration — the two ideas coincide at "10 min" only historically.
            const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
            batches
                .iter()
                .filter_map(|batch| batch.column_by_name("timestamp"))
                .filter_map(|column| column.as_any().downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>())
                .flat_map(|timestamps| {
                    timestamps.iter().flatten().filter_map(|timestamp| {
                        chrono::DateTime::from_timestamp_micros(timestamp).map(|time| (time.date_naive().to_string(), timestamp.div_euclid(BIN_MICROS)))
                    })
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .collect()
        };

        // Cluster by the declared sort keys (timestamp-first) so the parquet
        // SortingColumn footer is honest and the page index localizes the lead
        // key. `sorted` is false when a schema-evolved bucket can't be combined
        // (we then write unsorted) — declare the footer only when it's true.
        // Ingest: these rows are not committed anywhere else yet, so an unsorted
        // write beats losing them (`UnsortedFallback::Allow` never errors).
        let (batches, sorted) = self.sort_flush_group(schema, batches, UnsortedFallback::Allow).await?;
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);

        let staging_table = { table_ref.read().await.clone() };
        let stage_store = staging_table.log_store().object_store(None);
        let staged_writer = match deltalake::writer::RecordBatchWriter::for_table(&staging_table) {
            Ok(w) => {
                let w = w.with_writer_properties(writer_properties.clone());
                let arrow_schema = w.arrow_schema();
                let table_fields: HashSet<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
                let evolves = batches.schemas().iter().any(|s| s.fields().iter().any(|f| !table_fields.contains(f.name().as_str())));
                (!evolves).then_some(w)
            }
            Err(e) => {
                debug!("RecordBatchWriter::for_table failed, using merge path: {}", e);
                None
            }
        };
        Ok(PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer, sorted })
    }

    /// Insert batches and return the URIs of files newly added by this commit
    /// (empty for the buffered-layer / batch-queue paths where the actual
    /// Delta write happens later). Callers use the returned list to drive
    /// cache warming and the tantivy sidecar without paying for a second
    /// `update_state()` log scan.
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
        // Normalize timezone-as-offset (`+00:00`) timestamp columns to the
        // IANA `"UTC"` form. Delta-rs Arrow→Delta schema conversion only
        // accepts `"UTC"`; without this normalisation the flush callback
        // path (which feeds MemBuffer batches straight into Delta) errors
        // out and data piles up in MemBuffer.
        let batches: Vec<RecordBatch> =
            batches.into_iter().map(normalize_timestamp_tz).map(|batch| batch.and_then(derive_date_partition)).collect::<DFResult<_>>()?;

        // Extract project_id from first batch if not provided. If neither the
        // caller nor the data carries one, log loudly and bucket under
        // "default" — silently misrouting writes is the worst outcome, but
        // returning an error would break callers that already rely on the
        // legacy fallback.
        let project_id = if project_id.is_empty() && !batches.is_empty() {
            extract_project_id(&batches[0]).unwrap_or_else(|| {
                warn!("insert_records_batch: empty project_id and batch has no project_id column → bucketing under 'default'");
                "default".to_string()
            })
        } else if project_id.is_empty() {
            warn!("insert_records_batch: empty project_id and no batches → bucketing under 'default'");
            "default".to_string()
        } else {
            project_id.to_string()
        };

        // Use provided table_name or default to otel_logs_and_spans
        let table_name = if table_name.is_empty() { "otel_logs_and_spans".to_string() } else { table_name.to_string() };

        if watermark.is_none() {
            self.invalidate_rollup_batches(&project_id, &table_name, &batches)?;
        }

        // Stamp the schema's TF-owned version column. This is the single funnel
        // every *inbound* write passes through — pgwire INSERT (`write_all`),
        // the `__bulk` direct-to-Delta alias, and the legacy batch queue —
        // regardless of whether the buffered layer is configured, and it
        // runs before the WAL append so the durable record carries the value.
        //
        // A `watermark` marks the one caller that is NOT inbound: the flush of
        // buffered rows back out to Delta (bucket flush, coalesced flush, boot
        // relief). Those rows were stamped on their way in and must keep that
        // value — a re-stamp would give a crash-retried flush a different value
        // than the WAL holds. WAL replay bypasses this function entirely and
        // seeds the clock via `insert_coerce::observe_batch` instead.
        let batches = if watermark.is_none() { crate::write::stamp_version(&table_name, batches) } else { batches };

        // If buffered layer is configured and not skipping, use it (WAL → MemBuffer flow).
        // No files are written synchronously on this path; an empty URI list is correct.
        if !skip_queue && let Some(layer) = self.buffered_layer() {
            span.record("use_queue", "buffered_layer");
            layer.insert_bounded(&project_id, &table_name, batches, bound).await?;
            return Ok(Vec::new());
        }

        // Fallback to legacy batch queue if configured
        let enable_queue = self.config.core.enable_batch_queue;
        if !skip_queue
            && enable_queue
            && let Some(ref queue) = self.batch_queue
        {
            span.record("use_queue", true);
            for batch in batches {
                if let Err(e) = queue.queue(batch) {
                    return Err(anyhow::anyhow!("Queue error: {}", e));
                }
            }
            return Ok(Vec::new());
        }

        span.record("use_queue", false);

        // Identity of the batch set this commit carries, so a later boot can
        // decline to re-write it (see `LANDED_DIGESTS_KEY`). Computed on the
        // batches AS THE FLUSH HANDED THEM OVER — before `prepare_staged_write`
        // coerces or sorts — because that is exactly what the flush side hashes
        // when it checks. Only the flush path (`watermark.is_some()`) records
        // one: an inbound write is not something replay can duplicate.
        let landed = (self.config.buffer.landed_skip_enabled() && watermark.is_some() && crate::write::landed_identity_applies(&table_name))
            .then(|| crate::write::landed_digest(&batches))
            .flatten();

        let PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer, sorted } =
            self.prepare_staged_write(&project_id, &table_name, batches).await?;

        // Hoist out of the retry loop — the watermark is the same on every attempt.
        let commit_properties = watermark.map(|w| {
            build_watermark_commit_properties(
                [(project_id.clone(), table_name.clone(), w.clone())],
                landed.map(|d| (project_id.clone(), table_name.clone(), d)),
            )
        });
        // Let the post-commit hook advance the snapshot incrementally — carry
        // the materialized file list forward, append the committed files, drop
        // any removed ones — instead of re-materializing the whole active set.
        // Safe for the staged (pure-append) and schema-evolution merge paths
        // alike: the hook rebuilds the kernel snapshot from the log, so a
        // MetaData/schema change IS applied; only the file-list re-materialize
        // is skipped.
        let commit_properties = if self.config.maintenance.timefusion_incremental_snapshot {
            Some(commit_properties.unwrap_or_else(base_commit_properties).with_incremental_advance(true))
        } else {
            commit_properties
        };
        let max_retries = 5;
        // STAGED COMMIT (fast path): encode parquet + upload to S3 OUTSIDE the
        // per-table commit lock, then serialize only the tiny commit-log
        // append. The old path held the lock across the whole `.write()`
        // (parquet encode + S3 upload + commit), serializing every tenant's
        // upload behind one mutex — the ~8-17 rows/s flush ceiling under heavy
        // backfill. A staged write parallelizes the uploads and pays the lock
        // only for a sub-second log append; OCC conflicts re-commit the already
        // uploaded parquet (no re-encode/re-upload).
        //
        // delta-rs' Default-mode RecordBatchWriter cannot evolve schema on a
        // partitioned table, so when a batch carries a column absent from the
        // table schema `prepare_staged_write` returns no staged writer and we
        // fall back to the locked WriteBuilder merge path below.
        if let Some(mut writer) = staged_writer {
            use deltalake::{
                kernel::{Action, transaction::TableReference},
                protocol::DeltaOperation,
            };

            // Upload parquet (no commit) on the staging clone — outside the lock.
            let stage_span = tracing::trace_span!(parent: &span, "delta.stage_parquet");
            let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
            let adds: Vec<Action> = Self::stage_batches(&mut writer, batches, max_file_bytes)
                .instrument(stage_span)
                .await
                .map_err(|e| anyhow::anyhow!("staged parquet flush failed: {}", e))?;
            if adds.is_empty() {
                return Ok(Vec::new());
            }

            let partition_by = (!schema.partitions.is_empty()).then(|| schema.partitions.clone());
            let op = DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by, predicate: None };
            // Store to clean up the staged parquet on a terminal commit failure —
            // those objects have no Add/Remove in the log, so Delta VACUUM won't
            // reclaim them; abandoning them leaks files on S3 forever.
            let stage_store = stage_store.clone();

            let commit_lock = self.commit_lock(&project_id, &table_name).await;
            let flush_waiters = self.flush_waiters(&project_id, &table_name).await;
            let mut retry_count = 0;
            loop {
                // Refresh UNDER the lock (the merge path refreshes before locking).
                // The per-table commit lock serializes all in-process commits to
                // THIS log, so refreshing here guarantees we build on the previous
                // committer's version and never self-conflict; refresh is
                // probe-cheap (a single GET that 404-short-circuits when already
                // current), so the extra lock-hold is sub-millisecond on the common
                // path.
                // FLUSH PRIORITY: registered across the WAIT only. Waves stand
                // down while this is nonzero (see `flush_waiter_counts`), so the
                // count must fall the moment we hold the lock — or the moment a
                // watchdog cancels this future.
                let commit_guard = {
                    let _waiting = flush_waiter(&flush_waiters);
                    commit_lock.lock().await
                };
                // DIAG (commit-throughput profiling): time the serial commit phases
                // (refresh + Delta log append) under the lock — these bound the
                // process-wide commit rate. Remove once the flush bottleneck is found.
                let _t_refresh = std::time::Instant::now();
                if let Err(e) = bounded_commit_await(
                    COMMIT_LOCK_OP_TIMEOUT,
                    "flush_refresh",
                    &table_name,
                    refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot),
                )
                .await
                {
                    debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e.message);
                }
                let _refresh_ms = _t_refresh.elapsed().as_millis();
                let mut new_table = { table_ref.read().await.clone() };
                let _t_build = std::time::Instant::now();
                // Bounded for the same reason as the wave path: this await holds
                // the per-table commit lock every other committer queues on.
                let commit_res = bounded_commit_await(
                    COMMIT_LOCK_OP_TIMEOUT,
                    "flush_commit",
                    &table_name,
                    deltalake::kernel::transaction::CommitBuilder::from(commit_properties.clone().unwrap_or_else(base_commit_properties))
                        .with_actions(adds.clone())
                        .build(Some(new_table.snapshot()? as &dyn TableReference), new_table.log_store(), op.clone()),
                )
                .await;
                let _build_ms = _t_build.elapsed().as_millis();
                match commit_res {
                    Ok(finalized) => {
                        // Diff pre- vs post-commit file URIs for `added`. Capture
                        // pre-uris here (only on success) — before the state swap
                        // below makes `new_table` post-commit — so failed attempts
                        // don't pay the full-table file-URI walk.
                        let pre_uris: HashSet<String> = file_uris(&new_table);
                        new_table.state = Some(finalized.snapshot());
                        drop(commit_guard);
                        // AFTER the commit lands, never before: an abandoned
                        // attempt's staged parquet is deleted, and marking a
                        // deleted path is harmless but a marked path that never
                        // committed is a lie we would keep forever.
                        self.mark_written_sorted(schema, sorted, &adds);
                        let _t_record = std::time::Instant::now();
                        let _committed = self
                            .record_committed_write(
                                &table_ref,
                                &[(project_id.as_str(), dirty_bins.as_slice())],
                                &table_name,
                                new_table,
                                &pre_uris,
                                watermark.is_some(),
                            )
                            .await;
                        info!(
                            "commit_timing project={} table={} refresh_ms={} build_ms={} record_ms={} files={}",
                            project_id,
                            table_name,
                            _refresh_ms,
                            _build_ms,
                            _t_record.elapsed().as_millis(),
                            adds.len()
                        );
                        return Ok(_committed);
                    }
                    Err(CommitFailure { message: e, timed_out }) => {
                        drop(commit_guard);
                        if !timed_out && is_occ_conflict_err(&e) {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                                return Err(anyhow::anyhow!("staged commit failed after {} retries: {}", max_retries, e));
                            }
                            debug!("staged commit conflict, retrying ({}/{}): {}", retry_count, max_retries, e);
                            tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                            continue;
                        }
                        // Non-OCC error: the commit MAY have landed (post-commit
                        // hook / snapshot refresh failed AFTER N.json was written).
                        // Capture the pre-commit file set from the still-pre-commit
                        // clone (only on this rare branch — the OCC-retry path must
                        // not pay the full-table URI walk), then probe.
                        let pre_uris: HashSet<String> = file_uris(&new_table);
                        match probe_after_timeout(self.probe_commit_landed_bounded(&table_ref, &adds).await, timed_out) {
                            CommitProbe::Landed => {
                                warn!(
                                    "staged commit for {}/{} reported an error but LANDED (post-commit hook failed) — draining bucket: {}",
                                    project_id, table_name, e
                                );
                                let post = { table_ref.read().await.clone() };
                                let committed = self
                                    .record_committed_write(
                                        &table_ref,
                                        &[(project_id.as_str(), dirty_bins.as_slice())],
                                        &table_name,
                                        post,
                                        &pre_uris,
                                        watermark.is_some(),
                                    )
                                    .await;
                                return Ok(committed);
                            }
                            CommitProbe::NotLanded => {
                                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                                return Err(anyhow::anyhow!("staged commit failed: {}", e));
                            }
                            CommitProbe::Inconclusive => {
                                warn!(
                                    "staged commit for {}/{} errored and landing is UNCONFIRMED (snapshot read failed) — leaving staged parquet in place to avoid a dangling Add: {}",
                                    project_id, table_name, e
                                );
                                return Err(anyhow::anyhow!("staged commit failed (landing unconfirmed): {}", e));
                            }
                        }
                    }
                }
            }
        }

        // SCHEMA-EVOLUTION FALLBACK: locked WriteBuilder merge path. Holds the
        // commit lock across the whole write so the schema-metadata merge can't
        // race a concurrent commit. Rare (only when a batch adds a column).
        //
        // WriteBuilder re-submits the same rows on every OCC retry, so the lazy
        // sort-merge has to be materialized once here — this path keeps the old
        // whole-bucket residency by necessity. It is unreachable when a staged
        // writer exists (the block above always returns).
        let batches: Vec<RecordBatch> = batches.collect::<Result<_, _>>()?;
        let commit_lock = self.commit_lock(&project_id, &table_name).await;
        let flush_waiters = self.flush_waiters(&project_id, &table_name).await;
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count < max_retries {
            if let Err(e) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                debug!("Failed to update table state before write (attempt {}): {}", retry_count + 1, e);
            }
            let commit_guard = {
                let _waiting = flush_waiter(&flush_waiters);
                commit_lock.lock().await
            };
            let (table, pre_uris) = {
                let guard = table_ref.read().await;
                let pre: HashSet<String> = file_uris(&guard);
                (guard.clone(), pre)
            };

            let write_span = tracing::trace_span!(parent: &span, "delta.write_operation", retry_attempt = retry_count + 1);
            let write_result = async {
                table
                    .clone()
                    .write(batches.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
                    // Always set base properties (hooks off) — a None here would
                    // let WriteBuilder's own default re-enable the checkpoint hook.
                    .with_commit_properties(commit_properties.clone().unwrap_or_else(base_commit_properties))
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
                        retry_count += 1;
                        last_error = Some(e);
                        debug!("Delta write conflict detected, retrying... (attempt {}/{})", retry_count, max_retries);
                        // Release the commit lock BEFORE the backoff sleep — do
                        // not remove. Holding it across the sleep serializes
                        // every other writer behind this writer's backoff.
                        drop(commit_guard);
                        tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                        drop(table); // stale clone — the retry re-clones after the reload
                        if let Err(reload_err) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                            debug!("Failed to reload table state after conflict: {}", reload_err);
                        }
                    } else {
                        return Err(anyhow::anyhow!("Delta write failed: {}", e));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Delta write failed after {} retries: {}",
            max_retries,
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Cross-project flush commit coalescing.
    ///
    /// One tick's per-project flush units become a single Delta commit per physical table.
    /// Default-storage projects share one `_delta_log`; custom-storage projects keep their own
    /// commit. Parquet encode/upload still run in parallel outside the lock; only the commit-log
    /// append is shared. Staging failures exclude only that unit; shared commit failures fail every
    /// unit in the physical group; schema-evolving units are committed alone so they don't block
    /// co-tenants. Returns one result per input unit in input order.
    pub async fn insert_records_batches_coalesced(&self, units: Vec<CoalescedWriteUnit>) -> Vec<Result<Vec<String>>> {
        use deltalake::{kernel::Action, protocol::DeltaOperation};
        use futures::stream::{self, StreamExt};
        let parallelism = self.config.buffer.flush_parallelism();
        let mut results: Vec<Result<Vec<String>>> = units.iter().map(|_| Ok(Vec::new())).collect();
        let units = std::sync::Arc::new(units);

        // ---- Phase 1: prepare (bounded-concurrent; table resolution + casts).
        let prepared: Vec<(usize, Result<PreparedForPhysicalTable>)> = stream::iter(0..units.len())
            .map(|i| {
                let units = units.clone();
                async move {
                    let u = &units[i];
                    let prep = self.prepare_staged_write(&u.project_id, &u.table_name, u.batches.clone()).await;
                    let key = self.table_lock_key(&u.project_id, &u.table_name).await;
                    (i, prep.map(|p| (p, key)))
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // ---- Phase 2: stage parquet OUTSIDE any lock, `flush_parallelism`-wide.
        // Schema-evolution units never reach here: they are split out to the solo
        // (locked WriteBuilder) path so one project's merge can't stall the rest.
        let mut solo: Vec<usize> = Vec::new();
        let mut stageable: Vec<(usize, PreparedWrite, (String, String))> = Vec::new();
        for (i, prep) in prepared {
            match prep {
                Err(e) => results[i] = Err(e),
                Ok((p, _)) if p.staged_writer.is_none() => {
                    debug!("coalesced flush: {}/{} needs schema evolution — splitting out of the shared commit", units[i].project_id, units[i].table_name);
                    drop(p);
                    solo.push(i);
                }
                Ok((p, key)) => stageable.push((i, p, key)),
            }
        }

        let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
        let staged: Vec<(usize, (String, String), Result<StagedUnit>)> = stream::iter(stageable)
            .map(|(i, prep, key)| async move {
                let PreparedWrite { table_ref, schema, dirty_bins, batches, stage_store, staged_writer, sorted, .. } = prep;
                let mut writer = staged_writer.expect("filtered above");
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
            .map(|group| {
                let units = units.clone();
                async move {
                    let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
                    let table_name = units[indices[0]].table_name.clone();
                    let projects: Vec<&str> = indices.iter().map(|i| units[*i].project_id.as_str()).collect();
                    let table_ref = group[0].1.table_ref.clone();
                    let schema = group[0].1.schema;
                    let adds: Vec<Action> = group.iter().flat_map(|(_, u)| u.adds.iter().cloned()).collect();
                    let watermarks = indices.iter().map(|i| (units[*i].project_id.clone(), units[*i].table_name.clone(), units[*i].watermark.clone()));
                    // Per-unit landed identity, on the batches the flush handed
                    // over (see the single-unit path). Each unit's digest is
                    // scoped to its own topic, so one tenant's identity can
                    // never decline another's write.
                    let digests: Vec<(String, String, crate::write::LandedDigest)> = if self.config.buffer.landed_skip_enabled() {
                        indices
                            .iter()
                            .filter(|i| crate::write::landed_identity_applies(&units[**i].table_name))
                            .filter_map(|i| {
                                crate::write::landed_digest(&units[*i].batches).map(|d| (units[*i].project_id.clone(), units[*i].table_name.clone(), d))
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let commit_properties = build_watermark_commit_properties(watermarks, digests);
                    let commit_properties = if self.config.maintenance.timefusion_incremental_snapshot {
                        commit_properties.with_incremental_advance(true)
                    } else {
                        commit_properties
                    };
                    let per_project: Vec<(&str, &[(String, i64)])> =
                        group.iter().map(|(i, u)| (units[*i].project_id.as_str(), u.dirty_bins.as_slice())).collect();
                    let partition_by = (!schema.partitions.is_empty()).then(|| schema.partitions.clone());
                    let op = DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by, predicate: None };

                    let outcome = self
                        .commit_coalesced_group(&table_ref, &per_project, &table_name, adds.clone(), commit_properties, op)
                        .await
                        .map(|added| attribute_added_files(added, &projects));
                    match outcome {
                        Ok(per_project_added) => {
                            // PER UNIT, not per group: a group is one commit but
                            // many prepared writes, and one unit degrading to an
                            // unsorted write must not exonerate its neighbours'
                            // files — nor be exonerated by them.
                            for (_, unit) in &group {
                                self.mark_written_sorted(unit.schema, unit.sorted, &unit.adds);
                            }
                            indices.into_iter().zip(per_project_added).map(|(i, a)| (i, Ok(a))).collect::<Vec<_>>()
                        }
                        Err(e) => {
                            // Fail EVERY project in the group identically — no
                            // partial settle. The caller requeues each one's buckets
                            // with unchanged retry semantics.
                            if !e.to_string().contains(INCONCLUSIVE_COMMIT_MARKER) {
                                // Every unit in a physical group stages into the SAME
                                // store (same Delta table), so one store deletes all.
                                Self::cleanup_orphaned_parquet(&group[0].1.stage_store, &adds).await;
                            }
                            indices.into_iter().map(|i| (i, Err(anyhow::anyhow!("coalesced commit failed for {}: {}", table_name, e)))).collect()
                        }
                    }
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        for (i, r) in committed.into_iter().flatten() {
            results[i] = r;
        }

        // ---- Phase 4: schema-evolution units, each on its own (locked merge path).
        let solo_results: Vec<(usize, Result<Vec<String>>)> = stream::iter(solo)
            .map(|i| {
                let units = units.clone();
                async move {
                    let u = &units[i];
                    (i, self.insert_records_batch(&u.project_id, &u.table_name, u.batches.clone(), true, Some(&u.watermark)).await)
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        for (i, r) in solo_results {
            results[i] = r;
        }
        results
    }

    /// The shared commit-log append for one physical table's coalesced group.
    /// Mirrors the per-project staged-commit loop (same OCC retry budget +
    /// backoff, same landed-despite-error probe); the only difference is that
    /// the actions and the watermark metadata span several projects. Cleanup of
    /// staged parquet is the caller's (it owns every unit's store).
    async fn commit_coalesced_group(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, adds: Vec<deltalake::kernel::Action>,
        commit_properties: CommitProperties, op: deltalake::protocol::DeltaOperation,
    ) -> Result<Vec<String>> {
        use deltalake::kernel::transaction::TableReference;
        const MAX_RETRIES: u32 = 5;
        // Any member resolves to the same physical lock (the group key IS
        // `table_lock_key`), so serialization is identical to the per-project path.
        let commit_lock = self.commit_lock(projects[0].0, table_name).await;
        let flush_waiters = self.flush_waiters(projects[0].0, table_name).await;
        let mut retry_count = 0u32;
        loop {
            let commit_guard = {
                let _waiting = flush_waiter(&flush_waiters);
                commit_lock.lock().await
            };
            if let Err(e) = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "coalesced_refresh",
                table_name,
                refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot),
            )
            .await
            {
                debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e.message);
            }
            let mut new_table = { table_ref.read().await.clone() };
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "coalesced_commit",
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(commit_properties.clone()).with_actions(adds.clone()).build(
                    Some(new_table.snapshot()? as &dyn TableReference),
                    new_table.log_store(),
                    op.clone(),
                ),
            )
            .await;
            match commit_res {
                Ok(finalized) => {
                    let pre_uris: HashSet<String> = file_uris(&new_table);
                    new_table.state = Some(finalized.snapshot());
                    drop(commit_guard);
                    let added = self.record_committed_write(table_ref, projects, table_name, new_table, &pre_uris, true).await;
                    debug!("coalesced commit landed: table={} projects={} files={}", table_name, projects.len(), adds.len());
                    return Ok(added);
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    drop(commit_guard);
                    if !timed_out && is_occ_conflict_err(&e) {
                        retry_count += 1;
                        if retry_count >= MAX_RETRIES {
                            return Err(anyhow::anyhow!("coalesced staged commit failed after {} retries: {}", MAX_RETRIES, e));
                        }
                        debug!("coalesced commit conflict, retrying ({}/{}): {}", retry_count, MAX_RETRIES, e);
                        tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                        continue;
                    }
                    // Non-OCC: the commit MAY have landed (post-commit hook failed
                    // after N.json was written). Same three-way probe as the
                    // per-project path — never delete parquet a landed commit
                    // references.
                    let pre_uris: HashSet<String> = file_uris(&new_table);
                    match probe_after_timeout(self.probe_commit_landed_bounded(table_ref, &adds).await, timed_out) {
                        CommitProbe::Landed => {
                            warn!("coalesced commit for {} reported an error but LANDED (post-commit hook failed) — draining: {}", table_name, e);
                            let post = { table_ref.read().await.clone() };
                            return Ok(self.record_committed_write(table_ref, projects, table_name, post, &pre_uris, true).await);
                        }
                        CommitProbe::NotLanded => return Err(anyhow::anyhow!("coalesced staged commit failed: {}", e)),
                        CommitProbe::Inconclusive => {
                            warn!("coalesced commit for {} errored and landing is UNCONFIRMED — leaving staged parquet in place: {}", table_name, e);
                            // Signal "do not delete the parquet" by returning a
                            // distinct marker error the caller checks.
                            return Err(anyhow::anyhow!("{}: coalesced staged commit failed (landing unconfirmed): {}", INCONCLUSIVE_COMMIT_MARKER, e));
                        }
                    }
                }
            }
        }
    }

    /// Probe whether a staged commit landed despite returning an error.
    ///
    /// Refresh the snapshot and check that every Add we tried to commit is now active. `Landed`
    /// means treat as success; `NotLanded` means it is safe to delete staged parquet; `Inconclusive`
    /// means the refresh itself failed, so leak the parquet rather than risk deleting files a
    /// landed commit references. Bounded by the same last-resort timeout; a probe that times out
    /// is `Inconclusive` by construction, never `NotLanded`.
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
        use deltalake::kernel::Action;
        if refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await.is_err() {
            return CommitProbe::Inconclusive;
        }
        let our_paths: Vec<&str> = adds
            .iter()
            .filter_map(|a| match a {
                Action::Add(add) => Some(add.path.as_str()),
                _ => None,
            })
            .collect();
        if our_paths.is_empty() {
            return CommitProbe::NotLanded;
        }
        let guard = table_ref.read().await;
        let Ok(snap) = guard.snapshot() else {
            return CommitProbe::Inconclusive;
        };
        let active: HashSet<String> = snap.log_data().iter().map(|f| f.path().into_owned()).collect();
        if our_paths.iter().all(|p| active.contains(*p)) { CommitProbe::Landed } else { CommitProbe::NotLanded }
    }

    /// Best-effort delete of staged-but-uncommitted parquet after a terminal
    /// staged-commit failure. Those objects have no Add/Remove action in the
    /// Delta log, so VACUUM never reclaims them — abandoning them leaks files on
    /// S3 forever. Logs any path it couldn't remove so an operator can clean up.
    pub(crate) async fn cleanup_orphaned_parquet(store: &Arc<dyn object_store::ObjectStore>, adds: &[deltalake::kernel::Action]) {
        use object_store::ObjectStoreExt; // dyn-safe `delete` wrapper
        for action in adds {
            if let deltalake::kernel::Action::Add(add) = action {
                let path = object_store::path::Path::from(add.path.as_str());
                if let Err(e) = store.delete(&path).await {
                    warn!("orphaned staged parquet (manual cleanup needed): {} — delete failed: {}", add.path, e);
                }
            }
        }
    }

    /// Shared post-commit bookkeeping for staged and merge write paths.
    ///
    /// Records the version for read-after-write, swaps the shared handle (version-guarded), warms
    /// just-written files, invalidates stats, and returns the added file URIs. `projects` is every
    /// `(project_id, dirty_bins)` the commit carried. Per-project work runs once per entry; table-wide
    /// work runs once for the commit.
    #[allow(clippy::too_many_arguments)]
    async fn record_committed_write(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, new_table: DeltaTable, pre_uris: &HashSet<String>,
        warm: bool,
    ) -> Vec<String> {
        // Jitter anchor + logging identity: any member of the physical group is
        // equivalent (they all commit to the same log).
        let project_id = projects.first().map(|(p, _)| *p).unwrap_or("");
        let committed_version = new_table.version();
        if let Some(version) = committed_version {
            let mut versions = self.last_written_versions.write().await;
            for (project, _) in projects {
                versions.insert(table_key(project, table_name), version);
            }
            debug!("Stored last written version for {}/{} (+{} coalesced): {}", project_id, table_name, projects.len().saturating_sub(1), version);
        } else {
            debug!("WARNING: No version available after write for {}/{}", project_id, table_name);
        }
        let added: Vec<String> = new_table.get_file_uris().map(|it| it.filter(|u| !pre_uris.contains(u)).collect()).unwrap_or_default();
        // Capture the store off the committed handle so the warm task never
        // re-resolves the table (a possible PG roundtrip + Delta state reload).
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
        // Freshly-flushed files are queried next; warm them now (repeat queries
        // measured ~300 ms cold vs 8 ms warm on R2). Gated on `warm` (only the
        // BufferedWriteLayer flush path sets it): direct inserts — tests, tools
        // — must not spawn detached warm tasks whose in-flight connections
        // outlive a short-lived runtime and poison the shared client pool.
        if warm {
            // Influx-oracle ordering: the MemBuffer prefix drains right after
            // this returns (`settle_flushed_group`), so on the flush path we
            // confirm the new files are cached BEFORE that handoff — a detached
            // warm loses the race and the next dashboard query pays an R2
            // first-byte per fresh file. Bounded + best-effort: it can never
            // fail the commit. Same warm path either way — only WHEN it returns
            // differs.
            let warm_added = added.clone();
            if self.object_store_cache.is_some() {
                // Establish header/footer coverage before the MemBuffer drains.
                // Full bodies are warmed by the normal detached path below;
                // the confirm must never make flush durability depend on R2.
                self.warm_cache_for_uris(warm_store.clone(), warm_table_uri.clone(), warm_added.clone(), Some(crate::config::CACHE_CONFIRM_TIMEOUT), false)
                    .await;
            }
            let db = self.clone();
            let shutdown = self.maintenance_shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = db.warm_cache_for_uris(warm_store, warm_table_uri, warm_added, None, true) => {}
                }
            });
        }
        for (project, dirty_bins) in projects {
            self.statistics_extractor.invalidate(project, table_name).await;
            for (date, bin) in *dirty_bins {
                self.enqueue_dirty_bin(project, table_name, date, *bin);
            }
        }
        debug!("Invalidated statistics cache after write to {}/{}", project_id, table_name);
        // Periodic reconcile, OFF the flush path: every Nth commit (offset per
        // table so tables with uniform write rates don't all rebuild at once)
        // rebuild the file list from S3 truth in the background. This bounds any
        // incremental-replay drift without blocking the WAL cursor, and runs on
        // a detached clone so it never touches `added` (tantivy coverage) or the
        // persisted snapshot — both already captured from the committed state.
        let reconcile_n = self.config.maintenance.timefusion_snapshot_reconcile_commits;
        if self.config.maintenance.timefusion_incremental_snapshot
            && reconcile_n > 0
            && committed_version.is_some_and(|v| (v + Self::reconcile_offset(project_id, table_name, reconcile_n)).is_multiple_of(reconcile_n))
        {
            let (table_ref, shutdown) = (table_ref.clone(), self.maintenance_shutdown.clone());
            let (project_id, table_name) = table_key(project_id, table_name);
            tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = Self::reconcile_snapshot(&table_ref, &project_id, &table_name) => {}
                }
            });
        }
        added
    }

    /// Stable per-table offset into the reconcile cycle so tables committing in
    /// lockstep don't all hit their `% reconcile_n == 0` boundary together.
    fn reconcile_offset(project_id: &str, table_name: &str, reconcile_n: u64) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut h = DefaultHasher::new();
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
    /// `max(local, delta)` per shard.
    ///
    /// Closes the crash-mid-flush window where Delta committed but the watermark advance did not
    /// finish, so restart does not replay entries already in Delta. Must run before
    /// `recover_from_wal`. Best-effort: failures are logged and skipped, so this cannot make recovery
    /// worse than at-least-once.
    pub async fn derive_wal_cursors_from_delta(
        &self, wal: &crate::write::wal::WalManager, layer: Option<&crate::write::BufferedWriteLayer>,
    ) -> anyhow::Result<usize> {
        use futures::stream::{self, StreamExt};

        // Group logical WAL topics by physical Delta log. Default-storage
        // projects share one unified table, so opening and scanning that table
        // once per project made a dirty boot pay the same remote snapshot load
        // dozens of times. Custom-storage topics retain their isolated group.
        let custom = self.custom_storage_keys().await;
        let mut physical: HashMap<(String, String), Vec<(String, String)>> = HashMap::new();
        for (project_id, table_name) in wal.list_topic_pairs() {
            let physical_project = if custom.contains(&(project_id.clone(), table_name.clone())) { project_id.clone() } else { String::new() };
            physical.entry((physical_project, table_name.clone())).or_default().push((project_id, table_name));
        }
        let totals: Vec<usize> = stream::iter(physical.into_values())
            .map(|topics| async move { self.derive_wal_cursors_for_physical_table(wal, topics, layer).await.unwrap_or(0) })
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

        let mut total_advanced = 0;
        for (project_id, table_name) in topics {
            // Same scan, second (independent) reading: the batch-set identities
            // these commits contain. Feeds ONLY the flush-time decline — never
            // the cursor advance below, which stays governed by the conservative
            // watermark. See `LANDED_DIGESTS_KEY`.
            if self.config.buffer.landed_skip_enabled()
                && let Some(layer) = layer
            {
                let digests: Vec<crate::write::LandedDigest> = commits.iter().flat_map(|ci| parse_landed_digests_from_json(&ci.info, &project_id, &table_name)).collect();
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
            total_advanced += advanced;
        }
        Ok(total_advanced)
    }
}
