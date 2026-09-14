//! OPTIMIZE / compaction: Z-order, hot-tail, sealed-partition, and dedup rewrites.
use super::*;

impl Database {
    /// Optimizes recent Delta partitions for time-range reads.
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, _target_size: Option<i64>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let window_hours = self.config.maintenance.timefusion_optimize_window_hours.max(1);

        let table_clone = { table_ref.read().await.clone() };

        let now = Utc::now();
        let today = now.date_naive();
        let num_days = (window_hours / 24).max(1);
        // Cold consolidation owns sealed partitions older than
        // `cold_optimize_after_days`; excluding them here keeps the two tiers
        // from rewriting each other's output back and forth.
        let after_days = self.config.parquet.cold_optimize_after_days();
        // Light optimization owns today's event-time-disjoint runs.
        let skip_today = self.config.maintenance.timefusion_light_optimize_enabled;
        let window_dates: Vec<chrono::NaiveDate> = (0..=num_days)
            .map(|days_ago| (now - chrono::Duration::days(days_ago as i64)).date_naive())
            .filter(|d| !(Self::date_is_cold(today, *d, after_days) || skip_today && *d == today))
            .collect();

        let all_uris: Vec<String> = file_uris(&table_clone);
        // Version the `all_uris`/`pre_uris` snapshot describes; the tantivy
        // carry-forward below is only sound when the optimize commit is the ONLY
        // commit since it (see `sole_commit`).
        let pre_version = table_clone.version();
        let table_url = table_clone.table_url().to_string();
        let current = Self::filesets_for_dates(&all_uris, &window_dates);

        // Keep pre-state outside OCC retries; only a successful commit changes it.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let pre_uris: Option<HashSet<String>> = track_files.then(|| all_uris.into_iter().collect());

        // Keep the active partition at the light-compaction target. A single
        // day-sized file would make 1h and 3h predicates select the same file
        // even when timestamp ordering makes their row groups disjoint.
        let target_size = if window_dates.contains(&today) {
            self.config.maintenance.timefusion_light_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        };

        // delta-rs ZOrder has no idempotence guard: it rewrites every file in the
        // selected partitions on every run. Skip any partition whose live file set
        // is identical to the last successful optimize; `today` always runs.
        let kept_dates: Vec<chrono::NaiveDate> = {
            let guard = self.zorder_filesets.read().await;
            let prev = guard.get(&table_url);
            window_dates
                .iter()
                .filter(|d| current.get(*d).is_some_and(|cur| !cur.is_empty() && (**d == today || prev.and_then(|m| m.get(*d)).is_none_or(|p| p != cur))))
                .copied()
                .collect()
        };
        let skipped = window_dates.len().saturating_sub(kept_dates.len());

        if kept_dates.is_empty() {
            info!("optimize: table={} all {} window partitions unchanged since last run — skipping (cache churn avoided)", table_name, window_dates.len());
            crate::observability::record_optimize_partitions(0, skipped as u64);
            return Ok(());
        }

        info!(
            "Starting optimize (sort): table={} rewriting {} of {} window partitions, skipping {} unchanged (last {}h)",
            table_name,
            kept_dates.len(),
            window_dates.len(),
            skipped,
            window_hours
        );

        let partition_filters: Vec<PartitionFilter> =
            kept_dates.iter().filter_map(|d| PartitionFilter::try_from(("date", "=", d.to_string().as_str())).ok()).collect();

        let schema = schema_or_default(table_name);
        // Sorting keeps rewritten files timestamp-local, so short ranges can
        // prune whole files and row groups.
        let (optimize_type, declare_sorted) = choose_optimize_type(schema, false, self.config.maintenance.timefusion_optimize_sort_by);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        // SortBy materializes large Arrow buffers, so in-server bins are serial.
        let optimize_concurrency = if declare_sorted { 1 } else { self.config.derived.optimize_merge_tasks() };

        // Hold the rewrite permit only across optimize: stacking this
        // materializing rewrite with dedup exhausts the cgroup memory limit.
        const MAX_RETRIES: usize = 4;
        let optimize_result: Result<_> = {
            let mut attempt = 0;
            loop {
                if attempt > 0 {
                    tokio::time::sleep(occ_backoff(attempt - 1)).await;
                    if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                        break Err(anyhow::anyhow!("optimize refresh before retry failed: {e}"));
                    }
                }
                let table_clone = { table_ref.read().await.clone() };
                let result = {
                    let _rewrite_permit =
                        self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
                    table_clone
                        .optimize()
                        .with_filters(&partition_filters)
                        .with_type(optimize_type.clone())
                        .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::<u64>::MIN))
                        .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                        .with_max_concurrent_tasks(optimize_concurrency)
                        .with_writer_properties(writer_properties.clone())
                        .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
                        .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                        // Avoids the BinaryView read for Variant columns: delta-rs's
                        // internal session defaults to schema_force_view_types=true.
                        .with_session_state(Arc::new(self.maintenance_session_state()))
                        .await
                };
                match result {
                    Ok(result) => break Ok(result),
                    Err(e) if is_occ_conflict_err(&e.to_string()) && attempt + 1 < MAX_RETRIES => {
                        crate::observability::record_optimize_conflict();
                        attempt += 1;
                        warn!("Optimize OCC conflict for table={} (attempt {}/{}), refreshing + retrying: {}", table_name, attempt, MAX_RETRIES, e);
                    }
                    Err(e) => break Err(e.into()),
                }
            }
        };

        match optimize_result {
            Ok((new_table, metrics)) => {
                // Record the post-commit file set for the partitions we rewrote so
                // the next run can skip them. Must happen before the min_files
                // early-return: delta-rs has already committed by this point.
                let new_sets = Self::filesets_for_dates(&file_uris::<Vec<String>>(&new_table), &kept_dates);
                self.zorder_filesets.write().await.entry(table_url).or_default().extend(new_sets);
                crate::observability::record_optimize_partitions(kept_dates.len() as u64, skipped as u64);

                let min_files = self.config.maintenance.timefusion_compact_min_files;
                if metrics.total_considered_files < min_files {
                    debug!("Skipping optimization commit: {} files < min threshold {}", metrics.total_considered_files, min_files);
                    return Ok(());
                }
                let duration = start_time.elapsed();
                info!(
                    "Optimization completed in {:?}: {} files removed, {} files added, {} partitions optimized, {} total files considered, {} files skipped",
                    duration,
                    metrics.num_files_removed,
                    metrics.num_files_added,
                    metrics.partitions_optimized,
                    metrics.total_considered_files,
                    metrics.total_files_skipped
                );
                if metrics.num_files_removed > 0 {
                    info!("Optimization compression ratio: {:.2}x", metrics.num_files_removed as f64 / metrics.num_files_added as f64);
                }
                // `added` below is `live_uris - pre_uris`, i.e. everything that
                // appeared while optimize ran — not necessarily what optimize
                // wrote (a concurrent flush can land parquet in that window).
                // Carrying tantivy coverage onto such a file would mark it covered
                // when no index has seen its rows, which the read path cannot
                // tolerate; exactly one version of movement proves otherwise.
                let sole_commit = matches!((pre_version, new_table.version()), (Some(before), Some(after)) if after == before + 1);
                let live_uris = self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &[]).await;
                // Tantivy compaction reindex + GC. Order matters: build indexes for
                // the compaction's OUTPUT files first, then GC the inputs' entries,
                // so window coverage never regresses. Best-effort: errors are
                // logged; the coverage gate keeps queries correct.
                if let Some(svc) = self.tantivy_indexer().cloned()
                    && svc.config.is_table_indexed(table_name)
                {
                    use crate::tantivy::search::{parquet_rel_of_uri, project_id_of_uri};
                    let delta_store = { table_ref.read().await.log_store().object_store(None) };
                    let added: Vec<(String, String, String)> = live_uris
                        .iter()
                        // `None` (file tracking off) behaves as the empty pre-set:
                        // every live parquet is treated as new.
                        .filter(|u| !pre_uris.as_ref().is_some_and(|p| p.contains(*u)) && u.ends_with(".parquet"))
                        .filter_map(|u| Some((project_id_of_uri(u)?.to_string(), parquet_rel_of_uri(u)?.to_string(), u.clone())))
                        .collect();
                    // Carry coverage forward first: a rewrite's output holds its
                    // inputs' rows under the same ids, so when every input was
                    // already covered this is a manifest edit instead of a rebuild.
                    // Files still uncovered afterwards fall through to the rebuild.
                    let removed_by_pid: HashMap<String, Vec<String>> = pre_uris
                        .as_ref()
                        .map(|pre| {
                            pre.iter()
                                .filter(|u| !live_uris.contains(*u) && u.ends_with(".parquet"))
                                .filter_map(|u| Some((project_id_of_uri(u)?.to_string(), u.clone())))
                                .into_group_map()
                        })
                        .unwrap_or_default();
                    // `carry_forward_after_compaction` applies to a project's whole
                    // output set or to none of it, so its verdict is all this needs.
                    let mut carried: HashSet<String> = HashSet::new();
                    for (pid, removed) in removed_by_pid.iter().filter(|_| sole_commit) {
                        let for_pid: Vec<String> = added.iter().filter(|(p, _, _)| p == pid).map(|(_, _, uri)| uri.clone()).collect();
                        match svc.carry_forward_after_compaction(table_name, pid, removed, &for_pid).await {
                            Ok(true) => carried.extend(for_pid),
                            Ok(false) => {}
                            Err(e) => warn!("tantivy carry-forward failed table={} project={}: {}", table_name, pid, e),
                        }
                    }
                    let added: Vec<_> = added.into_iter().filter(|(_, _, uri)| !carried.contains(uri)).collect();
                    let table_owned = table_name.to_string();
                    let (built, reindex_errs) = futures::stream::iter(added.into_iter().map(|(pid, rel, uri)| {
                        let (svc, store, table) = (svc.clone(), delta_store.clone(), table_owned.clone());
                        async move { svc.build_index_for_file(&table, &pid, &rel, &uri, store).await }
                    }))
                    .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1))
                    .fold((0usize, 0usize), |(built, errs), r| async move {
                        match r {
                            Ok(()) => (built + 1, errs),
                            Err(e) => {
                                warn!("tantivy post-optimize reindex failed for table={}: {}", table_name, e);
                                (built, errs + 1)
                            }
                        }
                    })
                    .await;
                    if built > 0 || reindex_errs > 0 || !carried.is_empty() {
                        info!("tantivy post-optimize reindex: table={} built={} carried_forward={} errors={}", table_name, built, carried.len(), reindex_errs);
                    }
                }
                // Drop sidecar index entries for files rewritten away.
                if let Some(svc) = self.tantivy_indexer().cloned() {
                    // Manifests are keyed by the project uuid taken from the parquet
                    // URI at build time, so enumerate them rather than guessing.
                    let project_ids = crate::tantivy::list_manifest_projects(svc.object_store.as_ref(), table_name).await.unwrap_or_else(|e| {
                        warn!("tantivy gc: manifest enumeration failed for {}: {}", table_name, e);
                        Vec::new()
                    });
                    for pid in project_ids {
                        match svc.gc_after_compaction(table_name, &pid, &live_uris).await {
                            Ok(report) if report.entries_removed > 0 => {
                                info!(
                                    "tantivy gc: project={} table={} removed={} kept={} blobs_deleted={}",
                                    pid, table_name, report.entries_removed, report.kept, report.blobs_deleted
                                );
                            }
                            Ok(_) => {}
                            Err(e) => warn!("tantivy gc failed for project={} table={}: {}", pid, table_name, e),
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                if is_occ_conflict_err(&e.to_string()) {
                    crate::observability::record_optimize_conflict();
                }
                crate::observability::record_optimize_failed();
                error!("Optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Table optimization failed: {}", e))
            }
        }
    }

    /// Group live file URIs by their `date=YYYY-MM-DD` Hive partition, for the
    /// given dates only. URIs not matching any of `dates` are ignored. Every
    /// requested date gets an entry (possibly empty) so the idempotence guard
    /// can tell "no files" from "not looked at".
    pub(crate) fn filesets_for_dates(uris: &[String], dates: &[chrono::NaiveDate]) -> HashMap<chrono::NaiveDate, HashSet<String>> {
        dates
            .iter()
            .map(|d| {
                let marker = format!("date={d}");
                (*d, uris.iter().filter(|uri| uri.contains(&marker)).cloned().collect())
            })
            .collect()
    }

    /// Project IDs with live files in one hot `(project_id, date)` partition.
    /// A light optimize must use both partition predicates: filtering by `date`
    /// alone conflicts with every project's append to the active day.
    pub(crate) fn hot_project_ids(uris: &[String], date: chrono::NaiveDate) -> Vec<String> {
        let date_marker = format!("/date={date}/");
        let counts = uris
            .iter()
            .filter(|uri| uri.contains(&date_marker))
            .filter_map(|uri| path_partition_value(uri, "project_id"))
            .filter(|project_id| !project_id.is_empty())
            .counts();
        // Most-fragmented partition first: it's the one whose recent-window
        // queries open the most files, so it benefits most from an early tick.
        counts
            .into_iter()
            .sorted_by(|(a, a_count), (b, b_count)| b_count.cmp(a_count).then_with(|| a.cmp(b)))
            .map(|(project_id, _)| project_id.to_owned())
            .collect()
    }

    /// Select the specific files a light optimize should bin-pack.
    ///
    /// Rewriting the whole `date=today` partition would record a read predicate spanning the live
    /// tail, so every concurrent ingest flush trips the OCC checker. Instead pick only
    /// already-flushed small files up to `target_size`, plus at most one existing sorted run to
    /// merge into, and hand that exact set to `with_binned_files`.
    ///
    /// `sorted_run_cap` bounds which already-tagged sorted runs are re-admitted to packing: the
    /// cold tier passes `i64::MAX`, the hot tier `target/4` so a busy project does not accrue one
    /// run per tick. Files >= 7/8 target are excluded as converged — re-selecting one alone would
    /// rewrite it 1→1 forever.
    async fn light_optimize_tail(
        table: &DeltaTable, filters: &[PartitionFilter], target_size: i64, min_files: usize, sorted_run_cap: i64,
    ) -> Result<Vec<String>> {
        let adds: Vec<_> = table.get_active_add_actions_by_partitions(filters).try_collect::<Vec<_>>().await?;
        let tail: Vec<TailAdd> = adds
            .iter()
            // A DV-bearing file passes even at target size: it is not converged
            // until rewritten DV-free (pushdown is disabled while a DV is present).
            .filter(|add| add.size() < target_size.max(1) || add.deletion_vector_descriptor().is_some())
            .map(|add| {
                TailAdd::from_stats(
                    add.path().to_string(),
                    add.size(),
                    is_sorted_run(&add.tags()),
                    add.deletion_vector_descriptor().is_some(),
                    add.stats().as_deref(),
                )
            })
            .collect();
        Ok(select_tail_bin(&tail, target_size, min_files, sorted_run_cap, seal_micros_now(), TailPass::Pack, pack_size_ratio(), pack_value_floor()))
    }

    /// Plan one hot-optimize bin per hot project for `date=today` in a single snapshot walk.
    ///
    /// Skips converged and over-cap files by size/tag before touching their stats JSON, so parsing
    /// is O(live tail), not O(active files). Bins are ordered by compaction debt. `repair_dates` are
    /// sealed dates scanned for footer repair only; re-binning them would rewrite history every
    /// tick, so only unsorted files are admitted.
    pub(crate) fn select_all_hot_bins(
        table: &DeltaTable, schema: &crate::schema::TableSchema, today_str: &str, policy: &HotBinPolicy<'_>,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let date_marker = format!("date={today_str}/");
        let repair_markers: Vec<String> = policy.repair_dates.iter().map(|d| format!("date={d}/")).collect();
        let seal = seal_micros_now();
        // Only a table that declares a sort order has a footer to repair.
        let repairable = !schema.sorting_columns.is_empty();
        let per_project = table
            .snapshot()?
            .log_data()
            .iter()
            // Tag-first: every exclusion below is pure metadata, so an excluded
            // file never reaches the stats parse.
            .filter_map(|file| {
                let (size, sorted_run) = (file.size(), is_sorted_run(&file.tags()));
                hot_bin_admits(&file.path(), &date_marker, &repair_markers, size, sorted_run, repairable, policy).then_some(())?;
                let path = file.path();
                let project_id = path_partition_value(&path, "project_id").filter(|p| !p.is_empty()).map(str::to_owned)?;
                Some((
                    project_id,
                    TailAdd::from_stats(path.into_owned(), size, sorted_run, file.deletion_vector_descriptor().is_some(), file.stats().as_deref()),
                ))
            })
            .into_group_map();
        let planned = per_project
            .into_iter()
            .map(|(project_id, adds)| {
                let debt = adds.len();
                (
                    project_id,
                    select_tail_bin(
                        &adds,
                        policy.target_size,
                        policy.min_files,
                        policy.sorted_run_cap,
                        seal,
                        policy.pass,
                        pack_size_ratio(),
                        pack_value_floor(),
                    ),
                    debt,
                )
            })
            .filter(|(_, bin, _)| !bin.is_empty())
            .collect_vec();
        // Packing goes MOST-fragmented first (that partition opens the most files
        // per query). Repair inverts it to SHORTEST-JOB-FIRST: a repair backlog is
        // finite per project, so the goal is to finish projects. A project with
        // hundreds of candidates cannot be finished in any tick and would hold the
        // narrow repair slots for the whole pass while everyone waits at the barrier.
        Ok(planned
            .into_iter()
            .sorted_by(|a, b| {
                match policy.pass {
                    TailPass::Pack => b.2.cmp(&a.2),
                    TailPass::Repair => a.2.cmp(&b.2),
                }
                .then_with(|| a.0.cmp(&b.0))
            })
            .map(|(project_id, bin, _)| (project_id, bin))
            .collect())
    }

    /// `[min, max]` event time (micros) of a file from its parsed Add stats.
    /// Timestamp stats serialize as RFC3339 strings (epoch numbers accepted for
    /// long-typed columns). Takes the parsed `Value` so a caller that also wants
    /// `numRecords` out of the same blob parses it exactly once.
    pub(crate) fn event_time_range_from_stats(stats: &serde_json::Value) -> Option<(i64, i64)> {
        let get = |key: &str| {
            let v = &stats[key]["timestamp"];
            v.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_micros()).or_else(|| v.as_i64())
        };
        Some((get("minValues")?, get("maxValues")?))
    }

    /// Partition-ownership boundary between the warm and cold tiers: a `date` is
    /// cold-owned once it is at least `after_days` older than `today`. The warm
    /// optimize processes the complement, so the tiers never rewrite each other's
    /// output. Single source of truth for both schedulers.
    pub(crate) fn date_is_cold(today: chrono::NaiveDate, date: chrono::NaiveDate, after_days: u64) -> bool {
        (today - date).num_days() >= after_days as i64
    }

    /// Compacted-file target by partition age: sealed days consolidate to the
    /// larger cold target; the current day stays at the warm target so a
    /// still-filling partition is not rewritten to the cold target repeatedly.
    fn optimize_target_for_date(&self, date: chrono::NaiveDate) -> i64 {
        if Self::date_is_cold(Utc::now().date_naive(), date, self.config.parquet.cold_optimize_after_days()) {
            self.config.parquet.timefusion_cold_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        }
    }

    /// Compact a single `date=` partition by bin-packing its small files
    /// (`Compact`, not Z-order — a pure row-group merge that preserves
    /// Variant/Binary column bytes). Backs the `OPTIMIZE <table> WHERE date = '...'`
    /// pgwire command and the `optimize` CLI subcommand. Target size scales with
    /// partition age. Commits once; returns (removed, added).
    pub async fn compact_date(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>,
    ) -> Result<(u64, u64)> {
        self.compact_date_concurrent(table_ref, table_name, date, project_id, None).await
    }

    /// `compact_date` with an explicit bin concurrency (off-box CLI
    /// `--concurrency N`); `None` keeps the in-server default.
    ///
    /// A merge holds ~target-sized output buffers per task, so concurrency ×
    /// target size bounds peak memory — keep it low on a memory-tight instance.
    pub async fn compact_date_concurrent(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>, concurrency: Option<usize>,
    ) -> Result<(u64, u64)> {
        let max_concurrent = concurrency.unwrap_or_else(|| self.config.derived.optimize_merge_tasks()).max(1);
        let target_size = self.optimize_target_for_date(date);
        let schema = schema_or_default(table_name);
        let mut partition_filters = vec![PartitionFilter::try_from(("date", "=", date.to_string().as_str()))?];
        // Scope to one tenant when asked: a whole date spans every project's files
        // and may not fit in-process; one (project, date) partition does.
        if let Some(pid) = project_id {
            partition_filters.push(PartitionFilter::try_from(("project_id", "=", pid))?);
        }
        // Retry OCC/transient S3 errors; reset the no-progress budget only when
        // committed bins reduce the scoped file count.
        const MAX_ATTEMPTS: usize = 4;
        const TOTAL_ATTEMPTS: usize = 32;
        // Pre-state file set for the warm/evict diff, scoped to the partition being
        // compacted: `optimize().with_filters()` can only add/remove files under
        // these markers. `None` when neither warm- nor evict-after-compaction is on.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let scope: Vec<String> = std::iter::once(format!("date={date}/")).chain(project_id.map(|pid| format!("project_id={pid}/"))).collect();
        let scope: Vec<&str> = scope.iter().map(String::as_str).collect();
        let scoped = scoped_file_uris(&*table_ref.read().await, &scope);
        let mut scope_files = scoped.len();
        let pre_uris: Option<HashSet<String>> = track_files.then(|| scoped.into_iter().collect());
        let (mut attempt, mut total_attempts) = (0usize, 0usize);
        loop {
            // The snapshot is refreshed in the Err arm (needed there anyway for
            // the progress check), so every retry re-plans against fresh state.
            let table_clone = { table_ref.read().await.clone() };
            // SortBy declares a footer order for ordering pushdown (plain Compact
            // concatenates → declare false). Over already-sorted files it collapses
            // to a streaming SortPreservingMergeExec; only a partition holding
            // legacy pre-sort files pays a one-time blocking sort, which is why the
            // SortBy path is forced to concurrency 1 below.
            //
            // `timefusion_compact_dedup_merge` upgrades SortBy to SortByDedup so the
            // merge also collapses merge-on-read versions, keeping the greatest
            // `dedup_tiebreak` (appended DESC NULLS LAST, so a NULL tiebreak always
            // loses). This REQUIRES the sort to lead with the dedup keys, so all
            // versions of a key are consecutive. Tombstones survive because >= 1 row
            // per key is always emitted.
            let (optimize_type, declare_sorted) = if self.config.maintenance.timefusion_compact_dedup_merge {
                consolidate_optimize_type(schema, self.config.maintenance.timefusion_optimize_sort_by)
            } else {
                choose_optimize_type(schema, false, self.config.maintenance.timefusion_optimize_sort_by)
            };
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
            // Serialise SortBy at in-server concurrency; explicit off-box
            // concurrency opts into parallel transition sorts.
            let sort_concurrency = if declare_sorted && max_concurrent <= self.config.derived.optimize_merge_tasks() { 1 } else { max_concurrent };
            let result = table_clone
                .optimize()
                .with_filters(&partition_filters)
                .with_type(optimize_type)
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::<u64>::MIN))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(sort_concurrency)
                .with_writer_properties(writer_properties)
                // Short interval banks incremental commits: bins run serially on the
                // SortBy path, so an OCC loss costs one bin, not the whole partition.
                .with_min_commit_interval(tokio::time::Duration::from_secs(2 * 60))
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                .with_session_state(Arc::new(self.maintenance_session_state()))
                .await;
            match result {
                Ok((new_table, metrics)) => {
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &scope).await;
                    info!("compact date={date} table={table_name}: {} files removed, {} files added", metrics.num_files_removed, metrics.num_files_added);
                    return Ok((metrics.num_files_removed, metrics.num_files_added));
                }
                Err(e) => {
                    let msg = e.to_string();
                    let (occ, s3) = (is_occ_conflict_err(&msg), is_transient_s3_err(&msg));
                    total_attempts += 1;
                    // A failed attempt whose banked bin commits shrank the partition
                    // resets the no-progress budget; needs a fresh snapshot first.
                    if (occ || s3) && total_attempts < TOTAL_ATTEMPTS {
                        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
                        let now_files = scoped_file_uris(&*table_ref.read().await, &scope).len();
                        if now_files < scope_files {
                            scope_files = now_files;
                            attempt = 0;
                        } else {
                            attempt += 1;
                        }
                        if attempt < MAX_ATTEMPTS {
                            if occ {
                                crate::observability::record_optimize_conflict();
                                warn!(
                                    "compact date={date}: OCC conflict (no-progress attempt {attempt}/{MAX_ATTEMPTS}, total {total_attempts}), refreshing + retrying: {e}"
                                );
                                tokio::time::sleep(occ_backoff(attempt.max(1) - 1)).await;
                            } else {
                                warn!(
                                    "compact date={date}: transient S3 error (no-progress attempt {attempt}/{MAX_ATTEMPTS}, total {total_attempts}), backing off + retrying: {e}"
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(2 * attempt.max(1) as u64)).await;
                            }
                            continue;
                        }
                    }
                    if occ {
                        crate::observability::record_optimize_conflict();
                    }
                    crate::observability::record_optimize_failed();
                    return Err(anyhow::anyhow!("compact date={date} table={table_name} failed: {e}"));
                }
            }
        }
    }

    /// Distinct `date=YYYY-MM-DD` partitions present in the live file set,
    /// ascending. Drives the CLI/pgwire "compact old partitions" loop.
    pub async fn partition_dates(&self, table_ref: &Arc<RwLock<DeltaTable>>) -> Result<Vec<chrono::NaiveDate>> {
        let uris: Vec<String> = { file_uris(&*table_ref.read().await) };
        let dates: BTreeSet<chrono::NaiveDate> = uris
            .iter()
            .filter_map(|uri| {
                let tail = &uri[uri.find("date=")? + 5..];
                tail.get(..10).unwrap_or(tail).parse().ok()
            })
            .collect();
        Ok(dates.into_iter().collect())
    }

    /// Projects present in `date`'s live file set, most-fragmented first.
    /// Drives the CLI's per-project consolidate/dedup loops.
    pub async fn partition_projects(&self, table_ref: &Arc<RwLock<DeltaTable>>, date: chrono::NaiveDate) -> Result<Vec<String>> {
        let uris: Vec<String> = { file_uris(&*table_ref.read().await) };
        Ok(Self::hot_project_ids(&uris, date))
    }

    /// Read one object's parquet footer and extract from it, relativizing the absolute `uri`
    /// against the store's root `table_prefix`. `Ok(None)` if unreadable; `Err` is a log-ready reason.
    async fn probe_footer<T>(
        object_store: &Arc<dyn object_store::ObjectStore>, table_prefix: &str, uri: &str,
        extract: impl FnOnce(&deltalake::datafusion::parquet::file::metadata::ParquetMetaData) -> Option<T>,
    ) -> Result<Option<T>, String> {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};
        let Some(rel) = uri.strip_prefix(table_prefix).map(|s| s.strip_prefix('/').unwrap_or(s)) else {
            return Err(format!("could not relativize {uri} against {table_prefix}"));
        };
        let path = OsPath::from(rel);
        // Pass our own `path`, not `meta.location`: the latter is bucket-relative and would double-prefix.
        let meta = object_store.head(&path).await.map_err(|e| format!("head failed for {uri}: {e}"))?;
        let mut reader = ParquetObjectReader::new(object_store.clone(), path).with_file_size(meta.size);
        Ok(reader.get_metadata(None).await.ok().and_then(|pq| extract(&pq)))
    }

    /// Rewrite a date partition at a higher ZSTD level using Z-order (or Compact if no
    /// z-order columns).
    ///
    /// Skips partitions whose probe file already advertises a tier >= `target_level` in its Parquet
    /// footer. Probes one file per partition, since a fully recompressed partition shares one tier.
    pub async fn recompress_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_level: i32, project: Option<&str>,
    ) -> Result<RecompressOutcome> {
        let date_str = date.to_string();
        if project.is_some() {
            // Scoped `replace_where` can deadlock; reject it before reading the table.
            anyhow::bail!("recompress --project is disabled: scoped replace_where deadlocks; re-run without --project");
        }
        let date_marker = format!("date={date_str}");

        let (uris, log_store, table_uri) = {
            let table = table_ref.read().await;
            let uris: Vec<String> = table.get_file_uris()?.filter(|u| u.contains(&date_marker)).collect();
            (uris, table.log_store(), table.table_url().to_string())
        };
        if uris.is_empty() {
            debug!("recompress: no files in partition date={} for table={}", date_str, table_name);
            return Ok(RecompressOutcome::Skipped("no files in partition"));
        }

        // Whole-partition rewrite: pool-invisible Arrow materialization, so hold a
        // maintenance-rewrite permit. Acquired after the early-out so no-ops are free.
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;

        // Probe one file's footer KV metadata. delta-rs URIs are absolute and the
        // object_store is rooted at table_uri, so strip that prefix — including the
        // `?endpoint=...` query string `table_url()` may carry but URIs do not.
        let probe_uri = &uris[0];
        let table_prefix = table_uri.split('?').next().unwrap_or(&table_uri).trim_end_matches('/');
        let object_store = log_store.object_store(None);
        let probe_tier = Self::probe_footer(&object_store, table_prefix, probe_uri, |pq| {
            pq.file_metadata()
                .key_value_metadata()
                .and_then(|kvs| kvs.iter().find(|kv| kv.key == COMPRESSION_TIER_KEY).and_then(|kv| kv.value.as_ref()).and_then(|v| v.parse::<i32>().ok()))
        })
        .await
        .unwrap_or_else(|reason| {
            warn!("recompress probe: {}; rewriting anyway", reason);
            None
        });

        // A partition holding an UNSORTED file is never "done", whatever its
        // compression tier — a tier-qualified file with an unsorted footer voids the
        // declared ordering for every scan. Sortedness vetoes the tier-probe skip.
        let declares_order = get_schema(table_name).is_some_and(|s| !s.sorting_columns.is_empty());
        let any_unsorted = declares_order
            && futures::stream::iter(&uris)
                .any(|uri| {
                    let object_store = object_store.clone();
                    // An unreadable footer is not evidence of sortedness; say
                    // no for this file and let the tier probe decide.
                    async move {
                        Self::probe_footer(&object_store, table_prefix, uri, |pq| {
                            Some(pq.row_groups().iter().any(|rg| rg.sorting_columns().is_none_or(|sc| sc.is_empty())))
                        })
                        .await
                        .is_ok_and(|unsorted| unsorted == Some(true))
                    }
                })
                .await;

        // If probe failed or tier is unknown, fall through to rewrite — safer
        // than skipping a partition that may still be at hot tier.
        if let Some(t) = probe_tier
            && t >= target_level
            && !any_unsorted
        {
            debug!("recompress: skip date={} table={} (already at tier {})", date_str, table_name, t);
            return Ok(RecompressOutcome::Skipped("already at target tier and every footer is sorted"));
        }
        if any_unsorted {
            info!("recompress: date={} table={} has file(s) with no sorted footer — rewriting despite tier", date_str, table_name);
        }

        info!("recompress: rewriting date={} table={} at zstd={} ({} files)", date_str, table_name, target_level, uris.len());

        let schema = schema_or_default(table_name);
        let order_by = if self.config.maintenance.timefusion_optimize_sort_by { schema_order_by_clause(schema) } else { String::new() };
        let declare_sorted = !order_by.is_empty();
        let writer_properties = self.create_writer_properties(schema, target_level, declare_sorted);
        let target_size = self.config.parquet.timefusion_optimize_target_size;

        // Full-partition rewrite via a streaming `replace_where` overwrite, NOT
        // Z-order: `Compact` skips files already >= target so it cannot lift an
        // already-consolidated partition's tier, and Z-order's space-filling curve
        // scatters `timestamp` across row groups and wrecks time-range pruning.
        // `with_input_plan` streams the scan through the writer, so peak memory
        // matches a normal flush.
        let (snapshot, log_store, table_clone) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store(), table.clone())
        };
        let pre_uris: HashSet<String> = file_uris(&table_clone);

        let provider = deltalake::delta_datafusion::TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("recompress scan provider: {e}"))?;
        // Must be the delta *write* session (carries DeltaPlanner): the write wraps
        // its input in a MetricObserver node only that planner can physically plan.
        let session = build_delta_write_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env(), "8192");
        let ctx = datafusion::prelude::SessionContext::new_with_state(session);
        ctx.register_table("recompress_src", Arc::new(provider))?;
        // `date_str` is a parsed `NaiveDate`; `order_by` uses quoted identifiers.
        let input_plan = ctx.sql(&format!("SELECT * FROM recompress_src WHERE date = '{date_str}'{order_by}")).await?.into_optimized_plan()?;

        let replace_pred = format!("date = '{date_str}'");
        let write_result = table_clone
            .write(Vec::<RecordBatch>::new())
            .with_input_plan(input_plan)
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
            .with_replace_where(replace_pred.as_str())
            .with_writer_properties(writer_properties)
            .with_target_file_size(std::num::NonZero::new(target_size as u64))
            .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
            .with_session_state(Arc::new(ctx.state()))
            .await;

        match write_result {
            Ok(new_table) => {
                info!("recompress: date={} table={} rewritten at zstd={} (was {} files)", date_str, table_name, target_level, uris.len());
                // Swap + warm-added/evict-removed: a bare swap would leave the
                // rewritten files un-warmed and the tombstoned ones cached.
                self.swap_and_refresh_cache(table_ref, new_table, Some(&pre_uris), &[]).await;
                Ok(RecompressOutcome::Rewritten { files: uris.len() })
            }
            Err(e) => {
                error!("recompress failed for date={} table={}: {}", date_str, table_name, e);
                Err(anyhow::anyhow!("recompress failed: {}", e))
            }
        }
    }

    /// Leveled consolidation of one sealed `date`: per project, repeatedly select the earliest
    /// event-time slice of small files up to the cold target and rewrite it as one sorted run.
    ///
    /// Successive passes take strictly later slices, so output runs are event-time disjoint and
    /// range pruning works. Per-pass memory is bounded by one <= target sort. Converges because
    /// outputs >= 7/8 target are excluded from re-selection.
    pub async fn consolidate_date_binned(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_size: i64, only_project: Option<&str>, max_passes: usize,
    ) -> Result<()> {
        let schema = schema_or_default(table_name);
        // Deliberately NOT gated on `timefusion_optimize_sort_by`: each rewrite here
        // is bounded to one event-time bin, so it carries no whole-partition
        // external-sort hazard, and its contract is to produce disjoint sorted runs.
        let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        let date_str = date.to_string();
        let uris: Vec<String> = { file_uris(&*table_ref.read().await) };
        // Backstop against a selection that stops shrinking (e.g. a rewrite that
        // keeps losing OCC); a normal day converges in partition_bytes/target passes.
        let max_passes = max_passes.clamp(1, 128);
        for project_id in Self::hot_project_ids(&uris, date).into_iter().filter(|p| only_project.is_none_or(|only| only == p)) {
            let partition_filters =
                vec![PartitionFilter::try_from(("project_id", "=", project_id.as_str()))?, PartitionFilter::try_from(("date", "=", date_str.as_str()))?];
            for _ in 0..max_passes {
                let selected_files = {
                    let table = table_ref.read().await;
                    Self::light_optimize_tail(&table, &partition_filters, target_size, 2, i64::MAX).await?
                };
                if selected_files.is_empty() {
                    break;
                }
                self.optimize_table_light_inner(
                    table_ref,
                    table_name,
                    date,
                    &project_id,
                    &partition_filters,
                    &selected_files,
                    target_size,
                    &writer_properties,
                    optimize_type.clone(),
                    2,
                    std::time::Instant::now(),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Cross-flush dedup: collapse a `(project_id, date)` partition by `dedup_keys` and write back
    /// via `replace_where`. No-op on no dedup_keys or no duplicates.
    ///
    /// Returns `(rows_dropped, complete)`. `complete=false` means duplicate-bearing work was skipped
    /// (unsealed chunks, rewrite budget, vanished snapshot rows) — the partition must not be
    /// fingerprinted clean, or the read-side dedup skip would serve duplicates.
    ///
    /// Commits the whole partition as a SINGLE wave; the dirty-bin path instead stages with
    /// [`Self::stage_dedup_partition_range`] so one wave can span many bins.
    pub async fn dedup_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
    ) -> Result<(u64, bool)> {
        self.dedup_partition_range_limited(table_ref, table_name, project_id, date, None, None).await.map(|(dropped, complete, _)| (dropped, complete))
    }

    /// Third element: `Some(attachments)` iff every LANDED bin masked its losers in
    /// place (DV-dedup) — the `(path, dv_unique_id)` pairs this pass committed, from
    /// which certification builds the expected post-state for the DV-visibility
    /// guard. `None` for a CoW pass, keeping the dropped-rows-means-dirty rule.
    pub(crate) async fn dedup_partition_range_limited(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
        slice: Option<crate::maintenance_coordinator::TimeSlice>, limits: Option<DedupExecutionLimits>,
    ) -> Result<(u64, bool, Option<Vec<DvEntry>>)> {
        let options = DedupRangeOptions { slice, dirty_key: None, limits };
        let (units, complete) = self.stage_dedup_partition_range(table_ref, table_name, project_id, date, options).await?;
        if units.is_empty() {
            return Ok((0, complete, None));
        }
        let markers = vec![format!("date={date}/")];
        let result = self.commit_wave(table_ref, table_name, &markers, true, units, 0).await;
        let dropped = wave_dropped_rows(&result.landed);
        // Landed-only, so this counts committed duplicates and nothing else.
        // Recorded here, not at the coordinator: its incomplete branch discards
        // `dropped` even though those bins are already committed work.
        crate::observability::count_maintenance_work("Dedup", "rows_dropped", dropped);
        // CARRY THE ROLLUP WITNESS instead of letting the rewrite invalidate it.
        //
        // A rollup builds from a DEDUPLICATED read — `slice_input_sql` keeps
        // `ROW_NUMBER() OVER (PARTITION BY dedup_keys ORDER BY tiebreak DESC)
        // = 1` — and this sweep collapses the SAME `dedup_keys` keeping the
        // greatest `dedup_tiebreak`. One schema declaration drives both, so
        // dedup removes exactly the rows the build never counted: the rollup's
        // numbers are unchanged and only its PROOF moved.
        //
        // The proof is the partition's physical `num_records`, and the delta is
        // known exactly here — `dropped`, summed over LANDED bins only, so a bin
        // that failed to commit contributes nothing. Subtract it and the witness
        // is true again, with no scan and no object-store round trip.
        //
        // Prod 2026-09-13 measured this class at 18,418 `rollup_stale_shrank`
        // against 229,361 `grew`, so this is the 7.4% — worth taking because it
        // is nearly free and repairs slices already stamped, not because it is
        // the larger lever. See `2026-09-13-stop-re-rolling-deduped-partitions.md`.
        if dropped > 0 {
            self.carry_dedup_witness(table_name, project_id, &date.to_string(), dropped);
        }
        for bin in &result.landed {
            if let Some(d) = &bin.dedup {
                info!("dedup rewrite: table={} chunk=[{}] dropped={} (before={} after={})", table_name, d.label, d.dropped(), d.before, d.after);
            }
        }
        // LANDED bins only (incl. self-landed carries): a failed bin's DV was
        // never committed and must not enter the guard's expected set.
        let masked = (!result.landed.is_empty() && result.landed.iter().all(StagedBin::masked_in_place)).then(|| {
            result
                .landed
                .iter()
                .flat_map(|b| &b.adds)
                .filter_map(|a| match a {
                    deltalake::kernel::Action::Add(add) => Some((add.path.clone(), add.deletion_vector.as_ref().map(dv_identity))),
                    _ => None,
                })
                .collect()
        });
        // A unit that didn't land left its duplicates in place — the partition
        // must NOT be certified clean.
        Ok((dropped, complete && result.failed.is_empty(), masked))
    }

    /// Builds a `TableProviderBuilder` scoped to exactly `files`. `file_col`
    /// requests the synthetic file-identity column dedup rewrites key on.
    pub(crate) async fn narrow_provider(
        log_store: deltalake::logstore::LogStoreRef, snapshot: Arc<deltalake::kernel::EagerSnapshot>, files: Vec<String>, file_col: Option<&str>,
        row_index_col: Option<&str>,
    ) -> Result<Arc<dyn TableProvider>, deltalake::DeltaTableError> {
        use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};
        let mut builder =
            TableProviderBuilder::default().with_log_store(log_store).with_eager_snapshot(snapshot).with_file_selection(FileSelection::from_file_paths(files));
        if let Some(col) = file_col {
            builder = builder.with_file_column(col);
        }
        // A row-index column disables parquet predicate pushdown (positions must not
        // shift), so request it only when DV-dedup needs physical loser positions.
        if let Some(col) = row_index_col {
            builder = builder.with_row_index_column(col);
        }
        Ok(Arc::new(builder.build().await?))
    }

    /// Probe-only DataFusion context over one `(project, date)` partition's snapshot files.
    ///
    /// Bypasses `ProjectRoutingTable`: its MemBuffer union would feed in-flight rows to dedup,
    /// which would then be written to Delta on the next real flush. Restricts provider construction
    /// itself, not just the SQL scan — an unrestricted provider eagerly materializes statistics for
    /// every live file in the unified table before partition pruning.
    async fn dedup_probe_ctx(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, date_str: &str, limits: Option<DedupExecutionLimits>,
    ) -> Result<datafusion::prelude::SessionContext> {
        let (snapshot, log_store) = snapshot_and_store(table_ref).await?;
        let partition_files = dedup_partition_paths(snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
        // Probe-only provider; the rewrite builds its own per attempt from a FRESH
        // snapshot, with the synthetic source-file column.
        let provider =
            Self::narrow_provider(log_store, snapshot, partition_files, None, None).await.map_err(|e| anyhow::anyhow!("delta table provider: {e}"))?;
        // A fresh state is intentional: SessionState clones retain mutable
        // catalog/execution internals and can resolve the scan name to an older
        // eager snapshot.
        let state = limits.map_or_else(
            || build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()),
            |limits| {
                build_optimize_session_state_tuned(
                    self.config.memory.timefusion_query_partitions,
                    self.coordinator_runtime_env(),
                    Some(&limits.batch_rows.to_string()),
                    Some(UncappedSort { partitions: 1, reservation_bytes: Some(32 * 1024 * 1024) }),
                )
            },
        );
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);
        ctx.register_table(DEDUP_SCAN_NAME, provider)?;
        Ok(ctx)
    }

    /// The 10-minute duplicate probe: returns the bucket starts whose
    /// dedup-key groups have count > 1 under `filter`. Aggregates group keys only, so it is bounded
    /// by key cardinality rather than row width (a `SELECT *` here allocates outside any pool).
    async fn dup_bin_starts(ctx: &datafusion::prelude::SessionContext, filter: &str, keys_csv: &str) -> Result<Vec<chrono::NaiveDateTime>> {
        let probe = format!(
            "SELECT CAST(date_bin(INTERVAL '10 minutes', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00') AS VARCHAR) FROM \
             (SELECT \"timestamp\", count(*) AS c FROM {DEDUP_SCAN_NAME} WHERE {filter} GROUP BY {keys_csv}) AS g \
             WHERE c > 1 GROUP BY 1 ORDER BY 1"
        );
        Ok(read_string_column(crate::database::maintain::collect_watched(ctx, &probe).await?)?
            .iter()
            .filter_map(|value| {
                value.get(..19).and_then(|datetime| {
                    chrono::NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S"))
                        .ok()
                })
            })
            .collect())
    }

    /// Counts dedup keys whose versions disagree on a column declared IMMUTABLE,
    /// or `None` when the schema has nothing auditable.
    ///
    /// Immutability is enforced at plan time for UPDATE only; an INSERT is not
    /// checked, so a client re-emitting a corrected record appends a disagreeing
    /// version — and read filters on immutable columns are pushed BELOW the
    /// merge-on-read dedup precisely because they were promised not to.
    ///
    /// A group can disagree two ways, and the value term alone catches only the
    /// first: different non-null values (`MIN(c) <> MAX(c)`), or null in some
    /// versions and set in others (the shape enrichment produces), hence the
    /// two-sided `COUNT(c) > 0 AND COUNT(c) < COUNT(*)` — a column null in EVERY
    /// version has `COUNT(c) = 0` and agrees perfectly.
    ///
    /// Dedup keys are excluded (they are the grouping), as are the tiebreak and
    /// tombstone columns, which vary across versions by construction. Composite
    /// types are excluded because ordering over them is neither cheap nor
    /// meaningful.
    ///
    /// The SQL form uses `MIN(c) <> MAX(c)`, never `COUNT(DISTINCT c) > 1`: they
    /// answer the same question, but `COUNT(DISTINCT)` keeps a per-group hash set
    /// per column, which over ~150 columns is enough memory to OOM the rewrite.
    /// It is only the fallback for when the streaming collapse cannot run
    /// (`dedup_keys_lead_the_sort` false); otherwise the audit rides
    /// `RunCollapse::with_immutable_audit`. Single definition of which columns an
    /// audit compares, so SQL and streaming collapse cannot drift.
    fn immutable_audit_columns(schema: &crate::schema::TableSchema) -> Vec<String> {
        let excluded: HashSet<&str> =
            schema.dedup_keys.iter().map(String::as_str).chain(schema.dedup_tiebreak.as_deref()).chain(schema.tombstone_column.as_deref()).collect();
        schema
            .fields
            .iter()
            .filter(|field| !field.mutable && !excluded.contains(field.name.as_str()))
            .filter(|field| {
                let ty = field.data_type.to_ascii_lowercase();
                !["variant", "binary", "list", "struct", "map"].iter().any(|composite| ty.contains(composite))
            })
            .map(|field| field.name.clone())
            .collect()
    }

    fn immutable_audit_sql(schema: &crate::schema::TableSchema, scan_name: &str, rows_filter: &str) -> Option<String> {
        let columns = Self::immutable_audit_columns(schema);
        if columns.is_empty() || schema.dedup_keys.is_empty() {
            return None;
        }
        let predicates = columns
            .iter()
            .map(|name| {
                let column = crate::rollup::quoted(name);
                format!("MIN({column}) <> MAX({column}) OR (COUNT({column}) > 0 AND COUNT({column}) < COUNT(*))")
            })
            .join(" OR ");
        let keys = quoted_csv(&schema.dedup_keys);
        Some(format!("SELECT COUNT(*) FROM (SELECT {keys} FROM {scan_name} WHERE {rows_filter} GROUP BY {keys} HAVING {predicates})"))
    }

    /// Runs `sql` and pulls its first output row's `column(0)` as an `i64`.
    /// `None` if the result is empty or `column(0)` isn't an `Int64Array`.
    async fn scalar_i64(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<Option<i64>> {
        let batches = crate::database::maintain::collect_watched(ctx, sql).await?;
        Ok(batches
            .first()
            .filter(|b| b.num_rows() > 0)
            .and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>())
            .map(|a| a.value(0)))
    }

    /// The configured decode ceiling, tightened by the coordinator's execution limit when it supplied one.
    fn dedup_decoded_budget(&self, limits: Option<DedupExecutionLimits>) -> u64 {
        limits.map_or(u64::MAX, |limits| limits.max_decoded_bytes).min(self.config.maintenance.timefusion_dedup_max_decoded_bytes)
    }

    /// The largest dedup-key group under `rows_filter`, but only when that group alone would blow
    /// `decoded_budget`. Sharding can never split a key group (every copy of a key hashes to one
    /// bucket), so once one does, no shard count helps.
    async fn unshardable_group(
        ctx: &datafusion::prelude::SessionContext, rows_filter: &str, group_by: &str, bytes_per_row: u64, decoded_budget: u64,
    ) -> Result<Option<i64>> {
        let sql = format!("SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM {DEDUP_SCAN_NAME} WHERE {rows_filter} GROUP BY {group_by})");
        let max_group = Self::scalar_i64(ctx, &sql).await?.unwrap_or(0);
        Ok(((max_group.max(0) as u64).saturating_mul(bytes_per_row).saturating_mul(2) > decoded_budget).then_some(max_group))
    }

    /// Write one already-cast batch through a staging writer, flushing the
    /// completed files into `adds` once the writer's buffer reaches
    /// `max_file_bytes`. Single definition of the dedup staging file-size policy.
    async fn write_staged(
        writer: &mut deltalake::writer::RecordBatchWriter, adds: &mut Vec<deltalake::kernel::Action>, batch: RecordBatch, max_file_bytes: usize,
    ) -> Result<()> {
        use deltalake::{kernel::Action, writer::DeltaWriter};
        writer.write(batch).await.map_err(|e| anyhow::anyhow!("dedup rewrite stage: {e}"))?;
        if writer.buffer_len() >= max_file_bytes {
            adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("dedup rewrite flush: {e}"))?.into_iter().map(Action::Add));
        }
        Ok(())
    }

    /// Batch probe: classify every 10-minute bin of one `(project, date)` with a single duplicate
    /// probe, returning the bin ids that contain duplicates.
    ///
    /// A dup group shares one exact `timestamp` (it is a dedup key), so the group's bin is derived
    /// exactly. Only valid when `timestamp` is a dedup key.
    pub(crate) async fn probe_dup_bins(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date_str: &str) -> Result<HashSet<i64>> {
        let schema = schema_or_default(table_name);
        let ctx = self.dedup_probe_ctx(table_ref, project_id, date_str, None).await?;
        let filter = format!("project_id = '{}' AND date = DATE '{date_str}'", project_id.replace('\'', "''"));
        let keys_csv = quoted_csv(&schema.dedup_keys);
        Ok(Self::dup_bin_starts(&ctx, &filter, &keys_csv).await?.into_iter().map(|s| s.and_utc().timestamp_micros() / bin_micros()).collect())
    }

    /// Probe one partition/bin for duplicates and STAGE (never commit) a
    /// replacement parquet set per duplicate-bearing chunk. Returns the staged
    /// units plus `complete` — false when duplicate-bearing work was skipped
    /// (unsealed chunks, budget guards, vanished snapshot rows), which forbids
    /// certifying the partition clean.
    pub(crate) async fn stage_dedup_partition_range(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, options: DedupRangeOptions,
    ) -> Result<(Vec<StagedBin>, bool)> {
        let DedupRangeOptions { slice, dirty_key: key, limits } = options;
        let schema = schema_or_default(table_name);
        if schema.dedup_keys.is_empty() {
            return Ok((Vec::new(), true));
        }
        let date_str = date.to_string();
        let ctx = self.dedup_probe_ctx(table_ref, project_id, &date_str, limits).await?;
        let scan_name = DEDUP_SCAN_NAME;
        // Escape quotes so a caller cannot inject SQL through the partition
        // predicate; `date_str` comes from NaiveDate::to_string and is already safe.
        let safe_pid = project_id.replace('\'', "''");
        // The full partition predicate MUST stay separate from the bin probe scope:
        // `stage_dedup_chunk` removes every file the scoped chunk touches, then
        // re-reads those files with `partition_filter` so rows in adjacent bins
        // survive. Passing the bin predicate here silently drops them.
        let partition_filter = format!("project_id = '{}' AND date = DATE '{}'", safe_pid, date_str);
        let filter = if let Some(slice) = slice {
            let start = chrono::DateTime::from_timestamp_micros(slice.start_micros)
                .ok_or_else(|| anyhow::anyhow!("invalid dedup slice start {}", slice.start_micros))?;
            let end =
                chrono::DateTime::from_timestamp_micros(slice.end_micros).ok_or_else(|| anyhow::anyhow!("invalid dedup slice end {}", slice.end_micros))?;
            format!(
                "{partition_filter} AND \"timestamp\" >= TIMESTAMP '{}' AND \"timestamp\" < TIMESTAMP '{}'",
                start.format("%Y-%m-%d %H:%M:%S"),
                end.format("%Y-%m-%d %H:%M:%S")
            )
        } else {
            partition_filter.clone()
        };
        // Probe keys before materializing rows: it bounds the common no-duplicate
        // case by key cardinality rather than row width.
        let keys_csv = quoted_csv(&schema.dedup_keys);

        // Identify the buckets that actually contain duplicates. A dup group shares
        // one exact `timestamp` (it is a dedup key), so chunking by time can never
        // split a group, and it bounds materialization to one bin of one project.
        let (chunks, skipped_any): (Vec<(String, String)>, bool) = if schema.dedup_keys.iter().any(|k| k == "timestamp") {
            // Ten-minute sealed bins bound materialization and avoid racing late
            // flushes; newer duplicates are retried later.
            let sealed_before = Utc::now().naive_utc() - chrono::Duration::hours(2);
            let mut skipped_unsealed = false;
            // A bin that cannot be split further in time is instead split by the same
            // complete-key hash partitioning the rewrite uses, so no single pass
            // accumulates the whole bin's key cardinality in memory.
            let probe_shards = limits.map_or(1, |limits| limits.probe_hash_shards.max(1));
            let bucket_expr = dedup_bucket_expr(schema);
            let mut duplicate_starts = Vec::new();
            for shard in 0..probe_shards {
                let shard_filter = format!("{filter}{}", shard_bucket_pred(&bucket_expr, shard as u64, probe_shards as u64));
                duplicate_starts.extend(Self::dup_bin_starts(&ctx, &shard_filter, &keys_csv).await?);
            }
            let built: Vec<_> = duplicate_starts
                .into_iter()
                .sorted_unstable()
                .dedup()
                .filter_map(|start| {
                    let end = start + chrono::Duration::minutes(10);
                    if slice.is_none() && end > sealed_before {
                        debug!("dedup: skipping unsealed chunk starting {start} (cleared on a later sweep)");
                        skipped_unsealed = true;
                        return None;
                    }
                    let (s, e) = (start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
                    Some((
                        format!("{filter} AND \"timestamp\" >= TIMESTAMP '{s}' AND \"timestamp\" < TIMESTAMP '{e}'"),
                        // Log label only: the rewrite commits targeted Remove+Add
                        // actions, so no predicate is ever kernel-evaluated.
                        format!("project_id = '{safe_pid}' AND date = '{date_str}' AND timestamp in ['{s}', '{e}')"),
                    ))
                })
                .collect();
            (built, skipped_unsealed)
        } else {
            // No timestamp dedup key → can't chunk safely; whole-partition
            // rewrite, gated on the same any-dupes probe.
            let probe =
                format!("SELECT coalesce(sum(c - 1), 0) FROM (SELECT count(*) AS c FROM {scan_name} WHERE {filter} GROUP BY {keys_csv}) AS g WHERE c > 1");
            let dup_rows = Self::scalar_i64(&ctx, &probe).await?.unwrap_or(0);
            if dup_rows <= 0 { (Vec::new(), false) } else { (vec![(filter.clone(), format!("project_id = '{safe_pid}' AND date = '{date_str}'"))], false) }
        };
        if chunks.is_empty() {
            return Ok((Vec::new(), !skipped_any));
        }

        // `buffer_unordered` bounds tasks in flight; the rewrite semaphore bounds
        // concurrent Arrow materialization.
        let permits = self.config.derived.rewrite_permits().max(1);
        let staged: Vec<Result<BinOutcome<StagedBin>>> =
            futures::stream::iter(chunks.into_iter().map(|(chunk_filter, label)| {
                let (partition_filter, key, date_str) = (&partition_filter, key.clone(), date_str.as_str());
                async move {
                    self.stage_dedup_chunk(table_ref, table_name, project_id, schema, partition_filter, &chunk_filter, &label, date_str, key, limits).await
                }
            }))
            .buffer_unordered(permits)
            .collect()
            .await;
        let mut units = Vec::new();
        let mut all_complete = !skipped_any;
        let mut first_err = None;
        for outcome in staged {
            match outcome {
                Ok(BinOutcome::Staged(unit)) => units.push(unit),
                // Nothing was verified, so the partition stays uncertified.
                Ok(BinOutcome::Retry | BinOutcome::BudgetBusy) => all_complete = false,
                // Probe false-positive: verified duplicate-free, nothing to commit.
                Ok(BinOutcome::Converged) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            // One chunk's failure abandons the whole staging batch: delete the
            // siblings' parquet, whose Adds are in no commit and which VACUUM
            // would take days to notice.
            self.discard_bins(table_ref, &units, None).await;
            return Err(e);
        }
        Ok((units, all_complete))
    }

    /// Stage one duplicate-bearing chunk as a targeted file rewrite.
    ///
    /// Uses the provider's synthetic `DEDUP_FILE_COL` to find which files hold the chunk's rows,
    /// re-reads those files' full row sets, dedups, and writes replacement parquet. Returns
    /// `Remove(old) + Add(new)` actions for a wave commit; this function commits nothing. Explicit
    /// file actions rather than `replace_where`, because delta-rs cannot stringify typed TIMESTAMP
    /// literals for a commit predicate.
    #[allow(clippy::too_many_arguments)]
    async fn stage_dedup_chunk(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, schema: &crate::schema::TableSchema, partition_filter: &str,
        chunk_filter: &str, label: &str, date_str: &str, key: Option<DirtyBinKey>, limits: Option<DedupExecutionLimits>,
    ) -> Result<BinOutcome<StagedBin>> {
        // Writing a DV to a table without `enableDeletionVectors` is a protocol
        // violation, so fall back to copy-on-write unless the table declares it.
        if self.config.maintenance.timefusion_use_deletion_vectors
            && table_ref.read().await.snapshot().is_ok_and(|s| s.snapshot().table_properties().enable_deletion_vectors == Some(true))
        {
            return self.stage_dedup_chunk_dv(table_ref, table_name, project_id, schema, chunk_filter, label, date_str, key, limits).await;
        }
        use deltalake::{kernel::Action, writer::DeltaWriter};
        let scan_name = DEDUP_SCAN_NAME;
        // Re-plan against a fresh snapshot when concurrent rewrites invalidate
        // file mappings; `commit_wave` guards the remaining commit window.
        const MAX_REPLANS: usize = 3;
        for replan in 0..MAX_REPLANS {
            // Scan and file-mapping MUST share one snapshot — the live file set
            // diverges within seconds on the churned unified table.
            let (chunk_snapshot, chunk_log_store) = snapshot_and_store(table_ref).await?;
            let partition_files = dedup_partition_paths(chunk_snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
            let provider = Self::narrow_provider(chunk_log_store, Arc::clone(&chunk_snapshot), partition_files, Some(DEDUP_FILE_COL), None)
                .await
                .map_err(|e| anyhow::anyhow!("dedup rewrite provider: {e}"))?;
            // Sort parallelism descends on retry: the merge exec is unspillable
            // and per-partition, so a bin that exhausted the pool at the cap must
            // not be replanned at the same width.
            let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(
                limits.map_or(self.config.memory.timefusion_query_partitions, |l| l.sort_partitions),
                self.maintenance_runtime_env(),
            ));
            ctx.register_table(scan_name, provider)?;

            // 1. Which files hold the chunk's rows — ground truth from the scan.
            let files_sql = format!("SELECT DISTINCT \"{DEDUP_FILE_COL}\" FROM {scan_name} WHERE {chunk_filter}");
            let file_ids = read_string_column(crate::database::maintain::collect_watched(&ctx, &files_sql).await?)?;
            if file_ids.is_empty() {
                // Probe saw dupes but this snapshot has no rows for the chunk
                // (concurrent rewrite) — nothing verified, don't certify clean.
                return Ok(BinOutcome::Retry);
            }
            // 2. Map scan values to Add actions in the SAME snapshot.
            let targets = adds_for_file_ids(&chunk_snapshot, &file_ids, table_name);
            if targets.len() != file_ids.len() {
                warn!(
                    "dedup rewrite: mapped {}/{} files for table={} chunk=[{}] (sample scan value: {:?}), re-planning",
                    targets.len(),
                    file_ids.len(),
                    table_name,
                    label,
                    file_ids.first()
                );
                tokio::time::sleep(occ_backoff(replan)).await;
                continue;
            }

            // 3. Decide the shard count. A dedup `SELECT * … collect()` decodes to
            // Arrow OUTSIDE the memory pool, so the rewrite is split into K passes
            // bucketed by a hash of the dedup keys — every copy of a key hashes to
            // one bucket (never split), and hashing is even and NULL-safe.
            // K = ceil(estimated decoded bytes / budget).
            let rewrite_bytes: i64 = targets.iter().map(|a| a.size).sum();
            // Copied out because the per-shard closure below moves `targets`.
            let target_files = targets.len();
            // Fail closed unless the provider's full-file re-read can be checked
            // against Delta's independent row-count metadata. DV cardinality is
            // subtracted: the provider already hides those physical rows.
            let expected_live_rows = targets.iter().try_fold(0u64, |sum, add| -> Result<u64> {
                let stats = add.get_stats()?.ok_or_else(|| anyhow::anyhow!("dedup rewrite refuses target without num_records stats: {}", add.path))?;
                let rows = u64::try_from(stats.num_records).map_err(|_| anyhow::anyhow!("dedup rewrite target has negative num_records: {}", add.path))?;
                let deleted = u64::try_from(add.deletion_vector.as_ref().map_or(0, |dv| dv.cardinality))
                    .map_err(|_| anyhow::anyhow!("dedup rewrite target has negative deletion-vector cardinality: {}", add.path))?;
                let live = rows
                    .checked_sub(deleted)
                    .ok_or_else(|| anyhow::anyhow!("dedup rewrite target deletion-vector cardinality exceeds num_records: {}", add.path))?;
                sum.checked_add(live).ok_or_else(|| anyhow::anyhow!("dedup rewrite target row count overflow"))
            })?;
            let compressed_budget = self.config.maintenance.timefusion_dedup_max_rewrite_bytes;
            let inflation = self.config.maintenance.timefusion_dedup_decode_inflation.max(1);
            let decoded_budget = self.dedup_decoded_budget(limits);
            let bytes_per_row = self.config.maintenance.timefusion_dedup_bytes_per_row;
            let est_decoded_bytes: u64 = targets
                .iter()
                .map(|a| {
                    let by_rows = a.get_stats().ok().flatten().map_or(0, |s| (s.num_records.max(0) as u64).saturating_mul(bytes_per_row));
                    let by_size = (a.size.max(0) as u64).saturating_mul(inflation);
                    by_rows.max(by_size)
                })
                .sum::<u64>()
                .saturating_mul(2); // RowConverter keyed copy in dedup_batches
            let shards = dedup_shard_count(limits.is_some(), est_decoded_bytes, rewrite_bytes.max(0) as u64, decoded_budget, compressed_budget);
            // K is the read/decode amplification: each shard is an independent
            // query over the SAME files, so every unit of over-estimate in
            // `est_decoded` costs a whole extra pass. Log the inputs.
            if shards > 1 {
                info!(
                    table = %table_name,
                    shards,
                    est_decoded_mb = est_decoded_bytes / (1 << 20),
                    compressed_mb = rewrite_bytes.max(0) / (1 << 20),
                    decoded_budget_mb = decoded_budget / (1 << 20),
                    files = targets.len(),
                    event = "dedup_rewrite_sharded"
                );
            }
            let in_list = file_ids.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).join(", ");
            // THE row scope of the rewrite — full row sets of every file the chunk touches. Shared
            // by both oracles and by each shard, which only appends its bucket range.
            let chunk_rows_filter = format!("{partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list})");
            // ONE binding, read both by the writer properties below and by the
            // StagedBin this function returns — `mark_written_sorted` must be
            // told the same fact that decided the footer, not a re-derivation.
            let sorted = !schema_order_by_clause(schema).is_empty();
            // `keys_varchar` doubles as the GROUP BY for the skew probe below.
            let keys_varchar = schema.dedup_keys.iter().map(|k| format!("CAST(\"{k}\" AS VARCHAR)")).join(", ");
            let bucket_expr = dedup_bucket_expr(schema);
            // Independent narrow oracle for the staged output count: exactly one
            // row per distinct key. A disagreement rejects the unit before Remove
            // actions can reach `commit_wave`.
            let logical_rows_sql = format!("SELECT count(*) FROM (SELECT 1 FROM {scan_name} WHERE {chunk_rows_filter} GROUP BY {keys_varchar})");
            let expected_logical_rows = u64::try_from(
                Self::scalar_i64(&ctx, &logical_rows_sql).await?.ok_or_else(|| anyhow::anyhow!("dedup rewrite distinct-key validation returned no scalar"))?,
            )
            .map_err(|_| anyhow::anyhow!("dedup rewrite distinct-key validation returned a negative count"))?;

            if limits.is_none()
                && shards > 1
                && decoded_budget > 0
                && let Some(max_group) = Self::unshardable_group(&ctx, &chunk_rows_filter, &keys_varchar, bytes_per_row, decoded_budget).await?
            {
                crate::observability::record_dedup_chunk_skipped();
                error!(
                    "dedup rewrite SKIPPED (single key group of {} rows over decoded budget — unshardable): table={} chunk=[{}] files={} — duplicates persist until compaction shrinks the file set",
                    max_group,
                    table_name,
                    label,
                    targets.len()
                );
                return Ok(BinOutcome::Retry);
            }

            // 4. Rewrite each shard independently: collect, dedup, stage its own
            // parquet. The permit bounds concurrent Arrow materializations (dedup
            // materializes OUTSIDE any memory pool); held for the shard loop only,
            // dropped before the unit is handed to a wave. On any per-shard error,
            // already-staged parquet is cleaned so a mid-loop failure leaks nothing.
            let rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
            let staging_table = { table_ref.read().await.clone() };
            let stage_store = staging_table.log_store().object_store(None);
            // Shards have disjoint bucket ranges. The permit bounds concurrent
            // bins; `shard_k` bounds Arrow memory within each bin.
            let shard_k = dedup_shard_concurrency(decoded_budget, self.config.derived.cores).min(limits.map_or(usize::MAX, |l| l.max_concurrent_shards.max(1)));
            let staged_shards: Vec<StagedShard> = futures::stream::iter(0..shards)
                .map(|shard| {
                    let (ctx, staging_table, scan_name) = (&ctx, &staging_table, &scan_name);
                    let (chunk_rows_filter, bucket_expr) = (&chunk_rows_filter, &bucket_expr);
                    async move {
                        let mut adds: Vec<Action> = Vec::new();
                        let staged: anyhow::Result<(usize, usize)> = async {
                            let shard_pred = shard_bucket_pred(bucket_expr, shard, shards);
                            let rows_filter = format!("{chunk_rows_filter}{shard_pred}");
                            let rows_sql = format!("SELECT * FROM {scan_name} WHERE {rows_filter}");
                            // Version collapse: greatest `dedup_tiebreak` per key wins.
                            // Tombstones are RETAINED — an older version of a key can always
                            // outlive this rewrite (later appends, rows still in MemBuffer/WAL,
                            // keys spanning partitions), and a dropped tombstone silently
                            // resurrects the row.
                            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);
                            let mut writer = deltalake::writer::RecordBatchWriter::for_table(staging_table)
                                .map_err(|e| anyhow::anyhow!("dedup rewrite writer: {e}"))?
                                .with_writer_properties(writer_properties);
                            let target_schema = writer.arrow_schema();
                            let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
                            // One shard, not all K, so a 256-way rewrite cannot flood the log.
                            let log_decoded = |stage, rows, actual: usize| {
                                if shard == 0 {
                                    info!(
                                        shards,
                                        rows,
                                        actual_decoded_mb = actual / (1 << 20),
                                        predicted_decoded_mb = (est_decoded_bytes / shards.max(1)) / (1 << 20),
                                        event = "dedup_shard_decoded",
                                        stage,
                                        "what one dedup shard decoded to, against what the estimate predicted"
                                    );
                                }
                            };
                            let (shard_before, shard_after) = if limits.is_some() {
                                let count_sql = format!("SELECT COUNT(*) FROM {scan_name} WHERE {rows_filter}");
                                let shard_before = Self::scalar_i64(ctx, &count_sql).await?.map_or(0, |v| usize::try_from(v.max(0)).unwrap_or(usize::MAX));
                                if shard_before == 0 {
                                    return Ok((0, 0));
                                }
                                // Keys leading the sort make the window redundant: sort ONCE in
                                // schema order and collapse adjacent runs (`RunCollapse`). The
                                // window plan pays two full external sorts — its partition
                                // ordering is normalized to ASC, so it can never double as the
                                // DESC output sort.
                                let order_by = schema_order_by_clause(schema);
                                let streaming_collapse = dedup_keys_lead_the_sort(schema) && !order_by.is_empty();
                                // The collapse audits inline, so this aggregate is only for the
                                // non-collapse path.
                                if let Some(audit_sql) = (!streaming_collapse).then(|| Self::immutable_audit_sql(schema, scan_name, &rows_filter)).flatten() {
                                    // Diagnostics must never fail the rewrite that carries them.
                                    match Self::scalar_i64(ctx, &audit_sql).await {
                                        Ok(Some(disagreeing)) if disagreeing > 0 => {
                                            note_immutable_disagreements(table_name, project_id, u64::try_from(disagreeing).unwrap_or_default())
                                        }
                                        Ok(_) => {}
                                        Err(error) => warn!(%error, "immutable-column audit failed"),
                                    }
                                }
                                let columns = schema.fields.iter().map(|field| crate::rollup::quoted(&field.name)).join(", ");
                                let keys = quoted_csv(&schema.dedup_keys);
                                let order = schema
                                    .dedup_tiebreak
                                    .as_ref()
                                    .map_or_else(|| keys.clone(), |field| format!("{} DESC NULLS LAST", crate::rollup::quoted(field)));
                                let sql = if streaming_collapse {
                                    format!("SELECT {columns} FROM {scan_name} WHERE {rows_filter}{order_by}")
                                } else {
                                    format!(
                                        "SELECT {columns} FROM (SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order}) AS __tf_rn \
                                         FROM {scan_name} WHERE {rows_filter}) WHERE __tf_rn = 1{order_by}"
                                    )
                                };
                                let planned_at = std::time::Instant::now();
                                let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
                                let t_plan = planned_at.elapsed();
                                let (mut t_upstream, mut t_write) = (std::time::Duration::ZERO, std::time::Duration::ZERO);
                                // `RunCollapse` collapses ADJACENT runs, so it needs ONE ordered
                                // stream: `execute_stream` would coalesce a multi-partition plan
                                // without preserving ordering, interleaving equal keys.
                                let plan = match (streaming_collapse, plan.properties().output_partitioning().partition_count()) {
                                    (true, 2..) => {
                                        let ordering =
                                            plan.properties().output_ordering().cloned().ok_or_else(|| {
                                                anyhow::anyhow!("dedup rewrite: partitioned plan with no ordering cannot feed a run collapse")
                                            })?;
                                        Arc::new(datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(ordering, plan)) as _
                                    }
                                    _ => plan,
                                };
                                if shard == 0 {
                                    let rendered = datafusion::physical_plan::displayable(plan.as_ref()).indent(false).to_string();
                                    info!(
                                        table = %table_name,
                                        sorts = rendered.matches("SortExec").count(),
                                        merges = rendered.matches("SortPreservingMergeExec").count(),
                                        collapse = streaming_collapse,
                                        ordered_scan = rendered.contains("output_ordering="),
                                        event = "dedup_plan_shape",
                                        "does the rewrite still pay a sort, or does the footer ordering carry it"
                                    );
                                }
                                let _progress = crate::database::maintain::PlanProgress::watch(Arc::clone(&plan));
                                let mut collapse = streaming_collapse
                                    .then(|| {
                                        RunCollapse::new(&plan.schema(), &schema.dedup_keys, schema.dedup_tiebreak.as_deref())
                                            .and_then(|collapse| collapse.with_immutable_audit(&plan.schema(), &Self::immutable_audit_columns(schema)))
                                    })
                                    .transpose()?;
                                let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;
                                let mut shard_after = 0usize;
                                let mut decoded_bytes = 0usize;
                                let mut drained = false;
                                while !drained {
                                    // The run still open at end-of-stream is the last thing
                                    // written: it takes the same path as every other batch.
                                    let pulled_at = std::time::Instant::now();
                                    let next = stream.next().await;
                                    t_upstream += pulled_at.elapsed();
                                    let batches = match next {
                                        Some(batch) => match &mut collapse {
                                            Some(collapse) => collapse.push(cast_variant_columns_to_binary(batch?)?)?,
                                            None => vec![cast_variant_columns_to_binary(batch?)?],
                                        },
                                        None => {
                                            drained = true;
                                            collapse.as_mut().and_then(RunCollapse::finish).into_iter().collect()
                                        }
                                    };
                                    for batch in batches {
                                        shard_after = shard_after.saturating_add(batch.num_rows());
                                        // Without this, `run_until_idle` cannot tell a long
                                        // rewrite from a stall and kills it at the deadline.
                                        crate::database::maintain::note_unit_progress(batch.num_rows());
                                        decoded_bytes = decoded_bytes.saturating_add(batch.get_array_memory_size());
                                        let casted = deltalake::kernel::schema::cast_record_batch(&batch, target_schema.clone(), true, true)?;
                                        let wrote_at = std::time::Instant::now();
                                        Self::write_staged(&mut writer, &mut adds, casted, max_file_bytes).await?;
                                        t_write += wrote_at.elapsed();
                                    }
                                }
                                // Same three phases and event name as the Pack/Repair staging
                                // timers in `maintain.rs`, so both paths aggregate together.
                                info!(
                                    table_name,
                                    project_id,
                                    pass = "Dedup",
                                    files = target_files,
                                    rows_staged = shard_after,
                                    outputs = adds.len(),
                                    plan_secs = t_plan.as_secs_f64(),
                                    upstream_secs = t_upstream.as_secs_f64(),
                                    write_secs = t_write.as_secs_f64(),
                                    event = "unit_phase_timing",
                                    "where a maintenance unit's wall clock went"
                                );
                                // Reported here rather than mid-stream so one run straddling a
                                // batch boundary is counted once.
                                if collapse.as_ref().is_some_and(RunCollapse::is_auditing) {
                                    crate::observability::maintenance_stats().immutable_audit_shards_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                if let Some(disagreeing) = collapse.as_ref().map(RunCollapse::disagreements).filter(|count| *count > 0) {
                                    note_immutable_disagreements(table_name, project_id, disagreeing);
                                }
                                // Post-dedup rows, so this UNDER-states the input volume.
                                log_decoded("streamed", shard_after, decoded_bytes);
                                (shard_before, shard_after)
                            } else {
                                let batches: Vec<RecordBatch> = crate::database::maintain::collect_watched(ctx, &rows_sql)
                                    .await?
                                    .into_iter()
                                    .map(|batch| drop_batch_column(batch, DEDUP_FILE_COL))
                                    .collect();
                                let shard_before = batches.iter().map(RecordBatch::num_rows).sum();
                                log_decoded("collected", shard_before, batches.iter().map(RecordBatch::get_array_memory_size).sum());
                                if shard_before == 0 {
                                    return Ok((0, 0));
                                }
                                let deduped = crate::write::mem_buffer::dedup_batches(batches, &schema.dedup_keys, schema.dedup_tiebreak.as_deref(), None)?;
                                let shard_after = deduped.iter().map(RecordBatch::num_rows).sum();
                                let deduped = deduped.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;
                                let (deduped, _) = self.sort_flush_group(schema, deduped, UnsortedFallback::Forbid).await?;
                                for batch in deduped {
                                    let casted = deltalake::kernel::schema::cast_record_batch(&batch?, target_schema.clone(), true, true)?;
                                    Self::write_staged(&mut writer, &mut adds, casted, max_file_bytes).await?;
                                }
                                (shard_before, shard_after)
                            };
                            adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("dedup rewrite flush: {e}"))?.into_iter().map(Action::Add));
                            Ok((shard_before, shard_after))
                        }
                        .await;
                        (adds, staged)
                    }
                })
                .buffer_unordered(shard_k)
                .collect()
                .await;
            drop(rewrite_permit);
            // Every shard hands back its adds even when it failed, so collect
            // them all BEFORE folding the outcomes: a mid-flight failure still
            // leaks nothing.
            let (shard_adds, outcomes): (Vec<Vec<Action>>, Vec<_>) = staged_shards.into_iter().unzip();
            let adds: Vec<Action> = shard_adds.concat();
            let (before, after) = match outcomes
                .into_iter()
                .try_fold((0usize, 0usize), |(before, after), outcome| outcome.map(|(shard_before, shard_after)| (before + shard_before, after + shard_after)))
            {
                Ok(totals) => totals,
                Err(e) => {
                    Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                    return Err(e);
                }
            };
            if !dedup_rewrite_counts_match(before as u64, expected_live_rows, after as u64, expected_logical_rows) {
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                anyhow::bail!(
                    "dedup rewrite validation failed for table={} chunk=[{}]: reread={}/{} expected live rows, output={}/{} expected logical rows",
                    table_name,
                    label,
                    before,
                    expected_live_rows,
                    after,
                    expected_logical_rows
                );
            }
            if before == 0 {
                return Ok(BinOutcome::Retry);
            }
            if before == after {
                // Probe false-positive (a concurrent rewrite already deduped): discard
                // the staged no-op copies, certify clean, commit nothing.
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                return Ok(BinOutcome::Converged);
            }
            // Row-DROPPING rewrite: data_change=true on both sides — the
            // snapshot-isolation downgrade in `staged_actions` is only sound for
            // data-preserving commits.
            let (removes, adds) = staged_actions(&targets, adds, true);
            // Record the intent BEFORE the unit can be handed to a wave commit, so
            // a crash in the staging->commit window leaves a trail to clean up.
            let wave_id = uuid::Uuid::new_v4().to_string();
            self.record_staged_intent(dedup_staged_intent(
                wave_id.clone(),
                table_name,
                project_id,
                adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.path.clone()) } else { None }).collect(),
            ));
            debug!(table_name, project_id, chunk = label, files = targets.len(), before, after, event = "dedup_chunk_staged");
            return Ok(BinOutcome::Staged(StagedBin {
                project_id: project_id.to_string(),
                wave_id,
                targets,
                removes,
                adds,
                stage_store,
                discardable_paths: Vec::new(),
                dedup: Some(DedupUnit { key: key.clone(), date: date_str.to_string(), label: label.to_string(), before: before as u64, after: after as u64 }),
                sorted,
            }));
        }
        anyhow::bail!("dedup rewrite: re-plan attempts exhausted for table={} chunk=[{}]", table_name, label)
    }

    /// DV-dedup: mark loser rows deleted via a deletion vector instead of
    /// rewriting whole files. Same survivor rule as copy-on-write
    /// (`dedup_batches`); commit shape `Remove(old) + Add(same path, +DV)`.
    /// Sharded by dedup-key hash (a dup group shares one bucket, so no group
    /// splits) to bound decode memory.
    #[allow(clippy::too_many_arguments)]
    async fn stage_dedup_chunk_dv(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, schema: &crate::schema::TableSchema, chunk_filter: &str, label: &str,
        date_str: &str, key: Option<DirtyBinKey>, limits: Option<DedupExecutionLimits>,
    ) -> Result<BinOutcome<StagedBin>> {
        use datafusion::arrow::array::{RecordBatch, StringArray, UInt64Array};
        use datafusion::arrow::datatypes::DataType;
        use deltalake::kernel::Action;
        use deltalake::operations::deletion_vectors::{FileDeletion, dv_object_store_relative_path, write_deletion_vectors};
        const DV_ROW_INDEX_COL: &str = "__tf_dv_row_index";
        const MAX_REPLANS: usize = 3;

        let tiebreak = schema.dedup_tiebreak.as_deref();
        let proj_csv = [format!("\"{DEDUP_FILE_COL}\""), format!("\"{DV_ROW_INDEX_COL}\"")]
            .into_iter()
            .chain(schema.dedup_keys.iter().map(|k| crate::rollup::quoted(k)))
            .chain(tiebreak.map(crate::rollup::quoted))
            .join(", ");

        // (file path, 0-based physical row index) for every row of a projected
        // batch list. The scan exposes a 1-based physical row number; DV indexes
        // are 0-based (`v - 1`), and physical even on an already-DV'd file.
        let pairs_of = |batches: &[RecordBatch]| -> Result<Vec<(String, u64)>> {
            batches.iter().try_fold(Vec::new(), |mut out, b| {
                let files = datafusion::arrow::compute::cast(b.column(0), &DataType::Utf8)?;
                let files = files.as_any().downcast_ref::<StringArray>().expect("file column casts to Utf8");
                let idxs = b.column(1).as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("dv-dedup row-index column is not UInt64"))?;
                out.extend((0..b.num_rows()).map(|i| (files.value(i).to_string(), idxs.value(i).saturating_sub(1))));
                Ok(out)
            })
        };

        for replan in 0..MAX_REPLANS {
            let (chunk_snapshot, chunk_log_store) = snapshot_and_store(table_ref).await?;
            let partition_files = dedup_partition_paths(chunk_snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
            let provider =
                Self::narrow_provider(chunk_log_store.clone(), Arc::clone(&chunk_snapshot), partition_files, Some(DEDUP_FILE_COL), Some(DV_ROW_INDEX_COL))
                    .await
                    .map_err(|e| anyhow::anyhow!("dv-dedup provider: {e}"))?;
            let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(
                limits.map_or(self.config.memory.timefusion_query_partitions, |l| l.sort_partitions),
                self.maintenance_runtime_env(),
            ));
            ctx.register_table(DEDUP_SCAN_NAME, provider)?;

            // Files holding the chunk's rows → Add targets, from THIS snapshot.
            let files_sql = format!("SELECT DISTINCT \"{DEDUP_FILE_COL}\" FROM {DEDUP_SCAN_NAME} WHERE {chunk_filter}");
            let file_ids = read_string_column(crate::database::maintain::collect_watched(&ctx, &files_sql).await?)?;
            if file_ids.is_empty() {
                return Ok(BinOutcome::Retry);
            }
            let targets = adds_for_file_ids(&chunk_snapshot, &file_ids, table_name);
            if targets.len() != file_ids.len() {
                warn!("dv-dedup: mapped {}/{} files for table={} chunk=[{}], re-planning", targets.len(), file_ids.len(), table_name, label);
                tokio::time::sleep(occ_backoff(replan)).await;
                continue;
            }

            // Same-snapshot count oracle guards CERTIFICATION (not the commit): a
            // truncated read can only MISS dupes, never mask unique data — a loser
            // is marked only when scanned beside a same-key row that beat it.
            let oracle = Self::scalar_i64(&ctx, &format!("SELECT COUNT(*) FROM {DEDUP_SCAN_NAME} WHERE {chunk_filter}")).await?.unwrap_or(0).max(0) as u64;

            // Shard the chunk by dedup-key hash so decode memory is bounded; a dup
            // group shares one bucket and is never split.
            let bucket_expr = dedup_bucket_expr(schema);
            let bytes_per_row = self.config.maintenance.timefusion_dedup_bytes_per_row;
            let est_decoded = oracle.saturating_mul(bytes_per_row).saturating_mul(2);
            let decoded_budget = self.dedup_decoded_budget(limits);
            let shards = dedup_shard_count(limits.is_some(), est_decoded, 0, decoded_budget, u64::MAX).max(1);

            if limits.is_none()
                && shards > 1
                && decoded_budget > 0
                && let Some(max_group) = Self::unshardable_group(&ctx, chunk_filter, &quoted_csv(&schema.dedup_keys), bytes_per_row, decoded_budget).await?
            {
                crate::observability::record_dedup_chunk_skipped();
                error!(
                    "dv-dedup SKIPPED (single key group of {} rows over decoded budget — unshardable): table={} chunk=[{}] — duplicates persist until compaction shrinks the file set",
                    max_group, table_name, label
                );
                return Ok(BinOutcome::Retry);
            }

            let mut losers_by_file: HashMap<String, Vec<u64>> = HashMap::new();
            let mut survivors_total: u64 = 0;
            let mut scanned_total: u64 = 0;
            {
                let _permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
                for shard in 0..shards {
                    let sql = format!("SELECT {proj_csv} FROM {DEDUP_SCAN_NAME} WHERE {chunk_filter}{}", shard_bucket_pred(&bucket_expr, shard, shards));
                    let batches = crate::database::maintain::collect_watched(&ctx, &sql).await?;
                    let all_pairs = pairs_of(&batches)?;
                    scanned_total += all_pairs.len() as u64;
                    let survivors = crate::write::mem_buffer::dedup_batches(batches.clone(), &schema.dedup_keys, tiebreak, None)?;
                    let survivor_set: HashSet<(String, u64)> = pairs_of(&survivors)?.into_iter().collect();
                    survivors_total += survivor_set.len() as u64;
                    all_pairs.into_iter().filter(|pair| !survivor_set.contains(pair)).for_each(|(file, idx)| losers_by_file.entry(file).or_default().push(idx));
                }
            }

            // The scan and the oracle share one snapshot/ctx, so a mismatch is a
            // truncated re-read (concurrent rewrite) — retry, don't certify.
            if scanned_total != oracle {
                warn!("dv-dedup: scan saw {scanned_total} rows vs oracle {oracle} for table={table_name} chunk=[{label}] — retry");
                return Ok(BinOutcome::Retry);
            }
            let losers_total: u64 = losers_by_file.values().map(|v| v.len() as u64).sum();
            if survivors_total + losers_total != scanned_total {
                anyhow::bail!(
                    "dv-dedup accounting for table={table_name} chunk=[{label}]: survivors {survivors_total} + losers {losers_total} != scanned {scanned_total}"
                );
            }
            if losers_total == 0 {
                return Ok(BinOutcome::Converged);
            }

            // Attach each loser set to its Add (scan file paths are store paths;
            // the Add carries a log-relative path — suffix-match either way).
            let deletions: Vec<FileDeletion> = targets
                .iter()
                .filter_map(|add| {
                    let idxs: Vec<u64> = losers_by_file
                        .iter()
                        .filter(|(fpath, _)| fpath.ends_with(add.path.as_str()) || add.path.as_str().ends_with(fpath.as_str()))
                        .flat_map(|(_, positions)| positions.iter().copied())
                        .collect();
                    (!idxs.is_empty()).then(|| FileDeletion { add: add.clone(), deleted_indexes: idxs })
                })
                .collect();
            let n_files = deletions.len();

            let root = url::Url::parse(table_ref.read().await.table_url().as_ref()).map_err(|e| anyhow::anyhow!("dv-dedup table url: {e}"))?;
            let actions = write_deletion_vectors(chunk_log_store.as_ref(), &root, deletions).await.map_err(|e| anyhow::anyhow!("dv-dedup write: {e}"))?;
            // Every index was already deleted (idempotent re-run) — nothing staged.
            if actions.is_empty() {
                return Ok(BinOutcome::Converged);
            }

            let (removes, adds): (Vec<_>, Vec<_>) =
                actions.into_iter().filter(|a| matches!(a, Action::Remove(_) | Action::Add(_))).partition(|a| matches!(a, Action::Remove(_)));
            let discardable_paths: Vec<String> = adds
                .iter()
                .filter_map(|a| match a {
                    Action::Add(add) => add.deletion_vector.as_ref().and_then(dv_object_store_relative_path),
                    _ => None,
                })
                .collect();

            let stage_store = chunk_log_store.object_store(None);
            let wave_id = uuid::Uuid::new_v4().to_string();
            // Crash-before-commit cleanup: the fresh-UUID `.bin` sidecars. Boot
            // reconcile must treat a COMMITTED `.bin` as live via the descriptor rule.
            self.record_staged_intent(dedup_staged_intent(wave_id.clone(), table_name, project_id, discardable_paths.clone()));
            crate::observability::maintenance_stats().dv_dedup_bins_staged.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(table_name, project_id, chunk = label, files = n_files, scanned_total, losers_total, event = "dv_dedup_chunk_staged");
            return Ok(BinOutcome::Staged(StagedBin {
                project_id: project_id.to_string(),
                wave_id,
                targets,
                removes,
                adds,
                stage_store,
                discardable_paths,
                dedup: Some(DedupUnit {
                    key: key.clone(),
                    date: date_str.to_string(),
                    label: label.to_string(),
                    before: scanned_total,
                    after: survivors_total,
                }),
                // Same-path files, so don't claim sortedness we didn't write.
                sorted: false,
            }));
        }
        anyhow::bail!("dv-dedup: re-plan attempts exhausted for table={} chunk=[{}]", table_name, label)
    }

    /// Live parquet files of one `date=` partition, grouped by the
    /// `project_id=` path segment ("default" when absent — custom-project
    /// tables don't embed it). Shared by the sweep's fingerprint capture and
    /// the read-side dedup-skip check so both hash identical groupings.
    pub(crate) fn partition_files_by_pid(table: &DeltaTable, date_marker: &str) -> Result<HashMap<String, Vec<String>>> {
        Ok(table
            .get_file_uris()?
            .filter(|uri| uri.contains(date_marker) && uri.ends_with(".parquet"))
            .map(|uri| (path_partition_value(&uri, "project_id").unwrap_or("default").to_string(), uri))
            .into_group_map())
    }

    /// Live `(path, dv_unique_id)` set of one project's `date=` partition. Unlike
    /// the certification fingerprint (URIs only), this sees a same-path DV commit.
    pub(crate) fn partition_dv_state(table: &DeltaTable, project_id: &str, date_marker: &str) -> Result<HashSet<DvEntry>> {
        Ok(table
            .snapshot()?
            .snapshot()
            .log_data()
            .iter()
            .filter(|f| {
                let p = f.path();
                p.contains(date_marker) && path_partition_value(&p, "project_id").unwrap_or("default") == project_id
            })
            .map(|f| (f.path().into_owned(), f.deletion_vector_descriptor().map(|d| dv_identity(&d))))
            .collect())
    }
}

/// Width of a dedup "dirty bin" — THE definition, shared by the producer
/// (`write.rs`), the prober (`probe_dup_bins`) and the drain (`maintain.rs`);
/// they must agree or a bin marked dirty by one is never found by another.
/// Overridable at runtime with `timefusion_dedup_bin_minutes`; a wider bin reads
/// fewer files in total, since one unit rewrites every file overlapping its bin.
pub(crate) const DEFAULT_BIN_MINUTES: i64 = 10;

/// The packing VALUE floor from config (0 = off / shadow in configless
/// processes). See `refuse_low_value_bin`.
pub(crate) fn pack_value_floor() -> u64 {
    crate::config::try_config().map_or(0, |c| c.maintenance.timefusion_pack_max_rows_per_file_eliminated)
}

/// The similar-size admission ratio for packing bins, from config (0 = off in
/// processes with no config, e.g. unit tests). See `bin_breaks_size_ratio`.
pub(crate) fn pack_size_ratio() -> i64 {
    crate::config::try_config().map_or(0, |c| c.maintenance.timefusion_pack_max_size_ratio)
}

/// The dedup bin width, in micros. Read through the config `OnceLock`, so it is
/// fixed for the life of the process — a width that changed under a running
/// coordinator would leave the dirty-bin queue keyed two ways at once.
/// Falls back to the default before config init (unit tests, early boot).
#[inline]
pub(crate) fn bin_micros() -> i64 {
    crate::config::try_config().map_or(DEFAULT_BIN_MINUTES, |c| c.buffer.timefusion_dedup_bin_minutes).max(1) * 60 * 1_000_000
}

/// Whether every dedup key is a leading `sorting_columns` entry, in order.
///
/// When it holds, a stream in schema order has all versions of a key adjacent,
/// so `RunCollapse` can keep-greatest in one pass instead of paying the
/// two-sort `ROW_NUMBER() OVER (PARTITION BY keys)` plan.
pub(crate) fn dedup_keys_lead_the_sort(schema: &crate::schema::TableSchema) -> bool {
    !schema.dedup_keys.is_empty()
        && schema.sorting_columns.len() >= schema.dedup_keys.len()
        && schema.dedup_keys.iter().zip(&schema.sorting_columns).all(|(key, sort)| *key == sort.name)
}

/// One consistent `(eager snapshot, log store)` pair off the current table
/// state — the read every dedup scan/rewrite pins itself to.
async fn snapshot_and_store(table_ref: &Arc<RwLock<DeltaTable>>) -> Result<(Arc<deltalake::kernel::EagerSnapshot>, deltalake::logstore::LogStoreRef)> {
    let table = table_ref.read().await;
    Ok((Arc::new(table.snapshot()?.snapshot().clone()), table.log_store()))
}

/// Count + warn for dedup keys whose versions disagree on a column declared
/// immutable. Both audit forms (the streaming collapse and the `GROUP BY`
/// aggregate it replaces) report through here, so they cannot drift apart.
fn note_immutable_disagreements(table_name: &str, project_id: &str, disagreeing: u64) {
    crate::observability::maintenance_stats().immutable_column_disagreement_total.fetch_add(disagreeing, std::sync::atomic::Ordering::Relaxed);
    warn!(
        table = %table_name,
        project_id = %project_id,
        disagreeing,
        event = "immutable_column_disagreement",
        "versions of one key differ on a column declared immutable; read filters on it are pushed below dedup"
    );
}

/// A dedup unit's staged-intent line, recorded before the unit can be handed to
/// a wave commit. Dedup entries are CLEANUP-ONLY (no `target_paths`, no `adds`):
/// a dedup rewrite DROPS rows, so the resume path's row-preservation check
/// cannot tell a valid staging from a truncated one.
fn dedup_staged_intent(wave_id: String, table_name: &str, project_id: &str, paths: Vec<String>) -> StagedIntent {
    StagedIntent {
        wave_id,
        table_name: table_name.to_string(),
        project_id: project_id.to_string(),
        recorded_at: crate::support::now_secs(),
        paths,
        target_paths: Vec::new(),
        adds: Vec::new(),
        rollup: None,
        instance: None,
    }
}

/// `"a", "b", …` — quoted column list for SQL, the shape every dedup query needs.
fn quoted_csv(names: &[String]) -> String {
    names.iter().map(|name| crate::rollup::quoted(name)).join(", ")
}

/// The `hash_bucket(...)` SQL expression partitioning rows by dedup key into
/// `[0, DEDUP_BUCKET_COUNT)`; chr(31) separates keys so distinct tuples can't
/// collide, and hashing (not `key % K`) spreads evenly and is NULL-safe.
fn dedup_bucket_expr(schema: &crate::schema::TableSchema) -> String {
    let keys_varchar = schema.dedup_keys.iter().map(|k| format!("CAST(\"{k}\" AS VARCHAR)")).join(", ");
    format!("hash_bucket(arrow_cast(concat_ws(chr(31), {keys_varchar}), 'Utf8View'), {DEDUP_BUCKET_COUNT})")
}

/// ` AND <bucket_expr> >= lo[ AND < hi]` — one shard's contiguous bucket range
/// (even ±1); empty when unsharded.
fn shard_bucket_pred(bucket_expr: &str, shard: u64, shards: u64) -> String {
    if shards <= 1 {
        return String::new();
    }
    let (lo, hi) = (shard * DEDUP_BUCKET_COUNT / shards, (shard + 1) * DEDUP_BUCKET_COUNT / shards);
    let upper = if hi < DEDUP_BUCKET_COUNT { format!(" AND {bucket_expr} < {hi}") } else { String::new() };
    format!(" AND {bucket_expr} >= {lo}{upper}")
}

/// Non-null values of `column(0)` across `batches`, cast to Utf8.
fn read_string_column(batches: Vec<RecordBatch>) -> Result<Vec<String>> {
    batches.into_iter().try_fold(Vec::new(), |mut out, batch| {
        let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
        let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
        out.extend(col.iter().flatten().map(str::to_string));
        Ok(out)
    })
}

/// Map scan file-id values back to Add actions in the SAME snapshot
/// (suffix-match either direction: the scan column carries the store path, the
/// log a table-relative one).
fn adds_for_file_ids(snapshot: &deltalake::kernel::EagerSnapshot, file_ids: &[String], table_name: &str) -> Vec<deltalake::kernel::Add> {
    dedup_adds_by_path(
        snapshot
            .log_data()
            .iter()
            .filter(|f| {
                let p = f.path();
                file_ids.iter().any(|v| v.ends_with(p.as_ref()) || p.ends_with(v.as_str()))
            })
            .map(|f| {
                #[allow(deprecated)]
                f.add_action()
            }),
        table_name,
    )
}

/// Row encoder over `schema`'s columns at `idxs`, in that order.
fn row_converter(schema: &arrow_schema::Schema, idxs: &[usize]) -> Result<datafusion::arrow::row::RowConverter> {
    use datafusion::arrow::row::{RowConverter, SortField};
    Ok(RowConverter::new(idxs.iter().map(|idx| SortField::new(schema.field(*idx).data_type().clone())).collect())?)
}

/// The run of equal keys currently in flight. `winner: None` means the carried
/// row from the previous batch still holds it.
struct OpenRun {
    key: Vec<u8>,
    winner: Option<u32>,
    best: Option<Vec<u8>>,
}

/// Keep-greatest over a stream already sorted by the schema's sort key.
///
/// Runs of equal dedup keys are contiguous (see `dedup_keys_lead_the_sort`), so
/// one row per key is chosen in a single order-preserving pass, with only the
/// TRAILING run held back — the only one that can continue into the next batch.
/// Ties keep the first row; tombstones are retained.
pub(crate) struct RunCollapse {
    keys: datafusion::arrow::row::RowConverter,
    tiebreak: Option<(usize, datafusion::arrow::row::RowConverter)>,
    key_idxs: Vec<usize>,
    /// The winning row of the run still in flight, as its own compact batch.
    carry: Option<(RecordBatch, Vec<u8>, Option<Vec<u8>>)>,
    /// Immutable-column audit: the columns to compare, and the run state that
    /// carries across batches with `carry`.
    audit: Option<(Vec<usize>, datafusion::arrow::row::RowConverter)>,
    run_immutable: Option<Vec<u8>>,
    run_disagreed: bool,
    disagreements: u64,
}

impl RunCollapse {
    pub(crate) fn new(schema: &arrow_schema::Schema, keys: &[String], tiebreak: Option<&str>) -> Result<Self> {
        let index = |name: &str| schema.index_of(name).map_err(|_| anyhow::anyhow!("run collapse column `{name}` missing from rewrite output"));
        let key_idxs = keys.iter().map(|key| index(key)).collect::<Result<Vec<_>>>()?;
        Ok(Self {
            keys: row_converter(schema, &key_idxs)?,
            // Default `SortField` is ASC/nulls-first, so a byte compare of the
            // encoded tiebreak IS its value order and a NULL version ranks below
            // every stamped one.
            tiebreak: tiebreak.map(|name| index(name).and_then(|idx| Ok((idx, row_converter(schema, &[idx])?)))).transpose()?,
            key_idxs,
            carry: None,
            audit: None,
            run_immutable: None,
            run_disagreed: false,
            disagreements: 0,
        })
    }

    /// Audit immutable columns while collapsing, instead of with a separate
    /// `GROUP BY` aggregate: a run disagrees exactly when some row's immutable
    /// tuple differs from the run's first, in O(1) state per run. The row
    /// encoding distinguishes NULL from any value, so both shapes are caught —
    /// two differing non-null values, and a field absent then later filled.
    pub(crate) fn with_immutable_audit(mut self, schema: &arrow_schema::Schema, columns: &[String]) -> Result<Self> {
        let idxs = columns.iter().filter_map(|name| schema.index_of(name).ok()).collect::<Vec<_>>();
        self.audit = (!idxs.is_empty()).then(|| row_converter(schema, &idxs).map(|converter| (idxs, converter))).transpose()?;
        Ok(self)
    }

    /// Dedup keys whose versions disagreed on a column declared immutable.
    pub(crate) fn disagreements(&self) -> u64 {
        self.disagreements
    }

    /// Whether the immutable audit is armed — `disagreements()` is only a clean
    /// bill of health when this is true.
    pub(crate) fn is_auditing(&self) -> bool {
        self.audit.is_some()
    }

    fn idx(row: usize) -> Result<u32> {
        u32::try_from(row).map_err(|_| anyhow::anyhow!("run collapse row index overflow"))
    }

    /// Rows of `batch` at `indices`, as a new batch detached from its buffers —
    /// so a held-back run cannot pin the whole batch.
    fn take_rows(batch: &RecordBatch, indices: datafusion::arrow::array::UInt32Array) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            batch.schema(),
            batch.columns().iter().map(|column| datafusion::arrow::compute::take(column, &indices, None)).collect::<std::result::Result<Vec<_>, _>>()?,
        )?)
    }

    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let key_rows = self.keys.convert_columns(&self.key_idxs.iter().map(|idx| Arc::clone(batch.column(*idx))).collect::<Vec<_>>())?;
        let tiebreak_rows = self.tiebreak.as_ref().map(|(idx, converter)| converter.convert_columns(&[Arc::clone(batch.column(*idx))])).transpose()?;
        let tiebreak_at = |row: usize| tiebreak_rows.as_ref().map(|rows| rows.row(row).as_ref().to_vec());
        let audit_rows = self
            .audit
            .as_ref()
            .map(|(idxs, converter)| converter.convert_columns(&idxs.iter().map(|idx| Arc::clone(batch.column(*idx))).collect::<Vec<_>>()))
            .transpose()?;
        let immutable_at = |row: usize| audit_rows.as_ref().map(|rows| rows.row(row).as_ref().to_vec());

        let mut out = Vec::new();
        let mut winners: Vec<u32> = Vec::new();
        // The winner of the run in flight: either the carried row or an index
        // into this batch.
        let mut current = self.carry.as_ref().map(|(_, key, tiebreak)| OpenRun { key: key.clone(), winner: None, best: tiebreak.clone() });
        for row in 0..batch.num_rows() {
            let key = key_rows.row(row).as_ref().to_vec();
            let tiebreak = tiebreak_at(row);
            match &mut current {
                Some(open) if open.key == key => {
                    // Another version of the run in flight: count the run once
                    // if its immutable tuple ever differs from the run's first.
                    if self.audit.is_some() && !self.run_disagreed && immutable_at(row) != self.run_immutable {
                        self.run_disagreed = true;
                        self.disagreements += 1;
                    }
                    // Strictly greater only: ties keep the earlier row.
                    if tiebreak > open.best {
                        open.winner = Some(Self::idx(row)?);
                        open.best = tiebreak;
                    }
                }
                _ => {
                    // A closed run whose winner is the carry emits it first: it
                    // sorts before every row of this batch. (No open run at all
                    // implies no carry, so the `None` arm is then a no-op.)
                    match current.take().and_then(|open| open.winner) {
                        Some(index) => winners.push(index),
                        None => out.extend(self.carry.take().map(|(row, _, _)| row)),
                    }
                    current = Some(OpenRun { key, winner: Some(Self::idx(row)?), best: tiebreak });
                    self.run_immutable = immutable_at(row);
                    self.run_disagreed = false;
                }
            }
        }
        // Whatever is still open becomes the new carry.
        self.carry = match current {
            Some(OpenRun { key, winner: Some(index), best }) => Some((Self::take_rows(&batch, vec![index].into())?, key, best)),
            Some(OpenRun { winner: None, .. }) => self.carry.take(),
            None => None,
        };
        if !winners.is_empty() {
            out.push(Self::take_rows(&batch, winners.into())?);
        }
        Ok(out)
    }

    pub(crate) fn finish(&mut self) -> Option<RecordBatch> {
        self.carry.take().map(|(row, _, _)| row)
    }
}

#[cfg(test)]
mod immutable_audit_tests {
    use datafusion::arrow::{
        array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema},
    };
    use test_case::test_case;

    use super::*;

    fn logs_schema() -> &'static crate::schema::TableSchema {
        crate::schema::get_schema("otel_logs_and_spans").expect("the real shipped schema")
    }

    /// A batch from `(column, array)` pairs — every column nullable, as the
    /// real rewrite output is. The fixtures below differ only in their columns.
    fn batch_of(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter_with_nullable(columns.into_iter().map(|(name, array)| (name, array, true))).expect("batch")
    }

    /// The audit query over the real shipped schema — the shape every
    /// SQL-form assertion below is about.
    fn audit_sql() -> String {
        Database::immutable_audit_sql(logs_schema(), "scan", "true").expect("logs declare immutable columns")
    }

    /// Trim the real schema to `keep`, declare every kept column immutable and
    /// key it by `id`, then run the audit over `batch`. Returns `(count, sql)`.
    async fn audit_count(keep: &[&str], batch: RecordBatch) -> (i64, String) {
        let mut schema = logs_schema().clone();
        schema.fields.retain(|field| keep.contains(&field.name.as_str()));
        schema.fields.iter_mut().for_each(|field| field.mutable = false);
        schema.dedup_keys = vec!["id".to_owned()];
        schema.dedup_tiebreak = None;
        schema.tombstone_column = None;
        assert_eq!(schema.fields.len(), keep.len(), "the trimmed schema has exactly the columns this test reasons about: {keep:?}");

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_batch("scan", batch).expect("register");
        let sql = Database::immutable_audit_sql(&schema, "scan", "true").expect("columns to audit");
        let count = Database::scalar_i64(&ctx, &sql).await.expect("the audit must PLAN and RUN").expect("one row");
        (count, sql)
    }

    /// `MIN`/`MAX` catch differing non-null values with O(1) state per group —
    /// `COUNT(DISTINCT c)` would keep a per-group hash set for each of the ~150
    /// audited columns. They ignore nulls, so the two-sided null-count term is
    /// what sees a column absent on first emit and filled on retry, and only
    /// when the column is set in at least one version.
    #[test_case("MIN(" => true ; "MIN catches differing non-null values")]
    #[test_case("MAX(" => true ; "MAX catches differing non-null values")]
    #[test_case("> 0 AND COUNT(" => true ; "a null to value transition is caught")]
    #[test_case("< COUNT(*)" => true ; "a transition counts only when the column is set in some version")]
    #[test_case("COUNT(DISTINCT" => false ; "the audit builds no per-group hash set per column")]
    fn the_audit_sql_has_the_shape_that_makes_it_correct_and_cheap(fragment: &str) -> bool {
        audit_sql().contains(fragment)
    }

    /// The grouping columns, tiebreak and tombstone vary across versions by
    /// construction; auditing them would flag every duplicated key.
    #[test]
    fn the_audit_skips_columns_that_vary_by_construction() {
        let schema = logs_schema();
        let sql = audit_sql();
        let grouped =
            schema.dedup_keys.iter().map(String::as_str).chain(schema.dedup_tiebreak.as_deref()).chain(schema.tombstone_column.as_deref()).collect::<Vec<_>>();
        assert!(!grouped.is_empty(), "the schema has dedup keys, or this test proves nothing");
        for column in grouped {
            assert!(
                !sql.contains(&format!("MIN({})", crate::rollup::quoted(column))),
                "{column} varies across versions by construction and must not be audited"
            );
        }
    }

    /// A schema with nothing auditable must yield no query at all, rather than a
    /// `HAVING` with an empty predicate list that fails at plan time inside the
    /// dedup rewrite it is riding on.
    #[test]
    fn a_schema_with_nothing_to_audit_produces_no_query() {
        let mut schema = logs_schema().clone();
        for field in &mut schema.fields {
            field.mutable = true;
        }
        assert!(Database::immutable_audit_sql(&schema, "scan", "true").is_none(), "all-mutable schema has nothing to audit");
    }

    /// The assertions above check the SQL's shape; this one runs it, since a
    /// query that plans but counts the wrong groups reports a misleading zero.
    #[tokio::test]
    async fn the_audit_query_actually_counts_disagreeing_keys() {
        //                             differing values   null -> value      agrees      all null
        let batch = batch_of(vec![
            ("id", Arc::new(StringArray::from(vec!["a", "a", "b", "b", "c", "c", "d", "d"])) as ArrayRef),
            ("level", Arc::new(StringArray::from(vec![Some("info"), Some("error"), None, Some("error"), Some("info"), Some("info"), None, None])) as ArrayRef),
        ]);

        let (count, sql) = audit_count(&["id", "level"], batch).await;
        assert_eq!(count, 2, "`a` differs outright and `b` goes null -> error; `c` agrees and `d` is null throughout ({sql})");
    }

    /// `MIN`/`MAX` must PLAN for every non-composite type the audit admits: a
    /// plan-time error is swallowed into `warn!` at the call site, so the audit
    /// would go silently dead while still looking present in the SQL.
    #[tokio::test]
    async fn the_audit_plans_for_non_string_types() {
        // A bool, an int and a timestamp alongside the key.
        let kept = ["id", "context___is_remote", "message_size_bytes", "observed_timestamp"];
        let batch = batch_of(vec![
            ("id", Arc::new(StringArray::from(vec!["a", "a", "b", "b"])) as ArrayRef),
            ("context___is_remote", Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(true), Some(true)])) as ArrayRef),
            ("message_size_bytes", Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), Some(2)])) as ArrayRef),
            ("observed_timestamp", Arc::new(TimestampMicrosecondArray::from(vec![Some(10), Some(10), Some(20), Some(20)])) as ArrayRef),
        ]);

        let (count, sql) = audit_count(&kept, batch).await;
        assert_eq!(count, 1, "only `a` disagrees, on the boolean ({sql})");
    }

    /// The streaming-collapse rewrite rests on this property, and it lives in a
    /// YAML file anyone can reorder.
    #[test]
    fn the_shipped_dedup_keys_lead_the_shipped_sort() {
        assert!(dedup_keys_lead_the_sort(logs_schema()), "otel dedup keys must be the leading sorting_columns, or the rewrite silently reverts to the window");
        let mut misaligned = logs_schema().clone();
        misaligned.dedup_keys = vec!["timestamp".to_owned(), "id".to_owned()];
        assert!(!dedup_keys_lead_the_sort(&misaligned), "keys with `service` wedged between them are NOT a prefix");
    }

    /// The one-pass collapse must agree with the `ROW_NUMBER()` window it
    /// replaces, row for row and in order, on data built to break it — several
    /// versions per key, a tombstone, NULL service, NULL and tied tiebreaks, and
    /// (via `batch_size 1`) every run straddling a batch boundary.
    #[tokio::test]
    async fn the_streaming_collapse_agrees_with_the_window_it_replaces() {
        let mut schema = logs_schema().clone();
        let kept = ["timestamp", "resource___service___name", "id", "level", "updated_at"];
        schema.fields.retain(|field| kept.contains(&field.name.as_str()));
        schema.sorting_columns.retain(|column| kept.contains(&column.name.as_str()));
        assert!(dedup_keys_lead_the_sort(&schema), "the trimmed schema keeps the property under test");

        //         ts   service      id    level     updated_at
        let rows = [
            (10, Some("api"), "a", "info", Some(1)), // three versions of one key,
            (10, Some("api"), "a", "warn", Some(3)), // greatest wins
            (10, Some("api"), "a", "error", Some(2)),
            (10, Some("api"), "b", "info", None),    // a lone NULL-stamped row survives
            (10, Some("web"), "a", "info", Some(9)), // same (ts,id), OTHER service: distinct
            (20, None, "c", "info", Some(1)),        // NULL service is its own key
            (20, None, "c", "info", Some(1)),        // ...and ties keep one row
            (30, Some("api"), "d", "info", Some(5)), // tombstone-shaped: retained either way
        ];
        let batch = batch_of(vec![
            ("timestamp", Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()).with_timezone("UTC")) as ArrayRef),
            ("resource___service___name", Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>())) as ArrayRef),
            ("id", Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>())) as ArrayRef),
            ("level", Arc::new(StringArray::from(rows.iter().map(|r| r.3).collect::<Vec<_>>())) as ArrayRef),
            ("updated_at", Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.4).collect::<Vec<_>>()).with_timezone("UTC")) as ArrayRef),
        ]);

        let columns = schema.fields.iter().map(|field| crate::rollup::quoted(&field.name)).join(", ");
        let keys = quoted_csv(&schema.dedup_keys);
        let order_by = schema_order_by_clause(&schema);
        let run = |sql: String| {
            let batch = batch.clone();
            async move {
                // One row per batch: every run straddles a boundary.
                let ctx = datafusion::prelude::SessionContext::new_with_config(datafusion::prelude::SessionConfig::new().with_batch_size(1));
                ctx.register_batch("scan", batch).expect("register");
                ctx.sql(&sql).await.expect("plan").collect().await.expect("run")
            }
        };
        let render = |batches: Vec<RecordBatch>| {
            datafusion::arrow::util::pretty::pretty_format_batches(&batches.into_iter().filter(|b| b.num_rows() > 0).collect::<Vec<_>>())
                .expect("render")
                .to_string()
        };

        let windowed = render(
            run(format!(
                "SELECT {columns} FROM (SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY \"updated_at\" DESC NULLS LAST) AS __tf_rn FROM scan) WHERE __tf_rn = 1{order_by}"
            ))
            .await,
        );

        let sorted = run(format!("SELECT {columns} FROM scan{order_by}")).await;
        let mut collapse = RunCollapse::new(&sorted[0].schema(), &schema.dedup_keys, schema.dedup_tiebreak.as_deref()).expect("collapse");
        let mut collapsed: Vec<RecordBatch> = sorted
            .into_iter()
            .try_fold(Vec::new(), |mut out, batch| -> Result<Vec<RecordBatch>> {
                out.extend(collapse.push(batch)?);
                Ok(out)
            })
            .expect("collapse stream");
        collapsed.extend(collapse.finish());

        assert_eq!(render(collapsed), windowed, "the one-pass collapse must reproduce the window's rows exactly");
        assert!(windowed.contains("web"), "the fixture must actually exercise two services sharing (timestamp, id)");
    }

    /// Collapse `(timestamp, id, level)` rows keyed by `(timestamp, id)` in
    /// batches of `per_batch`, auditing `level` only when `audited` — the audit
    /// is opt-in and an un-armed collapse must stay silent.
    fn collapse_disagreements(rows: &[(i64, &str, Option<&str>)], audited: bool, per_batch: usize) -> u64 {
        let arrow = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("level", DataType::Utf8, true),
        ]));
        let collapse = RunCollapse::new(&arrow, &["timestamp".to_string(), "id".to_string()], None).expect("collapse");
        let mut collapse = if audited { collapse.with_immutable_audit(&arrow, &["level".to_string()]).expect("audit") } else { collapse };
        for chunk in rows.chunks(per_batch.max(1)) {
            let batch = RecordBatch::try_new(
                Arc::clone(&arrow),
                vec![
                    Arc::new(Int64Array::from(chunk.iter().map(|row| row.0).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(chunk.iter().map(|row| Some(row.1)).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(chunk.iter().map(|row| row.2).collect::<Vec<_>>())),
                ],
            )
            .expect("batch");
            collapse.push(batch).expect("push");
        }
        collapse.finish();
        collapse.disagreements()
    }

    /// The streaming audit must count a dedup key ONCE when its versions
    /// disagree on an immutable column, across both disagreement shapes, and
    /// with every run straddling a batch boundary (the carry path, where a
    /// per-run flag can double-count or reset). The last case pins that an
    /// un-armed collapse counts nothing — the audit is opt-in.
    #[test_case(&[(10, "a", Some("info")), (10, "a", Some("warn"))], true => 1 ; "two different non-null values")]
    #[test_case(&[(10, "a", None), (10, "a", Some("info"))], true => 1 ; "enrichment: absent then filled")]
    #[test_case(&[(10, "a", Some("info")), (10, "a", Some("info"))], true => 0 ; "versions that agree")]
    #[test_case(&[(10, "a", None), (10, "a", None)], true => 0 ; "null in every version agrees")]
    #[test_case(&[(10, "a", Some("info")), (20, "b", Some("warn"))], true => 0 ; "different keys never disagree")]
    #[test_case(&[(10, "a", Some("x")), (10, "a", Some("y")), (10, "a", Some("z"))], true => 1 ; "a three-way disagreement counts once")]
    #[test_case(&[(10, "a", Some("x")), (10, "a", Some("y")), (20, "b", Some("p")), (20, "b", Some("q"))], true => 2 ; "two disagreeing keys count separately")]
    #[test_case(&[(10, "a", Some("info")), (10, "a", Some("warn"))], false => 0 ; "a collapse without the audit counts nothing")]
    fn the_collapse_audit_counts_disagreeing_keys(rows: &[(i64, &str, Option<&str>)], audited: bool) -> u64 {
        // One row per batch, so every run is carried across a boundary; the
        // whole-batch form (which closes runs inside `push`) must agree.
        let carried = collapse_disagreements(rows, audited, 1);
        assert_eq!(collapse_disagreements(rows, audited, rows.len()), carried, "the count cannot depend on batch boundaries: {rows:?}");
        carried
    }

    /// The streaming audit arms by resolving column NAMES against the rewrite's
    /// output schema and disarms silently when none resolve, so a rename would
    /// leave the counter reading zero for the wrong reason.
    #[test]
    fn the_streaming_audit_arms_against_the_real_rewrite_schema() {
        let schema = logs_schema();
        // The rewrite selects every schema field, so this is `plan.schema()`.
        let arrow = Schema::new(schema.fields.iter().map(|field| Field::new(&field.name, DataType::Utf8, true)).collect::<Vec<_>>());
        let columns = Database::immutable_audit_columns(schema);
        let collapse = RunCollapse::new(&arrow, &schema.dedup_keys, schema.dedup_tiebreak.as_deref())
            .expect("collapse")
            .with_immutable_audit(&arrow, &columns)
            .expect("audit");

        assert!(collapse.is_auditing(), "the audit must arm against the real rewrite output schema, or it silently measures nothing");
    }

    /// Both audit forms must read the SAME column list, or the streaming path
    /// silently audits a different set than the SQL path it replaces. The size
    /// assertion is also what pins that the SQL form audits many columns.
    #[test]
    fn both_audit_forms_read_one_column_list() {
        let columns = Database::immutable_audit_columns(logs_schema());
        let sql = audit_sql();
        assert!(columns.len() > 50, "the real schema audits many columns, or this test proves nothing: {}", columns.len());
        for column in &columns {
            assert!(sql.contains(&format!("MIN(\"{column}\")")), "{column} is audited by the streaming form but absent from the SQL form");
        }
    }
}
