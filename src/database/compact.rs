//! OPTIMIZE / compaction: Z-order, hot-tail, sealed-partition, and dedup rewrites.
use super::*;

impl Database {
    /// Optimize the Delta table using Z-ordering on timestamp and id columns
    /// This improves query performance for time-based queries
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, _target_size: Option<i64>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let window_hours = self.config.maintenance.timefusion_optimize_window_hours.max(1);

        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Candidate date partitions in the window (today .. today-num_days).
        let now = Utc::now();
        let today = now.date_naive();
        let num_days = (window_hours / 24).max(1);
        // Cold consolidation (daily) owns sealed partitions older than
        // `cold_optimize_after_days` and bin-packs them to the 512MB target.
        // Exclude them from the 30-min warm Z-order so it can't fragment those
        // cold files back to the warm target every cycle (oscillation = wasted
        // S3 I/O). With after_days=1 this leaves warm processing only today —
        // the partition still taking writes.
        let after_days = self.config.parquet.cold_optimize_after_days();
        // Cold consolidation owns sealed dates; light optimization owns today
        // to preserve its event-time-disjoint runs.
        let skip_today = self.config.maintenance.timefusion_light_optimize_enabled;
        let window_dates: Vec<chrono::NaiveDate> = (0..=num_days)
            .map(|days_ago| (now - chrono::Duration::days(days_ago as i64)).date_naive())
            .filter(|d| !(Self::date_is_cold(today, *d, after_days) || skip_today && *d == today))
            .collect();

        // Snapshot the current live file set once: drives both the ZOrder
        // idempotence guard (below) and PR #39's warm/evict (`pre_uris`).
        let all_uris: Vec<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let table_url = table_clone.table_url().to_string();
        let current = Self::filesets_for_dates(&all_uris, &window_dates);

        // Pre-state file set, used to derive the files this optimize *adds*
        // (to warm) and *removes* (to evict) — see warm/evict_cache_for_uris.
        // Reuses (moves) the walk above instead of a second copy, and is hoisted
        // out of the OCC retry loop below: the live file set only changes on a
        // *successful* commit, which returns.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let pre_uris: Option<std::collections::HashSet<String>> = track_files.then(|| all_uris.into_iter().collect());

        // Keep the active partition at the light-compaction target. A single
        // day-sized file would make 1h and 3h predicates select the same file
        // even when timestamp ordering makes their row groups disjoint.
        let target_size = if window_dates.contains(&today) {
            self.config.maintenance.timefusion_light_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        };

        // delta-rs ZOrder has NO idempotence guard (unlike Compact it does no
        // size / single-file / already-sorted check): it rewrites every file in
        // the selected partitions on every run, even sealed days that didn't
        // change — and PR #39 then has to re-warm all those cold rewrites. Skip
        // any partition whose live file set is identical to the last successful
        // optimize. `today` is always processed (growing leading edge).
        let kept_dates: Vec<chrono::NaiveDate> = {
            let guard = self.zorder_filesets.read().await;
            let prev = guard.get(&table_url);
            window_dates
                .iter()
                .filter(|d| match current.get(*d) {
                    None => false,
                    Some(cur) if cur.is_empty() => false,
                    Some(cur) => **d == today || prev.and_then(|m| m.get(*d)).map(|p| p != cur).unwrap_or(true),
                })
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
        // prune whole files and row groups. It remains an incident kill switch.
        let (optimize_type, declare_sorted) = full_optimize_type(schema, self.config.maintenance.timefusion_optimize_sort_by);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        // SortBy materializes large Arrow buffers, so in-server bins are serial.
        let optimize_concurrency = if declare_sorted { 1 } else { self.config.derived.optimize_merge_tasks() };

        // Best-effort: retry bounded OCC conflicts against a fresh snapshot,
        // but never pause flushes (see optimize_table_light). This preserves
        // ingestion latency and prevents maintenance from running unbounded.
        //
        // Hold a maintenance-rewrite permit across the .optimize() — this is
        // the HEAVIEST rewrite (full-window ZOrder/Compact materializing a
        // large pool-invisible Arrow set), so leaving it outside the
        // concurrency cap would let it stack with a dedup/recompress and
        // reproduce the cgroup OOM the cap exists to prevent (prod 2026-07-04).
        // Scoped to the optimize call so the post-commit warm/evict bookkeeping
        // below runs without the permit.
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
                        .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                        .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                        .with_max_concurrent_tasks(optimize_concurrency)
                        .with_writer_properties(writer_properties.clone())
                        .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
                        .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                        // Avoid the BinaryView read for Variant columns (same issue as
                        // optimize_table_light); delta-rs's internal session defaults to
                        // schema_force_view_types=true.
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
                // Record the post-commit file set for the partitions we
                // rewrote so the next run skips them if nothing changes. Done
                // before the min_files early-return so state stays consistent
                // even when we don't adopt the new handle (delta-rs has already
                // committed the rewrite by this point regardless).
                {
                    let new_uris: Vec<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                    let new_sets = Self::filesets_for_dates(&new_uris, &kept_dates);
                    let mut guard = self.zorder_filesets.write().await;
                    let entry = guard.entry(table_url.clone()).or_default();
                    for d in &kept_dates {
                        entry.insert(*d, new_sets.get(d).cloned().unwrap_or_default());
                    }
                }
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
                    let compression_ratio = metrics.num_files_removed as f64 / metrics.num_files_added as f64;
                    info!("Optimization compression ratio: {:.2}x", compression_ratio);
                }
                // Swap the optimized table in and refresh the cache (warm
                // newly-added files, evict tombstoned ones). Returns the new
                // live file URIs for the tantivy GC hook below.
                let live_uris = self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &[]).await;
                // Tantivy compaction reindex + GC. Order matters: build
                // indexes for the compaction's OUTPUT files first, then GC the
                // inputs' entries — so window coverage never regresses (the
                // pre-existing gap where GC deleted indexes nothing rebuilt
                // left old windows permanently un-prefiltered). Best-effort:
                // errors are logged; the coverage gate keeps queries correct.
                if let Some(svc) = self.tantivy_indexer().cloned()
                    && svc.config.is_table_indexed(table_name)
                {
                    use crate::tantivy::search::{parquet_rel_of_uri, project_id_of_uri};
                    let delta_store = { table_ref.read().await.log_store().object_store(None) };
                    let added: Vec<(String, String, String)> = live_uris
                        .iter()
                        // `None` (file tracking off) behaves as the empty pre-set,
                        // exactly as before: every live parquet is treated as new.
                        .filter(|u| !pre_uris.as_ref().is_some_and(|p| p.contains(*u)) && u.ends_with(".parquet"))
                        .filter_map(|u| Some((project_id_of_uri(u)?.to_string(), parquet_rel_of_uri(u)?.to_string(), u.clone())))
                        .collect();
                    let mut built = 0usize;
                    let mut reindex_errs = 0usize;
                    let table_owned = table_name.to_string();
                    let mut jobs = futures::stream::iter(added.into_iter().map(|(pid, rel, uri)| {
                        let (svc, store, table) = (svc.clone(), delta_store.clone(), table_owned.clone());
                        async move { svc.build_index_for_file(&table, &pid, &rel, &uri, store).await }
                    }))
                    .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1));
                    while let Some(r) = jobs.next().await {
                        match r {
                            Ok(()) => built += 1,
                            Err(e) => {
                                reindex_errs += 1;
                                warn!("tantivy post-optimize reindex failed for table={}: {}", table_name, e);
                            }
                        }
                    }
                    drop(jobs);
                    if built > 0 || reindex_errs > 0 {
                        info!("tantivy post-optimize reindex: table={} built={} errors={}", table_name, built, reindex_errs);
                    }
                }
                // Drop sidecar index entries for files rewritten away.
                if let Some(svc) = self.tantivy_indexer().cloned() {
                    let svc_table = table_name.to_string();
                    // Manifests are keyed by the project uuid taken from the
                    // parquet URI at build time — enumerate them rather than
                    // guessing (a fixed "default"+customs list never visited
                    // unified tenants' manifests, so their stale entries
                    // outlived every compaction until the nightly reconcile).
                    let project_ids = match crate::tantivy::list_manifest_projects(svc.object_store.as_ref(), table_name).await {
                        Ok(pids) => pids,
                        Err(e) => {
                            warn!("tantivy gc: manifest enumeration failed for {}: {}", table_name, e);
                            Vec::new()
                        }
                    };
                    for pid in project_ids {
                        match svc.gc_after_compaction(&svc_table, &pid, &live_uris).await {
                            Ok(report) if report.entries_removed > 0 => {
                                info!(
                                    "tantivy gc: project={} table={} removed={} kept={} blobs_deleted={}",
                                    pid, svc_table, report.entries_removed, report.kept, report.blobs_deleted
                                );
                            }
                            Ok(_) => {}
                            Err(e) => warn!("tantivy gc failed for project={} table={}: {}", pid, svc_table, e),
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
    pub(crate) fn filesets_for_dates(uris: &[String], dates: &[chrono::NaiveDate]) -> HashMap<chrono::NaiveDate, std::collections::HashSet<String>> {
        let markers: Vec<(chrono::NaiveDate, String)> = dates.iter().map(|d| (*d, format!("date={d}"))).collect();
        let mut out: HashMap<chrono::NaiveDate, std::collections::HashSet<String>> = dates.iter().map(|d| (*d, std::collections::HashSet::new())).collect();
        for uri in uris {
            if let Some((d, _)) = markers.iter().find(|(_, marker)| uri.contains(marker)) {
                out.entry(*d).or_default().insert(uri.clone());
            }
        }
        out
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
            .fold(std::collections::HashMap::<&str, usize>::new(), |mut counts, project_id| {
                *counts.entry(project_id).or_default() += 1;
                counts
            });
        // Most-fragmented partition first: it's the one whose recent-window
        // queries open the most files, so it benefits most from an early tick.
        let mut projects: Vec<_> = counts.into_iter().collect();
        projects.sort_unstable_by(|(a, a_count), (b, b_count)| b_count.cmp(a_count).then_with(|| a.cmp(b)));
        projects.into_iter().map(|(project_id, _)| project_id.to_owned()).collect()
    }

    /// Select the specific files a light optimize should bin-pack.
    ///
    /// Letting `OptimizeBuilder` rewrite the whole `date=today` partition records a read predicate
    /// spanning the live tail, so every concurrent ingest flush trips the OCC conflict checker and
    /// the commit loses. Instead, pick only already-flushed small files up to `target_size`, plus at
    /// most one existing sorted run to merge into, and hand that exact set to `with_binned_files`.
    /// Appends that land after selection are not in the set, so they do not conflict.
    ///
    /// `sorted_run_cap` bounds which already-tagged sorted runs are re-admitted to packing. The cold
    /// tier passes `i64::MAX` because its leveled re-merge folds any sub-target run. The hot tier
    /// passes `target/4` so each tick's small output run folds into the next pack until it reaches
    /// ~1/4 target; otherwise a busy project accrues one run per tick. Files >= 7/8 target are
    /// always excluded as converged, because re-selecting one alone would rewrite it 1→1 forever.
    async fn light_optimize_tail(
        table: &DeltaTable, filters: &[PartitionFilter], target_size: i64, min_files: usize, sorted_run_cap: i64,
    ) -> Result<Vec<String>> {
        let adds: Vec<_> = table.get_active_add_actions_by_partitions(filters).try_collect::<Vec<_>>().await?;
        let tail: Vec<TailAdd> = adds
            .iter()
            .filter(|add| add.size() < target_size.max(1)) // cheap gate before the stats parse
            .map(|add| TailAdd::from_stats(add.path().to_string(), add.size(), is_sorted_run(&add.tags()), add.stats().as_deref()))
            .collect();
        Ok(select_tail_bin(&tail, target_size, min_files, sorted_run_cap, seal_micros_now(), TailPass::Pack))
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
            // Tag-first: every exclusion below is pure metadata, so a converged
            // file, an over-cap sorted run, or an already-sorted sealed file
            // never reaches the stats parse.
            .filter_map(|file| {
                let (size, sorted_run) = (file.size(), is_sorted_run(&file.tags()));
                hot_bin_admits(&file.path(), &date_marker, &repair_markers, size, sorted_run, repairable, policy).then_some(())?;
                // `stats()` is reached only past both tag/size exclusions.
                let path = file.path();
                let project_id = path_partition_value(&path, "project_id").filter(|p| !p.is_empty()).map(str::to_owned)?;
                Some((project_id, TailAdd::from_stats(path.into_owned(), size, sorted_run, file.stats().as_deref())))
            })
            .fold(HashMap::<String, Vec<TailAdd>>::new(), |mut per_project, (project_id, add)| {
                per_project.entry(project_id).or_default().push(add);
                per_project
            });
        let mut planned: Vec<(String, Vec<String>, usize)> = per_project
            .into_iter()
            .map(|(project_id, adds)| {
                let debt = adds.len();
                (project_id, select_tail_bin(&adds, policy.target_size, policy.min_files, policy.sorted_run_cap, seal, policy.pass), debt)
            })
            .filter(|(_, bin, _)| !bin.is_empty())
            .collect();
        // Packing goes MOST-fragmented first — that partition opens the most
        // files per query, so it is the most urgent. Repair inverts it:
        // SHORTEST-JOB-FIRST.
        //
        // A repair backlog is finite per project, so the goal is to finish
        // projects, not to nibble at the biggest. A project with 3 candidates
        // can be made clean — and its users unblocked — inside one tick; one
        // with 300 cannot be finished in any tick, and putting it first means it
        // holds the (deliberately narrow) repair slots for the whole pass while
        // everyone else waits behind the wave barrier.
        //
        // Prod 2026-08-08, exactly that: 65 minutes into a 144-minute pass, the
        // two tenants whose users were actually blocked — 3 and 28 candidates —
        // had not been served at all, because the whale project's hundreds of
        // candidates sorted first and its multi-GB rewrites occupied both slots.
        // Widening the admission reach the same morning made it worse, by
        // promoting that project's 1.6-2.3 GB files from "ineligible" to
        // "eligible and first in line".
        match policy.pass {
            TailPass::Pack => planned.sort_unstable_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0))),
            TailPass::Repair => planned.sort_unstable_by(|a, b| a.2.cmp(&b.2).then_with(|| a.0.cmp(&b.0))),
        }
        Ok(planned.into_iter().map(|(project_id, bin, _)| (project_id, bin)).collect())
    }

    /// `[min, max]` event time (micros) of a file from its raw Add stats JSON.
    /// Timestamp stats serialize as RFC3339 strings (epoch numbers accepted for
    /// long-typed columns).
    pub(crate) fn event_time_range_from_stats(stats: &str) -> Option<(i64, i64)> {
        let stats: serde_json::Value = serde_json::from_str(stats).ok()?;
        let get = |key: &str| {
            let v = &stats[key]["timestamp"];
            v.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_micros()).or_else(|| v.as_i64())
        };
        Some((get("minValues")?, get("maxValues")?))
    }

    /// Partition-ownership boundary between the warm (30-min Z-order) and cold
    /// (daily 512MB consolidate) tiers: a `date` is cold-owned once it's at least
    /// `after_days` older than `today`. The warm optimize processes the
    /// complement, so the two tiers never rewrite the same partition (no
    /// 256MB↔512MB oscillation). Single source of truth for both schedulers.
    pub(crate) fn date_is_cold(today: chrono::NaiveDate, date: chrono::NaiveDate, after_days: u64) -> bool {
        (today - date).num_days() >= after_days as i64
    }

    /// Compacted-file target by partition age (calendar-based): sealed days
    /// consolidate to the larger cold target (fewer files → smaller checkpoint
    /// → faster commits); the current day stays at the warm target so a
    /// still-filling partition isn't rewritten to the cold target repeatedly.
    fn optimize_target_for_date(&self, date: chrono::NaiveDate) -> i64 {
        if Self::date_is_cold(Utc::now().date_naive(), date, self.config.parquet.cold_optimize_after_days()) {
            self.config.parquet.timefusion_cold_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        }
    }

    /// Compact a single `date=` partition by bin-packing its small files
    /// (`Compact`, not Z-order — a pure row-group merge that preserves
    /// Variant/Binary column bytes). Powers the on-demand `OPTIMIZE <table>
    /// WHERE date = '...'` pgwire command and the `optimize` CLI subcommand
    /// (the daily cold sweep uses `consolidate_date_binned` for event-time
    /// disjoint runs). Target size scales with partition age
    /// (`optimize_target_for_date`). Commits once; returns (removed, added).
    pub async fn compact_date(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>,
    ) -> Result<(u64, u64)> {
        self.compact_date_with(table_ref, table_name, date, project_id, self.config.derived.optimize_merge_tasks()).await
    }

    /// `compact_date` with an explicit bin concurrency (off-box CLI
    /// `--concurrency N`); `None` keeps the in-server default.
    pub async fn compact_date_concurrent(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>, concurrency: Option<usize>,
    ) -> Result<(u64, u64)> {
        let n = concurrency.unwrap_or_else(|| self.config.derived.optimize_merge_tasks()).max(1);
        self.compact_date_with(table_ref, table_name, date, project_id, n).await
    }

    /// `compact_date` with an explicit merge concurrency. The cold consolidation
    /// sweep passes 1: a 512MB-target merge holds ~target-sized output buffers per
    /// task, so concurrency × 512MB can OOM the memory-tight in-process instance
    /// (the off-box recipe uses concurrency 1 for the same reason). The on-demand
    /// pgwire/CLI callers keep the configured concurrency.
    async fn compact_date_with(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>, max_concurrent: usize,
    ) -> Result<(u64, u64)> {
        let target_size = self.optimize_target_for_date(date);
        let schema = schema_or_default(table_name);
        let mut partition_filters = vec![PartitionFilter::try_from(("date", "=", date.to_string().as_str()))?];
        // Scope to one tenant when asked: a whole date spans every project's
        // files (tens of GB on a busy day — doesn't fit in-process), one
        // (project, date) partition is a few GB.
        if let Some(pid) = project_id {
            partition_filters.push(PartitionFilter::try_from(("project_id", "=", pid))?);
        }
        // Retry OCC/transient S3 errors; reset the no-progress budget only when
        // committed bins reduce the scoped file count.
        const MAX_ATTEMPTS: usize = 4;
        const TOTAL_ATTEMPTS: usize = 32;
        // Pre-state file set for the warm/evict diff, hoisted out of the retry
        // loop (only a successful commit — which returns — changes it) and
        // scoped to the partition being compacted: `optimize().with_filters()`
        // can only add/remove files under these markers, so diffing the whole
        // table's URI set was pure waste. `None` when neither warm- nor
        // evict-after-compaction is on, so the walk is skipped outright.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let scope: Vec<String> = std::iter::once(format!("date={date}/")).chain(project_id.map(|pid| format!("project_id={pid}/"))).collect();
        let scope: Vec<&str> = scope.iter().map(String::as_str).collect();
        let pre_uris: Option<std::collections::HashSet<String>> =
            if track_files { Some(scoped_file_uris(&*table_ref.read().await, &scope).into_iter().collect()) } else { None };
        let mut scope_files = scoped_file_uris(&*table_ref.read().await, &scope).len();
        let (mut attempt, mut total_attempts) = (0usize, 0usize);
        loop {
            // The snapshot is refreshed in the Err arm (needed there anyway for
            // the progress check), so every retry re-plans against fresh state.
            let table_clone = { table_ref.read().await.clone() };
            // SortBy: sort the partition by the schema keys and declare it, so
            // cold/consolidated partitions keep an honest DESC footer for the
            // ordering pushdown (plain Compact concatenates → declare false).
            // SortBy reads via the ordering-advertising DeltaScanNext: over
            // already-sorted files `df.sort()` collapses to a streaming
            // SortPreservingMergeExec (bounded k-way merge). The one exception
            // is a partition still holding legacy pre-sort files — its first
            // rewrite is a one-time blocking sort. Force concurrency 1 on the
            // SortBy path so those transition sorts can't stack and exhaust the
            // maintenance pool (the 2026-07-14 OOM multiplier); steady-state
            // SortBy is cheap SPM, so serializing partitions costs little.
            let (optimize_type, declare_sorted) = choose_optimize_type(schema, false, self.config.maintenance.timefusion_optimize_sort_by);
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
            // Serialise SortBy at in-server concurrency; explicit off-box
            // concurrency opts into parallel transition sorts.
            let sort_concurrency = if declare_sorted && max_concurrent <= self.config.derived.optimize_merge_tasks() { 1 } else { max_concurrent };
            let result = table_clone
                .optimize()
                .with_filters(&partition_filters)
                .with_type(optimize_type)
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(sort_concurrency)
                .with_writer_properties(writer_properties)
                // 2min (was 10): bins run serially on the SortBy path, so a
                // short interval banks incremental commits — an OCC loss to a
                // concurrent dedup/flush costs one bin's work, not the whole
                // partition (2026-07-14 all-or-nothing starvation).
                .with_min_commit_interval(tokio::time::Duration::from_secs(2 * 60))
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                // Variant columns: same BinaryView-avoidance session as optimize_table.
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
                    // Progress check: a failed attempt whose banked bin commits
                    // shrank the partition resets the no-progress budget (needs
                    // a fresh snapshot; the retry-refresh above is skipped when
                    // we bail, so refresh here before counting).
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
                                // Exponential backoff — matches dedup_partition. Zero-delay
                                // retries under concurrent heavy ingest amplify contention.
                                tokio::time::sleep(occ_backoff(attempt.max(1) - 1)).await;
                            } else {
                                // A multipart part connection-dropped mid-merge (nothing committed).
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
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        let dates: std::collections::BTreeSet<chrono::NaiveDate> = uris
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
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        Ok(Self::hot_project_ids(&uris, date))
    }

    /// Rewrite a date partition at a higher ZSTD level using Z-order (or Compact if no
    /// z-order columns).
    ///
    /// Skips partitions whose probe file already advertises a tier >= `target_level` via Parquet
    /// footer metadata. Probes only one file per partition: every file in a successfully
    /// recompressed partition shares the same tier. A partial rewrite can leave mixed tiers; the
    /// next sweep may skip based on the probe, but the partition is re-evaluated the next day.
    /// `project` scopes the rewrite to one `project_id=` partition, which is the honest unit of
    /// repair.
    pub async fn recompress_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_level: i32, project: Option<&str>,
    ) -> Result<RecompressOutcome> {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};

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

        // Recompress rewrites whole partitions — same pool-invisible Arrow
        // materialization as dedup/optimize; hold a maintenance-rewrite permit.
        // Acquired after the empty-partition early-out so no-op calls are free.
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;

        // Probe one file's footer KV metadata. URIs returned by delta-rs are
        // absolute (s3://bucket/...); the table's object_store is rooted at
        // table_uri, so the relative key is the URI with that prefix stripped.
        // `table_url()` may include a `?endpoint=...` query string (non-AWS
        // backends like MinIO) which `get_file_uris()` does not — strip it
        // before matching.
        let probe_uri = &uris[0];
        let table_prefix = table_uri.split('?').next().unwrap_or(&table_uri).trim_end_matches('/');
        let probe_tier = match probe_uri.strip_prefix(table_prefix).and_then(|s| s.strip_prefix('/').or(Some(s))) {
            Some(rel) => {
                let object_store = log_store.object_store(None);
                let path = OsPath::from(rel);
                // `head()` returns `meta.location` relative to the bucket,
                // but `ParquetObjectReader` consumes object-store-relative
                // paths and would double-prefix. Pass our original `path`.
                match object_store.head(&path).await {
                    Ok(meta) => {
                        let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone()).with_file_size(meta.size);
                        reader.get_metadata(None).await.ok().and_then(|pq| {
                            pq.file_metadata().key_value_metadata().and_then(|kvs| {
                                kvs.iter().find(|kv| kv.key == COMPRESSION_TIER_KEY).and_then(|kv| kv.value.as_ref()).and_then(|v| v.parse::<i32>().ok())
                            })
                        })
                    }
                    Err(e) => {
                        warn!("recompress probe: head failed for {}: {}; rewriting anyway", probe_uri, e);
                        None
                    }
                }
            }
            None => {
                warn!("recompress probe: could not relativize {} against {}; rewriting anyway", probe_uri, table_prefix);
                None
            }
        };

        // A partition holding an UNSORTED file is never "done", whatever its
        // compression tier. The tier probe alone made this unreachable: prod
        // 2026-08-07 had a 924 MB file with `sorting_columns=()` stamped
        // tier 9, so every `--recompress` at level <= 9 skipped it — and then
        // printed success. That file is exactly what this command exists to
        // repair, and it voided the declared ordering for every scan of the
        // last 30 days. Probe the footers (a ranged read each, and the same
        // metadata cache the scan would warm) and let sortedness veto the skip.
        let declares_order = get_schema(table_name).is_some_and(|s| !s.sorting_columns.is_empty());
        // A tier-qualified file without a sorted footer is not converged.
        let any_unsorted = match declares_order {
            false => false,
            true => {
                let object_store = log_store.object_store(None);
                let mut found = false;
                for uri in &uris {
                    let Some(rel) = uri.strip_prefix(table_prefix).map(|s| s.trim_start_matches('/')) else { continue };
                    let path = OsPath::from(rel);
                    let Ok(meta) = object_store.head(&path).await else { continue };
                    let mut reader = ParquetObjectReader::new(object_store.clone(), path).with_file_size(meta.size);
                    // An unreadable footer is not evidence of sortedness; leave
                    // `found` alone and let the tier probe decide.
                    if let Ok(pq) = reader.get_metadata(None).await
                        && pq.row_groups().iter().any(|rg| rg.sorting_columns().is_none_or(|sc| sc.is_empty()))
                    {
                        found = true;
                        break;
                    }
                }
                found
            }
        };

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
        // Sort and declare footer order when enabled; otherwise stream `SELECT *`.
        let order_by = if self.config.maintenance.timefusion_optimize_sort_by { schema_order_by_clause(schema) } else { String::new() };
        let declare_sorted = !order_by.is_empty();
        let writer_properties = self.create_writer_properties(schema, target_level, declare_sorted);
        let target_size = self.config.parquet.timefusion_optimize_target_size;

        // Force a full-partition rewrite at the new zstd tier via a streaming
        // `replace_where` overwrite — NOT Z-order. delta-rs `Compact` skips
        // files already ≥ target and drops single-file bins, so it can't lift
        // an already-consolidated partition's tier; Z-order *can* force the
        // rewrite but its space-filling curve scatters `timestamp` across row
        // groups, wrecking the dominant time-range predicate's pruning. Instead
        // we read the partition (`date = X`, all project_ids) and write it back
        // with `SaveMode::Overwrite` + `replace_where`, which atomically
        // Remove-tombstones the old files and Adds the recompressed ones
        // (data_change semantics preserved). `with_input_plan` streams the scan
        // through the writer (bounded by target_file_size) rather than
        // materializing the whole partition, so peak memory matches a normal
        // flush — unlike Z-order's global sort. The scan runs on the
        // variant-safe maintenance session (no `variant_to_json` wrap), so
        // Variant columns round-trip as raw Struct. Decoupling from
        // `z_order_columns` lets the schema keep that list empty for queries.
        let (snapshot, log_store, table_clone) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store(), table.clone())
        };
        let pre_uris: std::collections::HashSet<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();

        let provider = deltalake::delta_datafusion::TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("recompress scan provider: {e}"))?;
        // Must be the delta *write* session (carries DeltaPlanner): the write
        // wraps its input in a MetricObserver node only that planner can
        // physically plan. It now also reserves sort-spill memory so the added
        // ORDER BY spills rather than erroring on a large partition.
        let session = build_delta_write_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env());
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
                // Swap + warm-added/evict-removed like the other optimize
                // paths. A bare swap left the rewritten cold-tier files
                // un-warmed and the tombstoned ones cached — the next query
                // on a recompressed partition paid full S3 reads (1.5 s
                // observed against OVH).
                self.swap_and_refresh_cache(table_ref, new_table, Some(&pre_uris), &[]).await;
                Ok(RecompressOutcome::Rewritten { files: uris.len() })
            }
            Err(e) => {
                error!("recompress failed for date={} table={}: {}", date_str, table_name, e);
                Err(anyhow::anyhow!("recompress failed: {}", e))
            }
        }
    }

    /// Sweep partitions in [age_min_days, age_max_days) and recompress any
    /// whose probe tier is below `target_level`. Iterates day-by-day; each
    /// day's optimize is its own Delta commit so a mid-sweep failure leaves
    /// completed days at the new tier.
    pub async fn recompress_tier_window(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, age_min_days: u64, age_max_days: u64, target_level: i32,
    ) -> Result<()> {
        let today = Utc::now().date_naive();
        for days_ago in age_min_days..age_max_days {
            let date = today - chrono::Duration::days(days_ago as i64);
            if let Err(e) = self.recompress_partition(table_ref, table_name, date, target_level, None).await {
                warn!("recompress_tier_window: skipping date={} after error: {}", date, e);
            }
        }
        Ok(())
    }

    /// Daily cold consolidation: bin-pack every sealed partition (date older
    /// than `cold_optimize_after_days`) toward the 512MB cold target. Calendar-age
    /// driven and idempotent — converged runs are excluded from re-selection,
    /// so already-consolidated partitions cost a snapshot scan, not a rewrite
    /// (bounds S3 I/O across the whole cold backlog). Covers "previous days and
    /// further", picking up backfill that landed in old partitions.
    pub async fn consolidate_sealed_partitions(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        let today = crate::support::today_utc();
        let after_days = self.config.parquet.cold_optimize_after_days();
        let dates: Vec<chrono::NaiveDate> = self.partition_dates(table_ref).await?.into_iter().filter(|d| Self::date_is_cold(today, *d, after_days)).collect();
        info!("consolidate: table={} sweeping {} sealed partition(s) older than {}d", table_name, dates.len(), after_days);
        for date in dates {
            let target = self.optimize_target_for_date(date);
            if let Err(e) = self.consolidate_date_binned(table_ref, table_name, date, target, None, usize::MAX).await {
                warn!("consolidate: skipping date={} after error: {}", date, e);
            }
        }
        Ok(())
    }

    /// Incremental catch-up for the cold sweep, for partitions it has not reached.
    ///
    /// `consolidate_sealed_partitions` runs once a day and sweeps every cold date in one long job,
    /// which only helps if the process survives the whole sweep. Prod restarts frequently, so the
    /// newest sealed day often never gets consolidated. This does the same work from the frequent
    /// tick in a bounded slice: pick the single most fragmented cold partition and give it a few
    /// passes. Each pass is its own commit, so whatever finishes before a restart is kept and the
    /// next tick resumes from the new snapshot. No date can starve.
    pub async fn consolidate_catchup(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, max_passes: usize) -> Result<()> {
        let today = crate::support::today_utc();
        let after_days = self.config.parquet.cold_optimize_after_days();
        let target_of = |d| self.optimize_target_for_date(d);
        // Count only files still BELOW their date's target: a partition of big
        // converged runs is done, however many of them there are, and must not
        // out-rank a genuinely fragmented one.
        let worst = {
            let table = table_ref.read().await;
            table
                .snapshot()?
                .log_data()
                .iter()
                .filter_map(|f| {
                    let path = f.path();
                    let date = path.split('/').find_map(|s| s.strip_prefix("date="))?.parse::<chrono::NaiveDate>().ok()?;
                    let project_id = path.split('/').find_map(|s| s.strip_prefix("project_id="))?.to_owned();
                    (Self::date_is_cold(today, date, after_days) && f.size() < target_of(date)).then_some((date, project_id))
                })
                .fold(HashMap::<(chrono::NaiveDate, String), usize>::new(), |mut acc, key| {
                    *acc.entry(key).or_default() += 1;
                    acc
                })
                .into_iter()
                // Ties break to the NEWEST date: it is the one queries read.
                .filter(|(_, n)| *n >= 2)
                .max_by(|((a_date, a_project), a_n), ((b_date, b_project), b_n)| {
                    a_n.cmp(b_n).then_with(|| a_date.cmp(b_date)).then_with(|| b_project.cmp(a_project))
                })
        };
        let Some(((date, project_id), small_files)) = worst else {
            return Ok(());
        };
        info!(
            "consolidate-catchup: table={} project={} date={} {} small file(s), running up to {} pass(es)",
            table_name, project_id, date, small_files, max_passes
        );
        self.consolidate_date_binned(table_ref, table_name, date, target_of(date), Some(&project_id), max_passes).await
    }

    /// Leveled consolidation of one sealed `date`: per project, repeatedly select the earliest
    /// event-time slice of small files up to the cold target and rewrite it as one sorted run.
    ///
    /// Successive passes take strictly later slices, so output runs are event-time disjoint and
    /// range pruning works. Per-pass memory is bounded by one <= target sort. Converges because
    /// outputs >= 7/8 target are excluded from re-selection. `target_size` and `only_project` are
    /// caller-supplied so the off-box CLI can consolidate a still-hot date for one tenant.
    pub async fn consolidate_date_binned(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_size: i64, only_project: Option<&str>, max_passes: usize,
    ) -> Result<()> {
        let schema = schema_or_default(table_name);
        // This path already bounds each rewrite to one event-time bin at the
        // cold target, so it does not share the whole-partition external-sort
        // hazard guarded by `timefusion_optimize_sort_by`. Its contract is to
        // produce disjoint sorted runs: leaving this behind that global kill
        // switch made the default cold compactor strip ordering from historical
        // files and forced read-side greatest-version dedup to buffer the full
        // scan. Always sort/dedup the bounded bin; whole-partition optimize and
        // recompress remain gated by the kill switch.
        let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        let date_str = date.to_string();
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        // Backstop against a selection that stops shrinking (e.g. a rewrite
        // that keeps losing OCC to a dedup); a normal day converges in
        // partition_bytes/target passes.
        // Backstop for the full sweep; the catch-up caller passes a small budget
        // so one tick's work fits between restarts.
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
    pub async fn dedup_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
    ) -> Result<(u64, bool)> {
        self.dedup_partition_range(table_ref, table_name, project_id, date, None).await
    }

    /// Stage-and-commit one partition (or one 10-minute bin of it) as a SINGLE
    /// wave. Used by the fallback sweep, which has no queue to batch across; the
    /// dirty-bin path stages with [`Self::stage_dedup_partition_range`] directly
    /// so one wave can span many bins.
    async fn dedup_partition_range(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, bin: Option<i64>,
    ) -> Result<(u64, bool)> {
        let slice = bin.map(|bin| crate::maintenance_coordinator::TimeSlice {
            start_micros: bin.saturating_mul(crate::maintenance_coordinator::NORMAL_SLICE_MICROS),
            end_micros: bin.saturating_add(1).saturating_mul(crate::maintenance_coordinator::NORMAL_SLICE_MICROS),
        });
        self.dedup_partition_range_limited(table_ref, table_name, project_id, date, slice, None).await
    }

    pub(crate) async fn dedup_partition_range_limited(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
        slice: Option<crate::maintenance_coordinator::TimeSlice>, limits: Option<DedupExecutionLimits>,
    ) -> Result<(u64, bool)> {
        let options = DedupRangeOptions { slice, dirty_key: None, limits };
        let (units, complete) = self.stage_dedup_partition_range(table_ref, table_name, project_id, date, options).await?;
        if units.is_empty() {
            return Ok((0, complete));
        }
        let markers = vec![format!("date={date}/")];
        let result = self.commit_wave(table_ref, table_name, &markers, true, units, 0).await;
        let dropped = wave_dropped_rows(&result.landed);
        for bin in &result.landed {
            if let Some(d) = &bin.dedup {
                info!("dedup rewrite: table={} chunk=[{}] dropped={} (before={} after={})", table_name, d.label, d.dropped(), d.before, d.after);
            }
        }
        // A unit that didn't land left its duplicates in place — the partition
        // must NOT be certified clean (2026-07-05 review).
        Ok((dropped, complete && result.failed.is_empty()))
    }

    /// Builds a `TableProviderBuilder` scoped to exactly `files` off
    /// `snapshot`/`log_store` — the "scan just these files" shape every
    /// maintenance narrow-scan path uses. `file_col` requests the synthetic
    /// file-identity passthrough column dedup rewrites key on.
    pub(crate) async fn narrow_provider(
        log_store: deltalake::logstore::LogStoreRef, snapshot: Arc<deltalake::kernel::EagerSnapshot>, files: Vec<String>, file_col: Option<&str>,
    ) -> Result<Arc<dyn TableProvider>, deltalake::DeltaTableError> {
        use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};
        let mut builder =
            TableProviderBuilder::default().with_log_store(log_store).with_eager_snapshot(snapshot).with_file_selection(FileSelection::from_file_paths(files));
        if let Some(col) = file_col {
            builder = builder.with_file_column(col);
        }
        Ok(Arc::new(builder.build().await?))
    }

    /// Probe-only DataFusion context over one `(project, date)` partition's snapshot files.
    ///
    /// Bypasses `ProjectRoutingTable`: its MemBuffer union would feed in-flight rows to dedup,
    /// which would then be written to Delta on the next real flush. Restricts provider construction
    /// itself, not just the SQL scan: an unrestricted provider eagerly materializes statistics for
    /// every live file in the unified table before partition pruning. Paths come from this exact
    /// eager snapshot, so the selection cannot omit a file belonging to the project/date being
    /// certified.
    async fn dedup_probe_ctx(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, date_str: &str, limits: Option<DedupExecutionLimits>,
    ) -> Result<datafusion::prelude::SessionContext> {
        let (snapshot, log_store) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };
        let partition_files = dedup_partition_paths(snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
        // Probe-only provider (chunk detection). The rewrite builds its own
        // provider per attempt — from a FRESH snapshot, with the synthetic
        // source-file column — in `dedup_rewrite_chunk`.
        let provider = Self::narrow_provider(log_store, snapshot, partition_files, None).await.map_err(|e| anyhow::anyhow!("delta table provider: {e}"))?;
        // A fresh state is intentional: SessionState clones retain mutable
        // catalog/execution internals and can resolve the scan name to an older
        // eager snapshot. FileSelection above removes the expensive all-table
        // statistics replay that made fresh states harmful in production.
        let state = limits.map_or_else(
            || build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()),
            |_limits| {
                build_optimize_session_state_tuned(
                    self.config.memory.timefusion_query_partitions,
                    self.coordinator_runtime_env(),
                    Some("256"),
                    Some(UncappedSort { partitions: 1, reservation_bytes: Some(32 * 1024 * 1024) }),
                )
            },
        );
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);
        ctx.register_table(DEDUP_SCAN_NAME, provider)?;
        Ok(ctx)
    }

    /// The 10-minute duplicate probe: returns the bucket starts whose
    /// dedup-key groups have count > 1 under `filter`. Aggregates group keys only — bounded by key
    /// cardinality, not row width. A `SELECT *` + `collect()` of a whole day partition transiently
    /// allocated tens of gigabytes outside any memory pool.
    async fn dup_bin_starts(ctx: &datafusion::prelude::SessionContext, filter: &str, keys_csv: &str) -> Result<Vec<chrono::NaiveDateTime>> {
        let probe = format!(
            "SELECT CAST(date_bin(INTERVAL '10 minutes', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00') AS VARCHAR) FROM \
             (SELECT \"timestamp\", count(*) AS c FROM {DEDUP_SCAN_NAME} WHERE {filter} GROUP BY {keys_csv}) AS g \
             WHERE c > 1 GROUP BY 1 ORDER BY 1"
        );
        ctx.sql(&probe).await?.collect().await?.into_iter().try_fold(Vec::new(), |mut starts, batch| {
            let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
            let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
            starts.extend(col.iter().flatten().filter_map(|value| {
                value.get(..19).and_then(|datetime| {
                    chrono::NaiveDateTime::parse_from_str(datetime, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S"))
                        .ok()
                })
            }));
            Ok::<_, anyhow::Error>(starts)
        })
    }

    /// Runs `sql` and pulls its first output row's `column(0)` as an `i64` —
    /// the common "run an aggregate probe, get one scalar back" shape used
    /// throughout the dedup rewrite path. `None` if the result is empty or
    /// `column(0)` isn't an `Int64Array`; callers layer their own
    /// exactly-one-row / non-negative validation on top where needed.
    async fn scalar_i64(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<Option<i64>> {
        let batches = ctx.sql(sql).await?.collect().await?;
        Ok(batches
            .first()
            .filter(|b| b.num_rows() > 0)
            .and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>())
            .map(|a| a.value(0)))
    }

    /// Batch probe: classify every 10-minute bin of one `(project, date)` with a single duplicate
    /// probe, returning the bin ids that contain duplicates.
    ///
    /// A dup group shares one exact `timestamp` (it is a dedup key), so the group's bin is derived
    /// exactly. Only valid when `timestamp` is a dedup key.
    pub(crate) async fn probe_dup_bins(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date_str: &str,
    ) -> Result<std::collections::HashSet<i64>> {
        const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
        let schema = schema_or_default(table_name);
        let ctx = self.dedup_probe_ctx(table_ref, project_id, date_str, None).await?;
        let safe_pid = project_id.replace('\'', "''");
        let filter = format!("project_id = '{safe_pid}' AND date = DATE '{date_str}'");
        let keys_csv = schema.dedup_keys.iter().map(|k| crate::rollup::quoted(k)).collect::<Vec<_>>().join(", ");
        Ok(Self::dup_bin_starts(&ctx, &filter, &keys_csv).await?.into_iter().map(|s| s.and_utc().timestamp_micros() / BIN_MICROS).collect())
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
        // project_id is currently always a UUID/controlled identifier, but defend in depth: escape single quotes
        // so a future caller can't inject SQL through the partition predicate. date_str comes from NaiveDate::to_string
        // and is already safe.
        let safe_pid = project_id.replace('\'', "''");
        // Keep the full partition predicate separate from the dirty-bin probe
        // scope. `stage_dedup_chunk` removes every file touched by the scoped
        // chunk, then re-reads those files with `partition_filter` so rows in
        // adjacent bins survive the replacement. Passing the bin predicate as
        // `partition_filter` silently kept only ten minutes from a multi-bin
        // parquet file and dropped the rest (prod 2026-08-03).
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
        let keys_csv = schema.dedup_keys.iter().map(|k| crate::rollup::quoted(k)).collect::<Vec<_>>().join(", ");

        // Identify the hour buckets that actually contain duplicates. A dup
        // group shares one exact `timestamp` (it's a dedup key), so chunking
        // the rewrite by hour can never split a group — and it bounds the
        // materialization below to one hour of one project instead of the
        // whole day (the crash-loop backlog made EVERY project probe-positive,
        // so the probe alone still ballooned tens of GB per sweep).
        let (chunks, skipped_any): (Vec<(String, String)>, bool) = if schema.dedup_keys.iter().any(|k| k == "timestamp") {
            // Ten-minute sealed bins bound materialization and avoid racing late
            // flushes; newer duplicates are retried later.
            let sealed_before = Utc::now().naive_utc() - chrono::Duration::hours(2);
            let mut skipped_unsealed = 0usize;
            // A one-minute whale cannot be split further in time. Bound the
            // key GROUP BY used by the duplicate probe with the same complete-
            // key hash partitioning as the rewrite. Each pass may reread the
            // selected files, but no pass can accumulate the whale's full key
            // cardinality in memory.
            let probe_shards = limits.map_or(1, |limits| limits.probe_hash_shards.max(1));
            let mut duplicate_starts = Vec::new();
            for shard in 0..probe_shards {
                let shard_filter = if probe_shards == 1 {
                    filter.clone()
                } else {
                    let keys_varchar = schema.dedup_keys.iter().map(|key| format!("CAST(\"{key}\" AS VARCHAR)")).collect::<Vec<_>>().join(", ");
                    let bucket_expr = format!("hash_bucket(arrow_cast(concat_ws(chr(31), {keys_varchar}), 'Utf8View'), {DEDUP_BUCKET_COUNT})");
                    let lo = u64::try_from(shard).unwrap_or(u64::MAX).saturating_mul(DEDUP_BUCKET_COUNT) / u64::try_from(probe_shards).unwrap_or(1);
                    let hi = u64::try_from(shard + 1).unwrap_or(u64::MAX).saturating_mul(DEDUP_BUCKET_COUNT) / u64::try_from(probe_shards).unwrap_or(1);
                    let upper = if hi < DEDUP_BUCKET_COUNT { format!(" AND {bucket_expr} < {hi}") } else { String::new() };
                    format!("{filter} AND {bucket_expr} >= {lo}{upper}")
                };
                duplicate_starts.extend(Self::dup_bin_starts(&ctx, &shard_filter, &keys_csv).await?);
            }
            duplicate_starts.sort_unstable();
            duplicate_starts.dedup();
            let built: Vec<_> = duplicate_starts
                .into_iter()
                .filter_map(|start| {
                    let end = start + chrono::Duration::minutes(10);
                    if slice.is_none() && end > sealed_before {
                        debug!("dedup: skipping unsealed chunk starting {start} (cleared on a later sweep)");
                        skipped_unsealed += 1;
                        return None;
                    }
                    let (s, e) = (start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
                    Some((
                        format!("{filter} AND \"timestamp\" >= TIMESTAMP '{s}' AND \"timestamp\" < TIMESTAMP '{e}'"),
                        // Log label only. The rewrite commits targeted
                        // Remove+Add actions — no replace_where, so no
                        // predicate ever needs kernel evaluation (the old
                        // bare-string predicate defeated file pruning AND
                        // errored delta-kernel's OCC checker).
                        format!("project_id = '{safe_pid}' AND date = '{date_str}' AND timestamp in ['{s}', '{e}')"),
                    ))
                })
                .collect();
            (built, skipped_unsealed > 0)
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
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        let staged: Vec<Result<BinOutcome<StagedBin>>> = futures::stream::iter(chunks.into_iter().map(|(chunk_filter, label)| {
            let (partition_filter, key, date_str) = (&partition_filter, key.clone(), date_str.as_str());
            async move {
                self.stage_dedup_chunk(table_ref, table_name, project_id, schema, scan_name, partition_filter, &chunk_filter, &label, date_str, key, limits)
                    .await
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
                // The chunk's rows vanished / were rewritten concurrently:
                // nothing was verified, so the partition stays uncertified.
                Ok(BinOutcome::Retry) => all_complete = false,
                // Probe false-positive: verified duplicate-free, nothing to commit.
                Ok(BinOutcome::Converged) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            // One chunk's failure abandons the partition's whole staging batch:
            // clean up the siblings' parquet rather than leaking it (their
            // Adds are in no commit and VACUUM would take days to notice).
            self.discard_bins(&units).await;
            return Err(e);
        }
        Ok((units, all_complete))
    }

    /// Stage one duplicate-bearing chunk as a targeted file rewrite.
    ///
    /// Uses the provider's synthetic `DEDUP_FILE_COL` to find which files hold the chunk's rows,
    /// re-reads those files' full row sets, dedups, and writes replacement parquet. Returns
    /// `Remove(old) + Add(new)` actions for a wave commit; this function commits nothing. Batching
    /// chunks under a shared commit lock replaces serial per-chunk commits. Explicit file actions
    /// are used instead of `replace_where` because delta-rs cannot stringify typed TIMESTAMP
    /// literals for the commit predicate.
    #[allow(clippy::too_many_arguments)]
    async fn stage_dedup_chunk(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, schema: &crate::schema::TableSchema, scan_name: &str,
        partition_filter: &str, chunk_filter: &str, label: &str, date_str: &str, key: Option<DirtyBinKey>, limits: Option<DedupExecutionLimits>,
    ) -> Result<BinOutcome<StagedBin>> {
        use deltalake::{kernel::Action, writer::DeltaWriter};
        use futures::StreamExt;
        let read_string_column = |batches: Vec<RecordBatch>| -> Result<Vec<String>> {
            Ok(batches
                .into_iter()
                .map(|batch| -> Result<Vec<String>> {
                    let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
                    let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
                    Ok((0..col.len()).filter(|&i| !col.is_null(i)).map(|i| col.value(i).to_string()).collect())
                })
                .collect::<Result<Vec<Vec<String>>>>()?
                .into_iter()
                .flatten()
                .collect())
        };
        // Re-plan against a fresh snapshot when concurrent rewrites invalidate
        // file mappings; `commit_wave` guards the remaining commit window.
        const MAX_REPLANS: usize = 3;
        for replan in 0..MAX_REPLANS {
            // Scan and file-mapping MUST share one snapshot: the caller's ctx
            // is pinned at dedup_partition entry, and on the heavily-churned
            // unified table the live file set diverges from it within seconds
            // (flush appends + light optimize) — mapping scan results against
            // the LIVE snapshot mismatched on every attempt in prod
            // (28/28 re-plan exhaustions, zero successes, 2026-07-04). Each
            // re-plan therefore rebuilds provider + ctx from a fresh eager
            // snapshot; the commit-time liveness check below still guards the
            // remaining snapshot→commit window.
            let (chunk_snapshot, chunk_log_store) = {
                let table = table_ref.read().await;
                (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
            };
            let partition_files = dedup_partition_paths(chunk_snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
            let provider = Self::narrow_provider(chunk_log_store, Arc::clone(&chunk_snapshot), partition_files, Some(DEDUP_FILE_COL))
                .await
                .map_err(|e| anyhow::anyhow!("dedup rewrite provider: {e}"))?;
            let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(
                self.config.memory.timefusion_query_partitions,
                self.maintenance_runtime_env(),
            ));
            ctx.register_table(scan_name, provider)?;

            // 1. Which files hold the chunk's rows — ground truth from the
            // scan itself, no per-file stats parsing.
            let files_sql = format!("SELECT DISTINCT \"{DEDUP_FILE_COL}\" FROM {scan_name} WHERE {chunk_filter}");
            let file_ids = read_string_column(ctx.sql(&files_sql).await?.collect().await?)?;
            if file_ids.is_empty() {
                // Probe saw dupes but this snapshot has no rows for the chunk
                // (concurrent rewrite) — nothing verified, don't certify clean.
                return Ok(BinOutcome::Retry);
            }
            // 2. Map scan values to Add actions in the SAME snapshot
            // (suffix-match either direction: the scan column carries the
            // store path, the log a table-relative one).
            let targets = dedup_adds_by_path(
                chunk_snapshot
                    .log_data()
                    .iter()
                    .filter(|f| {
                        let p = f.path();
                        file_ids.iter().any(|v| v.ends_with(p.as_ref()) || p.ends_with(v.as_str()))
                    })
                    // Deprecated in favour of arrow-direct access, but the
                    // Remove tombstones below need the Add's exact fields.
                    .map(|f| {
                        #[allow(deprecated)]
                        f.add_action()
                    }),
                table_name,
            );
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

            // 2026-07-29 (Phase 2): the delta-rs `SortByDedup` OptimizeBuilder
            // fast path was removed here. It rewrote AND committed inside one
            // call, so it could not be staged for a wave — and its per-chunk
            // commit is precisely the delete-delete partner that aborted against
            // light optimize. The shard path below covers the same inputs: the
            // fast path only ran when the whole chunk fit the rewrite budgets,
            // which is the shard path's `shards == 1` case.
            //
            // 3. Decide the shard count. A dedup `SELECT * … collect()` decodes to
            // Arrow at 5-20× compressed OUTSIDE the memory pool, so an over-budget
            // chunk used to be skipped (dupe left forever). Instead we split the
            // rewrite into K passes bucketed by a hash of the dedup keys — every
            // copy of a key hashes to one bucket (never split), and hashing (not
            // `key % K`, which collides for ms-aligned values) spreads evenly and is
            // NULL-safe.
            // K = ceil(estimated decoded bytes / budget); the estimate is the
            // row-count-vs-inflation MAX ×2 documented on the config fields.
            let rewrite_bytes: i64 = targets.iter().map(|a| a.size).sum();
            // Fail closed unless the provider's full-file re-read can be
            // checked against Delta's independent row-count metadata. This is
            // the invariant that would have stopped the 2026-08-03 loss: the
            // buggy bin-scoped re-read produced 63k rows while removing files
            // whose Add actions described 5.8M live rows. Deletion-vector
            // cardinality is subtracted because the provider correctly hides
            // those already-deleted physical rows.
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
            let decoded_budget = limits.map_or(self.config.maintenance.timefusion_dedup_max_decoded_bytes, |limits| {
                self.config.maintenance.timefusion_dedup_max_decoded_bytes.min(limits.max_decoded_bytes)
            });
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
            // K is the read/decode AMPLIFICATION of this rewrite, and nothing
            // logged it. Each shard is an independent query over the SAME files
            // (`N shards paid N scans + sorts + writes`, below), so the partition
            // is decoded K times and every row is md5-hashed K times to keep
            // 1/K of them.
            //
            // K = ceil(est_decoded / budget), and `est_decoded` is deliberately
            // pessimistic — max(rows x bytes_per_row, compressed x inflation) x 2
            // — so every unit of over-estimate costs a whole extra pass over the
            // data. A perf profile on 2026-08-18 put `md5::compress` at 5.71% of
            // all CPU, above ZSTD decompression, which is what this multiplier
            // looks like from the outside. Log the inputs so the amplification is
            // visible before anyone tunes the estimate or the budget.
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
            let in_list = file_ids.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect::<Vec<_>>().join(", ");
            // Bucket = `hash_bucket` over the dedup keys, in `[0, DEDUP_BUCKET_COUNT)`
            // and evenly spread; chr(31) separates keys so distinct tuples can't
            // collide. Also the GROUP BY for the skew probe below.
            //
            // This was `substr(md5(…), 1, 2)` until 2026-08-18, when a live CPU
            // profile put `md5::compress` at 5.71% of all CPU — larger than the ZSTD
            // decompression it serves, because each of K passes hashes every row to
            // keep 1/K of them.
            let keys_varchar = schema.dedup_keys.iter().map(|k| format!("CAST(\"{k}\" AS VARCHAR)")).collect::<Vec<_>>().join(", ");
            let bucket_expr = format!("hash_bucket(arrow_cast(concat_ws(chr(31), {keys_varchar}), 'Utf8View'), {DEDUP_BUCKET_COUNT})");
            // Independent narrow oracle for the staged output count. The
            // Arrow rewrite below chooses the greatest tiebreak per key, but it
            // must still emit exactly one row per distinct key (tombstones are
            // retained). A disagreement rejects the unit before Remove actions
            // can reach `commit_wave`.
            let logical_rows_sql = format!(
                "SELECT count(*) FROM (SELECT 1 FROM {scan_name} WHERE {partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}) GROUP BY {keys_varchar})"
            );
            let expected_logical_rows =
                Self::scalar_i64(&ctx, &logical_rows_sql).await?.ok_or_else(|| anyhow::anyhow!("dedup rewrite distinct-key validation returned no scalar"))?;
            let expected_logical_rows =
                u64::try_from(expected_logical_rows).map_err(|_| anyhow::anyhow!("dedup rewrite distinct-key validation returned a negative count"))?;

            // Sharding can't split a single key group — all copies share one bucket.
            // If the largest group alone would blow the budget, no shard count helps,
            // so skip (preserving the pre-fix OOM-safety) rather than materialize it.
            if limits.is_none() && shards > 1 && decoded_budget > 0 {
                let max_group_sql = format!(
                    "SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM {scan_name} WHERE {partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}) GROUP BY {keys_varchar})"
                );
                let max_group = Self::scalar_i64(&ctx, &max_group_sql).await?.unwrap_or(0);
                if (max_group.max(0) as u64).saturating_mul(bytes_per_row).saturating_mul(2) > decoded_budget {
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
            }

            // 4. Rewrite each shard independently: collect (bounded to ~one budget by
            // the bucket range), dedup, stage its own parquet. The permit bounds
            // concurrent Arrow materializations across the sweep — unlike hot-wave
            // staging (which has its own K-bounded light pool), dedup materializes
            // Arrow OUTSIDE any pool, which is exactly what this semaphore is for.
            // Held for the shard loop only, dropped before the unit is handed to a
            // wave (the commit decodes nothing). Out-of-window rows in the target files carry through verbatim
            // (their keys are unique → no drop). On any per-shard error, already-staged
            // parquet is cleaned before returning so a mid-loop failure leaks nothing.
            let rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
            let staging_table = { table_ref.read().await.clone() };
            let stage_store = staging_table.log_store().object_store(None);
            // Shards have disjoint bucket ranges. The permit bounds concurrent
            // bins; `shard_k` bounds Arrow memory within each bin.
            let shard_k = limits.map_or_else(
                || dedup_shard_concurrency(decoded_budget, self.config.derived.cores),
                |limits| dedup_shard_concurrency(decoded_budget, self.config.derived.cores).min(limits.max_concurrent_shards.max(1)),
            );
            let staged_shards: Vec<StagedShard> = futures::stream::iter(0..shards)
                .map(|shard| {
                    let (ctx, staging_table, scan_name) = (&ctx, &staging_table, &scan_name);
                    let (partition_filter, in_list, bucket_expr) = (&partition_filter, &in_list, &bucket_expr);
                    async move {
                        let mut adds: Vec<Action> = Vec::new();
                        let staged: anyhow::Result<(usize, usize)> = async {
                            let shard_pred = if shards > 1 {
                                // Contiguous bucket range per shard (even ±1); string compare of
                                // zero-padded lowercase hex == numeric order.
                                let (lo, hi) = (shard * DEDUP_BUCKET_COUNT / shards, (shard + 1) * DEDUP_BUCKET_COUNT / shards);
                                let upper = if hi < DEDUP_BUCKET_COUNT { format!(" AND {bucket_expr} < {hi}") } else { String::new() };
                                format!(" AND {bucket_expr} >= {lo}{upper}")
                            } else {
                                String::new()
                            };
                            let rows_filter = format!("{partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}){shard_pred}");
                            let rows_sql = format!("SELECT * FROM {scan_name} WHERE {rows_filter}");
                            // Version collapse: greatest `dedup_tiebreak` per key wins, so a
                            // merge-on-read table's newest version survives and the older ones
                            // are dropped here rather than at every read.
                            //
                            // Tombstones are RETAINED (`drop_tombstones = None`). Dropping one
                            // requires that no older version of its key can exist outside this
                            // rewrite's input. The input is every live file of this
                            // (project_id, date) snapshot holding a row in the 10-minute chunk
                            // window; since `timestamp` is a dedup key and `date` derives from
                            // it, all versions of a key do share that window — but three ways
                            // an older version outlives the rewrite are NOT excludable here:
                            //   1. files appended after the file-id query (flush, WAL replay,
                            //      an off-box writer). `commit_wave`'s liveness check verifies
                            //      the TARGETS still exist; it cannot see a new file carrying
                            //      an older version of the same key.
                            //   2. rows still in MemBuffer/WAL/hot tier. The 2h sealed-chunk
                            //      guard bounds EVENT time, not arrival: a late client re-send
                            //      (or a version append, which carries the base row's original
                            //      `timestamp`) lands in a long-sealed window at any wall clock.
                            //   3. tables whose `dedup_keys` omit `timestamp` take the
                            //      whole-partition branch above, where versions of one key may
                            //      sit in date partitions this sweep never holds together.
                            // A retained tombstone costs one row per deleted key forever; a
                            // dropped one silently resurrects the row. Retain.
                            let sorted = !schema_order_by_clause(schema).is_empty();
                            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_intermediate, sorted);
                            let mut writer = deltalake::writer::RecordBatchWriter::for_table(staging_table)
                                .map_err(|e| anyhow::anyhow!("dedup rewrite writer: {e}"))?
                                .with_writer_properties(writer_properties);
                            let target_schema = writer.arrow_schema();
                            let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
                            let (shard_before, shard_after) = if limits.is_some() {
                                let count_sql = format!("SELECT COUNT(*) FROM {scan_name} WHERE {rows_filter}");
                                let shard_before = Self::scalar_i64(ctx, &count_sql).await?.map_or(0, |v| usize::try_from(v.max(0)).unwrap_or(usize::MAX));
                                if shard_before == 0 {
                                    return Ok((0, 0));
                                }
                                let columns = schema.fields.iter().map(|field| crate::rollup::quoted(&field.name)).collect::<Vec<_>>().join(", ");
                                let keys = schema.dedup_keys.iter().map(|field| crate::rollup::quoted(field)).collect::<Vec<_>>().join(", ");
                                let order = schema
                                    .dedup_tiebreak
                                    .as_ref()
                                    .map_or_else(|| keys.clone(), |field| format!("{} DESC NULLS LAST", crate::rollup::quoted(field)));
                                let order_by = schema_order_by_clause(schema);
                                let sql = format!(
                                    "SELECT {columns} FROM (SELECT {columns}, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {order}) AS __tf_rn \
                                     FROM {scan_name} WHERE {rows_filter}) WHERE __tf_rn = 1{order_by}"
                                );
                                let mut stream = ctx.sql(&sql).await?.execute_stream().await?;
                                let mut shard_after = 0usize;
                                let mut decoded_bytes = 0usize;
                                while let Some(batch) = stream.next().await {
                                    let batch = cast_variant_columns_to_binary(batch?)?;
                                    shard_after = shard_after.saturating_add(batch.num_rows());
                                    decoded_bytes = decoded_bytes.saturating_add(batch.get_array_memory_size());
                                    let casted = deltalake::kernel::schema::cast_record_batch(&batch, target_schema.clone(), true, true)?;
                                    writer.write(casted).await.map_err(|e| anyhow::anyhow!("dedup rewrite stage: {e}"))?;
                                    if writer.buffer_len() >= max_file_bytes {
                                        adds.extend(
                                            writer.flush().await.map_err(|e| anyhow::anyhow!("dedup rewrite flush: {e}"))?.into_iter().map(Action::Add),
                                        );
                                    }
                                }
                                // The coordinator takes this streaming branch, so the
                                // collecting branch's identical probe below would never fire in
                                // production. Post-dedup rows, so this UNDER-states the input
                                // volume — a ratio at or above the estimate is therefore
                                // conclusive, one below it is not.
                                if shard == 0 {
                                    info!(
                                        shards,
                                        rows = shard_after,
                                        actual_decoded_mb = decoded_bytes / (1 << 20),
                                        predicted_decoded_mb = (est_decoded_bytes / shards.max(1)) / (1 << 20),
                                        event = "dedup_shard_decoded",
                                        stage = "streamed",
                                        "what one dedup shard decoded to, against what the estimate predicted"
                                    );
                                }
                                (shard_before, shard_after)
                            } else {
                                let batches: Vec<RecordBatch> =
                                    ctx.sql(&rows_sql).await?.collect().await?.into_iter().map(|batch| drop_batch_column(batch, DEDUP_FILE_COL)).collect();
                                let shard_before = batches.iter().map(RecordBatch::num_rows).sum();
                                // K is driven entirely by `est_decoded_bytes`, and every 2x of
                                // over-estimate is a whole extra pass over the data. Neither
                                // `bytes_per_row = 4096` nor `inflation = 12` has ever been
                                // checked against a real decoded-vs-compressed ratio, so log
                                // what this shard ACTUALLY decoded to next to what was
                                // predicted for it. One shard, not all K, so a 256-way rewrite
                                // cannot flood the log.
                                if shard == 0 {
                                    let actual: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
                                    info!(
                                        shards,
                                        rows = shard_before,
                                        actual_decoded_mb = actual / (1 << 20),
                                        predicted_decoded_mb = (est_decoded_bytes / shards.max(1)) / (1 << 20),
                                        event = "dedup_shard_decoded",
                                        stage = "collected",
                                        "what one dedup shard decoded to, against what the estimate predicted"
                                    );
                                }
                                if shard_before == 0 {
                                    return Ok((0, 0));
                                }
                                let deduped = crate::write::mem_buffer::dedup_batches(batches, &schema.dedup_keys, schema.dedup_tiebreak.as_deref(), None)?;
                                let shard_after = deduped.iter().map(RecordBatch::num_rows).sum();
                                let deduped = deduped.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;
                                let (deduped, _) = self.sort_flush_group(schema, deduped, UnsortedFallback::Forbid).await?;
                                for batch in deduped {
                                    let casted = deltalake::kernel::schema::cast_record_batch(&batch?, target_schema.clone(), true, true)?;
                                    writer.write(casted).await.map_err(|e| anyhow::anyhow!("dedup rewrite stage: {e}"))?;
                                    if writer.buffer_len() >= max_file_bytes {
                                        adds.extend(
                                            writer.flush().await.map_err(|e| anyhow::anyhow!("dedup rewrite flush: {e}"))?.into_iter().map(Action::Add),
                                        );
                                    }
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
            // Fold: every shard hands back its adds even when it failed, so a
            // mid-flight failure still leaks nothing.
            let (mut before, mut after) = (0usize, 0usize);
            let mut adds: Vec<Action> = Vec::new();
            let mut stage_result: anyhow::Result<()> = Ok(());
            for (shard_adds, outcome) in staged_shards {
                adds.extend(shard_adds);
                match outcome {
                    Ok((shard_before, shard_after)) => (before, after) = (before + shard_before, after + shard_after),
                    Err(error) => {
                        stage_result = Err(match stage_result {
                            Ok(()) => error,
                            Err(first) => first,
                        })
                    }
                }
            }
            if let Err(e) = stage_result {
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                return Err(e);
            }
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
            // Row-DROPPING rewrite: data_change=true on both sides. See
            // `staged_actions` — the snapshot-isolation downgrade the hot path
            // enjoys is only sound for data-preserving commits.
            let (removes, adds) = staged_actions(&targets, adds, true);
            // Record the intent BEFORE the unit can be handed to a wave commit, so
            // a crash anywhere in the staging->commit window leaves a trail to
            // clean up (same guarantee as hot bins).
            let wave_id = uuid::Uuid::new_v4().to_string();
            self.record_staged_intent(&StagedIntent {
                wave_id: wave_id.clone(),
                table_name: table_name.to_string(),
                project_id: project_id.to_string(),
                recorded_at: crate::support::now_secs(),
                paths: adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.path.clone()) } else { None }).collect(),
                // Cleanup-only: a dedup rewrite DROPS rows, so the resume path's
                // row-preservation check can't tell a valid staging from a
                // truncated one. See `resumable_staged_bin`.
                target_paths: Vec::new(),
                adds: Vec::new(),
            });
            debug!(table_name, project_id, chunk = label, files = targets.len(), before, after, event = "dedup_chunk_staged");
            return Ok(BinOutcome::Staged(StagedBin {
                project_id: project_id.to_string(),
                wave_id,
                target_paths: targets.iter().map(|t| t.path.clone()).collect(),
                removes,
                adds,
                stage_store,
                dedup: Some(DedupUnit { key: key.clone(), date: date_str.to_string(), label: label.to_string(), before: before as u64, after: after as u64 }),
            }));
        }
        anyhow::bail!("dedup rewrite: re-plan attempts exhausted for table={} chunk=[{}]", table_name, label)
    }

    /// Live parquet files of one `date=` partition, grouped by the
    /// `project_id=` path segment ("default" when absent — custom-project
    /// tables don't embed it). Shared by the sweep's fingerprint capture and
    /// the read-side dedup-skip check so both hash identical groupings.
    pub(crate) fn partition_files_by_pid(table: &DeltaTable, date_marker: &str) -> Result<HashMap<String, Vec<String>>> {
        Ok(table.get_file_uris()?.filter(|uri| uri.contains(date_marker) && uri.ends_with(".parquet")).fold(HashMap::new(), |mut files, uri| {
            files.entry(path_partition_value(&uri, "project_id").unwrap_or("default").to_string()).or_default().push(uri);
            files
        }))
    }
}
