//! Maintenance: rollup planning/coordinator ticks, dedup sweeps + wave commits,
//! hot-tail packing/repair passes, vacuum, checkpoint/reconcile, shutdown.
use super::*;
use tap::Tap;

/// Estimated decoded (in-memory) bytes for a Parquet file of `compressed_size` on
/// disk — a fixed 12x compression-ratio guess used for budgeting, not measurement.
/// One tier's contribution to a fleet contiguity gauge, or `None` to abstain.
///
/// Every tier folds into one number with `.min()`, and that is the SAFETY
/// PROPERTY, not a bug. The fleet gauge drives `coverage_is_short`, which
/// overrides the journal ceiling so a starved tier can enqueue historical
/// backfill at all. Swap the fold for a median and a single starved tier stops
/// triggering it — and `dashboard_1h_v2`, the tier every 7d/30d query reads, is
/// exactly the one that would then wait on a ceiling the live frontier holds
/// shut forever.
///
/// What the fold could NOT distinguish is a tier RAMPING from one STARVED: both
/// read low, only one is an emergency. Prod 2026-08-24 — a two-hour-old
/// `dashboard_level_1m_v1` sat at 2 days and pinned the fleet gauge to 2 while
/// four healthy tiers sat at 30, running the whole cluster in coverage-short
/// mode for an auxiliary tier nobody queries.
///
/// A tier younger than the horizon CANNOT hold that many days, so it abstains.
/// That keeps the override armed for every real regression, because regression
/// only happens to tiers old enough to have been complete.
/// `seeded_by_real` is whether a NON-ramping tier has already contributed in this
/// sweep, and it is what makes the rule order-independent. A ramping tier still
/// seeds a gauge nothing real has touched — otherwise a fresh deployment, where
/// every tier is young, would publish no gauge at all and the reweighting would
/// run off whatever value the process started with. But a real tier always
/// OVERWRITES that provisional value rather than minimising into it, so a
/// ramping tier listed first cannot drag the fleet down either.
fn fold_fleet_gauge(previous: u64, value: u64, seeded_by_real: bool, ramping: bool) -> Option<u64> {
    match (ramping, seeded_by_real) {
        (true, true) => None,
        (true, false) | (false, false) => Some(value),
        (false, true) => Some(previous.min(value)),
    }
}

/// The window the one-shot orphan repair rebuilds. `BEFORE` is the date the
/// `duration_digest` spec edit landed and changed `generation_id`; `FROM` bounds
/// the work, leaving older orphans to age out of the 35-day horizon.
const ORPHAN_REPAIR_FROM: &str = "2026-08-01";
const ORPHAN_REPAIR_BEFORE: &str = "2026-08-22";

fn estimated_decoded_bytes(compressed_size: i64) -> u64 {
    u64::try_from(compressed_size.max(0)).unwrap_or_default().saturating_mul(12)
}

/// (project, slice_start, slice_end, generation, source_fp, source_rows) — the
/// coverage identity `recover_rollup_coverage` reads back off a tier file's tags.
type TaggedSliceIdentity = (String, i64, i64, String, u64, Option<u64>);

/// Time ranges keyed by `(project, date)` — the untagged files' statistics
/// spans, and separately the tagged files' slice ranges.
type SpansByPartition = HashMap<(String, String), Vec<(i64, i64)>>;

/// Per `(project, date)`: the timestamp spans of its UNTAGGED tier files, and
/// the slice ranges of its tagged ones. `uncovered_gaps` turns the pair into the
/// work that would let `slice_retires` reach the untagged files.
type UntaggedPartitions = HashMap<(String, String), (Vec<(i64, i64)>, Vec<(i64, i64)>)>;

impl Database {
    /// Push a coordinator task's next attempt out by `delay`, journaled and checkpointed.
    fn retry_task(&self, key: &crate::maintenance_coordinator::TaskKey, reason: String, delay: std::time::Duration) -> Result<()> {
        let delay_micros = i64::try_from(delay.as_micros()).unwrap_or(i64::MAX);
        let mut journal = self.journal();
        // Route through retry_or_split, not retry: a fast-fail retry (resource
        // admission, memory) repeats identically at the same size, and this was
        // the one retry path with NO split — a day-wide Repair whose estimate
        // can never be admitted looped here at 1s for days (attempts 140-211,
        // prod 2026-08-21) without ever reaching abandon_running's bisection.
        let attempts = journal.attempts(key);
        journal.retry_or_split(key, reason, crate::support::now_micros().saturating_add(delay_micros), attempts);
        journal.checkpoint()
    }

    /// Exclusive upper bound the coverage was built to. `i64::MAX` means whole-partition.
    ///
    /// Must be the bound stored with the coverage, never one recomputed here — for a day still
    /// being written the two would differ and invalidate good coverage every query.
    /// The partition fingerprint the ticket re-check compares against
    /// `coverage.source_fp`.
    ///
    /// UNBOUNDED, because `source_fp` is RECORDED unbounded — it is
    /// `partition_identity.0`, taken from
    /// `partition_stats_bounded(.., &|_, _| i64::MAX)` at publish time. Passing
    /// the coverage's `covered_through` here recomputed a DIFFERENT fingerprint
    /// over a smaller file set, so the re-check failed for every partition whose
    /// bound excluded anything: the rewrite planned, the physical plan built,
    /// and `rollup_ticket_current` then rejected it as `StaleCoverage` at the
    /// last step. Same mismatch as the planning path, one site further on.
    pub(crate) async fn rollup_source_fingerprint(&self, project_id: &str, source: &str, date: &str) -> Result<u64> {
        let table = self.resolve_table(project_id, source).await?;
        let table = table.read().await;
        let mut fingerprints = Self::partition_fingerprints_bounded(&table, tiebreak_of(source), &|_, _| i64::MAX)?;
        Ok(fingerprints
            .remove(&(project_id.to_string(), date.to_string()))
            .or_else(|| fingerprints.remove(&("default".to_string(), date.to_string())))
            .unwrap_or_default())
    }

    fn persist_rollup_journal(&self) -> std::io::Result<()> {
        let mut entries: Vec<_> = self
            .rollup_source_epochs
            .iter()
            .map(|entry| {
                let ((project_id, source, date), epoch) = (entry.key(), *entry.value());
                let dirty = self.rollup_dirty.get(entry.key()).map(|value| *value.value());
                crate::rollup_journal::RollupInvalidation {
                    project_id: project_id.clone(),
                    source: source.clone(),
                    date: date.clone(),
                    epoch,
                    dirty_hours: dirty.unwrap_or(crate::rollup::ALL_HOURS),
                    unknown: dirty.is_none_or(|hours| hours == crate::rollup::ALL_HOURS),
                    invalidated_unix_ms: self.rollup_invalidated_at.get(entry.key()).map_or(0, |value| *value.value()),
                }
            })
            .collect();
        entries.sort_by(|a, b| (&a.source, &a.project_id, &a.date).cmp(&(&b.source, &b.project_id, &b.date)));
        let dirty_entries = entries.iter().filter(|entry| entry.unknown || entry.dirty_hours != 0).collect::<Vec<_>>();
        let stats = crate::observability::maintenance_stats();
        stats.rollup_dirty_partitions.store(dirty_entries.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let now = crate::storage::now_unix_ms();
        let oldest_age_secs = dirty_entries
            .iter()
            .filter_map(|entry| (entry.invalidated_unix_ms != 0).then_some(now.saturating_sub(entry.invalidated_unix_ms) / 1_000))
            .max()
            .unwrap_or(0);
        stats.rollup_oldest_invalidation_age_secs.store(oldest_age_secs, std::sync::atomic::Ordering::Relaxed);
        crate::rollup_journal::store(&self.config.core.timefusion_data_dir, &entries)
    }

    pub(crate) fn enqueue_maintenance_hours(&self, project_id: &str, source: &str, date: &str, hours: u32) -> std::io::Result<()> {
        let Some(schema) = get_schema(source) else { return Ok(()) };
        if schema.rollups.is_empty() || hours == 0 {
            return Ok(());
        }
        let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").map_err(std::io::Error::other)?;
        let day_start = day.and_hms_opt(0, 0, 0).ok_or_else(|| std::io::Error::other("invalid maintenance date"))?.and_utc().timestamp_micros();
        let observed_at = crate::support::now_micros();
        let mut journal = self.journal();
        for spec in &schema.rollups {
            let target = spec.table_name(source);
            for (start, end) in crate::rollup::dirty_ranges(day_start, hours) {
                journal
                    .invalidate(crate::maintenance_coordinator::Invalidation {
                        source_table: source,
                        rollup_table: &target,
                        source,
                        project_id,
                        start_micros: start,
                        end_micros: end,
                        observed_at_micros: observed_at,
                        derived: spec.derive_from.is_some(),
                    })
                    .map_err(std::io::Error::other)?;
            }
        }
        journal.checkpoint().map_err(std::io::Error::other)
    }

    /// Reconcile the durable task cursor with each live Delta snapshot. This is
    /// intentionally metadata-only and conservative: commits after the cursor
    /// contribute only the partitions named by data-changing Add/Remove actions.
    /// A crash before the cursor checkpoint repeats work; a crash after it is
    /// safe because every task checkpoint happened first.
    pub(crate) async fn reconcile_maintenance_task_cursors(&self) -> Result<usize> {
        // The caller waits for preload. Reconciliation must inspect cached
        // handles only: cold-loading a source here previously put every
        // foreground reader and ingest writer behind a multi-minute Delta-log
        // replay while Docker health checks remained green.
        let mut queued = 0usize;
        for (storage_project, source, table_ref) in self.all_tables().await {
            let Some(schema) = get_schema(&source).filter(|schema| !schema.rollups.is_empty()) else { continue };
            let (version, log_store) = {
                let table = table_ref.read().await;
                (table.version().unwrap_or_default(), table.log_store())
            };
            let cursor_key = format!("{storage_project}:{source}");
            let current_cursor = self.journal().source_cursor(&cursor_key);
            // The first coordinator start has no durable cursor. Establish a
            // baseline at the already-loaded snapshot instead of expanding the
            // complete table history into thousands of urgent tasks. Existing
            // journal invalidations remain intact, and normal fair backfill
            // discovers historical debt. Production showed the old bootstrap
            // path monopolizing CPU for 21.9s immediately after preload.
            if current_cursor.is_none() {
                let mut journal = self.journal();
                journal.set_source_cursor(cursor_key, version);
                journal.checkpoint()?;
                continue;
            }
            if current_cursor.is_some_and(|cursor| cursor >= version) {
                continue;
            }
            let cursor = current_cursor.expect("missing cursor handled above");
            // Per partition, the hours the missed commits can have touched —
            // derived from the Add actions' timestamp stats. The naive form
            // invalidated ALL_HOURS per changed partition: a unified-table
            // flush commit names every active project, so EVERY restart
            // re-enqueued ~312 durable tasks per active stream AND reset the
            // day's completed frontier work to Pending (`invalidate` upsert
            // semantics). Measured 2026-08-18 across the 14:05 OOM boot:
            // +5,876 pending base rollups in one hour from ONE restart — the
            // queue's dominant growth source under deploy churn.
            let mut partition_hours: HashMap<(String, String), u32> = HashMap::new();
            for commit_version in cursor.saturating_add(1)..=version {
                let bytes = log_store
                    .read_commit_entry(commit_version)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("source commit {commit_version} is unavailable for {cursor_key}; cursor remains at {cursor}"))?;
                let mut partitions_with_adds = HashSet::new();
                let mut remove_only: HashSet<(String, String)> = HashSet::new();
                for action in deltalake::logstore::get_actions(commit_version, &bytes)? {
                    match action {
                        deltalake::kernel::Action::Add(add) if add.data_change => {
                            let Some(partition) = Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default") else { continue };
                            let mask = chrono::NaiveDate::parse_from_str(&partition.1, "%Y-%m-%d")
                                .ok()
                                .and_then(|date| date.and_hms_opt(0, 0, 0))
                                .and_then(|day| {
                                    add.stats.as_deref().and_then(|stats| crate::rollup::hours_from_stats_json(stats, day.and_utc().timestamp_micros()))
                                })
                                .unwrap_or(crate::rollup::ALL_HOURS);
                            partitions_with_adds.insert(partition.clone());
                            *partition_hours.entry(partition).or_insert(0) |= mask;
                        }
                        // A Remove carries no stats; in a rewrite commit its
                        // span is covered by the paired Adds. Only a partition
                        // with removes and NO adds anywhere in the missed
                        // commits (a deletion) needs the conservative day.
                        deltalake::kernel::Action::Remove(remove) if remove.data_change => {
                            let Some(partition) = Self::maintenance_partition_from_action(&remove.path, remove.partition_values.as_ref(), "default") else {
                                continue;
                            };
                            remove_only.insert(partition);
                        }
                        _ => {}
                    }
                }
                for partition in remove_only {
                    if !partitions_with_adds.contains(&partition) {
                        partition_hours.insert(partition, crate::rollup::ALL_HOURS);
                    }
                }
            }
            for ((partition_project, date), hours) in partition_hours {
                let project = if storage_project.is_empty() { partition_project } else { storage_project.clone() };
                self.enqueue_maintenance_hours(&project, &source, &date, hours)?;
                queued = queued.saturating_add(usize::try_from(hours.count_ones()).unwrap_or(24) * schema.rollups.len());
            }
            let mut journal = self.journal();
            journal.set_source_cursor(cursor_key, version);
            journal.checkpoint()?;
        }
        Ok(queued)
    }

    pub(crate) fn maintenance_partition_from_action(
        path: &str, partition_values: Option<&HashMap<String, Option<String>>>, default_project: &str,
    ) -> Option<(String, String)> {
        let value = |name: &str| partition_values.and_then(|values| values.get(name)).and_then(Option::as_deref).map(str::to_owned);
        let path_value = |name: &str| path_partition_value(path, name).map(str::to_owned);
        let date = value("date").or_else(|| path_value("date"))?;
        let project = value("project_id").or_else(|| path_value("project_id")).unwrap_or_else(|| default_project.to_owned());
        Some((project, date))
    }

    pub(crate) async fn plan_compaction_debt(&self) -> Result<usize> {
        use crate::maintenance_coordinator::{MaintenanceTask, Operation, TaskKey, TaskState, TimeSlice};
        if !self.config.maintenance.timefusion_light_optimize_enabled {
            return Ok(0);
        }
        let now = crate::support::now_micros();
        let today = crate::support::today_utc();
        let created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
        let mut planned = Vec::new();
        // Every table EXCEPT a rollup tier. Packing a tier destroys the coverage
        // it exists to prove.
        //
        // A tier file carries the identity tags `recover_rollup_coverage` reads —
        // source, project, generation, source_fingerprint, slice_start,
        // slice_end — and that function skips any file missing ANY of them.
        // Packing merges files from different slices, so `carried_coverage_tags`
        // finds the inputs disagree and emits nothing: the packed file proves no
        // coverage, and the slices it absorbed are no longer represented by any
        // tagged file either. Coverage for that range is not weakened, it is
        // gone, and the router answers `not_built` and falls back to a raw scan.
        //
        // Prod 2026-08-19 was spending half of file hygiene on exactly this — 58
        // of 119 HotPacking/SealedConsolidation claims targeted tier tables — and
        // the 1m tier had 79 untagged live files on 08-19 and 9 on 08-18 while
        // every older day was fully tagged. A 10-day historical window then
        // reported `rollup_miss_not_built_total +45` as its sole miss reason.
        //
        // The tier does not need packing anyway: its writer publishes one file
        // per unit, and units are day-wide since the coarsening cascade. The
        // small-file counts that made tiers look like debt came from the
        // ten-minute slices that over-splitting produced.
        let tiers: HashSet<String> = crate::schema::registry()
            .list_tables()
            .into_iter()
            .filter_map(|name| get_schema(&name).map(|schema| (name, schema)))
            .flat_map(|(name, schema)| schema.rollups.iter().map(|spec| spec.table_name(&name)).collect::<Vec<_>>())
            .collect();
        // The same declared-tier set retires work for a tier that no longer
        // exists — a spec removal or a `_v2` -> `_v3` rename leaves its queued
        // tasks claimable forever. Free here: `tiers` is already computed.
        {
            let mut journal = self.journal();
            let retired = journal.retire_undeclared_tiers(&tiers);
            if retired != 0 {
                let _ = journal.compact();
                warn!(retired, event = "maintenance_undeclared_tier_tasks_retired", "queued work for a tier no longer declared");
            }
        }
        // Why the biggest debt in the fleet is not being worked on.
        //
        // `planned=N` alone cannot distinguish "nothing needs doing" from "the
        // work is queued and never claimed", and prod 2026-08-24 was firmly the
        // second: 48 out-of-policy cells and `out_of_policy_cells` unchanged
        // across three object-storage censuses, while the top cell
        // (`87576849 / 2026-08-19`, 238 small files) appeared in no log line of
        // any kind for 73 minutes across three containers.
        //
        // Both hygiene operations, because they rank in the same pool but plan
        // from opposite ends of the calendar — a cell that HotPacking treats as
        // today is the one SealedConsolidation is waiting on tomorrow.
        {
            let journal = self.journal();
            for operation in [Operation::SealedConsolidation, Operation::HotPacking] {
                if let Some(refusal) = journal.most_indebted_unclaimed(operation, now) {
                    info!(?operation, refusal, event = "maintenance_hygiene_debt_unclaimed", "the most indebted hygiene cell is not being claimed");
                }
            }
        }
        for (storage_project, source, table_ref) in self.all_tables().await {
            if tiers.contains(&source) {
                continue;
            }
            let schema = schema_or_default(&source);
            let mut partitions: HashMap<(String, chrono::NaiveDate), Vec<CompactionDebtFile>> = HashMap::new();
            {
                let table = table_ref.read().await;
                let default_project = if storage_project.is_empty() { "default" } else { storage_project.as_str() };
                for file in table.snapshot()?.log_data().iter() {
                    let path = file.path();
                    let Some((project, date)) = Self::maintenance_partition_from_action(&path, None, default_project)
                        .and_then(|(project, date)| Some((project, date.parse::<chrono::NaiveDate>().ok()?)))
                    else {
                        continue;
                    };
                    partitions.entry((project, date)).or_default().push(CompactionDebtFile { size: file.size(), path: path.to_string() });
                }
            }
            // Every partition this scan examined, and every (partition, op) it
            // decided needs work. Anything seen-but-not-planned is COMPLIANT,
            // which is what retires the stale queue below.
            let mut seen: HashSet<(String, chrono::NaiveDate)> = HashSet::new();
            let mut planned_keys: HashSet<(String, chrono::NaiveDate, Operation)> = HashSet::new();
            for ((project_id, date), files) in partitions {
                seen.insert((project_id.clone(), date));
                // Future event timestamps are neither hot nor sealed. Treating
                // them as sealed debt lets maintenance rewrite a partition as
                // soon as it appears, racing foreground repair/DML and wasting
                // resources on malformed clocks. They become eligible normally
                // when their UTC date arrives.
                if date > today {
                    continue;
                }
                let day_start = date.and_hms_opt(0, 0, 0).ok_or_else(|| anyhow::anyhow!("invalid compaction date"))?.and_utc().timestamp_micros();
                let slice = TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS))?;
                let small_target = if date == today { COORDINATOR_HOT_TARGET_BYTES } else { COORDINATOR_SEALED_TARGET_BYTES };
                // SIZE only. Sortedness belongs to Repair, which owns it below.
                //
                // Admitting on `!file.sorted` made every partition permanently
                // out of policy, because the tag it reads is not the fact it
                // wants: `repair_verified_sorted`'s own comment records that
                // "the flush path sorts and stamps a correct footer WITHOUT the
                // tag, so an untagged file is only a *suspect*", and Repair
                // therefore footer-checks before rewriting. Consolidation did
                // not, so it treated suspicion as proof.
                //
                // Measured 2026-08-19 over 381 commits of the prod Delta log:
                // 1,593 of 1,648 add actions carry NO tags at all — only the
                // OPTIMIZE path tags its output. So every partition holding any
                // flush-written file — which is every partition — was
                // permanently admitted, and ingest recreated the condition
                // faster than consolidation cleared it. That is why the class
                // sat at -0.27/min forever: not a backlog, a treadmill.
                //
                // Object storage agrees with the size-only policy: of 1,033
                // partitions, 877 are already compliant and 108 sealed ones are
                // genuinely out of policy — against 2,130 pending tasks.
                let mut small = files.iter().filter(|file| file.size < small_target).collect::<Vec<_>>();
                // The policy must agree with what the PACKER can actually do.
                // `select_coordinator_compaction_candidates` merges files whose
                // SUM fits the target, so two files that are each under target
                // but together exceed it are unmergeable — admitting them queues
                // a unit that selects one file, retires nothing, and is re-minted
                // 60s later forever.
                //
                // Prod 2026-08-23, straight off the funnel log:
                //
                //   SealedConsolidation dcad860a/2026-08-22
                //     after_range_filter=2 under_target=2 selected=0
                //
                // Two files, both "small", nothing selectable. Cells of this
                // shape were claimed every 30-60s for hours and never lost a
                // file. Requiring the two SMALLEST to fit together is the same
                // test the packer applies, so a queued unit can always do work.
                small.sort_by_key(|file| file.size);
                let mergeable = small.len() >= 2 && small[0].size.saturating_add(small[1].size) <= small_target;
                if mergeable {
                    let operation = if date == today { Operation::HotPacking } else { Operation::SealedConsolidation };
                    planned_keys.insert((project_id.clone(), date, operation));
                    let estimate = small.iter().fold(0u64, |bytes, file| bytes.saturating_add(estimated_decoded_bytes(file.size)));
                    // The file count IS the benefit for hygiene: consolidation
                    // removes these and leaves one. `scheduling_class` ranks
                    // sealed hygiene on it, so a cell worth 200 files outranks
                    // one worth 3 regardless of which sealed first.
                    let footprint = crate::maintenance_coordinator::InputFootprint::new(small.iter().map(|file| &file.path), estimate);
                    // A sealed partition's age is measured from when it SEALED,
                    // not from when this scan happened to notice it again.
                    //
                    // `scheduling_class` escalates a task that has waited past
                    // `STARVATION_MICROS`, and that is the mechanism meant to
                    // rescue exactly this: prod 2026-08-19 had 2026-08-13 at 167
                    // files for FIVE days, unchanged across three dashboard
                    // snapshots, while seven younger sealed days converged past
                    // it. It never escalated because hygiene work is re-derived
                    // by this scan and (since it stopped being persisted) also
                    // re-created on every restart — so `created_unix_ms` reset to
                    // `now` several times a day and the 24 h threshold was
                    // unreachable by construction.
                    //
                    // The seal time is the honest clock for derived work: it says
                    // "this partition has been out of policy for five days",
                    // which is the fact the escalation exists to act on, and it
                    // survives restarts because it is a property of the data
                    // rather than of the process. Today's partition keeps `now`
                    // — it is the live tail, not a backlog.
                    let sealed_at_ms = u64::try_from(slice.end_micros.max(0).div_euclid(1_000)).unwrap_or_default();
                    let created_unix_ms = if date == today { created_unix_ms } else { sealed_at_ms.min(created_unix_ms) };
                    planned.push(MaintenanceTask {
                        key: TaskKey { physical_table: source.clone(), source: source.clone(), project_id: project_id.clone(), slice, operation },
                        state: TaskState::Pending,
                        deadline_micros: now,
                        estimated_decoded_bytes: estimate.max(1),
                        hash_shard: 0,
                        hash_shards: 1,
                        attempts: 0,
                        created_unix_ms,
                        retry_reason: None,
                        publication: None,
                        base_tier_present: false,
                        input: Some(footprint),
                        parent_measured_bytes: None,
                    });
                }
                if date < today && !schema.sorting_columns.is_empty() {
                    let suspects = files.iter().filter(|file| !self.repair_verified_sorted.contains(&file.path)).collect::<Vec<_>>();
                    if !suspects.is_empty() {
                        planned_keys.insert((project_id.clone(), date, Operation::Repair));
                        let estimate = suspects.iter().fold(0u64, |bytes, file| bytes.saturating_add(estimated_decoded_bytes(file.size)));
                        let footprint = crate::maintenance_coordinator::InputFootprint::new(suspects.iter().map(|file| &file.path), estimate);
                        planned.push(MaintenanceTask {
                            key: TaskKey {
                                physical_table: source.clone(),
                                source: source.clone(),
                                project_id: project_id.clone(),
                                slice,
                                operation: Operation::Repair,
                            },
                            state: TaskState::Pending,
                            deadline_micros: now,
                            estimated_decoded_bytes: estimate.max(1),
                            hash_shard: 0,
                            hash_shards: 1,
                            attempts: 0,
                            created_unix_ms,
                            retry_reason: None,
                            publication: None,
                            base_tier_present: false,
                            input: Some(footprint),
                            parent_measured_bytes: None,
                        });
                    }
                }
            }
            // Retire hygiene tasks for partitions this scan proved compliant.
            //
            // File hygiene is STATELESS work stated as a durable queue, and the
            // two disagree: the scan above re-derives the truth every 60s, while
            // the queue accumulates whatever was true whenever a task was minted.
            // Audited against object storage 2026-08-19: of 1,033 partitions,
            // 877 are already compliant and only 108 SEALED ones are out of
            // policy — against `pending_sealed_consolidation` = 2,218. A 20x
            // inflated queue, whose stale entries are claimed at the same ~21/h
            // as real ones, so ~95% of that budget rewrites nothing. The real
            // work is about five hours; it just cannot get a turn.
            //
            // Only partitions this pass actually SAW are retired. A partition
            // absent from the snapshot is unknown, not clean, and unknown must
            // never retire work — that is the direction that silently drops
            // compaction.
            let retired = {
                let mut journal = self.journal();
                let stale: Vec<_> = journal
                    .tasks()
                    .filter(|task| {
                        matches!(task.key.operation, Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair)
                            && task.key.source == source
                            && matches!(task.state, TaskState::Pending | TaskState::Retry)
                    })
                    .filter_map(|task| {
                        let date = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros)?.date_naive();
                        let cell = (task.key.project_id.clone(), date);
                        (seen.contains(&cell) && !planned_keys.contains(&(cell.0, date, task.key.operation))).then(|| task.key.clone())
                    })
                    .collect();
                for key in &stale {
                    journal.complete(key);
                }
                stale.len()
            };
            if retired != 0 {
                info!(source, retired, event = "maintenance_hygiene_tasks_retired");
            }
        }
        let count = planned.len();
        if count != 0 {
            let mut journal = self.journal();
            for task in planned {
                journal.enqueue(task.key, task.deadline_micros, task.estimated_decoded_bytes, task.created_unix_ms);
            }
            journal.checkpoint()?;
        }
        Ok(count)
    }

    /// Run coordinator rollup units until the queue stops yielding work or `max_units` have run.
    ///
    /// Tests use this synchronous helper instead of the concurrent production worker pool. Does not
    /// plan internally; call `plan_rollup_backfill` first, advance the clock past
    /// `FINALIZATION_DELAY`, then drain.
    pub async fn drain_coordinator_rollups(&self, max_units: usize) -> Result<usize> {
        use crate::maintenance_coordinator::Operation;
        let mut ran = 0;
        for _ in 0..max_units {
            let mut progressed = false;
            for operation in [Operation::BaseRollup, Operation::DerivedRollup] {
                if self.run_coordinator_rollup_once(operation).await? {
                    progressed = true;
                    ran += 1;
                }
            }
            if !progressed {
                break;
            }
        }
        Ok(ran)
    }

    /// Enqueue rollup work for sealed days that have source data but no rollup output yet.
    ///
    /// `reconcile_maintenance_task_cursors` only enqueues partitions named by commits after its
    /// durable cursor, so a day written before rollups existed is never enqueued by anything else.
    /// Bounded newest-first and at most `BACKFILL_PARTITIONS_PER_PASS` per pass.
    pub async fn plan_rollup_backfill(&self) -> Result<usize> {
        /// Newest-first per pass, repeating on the same 60s cadence as compaction-debt planning.
        ///
        /// Day-sized rollup units reduced the per-partition task explosion from hundreds to a
        /// handful, so the old low number kept the enqueue rate far below what the journal can
        /// absorb and the horizon took days to queue. At 24 the whole backfill horizon is queued
        /// in under 20 minutes while `BACKFILL_PENDING_CEILING` still bounds the journal.
        const BACKFILL_PARTITIONS_PER_PASS: usize = 24;
        /// Stop queueing more history while the queue is already deep. Every
        /// `claim_next` scans the task set, so an over-full journal taxes the
        /// live frontier to buy work that cannot start for hours anyway. The
        /// backfill is a convergence process, not a one-shot: backing off and
        /// refilling later costs nothing.
        const BACKFILL_PENDING_CEILING: usize = 25_000;

        let horizon = i64::from(self.config.maintenance.timefusion_rollup_backfill_days);
        if horizon == 0 || !self.config.maintenance.timefusion_rollup_enabled {
            return Ok(0);
        }
        // Defers the ENQUEUE, not the pass. The ceiling exists because a deep
        // journal taxes every `claim_next`, which is an argument about minting
        // MORE work — and this function does three other things that cost the
        // journal nothing and that nothing else does:
        //
        //   * recomputes `rollup_min_contiguous_days`, THE goal gauge, which
        //     `coverage_is_short()` reads to weight the whole scheduling cycle;
        //   * proves the base tier for queued derived units (below);
        //   * logs `rollup_coverage_contiguity`, the only per-project view of
        //     coverage that exists.
        //
        // Returning early took all of that with it. Prod 2026-08-18 23:30 UTC
        // sat at 61,306 pending against a 25,000 ceiling — and the frontier
        // alone keeps it there indefinitely — so the pass had not run at all,
        // the gauge everything reads was a stale 0, and #186's proof could never
        // fire. A ceiling that is permanently closed is not a safety valve.
        //
        // Exception: while `coverage_is_short()`, the goal outranks the ceiling.
        // The live frontier alone holds the journal at ~43,000 against a 25,000
        // ceiling, and it is REPLENISHED by ingest, so waiting for it to fall is
        // waiting forever — prod 2026-08-19 01:00 UTC had the derived backlog
        // draining and the 1h tier day counts still frozen, because the only
        // historical cells in the journal were ones enqueued before the ceiling
        // shut and none of them were the missing days.
        //
        // Bounded and self-limiting, which is what makes it safe: the pass
        // admits at most `BACKFILL_PARTITIONS_PER_PASS` (24) cells, contiguity
        // ordering aims them at the worst project's earliest hole, and coverage
        // reaching `COVERAGE_SHORT_DAYS` restores the ceiling. The cost the
        // ceiling exists to bound — `claim_next` scanning a longer task set — is
        // 24 cells per 60s against 43,000, which is noise next to never building
        // the coverage at all.
        let defer_enqueue = {
            let journal = self.journal();
            let pending = journal.tasks().filter(|task| task.state != crate::maintenance_coordinator::TaskState::Complete).count();
            let defer = pending >= BACKFILL_PENDING_CEILING && !coverage_is_short();
            if defer {
                debug!(pending, ceiling = BACKFILL_PENDING_CEILING, event = "rollup_backfill_enqueue_deferred");
            }
            defer
        };
        let today = crate::support::today_utc();
        let earliest = today - chrono::Duration::days(horizon);
        let mut queued = 0usize;
        // `rollup_min_contiguous_days` folds every (source, tier) into one
        // worst-case number, so the first tier of a sweep seeds it and the rest
        // minimise into it.
        let mut first_tier_of_sweep = true;
        // Accumulated across EVERY source, then published once below.
        //
        // `set_base_tier_ready` / `set_tier_holes` replace wholesale, which is
        // right — coverage can go backwards — but calling them per source inside
        // this loop meant the last table processed wiped every earlier one. Prod
        // 2026-08-19: the census logged `base_tier_ready=374` at
        // `otel_logs_and_spans` and `272` at `otel_metrics`, and whatever table
        // sorted last left the journal holding only its own cells. So
        // `dependencies_complete` saw an empty set for the source that mattered
        // and 303 sealed derived tasks stayed unclaimable — the ready set from
        // #197 and the hole ranking from #199 were both live and both inert.
        let mut all_base_tier_ready: HashSet<(String, String, String)> = HashSet::new();
        let mut all_tier_holes: HashSet<(String, String, String, String)> = HashSet::new();

        for (storage_project, source, table_ref) in self.all_tables().await {
            let Some(schema) = get_schema(&source) else { continue };
            if schema.rollups.is_empty() {
                continue;
            }
            // Metadata only — the Delta log already names every partition, so
            // this never touches parquet.
            // A partition holding only EMPTY files is not coverage.
            //
            // `log_data()` lists files, and a rollup unit that aggregated nothing
            // still commits one. Pre-#169 that was the normal outcome for history:
            // a derived unit dropped base files it could not read slice tags on,
            // published rows=0, and marked itself complete — which the comment on
            // that path already describes as reading "exactly like 'this slice is
            // genuinely empty'". The resulting zero-row partition then made the
            // day look COVERED to this planner, so it was never rebuilt, so it
            // stayed empty. Permanently.
            //
            // Measured on prod 2026-08-19 02:30 UTC with #181-#192 all live:
            // `94c5dc1f` had 34 CONTIGUOUS days of 1m tier and a 1h tier missing
            // 2026-08-01 through 08-13, and the backfill planner reported
            // `queued=1 remaining=0` — it could not see a single one of those
            // days as missing. That is why #186's proof, #189's reservation,
            // #190's worker reserve and #192's per-tier veto all had nothing to
            // act on: the work was never planned, because the holes were invisible.
            //
            // Missing stats mean UNKNOWN, so they count as covered: undercounting
            // coverage re-plans work that may already be done, which is wasteful
            // but safe, while this direction is only taken on an explicit zero.
            let default_project = if storage_project.is_empty() { "default" } else { storage_project.as_str() };
            let partitions_of = |table: &DeltaTable| -> Result<HashSet<(String, chrono::NaiveDate)>> {
                Ok(table
                    .snapshot()?
                    .log_data()
                    .iter()
                    .filter_map(|file| {
                        #[allow(deprecated)]
                        let add = file.add_action();
                        if partition_file_is_empty(add.get_stats().ok().flatten().map(|stats| stats.num_records)) {
                            return None;
                        }
                        let (project, date) = Self::maintenance_partition_from_action(&file.path(), None, default_project)?;
                        Some((project, date.parse::<chrono::NaiveDate>().ok()?))
                    })
                    .collect())
            };

            let source_partitions = {
                let table = table_ref.read().await;
                partitions_of(&table)?
            };
            // Projects still ingesting into THIS source. Taken from the source
            // rather than from the tier, deliberately: a project whose rollup is
            // broken still has recent source partitions, so it stays counted —
            // whereas asking the tier would let exactly the failure this metric
            // exists to catch hide itself.
            let active_projects: HashSet<&str> =
                source_partitions.iter().filter(|(_, date)| *date >= today - chrono::Duration::days(1)).map(|(project, _)| project.as_str()).collect();
            let candidates: Vec<(String, chrono::NaiveDate)> = source_partitions
                .clone()
                .into_iter()
                // Today is the live frontier's job; only sealed days are backfill.
                .filter(|(_, date)| *date < today && *date >= earliest)
                .collect();
            // Which TIERS each day is missing, not merely whether it is missing
            // one. Two bugs lived in the single `want` set this replaces.
            //
            // 1. `want.retain(|key| !covered.contains(key))` per tier subtracts
            //    the UNION, so `want` ended up as the days covered by NO tier —
            //    while the comment above it asks for the days not covered by
            //    EVERY tier. A day present in the 1m tier but missing from the
            //    1h tier was removed by the 1m pass and never enqueued, so the
            //    coarse tier could never be backfilled for any day the fine tier
            //    already had. Prod 2026-08-17: 1m held 22-32 days per project
            //    while 1h held 2-6, and 1h only ever gained days that 1m was
            //    ALSO missing.
            //
            // 2. Enqueueing every tier for a day that only lacks one re-reads
            //    the raw source to rebuild a rollup that already exists. A
            //    derived tier reads the BASE TIER, not raw, so a day missing
            //    only its derived tier needs no source scan at all — and no
            //    Dedup, which is the other full-day raw read.
            let mut covered_per_tier: Vec<(usize, HashSet<(String, chrono::NaiveDate)>)> = Vec::new();

            // Covered by EVERY declared tier, not just one: a date present in
            // the 1m tier but missing from the 1h tier still refuses a 30d
            // panel, which reads the coarse tier.
            for (index, spec) in schema.rollups.iter().enumerate() {
                let target = spec.table_name(&source);
                let Ok(target_ref) = self.resolve_table(&storage_project, &target).await else { continue };
                let (covered, tier_created_ms) = {
                    let table = target_ref.read().await;
                    (partitions_of(&table)?, table.snapshot().ok().and_then(|state| state.snapshot().metadata().created_time()))
                };
                // A tier YOUNGER than the coverage horizon cannot hold that many
                // days, so a low number from it is ramp-up, not starvation — and
                // only starvation should move the fleet gauges below.
                //
                // Durable on purpose. A high-water mark would be process state,
                // and this box restarts constantly: after every restart every
                // tier would look young, the gauges would never signal short, and
                // a rare false positive would become a permanent false negative.
                // `created_time` lives in the Delta log and survives.
                //
                // KNOWN GAP: a long-lived tier REBUILT from scratch (a `_v3` bump
                // reusing the same table) still reads old, so it will pin the
                // gauge while it rebuilds. That may well be correct — a rebuild
                // does want the backfill capacity — but it is untested, and
                // saying so is better than letting it look covered.
                let horizon_ms = horizon.saturating_mul(24 * 60 * 60 * 1_000);
                let tier_is_ramping =
                    tier_created_ms.is_some_and(|created| crate::support::now_micros().div_euclid(1_000).saturating_sub(created) < horizon_ms);
                // The goal metric, computed from the set this planner already
                // built: how many days back from yesterday are covered with NO
                // hole, minimised over projects. A 30d panel reads the coarse
                // tier and needs 30 contiguous days there, so a gap anywhere in
                // the window sends it to a raw scan — which is why `MIN(date)`
                // and `tasks_pending` both read as progress on 2026-08-17 while
                // 14d/30d queries stayed unroutable.
                let (contiguous, worst_project, median_contiguous) = min_contiguous_days(&covered, &source_partitions, today, &active_projects);
                // See `fold_fleet_gauge` for why the cross-tier `.min()` stays and why a
                // ramping tier abstains from it.
                for (gauge, value) in [
                    (&crate::observability::maintenance_stats().rollup_median_contiguous_days, median_contiguous),
                    (&crate::observability::maintenance_stats().rollup_min_contiguous_days, contiguous),
                ] {
                    let previous = gauge.load(std::sync::atomic::Ordering::Relaxed);
                    if let Some(folded) = fold_fleet_gauge(previous, value, !first_tier_of_sweep, tier_is_ramping) {
                        gauge.store(folded, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                // Only a REAL tier consumes the seed. A ramping tier's value is
                // provisional and the next real tier overwrites it.
                first_tier_of_sweep &= tier_is_ramping;
                // The gauge alone is not actionable: it folds every (project,
                // tier) into one number, so a zero says the fleet is short
                // without saying where. Finding that the zero came from ONE
                // project cost a manual sweep of every project across two
                // sources on 2026-08-17. Name it instead.
                // `contiguous_days` counts DATE PARTITIONS — see `partitions_of`,
                // which reads file paths and non-emptiness and nothing else. A
                // date whose files carry a superseded generation is therefore
                // counted as covered here while the READ path refuses it, and
                // the planner never re-derives it for the same reason.
                //
                // Prod 2026-08-24 sat at `contiguous_days=30` while exactly two
                // days were usable, because a spec edit on 08-22 changed
                // `generation_id` and orphaned everything before it. No gauge
                // moved. `usable_cells` is the read path's own answer — the size
                // of the coverage map the router actually consults — so a gap
                // between the two IS an orphaning event.
                //
                // CAVEAT, and it matters: the coverage map is process-scoped and
                // takes ~5.5 minutes to rebuild after a restart (measured), so
                // `usable_cells` reads low on a young process. Compare the two
                // only once the process has outlived that.
                let usable_cells = self.rollup_coverage.iter().filter(|entry| entry.key().1 == source && entry.key().2 == target).count();
                info!(
                    source,
                    tier = %target,
                    contiguous_days = contiguous,
                    partition_cells = covered.len(),
                    usable_cells,
                    worst_project = worst_project.unwrap_or("none"),
                    active_projects = active_projects.len(),
                    event = "rollup_coverage_contiguity"
                );

                covered_per_tier.push((index, covered));
            }
            // Which (project, date) have their BASE tier built, from the same
            // coverage the planner just read. `dependencies_complete` consults
            // this by DAY, so it cannot miss a derived task whatever slice that
            // task covers — the failure that made #184/#186/#195 inert, measured
            // on prod as derived_unproven=674 of derived_pending=674.
            all_base_tier_ready.extend(
                covered_per_tier
                    .iter()
                    .filter(|(index, _)| schema.rollups[*index].derive_from.is_none())
                    .flat_map(|(_, covered)| covered.iter().map(|(project, date)| (source.clone(), project.clone(), date.to_string()))),
            );
            let mut missing_tiers = tiers_missing_per_day(&candidates, &covered_per_tier);
            // ONE-SHOT REPAIR for coverage the planner is structurally blind to.
            //
            // `partitions_of` decides "covered" from a non-empty file existing at
            // the path — it never reads the generation. So when a spec edit
            // changes `generation_id`, every earlier slice keeps the old value,
            // the READ path refuses those dates, and the planner counts them
            // covered and never re-derives them. They stay dark forever.
            //
            // Measured 2026-08-24 on `dashboard_1m_v3`: 37 usable of 461 cells in
            // the 35-day window — 92% of the tier unreadable — while
            // `contiguous_days` reported 30 and no gauge moved.
            //
            // Forces every tier of every in-window day BEFORE that edit back into
            // `missing_tiers` so the ORDINARY enqueue path rebuilds them: it
            // already decides Dedup-vs-not, orders derived after base (the 1h
            // tier is blind for the identical reason, and it is what 7d/30d
            // panels read), and respects the already-queued veto.
            //
            // Bounded window, so this is a known quantity of work. Older orphans
            // age out of the 35-day horizon instead of being rebuilt days before
            // they leave it.
            if let Some(cursor_was) = self.journal().repair_orphaned_coverage_once()
                && let (Ok(from), Ok(before)) = (
                    chrono::NaiveDate::parse_from_str(ORPHAN_REPAIR_FROM, "%Y-%m-%d"),
                    chrono::NaiveDate::parse_from_str(ORPHAN_REPAIR_BEFORE, "%Y-%m-%d"),
                )
            {
                let mut forced = 0usize;
                for (project, date) in &candidates {
                    if *date >= from && *date < before {
                        missing_tiers.insert((project.clone(), *date), (0..schema.rollups.len()).collect());
                        forced += 1;
                    }
                }
                warn!(
                    source, forced, cursor_was,
                    from = ORPHAN_REPAIR_FROM, before = ORPHAN_REPAIR_BEFORE,
                    event = "rollup_orphaned_coverage_repair",
                    "re-enqueueing coverage a spec change orphaned and the planner cannot see"
                );
            }
            // Publish the holes so `claim_next` can rank them ahead of days that
            // already have tier output. Same coverage read, same 60s cadence.
            for ((project, date), missing) in &missing_tiers {
                for index in missing {
                    all_tier_holes.insert((source.clone(), project.clone(), schema.rollups[*index].table_name(&source), date.to_string()));
                }
            }
            let mut want: Vec<(String, chrono::NaiveDate)> = missing_tiers.keys().cloned().collect();
            let cells_missing = want.len();
            // Skip any day that already has ROLLUP work queued. `invalidate`
            // takes `deadline.max(new_deadline)`, so re-invalidating a day that
            // already has an eligible task pushes that task's deadline OUT by a
            // full finalization delay. A planner running every 60s would then
            // hold the live frontier permanently just out of reach — the suite
            // caught exactly that: two rollup-parity tests went from correct
            // totals to building nothing at all.
            //
            // Scoped to the operations this planner actually enqueues. It used
            // to match ANY non-complete task on the source, which quietly made
            // unrelated file debt veto rollup coverage: a day carrying a stuck
            // `SealedConsolidation` or an outstanding `HotPacking` could never
            // be backfilled. Prod 2026-08-17 — the whale's Aug 15
            // `SealedConsolidation` sat on attempt 370, and after the
            // coarse-backfill migration retired the fine-grained historical
            // tasks there was nothing left to claim and nothing allowed to
            // replace it: every task start for 30 minutes was today's, rollup
            // coverage froze at three days, and 7d/14d queries fell back to a
            // full raw scan (`rollup_miss_not_built`).
            //
            // Scoped to the TABLE, not the day. A day is not one unit of rollup
            // work — it is one per tier plus a dedup — and they are independent:
            // the 1h tier's unit reads the 1m TIER, so a pending unit for the 1m
            // tier says nothing about whether the 1h tier can be planned.
            //
            // Keyed on the day alone, one pending ten-minute frontier BaseRollup
            // slice vetoed every tier of that whole day. With ~47,000 pending
            // BaseRollup tasks — overwhelmingly frontier slices — that vetoed
            // essentially every day, so the derived tier could never be planned
            // at all. Prod 2026-08-19 01:30 UTC with all of #181-#191 live: the
            // planner reported `queued=2 remaining=0` while `94c5dc1f` sat at 34
            // days of 1m tier against 17 of 1h, and ZERO sealed-day derived units
            // were claimed in 25 minutes — all 84 derived claims were frontier
            // hours, because no historical derived task had ever been created.
            //
            // This is the same over-broad-veto bug the comment above records
            // being fixed once already (file debt disqualifying a day forever),
            // one level finer: narrowing from "any task" to "any rollup task"
            // was not narrow enough, because the rollup tiers do not block each
            // other either.
            //
            // The original rationale is also spent: it names `invalidate`'s
            // `deadline.max(...)` pushing an eligible task out of reach, but this
            // planner enqueues day-sized units through `enqueue`, which takes
            // `deadline.min(...)` and can only pull a deadline IN.
            let queued_tables: HashSet<(String, chrono::NaiveDate, String)> = {
                let journal = self.journal();
                journal
                    .tasks()
                    .filter(|task| task.key.source == source && crate::maintenance_coordinator::blocks_rollup_backfill(task))
                    .filter_map(|task| {
                        chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros)
                            .map(|time| (task.key.project_id.clone(), time.date_naive(), task.key.physical_table.clone()))
                    })
                    .collect()
            };
            // A cell is still wanted while ANY of its missing tiers is unqueued;
            // the per-table tests at the enqueue sites decide which to mint.
            want.retain(|(project_id, date)| {
                missing_tiers.get(&(project_id.clone(), *date)).is_some_and(|missing| {
                    missing.iter().any(|index| !queued_tables.contains(&(project_id.clone(), *date, schema.rollups[*index].table_name(&source))))
                })
            });
            // The days just filtered out are the ones that most need the proof:
            // a derived unit blocked by `dependencies_complete` stays queued
            // forever, which makes its day permanently ineligible for the
            // admission above, which is the only path that could have told it
            // the base tier is there. Prod 2026-08-18 22:50 UTC:
            // `pending_derived_rollup` did not move after #184 shipped, because
            // every one of those 759 tasks predated it.
            //
            // So prove it directly, over ALL candidate days rather than the 24
            // admitted per pass — this touches existing tasks only, mints
            // nothing, and cannot affect admission or deadlines.
            {
                let mut journal = self.journal();
                let mut proven = 0usize;
                for ((project_id, date), missing) in &missing_tiers {
                    if *date >= today || missing.iter().any(|index| schema.rollups[*index].derive_from.is_none()) {
                        continue;
                    }
                    let Some(day_start) = date.and_hms_opt(0, 0, 0).map(|time| time.and_utc().timestamp_micros()) else { continue };
                    let Ok(slice) = crate::maintenance_coordinator::TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS)) else { continue };
                    for index in missing {
                        let spec = &schema.rollups[*index];
                        if spec.derive_from.is_none() {
                            continue;
                        }
                        proven += journal.prove_base_tier_for_day(
                            &crate::maintenance_coordinator::TaskKey {
                                physical_table: spec.table_name(&source),
                                source: source.clone(),
                                project_id: project_id.clone(),
                                slice,
                                operation: crate::maintenance_coordinator::Operation::DerivedRollup,
                            },
                            day_start,
                            day_start.saturating_add(DAY_MICROS),
                        );
                    }
                }
                if proven != 0 {
                    journal.checkpoint()?;
                    info!(source, proven, event = "rollup_derived_base_tier_proven");
                }
            }
            // THE census, and it exists because its absence cost most of a night.
            // Every gauge in this system reports the STATE of coverage; none
            // reported what the planner BELIEVES about it, so four consecutive
            // correct fixes (#186, #189, #190, #192) shipped against a queue that
            // was empty for a reason none of them addressed, and the only way to
            // tell was to infer it from `queued=` on a line that does not print
            // when there is nothing to queue.
            //
            // `cells_missing` is what coverage says is absent; `cells_wanted` is
            // what survives the already-queued veto. missing=0 means the planner
            // sees no holes (suspect `partitions_of`); missing>0 with wanted=0
            // means the work is queued and the question is why it is not CLAIMED.
            // Those want opposite investigations and were indistinguishable.
            // Pairs with the cell census: that one says whether the planner sees
            // the holes, this one says why the work it already queued never runs.
            let (derived_pending, derived_sealed, derived_unproven, derived_quarantined, derived_not_due) = {
                let journal = self.journal();
                journal.claimability_census(crate::maintenance_coordinator::Operation::DerivedRollup, crate::support::now_micros())
            };
            info!(
                source,
                cells_missing,
                cells_wanted = want.len(),
                base_tier_ready = all_base_tier_ready.len(),
                tier_holes = all_tier_holes.len(),
                derived_pending,
                derived_sealed,
                derived_unproven,
                derived_quarantined,
                derived_not_due,
                derived_refusal = {
                    let journal = self.journal();
                    journal
                        .first_refused_sealed(crate::maintenance_coordinator::Operation::DerivedRollup, crate::support::now_micros())
                        .map_or_else(|| "none_pending".to_owned(), |(project, date, reason)| format!("{reason}:{project:.8}:{date}"))
                },
                cells_admitted = want.len().min(BACKFILL_PARTITIONS_PER_PASS),
                defer_enqueue,
                event = "rollup_backfill_census"
            );
            if want.is_empty() || defer_enqueue {
                continue;
            }
            // Newest first: recent days are what dashboards actually read, and
            // an oldest-first pass spends the whole horizon on data nobody has
            // queried yet.
            want.sort_by(|(pa, da), (pb, db)| db.cmp(da).then_with(|| pa.cmp(pb)));
            let total = want.len();
            want.truncate(BACKFILL_PARTITIONS_PER_PASS);
            // DAY-sized units, not the frontier's ten-minute slices.
            //
            // `enqueue_maintenance_hours` goes through `invalidate`, which
            // expands a day into `normal_units` — ~144 slices x
            // Dedup/BaseRollup/HotPacking x each tier, about 450 durable tasks
            // per (project, date). Prod 2026-08-17 reached 127k pending that
            // way, draining ~19 tasks/min because each unit costs 50-80s of
            // object-store work regardless of how little data it covers. That is
            // ~7 days to converge, and no amount of concurrency fixes it: the
            // task COUNT is the problem, not the throughput.
            //
            // `plan_compaction_debt` already enqueues day-sized slices for these
            // same partitions. Do the same here and let the coordinator split on
            // OBSERVED bytes — `run_coordinator_rollup_once` and
            // `run_coordinator_dedup_once` both call `split_time_task`, so an
            // oversized day divides by time and then by hash shard until each
            // child fits MAX_DECODED_BYTES. Coarse is safe; fine is merely slow.
            //
            // HotPacking is also dropped for sealed days: `plan_compaction_debt`
            // routes those to SealedConsolidation, so the ~41k HotPacking tasks
            // this used to mint for history were pure waste.
            let now = crate::support::now_micros();
            let created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            {
                let mut journal = self.journal();
                for (project_id, date) in &want {
                    let Some(day_start) = date.and_hms_opt(0, 0, 0).map(|time| time.and_utc().timestamp_micros()) else { continue };
                    let Ok(slice) = crate::maintenance_coordinator::TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS)) else { continue };
                    let mut enqueue = |physical_table: String, operation, base_tier_present| {
                        journal.enqueue_with_base_tier(
                            crate::maintenance_coordinator::TaskKey {
                                physical_table,
                                source: source.clone(),
                                project_id: project_id.clone(),
                                slice,
                                operation,
                            },
                            now,
                            crate::maintenance_coordinator::MAX_DECODED_BYTES,
                            created_unix_ms,
                            base_tier_present,
                        );
                    };
                    // Only the tiers this day is actually missing. Enqueueing
                    // every tier for a day that lacks one re-reads the whole raw
                    // partition to rebuild a rollup that already exists.
                    let missing = missing_tiers.get(&(project_id.clone(), *date)).cloned().unwrap_or_default();
                    let needs_source_scan = missing.iter().any(|index| schema.rollups[*index].derive_from.is_none());
                    // Dedup only when something must read RAW anyway. A derived
                    // tier aggregates the base TIER, so a day missing only its
                    // derived tier needs no source scan and no dedup — which is
                    // the common case here: 1m is 22-32 days deep while 1h is
                    // 2-6, so most queued days need the coarse tier alone.
                    if needs_source_scan && !queued_tables.contains(&(project_id.clone(), *date, source.clone())) {
                        enqueue(source.clone(), crate::maintenance_coordinator::Operation::Dedup, false);
                    }
                    for index in missing {
                        let spec = &schema.rollups[index];
                        let operation = if spec.derive_from.is_some() {
                            crate::maintenance_coordinator::Operation::DerivedRollup
                        } else {
                            crate::maintenance_coordinator::Operation::BaseRollup
                        };
                        // `!needs_source_scan` means no BASE tier is missing for
                        // this day, i.e. the tier this derived unit aggregates is
                        // already built. That is read from actual coverage, so it
                        // proves what `dependencies_complete` can otherwise only
                        // infer from journal records that a historical day does
                        // not have — see `MaintenanceTask::base_tier_present`.
                        //
                        // Sealed days only. `partitions_of` reports PRESENCE — a
                        // partition with one file counts — which is a true
                        // statement about a day that has stopped changing and a
                        // misleading one about a day still being written, where
                        // the base tier is mid-build by definition. Today's
                        // derived work is the frontier's anyway (`invalidate`
                        // mints it per hour with its base slices right there in
                        // the journal), so it loses nothing and keeps the strict
                        // check where the strict check is cheap and correct.
                        let physical_table = spec.table_name(&source);
                        if queued_tables.contains(&(project_id.clone(), *date, physical_table.clone())) {
                            continue;
                        }
                        let base_proven = operation == crate::maintenance_coordinator::Operation::DerivedRollup && !needs_source_scan && *date < today;
                        enqueue(physical_table, operation, base_proven);
                    }
                    queued = queued.saturating_add(1);
                }
                journal.checkpoint()?;
            }
            // Never let a bounded pass read as "covered everything".
            info!(source, queued = want.len(), remaining = total - want.len(), horizon_days = horizon, event = "rollup_backfill_planned");
        }
        // Published ONCE, after every source has contributed. Replacing per
        // source inside the loop meant the last table wiped the rest — see the
        // declaration above.
        {
            let mut journal = self.journal();
            journal.set_base_tier_ready(all_base_tier_ready);
            journal.set_tier_holes(all_tier_holes);
        }
        Ok(queued)
    }

    /// Claim one unit, bounding occupancy by units that have proven they cannot
    /// fit their deadline. See `maintenance_quarantine_slots` for the measurement.
    ///
    /// The permit is taken BEFORE the claim, because whether a unit is
    /// quarantined is a property of the task and only knowable once selected;
    /// it is released immediately when the claim turns out to be ordinary work,
    /// so the cap costs nothing in the common case.
    fn claim_coordinator_task(
        &self, operation: crate::maintenance_coordinator::Operation,
    ) -> Option<(crate::maintenance_coordinator::MaintenanceTask, Option<tokio::sync::OwnedSemaphorePermit>)> {
        let permit = Arc::clone(&self.maintenance_quarantine_slots).try_acquire_owned().ok();
        let task = {
            let mut journal = self.journal();
            journal.claim_next(operation, crate::support::now_micros(), permit.is_some())?
        };
        let quarantined = crate::maintenance_coordinator::TaskJournal::is_quarantined(&task);
        Some((task, permit.filter(|_| quarantined)))
    }

    fn log_task_started(&self, task: &crate::maintenance_coordinator::MaintenanceTask) {
        let key = &task.key;
        info!(operation = ?key.operation, table = %key.physical_table, project_id = %key.project_id, slice_start = key.slice.start_micros, slice_end = key.slice.end_micros,
            estimated_decoded_bytes = task.estimated_decoded_bytes, attempts = task.attempts,
            // Whether this unit knows what it reads. `record_input` has no
            // counter of its own, and without this there is no way to tell in
            // production whether footprint pricing has anything to work with.
            input_fp = task.input.map(|input| input.fp), event = "maintenance_task_started");
    }

    pub(crate) async fn run_coordinator_dedup_once(&self) -> Result<bool> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, Resources};
        use std::sync::atomic::Ordering::Relaxed;

        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(Operation::Dedup) else { return Ok(false) };
        let key = task.key.clone();
        self.log_task_started(&task);
        let _lease = crate::maintenance_coordinator::TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        let retry = |reason: String, delay: std::time::Duration| -> Result<()> { self.retry_task(&key, reason, delay) };

        if self.buffered_layer().is_some_and(|layer| layer.has_rows_in_range(&key.project_id, &key.source, key.slice.start_micros, key.slice.end_micros)) {
            retry("source_not_flushed".to_owned(), buffered_source_retry_delay(key.slice, crate::support::now_micros()))?;
            return Ok(true);
        }
        let Some(date) = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive()) else {
            retry("invalid_slice_timestamp".to_owned(), std::time::Duration::from_secs(3_600))?;
            return Ok(true);
        };
        let table = match self.resolve_table(&key.project_id, &key.source).await {
            Ok(table) => table,
            Err(error) => {
                retry(format!("resolve_source: {error:#}"), std::time::Duration::from_secs(30))?;
                return Ok(true);
            }
        };
        // Journal invalidations intentionally start with no byte estimate: the
        // acknowledged write does not have a Delta snapshot yet. Estimate the
        // narrow dedup projection now, from files whose timestamp statistics
        // overlap this exact slice. Without this preflight a whale project's
        // ordinary no-duplicate probe occupied one worker for 235 seconds in
        // production while still reporting `estimated_decoded_bytes=0`.
        let (estimated_bytes, whole_file_bytes, selected_paths) = {
            let schema = schema_or_default(&key.source);
            let required_columns = 3usize.saturating_add(schema.dedup_keys.len()).saturating_add(usize::from(schema.dedup_tiebreak.is_some()));
            let projected_numerator = u64::try_from(required_columns).unwrap_or(u64::MAX);
            let projected_denominator = u64::try_from(schema.fields.len().max(1)).unwrap_or(u64::MAX);
            let table = table.read().await;
            let date_string = date.to_string();
            let partition_paths = dedup_partition_paths(table.snapshot()?.log_data().iter().map(|file| file.path().to_string()), &key.project_id, &date_string)
                .into_iter()
                .collect::<HashSet<_>>();
            table
                .snapshot()?
                .log_data()
                .iter()
                .filter(|file| partition_paths.contains(file.path().as_ref()))
                .filter_map(|file| {
                    #[allow(deprecated)]
                    let add = file.add_action();
                    let bounds = add.get_stats().ok().flatten().map(|stats| {
                        (
                            stats.min_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                            stats.max_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                        )
                    });
                    let (min, max) = bounds.unwrap_or((None, None));
                    if let (Some(min), Some(max)) = (min, max)
                        && (min >= key.slice.end_micros || max < key.slice.start_micros)
                    {
                        return None;
                    }
                    let decoded = estimated_decoded_bytes(add.size);
                    let projected = decoded.saturating_mul(projected_numerator).div_ceil(projected_denominator);
                    let (share, whole) = slice_share_of_file(min, max, key.slice, estimated_row_groups(add.size));
                    Some((file.path().to_string(), projected.saturating_mul(share).div_ceil(whole.max(1)), projected))
                })
                // The prorated sum is this unit's own estimate; the unprorated
                // one is what a SIBLING over the same files would re-read, and
                // fusion needs both to price a bucket without counting a row
                // group once per child.
                .fold((0u64, 0u64, Vec::new()), |(share, whole, mut paths), (path, file_share, file_whole)| {
                    paths.push(path);
                    (share.saturating_add(file_share), whole.saturating_add(file_whole), paths)
                })
        };
        let input_footprint = crate::maintenance_coordinator::InputFootprint::new(selected_paths, whole_file_bytes);
        // Before the split test, because a unit that FITS still needs this: a
        // later timeout bisect knows only the key.
        if self.journal().record_input(&key, input_footprint) {
            self.journal().checkpoint()?;
        }
        if estimated_bytes > MAX_DECODED_BYTES && key.slice.width() > crate::maintenance_coordinator::MIN_SLICE_MICROS {
            let mut journal = self.journal();
            if journal.split_time_task(&key, estimated_bytes, Some(input_footprint)) {
                journal.checkpoint()?;
                info!(
                    table = %key.physical_table,
                    project_id = %key.project_id,
                    slice_start = key.slice.start_micros,
                    slice_end = key.slice.end_micros,
                    estimated_decoded_bytes = estimated_bytes,
                    event = "maintenance_dedup_task_split"
                );
                return Ok(true);
            }
        }
        let Some(_permit) = self.maintenance_admission.try_acquire(Resources { cpu: 1, decoded_bytes: MAX_DECODED_BYTES, object_reads: 1, object_writes: 1 })
        else {
            retry("resource_admission".to_owned(), std::time::Duration::from_secs(1))?;
            return Ok(true);
        };
        let probe_hash_shards = usize::try_from(estimated_bytes.div_ceil(MAX_DECODED_BYTES).clamp(1, DEDUP_BUCKET_COUNT)).unwrap_or(1);
        let limits = DedupExecutionLimits { max_decoded_bytes: MAX_DECODED_BYTES, max_concurrent_shards: 1, probe_hash_shards };
        // Certification is a property of the whole PARTITION — the read path keys
        // `dedup_clean_fp` on (project, table, date) and refuses the skip if ANY
        // in-window partition lacks an entry. This path is what grants it:
        // `dedup_sweep` was the sole caller of `record_certification`, and the
        // dedup cron skips every rollup-declared table ("owned by durable
        // coordinator tasks", 2026-08-16) — so for `otel_logs_and_spans`, the
        // table every 30d query reads, certification became unreachable the day
        // the coordinator took ownership. `DedupExec` then survives in every
        // plan, the single largest term left in 30d query latency.
        //
        // Unit shape must NOT gate the grant: `coarsen_to_width` caps units at
        // MAX_DECODED_BYTES (≈6h for otel_logs_and_spans) and true day-wide
        // units die at the 300s Dedup deadline, so prod never produces a
        // surviving day-wide unit (`cert_granted_total=0`, 2026-08-20). Instead
        // each clean pass records its slice in `dedup_slice_coverage`; when the
        // union covers the UTC day over one unmoved file fingerprint,
        // `record_clean_slice` grants the certification. A day-wide unit is the
        // degenerate single-slice case.
        let pre_files = {
            let table = table.read().await;
            Self::partition_files_by_pid(&table, &format!("date={date}"))?.remove(&key.project_id).unwrap_or_default()
        };
        match self.dedup_partition_range_limited(&table, &key.source, &key.project_id, date, Some(key.slice), Some(limits)).await {
            Ok((dropped, true)) => {
                // Before the journal lock: `record_clean_slice` awaits, and the
                // journal guard is a std Mutex.
                match self.record_clean_slice(&table, &key.physical_table, &key.project_id, date, (key.slice, dropped), &pre_files).await {
                    // Coordinator-owned tables are excluded from `dedup_sweep`,
                    // whose end-of-tick snapshot was otherwise the only
                    // persistence site for this cache.
                    Ok(Some(_)) => self.persist_certifications(),
                    Ok(None) => {}
                    Err(error) => warn!(%error, project_id = %key.project_id, %date, "certification bookkeeping failed after a clean dedup slice"),
                }
                let mut journal = self.journal();
                journal.complete(&key);
                journal.checkpoint()?;
                crate::observability::maintenance_stats().maintenance_processed_bytes.fetch_add(task.estimated_decoded_bytes, Relaxed);
            }
            Ok((_, false)) => retry("dedup_incomplete".to_owned(), std::time::Duration::from_secs(30))?,
            Err(error) => {
                let delay = std::time::Duration::from_secs(1u64 << task.attempts.min(8));
                let delay_micros = i64::try_from(delay.as_micros()).unwrap_or(i64::MAX);
                let mut journal = self.journal();
                journal.retry_or_split(&key, format!("dedup: {error:#}"), crate::support::now_micros().saturating_add(delay_micros), task.attempts);
                journal.checkpoint()?;
            }
        }
        Ok(true)
    }

    /// Execute ONE maintenance unit end-to-end and report where its time went.
    /// Backs the `run-unit` CLI: the per-unit cost decomposition (handover
    /// §7.2, plan Phase 1.1) as a five-minute command instead of fleet-counter
    /// inference. Point TIMEFUSION_DATA_DIR at a scratch dir: the journal must
    /// hold no other claimable work, or the coordinator may claim that first.
    pub async fn run_unit_once(
        &self, source: &str, project_id: &str, date: chrono::NaiveDate, operation: crate::maintenance_coordinator::Operation, slice_hours: i64,
        offset_hours: i64,
    ) -> Result<UnitRunReport> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, MaintenanceTask, Operation, TaskKey, TaskState, TimeSlice};
        use std::sync::atomic::Ordering::Relaxed;
        let schema = get_schema(source).ok_or_else(|| anyhow::anyhow!("unknown source table {source}"))?;
        let base_table = || schema.rollups.iter().find(|spec| spec.derive_from.is_none()).map(|spec| spec.table_name(source));
        let physical_table = match operation {
            Operation::BaseRollup => base_table().ok_or_else(|| anyhow::anyhow!("{source} declares no base rollup"))?,
            Operation::DerivedRollup => schema
                .rollups
                .iter()
                .find(|spec| spec.derive_from.is_some())
                .ok_or_else(|| anyhow::anyhow!("{source} declares no derived rollup"))?
                .table_name(source),
            _ => source.to_owned(),
        };
        let day_start = date.and_hms_opt(0, 0, 0).ok_or_else(|| anyhow::anyhow!("invalid date {date}"))?.and_utc().timestamp_micros();
        // Offset from midnight, so a day can be TILED. Without it every slice
        // starts at 00:00 and successive widths merely replace one another —
        // there is no way to publish 18:00-24:00 at all, and a tenant whose day
        // exceeds MAX_DECODED_BYTES has no day-wide slice either, so the late
        // hours of such a day were unreachable by any invocation.
        let start = day_start.saturating_add(offset_hours.saturating_mul(3_600_000_000));
        let slice = TimeSlice::new(start, start.saturating_add(slice_hours.saturating_mul(3_600_000_000)))?;
        let key = TaskKey { physical_table, source: source.to_owned(), project_id: project_id.to_owned(), slice, operation };
        let now = crate::support::now_micros();
        {
            let mut journal = self.journal();
            // A CLI unit must run the unit it was ASKED for. The runners below
            // claim whatever ranks FIRST, and this journal is not scratch: a
            // Database built against prod storage plans prod's whole outstanding
            // queue into it. So `run-unit --project X --date D` silently ran
            // someone else's slice — 2026-08-20, a repair pass of 100 targeted
            // units spent 28 of them on a 6-hour slice of a different project
            // while every requested day stayed Pending and its untagged files
            // survived. The report printed the REQUESTED key, so it read as
            // success. Retire everything else, so ours is the only claimable
            // task and the report cannot lie.
            let others = journal.tasks().map(|task| task.key.clone()).filter(|other| *other != key).collect::<Vec<_>>();
            for other in &others {
                journal.complete(other);
            }
            if operation == Operation::DerivedRollup {
                // A derived unit is unclaimable until its base generation is
                // Complete; a scratch journal has none. Seed the dependency
                // with a synthetic completed base covering the whole day.
                let base_key = TaskKey {
                    physical_table: base_table().ok_or_else(|| anyhow::anyhow!("{source} declares no base rollup"))?,
                    slice: TimeSlice::new(day_start, day_start.saturating_add(86_400_000_000))?,
                    operation: Operation::BaseRollup,
                    ..key.clone()
                };
                journal.upsert(MaintenanceTask {
                    key: base_key,
                    state: TaskState::Complete,
                    deadline_micros: 0,
                    estimated_decoded_bytes: 0,
                    hash_shard: 0,
                    hash_shards: 1,
                    attempts: 0,
                    created_unix_ms: 0,
                    retry_reason: None,
                    publication: None,
                    // A synthetic COMPLETE base for the day is exactly the proof
                    // `dependencies_complete` looks for, so say so.
                    base_tier_present: true,
                    input: None,
                    parent_measured_bytes: None,
                });
            }
            journal.enqueue(key.clone(), now, MAX_DECODED_BYTES, u64::try_from(now.div_euclid(1_000)).unwrap_or_default());
        }
        let stats = crate::observability::maintenance_stats();
        let snapshot = || {
            [
                stats.rollup_scan_duration_ms.load(Relaxed),
                stats.rollup_staging_duration_ms.load(Relaxed),
                stats.rollup_commit_duration_ms.load(Relaxed),
                stats.rollup_end_to_end_duration_ms.load(Relaxed),
                stats.rollup_scan_cohorts.load(Relaxed),
            ]
        };
        let counters = snapshot();
        let started = std::time::Instant::now();
        match operation {
            Operation::Dedup => {
                self.run_coordinator_dedup_once().await?;
            }
            Operation::BaseRollup | Operation::DerivedRollup => {
                self.run_coordinator_rollup_once(operation).await?;
            }
            _ => {
                self.run_coordinator_compaction_once(operation).await?;
            }
        }
        let wall = started.elapsed();
        let after = snapshot();
        let (state, retry_reason) = {
            let journal = self.journal();
            journal.tasks().find(|task| task.key == key).map(|task| (Some(task.state), task.retry_reason.clone())).unwrap_or((None, None))
        };
        Ok(UnitRunReport {
            operation,
            project_id: project_id.to_owned(),
            date,
            wall_ms: u64::try_from(wall.as_millis()).unwrap_or(u64::MAX),
            scan_ms: after[0] - counters[0],
            staging_ms: after[1] - counters[1],
            commit_ms: after[2] - counters[2],
            end_to_end_ms: after[3] - counters[3],
            cohorts: after[4] - counters[4],
            state,
            retry_reason,
        })
    }

    async fn run_coordinator_rollup_once(&self, operation: crate::maintenance_coordinator::Operation) -> Result<bool> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Resources, TaskState};
        use deltalake::{
            kernel::{Action, transaction::TableReference},
            protocol::{DeltaOperation, SaveMode},
            writer::DeltaWriter,
        };
        use std::{
            hash::{Hash, Hasher},
            sync::atomic::Ordering::Relaxed,
        };

        // Reads back the slice an `Add` was tagged with — used repeatedly below
        // to reconcile a rebuilt slice's coverage against already-live files.
        fn slice_tag_range(add: &deltalake::kernel::Add) -> Option<(i64, i64)> {
            let tag = |name: &str| add.tags.as_ref().and_then(|tags| tags.get(name)).and_then(Option::as_deref);
            Some((
                tag(crate::maintenance_coordinator::TAG_SLICE_START)?.parse::<i64>().ok()?,
                tag(crate::maintenance_coordinator::TAG_SLICE_END)?.parse::<i64>().ok()?,
            ))
        }
        fn tag_project(add: &deltalake::kernel::Add) -> Option<&str> {
            add.tags.as_ref().and_then(|tags| tags.get(crate::maintenance_coordinator::TAG_PROJECT)).and_then(Option::as_deref)
        }

        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(operation) else { return Ok(false) };
        let key = task.key.clone();
        self.log_task_started(&task);
        let _lease = crate::maintenance_coordinator::TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        let retry = |reason: String, delay: std::time::Duration| -> Result<()> { self.retry_task(&key, reason, delay) };
        let Some(source_schema) = get_schema(&key.source) else {
            retry("source_schema_missing".to_owned(), std::time::Duration::from_secs(300))?;
            return Ok(true);
        };
        let Some(spec) = source_schema.rollups.iter().find(|spec| spec.table_name(&key.source) == key.physical_table) else {
            retry("rollup_spec_missing".to_owned(), std::time::Duration::from_secs(300))?;
            return Ok(true);
        };
        let derived = operation == crate::maintenance_coordinator::Operation::DerivedRollup;
        let from = if derived {
            spec.derive_from
                .as_ref()
                .and_then(|name| source_schema.rollups.iter().find(|candidate| candidate.name.as_deref() == Some(name.as_str())))
                .map(|base| base.table_name(&key.source))
                .ok_or_else(|| anyhow::anyhow!("derived rollup {} has no base", key.physical_table))?
        } else {
            key.source.clone()
        };
        let Some(date) = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive()) else {
            retry("invalid_slice_timestamp".to_owned(), std::time::Duration::from_secs(3_600))?;
            return Ok(true);
        };
        if !derived
            && self.buffered_layer().is_some_and(|layer| layer.has_rows_in_range(&key.project_id, &key.source, key.slice.start_micros, key.slice.end_micros))
        {
            retry("source_not_flushed".to_owned(), buffered_source_retry_delay(key.slice, crate::support::now_micros()))?;
            return Ok(true);
        }

        // The witness must describe the RAW source partition, because that is
        // what the read path verifies it against: `route.source` is the raw
        // table for EVERY tier. A DERIVED unit reads its parent tier, so taking
        // the witness from `from_table` states a fact about the wrong table and
        // can never match.
        let witness_table = match derived {
            false => None,
            true => match self.resolve_table(&key.project_id, &key.source).await {
                Ok(table) => Some(table),
                Err(error) => {
                    retry(format!("resolve_witness_source: {error:#}"), std::time::Duration::from_secs(30))?;
                    return Ok(true);
                }
            },
        };
        let from_table = match self.resolve_table(&key.project_id, &from).await {
            Ok(table) => table,
            Err(error) => {
                retry(format!("resolve_input: {error:#}"), std::time::Duration::from_secs(30))?;
                return Ok(true);
            }
        };
        let mut required_columns: HashSet<&str> = ["project_id", "date", "timestamp"].into_iter().collect();
        required_columns.extend(source_schema.dedup_keys.iter().map(String::as_str));
        required_columns.extend(source_schema.dedup_tiebreak.iter().map(String::as_str));
        required_columns.extend(source_schema.tombstone_column.iter().map(String::as_str));
        required_columns.extend(spec.dimensions.iter().map(String::as_str));
        required_columns.extend(spec.measures.iter().filter_map(|measure| measure.column.as_deref()));
        required_columns.extend(
            spec.measures
                .iter()
                .filter_map(|measure| measure.filter.as_deref())
                .flat_map(|filter| filter.split(|character: char| !(character.is_ascii_alphanumeric() || character == '_')))
                .filter(|token| source_schema.fields.iter().any(|field| field.name == *token)),
        );
        let projected_numerator = u64::try_from(required_columns.len()).unwrap_or(u64::MAX);
        let projected_denominator = u64::try_from(source_schema.fields.len().max(1)).unwrap_or(u64::MAX);
        let mut untagged_inputs = 0u64;
        let (snapshot, log_store, selected, estimated_bytes, source_rows, partition_identity, whole_file_bytes) = {
            let table = from_table.read().await;
            let witness_guard = match &witness_table {
                Some(table) => Some(table.read().await),
                None => None,
            };
            let witness_source: &DeltaTable = witness_guard.as_deref().unwrap_or(&table);
            let snapshot = Arc::new(table.snapshot()?.snapshot().clone());
            let date_string = date.to_string();
            // The witness the read path re-checks this slice against, AND the
            // whole-partition fingerprint the DATE-level path checks. Taken with
            // the SAME call the read path uses and the same unbounded bound, so
            // both are the same computation — see `slice_coverage_agrees` and
            // the `coverage.source_fp != source_fp` test in `ProjectRoutingTable`.
            //
            // Taking the fingerprint HERE is what makes the date-level producer
            // sound. It is read from the very snapshot this build aggregates, so
            // it states what was true at build time rather than asserting
            // freshness after the fact — and if the partition moves afterwards
            // the read path sees a different fingerprint and refuses.
            let partition_stats = Self::partition_stats_bounded(witness_source, tiebreak_of(&key.source), &|_, _| i64::MAX).ok().and_then(|mut stats| {
                stats.remove(&(key.project_id.clone(), date_string.clone())).or_else(|| stats.remove(&("default".to_string(), date_string.clone())))
            });
            let source_rows = partition_stats.map(|stats| stats.rows);
            // `(fingerprint, min_ts)`: the identity the date-level read path
            // compares, plus the earliest row the partition actually holds —
            // which is what decides whether this slice may claim the day's
            // opening hours (see the publish site).
            let partition_identity = partition_stats.map(|stats| (stats.fingerprint, stats.min_ts));
            let partition_paths = dedup_partition_paths(snapshot.log_data().iter().map(|file| file.path().to_string()), &key.project_id, &date_string);
            let mut selected = Vec::new();
            let mut estimated = 0u64;
            let mut whole_file_bytes = 0u64;
            for file in snapshot.log_data().iter() {
                let path = file.path().to_string();
                if !partition_paths.contains(&path) {
                    continue;
                }
                #[allow(deprecated)]
                let add = file.add_action();
                if !derived
                    && let Ok(Some(stats)) = add.get_stats()
                    && let (Some(min), Some(max)) = (
                        stats.min_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                        stats.max_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                    )
                    && (min >= key.slice.end_micros || max < key.slice.start_micros)
                {
                    continue;
                }
                if derived {
                    // OVERLAP, not containment. A base file is tagged with the
                    // slice of the UNIT that wrote it, and that unit's width is
                    // unrelated to this one's: the backfill writes day-wide base
                    // units while derived units are an hour. Containment made a
                    // day-tagged file impossible to select from an hour-wide
                    // derived slice (`day_end <= hour_end` is never true), so
                    // every backfilled day published rows=0 and was then marked
                    // complete — prod 2026-08-17: project 87576849 had 17,705
                    // rows in the 1m tier for 08-03 and its 1h unit for 08-03
                    // produced nothing. Only days written by the live frontier
                    // (ten-minute units, which DO fit an hour) ever had a 1h
                    // tier, which is why 14d/30d queries never routed.
                    //
                    // Reading a wider file is safe because the aggregation
                    // already bounds rows exactly
                    // (`timestamp >= slice.start AND timestamp < slice.end`),
                    // and rebuilt generations are removed from the snapshot, so
                    // no row is counted twice.
                    // A base file with no readable slice tags is invisible to
                    // the coarse tier FOREVER — the unit publishes rows=0 and is
                    // marked complete, which reads exactly like "this slice is
                    // genuinely empty". Counted separately so the two are
                    // distinguishable: #139 fixed day-tagged files being
                    // unselectable, and if any tier still comes back empty this
                    // is the number that says whether tags are the reason.
                    match slice_tag_range(&add) {
                        Some((start, end)) => {
                            if tag_project(&add) != Some(key.project_id.as_str()) || !key.slice.overlaps(start, end) {
                                continue;
                            }
                        }
                        // No slice tags — a file written before tagging existed.
                        // Prune it on its OWN timestamp statistics, exactly as the
                        // base branch above already does, instead of dropping it.
                        //
                        // Dropping it was silent and permanent: prod 2026-08-18
                        // logged `maintenance_rollup_untagged_input` 15 times in 20
                        // minutes, and EVERY rollup published in that window was a
                        // derived unit with rows=0. The 1h tier — the one 14d/30d
                        // queries read — sat at 6 days while the 1m tier had 22,
                        // because history written before tagging can never reach it.
                        //
                        // Safe because this is pruning, not correctness: the
                        // candidate set is already scoped to this (project, date)
                        // partition, and the aggregation SQL filters `project_id`,
                        // `date` and the timestamp range itself. A file kept here
                        // that does not belong costs IO; a file dropped here loses
                        // its rows for good. Missing statistics therefore mean
                        // KEEP, never skip.
                        _ => {
                            untagged_inputs = untagged_inputs.saturating_add(1);
                            if let Ok(Some(stats)) = add.get_stats()
                                && let (Some(min), Some(max)) = (
                                    stats.min_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                                    stats.max_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                                )
                                && (min >= key.slice.end_micros || max < key.slice.start_micros)
                            {
                                continue;
                            }
                        }
                    }
                }
                let decoded = estimated_decoded_bytes(add.size);
                let projected = decoded.saturating_mul(projected_numerator).div_ceil(projected_denominator);
                let (file_min, file_max) = add
                    .get_stats()
                    .ok()
                    .flatten()
                    .map(|stats| {
                        (
                            stats.min_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                            stats.max_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
                        )
                    })
                    .unwrap_or((None, None));
                let (share, whole) = slice_share_of_file(file_min, file_max, key.slice, estimated_row_groups(add.size));
                estimated = estimated.saturating_add(projected.saturating_mul(share).div_ceil(whole.max(1)));
                // Unprorated: what a SIBLING slice over these same files would
                // re-read. `coarsen_to_width` charges that once per distinct
                // file set instead of once per child.
                whole_file_bytes = whole_file_bytes.saturating_add(projected);
                selected.push(path);
            }
            (snapshot, table.log_store(), selected, estimated, source_rows, partition_identity, whole_file_bytes)
        };
        let input_footprint = crate::maintenance_coordinator::InputFootprint::new(&selected, whole_file_bytes);
        // Same reason as the dedup preflight: record it on every claim, not only
        // when this one splits, or a timeout bisect mints footprint-less
        // children that fusion can only sum.
        if self.journal().record_input(&key, input_footprint) {
            self.journal().checkpoint()?;
        }
        if untagged_inputs > 0 {
            crate::observability::maintenance_stats().rollup_untagged_inputs.fetch_add(untagged_inputs, std::sync::atomic::Ordering::Relaxed);
            // Since #169 these files are KEPT — pruned on their own timestamp
            // statistics rather than discarded — so this is no longer a report of
            // data that can never arrive. It now measures how much of the base
            // tier predates tagging and is therefore being selected by the wider,
            // stats-only test. Left at `warn` while that population is still
            // large; it should shrink as those partitions are rewritten.
            //
            // The old text said "their rows can never reach this tier", which was
            // true when it was written and false the moment #169 shipped. Stale
            // log text is worse than none: this exact line was the evidence used
            // to diagnose the empty coarse tier, and it would have been read the
            // same way again.
            warn!(
                table = %key.physical_table,
                project_id = %key.project_id,
                slice_start = key.slice.start_micros,
                untagged_inputs,
                event = "maintenance_rollup_untagged_input",
                "base files carry no slice tags; selected on timestamp statistics instead"
            );
        }
        if estimated_bytes > MAX_DECODED_BYTES && key.slice.width() > crate::maintenance_coordinator::MIN_SLICE_MICROS {
            let mut journal = self.journal();
            if journal.split_time_task(&key, estimated_bytes, Some(input_footprint)) {
                journal.checkpoint()?;
                return Ok(true);
            }
        }
        let hash_shards = estimated_bytes.div_ceil(MAX_DECODED_BYTES).max(1);
        anyhow::ensure!(hash_shards <= 65_536, "one-minute slice needs {hash_shards} hash shards; maximum is 65536");
        let per_shard_bytes = estimated_bytes.div_ceil(hash_shards).max(1);
        let Some(_permit) = self.maintenance_admission.try_acquire(Resources { cpu: 1, decoded_bytes: per_shard_bytes, object_reads: 1, object_writes: 1 })
        else {
            retry("resource_admission".to_owned(), std::time::Duration::from_secs(1))?;
            return Ok(true);
        };
        let mut fingerprint_items = selected.clone();
        fingerprint_items.sort_unstable();
        let mut fingerprint = fnv::FnvHasher::default();
        fingerprint_items.hash(&mut fingerprint);
        let source_fp = fingerprint.finish();

        // Phase timings for the LIVE coordinator unit. The existing
        // `rollup_*_duration_ms` counters were only ever written by
        // `stage_rollup_wave` / `commit_rollup_wave`, which belong to the older
        // cohort path that no longer runs — so in production all four read 0
        // while `rollup_commit_actions` was non-zero, and the unit that actually
        // costs the time was completely un-instrumented.
        //
        // That time is the number every throughput conclusion depends on:
        // 16 concurrent slots at 2.0 completions/min means the average unit takes
        // ~8 minutes, and nobody has ever known which phase owns it. Scheduling
        // was reweighted on the assumption it was queueing (#167) and moved
        // throughput zero, because the constraint is per-unit cost.
        let unit_started = std::time::Instant::now();
        let ctx = self.bounded_rollup_maintenance_context()?;
        let provider = Self::narrow_provider(log_store, snapshot, selected, None).await.map_err(|error| anyhow::anyhow!("slice provider: {error}"))?;
        const RAW: &str = "__maintenance_slice_raw";
        // What the PHYSICAL table has, which is not what the spec declares — see
        // `slice_input_sql`.
        let present_columns: std::collections::HashSet<String> = provider.schema().fields().iter().map(|field| field.name().clone()).collect();
        ctx.register_table(RAW, provider)?;
        // The schema of whatever is registered as RAW — which is NOT `source_schema`
        // for a derived tier. A derived rollup reads the BASE TIER, and that tier is
        // merge-on-read: a rebuilt bucket appends a new version rather than replacing
        // the old one, so several rows share an `id` and differ only by `updated_at`.
        //
        // This branch used to be `if derived || …` — derived tiers read their source
        // with a bare `SELECT *` and no dedup, on the assumption that a rollup tier
        // already holds one row per bucket. It does not, and the derived aggregate
        // SUMs every superseded version. Prod 2026-08-20, project 98fdd4f3, hour
        // 08-18 10:00: the 1m tier held 2,453 rows for 342 distinct ids (7.17
        // versions each) and the 1h tier that reads it reported 157,110 requests
        // where the truth was 31,018 — every measure inflated by the SAME factor,
        // which is the fingerprint of summing versions rather than rows. A day whose
        // base tier had been compacted to one version per id (08-13) was exact, so
        // the error tracks version multiplicity at build time and is FROZEN into the
        // tier: unlike a raw-side over-count it never self-heals, it only goes away
        // when that day is rebuilt.
        let input_schema = if derived { get_schema(&from).unwrap_or(source_schema) } else { source_schema };
        let tier_dedup = derived.then(|| crate::rollup::rollup_tier_dedup(input_schema)).flatten();
        let generation = crate::rollup::generation_id(spec, &key.source, &key.project_id, &date.to_string(), source_fp);
        let target_schema = get_schema(&key.physical_table).ok_or_else(|| anyhow::anyhow!("rollup target schema missing"))?;
        let default_shard_keys = || ["project_id".to_owned(), "timestamp".to_owned()].into_iter().chain(spec.dimensions.iter().cloned()).collect::<Vec<_>>();
        let mut shard_keys = if derived { default_shard_keys() } else { source_schema.dedup_keys.clone() };
        if shard_keys.is_empty() {
            shard_keys = default_shard_keys();
        }
        let shard_key_sql = shard_keys.iter().map(|field| format!("CAST({} AS VARCHAR)", crate::rollup::quoted(field))).collect::<Vec<_>>().join(", ");
        // Same reasoning as the dedup rewrite's bucketing: an even, stable spread
        // is all this needs, and a cryptographic digest evaluated per row is not
        // free — see `hash_bucket`.
        const MAINTENANCE_SLICE_BUCKETS: u64 = 65_536;
        let shard_hash = format!("hash_bucket(arrow_cast(concat_ws(chr(31), {shard_key_sql}), 'Utf8View'), {MAINTENANCE_SLICE_BUCKETS})");
        let mut shard_states = Vec::new();
        let mut aggregate = Vec::new();
        for shard in 0..hash_shards {
            let input = format!("__maintenance_slice_input_{shard}");
            let shard_predicate = if hash_shards == 1 {
                String::new()
            } else {
                let lo = shard * MAINTENANCE_SLICE_BUCKETS / hash_shards;
                let hi = (shard + 1) * MAINTENANCE_SLICE_BUCKETS / hash_shards;
                let upper = if hi < MAINTENANCE_SLICE_BUCKETS { format!(" AND {shard_hash} < {hi}") } else { String::new() };
                format!(" AND {shard_hash} >= {lo}{upper}")
            };
            let dedup = match (&tier_dedup, derived) {
                (Some((keys, tiebreak, tombstone)), _) => Some(crate::rollup::SliceDedup { keys, tiebreak: Some(tiebreak), tombstone: *tombstone }),
                // A derived tier whose schema lacks the identity columns has no
                // safe collapse; a base slice uses its raw source's own keys.
                (None, true) => None,
                (None, false) => Some(crate::rollup::SliceDedup {
                    keys: &source_schema.dedup_keys,
                    tiebreak: source_schema.dedup_tiebreak.as_deref(),
                    tombstone: source_schema.tombstone_column.as_deref(),
                }),
            };
            let input_sql = crate::rollup::slice_input_sql(
                input_schema,
                dedup,
                RAW,
                &key.project_id,
                (key.slice.start_micros, key.slice.end_micros),
                &shard_predicate,
                Some(&present_columns),
            );
            let frame = ctx.sql(&input_sql).await?;
            ctx.register_table(&input, Arc::new(datafusion::datasource::ViewTable::new(frame.logical_plan().clone(), Some(input_sql))))?;
            let aggregate_sql = crate::rollup::build_cohort_sql_range_mode(
                spec,
                &key.source,
                &input,
                std::slice::from_ref(&key.project_id),
                &date.to_string(),
                (key.slice.start_micros, key.slice.end_micros),
                derived,
            )?;
            let shard_aggregate = ctx.sql(&aggregate_sql).await?.collect().await?;
            if hash_shards == 1 {
                aggregate = shard_aggregate;
            } else {
                let mut shaped = crate::rollup::to_rollup_batches_by_project(
                    spec,
                    &key.source,
                    &date.to_string(),
                    &HashMap::from([(key.project_id.clone(), generation.clone())]),
                    &shard_aggregate,
                )?;
                shard_states.extend(shaped.remove(&key.project_id).unwrap_or_default());
            }
        }
        if hash_shards > 1 && !shard_states.is_empty() {
            const STATES: &str = "__maintenance_slice_states";
            ctx.register_table(STATES, Arc::new(datafusion::datasource::MemTable::try_new(target_schema.schema_ref(), vec![shard_states])?))?;
            let merge_sql = crate::rollup::build_cohort_sql_range_mode(
                spec,
                &key.source,
                STATES,
                std::slice::from_ref(&key.project_id),
                &date.to_string(),
                (key.slice.start_micros, key.slice.end_micros),
                true,
            )?;
            aggregate = ctx.sql(&merge_sql).await?.collect().await?;
        }
        let mut by_project = crate::rollup::to_rollup_batches_by_project(
            spec,
            &key.source,
            &date.to_string(),
            &HashMap::from([(key.project_id.clone(), generation.clone())]),
            &aggregate,
        )?;
        let batches = by_project.remove(&key.project_id).unwrap_or_default();
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
        // Everything above is read + aggregate: the source scan, the per-shard
        // aggregates and the merge. Everything below is write.
        let scan_ms = unit_started.elapsed().as_millis() as u64;
        let stage_started = std::time::Instant::now();

        let target_ref = self.get_or_create_table(&key.project_id, &key.physical_table).await?;
        let staging_table = target_ref.read().await.clone();
        let stage_store = staging_table.log_store().object_store(None);
        let (batches, sorted) = self.sort_flush_group(target_schema, batches, UnsortedFallback::Forbid).await?;
        let mut writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)?.with_writer_properties(self.create_writer_properties(
            target_schema,
            self.config.parquet.timefusion_zstd_level_intermediate,
            sorted,
        ));
        let arrow_schema = writer.arrow_schema();
        for batch in batches {
            writer.write(deltalake::kernel::schema::cast_record_batch(&batch?, arrow_schema.clone(), true, true)?).await?;
        }
        let mut adds = writer.flush().await?.into_iter().map(Action::Add).collect::<Vec<_>>();
        let stage_ms = stage_started.elapsed().as_millis() as u64;
        let commit_started = std::time::Instant::now();
        for action in &mut adds {
            let Action::Add(add) = action else { continue };
            add.data_change = true;
            let mut tags = add.tags.take().unwrap_or_default();
            for (name, value) in [
                (crate::maintenance_coordinator::TAG_SOURCE, key.source.clone()),
                (crate::maintenance_coordinator::TAG_PROJECT, key.project_id.clone()),
                (crate::maintenance_coordinator::TAG_SLICE_START, key.slice.start_micros.to_string()),
                (crate::maintenance_coordinator::TAG_SLICE_END, key.slice.end_micros.to_string()),
                (crate::maintenance_coordinator::TAG_SOURCE_FINGERPRINT, source_fp.to_string()),
                (crate::maintenance_coordinator::TAG_GENERATION, generation.clone()),
                // Absent (older generations) means the read path cannot verify
                // this slice and must refuse it, so write the sentinel rather
                // than omitting the tag when the source reports no count.
                (crate::maintenance_coordinator::TAG_SOURCE_ROWS, source_rows.unwrap_or(-1).to_string()),
            ] {
                tags.insert(name.to_owned(), Some(value));
            }
            add.tags = Some(tags);
        }

        let live_adds = staging_table
            .snapshot()?
            .log_data()
            .iter()
            .map(|file| {
                #[allow(deprecated)]
                file.add_action()
            })
            .collect::<Vec<_>>();
        // Containment, not exact equality. Slice WIDTH is not stable for a
        // given range: `split_time_task` cuts a day into children and
        // `coarsen_sealed_slices` (#134) fuses them back, so the same hours get
        // published at different widths over time. Matching only the identical
        // slice let a day-wide file and an hour-wide file inside it both stay
        // live, and a dashboard SUMmed both — the test caught 10 where 9 was
        // right.
        //
        // Removing a file WIDER than this slice is safe because a unit is only
        // superseded by children that tile its whole range, so the rest is
        // republished; until it is, the coverage check sees an incomplete tier
        // and the query falls back to raw rather than reading a hole.
        //
        // `slice_retires` also retires UNTAGGED files — see there for the three
        // proofs it accepts and for the damage it undoes.
        let date_string = date.to_string();
        let in_partition = |add: &deltalake::kernel::Add| {
            Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default")
                .is_some_and(|(project, date)| project == key.project_id && date == date_string)
        };
        // Every tagged range that will be LIVE here after this commit, this
        // slice included. Ranges CONTAINED in this slice are omitted on purpose:
        // those files are the ones being replaced, and this slice already covers
        // their span. Their union is the only proof available for a tenant whose
        // day is too big to publish whole.
        let covered = std::iter::once((key.slice.start_micros, key.slice.end_micros))
            .chain(live_adds.iter().filter(|add| in_partition(add)).filter_map(slice_tag_range))
            .collect::<Vec<_>>();
        let publish = crate::rollup::SlicePublish {
            project_id: &key.project_id,
            date: &date_string,
            slice: (key.slice.start_micros, key.slice.end_micros),
            rows,
            covered: &covered,
        };
        let replaced = live_adds
            .iter()
            .filter(|add| {
                let partition = Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default");
                let file = crate::rollup::LiveFile {
                    slice: slice_tag_range(add),
                    project: tag_project(add),
                    partition: partition.as_ref().map(|(project, date)| (project.as_str(), date.as_str())),
                    stats: add.stats.as_deref().and_then(crate::rollup::stats_time_range),
                };
                crate::rollup::slice_retires(&file, &publish)
            })
            .cloned()
            .collect::<Vec<_>>();
        // Published from here because the information is already in hand — no
        // extra listing — and because this is the only place that sees a tier's
        // live set. `found` is the whole tier's untagged count, not just this
        // partition's, so it reads as a backlog draining rather than a per-unit
        // blip; `retired` proves this unit actually removed one.
        {
            let untagged = |add: &deltalake::kernel::Add| slice_tag_range(add).is_none();
            let stats = crate::observability::maintenance_stats();
            // Record per TABLE and export the SUM. One shared slot was
            // overwritten by whichever tier published last, so a publish to an
            // already-clean tier reported 0 while another still held 67 untagged
            // files — a gauge reading clean over live damage is the exact
            // failure it exists to catch.
            self.rollup_tier_untagged.insert(key.physical_table.clone(), live_adds.iter().filter(|add| untagged(add)).count() as u64);
            stats.rollup_tier_untagged_found.store(self.rollup_tier_untagged.iter().map(|entry| *entry.value()).sum(), Relaxed);
        }
        // Deferred to AFTER the commit. Counted here, this read 21 retired
        // within an hour on prod 2026-08-22 while a fresh Delta-log replay said
        // the live count had not moved at all — two deploys had killed the units
        // between the replace-set and the commit. Worse than the wrong number:
        // `clear_untagged_cell` was removing the hole boost from partitions
        // whose repair never landed, so the ranking fix would have switched
        // itself off on exactly the cells that still needed it.
        let no_identity = |add: &deltalake::kernel::Add| slice_tag_range(add).is_none();
        let retiring = replaced.iter().filter(|add| no_identity(add)).count() as u64;
        let leaves_partition_clean = !live_adds.iter().filter(|add| in_partition(add) && !replaced.iter().any(|gone| gone.path == add.path)).any(no_identity);
        // A slice covered by a STRICTLY WIDER live file must not publish: the
        // replace-set only removes files CONTAINED in this slice, so both would
        // stay live and a dashboard SUMs both. Widths are not stable for a range
        // (`split_time_task` cuts a day into children, `coarsen_sealed_slices`
        // #134 fuses them back), so this ordering is normal, not corruption.
        //
        // Applies to BOTH tiers, not base-exempt: escalate (rebuild the covering
        // slice, which includes any late-arriving rows) rather than silently
        // complete — exempting the base tier guaranteed the double count this
        // check exists to prevent.
        //
        // Regression: `a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer`
        // (reproduces on a 24h live-frontier clock boundary, not under load).
        let covered_by_wider = live_adds.iter().find_map(|add| {
            let (start, end) = slice_tag_range(add)?;
            (tag_project(add) == Some(key.project_id.as_str())
                && (start, end) != (key.slice.start_micros, key.slice.end_micros)
                && start <= key.slice.start_micros
                && end >= key.slice.end_micros)
                .then_some((start, end))
        });
        if let Some((covering_start, covering_end)) = covered_by_wider {
            // ESCALATE. This slice cannot publish — two overlapping files would
            // both stay live and a dashboard would SUM them — but completing it
            // silently is the other wrong answer: `invalidate` mints derived
            // work at DERIVED_SLICE_MICROS, so late rows for ONE HOUR inside an
            // already-published day arrive as an hour-wide unit, and dropping it
            // leaves that hour STALE in the coarse tier.
            //
            // #145 shipped this branch as a counter first, because the failure
            // could not be reproduced in a test and rebuilding a whole day on
            // every hour invalidation is a real cost. The counter then moved on
            // prod (`rollup_skipped_covered_by_wider` = 5 within an hour of
            // deploying), which is the condition that PR named for making this
            // change — so the cost is now justified by evidence rather than by
            // argument.
            //
            // Reopening the covering slice rebuilds the whole day, which absorbs
            // the change; its own replace-set then removes everything it
            // contains. It terminates: the wider unit publishes at its own
            // width, so it never re-enters this branch.
            crate::observability::maintenance_stats().rollup_skipped_covered_by_wider.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut journal = self.journal();
            if let Ok(covering) = crate::maintenance_coordinator::TimeSlice::new(covering_start, covering_end) {
                journal.enqueue(
                    crate::maintenance_coordinator::TaskKey { slice: covering, ..key.clone() },
                    crate::support::now_micros(),
                    MAX_DECODED_BYTES,
                    u64::try_from(crate::support::now_micros().div_euclid(1_000)).unwrap_or_default(),
                );
            }
            journal.complete(&key);
            journal.checkpoint()?;
            info!(
                table = %key.physical_table, project_id = %key.project_id,
                slice_start = key.slice.start_micros, covering_start, covering_end,
                event = "maintenance_rollup_escalated_to_covering_slice"
            );
            return Ok(true);
        }
        let target_paths = replaced.iter().map(|add| add.path.clone()).collect::<Vec<_>>();
        let mut actions = replaced.iter().map(|add| Action::Remove(remove_for_add(add, true))).collect::<Vec<_>>();
        actions.extend(adds.iter().cloned());

        // Counted before the commit consumes `actions`, and published below only
        // if this unit actually lands. The rollup_* stats were wired ONLY to the
        // retired cohort path (`stage_rollup_wave`), so on prod they read
        // rollup_output_rows_total = 0 / rollup_staged_projects_total = 0 while
        // the coordinator was publishing real rows — the rollup table held 1,220
        // rows for a project the counters called empty. A metric that reads zero
        // while the system works is worse than no metric: it cost a whole
        // diagnosis pass chasing a rollup outage that was not happening.
        let (action_count, output_files) = (actions.len() as u64, adds.len() as u64);

        let still_running = self.journal().state(&key) == Some(TaskState::Running);
        if !still_running {
            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
            return Ok(true);
        }
        if !actions.is_empty() {
            let commit_lock = self.commit_lock(&key.project_id, &key.physical_table).await;
            let guard = commit_lock.lock().await;
            refresh_table_snapshot(&target_ref, self.config.maintenance.timefusion_incremental_snapshot).await?;
            let mut table = target_ref.read().await.clone();
            let live = table.snapshot()?.log_data().iter().map(|file| file.path().to_string()).collect::<HashSet<_>>();
            if !target_paths.iter().all(|path| live.contains(path)) {
                drop(guard);
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                retry("slice_occ_stale".to_owned(), std::time::Duration::from_secs(1))?;
                return Ok(true);
            }
            let op = DeltaOperation::Write { mode: SaveMode::Overwrite, partition_by: Some(target_schema.partitions.clone()), predicate: None };
            let finalized =
                deltalake::kernel::transaction::CommitBuilder::from(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                    .with_actions(actions)
                    .build(Some(table.snapshot()? as &dyn TableReference), table.log_store(), op)
                    .await?;
            table.state = Some(finalized.snapshot());
            drop(guard);
            self.swap_and_refresh_cache(&target_ref, table, None, &[&format!("date={date}")]).await;
            if retiring > 0 {
                crate::observability::maintenance_stats().rollup_tier_untagged_retired.fetch_add(retiring, Relaxed);
                if leaves_partition_clean && self.journal().clear_untagged_cell(&key.source, &key.physical_table, &key.project_id, &date_string) {
                    self.persist_untagged_cells();
                }
                warn!(
                    table = %key.physical_table, project_id = %key.project_id, date = %date_string, retired = retiring,
                    event = "rollup_tier_untagged_files_retired"
                );
            }
        }

        let _journal_guard = self.rollup_journal_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut journal = self.journal();
        if journal.state(&key) == Some(TaskState::Running) {
            self.rollup_slice_coverage.insert(
                (key.project_id.clone(), key.source.clone(), key.physical_table.clone(), key.slice.start_micros, key.slice.end_micros),
                RollupCoverage {
                    source_fp,
                    source_epoch: 0,
                    generation: generation.clone(),
                    rows,
                    source_rows: source_rows.and_then(|rows| u64::try_from(rows).ok()),
                    covered_through: key.slice.end_micros,
                },
            );
            // DATE-level coverage, the second routing route. It had no producer
            // at all — two reads and two removals against zero inserts — so the
            // lookup in `ProjectRoutingTable::scan` returned `None` for every
            // date on every process and ALL routing fell to slice coverage, the
            // one path the per-slice witness rule can refuse. Prod 2026-08-22
            // measured 95.2% of `stale_coverage` as witness-less slices with
            // `rollup_hits_* = 0`; this route does not consult the witness at
            // all, it compares the whole-partition fingerprint instead.
            //
            // Gated on the partition's EARLIEST ROW, not on the slice starting at
            // midnight. The read path serves `[day_start, covered_through)` from
            // this entry, so a slice beginning mid-day would claim a morning it
            // never aggregated — the silent-wrong-number failure. But units are
            // planned from row statistics, so their slices begin at the first row
            // rather than at 00:00, and a midnight test would simply never fire
            // (it did not: the regression test still failed with it).
            //
            // `partition_min_ts >= slice.start` is the honest form of the same
            // guarantee: if the partition holds no row before this slice, then
            // `[day_start, slice.start)` is empty and serving it from the rollup
            // returns exactly what a raw scan would — nothing. `min_ts` comes
            // from the same `partition_stats_bounded` call as the fingerprint, so
            // it describes the snapshot this build actually read.
            if let Some((partition_fp, partition_min_ts)) = partition_identity
                && partition_min_ts >= key.slice.start_micros
            {
                self.rollup_coverage.insert(
                    (key.project_id.clone(), key.source.clone(), key.physical_table.clone(), date.to_string()),
                    RollupCoverage {
                        source_fp: partition_fp,
                        source_epoch: self
                            .rollup_source_epochs
                            .get(&(key.project_id.clone(), key.source.clone(), date.to_string()))
                            .map_or(0, |epoch| *epoch.value()),
                        generation: generation.clone(),
                        rows,
                        source_rows: source_rows.and_then(|rows| u64::try_from(rows).ok()),
                        covered_through: key.slice.end_micros,
                    },
                );
            }
            journal.publish(
                &key,
                crate::maintenance_coordinator::Publication {
                    source_fingerprint: source_fp,
                    generation: generation.clone(),
                    rows,
                    source_rows: source_rows.and_then(|rows| u64::try_from(rows).ok()),
                },
            );
            journal.checkpoint()?;
            // Per-unit outcome, because the counters are process-wide totals and
            // cannot answer "why has THIS project's coverage not moved". Prod
            // 2026-08-17: the whale started day-sized BaseRollup units for
            // 08-08..08-13 at attempt 1 and published nothing for any of them,
            // while 120k raw rows/hour sat in those partitions — and there was
            // no way to tell a zero-row publication from a unit that never got
            // that far.
            info!(
                operation = ?key.operation,
                table = %key.physical_table,
                project_id = %key.project_id,
                slice_start = key.slice.start_micros,
                slice_end = key.slice.end_micros,
                rows,
                output_files,
                estimated_decoded_bytes = estimated_bytes,
                event = "maintenance_rollup_published"
            );
            let stats = crate::observability::maintenance_stats();
            stats.maintenance_processed_bytes.fetch_add(estimated_bytes, Relaxed);
            stats.rollup_output_rows.fetch_add(rows, Relaxed);
            stats.rollup_output_files.fetch_add(output_files, Relaxed);
            stats.rollup_commit_actions.fetch_add(action_count, Relaxed);
            // The four counters that read 0 in production despite this path
            // running constantly. Totals, so the per-phase SHARE is what to read:
            // scan/(scan+stage+commit) says whether the ~8 minutes is the source
            // scan or the write.
            let commit_ms = commit_started.elapsed().as_millis() as u64;
            let unit_ms = unit_started.elapsed().as_millis() as u64;
            stats.rollup_scan_duration_ms.fetch_add(scan_ms, Relaxed);
            stats.rollup_staging_duration_ms.fetch_add(stage_ms, Relaxed);
            stats.rollup_commit_duration_ms.fetch_add(commit_ms, Relaxed);
            stats.rollup_end_to_end_duration_ms.fetch_add(unit_ms, Relaxed);
            // Totals cannot show a SLOW unit — one 8-minute unit and a hundred
            // fast ones sum the same as a hundred mediocre ones. Log the outliers
            // individually, with the phase split, so the expensive shape can be
            // named rather than inferred.
            if unit_ms >= 60_000 {
                warn!(
                    operation = ?key.operation,
                    table = %key.physical_table,
                    project_id = %key.project_id,
                    rows,
                    output_files,
                    scan_ms,
                    stage_ms,
                    commit_ms,
                    unit_ms,
                    event = "maintenance_rollup_slow_unit",
                    "a rollup unit took over a minute; phase split attached"
                );
            }
            // One coordinator unit is one (project, slice) publication, which is
            // what "staged project" counts on the cohort path too.
            stats.rollup_staged_projects.fetch_add(1, Relaxed);
            if derived {
                stats.rollup_rebuilds_incremental.fetch_add(1, Relaxed);
            } else {
                stats.rollup_rebuilds_full.fetch_add(1, Relaxed);
            }
        }
        Ok(true)
    }

    async fn coordinator_compaction_files(&self, table_ref: &Arc<RwLock<DeltaTable>>, key: &crate::maintenance_coordinator::TaskKey) -> Result<Vec<String>> {
        use crate::maintenance_coordinator::Operation;
        let date = chrono::DateTime::from_timestamp_micros(key.slice.start_micros)
            .map(|time| time.date_naive().to_string())
            .ok_or_else(|| anyhow::anyhow!("invalid compaction slice timestamp"))?;
        let date_marker = format!("date={date}/");
        // Per-stage survivor counts. `run_coordinator_compaction_once` marks a
        // unit COMPLETE when this returns empty and logs nothing, so a partition
        // that is out of policy but selects no files is retired silently and
        // re-minted 60s later — a treadmill that is invisible from every counter.
        //
        // Prod 2026-08-23: `SealedConsolidation` claimed units for
        // `6297304f/2026-08-17` (275 files), `87576849/2026-08-19` (238) and
        // `28f62f01/2026-08-18` (230) every 30-60s for ~45 minutes and not one
        // file was retired. Two hypotheses were tried against that and one was
        // shipped and refuted; the reason guessing was all that was available is
        // that this function reports only its final length.
        let (mut seen, mut after_date, mut after_project) = (0usize, 0usize, 0usize);
        let mut candidates = {
            let table = table_ref.read().await;
            table
                .snapshot()?
                .log_data()
                .iter()
                .filter_map(|file| {
                    let path = file.path();
                    seen += 1;
                    if !path.contains(&date_marker) {
                        return None;
                    }
                    after_date += 1;
                    let path_project = path_partition_value(&path, "project_id");
                    if path_project.is_some_and(|project| project != key.project_id) {
                        return None;
                    }
                    after_project += 1;
                    let add = TailAdd::from_stats(path.to_string(), file.size(), is_sorted_run(&file.tags()), file.stats().as_deref());
                    if add.event_range.is_some_and(|(start, end)| start >= key.slice.end_micros || end < key.slice.start_micros) {
                        return None;
                    }
                    Some(add)
                })
                .collect::<Vec<_>>()
        };
        let after_range = candidates.len();
        candidates.sort_by_key(|add| add.event_range.map_or(i64::MIN, |range| range.0));
        if key.operation == Operation::Repair {
            return Ok(candidates.into_iter().filter(|add| !self.repair_verified_sorted.contains(&add.path)).take(1).map(|add| add.path).collect());
        }
        let target = match key.operation {
            Operation::HotPacking => COORDINATOR_HOT_TARGET_BYTES,
            Operation::SealedConsolidation => COORDINATOR_SEALED_TARGET_BYTES,
            _ => return Ok(Vec::new()),
        };
        let unsorted_candidates = candidates.iter().filter(|add| !add.is_sorted_run).count();
        let under_target_candidates = candidates.iter().filter(|add| add.size < target).count();
        let selected = select_coordinator_compaction_candidates(candidates, target);
        // Only when the unit will do NOTHING, so this cannot become chatter: a
        // selection of one file is a 1:1 rewrite and retires no files either.
        if selected.len() < 2 {
            let unsorted = unsorted_candidates;
            info!(
                operation = ?key.operation,
                project_id = %key.project_id,
                table = %key.physical_table,
                date = %date,
                snapshot_files = seen,
                after_date_filter = after_date,
                after_project_filter = after_project,
                after_range_filter = after_range,
                unsorted_candidates = unsorted,
                under_target = under_target_candidates,
                selected = selected.len(),
                target,
                event = "compaction_unit_selected_nothing",
                "a compaction unit selected fewer than two files and will retire none"
            );
        }
        Ok(selected)
    }

    async fn run_coordinator_compaction_once(&self, operation: crate::maintenance_coordinator::Operation) -> Result<bool> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Resources, TaskLease, TaskState};
        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(operation) else { return Ok(false) };
        let key = task.key.clone();
        self.log_task_started(&task);
        let _lease = TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        let retry = |reason: String, seconds: u64| -> Result<()> { self.retry_task(&key, reason, std::time::Duration::from_secs(seconds)) };
        let Some(_permit) = self.maintenance_admission.try_acquire(Resources { cpu: 1, decoded_bytes: MAX_DECODED_BYTES, object_reads: 1, object_writes: 1 })
        else {
            retry("resource_admission".to_owned(), 1)?;
            return Ok(true);
        };
        let table_ref = match self.resolve_table(&key.project_id, &key.source).await {
            Ok(table) => table,
            Err(error) => {
                retry(format!("resolve_compaction_source: {error:#}"), 30)?;
                return Ok(true);
            }
        };
        let files = self.coordinator_compaction_files(&table_ref, &key).await?;
        if files.is_empty() {
            let mut journal = self.journal();
            journal.complete(&key);
            journal.checkpoint()?;
            return Ok(true);
        }
        let selected = files.iter().map(String::as_str).collect::<HashSet<_>>();
        let processed_bytes = {
            let table = table_ref.read().await;
            table
                .snapshot()?
                .log_data()
                .iter()
                .filter(|file| selected.contains(file.path().as_ref()))
                .fold(0u64, |bytes, file| bytes.saturating_add(estimated_decoded_bytes(file.size())))
        };
        if operation == crate::maintenance_coordinator::Operation::Repair && self.repair_bin_already_sorted(&table_ref, &files).await {
            let remaining = !self.coordinator_compaction_files(&table_ref, &key).await?.is_empty();
            let mut journal = self.journal();
            if remaining {
                journal.retry(&key, "compaction_debt_remaining".to_owned(), crate::support::now_micros());
            } else {
                journal.complete(&key);
            }
            journal.checkpoint()?;
            return Ok(true);
        }
        let Some(schema) = get_schema(&key.source) else {
            retry("compaction_schema_missing".to_owned(), 300)?;
            return Ok(true);
        };
        let pass = if operation == crate::maintenance_coordinator::Operation::Repair { TailPass::Repair } else { TailPass::Pack };
        let runtime = self.coordinator_runtime_env();
        let outcome = self.stage_hot_bin(&table_ref, &key.source, schema, &key.project_id, files, HotStageOptions { pass, runtime_env: Some(runtime) }).await;
        let completed = match outcome {
            Ok(BinOutcome::Staged(unit)) => {
                let date = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive().to_string()).unwrap_or_default();
                let result = self.commit_wave(&table_ref, &key.source, &[format!("date={date}/")], false, vec![unit], 0).await;
                result.failed.is_empty() && !result.landed.is_empty()
            }
            Ok(BinOutcome::Converged) => true,
            Ok(BinOutcome::Retry) => false,
            Err(error) => {
                let mut journal = self.journal();
                journal.retry_or_split(&key, format!("compaction: {error:#}"), crate::support::now_micros().saturating_add(30 * 1_000_000), task.attempts);
                journal.checkpoint()?;
                return Ok(true);
            }
        };
        let remaining = if completed { !self.coordinator_compaction_files(&table_ref, &key).await?.is_empty() } else { false };
        if completed {
            crate::observability::maintenance_stats().maintenance_processed_bytes.fetch_add(processed_bytes, std::sync::atomic::Ordering::Relaxed);
        }
        let mut journal = self.journal();
        if journal.state(&key) == Some(TaskState::Running) {
            if completed {
                if remaining {
                    journal.retry(&key, "compaction_debt_remaining".to_owned(), crate::support::now_micros());
                } else {
                    journal.complete(&key);
                }
            } else {
                journal.retry(&key, "compaction_incomplete".to_owned(), crate::support::now_micros().saturating_add(30_000_000));
            }
            journal.checkpoint()?;
        }
        Ok(true)
    }

    pub(crate) async fn run_maintenance_coordinator_once(&self) -> Result<bool> {
        use crate::maintenance_coordinator::Operation;
        let now = crate::support::now_micros();
        let last = self.maintenance_debt_planned_at.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(last) >= 60_000_000
            && self.maintenance_debt_planned_at.compare_exchange(last, now, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Relaxed).is_ok()
        {
            let planned = self.plan_compaction_debt().await?;
            if planned != 0 {
                info!(planned, event = "maintenance_compaction_debt_planned");
            }
            // Same 60s cadence, same reason: this is historical debt nothing
            // else will ever queue. Without it the rollup horizon never grows
            // past the live frontier and long-window queries stay unroutable.
            let backfilled = self.plan_rollup_backfill().await?;
            if backfilled != 0 {
                info!(backfilled, event = "maintenance_rollup_backfill_planned");
            }
            // Same cadence, and the reason is the same shape: work nothing else
            // will ever retire. A sealed day's ten-minute units are the live
            // path's granularity outliving its purpose — ~144 where one would
            // do — and every midnight mints another day of them. Collapsing
            // them is what keeps the queue from growing at the rate projects
            // are added.
            // What each partition can actually decode to, so the fit test stops
            // trusting whole-file estimates frozen at enqueue time. Built once
            // per pass from the same `partition_stats_bounded` the read path
            // uses; a table that cannot be resolved simply contributes nothing
            // and those groups keep the old summed behaviour.
            let ceilings: HashMap<(String, String), u64> = {
                let mut ceilings = HashMap::new();
                for source in crate::schema::registry().list_tables() {
                    let Ok(table_ref) = self.resolve_table("default", &source).await else { continue };
                    let table = table_ref.read().await;
                    if let Ok(stats) = Self::partition_stats_bounded(&table, tiebreak_of(&source), &|_, _| i64::MAX) {
                        for ((project, date), stat) in stats {
                            ceilings.insert((project, date), stat.bytes);
                        }
                    }
                }
                ceilings
            };
            let report = {
                let mut journal = self.journal();
                let report = journal.coarsen_sealed_slices_capped(crate::support::now_micros(), &|project, _source, date| {
                    ceilings.get(&(project.to_string(), date.to_string())).or_else(|| ceilings.get(&("default".to_string(), date.to_string()))).copied()
                });
                if report.total() != 0 {
                    // `checkpoint` again, not `compact`. It briefly had to be
                    // `compact` because the WAL could not express a deletion, so
                    // a pass that removed tasks persisted nothing — prod took
                    // `pending_base_rollup` 88,618 -> 2,294 with the on-disk
                    // journal byte-identical, and the next restart undid it.
                    // `JournalRecord::Removed` fixes that at the format level,
                    // so the cheap append is correct here and a full 84 MB
                    // rewrite every 60 s is not.
                    journal.checkpoint()?;
                }
                report
            };
            // Logged even when nothing collapsed, and that is the point: a pass
            // that removes nothing is the interesting case, and the old line
            // fired only on success so the stall was invisible. `candidates`
            // against `blocked` + `over_budget` says which of the three reasons
            // a small pass has, and they want different fixes.
            info!(
                subsumed = report.subsumed,
                fused = report.fused,
                candidates = report.candidates,
                blocked = report.blocked,
                over_budget = report.over_budget,
                priced_by_footprint = report.priced_by_footprint,
                event = "maintenance_sealed_slices_coarsened"
            );
        }
        // Tantivy coverage census: metadata-only, so it is throttled by wall
        // clock rather than admission. Every 15 minutes keeps
        // `tantivy_uncovered_files` meaningful between daily reconcile passes —
        // without it the reindex reports nothing for up to 24h after a deploy.
        const TANTIVY_CENSUS_INTERVAL_MICROS: i64 = 15 * 60 * 1_000_000;
        let census_last = self.tantivy_census_at.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(census_last) >= TANTIVY_CENSUS_INTERVAL_MICROS
            && self.tantivy_census_at.compare_exchange(census_last, now, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Relaxed).is_ok()
        {
            match self.tantivy_coverage_census().await {
                Ok((uncovered, oversized, by_age)) => {
                    info!(uncovered, oversized, today = by_age[0], week = by_age[1], older = by_age[2], event = "tantivy_coverage_census")
                }
                Err(error) => warn!(%error, event = "tantivy_coverage_census_failed"),
            }
        }
        // Interleave dependent publication with dedup instead of draining the
        // entire historical dedup backlog first. The cycles live in
        // `maintenance_coordinator` (one definition shared with the journal
        // simulator); the coverage-short reweighting is self-limiting via
        // `coverage_is_short`. `claim_next` still applies deadline,
        // recent-slice, dependency, and project fairness.
        let cycle = crate::maintenance_coordinator::operation_cycle(coverage_is_short());
        let start = self.maintenance_schedule_cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % cycle.len();
        let mut attempted = [false; 6];
        for offset in 0..cycle.len() {
            let operation = cycle[(start + offset) % cycle.len()];
            let index = operation as usize;
            if attempted[index] {
                continue;
            }
            attempted[index] = true;
            // Debt work cannot advance coverage — `dependencies_complete` makes
            // BaseRollup depend on nothing — but it holds a worker for 12-15
            // minutes while a rollup unit holds one for seconds. Cap how many
            // workers may be inside it at once so the rollup chain always has
            // somewhere to run. Failing to acquire falls through to the next
            // operation in the cycle, which is work-conserving: the worker picks
            // up rollup instead of idling.
            //
            // Only while coverage is short, on the same self-limiting signal as
            // the cycle weighting, so a healthy system goes back to using every
            // worker for whatever is queued.
            //
            // `DerivedRollup` is exempt from BOTH caps, and holds a reservation
            // of its own below, because #176's argument applies to it one level
            // further down. That change freed workers from debt for "the rollup
            // chain" — but BaseRollup's sealed day units are themselves 800s
            // (measured 2026-08-19), so they take the freed workers and derived
            // starves exactly as it did behind debt. Derived is the cheap half:
            // it aggregates the base TIER and reads no raw data at all.
            let _debt_slot = match operation {
                Operation::BaseRollup | Operation::DerivedRollup => None,
                _ if !coverage_is_short() => None,
                _ => match Arc::clone(&self.maintenance_debt_slots).try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) => continue,
                },
            };
            // Keep a couple of workers free for derived work while coverage is
            // short. Everything else must leave `maintenance_derived_reserve`
            // permits unclaimed; derived itself never takes one.
            //
            // Measured on prod 2026-08-19 00:40 UTC, after #189 made historical
            // derived units claimable at all: 8 derived claims in 25 minutes, of
            // which 2 were the sealed day units that build the tier. At that rate
            // the ~700-unit backlog is 145 hours. The cause is wall clock, not
            // attempts — the cycle already gives derived 2 of 10 slots, but a
            // worker that picks HotPacking is gone for 578s and one that picks a
            // sealed BaseRollup for 801s, so attempt share and slot-time share
            // differ by two orders of magnitude. That is #176's finding exactly.
            let _derived_reserve = match operation {
                Operation::DerivedRollup => None,
                _ if !coverage_is_short() => None,
                _ => match Arc::clone(&self.maintenance_derived_reserve).try_acquire_owned() {
                    Ok(permit) => Some(permit),
                    Err(_) => continue,
                },
            };
            let timeout = coordinator_operation_timeout(operation);
            let work = async {
                match operation {
                    Operation::Dedup => self.run_coordinator_dedup_once().await,
                    Operation::BaseRollup | Operation::DerivedRollup => self.run_coordinator_rollup_once(operation).await,
                    Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair => self.run_coordinator_compaction_once(operation).await,
                }
            };
            // A unit's DURATION is the number every deadline decision needs and
            // none of them has. Raising a deadline only helps if the units that
            // miss it would finish in the longer window; if they would not, the
            // waste per timeout rises with the deadline instead of falling.
            // Prod 2026-08-18 has that question open for three operations at once
            // — dedup (15 timeouts per 30 min at 300s, ~16% of capacity), sealed
            // consolidation (3, ~9%) and repair (2, ~6%) — and it cannot be
            // answered from the timeout count alone, because a timeout says only
            // "longer than the deadline", never how much longer.
            let started = std::time::Instant::now();
            let completed = match tokio::time::timeout(timeout, work).await {
                Ok(result) => {
                    let elapsed = started.elapsed();
                    // Only the slow tail: a unit finishing well inside its
                    // deadline says nothing, and this runs on every claim.
                    if elapsed.as_secs_f64() > timeout.as_secs_f64() / 4.0 {
                        info!(
                            ?operation,
                            elapsed_secs = elapsed.as_secs(),
                            deadline_secs = timeout.as_secs(),
                            headroom_pct = (100.0 * (1.0 - elapsed.as_secs_f64() / timeout.as_secs_f64())) as i64,
                            event = "maintenance_unit_slow",
                            "a maintenance unit used a large share of its deadline"
                        );
                    }
                    result?
                }
                Err(_) => {
                    // Dropping the operation future drops its TaskLease. The
                    // claimed unit is durably requeued and all resource tokens
                    // are released before another project gets a turn.
                    warn!(?operation, timeout_seconds = timeout.as_secs(), event = "maintenance_coordinator_unit_timed_out");
                    return Ok(true);
                }
            };
            if completed {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Drive a bounded number of already-eligible durable maintenance units.
    ///
    /// The background coordinator calls the same single-unit routine. This
    /// bounded entry point is useful for deterministic verification and manual
    /// control-plane drains without starting any legacy cron loops.
    pub async fn run_maintenance_units(&self, max_units: usize) -> Result<usize> {
        let mut completed = 0usize;
        while completed < max_units && self.run_maintenance_coordinator_once().await? {
            completed += 1;
        }
        Ok(completed)
    }

    pub(crate) fn invalidate_rollup_hours(&self, project_id: &str, source: &str, date: &str, hours: u32) -> std::io::Result<()> {
        let _journal_guard = self.rollup_journal_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let source_key = (project_id.to_string(), source.to_string(), date.to_string());
        // The mutation tells us exactly which hours became dirty. Expanding a
        // project's first observed hour to the full day creates 456 durable
        // units for the dashboard rollups (144 dedup + 144 base + 144 packing
        // + 24 derived), including future and empty slices. At 1,000 projects
        // that is 456,000 urgent tasks from a single ingest wave. Uncovered
        // intervals already use the exact raw fallback, so enqueue only work
        // whose source rows can actually have changed. Historical discovery
        // and source-wide DML continue to pass ALL_HOURS explicitly.
        let affected_hours = hours;
        self.rollup_invalidated_at.entry(source_key.clone()).or_insert_with(crate::storage::now_unix_ms);
        // This path is called with a precise timestamp-derived mask. Unknown
        // or source-wide changes use `invalidate_rollup_source` and pass
        // `ALL_HOURS` instead.
        self.rollup_dirty.entry(source_key.clone()).and_modify(|dirty| *dirty |= affected_hours).or_insert(affected_hours);
        self.rollup_source_epochs.entry(source_key).and_modify(|epoch| *epoch = epoch.saturating_add(1)).or_insert(1);
        if let Some(schema) = get_schema(source) {
            for spec in &schema.rollups {
                let key = (project_id.to_string(), source.to_string(), spec.table_name(source), date.to_string());
                self.rollup_coverage.remove(&key);
                self.rollup_backoff.remove(&key);
            }
        }
        if let Some(day_start) = date_start_micros(date) {
            let ranges = crate::rollup::dirty_ranges(day_start, affected_hours);
            self.rollup_slice_coverage.retain(|(project, table, _, start, end), _| {
                project != project_id || table != source || !ranges.iter().any(|(dirty_start, dirty_end)| *start < *dirty_end && *end > *dirty_start)
            });
        }
        // Checkpoint slice work before the legacy journal and before the write
        // is acknowledged. A crash at any later point can only leave redundant
        // tasks; it cannot leave a mutation with no maintenance record.
        self.enqueue_maintenance_hours(project_id, source, date, affected_hours)?;
        self.persist_rollup_journal()?;
        Ok(())
    }

    /// Invalidate only the partitions a non-MOR UPDATE/DELETE statement can have changed.
    ///
    /// For merge-on-read tables the caller skips this — re-appended rows invalidate their own
    /// dates. This path handles statements that rewrite Delta files in place, where the
    /// predicate is the only guide. A source-wide wipe is correct but expensive, so we narrow
    /// by timestamp window whenever the predicate confines the statement to a date range and
    /// it does not assign `timestamp`. Narrowing is safe because coverage is re-proved against
    /// the partition's data fingerprint when the plan is built and again on the ticket before
    /// use.
    pub(crate) fn invalidate_rollup_dml(
        &self, project_id: &str, source: &str, predicate: Option<&datafusion::logical_expr::Expr>, assignments: &[(String, datafusion::logical_expr::Expr)],
    ) -> std::io::Result<()> {
        let moves_rows = assignments.iter().any(|(column, _)| column == "timestamp");
        let masks = (!moves_rows).then(|| predicate.and_then(crate::rollup::timestamp_window)).flatten().and_then(|(lo, hi)| window_hour_masks(lo, hi));
        match masks {
            Some(masks) => {
                for (date, hours) in masks {
                    self.invalidate_rollup_hours(project_id, source, &date, hours)?;
                }
                Ok(())
            }
            None => self.invalidate_rollup_source(project_id, source),
        }
    }

    pub(crate) fn invalidate_rollup_source(&self, project_id: &str, source: &str) -> std::io::Result<()> {
        if get_schema(source).is_none_or(|schema| schema.rollups.is_empty()) {
            return Ok(());
        }
        let _journal_guard = self.rollup_journal_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys: Vec<_> =
            self.rollup_source_epochs.iter().filter(|entry| entry.key().0 == project_id && entry.key().1 == source).map(|entry| entry.key().clone()).collect();
        for key in &keys {
            self.rollup_source_epochs.entry(key.clone()).and_modify(|epoch| *epoch = epoch.saturating_add(1));
            self.rollup_dirty.insert(key.clone(), crate::rollup::ALL_HOURS);
            self.rollup_invalidated_at.entry(key.clone()).or_insert_with(crate::storage::now_unix_ms);
            self.enqueue_maintenance_hours(&key.0, &key.1, &key.2, crate::rollup::ALL_HOURS)?;
        }
        self.rollup_coverage.retain(|(project, table, _, _), _| project != project_id || table != source);
        self.rollup_slice_coverage.retain(|(project, table, ..), _| project != project_id || table != source);
        self.rollup_backoff.retain(|(project, table, _, _), _| project != project_id || table != source);
        self.persist_rollup_journal()?;
        Ok(())
    }

    /// Walks the batches' dates, so it is gated on the master switch as well as
    /// the schema: with rollups off this runs on every inbound write for nothing.
    pub(crate) fn invalidate_rollup_batches(&self, project_id: &str, source: &str, batches: &[RecordBatch]) -> std::io::Result<()> {
        if !self.config.maintenance.timefusion_rollup_enabled || get_schema(source).is_none_or(|schema| schema.rollups.is_empty()) {
            return Ok(());
        }
        let mut dates: HashMap<String, u32> = HashMap::new();
        for batch in batches {
            let Some(batch_dates) = batch_hours(batch) else {
                // A batch that cannot say which partitions it touches leaves no
                // choice but the source-wide wipe — but that wipe is what kept
                // nine days of rollups from ever surviving a read (prod
                // 2026-08-11), so it must never happen quietly again. It also
                // goes through `invalidate_rollup_source`, which bumps the
                // source epochs and clears the backoff; the bare `retain` this
                // replaces did neither, so a wipe could leave a date that no
                // longer had coverage but was still backing off from a retry.
                warn!(
                    project_id,
                    source,
                    event = "rollup_invalidate_unscoped",
                    "write batch carries no readable date; invalidating every partition's coverage"
                );
                return self.invalidate_rollup_source(project_id, source);
            };
            for (date, hours) in batch_dates {
                *dates.entry(date).or_default() |= hours;
            }
        }
        // The hours come from the rows themselves, so an enrichment touching one
        // hour marks one hour — and the repair rebuilds one hour instead of 24.
        for (date, hours) in dates {
            self.invalidate_rollup_hours(project_id, source, &date, hours)?;
        }
        Ok(())
    }

    /// Exclusive upper bound on row timestamps a file may contain to be folded into a slice.
    ///
    /// This lets a day still being written hold stable coverage. Its whole-day fingerprint moves on
    /// every flush, so coverage keyed on it would invalidate within minutes. Bounded to the part the
    /// build actually read, new files landing above the bound change nothing, while a rewrite or late
    /// file below it moves the fingerprint and correctly invalidates — the only direction that can
    /// serve a wrong number.
    fn partition_fingerprints_bounded(
        table: &DeltaTable, tiebreak: Option<&str>, bound_for: &dyn Fn(&str, &str) -> i64,
    ) -> Result<HashMap<(String, String), u64>> {
        Ok(Self::partition_stats_bounded(table, tiebreak, bound_for)?.into_iter().map(|(key, stats)| (key, stats.fingerprint)).collect())
    }

    /// As [`Self::partition_fingerprints_bounded`], but also carrying the row
    /// timestamps each partition's files span — one pass, since the fingerprint
    /// already folds those in and the add-actions scan is the dominant cost of
    /// planning a rollup route.
    /// Split a window's live Delta files into the ones that may skip
    /// `DedupExec` and the ones that may not.
    ///
    /// The two sets partition the window's in-window files exactly once, which
    /// is what lets the caller run them as two legs and union the certified one
    /// ABOVE the dedup — the same shape the per-DATE skip uses, one level finer.
    /// Both are RELATIVE paths (`parquet_rel_of_uri`).
    ///
    /// Returns `(empty, empty)` when the feature is off or the window cannot be
    /// enumerated, which the caller must read as "no split" rather than "nothing
    /// certified" — an empty certified set with a populated uncertified set
    /// would silently drop every file from the scan.
    pub(crate) fn certified_file_split(
        &self, table: &DeltaTable, project_id: &str, table_name: &str, (lo, hi): (i64, i64),
    ) -> (HashSet<String>, HashSet<String>) {
        let empty = (HashSet::new(), HashSet::new());
        if !self.config.maintenance.timefusion_read_dedup_skip_per_file {
            return empty;
        }
        let Some(dates) = window_dates(lo, hi) else { return empty };
        let (mut certified, mut uncertified) = (HashSet::new(), HashSet::new());
        for date in dates {
            let date = date.to_string();
            let Ok(spans) = Self::partition_file_spans(table, &format!("date={date}")) else { return empty };
            let skippable = self.certified_files_in_partition(table, project_id, table_name, &date);
            for rel in spans.into_keys() {
                match skippable.contains(&rel) {
                    true => certified.insert(rel),
                    false => uncertified.insert(rel),
                };
            }
        }
        // Nothing proved means no split is worth its second scan, and returning
        // a populated `uncertified` alone would be read as a restriction.
        if certified.is_empty() { empty } else { (certified, uncertified) }
    }

    /// The live files of one date partition that a certification still vouches
    /// for AND that no uncertified file could hold another version of.
    ///
    /// This is the additive half of certification. `dedup_window_certified`
    /// answers the all-or-nothing question — is the partition byte-for-byte the
    /// one that was proved — and any single new file makes it `false`. Recent
    /// partitions gain files continuously (ingest, hot-tail compaction, the
    /// sealed backlog), so in prod that question is `false` essentially always:
    /// 2026-08-22 measured `dedup_denied_never_certified` at 100% of eligible
    /// scans. This asks the weaker question instead — WHICH proved files are
    /// still live and still isolated — and a new file now costs only the files
    /// it overlaps.
    ///
    /// Returns the RELATIVE paths (`parquet_rel_of_uri`) that may skip, or an
    /// empty set when nothing qualifies. Soundness lives in
    /// `read::skippable_certified_files`; this only assembles its two inputs.
    pub(crate) fn certified_files_in_partition(&self, table: &DeltaTable, project_id: &str, table_name: &str, date: &str) -> HashSet<String> {
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        let Some(cert) = self.dedup_clean_fp.get(&key).map(|entry| entry.value().clone()) else { return HashSet::new() };
        let Ok(spans) = Self::partition_file_spans(table, &format!("date={date}")) else { return HashSet::new() };
        // A path the certification names but that no longer appears in `spans`
        // was compacted away; it cannot vouch for its replacement, so it simply
        // drops out of the certified side rather than being carried forward.
        let proved: HashSet<&str> = cert.files.iter().filter_map(|uri| crate::tantivy::search::parquet_rel_of_uri(uri)).collect();
        let (certified, uncertified): (Vec<_>, Vec<_>) = spans.iter().partition(|(rel, _)| proved.contains(rel.as_str()));
        crate::read::skippable_certified_files(
            certified.iter().map(|(rel, span)| (rel.as_str(), **span)),
            &uncertified.iter().map(|(_, span)| **span).collect::<Vec<_>>(),
        )
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    /// Per-FILE row-timestamp spans for one date partition, keyed by the
    /// `project_id=…/…parquet` RELATIVE path.
    ///
    /// Relative because the two sides of the join spell files differently:
    /// `partition_files_by_pid` (and therefore a certification's stored list)
    /// yields full object-store URIs, while the add-actions table yields
    /// relative paths. `parquet_rel_of_uri` normalises either to the same key,
    /// so callers must apply it to the certification side too.
    ///
    /// `partition_stats_bounded` folds these into one span per PARTITION, which
    /// is the wrong granularity for the per-file dedup skip: that rule asks
    /// whether one certified file overlaps one uncertified file. A file whose
    /// statistics are missing maps to `None`, which the rule treats as
    /// overlapping everything — never as empty.
    pub(crate) fn partition_file_spans(table: &DeltaTable, date_marker: &str) -> Result<HashMap<String, crate::read::FileSpan>> {
        let snapshot = table.snapshot()?.snapshot();
        let actions = snapshot.add_actions_table(true)?;
        let Some(paths) = actions.column_by_name("path").cloned() else { return Ok(HashMap::new()) };
        let min_ts = crate::read::ts_micros_column(&actions, "min.timestamp");
        let max_ts = crate::read::ts_micros_column(&actions, "max.timestamp");
        let at = |column: &Option<arrow::array::Int64Array>, row: usize| column.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row)));
        Ok((0..actions.num_rows())
            .filter_map(|row| {
                let path = crate::support::test_helpers::array_get_str(paths.as_ref(), row);
                let rel = crate::tantivy::search::parquet_rel_of_uri(&path)?.to_string();
                rel.contains(date_marker).then(|| (rel, at(&min_ts, row).zip(at(&max_ts, row))))
            })
            .collect())
    }

    pub(crate) fn partition_stats_bounded(
        table: &DeltaTable, tiebreak: Option<&str>, bound_for: &dyn Fn(&str, &str) -> i64,
    ) -> Result<HashMap<(String, String), PartitionStats>> {
        use std::hash::{Hash, Hasher};
        let snapshot = table.snapshot()?.snapshot();
        let actions = snapshot.add_actions_table(true)?;
        let column = |name: &str| actions.column_by_name(name).cloned();
        // The flattened add-actions batch exposes partition values as their own
        // columns, so there is no path to parse. A table without `numRecords`
        // stats cannot be fingerprinted this way at all; returning empty leaves
        // every partition uncovered, which costs a raw scan rather than risking
        // a wrong one.
        let (Some(records), Some(dates)) = (column("num_records"), column("partition.date")) else { return Ok(HashMap::new()) };
        let Some(records) = records.as_any().downcast_ref::<arrow::array::Int64Array>() else { return Ok(HashMap::new()) };
        let projects = column("partition.project_id");
        // Partition values arrive typed: `date` is a Date32, `project_id` a
        // string. Rendering the date the same way the partition path spells it
        // is what lets these keys match the `YYYY-MM-DD` used everywhere else.
        let string_at = |array: &Option<arrow::array::ArrayRef>, row: usize| -> Option<String> {
            let array = array.as_ref()?;
            if !array.is_valid(row) {
                return None;
            }
            match array.data_type() {
                arrow::datatypes::DataType::Date32 => {
                    let days = arrow::array::AsArray::as_primitive_opt::<arrow::datatypes::Date32Type>(array.as_ref())?.value(row);
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::days(days as i64)).map(|date| date.to_string())
                }
                _ => Some(crate::support::test_helpers::array_get_str(array.as_ref(), row)),
            }
        };
        let dates = Some(dates);
        // (rows, min_ts, max_ts, max_stamp) per partition.
        let min_ts = crate::read::ts_micros_column(&actions, "min.timestamp");
        let max_ts = crate::read::ts_micros_column(&actions, "max.timestamp");
        // The stamp is a timestamp on every current schema, but read an integer
        // one too rather than silently dropping the tightening if that changes.
        let stamp = tiebreak.map(|tiebreak| format!("max.{tiebreak}")).and_then(|name| {
            crate::read::ts_micros_column(&actions, &name)
                .or_else(|| actions.column_by_name(&name)?.as_any().downcast_ref::<arrow::array::Int64Array>().cloned())
        });
        // File size, for the partition-byte ceiling. Delta spells it `size_bytes`
        // in the flattened add-actions batch; fall back to `size` rather than
        // silently reporting a zero ceiling that would cap every estimate to 0.
        let sizes = actions
            .column_by_name("size_bytes")
            .or_else(|| actions.column_by_name("size"))
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>().cloned());
        // (rows, min_ts, max_ts, max_stamp, bytes) per partition.
        type Identity = (i64, i64, i64, i64, i64);
        let by_partition: HashMap<(String, String), Identity> = (0..actions.num_rows()).fold(HashMap::new(), |mut acc, row| {
            let Some(date) = string_at(&dates, row) else { return acc };
            // Custom-project tables carry no `project_id` partition; the sweep
            // groups those under "default", so match it exactly.
            let project = string_at(&projects, row).unwrap_or_else(|| "default".to_string());
            // A file whose rows all sit at or above this partition's bound is not
            // part of what was aggregated, so it must not perturb the fingerprint.
            if max_ts.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row))).is_some_and(|hi| hi >= bound_for(&project, &date)) {
                return acc;
            }
            let key = (project, date);
            let entry = acc.entry(key).or_insert((0, i64::MAX, i64::MIN, i64::MIN, 0));
            entry.0 += if records.is_valid(row) { records.value(row) } else { 0 };
            entry.4 += sizes.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row))).unwrap_or(0);
            if let Some(lo) = min_ts.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row))) {
                entry.1 = entry.1.min(lo);
            }
            if let Some(hi) = max_ts.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row))) {
                entry.2 = entry.2.max(hi);
            }
            if let Some(written) = stamp.as_ref().and_then(|c| c.is_valid(row).then(|| c.value(row))) {
                entry.3 = entry.3.max(written);
            }
            acc
        });
        Ok(by_partition
            .into_iter()
            .map(|(key, identity)| {
                let mut hasher = fnv::FnvHasher::default();
                // (rows, min_ts, max_ts, stamp) ONLY. `identity.4` is the
                // partition's byte size, added for the coarsening ceiling, and it
                // must NOT enter the fingerprint: every stored `source_fp` and
                // every slice witness was recorded against the 4-tuple, so
                // hashing a 5th field would invalidate all existing coverage at
                // once and send every query back to the raw path.
                (identity.0, identity.1, identity.2, identity.3).hash(&mut hasher);
                (
                    key,
                    PartitionStats {
                        fingerprint: hasher.finish(),
                        min_ts: identity.1,
                        max_ts: identity.2,
                        rows: identity.0,
                        bytes: u64::try_from(identity.4).unwrap_or(0),
                    },
                )
            })
            .collect())
    }

    /// Re-adopt rollup coverage that previous processes durably wrote.
    ///
    /// `rollup_coverage` is in-memory, so restarts used to lose it; because reads filter on
    /// `rollup_generation`, already-written rollup rows became unreadable. There is no sidecar
    /// file: the rollup table is the record. Each stored `(date, generation)` is re-proved against
    /// the current source partition's generation, so a moved source fails to match and the
    /// partition is left uncovered. Unproved claims cost a raw scan; trusting one would make a
    /// rewrite read zero rows.
    /// Queue a day-wide rebuild for every partition still holding a tier file
    /// with no identity tags.
    ///
    /// `slice_retires` can retire such a file, but only when something PUBLISHES
    /// that partition — and the coordinator publishes for the live frontier and
    /// for coverage gaps, neither of which describes a sealed day that already
    /// has coverage. Without this, the historical tail of the 2026-08-20 damage
    /// (352 files, 26 days) would never be republished and would sit there
    /// indefinitely; manual repair cannot reach it either, because a day over
    /// `MAX_DECODED_BYTES` splits before executing at every width down to an
    /// hour, so `run-unit` has no width that publishes for the largest tenants.
    ///
    /// Queued day-wide on purpose: the coordinator splits it as needed, and each
    /// child that publishes widens the union `slice_retires` uses as proof, so a
    /// tenant whose day cannot be published whole still converges.
    ///
    /// Self-limiting — the set is read from the log each recovery, so it shrinks
    /// as partitions are repaired and reaches zero. `rollup_tier_untagged_found`
    /// is the gauge that says whether it is converging.
    /// Write the damage set through to its sidecar. Best-effort by design: a
    /// lost or stale file costs one mis-ranked claim, never correctness, because
    /// recovery replaces each tier's slice and a clean publish clears its cell.
    fn persist_untagged_cells(&self) {
        let cells = self
            .journal()
            .untagged_cells()
            .map(|(source, project_id, table_name, date)| crate::storage::StoredUntaggedCell {
                source: source.clone(),
                project_id: project_id.clone(),
                table_name: table_name.clone(),
                date: date.clone(),
            })
            .collect::<Vec<_>>();
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::UNTAGGED_CELLS, &cells);
    }

    fn enqueue_untagged_rebuilds(&self, source: &str, spec: &crate::schema::RollupSpec, target: &str, partitions: &UntaggedPartitions) {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskKey, TimeSlice};
        // Published before the empty check, so a tier that has just converged
        // CLEARS its cells instead of ranking a clean partition forever.
        self.journal().set_untagged_cells(source, target, partitions.keys().cloned());
        self.persist_untagged_cells();
        if partitions.is_empty() {
            return;
        }
        let operation = if spec.derive_from.is_some() { Operation::DerivedRollup } else { Operation::BaseRollup };
        let now = crate::support::now_micros();
        let created = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
        let mut journal = self.journal();
        let mut queued = 0usize;
        for ((project_id, date), (untagged, tagged)) in partitions {
            let Ok(day) = date.parse::<chrono::NaiveDate>() else { continue };
            let Some(day_start) = day.and_hms_opt(0, 0, 0).map(|time| time.and_utc().timestamp_micros()) else { continue };
            let day_end = day_start.saturating_add(crate::maintenance_coordinator::DAY_MICROS);
            // No gaps means the proofs already HOLD and the file is still live
            // only because a proof is evaluated at publish time and nothing has
            // republished this partition — 15 of prod's 85 stalled files on
            // 2026-08-22. Republishing the untagged spans themselves is what
            // fires it, and is still bounded by those files rather than the day.
            let slices = crate::rollup::uncovered_gaps(untagged, tagged).tap_mut(|gaps| {
                if gaps.is_empty() {
                    // Merged for the same reason `uncovered_gaps` merges: one
                    // span per untagged file is one unit per file, and files in
                    // a partition overlap.
                    *gaps = crate::write::mem_buffer::merge_ranges(untagged.clone());
                }
            });
            for (start, end) in crate::rollup::rebuild_slices(slices, tagged, day_start, day_end) {
                let Ok(slice) = TimeSlice::new(start, end) else { continue };
                let key = TaskKey { physical_table: target.to_owned(), source: source.to_owned(), project_id: project_id.clone(), slice, operation };
                journal.enqueue(key, now, MAX_DECODED_BYTES, created);
                queued += 1;
            }
        }
        let _ = journal.checkpoint();
        warn!(
            source,
            target,
            queued,
            partitions = partitions.len(),
            event = "rollup_tier_untagged_rebuild_queued",
            "partitions still hold tier files with no identity tags"
        );
    }

    /// How far a date is covered contiguously, starting from its first row.
    ///
    /// `None` when the spans leave a hole. The read path serves
    /// `[day_start, covered_through)` from a date entry, so the answer must be a
    /// run with no gap in it — a union that merely SPANS the day would claim
    /// hours no build aggregated, which is the silent-wrong-number failure this
    /// whole path is careful about.
    ///
    /// Anchored on the partition's earliest row rather than on midnight, for the
    /// same reason the producer is: units are planned from row statistics, so a
    /// day's first slice begins at its first row, and `[day_start, first_row)`
    /// holds nothing a raw scan would find either.
    fn contiguous_coverage_end(partition_min_ts: i64, spans: &mut [(i64, i64)]) -> Option<i64> {
        spans.sort_unstable();
        let (first_start, first_end) = *spans.first()?;
        if first_start > partition_min_ts {
            return None;
        }
        spans.iter().skip(1).try_fold(first_end, |cursor, &(start, end)| (start <= cursor).then(|| cursor.max(end)))
    }

    /// Rebuild DATE-level coverage from the slices recovery just restored.
    ///
    /// Without this the date-level map is empty after every restart and refills
    /// only as new units publish, so the second routing route — the one that does
    /// NOT consult the per-slice witness — is unavailable exactly when the
    /// process is youngest. Measured 2026-08-23: coverage takes ~25 minutes to
    /// become usable while prod redeploys every ~15, so a 7d dashboard group-by
    /// ran raw (8.98s, zero hits) on a 15-minute-old container while the same
    /// query on a container that HAD stayed up routed in 3.7s.
    ///
    /// Soundness is the whole of this function. Stamping the CURRENT partition
    /// fingerprint asserts the rollup is current, which recovery cannot know from
    /// the tier alone — so it is proven first, using the row WITNESS the read
    /// path already trusts: every slice of the date must carry a witness that
    /// still equals the partition's live `num_records`. That pays the witness
    /// check once at boot instead of on every query, and a date with even one
    /// unverifiable slice is skipped rather than guessed at.
    async fn recover_date_coverage(&self, source: &str, target: &str) {
        let Ok(table_ref) = self.resolve_table("default", source).await else { return };
        let stats = {
            let table = table_ref.read().await;
            match Self::partition_stats_bounded(&table, tiebreak_of(source), &|_, _| i64::MAX) {
                Ok(stats) => stats,
                Err(_) => return,
            }
        };
        let mut by_date: HashMap<(String, String), Vec<(i64, RollupCoverage)>> = HashMap::new();
        for entry in self.rollup_slice_coverage.iter() {
            let ((project, entry_source, entry_target, start, _), coverage) = (entry.key(), entry.value());
            if entry_source != source || entry_target != target {
                continue;
            }
            let Some(date) = chrono::DateTime::from_timestamp_micros(*start).map(|time| time.date_naive().to_string()) else { continue };
            by_date.entry((project.clone(), date)).or_default().push((*start, coverage.clone()));
        }
        let mut recovered = 0u64;
        for ((project, date), slices) in by_date {
            let Some(partition) = stats.get(&(project.clone(), date.clone())).or_else(|| stats.get(&("default".to_string(), date.clone()))) else {
                continue;
            };
            let Ok(current_rows) = u64::try_from(partition.rows) else { continue };
            // Every slice must be verifiable AND agree. One unverifiable slice
            // means the date cannot be proven current, and an unproven date must
            // not be stamped — that is the difference between this and the
            // `source_fp` fallback that had to be removed (7e5bb5a).
            if !slices.iter().all(|(_, coverage)| coverage.source_rows == Some(current_rows)) {
                continue;
            }
            let mut spans: Vec<(i64, i64)> = slices.iter().map(|(start, coverage)| (*start, coverage.covered_through)).collect();
            let Some(covered_through) = Self::contiguous_coverage_end(partition.min_ts, &mut spans) else { continue };
            let newest = slices.iter().map(|(_, coverage)| coverage).max_by_key(|coverage| coverage.covered_through).expect("non-empty");
            self.rollup_coverage.insert(
                (project.clone(), source.to_string(), target.to_string(), date.clone()),
                RollupCoverage {
                    source_fp: partition.fingerprint,
                    source_epoch: self.rollup_source_epochs.get(&(project, source.to_string(), date)).map_or(0, |epoch| *epoch.value()),
                    generation: newest.generation.clone(),
                    rows: slices.iter().map(|(_, coverage)| coverage.rows).sum(),
                    source_rows: Some(current_rows),
                    covered_through,
                },
            );
            recovered += 1;
        }
        if recovered != 0 {
            info!(source, target, recovered, event = "rollup_date_coverage_recovered", "date-level coverage rebuilt from witnessed slices");
        }
    }

    /// Republish the slices whose coverage can never be verified.
    ///
    /// Same shape as `enqueue_untagged_rebuilds` and for the same reason: work the
    /// coordinator will not reach on its own, because nothing about a sealed,
    /// fully-covered day says "republish me" — the claim is unverifiable, not
    /// missing. Enqueue is idempotent (keyed), so re-running hourly re-queues only
    /// what has not drained.
    fn enqueue_witnessless_rebuilds(
        &self, source: &str, spec: &crate::schema::RollupSpec, target: &str, slices: &[(String, crate::maintenance_coordinator::TimeSlice)],
    ) {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskKey};
        if slices.is_empty() {
            return;
        }
        let operation = if spec.derive_from.is_some() { Operation::DerivedRollup } else { Operation::BaseRollup };
        let now = crate::support::now_micros();
        let created = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
        // NEWEST FIRST, and bounded. The first pass on prod found 23,337 of these
        // across the two sources — 92% of ALL recovered coverage — and queueing
        // them flat took `pending_base_rollup` from 7,075 to 12,131 against a
        // measured ~16 builds/hr. That backlog cannot drain, and an undrainable
        // queue is not merely slow: it makes the coordinator's ranking meaningless,
        // because a two-week-old slice nobody queries competes with yesterday's.
        //
        // A dashboard needs CONTIGUOUS RECENT days, so the newest slice is worth
        // more than any older one and the bound costs nothing it would have got.
        // Re-running hourly advances the frontier: a republished slice carries a
        // witness, leaves this list, and the next pass takes the next `BOUND`.
        let mut ordered: Vec<_> = slices.iter().collect();
        ordered.sort_unstable_by_key(|(_, slice)| std::cmp::Reverse(slice.start_micros));
        const BOUND: usize = 512;
        let queued = ordered.len().min(BOUND);
        let mut journal = self.journal();
        for (project_id, slice) in ordered.into_iter().take(BOUND) {
            let key = TaskKey { physical_table: target.to_owned(), source: source.to_owned(), project_id: project_id.clone(), slice: *slice, operation };
            journal.enqueue(key, now, MAX_DECODED_BYTES, created);
        }
        let _ = journal.checkpoint();
        warn!(
            source,
            target,
            queued,
            // Named, never silent: a cap that does not say what it dropped reads
            // as "everything is queued" and hides the real size of the backlog.
            deferred = slices.len().saturating_sub(queued),
            event = "rollup_witnessless_rebuild_queued",
            "slices predating the row witness cannot be verified and were queued for republish, newest first"
        );
    }

    /// How many DATE-level coverage entries exist. Currently always 0 — nothing
    /// writes `rollup_coverage` — which is the whole reason this is exposed:
    /// the dead map produced no miss, no error and no log, so only counting it
    /// makes it visible. A restored producer must move this off zero.
    pub fn rollup_coverage_entries(&self) -> usize {
        self.rollup_coverage.len()
    }

    /// Seed routing coverage from the durable ledger, returning how many slices
    /// it published. Off unless `timefusion_coverage_ledger_reads`.
    ///
    /// This is the read-path move, and its value is entirely about RESTARTS.
    /// `recover_rollup_coverage` rebuilds the same map by replaying every tier's
    /// Delta log — minutes of work — and until it lands the router does not
    /// attempt to route at all. The ledger already holds that answer durably, so
    /// routing can be correct from the first query after boot.
    ///
    /// Sound only because the ledger records exactly the slices that passed the
    /// read path's own filters — see `record_readable_coverage`. It seeds rather
    /// than replaces: the replay still runs, still verifies, and overwrites
    /// anything here, so a stale ledger costs one interval of narrower coverage
    /// and never a wrong answer.
    pub fn seed_routing_from_ledger(&self) -> usize {
        if !self.config.maintenance.timefusion_coverage_ledger_reads {
            return 0;
        }
        use crate::storage::CoverageLedger as _;
        let mut seeded = 0usize;
        for cell in self.coverage_ledger.cells() {
            let (source, project_id, table_name, _date) = cell.clone();
            for entry in self.coverage_ledger.coverage(&cell) {
                self.rollup_slice_coverage.insert(
                    (project_id.clone(), source.clone(), table_name.clone(), entry.start_micros, entry.end_micros),
                    RollupCoverage {
                        source_fp: entry.source_fingerprint,
                        source_epoch: 0,
                        generation: entry.generation.clone(),
                        rows: 0,
                        source_rows: entry.source_rows.and_then(|rows| u64::try_from(rows).ok()),
                        covered_through: entry.end_micros,
                    },
                );
                seeded += 1;
            }
        }
        if seeded > 0 {
            info!(seeded, event = "rollup_coverage_seeded_from_ledger", "routing coverage available before the tag replay");
        }
        seeded
    }

    /// Write one tier's READABLE coverage into the ledger, verifying it against
    /// what the ledger already held.
    ///
    /// Called with slices that passed every filter the read path applies, so the
    /// ledger claims exactly what a query would be served — never more. Writing
    /// it from the raw tag loop instead was a real defect: slices that are
    /// incomplete, or whose `generation_id` no longer matches the current spec,
    /// are refused by the read path, and a ledger recording them would have
    /// over-claimed the moment reads moved onto it.
    ///
    /// The verifier is why the tag replay survives once reads DO move over. The
    /// one risk this design adds is an authority that can drift where
    /// self-describing files cannot, and `coverage_ledger_disagreements` is the
    /// standing alarm against it — it must read zero before any read path trusts
    /// the ledger.
    fn record_readable_coverage(&self, source: &str, target: &str, readable: HashMap<crate::storage::CoverageCell, Vec<crate::storage::CoverageEntry>>) {
        use crate::storage::CoverageLedger as _;
        let mut disagreements = 0u64;
        // Accumulated and written ONCE. Per-cell writes would re-serialize the
        // whole ledger for every cell of every tier, every hour.
        let mut batch: Vec<(crate::storage::CoverageCell, Vec<crate::storage::CoverageEntry>)> = Vec::new();
        let seen: std::collections::HashSet<crate::storage::CoverageCell> = readable.keys().cloned().collect();
        for (cell, entries) in readable {
            let proved = crate::storage::merge_coverage(entries);
            let held = self.coverage_ledger.coverage(&cell);
            // An empty `held` is the FIRST replay for that cell, not a
            // disagreement — counting it would make every cell report drift on a
            // fresh boot, and an alarm that fires on every boot is ignored.
            if !held.is_empty() && held != proved {
                disagreements += 1;
                warn!(
                    table = %target, project_id = %cell.1, date = %cell.3,
                    held = held.len(), proved = proved.len(),
                    event = "coverage_ledger_disagreement",
                    "the ledger and the Delta tags disagree about this partition's coverage"
                );
            }
            batch.push((cell, proved));
        }
        // Cells this tier no longer covers at all. Scoped to (source, target)
        // because this replay proves nothing about a tier it did not read.
        let orphans: Vec<_> = self
            .coverage_ledger
            .cells()
            .into_iter()
            .filter(|(cell_source, _, cell_table, _)| cell_source == source && cell_table == target)
            .filter(|cell| !seen.contains(cell))
            .collect();
        // Retired in the same batch — an empty entry list drops the cell.
        batch.extend(orphans.into_iter().map(|cell| (cell, Vec::new())));
        self.coverage_ledger.replace_many(batch);
        if disagreements > 0 {
            crate::observability::maintenance_stats().coverage_ledger_disagreements.fetch_add(disagreements, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub async fn recover_rollup_coverage(&self, source: &str) -> Result<usize> {
        let Some(schema) = get_schema(source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(0) };
        if !self.config.maintenance.timefusion_rollup_enabled {
            return Ok(0);
        }
        let mut recovered = 0;
        // Summed over every declared tier, then stored ONCE. Storing per tier made
        // the last spec's count the whole reading, which is zero whenever only the
        // base tier has a backlog — the same "cannot tell none from unmeasured"
        // failure the untagged gauge already had.
        let mut unverifiable = 0u64;
        // Slices whose stored generation no longer matches the current spec:
        // coverage that exists on disk and cannot be used. See the skip below.
        let mut stale_generation = 0u64;
        for spec in &schema.rollups {
            let target = spec.table_name(source);
            let (mut untagged_spans, mut tagged_spans): (SpansByPartition, SpansByPartition) = Default::default();
            // Counted separately from `untagged_spans`, which drops a file with
            // no statistics — the gauge must count FILES, including those.
            let mut untagged_files = 0u64;
            // New generations carry complete coverage identity in Delta Add
            // tags. Recovery reads only the transaction log; no rollup data
            // scan competes with foreground queries at startup.
            let (tagged, paths_by_identity) = match self.resolve_table("default", &target).await {
                Ok(table) => {
                    let table = table.read().await;
                    // `source_rows` is part of the KEY so a partition rebuilt against a
                    // different source count cannot merge with the older evidence.
                    let mut groups: HashMap<TaggedSliceIdentity, u64> = HashMap::new();
                    // Paths per tagged identity, keyed exactly like `groups` so the
                    // FILTERED loop below can recover them. The ledger must not be
                    // written from this raw loop: the filters that follow
                    // (`rollup_slice_complete`, and the `generation_id` match) are
                    // what decide whether a slice is READABLE, and a ledger written
                    // before them claims coverage the read path refuses — the one
                    // failure this design must never have.
                    let mut paths_by_identity: HashMap<TaggedSliceIdentity, (String, Vec<String>)> = HashMap::new();
                    for add in table.snapshot()?.log_data().iter() {
                        #[allow(deprecated)]
                        let action = add.add_action();
                        // An untagged file proves no coverage, so this loop has
                        // always skipped it. Skipping SILENTLY is what let 352 of
                        // them accumulate over a month: `slice_retires` can now
                        // retire one, but only when something publishes that
                        // partition, and a sealed day that already has coverage is
                        // never republished — nothing would ever enqueue it.
                        // Remember the partition so the rebuild can be requested
                        // below, which is what makes the tail self-healing rather
                        // than a manual list someone has to keep.
                        // Both arms feed `uncovered_gaps`: the untagged file's
                        // own statistics span is the work, and the tagged
                        // ranges beside it are what is already done. `hi + 1`
                        // because statistics bounds are inclusive while a slice
                        // end is not.
                        let file_partition = Self::maintenance_partition_from_action(&action.path, Some(&action.partition_values), "default");
                        if let Some(partition) = file_partition.clone() {
                            let tags = action.tags.as_ref();
                            let tag = |name: &str| tags?.get(name).and_then(Option::as_deref)?.parse::<i64>().ok();
                            match (tag(crate::maintenance_coordinator::TAG_SLICE_START), tag(crate::maintenance_coordinator::TAG_SLICE_END)) {
                                (Some(start), Some(end)) => tagged_spans.entry(partition).or_default().push((start, end)),
                                _ => {
                                    untagged_files = untagged_files.saturating_add(1);
                                    untagged_spans
                                        .entry(partition)
                                        .or_default()
                                        .extend(action.stats.as_deref().and_then(crate::rollup::stats_time_range).map(|(lo, hi)| (lo, hi.saturating_add(1))));
                                }
                            }
                        }
                        let Some(tags) = action.tags.as_ref() else { continue };
                        let tag = |name: &str| tags.get(name).and_then(Option::as_deref);
                        if tag(crate::maintenance_coordinator::TAG_SOURCE) != Some(source) {
                            continue;
                        }
                        let (Some(project), Some(generation), Some(source_fp), Some(slice_start), Some(slice_end)) = (
                            tag(crate::maintenance_coordinator::TAG_PROJECT),
                            tag(crate::maintenance_coordinator::TAG_GENERATION),
                            tag(crate::maintenance_coordinator::TAG_SOURCE_FINGERPRINT).and_then(|value| value.parse::<u64>().ok()),
                            tag(crate::maintenance_coordinator::TAG_SLICE_START).and_then(|value| value.parse::<i64>().ok()),
                            tag(crate::maintenance_coordinator::TAG_SLICE_END).and_then(|value| value.parse::<i64>().ok()),
                        ) else {
                            continue;
                        };
                        let rows = action.get_stats().ok().flatten().map_or(0, |stats| u64::try_from(stats.num_records.max(0)).unwrap_or(0));
                        // Absent on generations written before the tag; `-1` is the
                        // sentinel a build writes when the source reported no count.
                        // Both become `None`, which the read path refuses to verify.
                        let source_rows = tag(crate::maintenance_coordinator::TAG_SOURCE_ROWS)
                            .and_then(|value| value.parse::<i64>().ok())
                            .and_then(|rows| u64::try_from(rows).ok());
                        let entry = groups.entry((project.to_owned(), slice_start, slice_end, generation.to_owned(), source_fp, source_rows)).or_default();
                        *entry = entry.saturating_add(rows);
                        // Same facts, recorded explicitly. The date comes from the
                        // file's own partition rather than from `slice_start`,
                        // because a day-wide slice starts at midnight of the day it
                        // covers while a file in `date=D` cannot hold rows outside
                        // `D` — the partition is the stronger statement.
                        if let Some((partition_project, _date)) = file_partition.as_ref() {
                            // The date comes from the file's PARTITION, not from
                            // `slice_start`: a file in `date=D` cannot hold rows
                            // outside `D`, so the partition is the stronger
                            // statement, and a day-wide slice beginning at midnight
                            // would otherwise be indistinguishable from one that
                            // merely starts there.
                            let entry = paths_by_identity
                                .entry((project.to_owned(), slice_start, slice_end, generation.to_owned(), source_fp, source_rows))
                                .or_insert_with(|| ((*partition_project).to_owned(), Vec::new()));
                            entry.1.push(action.path.clone());
                        }
                    }
                    (groups, paths_by_identity)
                }
                Err(_) => (HashMap::new(), HashMap::new()),
            };
            // Filled by the FILTERED loop below, then verified and written once
            // per tier. Nothing is recorded for a slice the read path would
            // refuse, because the ledger is meant to become the authority and an
            // authority that over-claims serves wrong results.
            let mut readable: HashMap<crate::storage::CoverageCell, Vec<crate::storage::CoverageEntry>> = HashMap::new();
            // Before any `continue` below, so a partition still holding
            // untagged files is queued whatever the coverage verdict is.
            // The gauge, from the WHOLE tier, hourly. Setting it only on publish
            // meant it read 0 both when a tier was clean and when nothing had
            // published yet — which on 2026-08-22 read as "converged" over 85
            // live untagged files for the first 40 minutes of every boot. A
            // reading that cannot distinguish "none" from "unmeasured" is not a
            // measurement.
            self.rollup_tier_untagged.insert(target.clone(), untagged_files);
            crate::observability::maintenance_stats()
                .rollup_tier_untagged_found
                .store(self.rollup_tier_untagged.iter().map(|entry| *entry.value()).sum(), std::sync::atomic::Ordering::Relaxed);
            let untagged_partitions: UntaggedPartitions = untagged_spans
                .drain()
                .map(|(partition, untagged)| {
                    let tagged = tagged_spans.remove(&partition).unwrap_or_default();
                    (partition, (untagged, tagged))
                })
                .collect();
            self.enqueue_untagged_rebuilds(source, spec, &target, &untagged_partitions);
            // Slices recovered WITHOUT a row witness. They predate `TAG_SOURCE_ROWS`
            // and carry no evidence any read-side rule can verify — not the witness
            // (absent) and not a partition fingerprint (never persisted; the slice's
            // own `source_fp` is incomparable, see 7e5bb5a). They are refused as
            // `stale_coverage` forever, and 2026-08-22/23 measured them as ~80% of
            // every stale decline on the whale (612 no_witness against 156 moved per
            // three reps, constant across query shapes — a fixed population re-judged,
            // not churn). Republishing is the only thing that clears them, and left in
            // the general queue they compete with ~7,000 other units behind a restart
            // cadence measured in tens of minutes. Queue them explicitly instead.
            let mut witnessless: Vec<(String, crate::maintenance_coordinator::TimeSlice)> = Vec::new();
            // The BACKLOG comes from the Delta tags, which are durable and unaffected
            // by journal state. Counting it from `published_rollups` instead made the
            // gauge lie the moment it worked: enqueueing a slice flips its task off
            // Complete, `published_rollups` filters on Complete, so the second hourly
            // pass saw zero and prod read `rollup_witnessless_slices = 0` over a
            // 23,337-slice backlog. Same "cannot tell none from unmeasured" failure
            // the untagged gauge above already had, reintroduced one field over.
            unverifiable += tagged.keys().filter(|(.., source_rows)| source_rows.is_none()).count() as u64;
            let published = self.journal().published_rollups(source, &target);
            for (key, publication) in &published {
                if publication.source_rows.is_none() {
                    witnessless.push((key.project_id.clone(), key.slice));
                }
                self.rollup_slice_coverage.insert(
                    (key.project_id.clone(), source.to_string(), target.clone(), key.slice.start_micros, key.slice.end_micros),
                    RollupCoverage {
                        source_fp: publication.source_fingerprint,
                        source_epoch: 0,
                        generation: publication.generation.clone(),
                        rows: publication.rows,
                        source_rows: publication.source_rows,
                        covered_through: key.slice.end_micros,
                    },
                );
                recovered += 1;
            }
            if !tagged.is_empty() {
                for ((project_id, slice_start, slice_end, generation, source_fp, source_rows), rows) in tagged {
                    let Ok(slice) = crate::maintenance_coordinator::TimeSlice::new(slice_start, slice_end) else { continue };
                    let complete = self.journal().rollup_slice_complete(source, &project_id, &target, slice);
                    let Some(date) = chrono::DateTime::from_timestamp_micros(slice_start).map(|time| time.date_naive().to_string()) else { continue };
                    // A stored generation that no longer matches the CURRENT spec
                    // is unrecoverable coverage, and it was a bare `continue` —
                    // the single most consequential silent skip in the rollup
                    // system.
                    //
                    // `generation_id` hashes the whole spec, so ADDING A MEASURE
                    // invalidates every slice built before it. The read path then
                    // answers `not_built` for those dates forever, while
                    // `rollup_coverage_contiguity` still reports 30 days because
                    // the census counts DATE PARTITIONS, not generation matches.
                    // A spec change therefore strips read coverage from the whole
                    // history and no gauge moves. `duration_digest` on 2026-08-22
                    // is exactly that shape.
                    //
                    // Counting it does not fix it — the cure is either a rebuild
                    // or an identity that tolerates additive change — but it makes
                    // the cost of a spec edit visible at the moment it is paid.
                    if !complete {
                        continue;
                    }
                    if crate::rollup::generation_id(spec, source, &project_id, &date, source_fp) != generation {
                        stale_generation += 1;
                        continue;
                    }
                    if source_rows.is_none() {
                        witnessless.push((project_id.clone(), slice));
                    }
                    // Past every readability filter, so this slice is exactly
                    // what the read path will serve — and therefore exactly what
                    // the ledger may claim.
                    if let Some((partition_project, paths)) =
                        paths_by_identity.get(&(project_id.clone(), slice_start, slice_end, generation.clone(), source_fp, source_rows))
                    {
                        readable.entry((source.to_owned(), partition_project.clone(), target.clone(), date.clone())).or_default().push(
                            crate::storage::CoverageEntry {
                                start_micros: slice_start,
                                end_micros: slice_end,
                                generation: generation.clone(),
                                source_fingerprint: source_fp,
                                source_rows: source_rows.and_then(|rows| i64::try_from(rows).ok()),
                                files: paths.clone(),
                            },
                        );
                    }
                    self.rollup_slice_coverage.insert(
                        (project_id, source.to_string(), target.clone(), slice_start, slice_end),
                        RollupCoverage { source_fp, source_epoch: 0, generation, rows, source_rows, covered_through: slice_end },
                    );
                    recovered += 1;
                }
                self.record_readable_coverage(source, &target, readable);
                self.enqueue_witnessless_rebuilds(source, spec, &target, &witnessless);
                self.recover_date_coverage(source, &target).await;
                continue;
            }
            self.enqueue_witnessless_rebuilds(source, spec, &target, &witnessless);
            self.recover_date_coverage(source, &target).await;
            if !published.is_empty() {
                continue;
            }
            // Untagged legacy generations cannot be recovered safely without
            // scanning rollup data.  A production restart showed that even a
            // single 230 MiB compatibility GROUP BY starved PGWire for 19s.
            // Leave those intervals uncovered so reads use exact raw data;
            // the coordinator will replace them with tagged slices whose
            // coverage is recoverable from Delta metadata alone.
        }
        crate::observability::maintenance_stats().rollup_witnessless_slices.store(unverifiable, std::sync::atomic::Ordering::Relaxed);
        // `stale_generation` is coverage that EXISTS on disk and cannot be used:
        // read it against `recovered` to price a spec change after the fact.
        info!(source, recovered, unverifiable, stale_generation, event = "rollup_coverage_recovered");
        Ok(recovered)
    }

    /// Fold one completed coordinator dedup unit into per-day clean-slice coverage,
    /// granting certification once the accumulated slices cover the whole UTC day.
    ///
    /// A slice counts as clean evidence only when the pass dropped nothing AND the partition's
    /// file fingerprint did not move across it. Evidence is per-fingerprint: a slice observed
    /// under a different fp than the accumulation resets it to just that slice — a moved file
    /// set (new write, compaction) voids what was proved over the old one. A dirty pass resets
    /// coverage and voids any existing certification via `record_certification`'s removal arm.
    /// The grant itself goes through `record_certification`, which recomputes the live fp one
    /// final time — so the rule cannot drift from the sweep/backfill paths.
    async fn record_clean_slice(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
        (slice, dropped): (crate::maintenance_coordinator::TimeSlice, u64), pre: &[String],
    ) -> Result<Option<u64>> {
        let day_start = date.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc().timestamp_micros();
        let day_end = day_start.saturating_add(crate::maintenance_coordinator::DAY_MICROS);
        let (start, end) = (slice.start_micros.max(day_start), slice.end_micros.min(day_end));
        if start >= end {
            metrics::counter!(scan_metric_names::CERT_SLICE_OUTSIDE_DAY).increment(1);
            return Ok(None); // a slice outside the day proves nothing about it
        }
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        let post = {
            let table = table_ref.read().await;
            Self::partition_files_by_pid(&table, &format!("date={date}"))?.remove(project_id).unwrap_or_default()
        };
        let fp = partition_file_fp(post.clone());
        if dropped != 0 || post.is_empty() || partition_file_fp(pre.to_vec()) != fp {
            metrics::counter!(scan_metric_names::CERT_SLICE_DIRTY).increment(1);
            if self.dedup_slice_coverage.remove(&key).is_some() {
                self.persist_slice_coverage();
            }
            return self.record_certification(table_ref, table_name, project_id, date, pre, (dropped, true)).await;
        }
        // Bind `covered` in its own block: the RefMut must drop before the
        // remove/await below (same DashMap-shard self-deadlock documented at
        // `dedup_window_clean`).
        let covered = {
            let mut entry = self.dedup_slice_coverage.entry(key.clone()).or_insert_with(|| SliceCoverage { fp, intervals: Vec::new() });
            if entry.fp != fp {
                *entry = SliceCoverage { fp, intervals: vec![(start, end)] };
            } else {
                merge_clean_interval(&mut entry.intervals, (start, end));
            }
            entry.intervals.iter().any(|&(s, e)| s <= day_start && e >= day_end)
        };
        // Write-through on every mutation: the journal durably marks this slice
        // Complete (it will never re-run), so its evidence must be equally
        // durable or a restart strands the day at partial coverage forever.
        self.persist_slice_coverage();
        if !covered {
            metrics::counter!(scan_metric_names::CERT_SLICE_PARTIAL).increment(1);
            return Ok(None);
        }
        metrics::counter!(scan_metric_names::CERT_SLICE_DAY_COVERED).increment(1);
        self.dedup_slice_coverage.remove(&key);
        self.persist_slice_coverage();
        self.record_certification(table_ref, table_name, project_id, date, &post, (0, true)).await
    }

    /// Apply the certification rule to one finished dedup pass and record the verdict.
    ///
    /// Returns the clean fingerprint when a zero-drop pass over the still-live file set proves the
    /// partition duplicate-free. `complete` is required because `Ok(0)` with skipped unsealed or
    /// over-budget chunks proves nothing; any concurrent commit changes the set and must not certify.
    /// Both the sweep and the backfill go through here so the rule cannot drift.
    async fn record_certification(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, pre: &[String],
        (dropped, complete): (u64, bool),
    ) -> Result<Option<u64>> {
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        let post = {
            let table = table_ref.read().await;
            Self::partition_files_by_pid(&table, &format!("date={date}"))?.remove(project_id).unwrap_or_default()
        };
        let fp_post = partition_file_fp(post.clone());
        if dropped == 0 && complete && !post.is_empty() && partition_file_fp(pre.to_vec()) == fp_post {
            // Re-certifying at the SAME fingerprint continues the existing
            // certification rather than starting a new one: the sweep re-proved a
            // partition nothing had touched. Keeping the original `since` is what
            // stops a 5-minute sweep cadence from capping every dwell at 5 minutes.
            let prior = self.dedup_clean_fp.get(&key).map(|e| e.value().clone()).filter(|prev| prev.fp == fp_post);
            // `post` is the very file list the pass proved clean. It was hashed
            // into `fp_post` and discarded; keeping it is what lets the per-FILE
            // skip ask "which of these files are still live" after the partition
            // has gained one.
            let files: Arc<[String]> = Arc::from(post.clone());
            if let Some(prev) = self
                .dedup_clean_fp
                .insert(key, Certification { fp: fp_post, since: prior.as_ref().map_or_else(std::time::Instant::now, |p| p.since), files, stale: false })
                && prev.fp != fp_post
            {
                self.scan_metrics.record_cert_dwell(prev.since);
            }
            if prior.is_none() {
                metrics::counter!(scan_metric_names::CERT_GRANTED_TOTAL).increment(1);
            }
            return Ok(Some(fp_post));
        }
        // Name the failing conjunct. Each is a different bug: `dropped` means the
        // pass genuinely removed rows, `incomplete` means chunks were skipped,
        // `empty` means the partition has no live files, and `fp_moved` means a
        // concurrent commit landed mid-pass (the irreducible one under ingest).
        metrics::counter!(match () {
            _ if dropped != 0 => scan_metric_names::CERT_REFUSED_DROPPED,
            _ if !complete => scan_metric_names::CERT_REFUSED_INCOMPLETE,
            _ if post.is_empty() => scan_metric_names::CERT_REFUSED_EMPTY,
            _ => scan_metric_names::CERT_REFUSED_FP_MOVED,
        })
        .increment(1);
        if let Some((_, prev)) = self.dedup_clean_fp.remove(&key) {
            self.scan_metrics.record_cert_dwell(prev.since);
        }
        Ok(None)
    }

    /// Is every partition in the window certified duplicate-free, and if not, why not?
    ///
    /// `FpMoved` outranks `NeverCertified`: one written-since-sweep date definitively denies the
    /// window, regardless of what is stored. Reporting the first denial by date order would
    /// systematically over-report `NeverCertified` because `window_dates` runs oldest-first.
    /// Short-circuiting on `FpMoved` avoids that bias. The extra work is one
    /// `partition_files_by_pid` per remaining date, bounded by `window_dates`'s 366-day cap.
    pub(crate) fn dedup_window_clean(&self, table: &DeltaTable, project_id: &str, table_name: &str, window: (i64, i64)) -> DedupSkipVerdict {
        self.dedup_window_certified(table, project_id, table_name, window).0
    }

    /// As `dedup_window_clean`, but also returns the `date=` values whose
    /// certification still matches the live file set.
    ///
    /// The set is what makes a PER-DATE skip possible: a window that is only
    /// partly certified used to lose the skip entirely, and prod never has a
    /// fully certified window (2026-08-22: 97 live certifications, longest
    /// consecutive run 5 days, against 7 for a week and 30 for a month). Per
    /// date is sound because `date` is derived from `timestamp` and DML
    /// re-appends preserve the original timestamp (`write/mod.rs:1104`), so
    /// every version and tombstone of a row shares one date partition — no
    /// dedup key can span dates.
    ///
    /// Unlike the old short-circuit, this does NOT return early on the first
    /// stale certification: the whole window must be walked to collect the
    /// certified set. `FpMoved` still outranks `NeverCertified` in the verdict.
    pub(crate) fn dedup_window_certified(
        &self, table: &DeltaTable, project_id: &str, table_name: &str, (lo, hi): (i64, i64),
    ) -> (DedupSkipVerdict, HashSet<String>) {
        let mut certified_dates: HashSet<String> = HashSet::new();
        let Some(dates) = window_dates(lo, hi) else { return (DedupSkipVerdict::NoWindow, certified_dates) };
        let mut verdict = DedupSkipVerdict::Granted;
        let mut saw_fp_moved = false;
        // Did any partition actually produce evidence? `Granted` is the loop's
        // seed, and every `continue` below leaves it untouched — so a window in
        // which EVERY date is skipped (no Delta files under this project's key)
        // used to return `Granted` having certified nothing. That is granting a
        // "provably duplicate-free" verdict from an absence of evidence, and the
        // skip it authorises removes DedupExec from the whole scan, not just the
        // Delta leg: the MemBuffer and hot-tier legs are unioned in and can hold
        // superseded merge-on-read versions of their own. A `count(*)` over such
        // a window then reports physical rows.
        //
        // Declining costs a fast path on a window that has no Delta files to
        // read anyway. Granting wrongly over-counts, silently, on every
        // dashboard tile — so the default must be to decline.
        let mut certified_any = false;
        for date in dates {
            let Ok(mut by_pid) = Self::partition_files_by_pid(table, &format!("date={date}")) else {
                return (DedupSkipVerdict::Unresolved, HashSet::new());
            };
            // The sweep keys custom-project tables (no project_id= path
            // segment) under "default"; match its grouping exactly.
            let Some((key_pid, files)) =
                by_pid.remove(project_id).map(|f| (project_id.to_string(), f)).or_else(|| by_pid.remove("default").map(|f| ("default".to_string(), f)))
            else {
                continue; // no Delta files for this date → nothing to dedup
            };
            let fp_key = (key_pid, table_name.to_string(), date.to_string());
            // Bound in a `let`, NOT inlined as the match scrutinee: a DashMap `Ref`
            // temporary in a scrutinee lives until the end of the match, and the
            // stale arm below removes from the same shard — a self-deadlock.
            let certified = self.dedup_clean_fp.get(&fp_key).map(|entry| entry.value().clone());
            match certified {
                Some(cert) if cert.fp == partition_file_fp(files) => {
                    certified_any = true;
                    certified_dates.insert(date.to_string());
                    continue;
                }
                Some(cert) => {
                    // Provably stale: this fingerprint can never match again until a
                    // sweep re-certifies. Drop it so the dwell is recorded exactly
                    // once rather than on every read that trips over it — but only if
                    // it is still the value we read, so a sweep that re-certified in
                    // the gap does not lose its fresh entry.
                    // With the per-FILE skip on, the entry is KEPT and merely
                    // marked stale: its file list stays true of the files it
                    // names, and deleting it would destroy the evidence for
                    // every file the new one does not overlap. Dwell is still
                    // recorded exactly once, on the transition.
                    match self.config.maintenance.timefusion_read_dedup_skip_per_file {
                        false => {
                            if self.dedup_clean_fp.remove_if(&fp_key, |_, live| live.fp == cert.fp).is_some() {
                                self.scan_metrics.record_cert_dwell(cert.since);
                            }
                        }
                        true if !cert.stale => {
                            self.dedup_clean_fp.alter(&fp_key, |_, mut live| {
                                live.stale = true;
                                live
                            });
                            self.scan_metrics.record_cert_dwell(cert.since);
                        }
                        true => {}
                    }
                    saw_fp_moved = true;
                }
                None => verdict = DedupSkipVerdict::NeverCertified,
            }
        }
        // `FpMoved` outranks `NeverCertified` (see the doc comment) INCLUDING
        // over the no-evidence fallback below — it is applied after the full
        // walk rather than by an early return only so the certified set is
        // complete for the per-date skip. Ordering it after `certified_any`
        // instead reports a written-to partition as never-certified
        // (regression caught by
        // `a_denied_skip_says_whether_it_was_never_certified_or_written_to_since`).
        if saw_fp_moved {
            return (DedupSkipVerdict::FpMoved, certified_dates);
        }
        match certified_any {
            true => (verdict, certified_dates),
            false => (DedupSkipVerdict::NeverCertified, HashSet::new()),
        }
    }

    pub(crate) fn logical_count_partition_snapshot(table: &DeltaTable, project_id: &str, date: &str) -> Result<(u64, Vec<String>)> {
        let snapshot = table.snapshot()?.snapshot();
        let files = dedup_partition_paths(snapshot.log_data().iter().map(|file| file.path().to_string()), project_id, date);
        Ok((partition_file_fp(files.clone()), files))
    }

    /// Memory-only lookup for a base whose files are all present in the table
    /// snapshot the caller holds. Newly appended files are returned for a
    /// narrow overlay; any removal/rewrite declines. Filesystem IO is forbidden
    /// on this query path.
    pub(crate) fn logical_count_memory_for_files(
        &self, project_id: &str, table_name: &str, date: &str, files: &HashSet<String>,
    ) -> Option<(Arc<crate::read::LogicalCountIndex>, Vec<String>)> {
        let key = crate::read::CountPartition { project_id: project_id.to_string(), table_name: table_name.to_string(), date: date.to_string() };
        self.logical_count_cache.get_memory_appendable(&key, files)
    }

    pub(crate) async fn logical_count_overlay_batches(
        &self, snapshot: Arc<deltalake::kernel::EagerSnapshot>, log_store: deltalake::logstore::LogStoreRef, files: Vec<String>,
        columns: crate::read::LogicalCountColumns<'_>,
    ) -> Result<Vec<RecordBatch>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let provider =
            Self::narrow_provider(log_store, snapshot, files, None).await.map_err(|error| anyhow::anyhow!("logical-count overlay provider: {error}"))?;
        let context = SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.shared_runtime_env()));
        context.register_table("__logical_count_overlay", provider)?;
        Ok(context
            .table("__logical_count_overlay")
            .await?
            .select_columns(&[columns.timestamp, columns.id, columns.tiebreak, columns.deleted])?
            .collect()
            .await?)
    }

    /// Schedule one exact partition build. Concurrent misses share the same
    /// single-flight key and the global semaphore bounds winner-map memory.
    pub(crate) fn schedule_logical_count_build(self: &Arc<Self>, project_id: &str, table_name: &str, date: &str, force_refresh: bool) {
        let key = crate::read::CountPartition { project_id: project_id.to_string(), table_name: table_name.to_string(), date: date.to_string() };
        if !self.logical_count_building.insert(key.clone()) {
            return;
        }
        let database = Arc::clone(self);
        tokio::spawn(async move {
            let result = database.build_logical_count_partition(&key, force_refresh).await;
            database.logical_count_building.remove(&key);
            if let Err(error) = result {
                warn!(project_id = key.project_id, table_name = key.table_name, date = key.date, %error, "logical-count background build failed");
            }
        });
    }

    pub(crate) async fn build_logical_count_partition(&self, key: &crate::read::CountPartition, force_refresh: bool) -> Result<()> {
        let _permit = tokio::select! {
            permit = self.logical_count_build_sem.acquire() => permit?,
            () = self.maintenance_shutdown.cancelled() => return Ok(()),
        };
        let started = std::time::Instant::now();
        let table_ref = self.resolve_table(&key.project_id, &key.table_name).await?;
        let (fingerprint, files, eager_snapshot, log_store) = {
            let table = table_ref.read().await;
            let (fingerprint, files) = Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?;
            (fingerprint, files, Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };

        // Restart warm-up first tries the persistent Arrow tier off the async
        // worker. A valid file installs its memory front without scanning Delta.
        let cache = Arc::clone(&self.logical_count_cache);
        let disk_key = key.clone();
        let current_files = files.iter().cloned().collect();
        if !force_refresh
            && let Some(added_files) = tokio::task::spawn_blocking(move || cache.load_appendable(&disk_key, &current_files)).await?
            && added_files <= crate::read::MAX_APPEND_OVERLAY_FILES
        {
            return Ok(());
        }

        let declared = get_schema(&key.table_name).ok_or_else(|| anyhow::anyhow!("logical-count table is not registered"))?;
        anyhow::ensure!(declared.dedup_keys == ["timestamp", "id"], "logical-count currently requires dedup keys [timestamp,id]");
        let tiebreak = declared.dedup_tiebreak.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no dedup tiebreak"))?;
        let deleted = declared.tombstone_column.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no tombstone column"))?;
        let columns = crate::read::LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak, deleted };
        let mut index = crate::read::LogicalCountIndex::new();

        if !files.is_empty() {
            let provider = Self::narrow_provider(log_store, eager_snapshot, files.clone(), None)
                .await
                .map_err(|error| anyhow::anyhow!("logical-count provider: {error}"))?;
            let context =
                SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()));
            context.register_table("__logical_count_src", provider)?;
            let frame = context.table("__logical_count_src").await?.select_columns(&[columns.timestamp, columns.id, columns.tiebreak, columns.deleted])?;
            let mut stream = frame.execute_stream().await?;
            loop {
                let batch = tokio::select! {
                    batch = stream.try_next() => batch?,
                    () = self.maintenance_shutdown.cancelled() => return Ok(()),
                };
                let Some(batch) = batch else { break };
                index.apply_batch(&batch, columns)?;
                // The mutable builder intentionally costs more than the packed
                // resident form. Let it use half of this cache's budget while
                // retaining the host brake below; applying the four-way
                // resident limit here prevented large days from ever reaching
                // `finalize`, where their allocator/hash overhead is released.
                let build_limit = (self.config.derived.logical_count_memory_bytes() / 2).max(1);
                anyhow::ensure!(
                    index.estimated_heap_bytes() <= build_limit,
                    "logical-count partition exceeded its {}MB temporary build limit",
                    build_limit / (1024 * 1024)
                );
                let host_limit = self.config.derived.memory_brake_limit_bytes();
                anyhow::ensure!(
                    process_memory_bytes().is_none_or(|used| used <= host_limit),
                    "logical-count build stopped at the host memory brake ({}MB)",
                    host_limit / (1024 * 1024)
                );
            }
        }

        // Release the allocation-heavy mutable hash map before cache
        // admission. The packed form is exact and is the representation used
        // by every query and persisted Arrow partition.
        index.finalize()?;
        // A three-day dashboard window can touch four UTC partitions. Reserve
        // room for all four after compaction so valid daily indexes cannot
        // evict one another into a permanent rebuild loop.
        let per_index_limit = (self.config.derived.logical_count_memory_bytes() / 4).max(1);
        anyhow::ensure!(
            index.estimated_heap_bytes() <= per_index_limit,
            "logical-count partition exceeded its {}MB packed resident limit",
            per_index_limit / (1024 * 1024)
        );

        // Concurrent appends are safe: the query overlays their new files.
        // A removal/rewrite is not; it would leave winners from files no longer
        // in the table, so refuse publication and let the next miss rebuild.
        let current_files = {
            let table = table_ref.read().await;
            Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?.1.into_iter().collect::<HashSet<_>>()
        };
        anyhow::ensure!(files.iter().all(|file| current_files.contains(file)), "logical-count partition was rewritten during build");

        let physical_keys = index.physical_keys();
        let logical_rows = index.logical_rows();
        let estimated_bytes = index.estimated_heap_bytes();
        let file_count = files.len();
        let cache = Arc::clone(&self.logical_count_cache);
        let install_key = key.clone();
        tokio::task::spawn_blocking(move || cache.install(install_key, fingerprint, files, index)).await??;
        info!(
            project_id = key.project_id,
            table_name = key.table_name,
            date = key.date,
            fingerprint,
            file_count,
            physical_keys,
            logical_rows,
            estimated_bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "logical-count partition ready"
        );
        Ok(())
    }

    /// Sweep every `(project_id, today)` partition in this table via
    /// `dedup_partition`. Skips when Delta version is unchanged since the
    /// last sweep, and skips partitions in failure backoff. Best-effort:
    /// per-partition errors are logged and back the partition off.
    pub async fn dedup_today_partitions(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str) -> Result<()> {
        self.dedup_sweep(table_ref, table_name, dedup_key, None).await
    }

    /// `dedup_today_partitions` with a wall-clock bound on the tick.
    ///
    /// The sweep is O(dates × projects) with real IO per item. Without a deadline it can run far
    /// past the schedule, holding `maintenance_job_sem` and starving the dirty-bin drain. Every
    /// item is independent and idempotent, so truncation is safe; the cursor rotates so the next
    /// tick resumes where this one stopped rather than re-serving the same prefix forever.
    async fn dedup_sweep(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str, deadline: Option<std::time::Instant>) -> Result<()> {
        let schema = schema_or_default(table_name);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        // Sweep today plus a lookback window: a cross-flush dupe that lands in a
        // prior-day partition (late DLQ replay crossing midnight UTC) would never
        // collapse under a today-only scope. The global version skip below still
        // bounds cost — we only re-scan the window when the table has new commits.
        let today = Utc::now().date_naive();
        let lookback = self.config.maintenance.timefusion_dedup_lookback_days as i64;
        let dates: Vec<chrono::NaiveDate> = (0..=lookback).rev().map(|d| today - chrono::Duration::days(d)).collect();

        let pre_version = table_ref.read().await.version().unwrap_or(0);
        let needs_rollup_retry = self.config.maintenance.timefusion_rollup_enabled && get_schema(table_name).is_some_and(|schema| !schema.rollups.is_empty());
        if !needs_rollup_retry && self.last_dedup_versions.read().await.get(dedup_key).copied() == Some(pre_version) {
            debug!("dedup sweep: table={} version={} unchanged — skipping", table_name, pre_version);
            return Ok(());
        }

        let mut total_dropped = 0u64;
        let mut any_ok = false;
        // One (date, project) work list, so the deadline can cut the pass at any
        // point and the cursor can resume there. The old nested loops iterated a
        // HashSet, so the order was not even stable between ticks.
        let mut work: Vec<(chrono::NaiveDate, String, Vec<String>)> = Vec::new();
        for date in dates {
            let date_marker = format!("date={}", date);
            // Per-project live file lists for this date. Custom-project tables
            // don't embed project_id in the path; sweep "default".
            let files_by_pid: HashMap<String, Vec<String>> = {
                let table = table_ref.read().await;
                Self::partition_files_by_pid(&table, &date_marker)?
            };
            match files_by_pid.is_empty() {
                true => work.push((date, "default".to_string(), Vec::new())),
                false => work.extend(files_by_pid.into_iter().map(|(pid, files)| (date, pid, files))),
            }
        }
        // Stable order (newest date first), then rotate: a truncated tick must
        // not re-serve the same prefix on the next one (the starvation
        // `light_optimize_cursor` fixes for packing).
        work.sort_by(|(da, pa, _), (db, pb, _)| db.cmp(da).then_with(|| pa.cmp(pb)));
        let total_work = work.len();
        // Today never rotates out. Rotation exists so a deadline-truncated tick
        // resumes into UNSEEN sealed work, but today is re-dirtied by every
        // flush and is what the hot queries read, so it has to be swept on every
        // tick — not once per full rotation. That distinction did not matter
        // while the window was today+1 (two dates, nothing to starve); at a 35d
        // certification window the list is ~36 dates x projects, and first-time
        // passes over never-certified sealed days truncate ticks often, which is
        // exactly when today would be rotated past.
        let sealed_from = work.partition_point(|(date, _, _)| *date >= today);
        rotate_sealed_tail(&mut work, sealed_from, self.dedup_sweep_cursor.load(std::sync::atomic::Ordering::Relaxed));
        for (swept, (date, pid, cur_files)) in work.iter().enumerate() {
            let (date, pid) = (*date, pid);
            // Bail promptly on shutdown — a mid-sweep tick must not run
            // against a closing Foyer cache and hang the graceful drain.
            if self.maintenance_shutdown.is_cancelled() {
                debug!("dedup sweep: shutdown requested, aborting table={}", table_name);
                return Ok(());
            }
            if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                // Advance by the SEALED items covered, since only the sealed
                // tail rotates. Counting today's un-rotated prefix here would
                // walk the cursor a whole prefix-length per tick and skip sealed
                // work that was never swept.
                self.dedup_sweep_cursor.fetch_add(swept.saturating_sub(sealed_from), std::sync::atomic::Ordering::Relaxed);
                info!(table_name, swept, remaining = total_work - swept, event = "dedup_sweep_truncated");
                break;
            }
            // Incremental skip: a partition already certified clean whose live
            // file set is unchanged since that pass can't have gained dupes —
            // they only arrive in NEW files. Skip the whole-partition probe,
            // keeping the sweep O(partitions-changed). The version guard above
            // only fires when the WHOLE table is unchanged, which never holds
            // under continuous ingest; this per-partition check does (sealed
            // lookback days, and today between flushes).
            let fp_key = (pid.clone(), table_name.to_string(), date.to_string());
            let current_fp = partition_file_fp(cur_files.clone());
            if !cur_files.is_empty() && self.dedup_clean_fp.get(&fp_key).map(|entry| entry.value().fp) == Some(current_fp) {
                continue;
            }
            let backoff_key = format!("{dedup_key}:{pid}:{date}");
            if let Some(entry) = self.dedup_backoff.get(&backoff_key)
                && std::time::Instant::now() < entry.value().1
            {
                crate::observability::record_dedup_chunk_skipped();
                debug!("dedup sweep: {} in failure backoff, skipping", backoff_key);
                continue;
            }
            // BOUND the partition by what is left of the sweep, exactly as the
            // drain bounds each bin (`stage_deadline.min(remaining)`). The
            // deadline check above only gates ADMISSION: without this, a single
            // slow partition runs unbounded past it, and because
            // `spawn_cron_job` drops overlapping ticks that wedges the WHOLE
            // dedup job — including the dirty-bin drain, which runs before the
            // sweep and so never gets another tick.
            //
            // Prod 2026-08-15 measured exactly that: "Dedup job run still in
            // progress after 600s" while `dirty_bin_processed_total` sat frozen
            // at 27 for 30 minutes and 58 more bins were enqueued behind it.
            // Light compaction committed 47 bins in the same window, so the box
            // was not busy — only this job was stuck.
            //
            // A partition abandoned here is simply re-swept next tick: the pass
            // is idempotent and `record_certification` only certifies a pass
            // that ran to completion, so a truncated one certifies nothing.
            let partition_budget = deadline.map(|d| d.saturating_duration_since(std::time::Instant::now()));
            let swept = match partition_budget {
                Some(budget) => match tokio::time::timeout(budget, self.dedup_partition(table_ref, table_name, pid, date)).await {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("dedup of {pid}/{date} exceeded the sweep's remaining {budget:?}")),
                },
                None => self.dedup_partition(table_ref, table_name, pid, date).await,
            };
            match swept {
                Ok((d, complete)) => {
                    self.dedup_backoff.remove(&backoff_key);
                    total_dropped += d;
                    any_ok = true;
                    // Clean-partition fingerprint for the read-side dedup
                    // skip: a 0-drop pass over a file set that is STILL
                    // the live set proves the partition duplicate-free.
                    // Any concurrent commit (flush/compaction) changes
                    // the set → don't mark; a >0 pass marks nothing (the
                    // NEXT 0-drop pass confirms the rewrite held).
                    self.record_certification(table_ref, table_name, pid, date, cur_files, (d, complete)).await?;
                }
                Err(e) => {
                    // Exponential backoff, 10min doubling to a 6h cap —
                    // a failing partition must not re-run (and re-fail)
                    // on every 5-minute sweep tick.
                    let attempts = self.dedup_backoff.get(&backoff_key).map_or(0, |e| e.value().0) + 1;
                    let delay = std::time::Duration::from_secs((600u64 << (attempts.min(7) - 1)).min(21_600));
                    self.dedup_backoff.insert(backoff_key, (attempts, std::time::Instant::now() + delay));
                    if let Some((_, prev)) = self.dedup_clean_fp.remove(&fp_key) {
                        self.scan_metrics.record_cert_dwell(prev.since);
                    }
                    warn!(
                        "dedup sweep: project={} date={} table={} failed (attempt {}, next retry in {}s): {}",
                        pid,
                        date,
                        table_name,
                        attempts,
                        delay.as_secs(),
                        e
                    );
                }
            }
        }
        // Only refresh the skip cache when at least one partition ran cleanly,
        // so persistent failures don't silently suppress future sweeps.
        //
        // ...and never after a pass that REWROTE. `record_certification` requires
        // `dropped == 0` over an unmoved file set, so a rewriting pass certifies
        // nothing by design — the next 0-drop pass is what confirms the rewrite
        // held. But recording the version here is what stops that pass from ever
        // running: the guard at the top of this function returns immediately while
        // the version is unchanged, and the rewrite was the last thing to move it.
        // The confirming pass then waits on an unrelated commit, which in prod
        // arrives from other projects' ingest and on a quiet table may not arrive
        // at all — leaving exactly the partitions that HAD duplicates as the ones
        // that never get certified.
        //
        // Leaving the version unrecorded costs one extra sweep pass over a window
        // that was just rewritten, and that pass is what earns the certification.
        // TODO: same unbounded-growth caveat as `last_written_versions`.
        if any_ok && total_dropped == 0 {
            let post_version = table_ref.read().await.version().unwrap_or(pre_version);
            self.last_dedup_versions.write().await.insert(dedup_key.to_string(), post_version);
        }
        if any_ok {
            self.persist_certifications();
        }
        if total_dropped > 0 {
            info!("dedup sweep: table={} key={} total_dropped={}", table_name, dedup_key, total_dropped);
        }
        Ok(())
    }

    /// Snapshot `dedup_clean_fp` to the data dir. Called at the end of a sweep
    /// rather than per certification: one write per tick instead of one per
    /// partition, and the snapshot picks up invalidations in the same pass.
    ///
    /// Best-effort by design — this is a cache, and a lost write costs a cold
    /// start, never a wrong answer.
    /// Mirror of `persist_certifications` for the accumulating half of the
    /// evidence. Same flag, same lock (one writer at a time across both
    /// sidecars), same newest-irrelevant cap semantics — coverage entries are
    /// few (only days mid-accumulation) so the cap is a formality.
    fn persist_slice_coverage(&self) {
        if !self.config.maintenance.timefusion_dedup_certification_persist {
            return;
        }
        let _persist = self.dedup_certification_persist_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut entries: Vec<_> = self
            .dedup_slice_coverage
            .iter()
            .map(|entry| {
                let ((project_id, table_name, date), cov) = (entry.key().clone(), entry.value().clone());
                crate::storage::StoredSliceCoverage { project_id, table_name, date, fp: cov.fp, intervals: cov.intervals }
            })
            .collect();
        entries.truncate(crate::storage::PERSIST_CAP);
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::SLICE_COVERAGE, &entries);
    }

    fn persist_certifications(&self) {
        if !self.config.maintenance.timefusion_dedup_certification_persist {
            return;
        }
        let _persist = self.dedup_certification_persist_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now_ms = crate::storage::now_unix_ms();
        let mut entries: Vec<_> = self
            .dedup_clean_fp
            .iter()
            .map(|entry| {
                let ((project_id, table_name, date), cert) = (entry.key().clone(), entry.value().clone());
                crate::storage::StoredCertification {
                    project_id,
                    table_name,
                    date,
                    fp: cert.fp,
                    granted_unix_ms: now_ms.saturating_sub(cert.since.elapsed().as_millis() as u64),
                    files: cert.files.to_vec(),
                }
            })
            .collect();
        // Newest first, so the cap drops the oldest — which is also the likeliest
        // to have been invalidated by a write already.
        entries.sort_by(|a, b| b.granted_unix_ms.cmp(&a.granted_unix_ms));
        entries.truncate(crate::storage::PERSIST_CAP);
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::CERTIFICATIONS, &entries);
    }

    fn persist_dirty_bins(&self) {
        let mut bins: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .map(|entry| {
                let (project_id, table_name, date, bin) = entry.key();
                crate::storage::DirtyBin { project_id: project_id.clone(), table_name: table_name.clone(), date: date.clone(), bin: *bin }
            })
            .collect();
        bins.sort_by(|a, b| (&a.table_name, &a.project_id, &a.date, a.bin).cmp(&(&b.table_name, &b.project_id, &b.date, b.bin)));
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::DIRTY_BINS, &bins);
        crate::observability::maintenance_stats().dirty_bin_queue_depth.store(bins.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn enqueue_dirty_bin(&self, project_id: &str, table_name: &str, date: &str, bin: i64) {
        let key = (project_id.to_string(), table_name.to_string(), date.to_string(), bin);
        if self.dedup_dirty_bins.insert(key, ()).is_none() {
            crate::observability::maintenance_stats().dirty_bin_enqueued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(project_id, table_name, date, bin, event = "dirty_bin_enqueued");
            self.persist_dirty_bins();
        }
    }

    /// Is persistence healthy enough to spend the shared commit path on dedup?
    /// Reuses the compaction brake's signal (`is_wal_backlog_over_threshold`),
    /// which is true both when the unflushed backlog is over its threshold and
    /// while a recent flush FAILURE is inside the brake window.
    fn dedup_flush_healthy(&self) -> bool {
        // Memory brake included: the drain's chunk loop is the one heavy path
        // that never crosses a wave boundary, so the wave-level brake could
        // not stop it — prod 2026-07-30 04:31 OOM-killed at 112GB anon with
        // memory_brakes_total=0 while a drain pass rode RSS up unbraked.
        // `Repair` only to opt out of the packing-specific light-pool guard: the
        // dedup drain runs on the HEAVY pool, so a repair sort is not its rival.
        !self.buffered_layer().is_some_and(|layer| layer.is_wal_backlog_over_threshold()) && self.light_optimize_brake().is_none()
    }

    /// Order one drain pass and split off the work it will not do.
    ///
    /// Newest-first: recent partitions are the ones queries read, so dedup there pays
    /// immediately. Cold bins (owned by nightly consolidate, `date_is_cold`) go last and are
    /// never dropped: `consolidate_date_binned` is pure compaction and does not collapse
    /// duplicates, so this drain is their only physical dedup. They sink to lowest priority and
    /// are counted/summarised once per pass. Returns `(ready, deferred_cold)`; `deferred_cold`
    /// stays on the queue.
    pub(crate) fn select_drain_bins(mut candidates: Vec<DrainBin>, today: chrono::NaiveDate, after_days: u64, batch: usize) -> (Vec<DrainBin>, Vec<DrainBin>) {
        candidates.sort_by(|a, b| (&b.1, b.2).cmp(&(&a.1, a.2)));
        // An unparseable date sorts cold: it can't be shown to be hot, and the
        // staging call will surface the parse error when it is finally served.
        let (hot, mut cold): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, date, _)| chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok_and(|d| !Self::date_is_cold(today, d, after_days)));
        // Cold bins get a RESERVED share of the batch. Hot-first is right — a
        // boot that drained a 10-day backlog oldest-first never reached the hot
        // window (2026-07-30) — but giving hot the WHOLE batch starves cold
        // forever whenever hot work is continuous: prod 2026-08-02 sat at
        // queue=22135 with 20556 deferred cold and dirty_bin_processed=0, so the
        // backlog that keeps files duplicated never shrank at all. Reserving half
        // keeps hot's priority while making the cold backlog drain monotonically.
        let cold_reserve = cold.len().min(batch / 2);
        let mut ready: Vec<_> = hot.into_iter().take(batch.saturating_sub(cold_reserve)).collect();
        // Hot under-using its share hands the remainder back to cold.
        let deferred = cold.split_off(cold.len().min(batch.saturating_sub(ready.len())));
        ready.extend(cold);
        (ready, deferred)
    }

    pub(crate) async fn dedup_dirty_bins_for_table(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, flush_healthy: &(dyn Fn() -> bool + Sync), stage_deadline: std::time::Duration,
        pass_deadline: std::time::Instant,
    ) -> Result<()> {
        let schema = schema_or_default(table_name);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        // Dedup is an OPTIMIZATION — read-side DedupExec and flush-time dedup
        // already keep results correct — so it must never compete with the
        // persistence path for the per-table commit lock.
        if !flush_healthy() {
            crate::observability::maintenance_stats().dedup_passes_flush_yields.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(table_name, event = "dedup_drain_flush_yield");
            return Ok(());
        }
        const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
        // Eligible bins drained per table per tick. 8 couldn't keep up with the
        // enqueue rate (prod backlog 3341, 2026-07-20); 128 was sized for the
        // per-bin-probe cost model and drained a 22k backlog in ~a day. With
        // the batch probe, per-pass cost is ~one probe per (project, date)
        // GROUP plus staging for the dup-bearing minority (~3%), so a large
        // batch classifies a whole backlog in a handful of cheap probes — a
        // 25k queue spans only ~140 groups (2026-08-05). Clean bins are
        // consumed in the probe phase BEFORE the staging stream, so this cap
        // directly limits how much of the backlog each (long) pass can
        // retire — at 1024 a 20k backlog needed ~20 passes of 1-2h each.
        // 16384 admits any realistic backlog whole; per-pass cost stays
        // bounded by the GROUP count (probes) plus the dup-bearing minority
        // (staging, wave-committed incrementally under the flush gate).
        const DIRTY_BIN_DRAIN_BATCH: usize = 16384;
        // Per-shard byte budgets in `stage_dedup_chunk` bound Arrow
        // materialization; `stage_deadline` bounds each bin's WALL CLOCK (see
        // the call site in `run_dedup_for_table`).
        let sealed_before = (Utc::now() - chrono::Duration::hours(2)).timestamp_micros();
        // Today's SEALED bins are eligible. The old whole-partition
        // replace_where path had to skip today because it repeatedly planned
        // and rewrote a growing partition (579 rows dropped / 648s observed).
        // Staging is now restricted to snapshot-exact project/date files and
        // rewrites only files proven to hold the bin, while `sealed_before`
        // keeps it away from the live MemBuffer/late-arrival window. Deferring
        // all of today until tomorrow left recent dashboard queries doing >2x
        // merge-on-read work by construction.
        let today_date = Utc::now().date_naive();
        let candidates: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .filter_map(|entry| {
                let (project, name, date, bin) = entry.key();
                (name == table_name && (*bin + 1) * BIN_MICROS <= sealed_before).then(|| (project.clone(), date.clone(), *bin))
            })
            .collect();
        let (ready, deferred) = Self::select_drain_bins(
            candidates,
            today_date,
            self.config.parquet.cold_optimize_after_days(),
            // Fixed, not a knob: the env override this replaced
            // (TIMEFUSION_DIRTY_BIN_DRAIN_BATCH=1, a stale incident throttle
            // resurrected by every CapRover deploy) froze a 22k-bin backlog at
            // one bin per tick (2026-08-03). Deleted like the other drifted
            // memory knobs — the drain self-regulates via flush-health yields,
            // the memory brake and the rewrite semaphore, not operator envs.
            DIRTY_BIN_DRAIN_BATCH,
        );
        if !deferred.is_empty() {
            crate::observability::maintenance_stats().dedup_bins_deferred_cold.fetch_add(deferred.len() as u64, std::sync::atomic::Ordering::Relaxed);
            // ONE bounded summary per pass — a 10-day backlog is thousands of bins.
            info!(
                table_name,
                deferred = deferred.len(),
                oldest = deferred.last().map(|(_, date, _)| date.as_str()).unwrap_or_default(),
                event = "dedup_bins_deferred_cold"
            );
        }
        if ready.is_empty() {
            return Ok(());
        }
        // Phase 3 (2026-08-05): BATCH the probes. Every flushed bin is
        // enqueued, so most queued bins carry no duplicates at all (~97% in
        // prod: 601 probed clean vs 18 rewritten) — yet each paid its own
        // partition-restricted probe scan. One whole-date probe classifies
        // every queued bin of a (project, date) at once; only dup-bearing
        // bins continue into per-bin staging. Probe failure or timeout fails
        // OPEN to the per-bin path.
        let mut ready = if schema.dedup_keys.iter().any(|k| k == "timestamp") {
            // Bound the probe phase by what is LEFT of the PASS, not just by the
            // per-probe ceiling: a whole-date probe over a fragmented whale
            // partition can run for the ceiling's full hour, and the phase logs
            // only on completion, so one such probe silently consumes the tick
            // (and with it every later tick, since the pass holds
            // `maintenance_job_sem`). Failing open to the per-bin path is the
            // established behaviour for a probe that does not finish.
            //
            // An INSTANT, not a duration. A duration is a per-probe ceiling, and
            // probes run in waves of `rewrite_permits` — so 16 groups at 2
            // permits could each burn the whole remaining pass and take EIGHT
            // times the budget the caller meant to hand out. That is the
            // "Dedup job run still in progress after 600s (skips=5)" prod
            // 2026-08-13 was reporting: the phase outran its 5-minute tick and
            // every overlapping tick was dropped, so the queue only grew.
            // `checked_add`: `stage_deadline` is `Duration::MAX` wherever the
            // caller means "no per-probe ceiling", and that overflows an Instant.
            let probe_deadline = std::time::Instant::now().checked_add(stage_deadline).map_or(pass_deadline, |ceiling| pass_deadline.min(ceiling));
            self.batch_probe_classify(table, table_name, ready, probe_deadline).await
        } else {
            ready
        };
        if ready.is_empty() {
            self.persist_dirty_bins();
            return Ok(());
        }
        // STAGING admission is capped SEPARATELY from classification: staging
        // is the phase implicated in the never-attributed pass-scoped RSS
        // leak (~7GB/min over long passes — the 2026-08-03 OOM×4, mitigated
        // then by capping the whole batch at 128). Raising the batch to 1024
        // reintroduced multi-hour passes and the host went down under memory
        // pressure on 2026-08-06 ~10:25Z (load ~400, SSH unresponsive).
        // Short passes keep whatever leaks pass-scoped — it frees at pass
        // end. Overflow goes straight back on the queue for the next tick.
        const DIRTY_BIN_STAGE_BATCH: usize = 64;
        // Recent dates get a small RESERVED quota — a dup-bearing bin on
        // today's partition taxes every live query until it's rewritten — but
        // must not monopolize the pass: hot-partition rewrites are the
        // slowest (10-23 min each on 2026-08-06), and giving them all 64
        // slots stalled the sealed backlog flat. The remaining slots go to
        // the OLDEST bins so the backlog keeps draining.
        const DIRTY_BIN_STAGE_RECENT_SLOTS: usize = 16;
        ready.sort_by(|(_, da, ba), (_, db, bb)| db.cmp(da).then(bb.cmp(ba)));
        if ready.len() > DIRTY_BIN_STAGE_BATCH {
            let mut rest = ready.split_off(DIRTY_BIN_STAGE_RECENT_SLOTS);
            rest.reverse(); // oldest-first for the backlog share
            for (project, date, bin) in rest.split_off(DIRTY_BIN_STAGE_BATCH - ready.len()) {
                self.dedup_dirty_bins.insert((project, table_name.to_string(), date, bin), ());
            }
            ready.extend(rest);
        }
        // Phase 2 (2026-07-29): bins STAGE in parallel and commit in WAVES.
        // Previously each bin rewrote and committed strictly one at a time —
        // serialization was deliberate, because concurrent per-bin commits to
        // one Delta log were an OCC storm — and a drain took up to 572s. Batched
        // commits remove that reason, so the only remaining bound on rewrite
        // parallelism is memory: `stage_dedup_chunk` takes a
        // `maintenance_rewrite_sem` permit around its (pool-invisible) Arrow
        // materialization, and `buffer_unordered(permits)` keeps in-flight
        // staging matched to it rather than unbounded.
        //
        // A bounded stream, not the hot path's `round_robin_bins` driver: the
        // dirty queue is already fair (FIFO, capped per tick by
        // `dirty_bin_drain_batch`), each bin is served exactly once per tick,
        // and there is no per-round re-plan — the driver's rotation/round
        // semantics would add ceremony with nothing to schedule.
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        // A wave's units all sit in memory as Delta actions only (their parquet
        // is already in R2), so the cap is about commit size, not memory.
        const DEDUP_WAVE_UNITS: usize = 8;
        let mut staging = futures::stream::iter(ready.into_iter().map(|(project_id, date, bin)| async move {
            let key: DirtyBinKey = (project_id.clone(), table_name.to_string(), date.clone(), bin);
            // STOP ADMITTING once the pass is out of budget, and bound each bin
            // by what is left rather than by `stage_deadline` alone.
            //
            // Staging used to consult neither: 64 bins each free to run for the
            // 3600s per-bin ceiling, inside a tick whose whole budget is ~240s.
            // The pass holds `maintenance_job_sem` and `spawn_cron_job` drops
            // overlapping ticks, so one over-running pass costs every tick
            // behind it AND every other maintenance job — prod 2026-08-14 logged
            // "Dedup job run still in progress after 600s" on every 5-minute
            // tick for 45 minutes straight, alongside the same warning for hot
            // compaction and the rollup backfill queued behind it.
            //
            // Bailing here leaves the bin QUEUED (the dequeue below has not run
            // yet), so an unadmitted bin is simply served by the next tick.
            let remaining = pass_deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            // No persist here: rewriting the whole multi-MB queue file per
            // dequeue made the drain O(queue x batch) in fsync I/O. Crash
            // direction is safe — an unpersisted dequeue reappears after
            // restart and re-dedups (idempotent). End-of-pass persists.
            self.dedup_dirty_bins.remove(&key);
            crate::observability::maintenance_stats().dirty_bin_eligible.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(project_id, table_name, date, bin, event = "dirty_bin_dequeued");
            let started = std::time::Instant::now();
            let staged = match chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d") {
                // Timing out a bin discards its staged work (any uploaded
                // parquet is uncommitted and falls to VACUUM) and retries it
                // next pass — acceptable now that per-shard byte budgets keep
                // legitimate bins to minutes, after an UNBOUNDED staging read
                // wedged the whole drain for 6.5h behind the 1-permit
                // maintenance semaphore (prod 2026-08-05). The Err lands in
                // the ordinary failure arm below: requeue + warn.
                Ok(parsed) => {
                    let bin_deadline = stage_deadline.min(remaining);
                    match tokio::time::timeout(
                        bin_deadline,
                        self.stage_dedup_partition_range(
                            table,
                            table_name,
                            &project_id,
                            parsed,
                            DedupRangeOptions {
                                slice: Some(crate::maintenance_coordinator::TimeSlice {
                                    start_micros: bin.saturating_mul(crate::maintenance_coordinator::NORMAL_SLICE_MICROS),
                                    end_micros: bin.saturating_add(1).saturating_mul(crate::maintenance_coordinator::NORMAL_SLICE_MICROS),
                                }),
                                dirty_key: Some(key.clone()),
                                limits: None,
                            },
                        ),
                    )
                    .await
                    {
                        Ok(staged) => staged,
                        Err(_) => {
                            crate::observability::maintenance_stats().dedup_bin_stage_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Err(anyhow::anyhow!("staging exceeded the {bin_deadline:?} deadline (hung object-store read, or the pass ran out of budget)"))
                        }
                    }
                }
                Err(e) => Err(anyhow::anyhow!("invalid dirty-bin date {date}: {e}")),
            };
            Some((key, started.elapsed(), staged))
        }))
        .buffer_unordered(permits);

        let mut wave: Vec<StagedBin> = Vec::new();
        let requeue = |key: DirtyBinKey, counter: &std::sync::atomic::AtomicU64| {
            self.dedup_dirty_bins.insert(key, ());
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        };
        // Once the wave gate gives up on flush recovery, stop committing but
        // KEEP DRAINING the stream — an in-flight staging future has already
        // removed its key from the queue and dropping it would lose the bin.
        let mut committing = true;
        while let Some(admitted) = staging.next().await {
            // `None` = the pass ran out of budget before this bin was admitted;
            // it is still queued, so the next tick serves it.
            let Some((key, elapsed, staged)) = admitted else { continue };
            let stats = crate::observability::maintenance_stats();
            let (project_id, _, date, bin) = key.clone();
            if !committing {
                requeue(key, &stats.dirty_bin_requeued);
                continue;
            }
            match staged {
                Err(error) => {
                    requeue(key, &stats.dirty_bin_requeued);
                    warn!(project_id, table_name, date, bin, %error, event = "dirty_bin_failure");
                    continue;
                }
                Ok((units, complete)) => {
                    stats.dirty_bin_rewrite_duration_ms.fetch_add(elapsed.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    // Duplicate-bearing work was skipped inside the bin (unsealed
                    // chunk, unshardable key group): the bin is NOT done, so it
                    // goes back on the queue even if its other chunks land.
                    if !complete {
                        requeue(key, &stats.dirty_bin_requeued);
                        warn!(project_id, table_name, date, bin, event = "dirty_bin_requeued");
                    } else if units.is_empty() {
                        // A bin with nothing to rewrite (already compacted /
                        // no duplicates) never enters a wave, so count its
                        // drain here or the processed metric reads 0 while
                        // the queue empties.
                        stats.dirty_bin_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    wave.extend(units);
                }
            }
            if wave.len() >= DEDUP_WAVE_UNITS {
                committing = self.commit_dedup_wave_when_flush_healthy(table, table_name, &mut wave, flush_healthy, &requeue).await;
            }
        }
        if !wave.is_empty() {
            self.commit_dedup_wave_when_flush_healthy(table, table_name, &mut wave, flush_healthy, &requeue).await;
        }
        self.persist_dirty_bins();
        Ok(())
    }

    /// Runs the batch probe over each (project, date) with ≥2 queued bins and
    /// strips the probe-clean bins out of `ready`, consuming them. Group keys
    /// are dequeued BEFORE the probe so dirtiness enqueued while it runs
    /// re-queues the bin (the same ordering the per-bin path relies on). A
    /// singleton keeps the per-bin path — its bin-scoped probe prunes to ten
    /// minutes of files where the whole-date probe scans them all.
    pub(crate) async fn batch_probe_classify(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, ready: Vec<(String, String, i64)>, deadline: std::time::Instant,
    ) -> Vec<(String, String, i64)> {
        use std::sync::atomic::Ordering::Relaxed;
        let mut groups: HashMap<(String, String), Vec<i64>> = Default::default();
        for (project, date, bin) in &ready {
            groups.entry((project.clone(), date.clone())).or_default().push(*bin);
        }
        // Probes run CONCURRENTLY (they were sequential until 2026-08-06 —
        // ~140 groups × seconds-to-a-minute each serialized the whole
        // classification phase). Bounded by the rewrite permits like every
        // other maintenance scan. Probe RESULTS are key-only aggregates, but
        // each probe builds a provider + eager snapshot over a whole date's
        // files — allocator churn jemalloc retains as RSS — so groups per
        // pass are capped too, largest first (most clean bins retired per
        // provider built). The rest keep their queue entries and classify on
        // later ticks.
        const BATCH_PROBE_GROUPS: usize = 16;
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        let mut groups: Vec<((String, String), Vec<i64>)> =
            groups.into_iter().filter(|((_, date), bins)| bins.len() >= 2 && chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok()).collect();
        // Recent dates first (queries hit them, and dup-bearing bins there
        // force read-side DedupExec on every query — 2026-08-06: a 0.2% dup
        // fraction cost 6297304f a 10x latency penalty); largest group as
        // the tiebreak so each provider built still retires the most bins.
        groups.sort_by(|((_, da), a), ((_, db), b)| db.cmp(da).then(b.len().cmp(&a.len())));
        groups.truncate(BATCH_PROBE_GROUPS);
        // BEFORE the probes, not only after each one. Every other line in this
        // phase is emitted on completion, so a phase that does not complete
        // prints nothing at all — which is how prod 2026-08-12 spent 55 minutes
        // in a 5-minute dedup tick with no log between its first and last line.
        info!(
            table_name,
            groups = groups.len(),
            budget_secs = deadline.saturating_duration_since(std::time::Instant::now()).as_secs(),
            event = "dedup_batch_probe_start"
        );
        let clean: HashSet<(String, String, i64)> = futures::stream::iter(groups.into_iter().map(|((project, date), bins)| async move {
            // What is left of the PHASE at the moment this probe starts, so the
            // waves behind the permit limit share one budget instead of each
            // claiming it whole. Zero left means this group was never examined:
            // leave its bins queued rather than dequeuing them for a probe that
            // cannot run, so a later tick can still classify them cheaply.
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Vec::new();
            }
            for bin in &bins {
                self.dedup_dirty_bins.remove(&(project.clone(), table_name.to_string(), date.clone(), *bin));
            }
            match tokio::time::timeout(remaining, self.probe_dup_bins(table, table_name, &project, &date)).await {
                Ok(Ok(dup_bins)) => {
                    let stats = crate::observability::maintenance_stats();
                    let cleared: Vec<_> = bins.iter().filter(|b| !dup_bins.contains(b)).map(|b| (project.clone(), date.clone(), *b)).collect();
                    stats.dirty_bin_processed.fetch_add(cleared.len() as u64, Relaxed);
                    stats.dirty_bin_batch_probe_clean.fetch_add(cleared.len() as u64, Relaxed);
                    info!(project, table_name, date, queued = bins.len(), clean = cleared.len(), event = "dedup_batch_probe");
                    cleared
                }
                Ok(Err(error)) => {
                    warn!(project, table_name, date, %error, event = "dedup_batch_probe_failure");
                    Vec::new()
                }
                Err(_) => {
                    crate::observability::maintenance_stats().dedup_bin_stage_timeouts.fetch_add(1, Relaxed);
                    warn!(project, table_name, date, event = "dedup_batch_probe_timeout");
                    Vec::new()
                }
            }
        }))
        .buffer_unordered(permits)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .collect();
        ready.into_iter().filter(|b| !clean.contains(b)).collect()
    }

    /// Wave-commit gate: waits (bounded) for flush to recover before committing a dedup wave.
    ///
    /// A pass can outlive its start-of-pass health check, and dedup must not compete with persistence
    /// for the commit lock. One transient unhealthy sample must not forfeit the whole batch. Latching
    /// used to requeue every remaining bin after a single bad sample, silently discarding an entire
    /// pass. If flush does not recover, requeue the wave and return false so the pass stops committing.
    async fn commit_dedup_wave_when_flush_healthy(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, wave: &mut Vec<StagedBin>, flush_healthy: &(dyn Fn() -> bool + Sync),
        requeue: &(dyn Fn(DirtyBinKey, &std::sync::atomic::AtomicU64) + Sync),
    ) -> bool {
        const FLUSH_RECOVERY_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
        let t0 = std::time::Instant::now();
        while !flush_healthy() {
            if t0.elapsed() >= FLUSH_RECOVERY_WAIT {
                let stats = crate::observability::maintenance_stats();
                stats.dedup_passes_flush_yields.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(table_name, requeued = wave.len(), event = "dedup_drain_flush_yield");
                for key in wave.drain(..).filter_map(|unit| unit.dedup.as_ref().and_then(|d| d.key.clone())) {
                    requeue(key, &stats.dirty_bin_requeued);
                }
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        self.commit_dedup_wave(table, table_name, std::mem::take(wave)).await;
        true
    }

    /// Commit one dedup wave and settle its units' dirty-bin bookkeeping: a unit
    /// that didn't land (stale target, failed/unconfirmed commit) puts its bin
    /// back on the queue, because its duplicates are still in the table.
    async fn commit_dedup_wave(&self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, units: Vec<StagedBin>) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut markers: Vec<String> = units.iter().filter_map(|u| u.dedup.as_ref()).map(|d| format!("date={}/", d.date)).collect();
        markers.sort();
        markers.dedup();
        let result = self.commit_wave(table, table_name, &markers, true, units, 0).await;
        let stats = crate::observability::maintenance_stats();
        let mut landed_bins: HashSet<DirtyBinKey> = result
            .landed
            .iter()
            .filter_map(|unit| {
                let d = unit.dedup.as_ref()?;
                info!(table_name, chunk = d.label, dropped = d.dropped(), before = d.before, after = d.after, event = "dirty_bin_chunk_complete");
                stats.dirty_bin_dropped_rows.fetch_add(d.dropped(), Relaxed);
                d.key.clone()
            })
            .collect();
        for unit in &result.failed {
            let Some(key) = unit.dedup.as_ref().and_then(|d| d.key.clone()) else { continue };
            landed_bins.remove(&key);
            let (project_id, _, date, bin) = key.clone();
            self.dedup_dirty_bins.insert(key, ());
            stats.dirty_bin_requeued.fetch_add(1, Relaxed);
            warn!(project_id, table_name, date, bin, event = "dirty_bin_requeued");
        }
        stats.dirty_bin_processed.fetch_add(landed_bins.len() as u64, Relaxed);
        self.persist_dirty_bins();
    }

    /// One table's dedup of sealed partitions (dirty-bin rewrite + optional
    /// fallback sweep). The 90s deadline is a warning threshold, not a
    /// cancellation: a slow-but-healthy table is allowed to finish.
    pub(crate) async fn run_dedup_for_table(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str, label: &str, drain_deadline: std::time::Instant,
        sweep_deadline: std::time::Instant,
    ) {
        if !self.config.maintenance.timefusion_dirty_bin_dedup_enabled {
            debug!(table_name, event = "dirty_bin_dedup_paused", "physical dirty-bin dedup is disabled; read-side dedup remains active");
            return;
        }
        const DEDUP_WARN: std::time::Duration = std::time::Duration::from_secs(90);
        let t0 = std::time::Instant::now();
        // Deadline per bin STAGING attempt, not per pass. It must clear the
        // WORST legitimate bin, not the typical one: a dup-bearing bin in a
        // fragmented partition re-reads multi-bin files (~3.4M rows/chunk
        // observed) and shares the rewrite sem with light-optimize's
        // hour-long passes — at 900s, 36 such bins (6297304f/08-03,
        // 2026-08-06) timed out on EVERY pass, an infinite requeue loop that
        // dragged each pass by hours while the bins never finished. 3600s
        // still bounds the hung-read wedge this exists for (was 6.5h).
        const DEDUP_BIN_STAGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(3600);
        match self.dedup_dirty_bins_for_table(table, table_name, &|| self.dedup_flush_healthy(), DEDUP_BIN_STAGE_DEADLINE, drain_deadline).await {
            Ok(()) if t0.elapsed() > DEDUP_WARN => {
                warn!("Dirty-bin dedup for {label} took {:?} (exceeds {DEDUP_WARN:?} warning threshold)", t0.elapsed());
                crate::observability::maintenance_stats().dedup_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(()) => {}
            Err(e) => {
                crate::observability::maintenance_stats().dedup_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("Dirty-bin dedup failed for {label}: {e}");
            }
        }
        if self.config.maintenance.timefusion_dedup_sweep_fallback {
            let t0 = std::time::Instant::now();
            // The sweep is the pass's unbounded half (see `dedup_sweep`); the
            // drain above is bounded per bin and is the work worth finishing.
            match self.dedup_sweep(table, table_name, dedup_key, Some(sweep_deadline)).await {
                Ok(()) if t0.elapsed() > DEDUP_WARN => {
                    warn!("Dedup fallback sweep for {label} took {:?} (exceeds {DEDUP_WARN:?} warning threshold)", t0.elapsed());
                    crate::observability::maintenance_stats().dedup_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(()) => {}
                Err(e) => {
                    crate::observability::maintenance_stats().dedup_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error!("Dedup fallback sweep failed for {label}: {e}");
                }
            }
        }
    }

    /// Run `pass` over every table under one shared tick deadline, rotating the starting table.
    ///
    /// Each table used to derive its own full budget, so a sweep over many tables could run
    /// multiples of the schedule's budget and hold the maintenance semaphore without yielding.
    /// Sharing one deadline prevents overruns; rotating prevents the head of the list from
    /// starving the tail every tick.
    pub(crate) async fn run_hot_compact_sweep(&self, pass: TailPass) {
        use std::sync::atomic::Ordering::Relaxed;
        let deadline = std::time::Instant::now() + self.tail_pass_tick_budget(pass);
        let mut tables = self.all_tables().await;
        let start = sweep_resume_offset(tables.len(), self.hot_compact_table_cursor.fetch_add(1, Relaxed));
        tables.rotate_left(start);
        for (project_id, table_name, table) in tables {
            if self.maintenance_shutdown.is_cancelled() {
                return;
            }
            // A table reached with no budget left can only plan a bin it cannot
            // finish. Leave it for the next tick, which rotation puts first.
            if std::time::Instant::now() >= deadline {
                info!(?pass, event = "hot_compact_tick_budget_exhausted");
                return;
            }
            self.run_hot_compact_for_table(&table, &table_name, &Self::table_label(&project_id, &table_name), pass, Some(deadline)).await;
        }
    }

    async fn run_hot_compact_for_table(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, label: &str, pass: TailPass, tick_deadline: Option<std::time::Instant>,
    ) {
        use std::sync::atomic::Ordering::Relaxed;
        const OPTIMIZE_WARN: std::time::Duration = std::time::Duration::from_secs(180);
        let t0 = std::time::Instant::now();
        let m = crate::observability::maintenance_stats();
        // Per-tick counter deltas in every outcome line — tick health (planned
        // vs completed, bins landed, brakes hit) must be readable from one log
        // line, not reconstructed from counter scrapes (2026-08-05 review:
        // brake-starved ticks were invisible until SSH + grep).
        let snap = || {
            [
                m.light_optimize_projects_planned.load(Relaxed),
                m.light_optimize_projects_completed.load(Relaxed),
                m.light_optimize_bins_committed.load(Relaxed),
                m.light_optimize_memory_brakes.load(Relaxed),
            ]
        };
        let before = snap();
        let result = self.optimize_table_light_until(table, table_name, pass, tick_deadline).await;
        let after = snap();
        let [planned, completed, bins, brakes] = std::array::from_fn(|i| after[i] - before[i]);
        let elapsed = t0.elapsed();
        match result {
            Ok(()) if elapsed > OPTIMIZE_WARN => {
                warn!(
                    "Light optimize for {label} took {elapsed:?} (exceeds {OPTIMIZE_WARN:?} threshold): planned={planned} completed={completed} bins={bins} brakes={brakes}"
                );
                m.light_optimize_timed_out.fetch_add(1, Relaxed);
            }
            Ok(()) => info!("Light optimize completed for {label} in {elapsed:?}: planned={planned} completed={completed} bins={bins} brakes={brakes}"),
            // PARTIAL is not FAILED. `optimize_table_light` returns Err if ANY
            // bin failed, so a tick that compacted 6 of 9 projects logged at
            // ERROR identically to one that compacted nothing. That cost real
            // diagnosis time on 2026-08-08: the otel_metrics recovery from
            // `completed=0` to `completed=6` was invisible, because both states
            // printed the same red line. Reserve ERROR for "this tick achieved
            // nothing", which is the condition actually worth paging on.
            Err(e) if bins > 0 => {
                m.light_optimize_failed.fetch_add(1, Relaxed);
                warn!("Light optimize PARTIAL for {label} in {elapsed:?} (planned={planned} completed={completed} bins={bins} brakes={brakes}): {e}");
            }
            Err(e) => {
                m.light_optimize_failed.fetch_add(1, Relaxed);
                error!("Light optimize failed for {label} in {elapsed:?} (planned={planned} completed={completed} bins={bins} brakes={brakes}): {e}");
            }
        }
    }

    /// Sealed dates a REPAIR pass scans for footer repair (yesterday backwards).
    /// A packing pass carries none: the two passes own disjoint partitions.
    fn repair_dates(&self, today: chrono::NaiveDate, pass: TailPass) -> Vec<String> {
        match pass {
            TailPass::Pack => vec![],
            TailPass::Repair => (1..=self.config.maintenance.timefusion_light_optimize_repair_days)
                .filter_map(|d| today.checked_sub_days(chrono::Days::new(d)))
                .map(|d| d.to_string())
                .collect(),
        }
    }

    /// The admission policy for one tail pass. `budget` is an input, not a
    /// detail: a repair pass's reach is derived from how long it may run.
    fn tail_pass_policy<'a>(&'a self, pass: TailPass, budget: std::time::Duration, repair_dates: &'a [String]) -> HotBinPolicy<'a> {
        // A packing bin is only worth assembling if the tick can finish it —
        // see `pack_target_bytes`. Repair takes ONE file and bounds it with
        // `repair_max_bytes` below, so its target stays the configured value
        // (there it only sets the converged/sorted-run thresholds).
        let target_size = match pass {
            TailPass::Pack => pack_target_bytes(self.config.maintenance.timefusion_light_optimize_target_size, budget),
            TailPass::Repair => self.config.maintenance.timefusion_light_optimize_target_size,
        };
        HotBinPolicy {
            repair_dates,
            target_size,
            min_files: self.config.maintenance.timefusion_compact_min_files,
            sorted_run_cap: target_size / 2,
            // On a REPAIR pass the reach is what the budget can actually finish,
            // not the hot-tick knob. `timefusion_repair_max_file_bytes` was sized
            // to stop a FIVE-MINUTE tick dragging in a multi-GB file; repair now
            // owns a 144-minute pass, so applying that number there refuses work
            // it has ample room for — permanently, since a file the pass will not
            // admit is never a candidate at any budget or cadence.
            //
            // Prod 2026-08-08, the cost of getting this wrong: shipbubble's
            // `date=2026-07-30` is pinned by a **1,088,634,971-byte** file, 14 MB
            // over the 1 GiB prod setting. Its 30-day queries could not be fixed
            // by ANY amount of budget, concurrency or cadence tuning, because
            // `hot_bin_admits` never even considered the file.
            repair_max_bytes: match pass {
                TailPass::Pack => self.config.maintenance.timefusion_repair_max_file_bytes as i64,
                TailPass::Repair => repair_reach_bytes(self.config.maintenance.timefusion_repair_max_file_bytes as i64, budget),
            },
            pass,
            verified_sorted: &self.repair_verified_sorted,
            failures: &self.repair_failures,
        }
    }

    /// Plan one tail pass: which projects have work and which files each bin takes.
    ///
    /// Split out so tests can reproduce the exact bin the next real pass will select (resume is
    /// keyed on input-set equality). Drop verified-sorted suspects and re-select until what
    /// remains is real repair work. Load-bearing at both call sites: admission offers every
    /// un-verified sealed file as a suspect — the sorted-run tag can lie, so only the footer
    /// decides. A project's selected bin is therefore often a correctly sorted file; verify and
    /// re-select, or the project drops out of the wave engine's pending set for the whole
    /// pass.
    async fn reselect_until_real_work(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, schema: &crate::schema::TableSchema, today_str: &str, policy: &HotBinPolicy<'_>,
        mut planned: Vec<(String, Vec<String>)>,
    ) -> Result<Vec<(String, Vec<String>)>> {
        for _ in 0..REPAIR_RESELECT_ROUNDS {
            let before = planned.len();
            planned = self.drop_verified_sorted_bins(table_ref, table_name, policy.pass, planned).await;
            if policy.pass != TailPass::Repair || planned.len() == before {
                break;
            }
            let next = {
                let table = table_ref.read().await;
                Self::select_all_hot_bins(&table, schema, today_str, policy)?
            };
            if next.is_empty() {
                break;
            }
            planned = next;
        }
        Ok(planned)
    }

    async fn plan_tail_pass(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, today_str: &str, policy: &HotBinPolicy<'_>,
    ) -> Result<Vec<(String, Vec<String>)>> {
        use std::sync::atomic::Ordering::Relaxed;
        let schema = schema_or_default(table_name);
        // Plan ONCE for round 0; later rounds re-plan from the post-commit
        // snapshot so a wave never re-selects the run it just wrote. Bins are
        // ordered by compaction debt.
        let mut planned = {
            let table = table_ref.read().await;
            Self::select_all_hot_bins(&table, schema, today_str, policy)?
        };
        // Rotation cursor: start where the last truncated tick stopped so the
        // same tail is never skipped twice in a row (a truncated tick otherwise
        // always serves the same debt-ordered prefix).
        let cursor = self.light_optimize_cursor.swap(0, Relaxed);
        if cursor > 0 && cursor < planned.len() {
            planned.rotate_left(cursor);
        }
        // Clearing a project's suspect must not cost it the tick. A repair pass
        // selects ONE file per project, and admission offers every un-verified
        // sealed file, so the selected one is usually a correctly sorted file
        // nothing has read yet. Verifying it cleared the project and dropped it
        // for the rest of the tick — one file of progress per TICK, while the
        // pass reported converging. Prod 2026-08-08 logged `cleared=20
        // remaining=1` on every hourly firing with none of the poisoned files
        // changing; 6297304f's `date=2026-07-16` holds 142 objects of which 12
        // are unsorted, i.e. ~142 hours to reach them at that rate.
        //
        // So re-plan after each clear and keep going. `repair_verified_sorted`
        // makes every re-plan skip what was just checked, so a tick walks a
        // project's candidates until it finds real work or runs out — within one
        // tick instead of one per tick.
        let mut planned = self.reselect_until_real_work(table_ref, table_name, schema, today_str, policy, planned).await?;
        // Serve the most RECENT poison first, across projects.
        //
        // By this point the suspects are gone and every remaining bin is real
        // work, so its date is known — and that date is the only thing that
        // says how much damage the file is doing. One footer-less file voids
        // the scan ordering for EVERY query window that reaches it, so a file
        // on 2026-07-30 breaks 14- and 30-day queries while one on 2026-05-30
        // breaks only queries reaching back 70+ days, which almost nobody runs.
        //
        // Ordering by candidate COUNT (shortest-job-first, which is right for
        // finishing projects) ignores that entirely. Prod 2026-08-09, measured
        // with the `selected=` log: a 30-minute uninterrupted pass served
        // 2026-05-30, 05-31, 06-09 and 07-10 — every one of them older, and
        // less user-visible, than the 2026-07-30 file that was the reason two
        // tenants could not query 14 or 30 days.
        if policy.pass == TailPass::Repair {
            planned.sort_by(|a, b| repair_bin_date(&b.1).cmp(repair_bin_date(&a.1)));
        }
        Ok(planned)
    }

    /// TEST SEAM: plan one tail pass, stage its first bin, and ABANDON it —
    /// staged parquet on the object store plus an intent line on disk, and no
    /// commit. That is the state a process killed mid-rewrite leaves behind,
    /// and going through the real planner is the point: resume matches on
    /// input-set equality, so the bin must be the one the next pass re-selects.
    /// Returns the abandoned bin's `(project_id, input paths)`.
    #[doc(hidden)]
    pub async fn stage_and_abandon_first_bin(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, pass: TailPass,
    ) -> Result<Option<(String, Vec<String>)>> {
        let today = chrono::DateTime::from_timestamp_micros(crate::support::now_micros()).map(|d| d.date_naive()).unwrap_or_else(|| Utc::now().date_naive());
        let repair_dates = self.repair_dates(today, pass);
        let policy = self.tail_pass_policy(pass, self.tail_pass_tick_budget(pass), &repair_dates);
        let planned = self.plan_tail_pass(table_ref, table_name, &today.to_string(), &policy).await?;
        let Some((project_id, files)) = planned.into_iter().next() else { return Ok(None) };
        let schema = schema_or_default(table_name);
        match self.stage_hot_bin(table_ref, table_name, schema, &project_id, files.clone(), HotStageOptions { pass, runtime_env: None }).await? {
            BinOutcome::Staged(_) => Ok(Some((project_id, files))),
            _ => Ok(None),
        }
    }

    /// Hot-tail compaction for one table: plan-once, rewrite-parallel, commit-once waves.
    ///
    /// One tag-first metadata walk plans a bin for every hot project (`select_all_hot_bins`), each
    /// round's bins are rewritten to staged parquet in parallel, and the whole round lands in one
    /// `CommitBuilder` transaction. This replaces the per-bin `OptimizeBuilder` path, which spent
    /// most of its time in OCC retries and left many hot projects unreached. The environment kill
    /// switch remains available.
    pub async fn optimize_table_light(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, pass: TailPass) -> Result<()> {
        self.optimize_table_light_until(table_ref, table_name, pass, None).await
    }

    /// `tick_deadline` caps this table's wall clock at what is left of the
    /// TICK, rather than granting it a fresh per-table budget. `None` keeps the
    /// nominal per-pass budget, which is what a direct caller (a test, a manual
    /// invocation) wants.
    pub async fn optimize_table_light_until(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, pass: TailPass, tick_deadline: Option<std::time::Instant>,
    ) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        // `crate::support`, not `Utc::now()`: the hot tail scopes itself to TODAY's
        // partition and to an event-time seal window, so a wall-clock read here
        // makes the whole pass unreachable from the virtual-time e2e harness —
        // which is why this path had no end-to-end coverage and shipped writing
        // every output unsorted. In production the clock IS the wall clock.
        // Take the process-wide repair permit FIRST — before planning, not just
        // before staging. Two tables' repair passes otherwise duplicate the
        // suspect walk and then both stage into one shared light pool. Held for
        // the pass and released on every exit path; `try_acquire` rather than
        // `acquire` so the loser SKIPS its tick instead of queueing behind a
        // 144-minute budget. See `repair_pass_permit`.
        let _repair_permit = match pass {
            TailPass::Pack => None,
            TailPass::Repair => match Arc::clone(&self.repair_pass_permit).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    crate::observability::maintenance_stats().repair_ticks_yielded.fetch_add(1, Relaxed);
                    info!(table_name, event = "repair_pass_yielded_to_another_table");
                    return Ok(());
                }
            },
        };
        let today = chrono::DateTime::from_timestamp_micros(crate::support::now_micros()).map(|d| d.date_naive()).unwrap_or_else(|| Utc::now().date_naive());
        let today_str = today.to_string();
        let repair_dates = self.repair_dates(today, pass);
        let budget = self.tail_pass_tick_budget(pass);
        let policy = self.tail_pass_policy(pass, budget, &repair_dates);
        let schema = schema_or_default(table_name);
        let planned = self.plan_tail_pass(table_ref, table_name, &today_str, &policy).await?;
        if planned.is_empty() {
            return Ok(());
        }
        crate::observability::maintenance_stats().light_optimize_projects_planned.fetch_add(planned.len() as u64, Relaxed);
        info!(table_name, date = %today, ?pass, projects = planned.len(), event = "light_optimize_planned");
        let project_ids: Vec<String> = planned.iter().map(|(project_id, _)| project_id.clone()).collect();
        // Bins the current wave should stage, replaced wholesale by each wave's
        // post-commit re-plan. A project absent from the map has no work left
        // this tick and drops out of the round-robin.
        let plan: tokio::sync::Mutex<HashMap<String, Vec<String>>> = tokio::sync::Mutex::new(planned.into_iter().collect());

        // COMMIT RECOVERED REWRITES FIRST, outside the round.
        //
        // A resumed bin is already staged and already verified row-exact — it
        // needs nothing but a commit. Committing it inside the round means
        // waiting for every sibling to finish staging, because
        // `round_robin_bins` awaits the whole round's
        // `buffer_unordered(..).collect()` before it commits anything; even
        // `commit_each_bin` only splits what that collect produced.
        //
        // Prod 2026-08-09 measured the consequence: 6 `staged_intent_resumed`
        // against ZERO matching `wave_commit_enter`, the resumes landing at
        // 13:46:55 with no `round_staged` after them at all. Process lifetime
        // under deploy churn is shorter than a round of minutes-long bins, so
        // the recovered rewrite died with the round every single pass —
        // resume kept finding the same finished work and the round kept losing
        // it. shipbubble's `date=2026-07-30` sat that way for days with a
        // complete, sorted, row-exact replacement already on S3.
        let resumed: Vec<(String, StagedBin)> = {
            let plan_guard = plan.lock().await;
            let mut found = Vec::new();
            for (project_id, files) in plan_guard.iter() {
                if let Some(bin) = self.resumable_staged_bin(table_ref, table_name, project_id, files).await {
                    found.push((project_id.clone(), bin));
                }
            }
            found
        };
        for (project_id, bin) in resumed {
            // Drop it from the plan: its inputs are about to stop being live, so
            // re-staging them this tick would rewrite files the commit removed.
            plan.lock().await.remove(&project_id);
            let markers: Vec<String> = match pass {
                TailPass::Pack => vec![format!("date={today_str}/")],
                TailPass::Repair => policy.repair_dates.iter().map(|d| format!("date={d}/")).collect(),
            };
            let failed = self.commit_wave(table_ref, table_name, &markers, false, vec![bin], 0).await.failed.len();
            info!(table_name, project_id, failed, event = "resumed_bin_committed_early");
        }

        // A repair wave takes ONE rewrite slot, never the pool.
        //
        // `stage_hot_bin` acquires `light_rewrite_sem`, and BOTH passes share it.
        // Giving repair its own long budget without capping its concurrency let
        // a single wave hold every permit for the whole 48 minutes, so the
        // 5-minute packing tick blocked in `acquire()` and burned its entire
        // 240s budget waiting — prod 2026-08-08 05:09, five projects at once
        // reporting `hot bin staging exceeded the 239.99s left in the tick
        // budget`, with the same signature during the earlier 16-minute repair
        // at 04:24. That trades the repair starvation for a packing starvation,
        // which is worse: packing is continuous and latency-critical, repair is
        // a finite background backlog.
        //
        // Half the pool for repair, so several projects progress at once.
        //
        // This was ONE at a time (330afa8), on the theory that the blocking bin
        // needed the whole machine. That theory was wrong twice over. The sort
        // was never CPU-starved by its neighbours — it was pinned to 2
        // partitions by `MAINTENANCE_MAX_PARTITIONS` (fixed in 4d63bb1), which
        // is what actually made a ~1 GiB rewrite take ~40 minutes. And
        // serialising ten projects behind one slot means a tenant late in the
        // order waits for every bin ahead of it: prod 2026-08-09 managed FOUR
        // bins in a 30-minute uninterrupted window — dates 2026-05-30, 05-31,
        // 06-09, 07-10 — and never reached the tenant whose queries were down.
        //
        // With 16 partitions per sort, two concurrent bins are 32 reservations
        // against a 22.5 GB pool, and repair's 256-row batches keep each merge's
        // ask near 8 MB. Parallel progress is affordable; making a blocked
        // tenant queue behind nine others is not.
        //
        // A repair bin is CPU-bound — prod 2026-08-09 measured the pass at
        // ~1350% CPU spread across several concurrent bins — and it must finish
        // inside a single process lifetime, because a stage killed part-way
        // resumes nothing. That lifetime is set by the deploy rate, not by us:
        // on 2026-08-09 it was 18-21 minutes against a rewrite that needs ~40.
        // Splitting the cores N ways multiplies the wall clock of EVERY bin by
        // N and makes all of them miss the window; running them one at a time
        // means the first one — shortest-job-first, so the tenant closest to
        // being unblocked — actually lands.
        //
        // Throughput is unchanged in aggregate (the work is CPU-bound, so N
        // bins at 1/N speed take the same total time); what changes is that
        // completions arrive serially instead of all-or-nothing at the end.
        // Packing is untouched and keeps the whole pool minus this one slot.
        let k = self.config.derived.light_optimize_k(project_ids.len());
        let concurrency = match pass {
            TailPass::Pack => k,
            TailPass::Repair => (k / 2).max(1),
        };
        // Bound total rounds so a large backlog can't wedge the tick even if the
        // wall-clock budget is raised.
        let max_waves = max_waves(pass);
        // The tick's remaining time wins over this pass's nominal budget. Both
        // bounds matter: the nominal one sizes a pass in isolation, the tick one
        // stops N tables from each claiming a full budget (see the cron).
        let deadline = tick_deadline.map_or_else(|| std::time::Instant::now() + budget, |tick| tick.min(std::time::Instant::now() + budget));
        let order_index: HashMap<String, usize> = project_ids.iter().enumerate().map(|(i, p)| (p.clone(), i)).collect();
        let failed = round_robin_bins(
            project_ids,
            max_waves,
            concurrency,
            deadline,
            |round, remaining| {
                info!(table_name, round, remaining = remaining.len(), event = "light_optimize_tick_budget_exhausted");
                crate::observability::maintenance_stats().light_optimize_tick_truncated.fetch_add(1, Relaxed);
                // Next tick starts at the first project this tick never served.
                let resume = remaining.first().and_then(|p| order_index.get(p).copied()).unwrap_or(0);
                self.light_optimize_cursor.store(resume, Relaxed);
            },
            || self.light_optimize_brake(),
            // Repair commits per bin: its bins are few and minutes long, so a
            // restart mid-wave must not discard the ones already finished.
            pass == TailPass::Repair,
            |project_id, round| {
                let (schema, plan) = (schema, &plan);
                async move {
                    let files = plan.lock().await.remove(&project_id).unwrap_or_default();
                    if files.is_empty() {
                        return (project_id, Ok(BinOutcome::Converged));
                    }
                    // Log WHICH file, not just how many. A repair bin is one
                    // file, and every question worth asking about a stalled
                    // repair — is the blocking file being selected at all, is it
                    // selected and failing, or is the pass spending itself
                    // elsewhere — needs the path. Inferring it from counters and
                    // checkpoints cost most of a night on 2026-08-09 and every
                    // inference was one indirection too far from the answer.
                    let selected = match pass {
                        TailPass::Repair => files.first().map(String::as_str).unwrap_or(""),
                        TailPass::Pack => "",
                    };
                    info!(table_name, project_id, date = %today, selected_files = files.len(), selected, round, event = "light_optimize_tail_selected");
                    // Admission picked this file off the ABSENT sort tag, which
                    // is only a suspicion (see `repair_verified_sorted`). Read
                    // the footer before spending minutes rewriting it: one
                    // ranged read against the same metadata cache the scan
                    // warms, versus a whole-file sort of a file that was
                    // already fine.
                    // `Retry`, NOT `Converged`: clearing a suspect means this
                    // project's NEXT candidate is still unexamined, and
                    // `Converged` drops it from the rest of the tick. A project
                    // whose recent sealed dates hold several untagged-but-sorted
                    // flush outputs would then clear exactly one per tick and
                    // could take days to reach the file that actually poisons
                    // its scans — while looking busy the whole time.
                    if pass == TailPass::Repair && self.repair_bin_already_sorted(table_ref, &files).await {
                        return (project_id, Ok(BinOutcome::Retry));
                    }
                    // Did a previous attempt already WRITE this exact rewrite?
                    // Then commit it instead of spending another 40 minutes on
                    // it. Selection is deterministic given the snapshot, so the
                    // first pass after a restart re-selects the killed pass's
                    // files and finds its own abandoned output here.
                    //
                    // NOT gated on `TailPass::Repair`: a packing bin is equally
                    // data-preserving, and today's oversized unsorted file is
                    // repaired by the PACK pass (see the gap-rule case in
                    // `select_tail_bin_policy`) — those rewrites are just as long.
                    // A packing bin's inputs
                    // change every tick, so the lookup almost always misses; the
                    // cost of that miss is one small local file read.
                    if let Some(bin) = self.resumable_staged_bin(table_ref, table_name, &project_id, &files).await {
                        return (project_id, Ok(BinOutcome::Staged(bin)));
                    }
                    // Bound the bin by what is LEFT of the tick, the same way
                    // the dedup drain bounds its own staging. Without it a
                    // single slow bin runs past the budget and the invocation
                    // never returns, so the cron cannot re-plan: prod
                    // 2026-08-07 planned once, committed 5 of 14 bins, logged
                    // `light_optimize_tick_budget_exhausted`, and then sat for
                    // 30+ minutes with `cron_long_running` climbing while the
                    // repair backlog stopped draining entirely. (Changing the
                    // per-file size ceiling 3 GiB -> 1 GiB did nothing, which is
                    // what ruled the cause out as bytes and in as wall clock.)
                    //
                    // Discarding a timed-out bin is safe and already the
                    // established trade here: its parquet is uploaded but
                    // uncommitted, so it falls to VACUUM, and the next tick
                    // re-selects the same files.
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        // Out of budget before we began: report nothing staged
                        // rather than a failure, so the truncation path (not the
                        // error path) accounts for it.
                        return (project_id, Ok(BinOutcome::Converged));
                    }
                    // Publish the sort while it runs, so `timefusion_stats` can
                    // distinguish a grinding repair from a wedged one. The guard
                    // decrements on every exit path including the timeout.
                    let _in_flight = (pass == TailPass::Repair).then(|| in_flight_guard(&crate::observability::maintenance_stats().repair_bins_in_flight));
                    let staged = match tokio::time::timeout(
                        left,
                        self.stage_hot_bin(table_ref, table_name, schema, &project_id, files, HotStageOptions { pass, runtime_env: None }),
                    )
                    .await
                    {
                        Ok(staged) => staged,
                        Err(_) => Err(anyhow::anyhow!("hot bin staging exceeded the {left:?} left in the tick budget")),
                    };
                    (project_id, staged)
                }
            },
            |bins, round| {
                let (plan, today_str, policy) = (&plan, today_str.as_str(), &policy);
                async move {
                    let staged = bins.len();
                    // Scope the warm/evict diff to the dates this pass actually rewrote.
                    // Handing a repair wave today's marker would diff the wrong
                    // partition entirely.
                    let markers: Vec<String> = match pass {
                        TailPass::Pack => vec![format!("date={today_str}/")],
                        TailPass::Repair => policy.repair_dates.iter().map(|d| format!("date={d}/")).collect(),
                    };
                    let failed = self.commit_wave(table_ref, table_name, &markers, false, bins, round).await.failed.len();
                    // Round 0 only: one bin per project, so this is directly
                    // comparable to `projects_planned` (the alert is
                    // completed < planned for N consecutive ticks).
                    if round == 0 {
                        crate::observability::maintenance_stats().light_optimize_projects_completed.fetch_add((staged - failed.min(staged)) as u64, Relaxed);
                    }
                    // Re-plan the NEXT wave from the just-committed snapshot: the
                    // outputs are tagged sorted runs and excluded from
                    // re-selection, so this yields each project's next time slice
                    // — never the run this wave wrote. One walk per wave, not per
                    // project per pass — and none at all when no further round
                    // can run (round cap / deadline), which would walk the
                    // snapshot only to discard the result.
                    if round + 1 < max_waves && std::time::Instant::now() < deadline {
                        let next = {
                            let table = table_ref.read().await;
                            Self::select_all_hot_bins(&table, schema, today_str, policy).unwrap_or_default()
                        };
                        // Filter the RE-PLAN too. Filtering only round 0 is what
                        // made the first attempt at this useless: every later
                        // round re-selected unverified suspects and fell back to
                        // one clear per round, leaving the measured rate at
                        // 1.24/min with zero rewrites either way.
                        let next = self.reselect_until_real_work(table_ref, table_name, schema, today_str, policy, next).await.unwrap_or_default();
                        *plan.lock().await = next.into_iter().collect();
                    }
                    failed
                }
            },
        )
        .await;
        // Checkpoint after the tick's final commit rather than per N versions:
        // wave commits are ~40x rarer than the old per-bin commits, so a
        // version-count cadence would checkpoint ~40x less often exactly where
        // replay-tail length is the top CPU cost (34.5% of process CPU in
        // ScanLogReplayProcessor, prod profile 2026-07-29).
        self.checkpoint_after_waves(table_ref, table_name).await;
        anyhow::ensure!(failed == 0, "Light optimize failed for {failed} hot bin(s)");
        Ok(())
    }

    /// Stage one bin's rewrite: read the selected files, sort by the schema keys, write staged
    /// parquet, and return the `Remove+Add` actions for the wave commit. No Delta commit and no
    /// table lock, so waves parallelize instead of serializing behind the log. Uncommitted parquet is
    /// invisible to readers, and failures clean up their own staged files. `Retry` means the bin's
    /// files were rewritten concurrently; the project stays in rotation and the next re-plan serves
    /// a fresh bin. `Converged` means nothing worth staging.
    async fn stage_hot_bin(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, schema: &crate::schema::TableSchema, project_id: &str, files: Vec<String>,
        options: HotStageOptions,
    ) -> Result<BinOutcome<StagedBin>> {
        use deltalake::{delta_datafusion::TableProviderBuilder, kernel::Action, writer::DeltaWriter};
        let HotStageOptions { pass, runtime_env } = options;
        // One read-lock, one table clone per bin: the pinned scan snapshot and
        // the writer's staging table both derive from it (a second clone per
        // bin was pure waste — K bins x up to 12 waves per tick).
        let staging_table = { table_ref.read().await.clone() };
        let (snapshot, log_store) = (Arc::new(staging_table.snapshot()?.snapshot().clone()), staging_table.log_store());
        // Map paths to Add actions in the SAME snapshot the scan reads, so the
        // Remove tombstones carry the exact fields of the files we rewrote.
        let wanted: HashSet<&str> = files.iter().map(String::as_str).collect();
        let targets = dedup_adds_by_path(
            snapshot.log_data().iter().filter(|f| wanted.contains(f.path().as_ref())).map(|f| {
                #[allow(deprecated)]
                f.add_action()
            }),
            table_name,
        );
        if targets.len() != files.len() {
            // WARN, not debug: prod runs RUST_LOG=info, so at debug this exit is
            // INVISIBLE — a bin that is selected every pass and silently
            // abandoned every pass looks identical to one that was never
            // selected. That cost a night on 2026-08-09, where shipbubble's
            // blocking file logged 13 selections, 13 staging starts and only 11
            // `wave_bin_staged`, with nothing at all explaining the other two.
            warn!(
                table_name,
                project_id,
                mapped = targets.len(),
                selected = files.len(),
                missing = ?files.iter().filter(|f| !targets.iter().any(|t| &&t.path == f)).collect::<Vec<_>>(),
                event = "light_optimize_bin_vanished"
            );
            return Ok(BinOutcome::Retry);
        }
        // The wave engine's OWN permit — NEVER maintenance_rewrite_sem. That
        // semaphore (2 permits, dedup/optimize/recompress) exists because heavy
        // rewrites' Arrow is pool-invisible; taking it here would cap waves at 2
        // (or 0 while a dedup drain holds both — prod 2026-07-30: 25+ min of
        // hot-compact starvation) and burn the tick deadline waiting. Wave
        // staging is already bounded by K and sized by the light pool slice, so
        // this permit is a ceiling + the instrumented wait point below.
        let permit_wait = std::time::Instant::now();
        let _light_permit = self.light_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("light rewrite semaphore closed: {e}"))?;
        let permit_wait_ms = permit_wait.elapsed().as_millis() as u64;
        let stage_started = std::time::Instant::now();
        // Bytes read into this rewrite — free here (the Adds are already mapped)
        // and the divisor that turns staging duration into observed R2 throughput.
        let bytes_in: i64 = targets.iter().map(|a| a.size).sum();
        // Emitted BEFORE the rewrite, because the interesting bins are the ones
        // that never reach `wave_bin_staged`. A timed-out bin used to report
        // only "exceeded the Ns left in the tick budget" — no size, no file
        // count, no permit wait — so diagnosing it meant guessing at which of
        // bytes, file count or contention was the binding constraint (prod
        // 2026-08-08: two wrong hypotheses before this line existed). With it,
        // a failure is a subtraction against the matching start line.
        info!(table_name, project_id, selected_files = targets.len(), bytes_in, permit_wait_ms, event = "wave_bin_staging_started");
        let stage_store = staging_table.log_store().object_store(None);
        let mut adds: Vec<Action> = Vec::new();
        let staged: Result<()> = async {
            // File-scoped provider over the pinned snapshot: reads exactly this
            // bin's files, so no predicate and no per-file stats parsing.
            let provider = TableProviderBuilder::default()
                .with_log_store(log_store)
                .with_eager_snapshot(Arc::clone(&snapshot))
                .with_file_paths(files.clone())
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("hot bin provider: {e}"))?;
            // The light session state forces non-view Parquet types: Variant
            // columns are Struct{Binary, Binary} on disk and a view-typed read
            // blows the rewrite up mid-scan with "Expected Binary, got BinaryView".
            // A bin that already exhausted the pool retries with fewer sort
            // partitions — the unspillable merge exec is per-partition, so this
            // is the one dial that changes whether the same file can fit.
            let repair_level = files.first().and_then(|f| self.repair_degradation.get(f).map(|v| *v)).unwrap_or(0);
            let state = runtime_env.map_or_else(
                || match pass {
                    TailPass::Pack => self.light_optimize_session_state(),
                    TailPass::Repair => self.repair_session_state(REPAIR_SORT_PARTITION_LADDER[repair_level.min(REPAIR_SORT_PARTITION_LADDER.len() - 1)]),
                },
                |runtime| {
                    build_optimize_session_state_tuned(
                        self.config.memory.timefusion_query_partitions,
                        runtime,
                        Some("256"),
                        Some(UncappedSort { partitions: 1, reservation_bytes: Some(32 * 1024 * 1024) }),
                    )
                },
            );
            let ctx = datafusion::prelude::SessionContext::new_with_state(state);
            // Unique per staging: the cached session state's clone SHARES its
            // catalog, so a fixed name collides across the k concurrent
            // stagings ("The table hot_bin already exists", prod 2026-07-30 —
            // serial k=1-2 never collided; k~9 parallelism exposed it).
            // Deregistered right after the read so the shared catalog can't
            // accumulate entries.
            let bin_table = format!("hot_bin_{}", uuid::Uuid::new_v4().simple());
            ctx.register_table(&bin_table, Arc::new(provider))?;
            // ORDER BY in the PLAN, streamed — not `collect()` + an in-process
            // Arrow lexsort.
            //
            // `sort_batches_by_schema` refuses to sort past `SORT_SKIP_BYTES`
            // (256 MB of in-memory Arrow) and silently returns `sorted=false`.
            // A hot bin is packed to `light_optimize_target_size` — 256 MB of
            // FILE bytes — and prod's zstd ratio is ~17x, so EVERY bin arrived
            // ~17x over that threshold and every hot-tail output was written
            // unsorted: measured 2026-08-01, 0 of the 8 largest files in a live
            // partition declared `sorting_columns`. One such file is enough,
            // because the reader's `derive_common_ordering` is all-or-nothing —
            // so the scan lost its declared ordering, which cost the streaming
            // top-N pushdown AND forced `DedupExec` into its unbounded
            // `full-set` seen-set, the per-query memory behind the OOM/restart
            // cycle.
            //
            // Sorting in the plan fixes both halves: DataFusion merges the
            // already-sorted inputs with a `SortPreservingMergeExec` (one batch
            // per file, independent of bin size) and falls back to a SortExec
            // that spills into the light pool where they are not — instead of
            // materialising the whole bin 2-3x with no pool and no spill. The
            // footer declaration is then honest by construction.
            let order_by = schema_order_by_clause(schema);
            let sorted = !order_by.is_empty();
            // SLICE a repair rewrite, or one individually oversized L0 file,
            // by event time so no single sort has to fit the whole file. See
            // `REPAIR_SLICE_TARGET_BYTES` for why no partition count can do
            // this instead.
            //
            // The slices are emitted in the output's OWN sort direction and
            // appended to one writer, so the concatenation is globally sorted
            // and every footer stays honest — the same property the
            // `max_file_bytes` cut below already relies on.
            //
            // Declined unless every precondition holds: this is a repair or a
            // single oversized L0 input, the table sorts on a leading
            // timestamp, and the bin has a usable non-null range. A NULL
            // timestamp would sort outside every slice and silently drop rows,
            // so its presence declines slicing outright; `rows_staged` is
            // verified against the input count before anything commits.
            let lead = schema.sorting_columns.first();
            let slice_target = coordinator_slice_target(pass, targets.len(), bytes_in);
            let slice_col = lead.filter(|_| sorted && slice_target.is_some()).map(|c| c.name.clone());
            let slices: Vec<String> = match slice_col {
                None => Vec::new(),
                Some(col) => {
                    let want = (bytes_in / slice_target.expect("slice column requires a target")).max(0) as usize + 1;
                    let probe = format!(
                        "SELECT min(\"{col}\") AS lo, max(\"{col}\") AS hi, sum(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS nulls FROM {bin_table}"
                    );
                    // The bounds come back as the column's raw i64 (micros), but
                    // the column is `Timestamp(Microsecond, Some("UTC"))` — and
                    // DataFusion will not coerce a bare integer to a timestamp,
                    // so an untyped literal fails the whole bin in `type_coercion`
                    // before it reads a row (prod 2026-08-09). `{:?}` on the
                    // Arrow type is exactly `arrow_cast`'s type syntax. No type
                    // in hand means no honest literal, so decline slicing rather
                    // than emit a predicate that cannot plan.
                    let cast_ty = ctx
                        .table_provider(bin_table.as_str())
                        .await
                        .ok()
                        .and_then(|p| p.schema().field_with_name(&col).ok().map(|f| format!("{:?}", f.data_type())));
                    match (want > 1).then_some(()).and(cast_ty).zip(bin_time_range(&ctx, &probe).await) {
                        Some((ty, (lo, hi))) if hi > lo => {
                            // Equal-ROW cuts where they can be had; the equal-TIME
                            // split only bounds memory when rows are spread evenly,
                            // and on the file this exists for they are not.
                            let cuts = repair_slice_cuts(&ctx, bin_table.as_str(), &col, want).await;
                            let mut bounds = match cuts.is_empty() {
                                false => repair_bounds_from_cuts(lo, hi, &cuts),
                                true => repair_slice_bounds(lo, hi, want),
                            };
                            if lead.is_some_and(|c| c.descending) {
                                bounds.reverse();
                            }
                            let lit = |v: i64| format!("arrow_cast({v}, '{ty}')");
                            bounds
                                .into_iter()
                                .map(|(start, end)| match end {
                                    // Half-open on the high side; the final
                                    // ascending slice is unbounded so `hi` itself
                                    // is never dropped.
                                    Some(e) => format!(" WHERE \"{col}\" >= {} AND \"{col}\" < {}", lit(start), lit(e)),
                                    None => format!(" WHERE \"{col}\" >= {}", lit(start)),
                                })
                                .collect()
                        }
                        _ => Vec::new(),
                    }
                }
            };
            if !slices.is_empty() {
                info!(table_name, project_id, bytes_in, slices = slices.len(), event = "repair_bin_sliced");
            }
            // Intermediate tier: this output is rewritten tonight by
            // consolidate/recompress, so it isn't worth max compression.
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_intermediate, sorted);
            let mut writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)
                .map_err(|e| anyhow::anyhow!("hot bin writer: {e}"))?
                .with_writer_properties(writer_properties);
            let target_schema = writer.arrow_schema();
            let mut rows_staged = 0usize;
            let max_file_bytes = self.config.maintenance.timefusion_writer_max_file_bytes;
            // Coverage identity to carry onto the outputs, when every input
            // agrees on ALL of it. See `carried_coverage_tags`.
            let carried = carried_coverage_tags(&targets);
            let tag_sorted = |mut add: deltalake::kernel::Add| {
                // Tag the output so the next tick's selection treats it as a
                // sorted run (folded only while under the sorted-run cap).
                if sorted {
                    add.tags.get_or_insert_with(Default::default).insert(SORTED_RUN_TAG.to_string(), Some("true".to_string()));
                }
                if !carried.is_empty() {
                    add.tags.get_or_insert_with(Default::default).extend(carried.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));
                }
                Action::Add(add)
            };
            // One pass when not sliced; otherwise one pass per slice, in sort
            // order, all feeding the SAME writer.
            let passes: Vec<String> = if slices.is_empty() { vec![String::new()] } else { slices };
            for predicate in &passes {
                let mut stream = ctx.sql(&format!("SELECT * FROM {bin_table}{predicate}{order_by}")).await?.execute_stream().await?;
                while let Some(batch) = stream.next().await {
                    let batch = cast_variant_columns_to_binary(batch?)?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    rows_staged += batch.num_rows();
                    let casted = deltalake::kernel::schema::cast_record_batch(&batch, target_schema.clone(), true, true)?;
                    writer.write(casted).await.map_err(|e| anyhow::anyhow!("hot bin stage: {e}"))?;
                    // Cut the file at the ceiling instead of buffering the whole bin
                    // into one Add. The cut is on a contiguous slice of the sorted
                    // stream, so each piece keeps an honest footer and the pieces
                    // stay event-time disjoint.
                    if writer.buffer_len() >= max_file_bytes {
                        adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("hot bin flush: {e}"))?.into_iter().map(tag_sorted));
                    }
                }
            }
            let _ = ctx.deregister_table(&bin_table);
            if rows_staged == 0 {
                // The other silent exit. A bin whose scan yields no rows returns
                // Ok and stages nothing, which is indistinguishable in the logs
                // from success — see the `bin_vanished` note above.
                warn!(table_name, project_id, files = files.len(), event = "light_optimize_bin_no_rows");
                return Ok(());
            }
            // A sliced rewrite must reproduce EVERY input row. Slicing is the one
            // thing here that could silently drop rows (a value outside every
            // range), so the count is checked before anything is committed and a
            // mismatch aborts the bin — the inputs stay live and the staged
            // parquet is cleaned up, exactly as any other staging failure.
            // A deletion vector makes the SCAN return fewer rows than
            // `numRecords`, so the count is only comparable when no input
            // carries one — otherwise the guard would abort perfectly good
            // repairs. Declining to check is safe; falsely aborting is not.
            if passes.len() > 1 && targets.iter().all(|a| a.deletion_vector.is_none()) {
                let expected: usize = targets
                    .iter()
                    .filter_map(|a| a.stats.as_deref())
                    .filter_map(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .filter_map(|v| v.get("numRecords").and_then(serde_json::Value::as_u64))
                    .sum::<u64>() as usize;
                if expected > 0 && rows_staged != expected {
                    anyhow::bail!("sliced repair staged {rows_staged} rows but the inputs hold {expected} — refusing to commit a lossy rewrite");
                }
            }
            adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("hot bin flush: {e}"))?.into_iter().map(tag_sorted));
            Ok(())
        }
        .await;
        if let Err(e) = staged {
            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
            warn!("Light optimize staging failed for project={} table={}: {}", project_id, table_name, e);
            // Count it against the candidate so a deterministically-impossible
            // file stops being re-offered and the queue behind it drains.
            //
            // A pool exhaustion counts for the WHOLE threshold, because it is not
            // a coin flip: the working set of a whole-file sort is a function of
            // the file's size and the pool's, so it recurs on every attempt. The
            // 3-strike rule exists for genuinely transient failures (a lost OCC
            // race, a file rewritten underneath us, a restart mid-stage) and
            // those keep it.
            //
            // Measured 2026-08-09: with one repair pass per process (the budget
            // is per-table and a pass legitimately runs for hours, so the hourly
            // cron stands down behind the guard), a 3-strike counter that also
            // resets on restart NEVER reached 3 — 2 h 13 m produced one failure
            // and zero quarantines while shipbubble's file was never reached.
            // A deterministic failure has to be worth the whole threshold or the
            // mechanism cannot fire at the rate passes actually run.
            let exhausted = e.to_string().contains("Resources exhausted");
            if pass == TailPass::Repair {
                let level = files.first().and_then(|f| self.repair_degradation.get(f).map(|v| *v)).unwrap_or(0);
                let (retry_at, step) = repair_failure_action(exhausted, level);
                let deterministic = exhausted && retry_at.is_none();
                for path in &files {
                    if retry_at.is_some() {
                        self.repair_degradation.insert(path.clone(), level + 1);
                    }
                    let hits = *self.repair_failures.entry(path.clone()).and_modify(|n| *n += step).or_insert(step);
                    if hits >= REPAIR_QUARANTINE_AFTER && hits - step < REPAIR_QUARANTINE_AFTER {
                        warn!(
                            table_name,
                            project_id,
                            path,
                            failures = hits,
                            deterministic,
                            event = "footer_repair_quarantined",
                            "repair candidate failed {hits}x consecutively — parking it so other candidates can be reached; it needs the off-box `timefusion optimize --recompress` or a chunked rewrite"
                        );
                    }
                    if let Some(partitions) = retry_at {
                        info!(
                            table_name,
                            project_id,
                            path,
                            partitions,
                            event = "footer_repair_parallelism_degraded",
                            "pool exhausted — retrying this bin at {partitions} sort partitions before believing it"
                        );
                    }
                }
            }
            return Err(e);
        }
        if adds.is_empty() {
            // Zero rows staged: nothing to commit, and retrying the same
            // zero-row selection would loop — treat as converged for this tick.
            return Ok(BinOutcome::Converged);
        }
        // Record the intent BEFORE the bin can be handed to a wave commit, so a
        // crash anywhere in the staging→commit window leaves a trail to clean up.
        let wave_id = uuid::Uuid::new_v4().to_string();
        self.record_staged_intent(&StagedIntent {
            wave_id: wave_id.clone(),
            table_name: table_name.to_string(),
            project_id: project_id.to_string(),
            recorded_at: crate::support::now_secs(),
            paths: adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.path.clone()) } else { None }).collect(),
            // Recorded so a restart RESUMES this bin instead of re-staging it:
            // the inputs make staleness decidable, the Adds rebuild the bin
            // without re-reading footers. See `resumable_staged_bin`.
            target_paths: files.clone(),
            adds: adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.clone()) } else { None }).collect(),
        });
        // Data-preserving compaction: BOTH sides carry data_change=false so the
        // fork's snapshot-isolation downgrade applies and concurrent ingest
        // appends can't veto the wave (see `staged_actions`; aa50480).
        let (removes, adds) = staged_actions(&targets, adds, false);
        // Splits a slow tick into its two causes: permit contention vs the
        // object-store rewrite itself (bytes_in / staging_ms = observed R2
        // throughput). One line per staged bin — waves are ~K per round.
        info!(
            table_name,
            project_id,
            selected_files = targets.len(),
            bytes_in,
            staging_ms = stage_started.elapsed().as_millis() as u64,
            permit_wait_ms,
            event = "wave_bin_staged"
        );
        // Consecutive, not cumulative: a bin that staged is not a poison pill,
        // whatever transient failures preceded it.
        if pass == TailPass::Repair {
            for path in &files {
                self.repair_failures.remove(path);
                self.repair_degradation.remove(path);
            }
        }
        Ok(BinOutcome::Staged(StagedBin { project_id: project_id.to_string(), wave_id, target_paths: files, removes, adds, stage_store, dedup: None }))
    }

    /// Commit one WAVE: every staged unit's Remove+Add in a single transaction.
    ///
    /// Before committing, each unit's target files are verified still live in the refreshed
    /// snapshot; a unit whose target was rewritten concurrently has only its own actions
    /// dropped and the rest of the wave still commits. Shared by hot-tail compaction (today) and
    /// dirty-bin dedup (sealed dates), which are disjoint by construction and never stage the
    /// same file. Sharing the per-physical-table commit lock prevents optimize-vs-dedup
    /// delete-delete aborts. `data_change` is the one real difference between the engines.
    pub(crate) async fn commit_wave(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date_markers: &[String], data_change: bool, bins: Vec<StagedBin>, round: usize,
    ) -> WaveResult {
        use deltalake::kernel::{Action, transaction::TableReference};
        debug_assert!(bins.iter().all(|b| b.data_change() == data_change), "a wave must not mix data-preserving and row-dropping units");
        let engine = if data_change { "dedup" } else { "light optimize" };
        // Which bins actually REACH a commit. `staged_intent_resumed` proves a
        // recovered rewrite was handed back (prod 2026-08-09: wave 980e1400
        // resumed 6x across restarts, complete and row-exact on S3), but nothing
        // downstream says whether it reached commit_wave, and the mode stayed
        // `full-set`. Four hypotheses died for want of exactly this line —
        // budget discard, dedup contention, flush yield, probe fallback — each
        // refuted by a counter that was already zero. Name the waves on the way
        // in, so the next pass says where the bin went instead of what it wasn't.
        info!(
            table_name,
            engine,
            round,
            bins = bins.len(),
            wave_ids = ?bins.iter().map(|b| b.wave_id.as_str()).collect::<Vec<_>>(),
            event = "wave_commit_enter"
        );
        let mut bins = bins;
        let mut failed: Vec<StagedBin> = Vec::new();
        // Bins already CONFIRMED landed by an earlier attempt of this wave (see
        // the self-landed split below). Carried across OCC retries so their
        // credit — and their dirty-bin certification — is never lost.
        let mut carried: Vec<StagedBin> = Vec::new();
        // Key on "" explicitly: the wave spans MULTIPLE projects of one physical
        // table, and every other unified-log writer (flush, dedup, coalesced
        // commit) serializes under the ("", table) key. Keying on
        // bins[0].project_id would silently pick a DIFFERENT lock if that
        // project has custom storage (table_lock_key only collapses non-custom
        // projects) — the liveness check would then race dedup's Removes.
        let commit_lock = self.commit_lock("", table_name).await;
        // Same key as the lock above — flush/ingest committers queued on it.
        let flush_waiters = self.flush_waiters("", table_name).await;
        // The wave spans several projects of a handful of dates, so the
        // warm/evict diff is scoped to those dates rather than the whole
        // (26k-file) table.
        let markers: Vec<&str> = date_markers.iter().map(String::as_str).collect();
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        const MAX_RETRIES: usize = 4;
        for attempt in 0..MAX_RETRIES {
            // FLUSH PRIORITY (prod 2026-07-30). The lock is FIFO, so joining the
            // queue ahead of a waiting flush costs it OUR whole commit — and on a
            // backlogged tick several wave commits, each legally minutes long,
            // stack up in front of it (flush waited >600s to ACQUIRE and its
            // watchdog killed the attempt; nothing was hung). Durability outranks
            // maintenance: we don't enqueue at all while a flush is waiting, so
            // flush latency is bounded by ONE in-flight wave commit.
            //
            // NOT a starvation risk for waves: flush is periodic (60s cadence) and
            // its commit is a short log append, so the count is zero for most of
            // every minute — a wave that stands down here re-stages and finds a
            // gap on a later tick. If it were EVER continuously nonzero, flush
            // would be saturating the commit path and compaction is exactly the
            // work that should yield.
            if flush_waiters.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                crate::observability::maintenance_stats().wave_commits_yielded_to_flush.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(table_name, engine, round, attempt = attempt + 1, bins = bins.len(), event = "wave_commit_flush_yield");
                // Nothing was committed, so the bins' target files are still live
                // and the staged parquet is referenced by nothing. Delta VACUUM
                // cannot see uncommitted staged files, so leaving them would leak
                // on S3 forever; the next tick re-stages from the same (still
                // live) targets. Dedup's bins go back on the dirty queue via the
                // `failed` list — a partition with duplicates still in it must
                // never be certified clean.
                self.discard_bins(&bins).await;
                failed.extend(bins);
                return WaveResult { landed: carried, failed };
            }
            let commit_guard = commit_lock.lock().await;
            // Bounded: this reads the log over the network with the commit lock
            // held. A timeout here just means we build on a possibly-stale
            // snapshot — the liveness check + OCC retry ladder below already
            // handle that, and the lock is freed on schedule either way.
            if let Err(e) = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "wave_refresh",
                table_name,
                refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot),
            )
            .await
            {
                debug!("{engine} wave pre-commit refresh failed (attempt {}): {}", attempt + 1, e.message);
            }
            let mut new_table = { table_ref.read().await.clone() };
            let live: HashSet<String> = match new_table.snapshot() {
                Ok(s) => s.log_data().iter().map(|f| f.path().into_owned()).collect(),
                Err(e) => {
                    drop(commit_guard);
                    error!("{engine} wave: no snapshot for {table_name}: {e}");
                    self.discard_bins(&bins).await;
                    failed.extend(bins);
                    return WaveResult { landed: carried, failed };
                }
            };
            let (fresh, stale) = split_live_bins(bins, |b| &b.target_paths, &live);
            // SELF-LANDED SPLIT — do not remove. A bin is "stale" because its
            // target files left the snapshot, and the normal cause is a
            // concurrent rewrite, whose staged parquet nothing references (safe
            // to delete). But OUR OWN previous attempt landing is
            // indistinguishable by targets alone: a commit that landed and then
            // reported an error (post-commit hook, or an outer bound firing)
            // takes the same shape — targets gone, and this retry would DELETE
            // the very files the landed commit now references (dangling Adds,
            // the 2026-07-09 incident shape).
            //
            // The Adds settle it: staged parquet is uuid-named by the writer, so
            // nobody else can produce those paths. Present in the snapshot ⇒ our
            // commit landed.
            let (self_landed, stale): (Vec<StagedBin>, Vec<StagedBin>) = stale.into_iter().partition(|b| bin_adds_live(b, &live));
            for bin in &stale {
                debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_stale_at_commit");
            }
            self.discard_bins(&stale).await;
            failed.extend(stale);
            if !self_landed.is_empty() {
                warn!(
                    table_name,
                    engine,
                    bins = self_landed.len(),
                    attempt = attempt + 1,
                    event = "wave_bin_self_landed",
                    "a previous attempt's commit LANDED despite erroring — crediting its bins instead of deleting their (now live) files"
                );
                self.clear_bin_intents(&self_landed);
                Self::record_wave_landed(&self_landed, data_change);
                carried.extend(self_landed);
            }
            // Two dirty 10-minute bins can live in the same compacted parquet
            // file. Each staged unit is a full-file replacement, so committing
            // both units would remove the file twice and add two copies of all
            // its rows. Land only a target-disjoint subset per wave; failed
            // units are requeued and will re-plan from the replacement file on
            // the next tick. This is also required for Delta action validity.
            let mut claimed_targets = HashSet::new();
            let (fresh, overlapping): (Vec<_>, Vec<_>) = fresh.into_iter().partition(|bin| {
                if bin.target_paths.iter().any(|path| claimed_targets.contains(path)) {
                    false
                } else {
                    claimed_targets.extend(bin.target_paths.iter().cloned());
                    true
                }
            });
            for bin in &overlapping {
                debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_overlapping_target");
            }
            self.discard_bins(&overlapping).await;
            failed.extend(overlapping);
            if fresh.is_empty() {
                drop(commit_guard);
                return WaveResult { landed: carried, failed };
            }
            let actions: Vec<Action> = fresh.iter().flat_map(|b| b.removes.iter().chain(b.adds.iter()).cloned()).collect();
            let pre_uris: Option<HashSet<String>> = track_files.then(|| scoped_file_uris(&new_table, &markers).into_iter().collect());
            let partitions = schema_or_default(table_name).partitions.clone();
            let op = wave_operation(data_change, self.config.maintenance.timefusion_light_optimize_target_size, (!partitions.is_empty()).then_some(partitions));
            let snapshot_ref = match new_table.snapshot() {
                Ok(s) => s as &dyn TableReference,
                Err(_) => {
                    drop(commit_guard);
                    failed.extend(fresh);
                    return WaveResult { landed: carried, failed };
                }
            };
            // Bounded: the proven prod hang (2026-07-30) was HERE — one R2
            // request pinned this lock and every committer on the table stalled.
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "wave_commit",
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                    .with_actions(actions)
                    .build(Some(snapshot_ref), new_table.log_store(), op),
            )
            .await;
            match commit_res {
                Ok(finalized) => {
                    new_table.state = Some(finalized.snapshot());
                    // Release before post-commit work (swap + cache warm) —
                    // holding it would serialize ingest appends.
                    drop(commit_guard);
                    let bins_committed = fresh.len();
                    self.clear_bin_intents(&fresh);
                    info!(table_name, engine, round, bins = bins_committed, attempt = attempt + 1, event = "wave_committed");
                    // WARM BEFORE EVICT: a wave swaps K bins at once, so
                    // evicting first would cold-start the hottest query window
                    // every wave (the 2026-07-21 cache-thrash lesson).
                    let live_uris = self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &markers).await;
                    self.reindex_wave_outputs(table_ref, table_name, &fresh, &live_uris).await;
                    Self::record_wave_landed(&fresh, data_change);
                    return WaveResult { landed: carried.tap_mut(|landed| landed.extend(fresh)), failed };
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    // Released BEFORE the probe: on a timeout the store is
                    // already slow, and the probe is another log read — holding
                    // the lock across it would re-create the very stall this
                    // bound exists to end.
                    drop(commit_guard);
                    let occ = !timed_out && is_occ_conflict_err(&e);
                    if occ {
                        crate::observability::record_optimize_conflict();
                    }
                    if occ && attempt + 1 < MAX_RETRIES {
                        debug!("{engine} wave OCC conflict (attempt {}/{}) table={}", attempt + 1, MAX_RETRIES, table_name);
                        tokio::time::sleep(occ_backoff(attempt)).await;
                        bins = fresh; // re-verify liveness against the newer snapshot
                        continue;
                    }
                    // Terminal: probe before deleting the NEW files. A
                    // landed-but-hook-failed commit already Removed the OLD
                    // files, so the new files are the only live copy.
                    let all_adds: Vec<Action> = fresh.iter().flat_map(|b| b.adds.iter().cloned()).collect();
                    match probe_after_timeout(self.probe_commit_landed_bounded(table_ref, &all_adds).await, timed_out) {
                        CommitProbe::Landed => {
                            warn!("{engine} wave for '{}' reported an error but LANDED (post-commit hook failed): {}", table_name, e);
                            let post = { table_ref.read().await.clone() };
                            let live_uris = self.swap_and_refresh_cache(table_ref, post, pre_uris.as_ref(), &markers).await;
                            self.reindex_wave_outputs(table_ref, table_name, &fresh, &live_uris).await;
                            self.clear_bin_intents(&fresh);
                            Self::record_wave_landed(&fresh, data_change);
                            return WaveResult { landed: carried.tap_mut(|landed| landed.extend(fresh)), failed };
                        }
                        CommitProbe::NotLanded => {
                            crate::observability::record_optimize_failed();
                            error!("{engine} wave commit failed for '{}': {}", table_name, e);
                            self.discard_bins(&fresh).await;
                            failed.extend(fresh);
                            return WaveResult { landed: carried, failed };
                        }
                        CommitProbe::Inconclusive => {
                            // Staged files stay in place (they may be the only
                            // live copy) — the units still count as failed, so a
                            // dedup unit's dirty bin is requeued.
                            //
                            // CONVERGENCE (both for an errored and a TIMED-OUT
                            // commit): the next wave's first act under the lock
                            // is `refresh_table_snapshot`, which re-reads the
                            // Delta log — so a commit that landed while we were
                            // not looking is observed there. Its Adds are then
                            // live, its Removes applied, and this wave's targets
                            // are gone from the snapshot, so the re-staged bins
                            // fail the liveness check and drop out instead of
                            // double-applying. If it truly did not land, the
                            // targets are still live and the bin is simply
                            // re-staged. Either way the only cost of an
                            // unconfirmed landing is a leaked staged file, which
                            // the boot-time staged-intent reconcile reclaims.
                            warn!("{engine} wave for '{}' errored, landing UNCONFIRMED — leaving new files in place: {}", table_name, e);
                            failed.extend(fresh);
                            return WaveResult { landed: carried, failed };
                        }
                    }
                }
            }
        }
        WaveResult { landed: carried, failed }
    }

    /// Publish search sidecars for every file a coordinator/dedup wave just
    /// committed. The legacy optimize path already did this; `commit_wave`
    /// did not, so every coordinator rewrite created a fresh coverage hole and
    /// queries fell back to a multi-gigabyte raw leg until the nightly global
    /// reconcile. Awaiting publication keeps a successfully returned wave
    /// covered. Failure remains correctness-safe (hybrid reads use raw data)
    /// and is visible for the reconcile retry path.
    async fn reindex_wave_outputs(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, bins: &[StagedBin], live_uris: &[String]) {
        let Some(svc) = self.tantivy_indexer().cloned().filter(|svc| svc.config.is_table_indexed(table_name)) else {
            return;
        };
        let files = wave_added_parquet(bins, live_uris);
        if files.is_empty() {
            return;
        }
        let store = { table_ref.read().await.log_store().object_store(None) };
        let table = table_name.to_owned();
        // Carry coverage forward first. A wave output holds exactly its inputs'
        // rows under the same ids — a dedup unit DROPS superseded versions, which
        // leaves the index a superset (false positives the scan filters), never a
        // false negative — so when every input was already covered this is a
        // manifest edit instead of a full S3 read-back and rebuild.
        //
        // This path, not the optimize one, is where the cost lands: `older` grew
        // ~70/hr on 2026-08-23 from sealed-partition rewrites, each output
        // rebuilt from scratch (a 195 MB build at 12:57 was one of them).
        let mut carried: HashSet<String> = HashSet::new();
        for bin in bins {
            let for_bin: Vec<String> = files.iter().filter(|(project, _, _)| *project == bin.project_id).map(|(_, _, uri)| uri.clone()).collect();
            if for_bin.is_empty() {
                continue;
            }
            match svc.carry_forward_after_compaction(table_name, &bin.project_id, &bin.target_paths, &for_bin).await {
                Ok(true) => carried.extend(for_bin),
                Ok(false) => {}
                Err(error) => warn!(table_name, %error, event = "tantivy_wave_carry_forward_failed"),
            }
        }
        let files: Vec<_> = files.into_iter().filter(|(_, _, uri)| !carried.contains(uri)).collect();
        // Log whenever there was work, not only when something carried: gating on
        // `!carried.is_empty()` hides every all-rebuild wave, so the event reads
        // `rebuilding=0` 100% of the time and the refusals are invisible.
        if !carried.is_empty() || !files.is_empty() {
            info!(table_name, carried = carried.len(), rebuilding = files.len(), event = "tantivy_wave_carried_forward");
        }
        let (built, failed) = futures::stream::iter(files.into_iter().map(|(project, rel, uri)| {
            let (svc, store, table) = (svc.clone(), store.clone(), table.clone());
            async move { svc.build_index_for_file(&table, &project, &rel, &uri, store).await }
        }))
        .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1))
        .fold((0usize, 0usize), |(built, failed), result| async move {
            match result {
                Ok(()) => (built + 1, failed),
                Err(error) => {
                    warn!(table_name, %error, event = "tantivy_wave_reindex_failed");
                    (built, failed + 1)
                }
            }
        })
        .await;
        info!(table_name, built, failed, event = "tantivy_wave_reindex_complete");
    }

    /// Per-engine counters for a landed wave. Dedup's dropped-row accounting is
    /// reported HERE and nowhere else: staging knows `before`/`after` long before
    /// the transaction exists, and a unit that loses the liveness check or the
    /// commit dropped exactly zero rows from the table.
    fn record_wave_landed(landed: &[StagedBin], data_change: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        if data_change {
            for dropped in landed.iter().filter_map(|b| b.dedup.as_ref()).map(DedupUnit::dropped).filter(|d| *d > 0) {
                crate::observability::record_compaction_dedup_dropped(dropped);
            }
            // Dedup waves count under their own counters — crediting them to
            // light_optimize_* made the stats under-report committed waves
            // (2026-07-30: 3 wave_committed log events, counter said 1).
            stats.dedup_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.dedup_waves_committed.fetch_add(1, Relaxed);
        } else {
            stats.light_optimize_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.light_optimize_waves_committed.fetch_add(1, Relaxed);
        }
    }

    /// Cleanup + intent-clear for bins leaving the wave uncommitted. One helper
    /// because the pair IS the crash-safety invariant — a drifted copy that
    /// cleans without clearing (or vice versa) breaks the manifest's meaning.
    pub(crate) async fn discard_bins(&self, bins: &[StagedBin]) {
        for bin in bins {
            Self::cleanup_orphaned_parquet(&bin.stage_store, &bin.adds).await;
        }
        self.clear_bin_intents(bins);
    }

    fn clear_bin_intents(&self, bins: &[StagedBin]) {
        self.clear_staged_intent(&bins.iter().map(|b| b.wave_id.as_str()).collect::<Vec<_>>());
    }

    /// A maintenance state file living beside the WAL dir (not in it).
    fn maintenance_state_path(&self, filename: &str) -> PathBuf {
        let wal_dir = self.config.core.wal_dir();
        wal_dir.parent().map(|p| p.to_path_buf()).unwrap_or(wal_dir).join(filename)
    }

    /// Path where verified-sorted paths are remembered across restarts.
    fn repair_verified_path(&self) -> PathBuf {
        self.maintenance_state_path("repair_verified_sorted.txt")
    }

    /// Persist footers already probed as sorted, so a restart does not re-probe them.
    ///
    /// Sound because a Delta object path is immutable: a rewrite always produces a new path, so
    /// "this object carries a `sorting_columns` footer" is a permanent fact. A stale entry for a
    /// tombstoned path is harmless because admission never sees that path again. Best-effort:
    /// a write failure costs re-probing, never correctness.
    pub(crate) fn persist_verified_sorted(&self, paths: &[String]) {
        use std::io::Write;
        let _guard = self.repair_verified_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let file_path = self.repair_verified_path();
        let write = (|| -> std::io::Result<()> {
            if let Some(dir) = file_path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&file_path)?;
            for path in paths {
                writeln!(file, "{path}")?;
            }
            Ok(())
        })();
        if let Err(e) = write {
            warn!("verified-sorted append failed ({:?}): {} — repair will re-probe these footers after a restart", file_path, e);
        }
    }

    /// Load the persisted verified-sorted paths at boot, compacting the file if
    /// it has grown past [`REPAIR_VERIFIED_PERSIST_CAP`]. Newest entries win: the
    /// tail of the file is the most recently probed.
    pub(crate) fn load_verified_sorted(&self) {
        let _guard = self.repair_verified_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let file_path = self.repair_verified_path();
        let Ok(contents) = std::fs::read_to_string(&file_path) else { return };
        let all: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
        let kept = &all[all.len().saturating_sub(REPAIR_VERIFIED_PERSIST_CAP)..];
        for path in kept {
            self.repair_verified_sorted.insert((*path).to_string());
        }
        if all.len() > kept.len()
            && let Err(e) = std::fs::write(&file_path, kept.iter().map(|p| format!("{p}\n")).collect::<String>())
        {
            warn!("verified-sorted compaction failed ({:?}): {e}", file_path);
        }
        info!(loaded = kept.len(), dropped = all.len() - kept.len(), event = "footer_repair_verified_loaded");
    }

    fn staged_intent_path(&self) -> PathBuf {
        self.maintenance_state_path("staged_intent.jsonl")
    }

    /// Append one bin's staged paths. Best-effort: a manifest write failure
    /// must never fail the compaction, only widen the VACUUM backstop's job.
    pub(crate) fn record_staged_intent(&self, entry: &StagedIntent) {
        use std::io::Write;
        let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.staged_intent_path();
        let write = (|| -> std::io::Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
            writeln!(file, "{}", serde_json::to_string(entry)?)
        })();
        if let Err(e) = write {
            warn!("staged-intent manifest append failed ({:?}): {} — orphan cleanup falls back to VACUUM", path, e);
        }
    }

    /// Drop one wave's entries, rewrite-compacting the append-only file. Called
    /// after the wave commits or after its staged parquet is cleaned up, i.e.
    /// once the entry can no longer describe an orphan.
    fn clear_staged_intent(&self, wave_ids: &[&str]) {
        let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.staged_intent_path();
        let Ok(contents) = std::fs::read_to_string(&path) else { return };
        let kept: Vec<String> = parse_staged_intents(&contents)
            .into_iter()
            .filter(|e| !wave_ids.contains(&e.wave_id.as_str()))
            .filter_map(|e| serde_json::to_string(&e).ok())
            .collect();
        let write = if kept.is_empty() { std::fs::write(&path, b"") } else { std::fs::write(&path, kept.join("\n") + "\n") };
        if let Err(e) = write {
            warn!("staged-intent manifest compaction failed ({:?}): {}", path, e);
        }
    }

    /// Resume a repair bin that a previous attempt already wrote.
    ///
    /// Returns the staged output as a `StagedBin` instead of spending another 40+ minutes
    /// rewriting the same file. Hooked at bin selection, keyed on an intent whose inputs exactly
    /// match the bin we were about to stage. The restart case falls out for free because
    /// selection is deterministic given the snapshot, and the same lookup covers a stage whose
    /// commit lost an OCC race. `commit_wave` lands it with the same lock, flush priority, liveness
    /// re-check and OCC ladder. `None` is always safe: the caller stages normally, and the
    /// declined entry's parquet falls to boot-time reconcile / VACUUM.
    async fn resumable_staged_bin(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, files: &[String]) -> Option<StagedBin> {
        use deltalake::kernel::Action;
        use object_store::{ObjectStoreExt, path::Path as OsPath};
        use std::sync::atomic::Ordering::Relaxed;
        if !self.config.maintenance.timefusion_repair_resume_enabled {
            return None;
        }
        let contents = {
            let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            std::fs::read_to_string(self.staged_intent_path()).ok()?
        };
        let wanted: HashSet<&str> = files.iter().map(String::as_str).collect();
        // Set equality, not order: the bin is a SET of inputs, and admission may
        // legitimately hand them over in a different order than last time.
        let candidates: Vec<StagedIntent> = parse_staged_intents(&contents)
            .into_iter()
            .filter(|e| e.project_id == project_id && e.target_paths.len() == files.len())
            .filter(|e| e.target_paths.iter().all(|p| wanted.contains(p.as_str())))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        // Only the paths in play — the snapshot holds ~26k files and this must
        // not parse every one of their stats blobs.
        let interest: HashSet<&str> = candidates.iter().flat_map(|e| e.target_paths.iter().chain(e.adds.iter().map(|a| &a.path))).map(String::as_str).collect();
        let (live, target_adds, store) = {
            let table = table_ref.read().await;
            let snapshot = table.snapshot().ok()?;
            let mut live: HashMap<String, Option<i64>> = HashMap::new();
            let mut target_adds: HashMap<String, deltalake::kernel::Add> = HashMap::new();
            for file in snapshot.log_data().iter() {
                let file_path = file.path().into_owned();
                if !interest.contains(file_path.as_str()) {
                    continue;
                }
                // A deletion vector makes the file's LOGICAL row count smaller
                // than its `numRecords`, and the rewrite read logical rows — so
                // comparing against `numRecords` would flag every DV'd input as
                // a mismatch and decline the very bins this exists to save.
                let dropped = file.deletion_vector_descriptor().map_or(0, |dv| dv.cardinality);
                live.insert(file_path.clone(), file.num_records().and_then(|n| i64::try_from(n).ok()).map(|n| n - dropped));
                #[allow(deprecated)]
                target_adds.insert(file_path, file.add_action());
            }
            (live, target_adds, table.log_store().object_store(None))
        };
        let live_view: HashMap<&str, Option<i64>> = live.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let now_secs = crate::support::now_secs();
        let stats = crate::observability::maintenance_stats();
        for entry in &candidates {
            match classify_resume(entry, table_name, now_secs, &live_view) {
                ResumeVerdict::Skip => continue,
                ResumeVerdict::AlreadyLanded => {
                    // The commit landed and only the bookkeeping was lost. Never
                    // re-commit: that would Remove the files it just Added.
                    info!(table_name, project_id, wave_id = %entry.wave_id, event = "staged_intent_already_landed");
                    self.clear_staged_intent(&[entry.wave_id.as_str()]);
                }
                ResumeVerdict::Stale => {
                    stats.repair_resume_declined_stale.fetch_add(1, Relaxed);
                    info!(table_name, project_id, wave_id = %entry.wave_id, event = "staged_intent_resume_stale");
                }
                ResumeVerdict::RowMismatch { target_rows, staged_rows } => {
                    stats.repair_resume_row_mismatch.fetch_add(1, Relaxed);
                    error!(
                        table_name,
                        project_id,
                        wave_id = %entry.wave_id,
                        target_rows,
                        staged_rows,
                        targets = ?entry.target_paths,
                        staged = ?entry.adds.iter().map(|a| a.path.as_str()).collect::<Vec<_>>(),
                        event = "staged_intent_resume_row_mismatch",
                        "REFUSING to resume a repair whose staged rows don't match its inputs — this would have dropped rows"
                    );
                }
                ResumeVerdict::Commit => {
                    // The one network check: a process killed mid-PUT leaves a
                    // short (or absent) object under a name the manifest already
                    // knows, and its recorded stats would still add up.
                    let mut complete = true;
                    for add in &entry.adds {
                        let meta = store.head(&OsPath::from(add.path.as_str())).await;
                        if meta.map_or(true, |m| i64::try_from(m.size).unwrap_or(-1) != add.size) {
                            complete = false;
                            break;
                        }
                    }
                    if !complete {
                        stats.repair_resume_declined_incomplete.fetch_add(1, Relaxed);
                        info!(table_name, project_id, wave_id = %entry.wave_id, event = "staged_intent_resume_incomplete");
                        continue;
                    }
                    let targets: Vec<deltalake::kernel::Add> = entry.target_paths.iter().filter_map(|p| target_adds.get(p).cloned()).collect();
                    let (removes, adds) = staged_actions(&targets, entry.adds.iter().cloned().map(Action::Add).collect(), false);
                    stats.repair_resumed.fetch_add(1, Relaxed);
                    info!(table_name, project_id, wave_id = %entry.wave_id, files = entry.target_paths.len(), event = "staged_intent_resumed");
                    return Some(StagedBin {
                        project_id: project_id.to_string(),
                        wave_id: entry.wave_id.clone(),
                        target_paths: entry.target_paths.clone(),
                        removes,
                        adds,
                        stage_store: Arc::clone(&store),
                        dedup: None,
                    });
                }
            }
        }
        None
    }

    /// Boot-time orphan sweep: delete staged parquet the Delta log doesn't
    /// reference, BY KEY (no LIST — R2 listing is a known incident source).
    /// Every failure mode degrades to a `warn!` and a no-op: the manifest is a
    /// cleanup aid, correctness never depends on it.
    pub async fn reconcile_staged_intents(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        use object_store::ObjectStoreExt;
        let path = self.staged_intent_path();
        let contents = {
            let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let Ok(contents) = std::fs::read_to_string(&path) else { return };
            contents
        };
        let entries = parse_staged_intents(&contents);
        if entries.is_empty() {
            return;
        }
        let (referenced, store) = {
            let table = table_ref.read().await;
            let Ok(snapshot) = table.snapshot() else {
                warn!("staged-intent reconcile skipped for '{table_name}': no snapshot loaded");
                return;
            };
            (snapshot.log_data().iter().map(|f| f.path().into_owned()).collect::<HashSet<String>>(), table.log_store().object_store(None))
        };
        let now_secs = crate::support::now_secs();
        let orphans = staged_orphan_deletions(&entries, table_name, now_secs, &referenced);
        // Deletes are independent single-key calls — run them concurrently so a
        // crash that left many staged bins doesn't serialize N R2 round-trips
        // in front of maintenance startup.
        let orphan_count = orphans.len();
        let deleted = futures::stream::iter(orphans)
            .map(|orphan| {
                let store = &store;
                async move {
                    match store.delete(&object_store::path::Path::from(orphan.as_str())).await {
                        // NotFound = already gone (cleanup ran, or the crash preceded the PUT).
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => 1usize,
                        Err(e) => {
                            warn!("staged-intent reconcile: delete failed for {}: {}", orphan, e);
                            0
                        }
                    }
                }
            })
            .buffer_unordered(8)
            .fold(0usize, |acc, n| async move { acc + n })
            .await;
        info!(table_name, entries = entries.len(), orphans = orphan_count, deleted, event = "staged_intent_reconciled");
        // Clear ONLY the entries this reconcile actually judged: this table's,
        // old enough to be unambiguous. Other tables' entries (and young ones)
        // stay for their own reconcile pass.
        let ids: Vec<&str> = entries
            .iter()
            .filter(|e| e.table_name == table_name && now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
            .map(|e| e.wave_id.as_str())
            .collect();
        self.clear_staged_intent(&ids);
    }

    /// Checkpoint after a tick's waves when the log has advanced enough since the
    /// last checkpoint. Owned by the wave engine rather than left to the commit count: waves cut
    /// compaction commits ~40x, so a per-N-versions cadence would checkpoint far less often exactly
    /// where the replay tail is the top CPU cost.
    async fn checkpoint_after_waves(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        /// Small on purpose: a tick's waves add ~2-3 versions, so this
        /// checkpoints every few ticks instead of every tick.
        const WAVE_CHECKPOINT_VERSIONS: u64 = 20;
        let (url, version) = {
            let g = table_ref.read().await;
            (g.table_url().to_string(), g.version().unwrap_or(0))
        };
        if self.checkpoint_lag(&url, version) >= WAVE_CHECKPOINT_VERSIONS {
            self.checkpoint_and_cleanup_table(table_ref, table_name).await;
        }
    }

    /// Versions committed since `url`'s last checkpoint (0 if never checkpointed).
    fn checkpoint_lag(&self, url: &str, version: u64) -> u64 {
        version.saturating_sub(self.checkpoint_versions.get(url).map_or(0, |e| *e))
    }

    /// One-way safety brakes, checked at wave boundaries only. In-flight bins always finish and commit.
    ///
    /// Convert overload into a smaller tick rather than an incident: durability outranks compaction,
    /// and an OOM kill means WAL recovery and quarantine. Two levels because the failure modes differ.
    /// WAL backlog can be sustained, so the brake degrades to a service floor rather than stopping
    /// outright and starving compaction. Memory near the cgroup limit is an imminent OOM: hard stop.
    pub(crate) fn light_optimize_brake(&self) -> Option<Brake> {
        use std::sync::atomic::Ordering::Relaxed;
        // NOTE: packing is no longer braked by an in-flight repair. It was, for
        // as long as the two passes shared one memory pool — a repair sort
        // measured 12.7-14.4 GB of a 15.4 GB pool, leaving a 1.3-1.5 GB packing
        // bin nowhere to go, and a packing bin that dies with `Resources
        // exhausted` compacts nothing AND takes the repair bin down with it.
        //
        // The interlock was the wrong fix for that: it made packing's
        // availability a function of repair's schedule, and repair's budget
        // (8640s) exceeds its period (3600s), with a startup pass 3 minutes
        // after every boot. Prod 2026-08-12 21:54-22:50 therefore planned 193
        // packing projects and completed ONE — every tick pausing at round 0 in
        // under a millisecond while ingest kept making small files. The two
        // passes now hold disjoint pools (`pack_pool_bytes` / `repair_pool_bytes`),
        // which removes the contention the brake was standing in for.
        if let Some(stale_buckets) = self.buffered_layer().map(|layer| layer.stale_unflushed_bucket_count()).filter(|count| *count > 0) {
            info!(stale_buckets, event = "light_optimize_flush_debt_yield");
            crate::observability::maintenance_stats().light_optimize_flush_debt_yields.fetch_add(1, Relaxed);
            return Some(Brake::Stop("stale_unflushed_buckets"));
        }
        if self.buffered_layer().is_some_and(|layer| layer.is_wal_backlog_over_threshold()) {
            info!(event = "light_optimize_wal_yield");
            crate::observability::maintenance_stats().light_optimize_wal_yields.fetch_add(1, Relaxed);
            return Some(Brake::Degrade("wal_backlog_over_threshold"));
        }
        // HOST pressure, not just our cgroup: on an over-committed host the
        // kernel's global OOM killer fires long before our 120GiB memcg limit
        // (2026-07-30 10:57: TF killed at 91.5GB anon by a GLOBAL oom while
        // the cgroup brake read healthy). /proc/meminfo is the host's inside
        // a container, so MemAvailable is exactly the number the global OOM
        // killer is racing against.
        const HOST_MEM_BRAKE_FLOOR_BYTES: u64 = 12 * 1024 * 1024 * 1024;
        if host_mem_available_bytes().is_some_and(|avail| avail < HOST_MEM_BRAKE_FLOOR_BYTES) {
            info!(event = "light_optimize_host_memory_brake");
            crate::observability::maintenance_stats().light_optimize_memory_brakes.fetch_add(1, Relaxed);
            return Some(Brake::Stop("host_memory_low"));
        }
        let limit = self.config.derived.memory_brake_limit_bytes();
        if limit > 0 && process_memory_bytes().is_some_and(|used| used > limit) {
            info!(limit, event = "light_optimize_memory_brake");
            crate::observability::maintenance_stats().light_optimize_memory_brakes.fetch_add(1, Relaxed);
            return Some(Brake::Stop("memory_brake"));
        }
        None
    }

    /// Drop bins whose files all already carry a `sorting_columns` footer, verifying them
    /// concurrently.
    ///
    /// Admission offers every un-verified sealed file because the sort tag cannot be trusted
    /// to mean the footer is sorted, so most suspects are already fine. A footer read is one
    /// ranged GET; a rewrite is a whole-file sort. Paying the reads up front in bulk leaves the
    /// round's budget for rewrites. Must be applied to every plan, including each round's
    /// re-plan, or later rounds re-select the same suspects.
    async fn drop_verified_sorted_bins(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, pass: TailPass, planned: Vec<(String, Vec<String>)>,
    ) -> Vec<(String, Vec<String>)> {
        if pass != TailPass::Repair || planned.is_empty() {
            return planned;
        }
        use futures::StreamExt;
        let before = planned.len();
        let checked: Vec<(String, Vec<String>, bool)> = futures::stream::iter(planned)
            .map(|(project_id, files)| async move {
                let sorted = self.repair_bin_already_sorted(table_ref, &files).await;
                (project_id, files, sorted)
            })
            .buffer_unordered(REPAIR_VERIFY_CONCURRENCY)
            .collect()
            .await;
        let kept: Vec<(String, Vec<String>)> = checked.into_iter().filter(|(_, _, sorted)| !sorted).map(|(p, f, _)| (p, f)).collect();
        if before != kept.len() {
            info!(table_name, cleared = before - kept.len(), remaining = kept.len(), event = "footer_repair_suspects_bulk_cleared");
        }
        kept
    }

    /// True if every file in the repair bin already carries a `sorting_columns` footer.
    ///
    /// Records the bin in `repair_verified_sorted` when true so admission stops offering it.
    /// Unreadable footer is not evidence of sortedness, so returns false.
    async fn repair_bin_already_sorted(&self, table_ref: &Arc<RwLock<DeltaTable>>, files: &[String]) -> bool {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};
        let object_store = { table_ref.read().await.log_store().object_store(None) };
        for path in files {
            let os_path = OsPath::from(path.as_str());
            let Ok(meta) = object_store.head(&os_path).await else { return false };
            let mut reader = ParquetObjectReader::new(object_store.clone(), os_path).with_file_size(meta.size);
            let Ok(pq) = reader.get_metadata(None).await else { return false };
            if pq.row_groups().iter().any(|rg| rg.sorting_columns().is_none_or(|sc| sc.is_empty())) {
                return false;
            }
        }
        for path in files {
            self.repair_verified_sorted.insert(path.clone());
        }
        self.persist_verified_sorted(files);
        info!(files = files.len(), event = "footer_repair_suspect_cleared");
        true
    }

    /// Each pass is budgeted from its own cron period because their units are orders of magnitude
    /// apart: a packing bin is a handful of small files, a repair bin is one large whole-file rewrite.
    /// Sharing the same short period gave repair a slice it could not fit, and `stage_hot_bin` discards
    /// an over-budget bin — so repair could never finish, at any backlog size.
    pub(crate) fn tail_pass_tick_budget(&self, pass: TailPass) -> std::time::Duration {
        match pass {
            TailPass::Pack => self.config.derived.tick_budget(cron_period(&self.config.maintenance.timefusion_light_optimize_schedule)),
            // Repair's run length is set OUTRIGHT, not derived from its period —
            // see `timefusion_footer_repair_budget_secs`. Tying it to the period
            // forces "frequent attempts XOR a long run", and repair needs both.
            TailPass::Repair => std::time::Duration::from_secs(self.config.maintenance.timefusion_footer_repair_budget_secs),
        }
    }

    /// Inner optimize loop for the COLD consolidate path (the 5-min hot tail
    /// moved to `stage_hot_bin`/`commit_hot_wave`). Caller is expected to hold the flush lock when
    /// a `BufferedWriteLayer` is active; the retry loop here remains as a
    /// safety net against bursts from `flush_all_now` or shutdown flushes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn optimize_table_light_inner(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, today: chrono::NaiveDate, project_id: &str, partition_filters: &[PartitionFilter],
        selected_files: &[String], target_size: i64, writer_properties: &WriterProperties, optimize_type: deltalake::operations::optimize::OptimizeType,
        min_files: usize, start_time: std::time::Instant,
    ) -> Result<()> {
        const MAX_RETRIES: usize = 4;
        // Optimize rewrites (compaction) materialize Arrow like dedup — hold a
        // maintenance-rewrite permit so it can't stack with a concurrent dedup
        // or recompress and blow the cgroup (their footprint is pool-invisible).
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
        let mut last_err: Option<deltalake::DeltaTableError> = None;
        // Pre-state file set for deriving the files this optimize adds (to warm)
        // and removes (to evict). Hoisted out of the retry loop — only a
        // successful commit (which returns) changes the file set — and scoped to
        // the one hot `(project_id, today)` partition this optimize is filtered
        // to. Sole remaining caller is the nightly consolidate sweep (up to
        // 128 passes x 4 attempts per sealed date) — still worth scoping: it
        // once walked the whole 26k-file table each time.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let (pid_marker, date_marker) = (format!("project_id={project_id}/"), format!("date={today}/"));
        let scope = [pid_marker.as_str(), date_marker.as_str()];
        let pre_uris: Option<HashSet<String>> = if track_files { Some(scoped_file_uris(&*table_ref.read().await, &scope).into_iter().collect()) } else { None };
        for attempt in 0..MAX_RETRIES {
            let table_clone = {
                let table = table_ref.read().await;
                table.clone()
            };
            if attempt == 0 {
                info!(table_name, project_id, date = %today, target_size, max_concurrent_tasks = self.config.derived.optimize_merge_tasks(), event = "light_optimize_started");
            } else {
                debug!("Light optimize retry {}/{} after OCC conflict", attempt + 1, MAX_RETRIES);
            }
            let optimize_result = table_clone
                .optimize()
                .with_filters(partition_filters)
                // Restrict the rewrite to the pre-selected sealed files so live
                // appends after selection aren't in the commit's file set (avoids
                // the OCC race on the hot today-partition).
                .with_binned_files(selected_files)
                // Cloned per attempt: the retry loop re-submits after OCC conflicts.
                .with_type(optimize_type.clone())
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(self.config.derived.optimize_merge_tasks())
                .with_writer_properties(writer_properties.clone())
                .with_min_commit_interval(tokio::time::Duration::from_secs(30))
                // Apply the compaction's Add+Remove to the materialized snapshot
                // incrementally rather than re-materializing all active files in
                // the post-commit hook (see the dedup path).
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                // Variant columns are stored as Struct{Binary, Binary} on disk; if
                // the optimize-internal Parquet read uses `schema_force_view_types=true`
                // (delta-rs's default), it returns BinaryView and the rewrite blows up
                // mid-scan with "Expected ... Binary, got ... BinaryView".
                .with_session_state(Arc::new(self.light_optimize_session_state()))
                .await;
            match optimize_result {
                Ok((new_table, metrics)) => {
                    if metrics.total_considered_files < min_files {
                        debug!(
                            "Skipping light optimization commit for table={} project={} date={}: {} files < min threshold {}",
                            table_name, project_id, today, metrics.total_considered_files, min_files
                        );
                        return Ok(());
                    }
                    let duration = start_time.elapsed();
                    info!(
                        "Light optimization completed for table={} project={} date={} in {:?} (attempt {}): {} files considered, {} removed, {} added",
                        table_name,
                        project_id,
                        today,
                        duration,
                        attempt + 1,
                        metrics.total_considered_files,
                        metrics.num_files_removed,
                        metrics.num_files_added
                    );
                    // Swap the optimized table in and refresh the cache (warm
                    // freshly-compacted files, evict the small files just
                    // tombstoned) via the shared helper.
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &scope).await;
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_conflict = is_occ_conflict_err(&msg);
                    if is_conflict {
                        crate::observability::record_optimize_conflict();
                    }
                    // "Found unmasked nulls for non-nullable StructArray" surfaces
                    // when delta-rs is mid-rewrite and the in-flight Add log lines
                    // for partition struct values aren't fully populated yet.
                    // It usually clears on a fresh re-scan, so treat as transient.
                    let is_transient_schema = msg.contains("Found unmasked nulls");
                    if (is_conflict || is_transient_schema) && attempt + 1 < MAX_RETRIES {
                        tokio::time::sleep(occ_backoff(attempt)).await;
                        last_err = Some(e);
                        continue;
                    }
                    crate::observability::record_optimize_failed();
                    error!(
                        "Light optimization operation failed for table={} project={} date={} (attempt {}): {}",
                        table_name,
                        project_id,
                        today,
                        attempt + 1,
                        e
                    );
                    return Err(anyhow::anyhow!("Light table optimization failed: {}", e));
                }
            }
        }
        let err = last_err.map(|e| e.to_string()).unwrap_or_else(|| "exhausted retries".into());
        warn!(
            "Light optimization gave up for table={} project={} date={} after {} OCC conflicts; will retry next tick: {}",
            table_name, project_id, today, MAX_RETRIES, err
        );
        Ok(())
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    /// On-demand vacuum of a single unified table (pgwire `VACUUM <table>`).
    /// `retention_hours = None` uses the configured default. Mirrors
    /// `compact_date`: resolves the table then delegates, keeping config private.
    pub async fn vacuum_named(&self, table_name: &str, retention_hours: Option<u64>) -> Result<usize> {
        let retention = retention_hours.unwrap_or(self.config.maintenance.timefusion_vacuum_retention_hours);
        let table_ref = self.get_or_create_unified_table(table_name).await?;
        Ok(self.vacuum_table("", table_name, &table_ref, retention).await)
    }

    /// Returns the number of files deleted (0 on failure — the error is logged).
    pub(crate) async fn vacuum_table(&self, project_id: &str, table_name: &str, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) -> usize {
        // Log the start of the vacuum operation
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Full vacuum lists unreferenced parquet as well as retained Remove
        // actions. Serialize that classification with every local writer and
        // refresh inside the critical section: cloning before the commit lock
        // lets a concurrent flush land a file that Full vacuum can mistake for
        // an orphan. The table RwLock alone is insufficient because commit
        // paths deliberately clone-update-swap without holding it across IO.
        let commit_lock = self.commit_lock(project_id, table_name).await;
        let _commit_guard = commit_lock.lock().await;
        if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
            error!("Vacuum aborted: failed to refresh '{}' before Full orphan sweep: {}", Self::table_label(project_id, table_name), e);
            return 0;
        }

        // Get a clone so the table RwLock is not held across object-store IO.
        // The per-physical-table commit lock above keeps this snapshot stable.
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match table_clone
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            // Full also sweeps orphaned parquet whose tombstones have already
            // left the retained log. Keep this mode: bounding the transaction
            // log must not turn old orphan files into a permanent storage leak.
            .with_mode(deltalake::operations::vacuum::VacuumMode::Full)
            .await
        {
            Ok((_, metrics)) => {
                let duration = start_time.elapsed();
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed in {:?}, deleted {} files", duration, files_deleted);

                // Log file sizes for monitoring storage savings
                if !metrics.files_deleted.is_empty() {
                    debug!("Vacuum operation details: {:?}", metrics.files_deleted);
                }

                // Update the table state after vacuum
                if refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await.is_ok() {
                    info!("Table state updated after vacuum");
                } else {
                    error!("Failed to update table state after vacuum");
                }
                files_deleted
            }
            Err(e) => {
                error!("Vacuum operation failed: {}", e);
                0
            }
        }
    }

    /// Out-of-band checkpoint + expired-log cleanup for one table. Runs on the
    /// maintenance schedule instead of in the delta-rs commit hook (`base_commit_properties`
    /// disables the hook), so a checkpoint PUT or bulk log delete failure cannot fail a landed commit.
    /// Best-effort: errors are logged, counted, and retried next tick; ingest is never touched.
    /// Checkpoints only when the version advanced by at least `checkpoint_interval` since the last
    /// checkpoint, tracked in-memory per table URL, so idle tables are skipped.
    async fn checkpoint_and_cleanup_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        use std::sync::atomic::Ordering::Relaxed;
        // Checkpoint the latest committed version, not a stale clone.
        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let (table, url, version) = {
            let g = table_ref.read().await;
            (g.clone(), g.table_url().to_string(), g.version().unwrap_or(0))
        };
        let interval = self.config.parquet.timefusion_checkpoint_interval.max(1);
        let lag = self.checkpoint_lag(&url, version);
        // Gauge: max lag seen this tick (job resets to 0 first). A large, growing
        // value means the checkpoint task is failing or wedged.
        crate::observability::maintenance_stats().checkpoint_lag_versions.fetch_max(lag, Relaxed);
        if lag < interval {
            return;
        }
        // Each store-heavy op is individually bounded so one wedged R2 call
        // can't starve the rest of the sweep (and each timeout lands in the
        // right failure counter). 600s is ~35x the largest observed catch-up
        // (a 179k-version lag checkpointed in 17s, 2026-07-14); hitting it
        // means a stuck backend, not a big table. Dropping the future
        // mid-checkpoint is safe: the checkpoint PUT is atomic and retried
        // next tick.
        const CHECKPOINT_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
        match tokio::time::timeout(CHECKPOINT_OP_TIMEOUT, deltalake::checkpoints::create_checkpoint(&table, None)).await {
            Ok(Ok(())) => {
                // Verify the just-written checkpoint is a readable Parquet before
                // advancing the boundary or letting cleanup prune JSON behind it.
                // A foreign/corrupt checkpoint object (an S3 error/Select body
                // written over it, 2026-07-17) must never gate log cleanup — the
                // JSON commit log is the only recovery source, and today's
                // recovery depended on it still being present.
                let store = table.log_store().object_store(None);
                match last_checkpoint_readable(&store).await {
                    Ok(true) => {
                        self.checkpoint_versions.insert(url, version);
                        crate::observability::maintenance_stats().checkpoints_created.fetch_add(1, Relaxed);
                        debug!("out-of-band checkpoint created + verified for '{}' at v{}", table_name, version);
                    }
                    Ok(false) => {
                        crate::observability::record_checkpoint_corrupt();
                        error!(
                            "checkpoint for '{}' at v{} is unreadable after write (foreign/corrupt object) — withholding log cleanup to preserve the JSON recovery log; PAGE",
                            table_name, version
                        );
                        return;
                    }
                    Err(e) => {
                        crate::observability::record_checkpoint_failed();
                        warn!("could not verify checkpoint for '{}' at v{}: {} — withholding log cleanup this tick", table_name, version, e);
                        return;
                    }
                }
            }
            Ok(Err(e)) => {
                crate::observability::record_checkpoint_failed();
                warn!("out-of-band checkpoint failed for '{}' at v{}: {} (retry next tick)", table_name, version, e);
                return; // no fresh checkpoint boundary → skip cleanup this tick
            }
            Err(_) => {
                crate::observability::record_checkpoint_failed();
                warn!("out-of-band checkpoint for '{}' timed out after {CHECKPOINT_OP_TIMEOUT:?} (retry next tick)", table_name);
                return;
            }
        }
        // Log cleanup prunes only up to a checkpoint boundary, so run it after a
        // successful checkpoint. Uses the table's logRetentionDuration.
        match tokio::time::timeout(CHECKPOINT_OP_TIMEOUT, deltalake::checkpoints::cleanup_metadata(&table, None)).await {
            Ok(Ok(n)) if n > 0 => {
                crate::observability::maintenance_stats().log_files_cleaned.fetch_add(n as u64, Relaxed);
                debug!("out-of-band log cleanup removed {} expired files for '{}'", n, table_name);
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                crate::observability::record_log_cleanup_failed();
                warn!("out-of-band log cleanup failed for '{}': {} (retry next tick)", table_name, e);
            }
            Err(_) => {
                crate::observability::record_log_cleanup_failed();
                warn!("out-of-band log cleanup for '{}' timed out after {CHECKPOINT_OP_TIMEOUT:?} (retry next tick)", table_name);
            }
        }
    }

    /// Reconcile a table's active Add entries against object-store truth and commit `Remove`
    /// actions for any whose parquet is missing.
    ///
    /// Repairs dangling Adds left by a commit-path parquet deletion. The rows were re-flushed into
    /// fresh files, so the `Remove` is lossless — it just stops queries 404-ing on dead paths. A
    /// nonzero removal count means committed data was destroyed elsewhere, so it is logged and
    /// counted. delta-rs `filesystem_check` does the list-and-diff; this forces hooks off and surfaces
    /// the count.
    async fn reconcile_dangling_adds(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let table = { table_ref.read().await.clone() };
        match table.filesystem_check().with_commit_properties(base_commit_properties()).await {
            Ok((_, metrics)) => {
                let n = metrics.files_removed.len();
                if n > 0 {
                    crate::observability::record_dangling_removed(n as u64);
                    warn!(
                        "reconcile: '{}' had {} dangling Add(s) (committed parquet missing from store) — Remove'd: {:?}",
                        table_name, n, metrics.files_removed
                    );
                    let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
                }
            }
            Err(e) => {
                crate::observability::maintenance_stats().reconcile_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!("reconcile filesystem_check failed for '{}': {} (retry next tick)", table_name, e);
            }
        }
    }

    /// One out-of-band checkpoint + log-cleanup tick across every registered
    /// table. Driven by the checkpoint cron job (and directly by tests).
    pub async fn run_checkpoint_maintenance(&self) {
        // Reset the lag gauge so it reflects THIS tick's worst table.
        crate::observability::maintenance_stats().checkpoint_lag_versions.store(0, std::sync::atomic::Ordering::Relaxed);
        for (_project_id, name, table) in self.all_tables().await {
            self.checkpoint_and_cleanup_table(&table, &name).await;
        }
    }

    /// One dangling-Add reconcile tick across every registered table. Driven by
    /// the reconcile cron job (and directly by tests).
    pub async fn run_reconcile_maintenance(&self) {
        for (_project_id, name, table) in self.all_tables().await {
            self.reconcile_dangling_adds(&table, &name).await;
        }
    }

    /// Test-only: run `probe_commit_landed` against the table's current active
    /// files. Returns true iff the probe reports `Landed` (every active file's
    /// object is present). Lets an e2e test exercise the landed-vs-not-landed
    /// decision deterministically against a real store, without fighting
    /// delta-rs's post-commit error timing.
    #[cfg(any(test, feature = "e2e"))]
    #[allow(deprecated)] // add_action() is deprecated but fine for a test-only probe
    pub async fn test_probe_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let adds: Vec<deltalake::kernel::Action> = {
            let guard = table_ref.read().await;
            guard.snapshot()?.log_data().iter().map(|f| deltalake::kernel::Action::Add(f.add_action())).collect()
        };
        Ok(matches!(self.probe_commit_landed(&table_ref, &adds).await, CommitProbe::Landed))
    }

    /// Test-only: probe with a fabricated Add whose path was never committed.
    /// The probe must report NOT landed (the "commit didn't write our adds to
    /// the log" case that the flush error arm treats as safe-to-clean-up).
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_probe_bogus_not_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let bogus = deltalake::kernel::Action::Add(deltalake::kernel::Add {
            path: "project_id=nope/date=1970-01-01/part-never-committed.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        });
        Ok(matches!(self.probe_commit_landed(&table_ref, &[bogus]).await, CommitProbe::NotLanded))
    }

    /// Test-only: number of `.checkpoint.parquet` objects in the table's
    /// `_delta_log`. Lets a test assert the commit path does NOT checkpoint
    /// (Phase 1) and that the out-of-band task DOES (Phase 2).
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_checkpoint_file_count(&self, project_id: &str, table_name: &str) -> Result<usize> {
        use futures::StreamExt;
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let store = { table_ref.read().await.log_store().object_store(None) };
        let prefix = object_store::path::Path::from("_delta_log");
        let mut n = 0;
        let mut stream = store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            if item?.location.as_ref().contains(".checkpoint.parquet") {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Test-only: delete the first active parquet object of a table directly
    /// from the store (no Delta commit), reproducing the commit-path deletion
    /// bug so a test can then assert `reconcile_dangling_adds` heals the dangling
    /// Add. Returns the deleted relative path.
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_delete_first_active_file(&self, project_id: &str, table_name: &str) -> Result<String> {
        use object_store::ObjectStoreExt;
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let guard = table_ref.read().await;
        let snap = guard.snapshot()?;
        let path = snap.log_data().iter().next().map(|f| f.path().into_owned()).ok_or_else(|| anyhow::anyhow!("no active files to delete"))?;
        guard.log_store().object_store(None).delete(&object_store::path::Path::from(path.as_str())).await?;
        Ok(path)
    }

    /// Flatten unified + custom project tables into one (project_id, name, handle)
    /// list — `project_id` empty for unified tables (shared by all default
    /// projects). A SNAPSHOT by design: every maintenance pass must iterate this
    /// instead of `MAP.read().await.iter()`, because holding a table-map read guard across the pass's
    /// awaits lets one queued writer block every subsequent reader — tokio's RwLock is
    /// write-preferring — and wedge all maintenance jobs.
    pub(crate) async fn all_tables(&self) -> Vec<(String, String, Arc<RwLock<DeltaTable>>)> {
        let mut out: Vec<(String, String, Arc<RwLock<DeltaTable>>)> =
            self.unified_tables.read().await.iter().map(|(n, t)| (String::new(), n.clone(), t.clone())).collect();
        out.extend(self.custom_project_tables.read().await.iter().map(|((p, n), t)| (p.clone(), n.clone(), t.clone())));
        out
    }

    /// Human label for a table from `all_tables`, matching the pre-existing
    /// per-job log wording so operator greps keep working.
    pub(crate) fn table_label(project_id: &str, table_name: &str) -> String {
        if project_id.is_empty() { format!("unified table '{table_name}'") } else { format!("custom project '{project_id}' table '{table_name}'") }
    }

    /// Get table statistics using the statistics extractor
    pub async fn get_table_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        self.statistics_extractor.extract_statistics(table, project_id, table_name).await
    }

    /// Clear the statistics cache
    pub async fn clear_statistics_cache(&self) {
        self.statistics_extractor.clear_cache().await
    }

    /// Foyer cache handle (None if Foyer disabled). Test hook for harnesses
    /// that want hit/miss assertions; also used by the warm-cache path.
    pub fn object_store_cache(&self) -> Option<&Arc<SharedFoyerCache>> {
        self.object_store_cache.as_ref()
    }

    /// Invalidate statistics for a specific table
    pub async fn invalidate_table_statistics(&self, project_id: &str, table_name: &str) {
        self.statistics_extractor.invalidate(project_id, table_name).await
    }

    /// Gracefully shutdown the database, including cache and maintenance tasks
    /// Signal maintenance/background tasks (scheduler, dedup sweep, coalescer)
    /// to stop. Idempotent; `shutdown()` also fires it. Called early in the
    /// drain so an in-flight sweep bails before the buffered-layer flush.
    pub fn cancel_maintenance(&self) {
        self.maintenance_shutdown.cancel();
    }

    /// True once maintenance/background tasks have been told to stop. Exposed for tests; on a
    /// live instance `true` means every cron job is dead.
    pub fn is_maintenance_cancelled(&self) -> bool {
        self.maintenance_shutdown.is_cancelled()
    }

    /// Clone for long-lived background tasks (cron loops, DML coalescer):
    /// omits the cancel guard so a task waiting on `maintenance_shutdown`
    /// doesn't keep its own kill-switch alive (guard-holding clone captured by
    /// the task → last-drop cancellation unreachable).
    pub(crate) fn background_clone(&self) -> Self {
        Self { _maintenance_cancel_guard: None, ..self.clone() }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_by(tokio::time::Instant::now() + self.config.buffer.stop_grace()).await
    }

    /// Graceful shutdown; every phase that can block on a slow/stuck Delta or
    /// S3 backend — the DML-coalescer drain and the foyer `close()` (whose
    /// flush-on-close overran for minutes in prod, stalling `wal.lock`
    /// release, #82) — is bounded by `deadline`, the remainder of the
    /// process-wide stop grace shared with `BufferedWriteLayer::shutdown_by`.
    /// Un-drained deferred Delta legs are the coalescer's documented
    /// crash-equivalent loss (mem-leg values survive in the WAL); foyer close
    /// abandons only rebuildable cache warmth.
    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> Result<()> {
        info!("Shutting down TimeFusion database...");

        // Flush deferred DML merges before anything is torn down. The drain
        // task also runs a final drain on cancellation, but doing it here
        // deterministically (drains are serialized + idempotent) means
        // shutdown doesn't race the task's select loop. Bounded by `deadline`:
        // an un-drained group's deferred Delta leg is the SAME accepted,
        // WAL-surfaced loss a crash incurs (dml_coalescer durability contract —
        // mem-leg rows are WAL-durable; only the Delta leg for rows already in
        // Delta is at risk). Better than overrunning the stop grace on a
        // slow/stuck Delta backend and being SIGKILLed mid-drain, which loses
        // the same legs AND stalls wal.lock release (issue #82).
        if let Some(coalescer) = self.dml_coalescer()
            && tokio::time::timeout_at(deadline, coalescer.drain(self)).await.is_err()
        {
            warn!("DML coalescer drain exceeded shutdown deadline — un-drained deferred Delta legs lost (crash-equivalent; mem-leg values survive in WAL)");
        }

        // Cancel maintenance tasks
        self.maintenance_shutdown.cancel();

        // Shutdown batch queue if present
        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            if tokio::time::timeout_at(deadline, queue.shutdown()).await.is_err() {
                warn!("Batch queue shutdown exceeded shutdown deadline — proceeding with process teardown");
            }
        }

        // Log final cache stats and shutdown cache
        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown_by(deadline).await?;
        }

        // Close PostgreSQL connection pool if present
        if let Some(ref pool) = self.config_pool
            && tokio::time::timeout_at(deadline, pool.close()).await.is_err()
        {
            warn!("PostgreSQL pool close exceeded shutdown deadline — dropping connections on process exit");
        }

        info!("Database shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod date_coverage_recovery_tests {
    use super::*;

    /// The read path serves `[day_start, covered_through)` from a date entry, so
    /// a hole anywhere in the run would claim hours no build aggregated. These
    /// pin that a gap refuses outright rather than being papered over by taking
    /// the maximum end — which a span-based union would silently do.
    #[test]
    fn a_gap_refuses_and_only_a_true_run_answers() {
        let hour = 3_600_000_000i64;
        // Contiguous from the first row: answers the end of the run.
        assert_eq!(Database::contiguous_coverage_end(hour, &mut [(hour, 2 * hour), (2 * hour, 5 * hour)]), Some(5 * hour));
        // Overlapping slices are still a run.
        assert_eq!(Database::contiguous_coverage_end(hour, &mut [(hour, 3 * hour), (2 * hour, 4 * hour)]), Some(4 * hour));
        // Out of order input is sorted, not rejected.
        assert_eq!(Database::contiguous_coverage_end(hour, &mut [(2 * hour, 5 * hour), (hour, 2 * hour)]), Some(5 * hour));
        // A HOLE between 2h and 3h: refuse, do not answer 5h.
        assert_eq!(Database::contiguous_coverage_end(hour, &mut [(hour, 2 * hour), (3 * hour, 5 * hour)]), None);
        // Coverage starts AFTER the partition's first row, so the opening rows
        // were never aggregated — refuse.
        assert_eq!(Database::contiguous_coverage_end(hour, &mut [(2 * hour, 5 * hour)]), None);
        // Starting at or before the first row is fine; the read path clamps.
        assert_eq!(Database::contiguous_coverage_end(2 * hour, &mut [(hour, 5 * hour)]), Some(5 * hour));
        assert_eq!(Database::contiguous_coverage_end(hour, &mut []), None);
    }

    /// A ramping tier must not pin the fleet gauge; a starved one must.
    ///
    /// Prod 2026-08-24: a two-hour-old `dashboard_level_1m_v1` at 2 days pinned
    /// the fleet gauge to 2 while `dashboard_1m_v3`, `dashboard_1h_v2` and both
    /// otel_metrics tiers sat at 30 — running the whole cluster in
    /// coverage-short mode, which OVERRIDES the journal ceiling, for an
    /// auxiliary tier nobody queries.
    #[test]
    fn a_ramping_tier_abstains_from_the_fleet_gauge_but_a_starved_one_pins_it() {
        // The prod shape: a real tier seeds 30, the ramping tier abstains, so
        // the fleet stays 30 instead of collapsing to 2.
        let seeded = fold_fleet_gauge(0, 30, false, false).expect("a real tier always counts");
        assert_eq!(seeded, 30);
        assert_eq!(fold_fleet_gauge(seeded, 2, true, true), None, "a ramping tier must not drag an established fleet value down");

        // ORDER-INDEPENDENT: the ramping tier listed FIRST seeds provisionally,
        // and the real tier overwrites rather than minimising into it. Without
        // this, schema order alone decided whether the fleet read 2 or 30.
        assert_eq!(fold_fleet_gauge(0, 2, false, true), Some(2), "with nothing real yet, even a ramping tier is better than no gauge");
        assert_eq!(fold_fleet_gauge(2, 30, false, false), Some(30), "the first real tier REPLACES a provisional value");

        // A fresh deployment is all-ramping, and must still publish something —
        // abstaining everywhere left the gauge at its start value, which is how
        // `the_backfill_ceiling_defers_enqueueing_without_stopping_the_pass`
        // caught this.
        assert_eq!(fold_fleet_gauge(u64::MAX, 3, false, true), Some(3));

        // And the property that must NOT be lost: a genuinely starved tier still
        // pins the fleet, because that is what arms the ceiling override for the
        // tier every 7d/30d query reads.
        assert_eq!(fold_fleet_gauge(seeded, 5, true, false), Some(5));
        assert_eq!(fold_fleet_gauge(5, 30, true, false), Some(5), "min, not last-writer");
    }
}
