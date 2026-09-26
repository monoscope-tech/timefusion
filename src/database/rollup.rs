//! Rollup coverage accounting and the maintenance scheduler loops.
//!
//! A slice of `database`, not a layer over it: these are `Database` methods that
//! happen to live in their own file.

use super::*;

use datafusion::physical_plan::ExecutionPlan;

/// Release oversized backing buffers before a rollup sort charges its input.
#[derive(Debug)]
struct CompactRollupSortInput(Arc<dyn ExecutionPlan>);

impl datafusion::physical_plan::DisplayAs for CompactRollupSortInput {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CompactRollupSortInput")
    }
}

impl ExecutionPlan for CompactRollupSortInput {
    no_physical_exprs!();
    fn name(&self) -> &'static str {
        "CompactRollupSortInput"
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.0.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] =
            children.try_into().map_err(|_| datafusion::common::DataFusionError::Internal("CompactRollupSortInput requires one child".into()))?;
        Ok(Arc::new(Self(input)))
    }
    fn execute(
        &self, partition: usize, context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        use futures::TryStreamExt;
        let stream = self.0.execute(partition, context)?.map_ok(crate::write::mem_buffer::compact_batch);
        Ok(datafusion::physical_plan::coop::make_cooperative(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(self.schema(), stream))))
    }
}

#[derive(Debug)]
struct CompactRollupSortInputs;

/// Keep downstream spill workspace available while the sort chooses its merge fan-in.
#[derive(Debug)]
struct RollupSortHeadroom(Arc<dyn ExecutionPlan>);

impl datafusion::physical_plan::DisplayAs for RollupSortHeadroom {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RollupSortHeadroom")
    }
}

impl ExecutionPlan for RollupSortHeadroom {
    no_physical_exprs!();
    fn name(&self) -> &'static str {
        "RollupSortHeadroom"
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.0.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] =
            children.try_into().map_err(|_| datafusion::common::DataFusionError::Internal("RollupSortHeadroom requires one child".into()))?;
        Ok(Arc::new(Self(input)))
    }
    fn execute(
        &self, partition: usize, context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        use datafusion::execution::memory_pool::MemoryConsumer;
        use futures::TryStreamExt;
        let headroom = MemoryConsumer::new("RollupSortHeadroom").register(context.memory_pool());
        headroom.try_grow(context.session_config().options().execution.sort_spill_reservation_bytes)?;
        let stream = self.0.execute(partition, context)?;
        let stream = futures::stream::try_unfold((stream, Some(headroom)), |(mut stream, headroom)| async move {
            let batch = stream.try_next().await?;
            // The blocking sort has chosen its final merge before its first batch.
            drop(headroom);
            Ok(batch.map(|batch| (batch, (stream, None))))
        });
        Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(self.schema(), stream)))
    }
}

impl datafusion::physical_optimizer::PhysicalOptimizerRule for CompactRollupSortInputs {
    fn name(&self) -> &str {
        "CompactRollupSortInputs"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &datafusion::common::config::ConfigOptions) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
        plan.transform_up(|node| {
            let Some(sort) = node.downcast_ref::<datafusion::physical_plan::sorts::sort::SortExec>() else {
                return Ok(Transformed::no(node));
            };
            if sort.input().is::<CompactRollupSortInput>() {
                return Ok(Transformed::no(node));
            }
            let input = Arc::new(CompactRollupSortInput(Arc::clone(sort.input())));
            let sort = datafusion::physical_plan::replace_children_if_necessary(node, vec![input])?;
            Ok(Transformed::yes(Arc::new(RollupSortHeadroom(sort)) as Arc<dyn ExecutionPlan>))
        })
        .data()
    }
}

/// Nice value for maintenance runtime threads. Positive = lower priority, so the
/// kernel schedules the pgwire runtime ahead of compaction whenever both are
/// runnable. 5 is a clear preference without starving maintenance outright.
/// Only `setpriority` reads it, and only on Linux — prod is the only place this
/// applies, but a dev build on macOS must not fail `-D warnings` over it.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const MAINTENANCE_THREAD_NICE: i32 = 5;

/// Longest an idle coordinator worker parks without an enqueue signal.
const COORDINATOR_IDLE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

impl Database {
    pub(super) fn rollup_generation_current(source: &str, target: &str, project: &str, date: &str, coverage: &RollupCoverage) -> bool {
        let Some(spec) = get_schema(source).and_then(|schema| schema.rollups.iter().find(|spec| spec.table_name(source) == target)) else {
            return false;
        };
        let measures = coverage.measures.as_ref().map(|names| names.iter().cloned().collect::<Vec<_>>());
        coverage.generation == crate::rollup::generation_id(spec, source, project, date, coverage.source_fp, measures.as_deref())
    }

    /// Why a coverage cell cannot serve this route: a stale generation first, then
    /// the measures its files actually materialized — a cell missing one serves
    /// NULLs, so it drops to the raw fringe.
    fn coverage_decline(route: &crate::rollup::RoutedRollup, project: &str, date: &str, coverage: &RollupCoverage) -> Option<crate::rollup::MissReason> {
        (!Self::rollup_generation_current(&route.source, &route.target, project, date, coverage))
            .then_some(crate::rollup::MissReason::StaleCoverage)
            .or_else(|| (!route.measures_available(coverage.measures.as_ref())).then_some(crate::rollup::MissReason::MeasureNotStored))
    }

    pub(crate) async fn rollup_sql(
        &self, logical_plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
    ) -> std::result::Result<Option<RollupRewrite>, crate::rollup::MissReason> {
        // Checked before the matcher: `match_aggregates` plans one statement per filtered
        // measure, and with the feature off that cost must not be paid.
        if self.bypass_rollup {
            return Ok(None);
        }
        let routes = crate::rollup::match_aggregates(logical_plan, session).await?;
        if routes.is_empty() {
            return Ok(None);
        }
        // A cross-project route reads every project at once, so a per-project allowlist cannot
        // be honoured: enabled only when the rollout is on for everyone.
        let enabled = match routes[0].project_id.as_deref() {
            Some(project) => self.config.maintenance.rollup_read_enabled_for(project),
            None => self.config.maintenance.timefusion_rollup_read_projects.is_none(),
        };
        if !enabled {
            return Ok(None);
        }
        // Try every viable tier, best first, taking the first actually built across the window.
        let mut best_miss = None;
        for route in routes {
            match self.rollup_rewrite_for(route, session).await {
                Ok(Some(rewrite)) => return Ok(Some(rewrite)),
                Ok(None) => return Ok(None),
                // A measure decline outranks whatever an earlier spec reported; plain `.or()`
                // keeps the first reason and masks it.
                Err(reason) => {
                    best_miss = if matches!(reason, crate::rollup::MissReason::MeasureNotStored) { Some(reason) } else { best_miss.or(Some(reason)) }
                }
            }
        }
        Err(best_miss.unwrap_or(crate::rollup::MissReason::NotBuilt))
    }

    /// Resolve ONE candidate tier against its coverage, or say why it cannot serve.
    async fn rollup_rewrite_for(
        &self, route: crate::rollup::RoutedRollup, _session: &datafusion::execution::context::SessionState,
    ) -> std::result::Result<Option<RollupRewrite>, crate::rollup::MissReason> {
        let end = route.hi.checked_sub(1).ok_or(crate::rollup::MissReason::UnboundedTime)?;
        let dates = window_dates(route.lo, end).ok_or(crate::rollup::MissReason::IncompleteCoverage)?;
        // A cross-project route cannot see a custom-storage project's table, so its coverage
        // would be assumed rather than proved. Refuse instead.
        if route.project_id.is_none() && self.custom_storage_keys().await.iter().any(|(_, table)| table == &route.source) {
            return Err(crate::rollup::MissReason::IncompleteCoverage);
        }
        // One pass over the add actions for the whole window: asking per date rebuilds the
        // entire add-actions batch each time, which dominates planning cost on a large table.
        let lookup_project = route.project_id.clone().unwrap_or_else(|| "default".to_string());
        let source_table = self.resolve_table(&lookup_project, &route.source).await.map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?;
        let fingerprints = {
            let table = source_table.read().await;
            // Unbounded (`i64::MAX`) to match how the write side stamps `source_rows` and the
            // date-level `source_fp`; the computation must stay identical on both sides.
            Self::partition_stats_bounded(&table, tiebreak_of(&route.source), &|_, _| i64::MAX).map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?
        };
        // A project's own live stats for a date, falling back to the unified table's row (a
        // project on "default" storage has no row of its own).
        fn stats_of<'a>(fingerprints: &'a HashMap<(String, String), PartitionStats>, project: &str, date: &str) -> Option<&'a PartitionStats> {
            fingerprints.get(&(project.to_string(), date.to_string())).or_else(|| fingerprints.get(&("default".to_string(), date.to_string())))
        }
        // Per-file `(max_ts, rows)` for the bounded-witness rescue. Loaded lazily, at
        // most once per route call, and only when some slice fails the cheap
        // whole-partition compare while carrying a bounded witness.
        let mut file_rows: Option<std::sync::Arc<crate::database::maintain::PartitionFileRows>> = None;
        // Projects come from the SOURCE, never the tier, so a project with no rollup still
        // counts against coverage. Test the window, not just the date.
        let window_dates: HashSet<String> = dates.iter().map(chrono::NaiveDate::to_string).collect();
        let projects: Vec<String> = match &route.project_id {
            Some(project) => vec![project.clone()],
            None => fingerprints
                .iter()
                .filter(|((_, date), stats)| window_dates.contains(date) && stats.overlaps(route.lo, route.hi))
                .map(|((project, _), _)| project.clone())
                .sorted_unstable()
                .dedup()
                .collect(),
        };
        if projects.is_empty() {
            return Err(crate::rollup::MissReason::NotBuilt);
        }
        let target_table = self.resolve_table(&lookup_project, &route.target).await.map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?;
        let output = {
            let table = target_table.read().await;
            self.rollup_output_coverage(&route.source, (&route.target, &table, &lookup_project), &fingerprints)
                .map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?
        };
        // Buffered rows are absent from every rollup partition; the earliest project's bound
        // governs, and everything at or above it is read raw.
        let buffered = projects
            .iter()
            .filter_map(|project| self.buffered_layer().and_then(|layer| layer.min_buffered_micros(project, &route.source, route.lo, end)))
            .min();
        let mut generations = Vec::with_capacity(dates.len());
        let mut ticket = Vec::with_capacity(dates.len());
        let mut slice_ticket = Vec::new();
        let mut accepted_output = maintain::RollupOutputCoverage::default();
        let mut miss = None;
        // Recorded on the way out, not at the gates: `dml.rs` already counts a total decline.
        let mut measure_declined = false;
        // Projects that proved coverage, with their ranges; intersected below. Projects with no
        // coverage go to `raw_only` — including them here would empty the intersection.
        let mut covered_projects: Vec<(String, Vec<(i64, i64)>)> = Vec::with_capacity(projects.len());
        let mut raw_only: Vec<String> = Vec::new();
        for project in &projects {
            let mut covered: Vec<(i64, i64)> = Vec::new();
            for date in &dates {
                let day = *date;
                let date = date.to_string();
                let key = (project.clone(), route.source.clone(), route.target.clone(), date.clone());
                let day_start = date_start_micros(&date).ok_or(crate::rollup::MissReason::IncompleteCoverage)?;
                let Some(coverage) = self.rollup_coverage.get(&key) else {
                    // Deliberately does NOT set `miss`: after a restart only slice coverage is
                    // recovered, and the slice loop below may still cover the window whole.
                    // Counted by cause: an invalidation record means the coverage EXISTED
                    // and `apply_rollup_hours` removed it — destruction that otherwise
                    // reads as `not_built` and points the blame at the build lane.
                    match self.rollup_invalidated_at.contains_key(&(project.clone(), route.source.clone(), date.clone())) {
                        true => metrics::counter!(crate::database::scan_metric_names::ROLLUP_COVERAGE_ABSENT_INVALIDATED).increment(1),
                        false => metrics::counter!(crate::database::scan_metric_names::ROLLUP_COVERAGE_ABSENT_NEVER_BUILT).increment(1),
                    }
                    continue;
                };
                let source_fp = stats_of(&fingerprints, project, &date).map_or(0, |stats| stats.fingerprint);
                let source_epoch = self.rollup_source_epochs.get(&(project.clone(), route.source.clone(), date.clone())).map_or(0, |entry| *entry.value());
                let moved = !coverage.matches_day(source_fp, source_epoch);
                if moved {
                    match coverage.source_fp != source_fp {
                        true => metrics::counter!(crate::database::scan_metric_names::ROLLUP_STALE_FP_MOVED).increment(1),
                        false => metrics::counter!(crate::database::scan_metric_names::ROLLUP_STALE_EPOCH_MOVED).increment(1),
                    }
                }
                if let Some(reason) =
                    moved.then_some(crate::rollup::MissReason::StaleCoverage).or_else(|| Self::coverage_decline(&route, project, &date, &coverage))
                {
                    measure_declined |= reason == crate::rollup::MissReason::MeasureNotStored;
                    miss = miss.or(Some(reason));
                    continue;
                }
                debug!(project_id = %project, source = %route.source, target = %route.target, date, "rollup coverage selected");
                // The build's own bound, not the day end: for a day still being written the
                // build stops short, and reading past it serves buckets never aggregated.
                let end = coverage.covered_through.min(day_start + DAY_MICROS);
                if end <= day_start {
                    miss = miss.or(Some(crate::rollup::MissReason::NotBuilt));
                    continue;
                }
                let proven = intersect_ranges(&[(day_start, end)], output.ranges(project, day, &coverage));
                if proven.is_empty() {
                    miss = miss.or(Some(crate::rollup::MissReason::IncompleteCoverage));
                    continue;
                }
                covered.extend(proven.iter().copied());
                accepted_output.record(project, day, &coverage, &proven);
                if coverage.output != RollupOutputEvidence::Empty {
                    generations.extend(proven.into_iter().map(|range| crate::rollup::GenerationRange {
                        project: project.clone(),
                        date: date.clone(),
                        generation: coverage.generation.clone(),
                        range,
                    }));
                }
                ticket.push((key, source_fp, source_epoch, coverage.generation.clone()));
            }
            // Per date: a date is readable from the tier only if every slice covering it
            // witnessed the partition as it stands now.
            let by_date: HashMap<String, Vec<(RollupSliceCoverageKey, RollupCoverage)>> = self
                .rollup_slice_coverage
                .iter()
                .filter_map(|entry| {
                    let (slice_project, source, target, start, end) = entry.key();
                    let overlaps = slice_project == project && source == &route.source && target == &route.target && *start < route.hi && *end > route.lo;
                    let date = overlaps.then(|| chrono::DateTime::from_timestamp_micros(*start))??.date_naive().to_string();
                    Some((date, (entry.key().clone(), entry.value().clone())))
                })
                .into_group_map();
            for (date, slices) in by_date {
                let day = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d").map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?;
                let current = stats_of(&fingerprints, project, &date).and_then(|stats| u64::try_from(stats.rows).ok());
                // Per slice, not all-or-nothing over the date: the witness states the WHOLE
                // partition's row count, so slices agreeing with it are independently current.
                // Do NOT fall back to `coverage.source_fp` — a slice's own fingerprint hashes
                // only that slice's files, so it can never equal the whole-partition value.
                let mut fresh = Vec::with_capacity(slices.len());
                let mut stale = Vec::new();
                for (key, coverage) in slices {
                    if coverage.matches_slice(current, None) {
                        fresh.push((key, coverage));
                        continue;
                    }
                    // The whole-partition witness disagreed — which any ingest anywhere
                    // in the day causes, and 96.8% of measured staleness is exactly that.
                    // Re-prove against the BOUNDED witness before refusing: rows in
                    // files wholly below `covered_through`, which a tail append cannot
                    // move. The per-file pass is loaded at most once per route call and
                    // only on this path, so a day with no stale-looking slice never
                    // pays for it.
                    if self.config.maintenance.timefusion_rollup_bounded_witness && coverage.source_rows_below.is_some() {
                        if file_rows.is_none() {
                            let table = source_table.read().await;
                            let version = table.version().unwrap_or(u64::MAX);
                            file_rows = Some(match self.rollup_file_rows_cache.get(&route.source).filter(|hit| hit.0 == version) {
                                Some(hit) => std::sync::Arc::clone(&hit.1),
                                None => {
                                    let fresh = std::sync::Arc::new(Self::partition_file_rows(&table).unwrap_or_default());
                                    self.rollup_file_rows_cache.insert(route.source.clone(), (version, std::sync::Arc::clone(&fresh)));
                                    fresh
                                }
                            });
                        }
                        let files = file_rows
                            .as_ref()
                            .and_then(|map| map.get(&(project.clone(), date.clone())).or_else(|| map.get(&("default".to_string(), date.clone()))));
                        if files.is_some_and(|files| coverage.matches_slice(current, crate::rollup::rows_below(files, coverage.covered_through))) {
                            metrics::counter!(scan_metric_names::ROLLUP_WITNESS_BOUNDED_RESCUED).increment(1);
                            fresh.push((key, coverage));
                            continue;
                        }
                        metrics::counter!(scan_metric_names::ROLLUP_WITNESS_BOUNDED_STALE_TOO).increment(1);
                    }
                    stale.push((key, coverage));
                }
                if !stale.is_empty() {
                    miss = miss.or(Some(crate::rollup::MissReason::StaleCoverage));
                    // `no_witness` = unverifiable, cleared only by a republish; `moved` = the
                    // partition genuinely changed; `no_source_rows` = no live fingerprint.
                    for (_, coverage) in &stale {
                        metrics::counter!(stale_coverage_metric(coverage.source_rows, current)).increment(1);
                    }
                }
                for (key, coverage) in fresh {
                    // Per slice: one built before the measure existed sends only ITS range raw.
                    if let Some(reason) = Self::coverage_decline(&route, project, &date, &coverage) {
                        measure_declined |= reason == crate::rollup::MissReason::MeasureNotStored;
                        miss = miss.or(Some(reason));
                        continue;
                    }
                    let proven = intersect_ranges(&[(key.3, key.4)], output.ranges(project, day, &coverage));
                    if proven.is_empty() {
                        miss = miss.or(Some(crate::rollup::MissReason::IncompleteCoverage));
                        continue;
                    }
                    covered.extend(proven.iter().copied());
                    accepted_output.record(project, day, &coverage, &proven);
                    if coverage.output != RollupOutputEvidence::Empty {
                        generations.extend(proven.into_iter().map(|range| crate::rollup::GenerationRange {
                            project: project.clone(),
                            date: date.clone(),
                            generation: coverage.generation.clone(),
                            range,
                        }));
                    }
                    slice_ticket.push((key, coverage.source_fp, coverage.generation.clone()));
                }
            }
            let covered = crate::write::mem_buffer::merge_ranges(covered);
            // Uncovered by both routes: read this project raw while the others still route.
            if covered.is_empty() {
                miss = miss.or(Some(crate::rollup::MissReason::NotBuilt));
                raw_only.push(project.clone());
            } else {
                covered_projects.push((project.clone(), covered));
            }
        }
        // `None` = every project the query reads.
        let split = crate::rollup::ProjectSplit {
            covered: (route.project_id.is_none() && !raw_only.is_empty()).then(|| covered_projects.iter().map(|(project, _)| project.clone()).collect()),
            raw_only: if route.project_id.is_none() { raw_only.clone() } else { Vec::new() },
        };
        // A range is read from the rollup only where every covered project proved it.
        let covered = covered_projects.into_iter().map(|(_, r)| r).reduce(|left, right| intersect_ranges(&left, &right)).unwrap_or_default();
        let mut generations = crate::rollup::merge_generation_ranges(generations);
        // A buffered row is missing from EVERY rollup partition, so this caps the whole set.
        let horizon = buffered.unwrap_or(route.hi);
        if let Some(project) = raw_only.first()
            && crate::observability::sample_rollup_miss("rollup_uncovered_project")
        {
            warn!(
                project_id = %project,
                source = %route.source,
                target = %route.target,
                projects_in_window = projects.len(),
                event = "rollup_uncovered_project",
                "a project in the window contributed NO covered range; with coverage intersected across the set this refuses the whole query"
            );
        }
        let interiors = crate::rollup::interiors(route.lo, route.hi, route.grain, horizon, &covered);
        if interiors.is_empty() {
            if crate::observability::sample_rollup_miss("rollup_empty_interior") {
                warn!(
                    lo = route.lo,
                    hi = route.hi,
                    horizon,
                    horizon_shortfall_secs = (route.hi - horizon).max(0) / 1_000_000,
                    grain_secs = route.grain / 1_000_000,
                    covered_ranges = covered.len(),
                    covered_start = covered.first().map(|range| range.0).unwrap_or_default(),
                    covered_end = covered.last().map(|range| range.1).unwrap_or_default(),
                    projects_in_window = projects.len(),
                    target = %route.target,
                    event = "rollup_empty_interior",
                    "coverage produced no usable interior for this window"
                );
            }
            return Err(if measure_declined { crate::rollup::MissReason::MeasureNotStored } else { miss.unwrap_or(crate::rollup::MissReason::TinyInterior) });
        }
        if crate::rollup::hybrid_branch_count(route.lo, route.hi, &interiors) > 32 {
            return Err(crate::rollup::MissReason::TooManyBranches);
        }
        // Drop dates no interval actually reads: keeping them in the ticket would let an
        // unrelated partition's churn invalidate a valid plan.
        let reads = |date: &str| interiors.iter().any(|interval| date_intersects(date, *interval));
        generations.retain(|generation| interiors.iter().any(|(start, end)| generation.range.0 < *end && generation.range.1 > *start));
        ticket.retain(|((_, _, _, date), ..)| reads(date));
        slice_ticket.retain(|((_, _, _, start, end), ..)| interiors.iter().any(|(covered_start, covered_end)| *start < *covered_end && *end > *covered_start));
        accepted_output.restrict_to(&interiors);
        // Proven empty ranges contribute coverage and tickets, but authorize no output.
        if measure_declined {
            crate::observability::record_rollup_miss(crate::rollup::MissReason::MeasureNotStored);
        }
        let mode = if interiors == [(route.lo, route.hi)] { "full" } else { "hybrid" };
        Ok(Some(RollupRewrite {
            sql: route.sql(&generations, &interiors, &split),
            grain: format!("{}us", route.grain),
            mode,
            matched: route.matched,
            ticket: RollupReadTicket {
                dates: ticket,
                slices: slice_ticket,
                output: RollupOutputTicket { source: route.source, target: route.target, lookup_project, accepted: accepted_output },
            },
        }))
    }

    pub(crate) async fn rollup_ticket_current(&self, ticket: &RollupReadTicket) -> bool {
        for ((project_id, source, target, date), source_fp, source_epoch, generation) in &ticket.dates {
            if self
                .rollup_coverage
                .get(&(project_id.clone(), source.clone(), target.clone(), date.clone()))
                .is_none_or(|coverage| coverage.source_fp != *source_fp || coverage.source_epoch != Some(*source_epoch) || coverage.generation != *generation)
                || self.rollup_source_epochs.get(&(project_id.clone(), source.clone(), date.clone())).map_or(0, |epoch| *epoch.value()) != *source_epoch
                || !self.rollup_source_fingerprint(project_id, source, date).await.is_ok_and(|fingerprint| fingerprint == *source_fp)
            {
                return false;
            }
        }
        if !ticket
            .slices
            .iter()
            .all(|(key, source_fp, generation)| self.rollup_slice_coverage.get(key).is_some_and(|c| c.source_fp == *source_fp && c.generation == *generation))
        {
            return false;
        }
        let RollupOutputTicket { source, target, lookup_project, accepted } = &ticket.output;
        let Ok(source_table) = self.resolve_table(lookup_project, source).await else { return false };
        let fingerprints = {
            let table = source_table.read().await;
            let Ok(stats) = Self::partition_stats_bounded(&table, tiebreak_of(source), &|_, _| i64::MAX) else { return false };
            stats
        };
        let Ok(target_table) = self.resolve_table(lookup_project, target).await else { return false };
        let table = target_table.read().await;
        self.rollup_output_coverage(source, (target, &table, lookup_project), &fingerprints).is_ok_and(|live| live.contains(accepted))
    }

    /// Query Delta tables directly, bypassing the in-memory buffer (for testing).
    pub async fn query_delta_only(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let ctx = self.rollup_maintenance_context()?;
        Ok(ctx.sql(sql).await?.collect().await?)
    }

    fn rollup_maintenance_context(&self) -> Result<datafusion::prelude::SessionContext> {
        let mut db_clone = self.clone();
        db_clone.bypass_buffer = true;
        db_clone.bypass_rollup = true;
        db_clone.maintenance_scan = true;
        let db_arc = Arc::new(db_clone);
        let mut ctx = Arc::clone(&db_arc).create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db_arc.setup_session_context(&mut ctx)?;
        Ok(ctx)
    }

    /// Session with a private spillable pool bounded by the decoded-work ceiling.
    /// Only UDFs are registered; the caller registers its own Delta provider.
    pub(super) fn bounded_rollup_maintenance_context(&self, batch_rows: usize) -> Result<datafusion::prelude::SessionContext> {
        let runtime = self.coordinator_runtime_env();
        let state = build_optimize_session_state_tuned(1, runtime, Some(&batch_rows.to_string()), None);
        let state =
            datafusion::execution::SessionStateBuilder::new_from_existing(state).with_physical_optimizer_rule(Arc::new(CompactRollupSortInputs)).build();
        let mut ctx = datafusion::prelude::SessionContext::new_with_state(state);
        datafusion_functions_json::register_all(&mut ctx)?;
        self.setup_session_udfs(&mut ctx)?;
        Ok(ctx)
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        let db = Arc::new(self.background_clone());
        let cancel = self.maintenance_shutdown.clone();

        // Must precede any repair pass: re-adopt footers earlier processes already
        // probed as sorted, so this one does not re-clear them.
        db.load_verified_sorted();

        // Cadence is load-bearing: the first pass runs while tables are still
        // loading and reads nothing, so retry fast until a pass actually reads a
        // table, then fall back to hourly.
        {
            let db = Arc::clone(&db);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                loop {
                    let (tables_read, _) = db.seed_verified_sorted(REPAIR_VERIFY_SEED_LIMIT).await;
                    let wait = std::time::Duration::from_secs(if tables_read == 0 { 60 } else { 3600 });
                    if cancel.run_until_cancelled(tokio::time::sleep(wait)).await.is_none() {
                        break;
                    }
                }
            });
        }

        // Coordinator work gets its own runtime so it cannot starve PGWire's
        // workers. It needs headroom beyond the job count — timers, cancellation
        // and object-store I/O run on it too, and sizing it to exactly the jobs
        // lets a blocking decode stall the timers that would cancel it.
        let coordinator_job_workers = self.config.derived.coordinator_job_slots();
        // Threads track the BOX, slots track the queue: tying threads to slots
        // would spawn 50 threads for 32 cores the moment slots grew.
        let coordinator_runtime_workers = self.config.derived.coordinator_runtime_threads();
        db.maintenance_debt_planned_at.store(crate::support::now_micros(), std::sync::atomic::Ordering::Relaxed);
        {
            let db = Arc::clone(&db);
            let cancel = cancel.clone();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(coordinator_runtime_workers)
                .thread_name("maintenance-worker")
                // READS WIN. Maintenance may fill every idle core, but the kernel
                // must prefer the pgwire runtime whenever both are runnable, or
                // expanding the slot count would buy backlog throughput with query
                // latency. `nice` is the cheapest expression of that: it costs
                // nothing while the box is idle and only bites under contention.
                // ClickHouse lowers background-merge thread priority for the same
                // reason. The admission ceiling still backs maintenance off when
                // `runtime_lag_ms` shows the query runtime actually starving —
                // niceness is the fast, per-timeslice guard, admission the slow one.
                .on_thread_start(|| {
                    // LINUX ONLY on purpose: Linux scopes `PRIO_PROCESS` to the
                    // calling THREAD, which is what we want. macOS scopes it to the
                    // whole process, so running this on a dev box would quietly
                    // deprioritise the server itself.
                    #[cfg(target_os = "linux")]
                    // SAFETY: sets the calling thread's nice value; a denial under a
                    // restricted sandbox is not worth failing boot over.
                    unsafe {
                        libc::setpriority(libc::PRIO_PROCESS, 0, MAINTENANCE_THREAD_NICE);
                    }
                })
                .enable_all()
                .build()
                .map_err(|error| anyhow::anyhow!("failed to build maintenance runtime: {error}"))?;
            db.maintenance_executor.set(runtime.handle().clone()).map_err(|_| anyhow::anyhow!("maintenance runtime already started"))?;
            std::thread::Builder::new()
                .name("maintenance-runtime".to_owned())
                .spawn(move || {
                    runtime.block_on(async move {
                        let journal = Arc::clone(&db.maintenance_tasks);
                        let migration = tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
                            let mut journal = crate::support::lock(&journal);
                            let discarded = journal.migrate_bootstrap_backlog();
                            let migrated = journal.migrate_derived_slices();
                            if let Some(cleared) = journal.clear_stale_estimates() {
                                journal.compact()?;
                                info!(cleared, event = "maintenance_stale_estimates_cleared");
                            }
                            if let Some(reset) = journal.reset_repair_attempts() {
                                journal.compact()?;
                                info!(reset, event = "maintenance_repair_attempts_reset");
                            }
                            let retired = journal.retire_drain_backlog(crate::support::now_micros()).unwrap_or_default();
                            if retired != 0 {
                                info!(retired, event = "maintenance_drain_backlog_retired");
                            }
                            let coarsened = journal.migrate_fine_grained_backfill(crate::support::now_micros()).unwrap_or_default();
                            if coarsened != 0 {
                                info!(coarsened, event = "maintenance_coarse_backfill_migrated");
                            }
                            if discarded.is_some_and(|count| count != 0) || coarsened != 0 || retired != 0 {
                                journal.compact()?;
                            } else if discarded.is_some() || migrated != 0 {
                                journal.checkpoint()?;
                            }
                            Ok((discarded.unwrap_or_default(), migrated))
                        })
                        .await;
                        let (discarded_bootstrap_tasks, migrated_tasks) = match migration {
                            Ok(Ok(counts)) => counts,
                            Ok(Err(error)) => {
                                warn!(%error, event = "maintenance_task_journal_migration_failed");
                                return;
                            }
                            Err(error) => {
                                warn!(%error, event = "maintenance_task_journal_migration_panicked");
                                return;
                            }
                        };
                        // The cleanup removes only unpublished work; recreate the
                        // persisted invalidations so they still converge.
                        let requeued_dirty_partitions = if discarded_bootstrap_tasks == 0 {
                            0
                        } else {
                            db.rollup_dirty
                                .iter()
                                // fold, not filter().count(): the enqueue must not sit in a
                                // predicate a short-circuiting adapter could stop driving.
                                .fold(0usize, |requeued, entry| {
                                    let ((project, source, date), hours) = (entry.key(), *entry.value());
                                    let enqueued = hours != 0
                                        && db
                                            .enqueue_maintenance_hours(project, source, date, hours, true)
                                            .inspect_err(|error| warn!(%error, project, source, date, event = "maintenance_dirty_partition_requeue_failed"))
                                            .is_ok();
                                    requeued + usize::from(enqueued)
                                })
                        };
                        info!(
                            discarded_bootstrap_tasks,
                            requeued_dirty_partitions,
                            migrated_tasks,
                            runtime_workers = coordinator_runtime_workers,
                            job_workers = coordinator_job_workers,
                            io_slots = db.config.derived.coordinator_io_slots(),
                            event = "maintenance_runtime_started"
                        );

                        for worker in 0..coordinator_job_workers {
                            Self::spawn_after_preload(Arc::clone(&db), cancel.clone(), move |db, cancel| async move {
                                loop {
                                    if cancel.is_cancelled() {
                                        return;
                                    }
                                    let idle = match tokio::time::timeout(COORDINATOR_LOOP_TIMEOUT, db.run_maintenance_coordinator_once()).await {
                                        Ok(Ok(true)) => {
                                            tokio::task::yield_now().await;
                                            false
                                        }
                                        Ok(Ok(false)) => true,
                                        Ok(Err(error)) => {
                                            crate::observability::maintenance_stats()
                                                .maintenance_coordinator_errors
                                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                            warn!(worker, %error, event = "maintenance_coordinator_error");
                                            true
                                        }
                                        Err(_) => {
                                            // Dropping the future drops its TaskLease, which durably
                                            // requeues the claimed unit and releases its tokens.
                                            warn!(
                                                worker,
                                                timeout_seconds = COORDINATOR_LOOP_TIMEOUT.as_secs(),
                                                event = "maintenance_coordinator_loop_timed_out"
                                            );
                                            false
                                        }
                                    };
                                    if idle {
                                        // Wake on the ENQUEUE, not on a timer. The sleep
                                        // stays as a floor so a notify that races an idle
                                        // worker cannot park it indefinitely.
                                        let woken = cancel
                                            .run_until_cancelled(async {
                                                tokio::select! {
                                                    () = db.maintenance_work.notified() => {}
                                                    () = tokio::time::sleep(COORDINATOR_IDLE_BACKOFF) => {}
                                                }
                                            })
                                            .await;
                                        if woken.is_none() {
                                            return;
                                        }
                                    }
                                }
                            });
                        }

                        Self::spawn_after_preload(Arc::clone(&db), cancel.clone(), |db, cancel| async move {
                            loop {
                                match cancel.run_until_cancelled(db.reconcile_maintenance_task_cursors()).await {
                                    None => return,
                                    Some(Ok(reconciled_tasks)) => info!(reconciled_tasks, event = "maintenance_task_reconcile_complete"),
                                    Some(Err(error)) => warn!(%error, event = "maintenance_task_reconcile_failed"),
                                }
                                // Keeps the durable cursor within Delta log retention.
                                if cancel.run_until_cancelled(tokio::time::sleep(std::time::Duration::from_secs(60))).await.is_none() {
                                    return;
                                }
                            }
                        });

                        Self::spawn_after_preload(Arc::clone(&db), cancel.clone(), |db, cancel| async move {
                            // Recurring, not once at startup: only this pass can
                            // enqueue the republish that retires an untagged tier
                            // file, and files become retirable long after boot.
                            loop {
                                for source in crate::schema::registry().list_tables() {
                                    if let Err(error) = db.recover_rollup_coverage(&source).await {
                                        warn!(source, %error, "rollup coverage recovery failed; those partitions stay on raw scans");
                                    }
                                }
                                if cancel.run_until_cancelled(tokio::time::sleep(COVERAGE_RECOVERY_INTERVAL)).await.is_none() {
                                    return;
                                }
                            }
                        });

                        cancel.cancelled().await;
                    });
                })
                .map_err(|error| anyhow::anyhow!("failed to start maintenance runtime: {error}"))?;
        }

        // `scan_pressure_permits` otherwise samples only on gated decode polls;
        // this heartbeat keeps the tier fresh whoever is allocating.
        {
            let total = self.config.memory.timefusion_max_concurrent_scan_readers as u32;
            let cancel = cancel.clone();
            tokio::spawn(async move {
                while !cancel.is_cancelled() {
                    let _ = scan_pressure_permits(total);
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            });
        }

        // Delete staged parquet left by an interrupted wave. Best-effort and
        // after readiness, so serial DELETE latency cannot hold PGWire in 57P03.
        {
            let cleanup_db = Arc::clone(&db);
            tokio::spawn(async move {
                for (_project_id, table_name, table) in cleanup_db.all_tables().await {
                    if cleanup_db.maintenance_shutdown.is_cancelled() {
                        return;
                    }
                    cleanup_db.reconcile_staged_intents(&table, &table_name).await;
                }
            });
        }

        // Dedup — collapse duplicates in sealed (< today) partitions. Holds
        // `maintenance_job_sem` to stay serialized against the full optimize job.
        spawn_cron_job_on("Dedup", &self.config.maintenance.timefusion_dedup_schedule, cancel.clone(), self.maintenance_executor.get().cloned(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    let Ok(_maintenance_job) = db.maintenance_job_sem.clone().acquire_owned().await else {
                        return;
                    };
                    info!("Running scheduled dedup on sealed partitions");
                    // One budget for the whole pass, split between its two
                    // sequential halves so the drain cannot starve the sweep.
                    let budget = db.config.derived.tick_budget(cron_period(&db.config.maintenance.timefusion_dedup_schedule));
                    let now = std::time::Instant::now();
                    // The drain gets the larger share: the sweep resumes at a
                    // cursor, a half-drained bin does not.
                    let drain_deadline = now + budget.mul_f64(0.6);
                    let sweep_deadline = now + budget;
                    // Rotate the table order: a fixed order lets the first table
                    // spend the shared budget and starve the rest forever.
                    let mut tables = db.all_tables().await;
                    let start = sweep_resume_offset(tables.len(), db.dedup_table_cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
                    tables.rotate_left(start);
                    for (project_id, table_name, table) in tables {
                        if db.maintenance_shutdown.is_cancelled() {
                            return;
                        }
                        // Certification runs for EVERY table, including the ones
                        // skipped below, so it must stay ahead of that `continue`.
                        // Do NOT re-enable the drain for rollup-declared sources:
                        // `drain_deadline` bounds admission only, so one admitted
                        // bin can hold `maintenance_job_sem` for its full stage
                        // deadline and wedge every other job.
                        db.run_certification_pass(&table, &table_name, sweep_deadline).await;
                        if get_schema(&table_name).is_some_and(|schema| !schema.rollups.is_empty()) {
                            continue;
                        }
                        // Dedup key: bare table name for unified tables, tenant-scoped
                        // for custom-storage ones (they are separate Delta logs).
                        let key = if project_id.is_empty() { table_name.clone() } else { format!("{project_id}:{table_name}") };
                        db.run_dedup_for_table(&table, &table_name, &key, &Self::table_label(&project_id, &table_name), drain_deadline, sweep_deadline).await;
                    }
                }
            }
        });

        // Vacuum — expired-file removal (default: daily at 2AM).
        let vacuum_retention = self.config.maintenance.timefusion_vacuum_retention_hours;
        spawn_db_cron(&db, "Vacuum", &self.config.maintenance.timefusion_vacuum_schedule, cancel.clone(), move |db| async move {
            info!("Running scheduled vacuum on all tables");
            for (project_id, table_name, table) in db.all_tables().await {
                info!("Vacuuming {} (retention: {}h)", Self::table_label(&project_id, &table_name), vacuum_retention);
                db.vacuum_table(&project_id, &table_name, &table, vacuum_retention).await;
            }
        });

        // Tantivy reconcile — backfill every live parquet no manifest covers,
        // then GC entries for dead files. The indexer is attached after
        // construction and may be absent, so it is checked at tick time.
        spawn_db_cron(&db, "Tantivy reconcile", &self.config.maintenance.timefusion_tantivy_reconcile_schedule, cancel.clone(), |db| async move {
            let Some(svc) = db.tantivy_indexer().cloned() else {
                warn!(event = "tantivy_reconcile_no_indexer");
                return;
            };
            // Rotate where the pass starts, or one expensive table at the front of
            // the sorted list starves the rest. The offset comes from the clock,
            // not from state, so a restart cannot reset the starving order.
            let tables = svc.config.indexed_tables();
            let offset = rotation_offset(crate::support::now_micros(), tables.len());
            let ordered: Vec<String> = tables.iter().cycle().skip(offset).take(tables.len()).cloned().collect();
            // Concurrent, because passes differ hugely in cost. Bounded because
            // each pass holds a live-file list plus a tantivy writer arena.
            futures::stream::iter(ordered.into_iter().map(|table_name| {
                let db = Arc::clone(&db);
                async move {
                    match db.tantivy_reconcile_table(&table_name).await {
                        Ok((built, removed, blobs)) => {
                            info!("tantivy reconcile: table={} built={} entries_removed={} blobs_deleted={}", table_name, built, removed, blobs);
                        }
                        Err(e) => warn!("tantivy reconcile failed for {}: {}", table_name, e),
                    }
                }
            }))
            .buffer_unordered(TANTIVY_RECONCILE_CONCURRENCY)
            .collect::<Vec<()>>()
            .await;
        });

        spawn_db_cron(&db, "Bloom sidecar reconcile", &self.config.maintenance.timefusion_bloom_sidecar_schedule, cancel.clone(), |db| async move {
            if db.bloom_prune().is_none() {
                return;
            }
            match db.bloom_sidecar_reconcile().await {
                Ok((built, errors)) => info!("bloom sidecar reconcile: built={built} errors={errors}"),
                Err(e) => warn!("bloom sidecar reconcile failed: {e:#}"),
            }
        });

        // Tantivy cache reap — the only thing that bounds the extracted-index disk
        // tree, which shares a volume with the WAL. Uses the search service, which
        // owns the cache, not the indexer.
        spawn_db_cron(&db, "Tantivy cache reap", &self.config.tantivy.timefusion_tantivy_cache_reap_schedule, cancel.clone(), |db| async move {
            let Some(svc) = db.tantivy_search().cloned() else { return };
            let budget = db.config.tantivy.cache_disk_bytes();
            // Walks the whole cache tree and unlinks — never on the runtime's worker threads.
            let Ok(report) = tokio::task::spawn_blocking(move || svc.reap_disk_cache(budget)).await else { return };
            crate::observability::record_tantivy_cache_bytes(report.bytes_before - report.bytes_removed);
            if report.dirs_removed > 0 || report.errors > 0 {
                info!(
                    "tantivy cache reap: scanned={} before={}MB removed={} freed={}MB errors={} budget={}MB",
                    report.dirs_scanned,
                    report.bytes_before / 1024 / 1024,
                    report.dirs_removed,
                    report.bytes_removed / 1024 / 1024,
                    report.errors,
                    budget / 1024 / 1024
                );
            }
        });

        // Tantivy hot-window re-warm. Also re-stamps `last_used` on every hot dir,
        // keeping them at the young end of the reaper's eviction order.
        spawn_db_cron(&db, "Tantivy hot warm", &self.config.tantivy.timefusion_tantivy_prefetch_schedule, cancel.clone(), |db| async move {
            db.tantivy_warm_hot_window().await;
        });

        // Checkpoint + expired-log cleanup — out-of-band so object-store errors on
        // the checkpoint PUT or bulk log delete cannot fail a landed commit.
        spawn_db_cron(&db, "Checkpoint", &self.config.maintenance.timefusion_checkpoint_schedule, cancel.clone(), |db| async move {
            db.run_checkpoint_maintenance().await
        });

        // Reconcile — Remove dangling Add entries (committed parquet that no longer
        // exists) via filesystem_check.
        spawn_db_cron(&db, "Reconcile", &self.config.maintenance.timefusion_reconcile_schedule, cancel.clone(), |db| async move {
            db.run_reconcile_maintenance().await
        });

        // Cache stats — every 5 minutes.
        spawn_db_cron(&db, "Cache stats", "0 */5 * * * *", cancel.clone(), |db| async move {
            if let Some(ref cache) = db.object_store_cache {
                cache.log_stats();
            }
            let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
            info!("Statistics cache: {}/{} entries used", used, capacity);
        });

        // Statistics refresh — every 15 minutes.
        spawn_db_cron(&db, "Statistics refresh", "0 */15 * * * *", cancel.clone(), |db| async move {
            info!("Refreshing Delta Lake statistics cache");
            db.statistics_extractor.clear_cache().await;
            // Unified tables pre-warm under an empty project_id — they're shared.
            for (project_id, table_name, table) in db.all_tables().await {
                let label = Self::table_label(&project_id, &table_name);
                let table = table.read().await;
                if let Err(e) = db.statistics_extractor.extract_statistics(&table, &project_id, &table_name).await {
                    error!("Failed to refresh statistics for {}: {}", label, e);
                } else {
                    debug!("Refreshed statistics for {} (version {})", label, table.version().unwrap_or(0));
                }
            }
        });

        Ok(self)
    }

    /// Spawn a maintenance loop that must not start before table replay has
    /// finished (or its budget expired). A shutdown during the wait drops the body.
    fn spawn_after_preload<F, Fut>(db: Arc<Self>, cancel: Arc<CancellationToken>, body: F)
    where
        F: FnOnce(Arc<Self>, Arc<CancellationToken>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let tracker = db.maintenance_tasks_tracker.clone();
        tracker.spawn(async move {
            if db.wait_for_preload(&cancel).await {
                body(db, cancel).await;
            }
        });
    }
}

#[cfg(test)]
mod compact_rollup_input_tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringViewBuilder},
        compute::SortOptions,
        datatypes::DataType,
    };
    use datafusion::{
        datasource::{memory::MemorySourceConfig, source::DataSourceExec},
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_optimizer::PhysicalOptimizerRule,
        physical_plan::{displayable, sorts::sort::SortExec},
    };
    use futures::TryStreamExt;

    #[tokio::test]
    async fn rollup_spill_reader_keeps_unread_disk_bytes_charged() -> Result<()> {
        use datafusion::{
            execution::runtime_env::RuntimeEnvBuilder,
            physical_plan::{
                metrics::{ExecutionPlanMetricsSet, SpillMetrics},
                spill::SpillManager,
            },
        };
        let dir = tempfile::tempdir()?;
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(super::super::write::spill_disk_builder(dir.path().to_owned(), 1).with_max_temp_directory_size(8 * 1024 * 1024))
            .build_arc()?;
        let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from_iter_values(0..256)) as arrow::array::ArrayRef)])?;
        let manager = SpillManager::new(Arc::clone(&runtime), SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0), batch.schema());
        let file = manager.spill_record_batch_and_finish(&[batch.clone(), batch.clone()], "rollup reader lifetime")?.expect("two nonempty batches");
        let bytes = runtime.disk_manager.used_disk_space();
        assert!(bytes > 0, "the fixture must charge its real spill file");
        let mut stream = manager.read_spill_as_stream_unbuffered(file, None)?;
        assert_eq!(stream.try_next().await?, Some(batch), "only the first of two batches was consumed");
        assert_eq!(runtime.disk_manager.used_disk_space(), bytes, "an open reader with an unread batch must retain its disk charge");
        drop(stream);
        assert_eq!(runtime.disk_manager.used_disk_space(), 0, "cancelling the reader must release its charge");
        for path in runtime.disk_manager.temp_dir_paths() {
            assert!(std::fs::read_dir(path)?.next().is_none(), "cancelling the reader must remove its spill file");
        }
        Ok(())
    }

    #[test_case::test_case(false ; "cancel during spill preparation")]
    #[test_case::test_case(true ; "cancel during final merge")]
    #[tokio::test]
    async fn rollup_sort_cancellation_releases_active_spills(after_output: bool) -> Result<()> {
        use anyhow::Context;
        use datafusion::execution::{
            TaskContext,
            memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool},
            runtime_env::RuntimeEnvBuilder,
        };
        const HEADROOM: usize = 64 * 1024;
        let pool = Arc::new(TrackConsumersPool::new(FairSpillPool::new(512 * 1024), std::num::NonZeroUsize::new(4).expect("four consumers")));
        let dir = tempfile::tempdir()?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_disk_manager_builder(super::super::write::spill_disk_builder(dir.path().to_owned(), 1).with_max_temp_directory_size(8 * 1024 * 1024))
            .build_arc()?;
        let batches = (0..128)
            .rev()
            .map(|part| {
                let mut strings = StringViewBuilder::new();
                strings.try_append_value_n("x".repeat(32), 256)?;
                RecordBatch::try_from_iter([
                    ("id", Arc::new(Int64Array::from_iter_values((part * 256..(part + 1) * 256).rev())) as arrow::array::ArrayRef),
                    ("payload", Arc::new(strings.finish()) as arrow::array::ArrayRef),
                ])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let schema = batches[0].schema();
        let source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&[batches], schema, None)?)));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("id", 0)), SortOptions::default())]).expect("one key");
        let plan = CompactRollupSortInputs.optimize(Arc::new(SortExec::new(ordering, source)), &Default::default())?;
        let mut config = datafusion::prelude::SessionConfig::new().with_batch_size(256);
        config.options_mut().execution.sort_spill_reservation_bytes = HEADROOM;
        let context = Arc::new(TaskContext::default().with_session_config(config).with_runtime(Arc::clone(&runtime)));
        let mut stream = plan.execute(0, context)?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            if after_output {
                let batch = stream.try_next().await?.context("the spilling sort must produce a batch")?;
                assert_eq!(batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id array").value(0), 0);
            } else {
                tokio::select! {
                    result = stream.try_next() => {
                        result?;
                        anyhow::bail!("sort finished before an active spill was observed");
                    }
                    () = async {
                        while runtime.disk_manager.used_disk_space() == 0 {
                            tokio::task::yield_now().await;
                        }
                    } => {}
                }
            }
            anyhow::Ok(())
        })
        .await??;
        assert!(runtime.disk_manager.used_disk_space() > 0, "cancellation must interrupt a real spill, not an in-memory sort");
        let headroom: usize = pool.metrics().iter().filter(|consumer| consumer.name == plan.name()).map(|consumer| consumer.reserved).sum();
        assert_eq!(headroom, if after_output { 0 } else { HEADROOM }, "headroom must transfer only after first output");
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.reserved() != 0 || runtime.disk_manager.used_disk_space() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("cancellation must release execution and scratch reservations")?;
        for path in runtime.disk_manager.temp_dir_paths() {
            assert!(std::fs::read_dir(path)?.next().is_none(), "cancellation must remove physical spill files too");
        }
        Ok(())
    }

    #[test_case::test_case(32, 0, 512 * 1024 ; "cancel before polling")]
    #[test_case::test_case(32, 1, 512 * 1024 ; "cancel after first batch")]
    #[test_case::test_case(32, 3, 512 * 1024 ; "consume sorted output")]
    #[test_case::test_case(0, 3, 512 * 1024 ; "empty input")]
    #[test_case::test_case(32, 0, 32 * 1024 ; "headroom exceeds budget")]
    #[tokio::test]
    async fn rollup_sort_headroom_releases_its_reservation(rows: i64, consumed: usize, pool_bytes: usize) -> Result<()> {
        use datafusion::execution::{
            TaskContext,
            memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool},
            runtime_env::RuntimeEnvBuilder,
        };
        const HEADROOM: usize = 64 * 1024;
        let pool = Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_bytes), std::num::NonZeroUsize::new(4).expect("four consumers")));
        let runtime = RuntimeEnvBuilder::new().with_memory_pool(pool.clone()).build_arc()?;
        let mut config = datafusion::prelude::SessionConfig::new().with_batch_size(16);
        config.options_mut().execution.sort_spill_reservation_bytes = HEADROOM;
        let context = Arc::new(TaskContext::default().with_session_config(config).with_runtime(runtime));
        let batch = RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from_iter_values((0..rows).rev())) as arrow::array::ArrayRef)])?;
        let schema = batch.schema();
        let source = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&[vec![batch]], schema, None)?)));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("id", 0)), SortOptions::default())]).expect("one key");
        let plan = RollupSortHeadroom(Arc::new(SortExec::new(ordering, source)));
        let headroom = || pool.metrics().iter().filter(|consumer| consumer.name == plan.name()).map(|consumer| consumer.reserved).sum::<usize>();
        let result = plan.execute(0, context);
        if pool_bytes < HEADROOM {
            assert!(matches!(result, Err(datafusion::common::DataFusionError::ResourcesExhausted(_))), "insufficient capacity must remain a typed error");
        } else {
            let mut stream = result?;
            assert_eq!(headroom(), HEADROOM, "workspace must be reserved before the sort is polled");
            let mut output = Vec::new();
            for _ in 0..consumed {
                let batch = stream.try_next().await?;
                assert_eq!(headroom(), 0, "first output or EOF must release downstream workspace");
                let Some(batch) = batch else { break };
                output.extend(batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id array").values().iter().copied());
            }
            assert_eq!(output, (0..i64::try_from(output.len())?).collect::<Vec<_>>(), "sorted values must survive the wrapper");
            if consumed == 3 {
                assert_eq!(i64::try_from(output.len())?, rows, "full consumption must preserve every row");
            }
            drop(stream);
        }
        assert_eq!(pool.reserved(), 0, "completion, cancellation, and refusal must release every reservation");
        Ok(())
    }

    #[test_case::test_case(0 ; "drop before polling")]
    #[test_case::test_case(1 ; "drop after a prefix")]
    #[test_case::test_case(2 ; "consume each partition")]
    #[tokio::test]
    async fn compact_rollup_input_preserves_properties_and_releases_input(consumed: usize) -> Result<()> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("text", DataType::Utf8View, false),
        ]));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("id", 0)), SortOptions::default())]).expect("one key");
        let (source, input_array, retained) = {
            let batches = (0..2)
                .map(|part| {
                    let mut strings = StringViewBuilder::new().with_fixed_block_size(1024 * 1024);
                    strings.try_append_value_n("longer than an inline string", 32)?;
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int64Array::from_iter_values(part * 32..part * 32 + 32)), Arc::new(strings.finish())],
                    )
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let input_array = Arc::downgrade(batches[0].column(1));
            let retained = datafusion::common::utils::memory::get_record_batch_memory_size(&batches[0]);
            let config = MemorySourceConfig::try_new(&[batches.clone(), batches], schema, None)?.try_with_sort_information(vec![ordering.clone()])?;
            (Arc::new(DataSourceExec::new(Arc::new(config))) as Arc<dyn ExecutionPlan>, input_array, retained)
        };
        let sort = Arc::new(SortExec::new(ordering, Arc::clone(&source)).with_preserve_partitioning(true));
        let once = CompactRollupSortInputs.optimize(sort, &Default::default())?;
        let twice = CompactRollupSortInputs.optimize(Arc::clone(&once), &Default::default())?;
        assert_eq!(displayable(once.as_ref()).indent(false).to_string(), displayable(twice.as_ref()).indent(false).to_string(), "rule must be idempotent");
        assert!(once.is::<RollupSortHeadroom>(), "sort output must reserve downstream workspace");
        let wrapper = Arc::clone(once.children()[0].children()[0]);
        assert!(wrapper.is::<CompactRollupSortInput>(), "sort input must be compacted");
        assert!(Arc::ptr_eq(wrapper.properties(), source.properties()), "all partition and ordering properties must stay unchanged");
        let context = Arc::new(datafusion::execution::TaskContext::default());
        let mut streams = (0..2).map(|partition| wrapper.execute(partition, Arc::clone(&context))).collect::<datafusion::common::Result<Vec<_>>>()?;
        drop((source, wrapper, once, twice));
        for stream in &mut streams {
            for part in 0..consumed {
                let batch = stream.try_next().await?.expect("two source batches");
                assert!(
                    datafusion::common::utils::memory::get_record_batch_memory_size(&batch) < retained / 4,
                    "oversized backing allocation must be released"
                );
                let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id array");
                assert_eq!(ids.values().as_ref(), (part as i64 * 32..part as i64 * 32 + 32).collect::<Vec<_>>(), "partition row order must survive");
                let strings = batch.column(1).as_any().downcast_ref::<arrow::array::StringViewArray>().expect("text array");
                assert!(strings.iter().all(|value| value == Some("longer than an inline string")), "values must survive compaction");
            }
            if consumed == 2 {
                assert!(stream.try_next().await?.is_none(), "compaction must not add rows");
            }
        }
        drop(streams);
        assert!(input_array.upgrade().is_none(), "dropping streams must release source arrays even before EOF");
        Ok(())
    }
}
