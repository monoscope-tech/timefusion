//! Maintenance: rollup planning/coordinator ticks, dedup sweeps + wave commits,
//! hot-tail packing/repair passes, vacuum, checkpoint/reconcile, shutdown.
use super::*;
use anyhow::Context;
use tap::Tap;

#[derive(Clone, Copy)]
enum TaskSelection<'a> {
    Next(crate::maintenance_coordinator::Operation),
    Exact(&'a crate::maintenance_coordinator::TaskKey),
}

impl TaskSelection<'_> {
    fn operation(self) -> crate::maintenance_coordinator::Operation {
        match self {
            Self::Next(operation) => operation,
            Self::Exact(key) => key.operation,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RollupRebuildReason {
    MissingRowWitness,
    ObsoleteGeneration,
    /// The slice HAS a witness but the partition moved under it — verifiable,
    /// and simply lost the race with ingest or dedup.
    WitnessMoved,
}

/// One tier's contribution to a fleet contiguity gauge, or `None` to abstain.
/// A real tier OVERWRITES a ramping tier's provisional seed rather than
/// minimising into it, which makes the fold order-independent.
fn fold_fleet_gauge(previous: u64, value: u64, seeded_by_real: bool, ramping: bool) -> Option<u64> {
    match (ramping, seeded_by_real) {
        (true, true) => None,
        (true, false) | (false, false) => Some(value),
        (false, true) => Some(previous.min(value)),
    }
}

/// The window the one-shot orphan repair rebuilds; older orphans age out of the
/// 35-day horizon on their own.
const ORPHAN_REPAIR_FROM: &str = "2026-08-01";
const ORPHAN_REPAIR_BEFORE: &str = "2026-08-22";

/// The (project, date) cells a one-shot repair forces a full re-derive of.
/// Empty in normal operation. To run a repair, refill this and bump
/// `DAMAGE_REPAIR_MIGRATION` to a fresh key — a spent cursor is never reused.
const DAMAGED_CELLS: &[(&str, &str)] = &[];

/// One (project, date) the rollup backfill can plan.
pub(crate) type BackfillCell = (String, chrono::NaiveDate);

/// The repair list in the order the backfill consumes it — newest date first.
/// Sorted here rather than trusted from the source list: the cursor into it is
/// durable, so a reorder would silently re-target it. `configured` REPLACES
/// `DAMAGED_CELLS` when non-empty; the two are never merged.
pub(crate) fn damaged_cells_newest_first(configured: &[String]) -> Vec<BackfillCell> {
    let listed = if configured.is_empty() {
        itertools::Either::Left(DAMAGED_CELLS.iter().map(|(project, date)| ((*project).to_owned(), (*date).to_owned())))
    } else {
        itertools::Either::Right(configured.iter().filter_map(|cell| match cell.trim().split_once(':') {
            Some((project, date)) => Some((project.trim().to_owned(), date.trim().to_owned())),
            None => {
                warn!(entry = %cell, event = "damage_repair_cell_unparsed", "damage repair cell is not `project:YYYY-MM-DD`; it will NOT be repaired");
                None
            }
        }))
    };
    listed
        .filter_map(|(project, date)| match date.parse::<chrono::NaiveDate>() {
            Ok(parsed) => Some((project, parsed)),
            Err(_) => {
                warn!(project = %project, date = %date, event = "damage_repair_cell_unparsed", "damage repair cell has an unparseable date; it will NOT be repaired");
                None
            }
        })
        .sorted_by(|(project_a, date_a), (project_b, date_b)| date_b.cmp(date_a).then_with(|| project_a.cmp(project_b)))
        .collect()
}

/// Order one backfill pass, bound it, and report how much of its forced prefix
/// it CONSUMED. `offered` is the unconsumed prefix of the repair list this pass
/// tried, in list order; `forced` is the subset of it that reached `want`.
///
/// The consumed count stops at the first offered cell that TRUNCATION dropped;
/// advancing past a cell that was merely OFFERED loses it permanently. Forced
/// cells sort FIRST, or newest-date-first ranking starves an old forced cell.
pub(crate) fn admit_backfill_pass(
    mut want: Vec<BackfillCell>, offered: &[BackfillCell], forced: &HashSet<BackfillCell>, cap: usize,
) -> (Vec<BackfillCell>, usize) {
    let wanted: HashSet<BackfillCell> = want.iter().cloned().collect();
    want.sort_by(|a, b| forced.contains(b).cmp(&forced.contains(a)).then_with(|| b.1.cmp(&a.1)).then_with(|| a.0.cmp(&b.0)));
    want.truncate(cap);
    let admitted: HashSet<BackfillCell> = want.iter().cloned().collect();
    let consumed = offered.iter().take_while(|cell| !(forced.contains(*cell) && wanted.contains(*cell) && !admitted.contains(*cell))).count();
    (want, consumed)
}

/// Arrow bytes one compressed parquet byte decodes to, at the observed zstd
/// ratio. Every sort budget in this crate is denominated in DECODED bytes while
/// `Add.size` is compressed; mixing the two units exhausts the sort pool.
pub(crate) const DECODED_BYTES_PER_COMPRESSED: i64 = 12;

pub(crate) fn estimated_decoded_bytes(compressed_size: i64) -> u64 {
    u64::try_from(compressed_size.max(0)).unwrap_or_default().saturating_mul(DECODED_BYTES_PER_COMPRESSED as u64)
}

/// Rows per scan batch that put one batch near `target_bytes` of decoded Arrow.
///
/// Derived rather than fixed: the batch size is the sort's indivisible admission
/// unit and the spill-write granularity, both in BYTES, while row widths differ
/// by 20x+ between tenants. Clamped to [256, DataFusion's 8192 default].
pub(crate) fn batch_rows_for(decoded_bytes: u64, rows: u64, target_bytes: u64) -> usize {
    const MIN_BATCH_ROWS: u64 = 256;
    const MAX_BATCH_ROWS: u64 = 8192;
    let Some(per_row) = decoded_bytes.checked_div(rows).filter(|width| *width > 0) else {
        return MIN_BATCH_ROWS as usize;
    };
    (target_bytes / per_row).clamp(MIN_BATCH_ROWS, MAX_BATCH_ROWS) as usize
}

#[cfg(test)]
mod liveness_clock_tests {
    use datafusion::prelude::SessionContext;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    };
    use std::time::Duration;

    /// 100s of work in five 20s steps, against a 30s idle window.
    async fn five_steps_under_a_30s_window(progress: &Arc<AtomicU64>, step: impl Fn()) -> Option<&'static str> {
        let work = async {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_secs(20)).await;
                step();
            }
            "committed"
        };
        super::run_until_idle_capped(Duration::from_secs(30), None, Arc::clone(progress), work).await.ok()
    }

    /// A unit that is still writing rows outlives its idle window.
    #[tokio::test(start_paused = true)]
    async fn work_that_keeps_writing_rows_outlives_its_idle_window() {
        let progress = Arc::new(AtomicU64::new(0));
        let ticker = Arc::clone(&progress);
        let result = five_steps_under_a_30s_window(&progress, move || {
            ticker.fetch_add(1, Relaxed);
        })
        .await;
        assert_eq!(result, Some("committed"), "100s of steady progress must survive a 30s idle window");
    }

    /// A write loop deep inside the unit keeps the clock alive through
    /// `note_unit_progress`, with no handle threaded to it.
    #[tokio::test(start_paused = true)]
    async fn a_deep_write_loop_keeps_its_unit_alive() {
        let progress = Arc::new(AtomicU64::new(0));
        assert_eq!(five_steps_under_a_30s_window(&progress, || super::note_unit_progress(1_000)).await, Some("committed"));
        assert_eq!(progress.load(Relaxed), 5_000, "the task-local reached the counter the clock reads");
    }

    /// Completing many plans faster than the sampling tick is still progress.
    #[tokio::test(start_paused = true)]
    async fn short_queries_keep_the_unit_alive_between_watcher_ticks() -> anyhow::Result<()> {
        let ctx = SessionContext::new();
        let progress = Arc::new(AtomicU64::new(0));
        let work = async {
            for _ in 0..40 {
                let batches = super::collect_watched(&ctx, "SELECT 1").await?;
                assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok::<_, anyhow::Error>("committed")
        };
        let result = super::run_until_idle_capped(Duration::from_secs(30), None, progress, work).await??;
        assert_eq!(result, "committed", "completed short queries must prevent a false idle timeout");
        Ok(())
    }

    #[test]
    fn note_unit_progress_outside_a_unit_is_a_no_op() {
        super::note_unit_progress(1);
    }

    /// `ORDER BY` is blocking, so a unit can be working hard and writing
    /// nothing: liveness must come from the plan's row counters, not the output.
    #[tokio::test(start_paused = true)]
    async fn plan_rows_reach_the_liveness_counter() {
        let progress = Arc::new(AtomicU64::new(0));
        super::UNIT_PROGRESS
            .scope(Arc::clone(&progress), async {
                let ctx = SessionContext::new();
                let plan = ctx.sql("SELECT 1 AS a UNION ALL SELECT 2 ORDER BY a").await.expect("plan").create_physical_plan().await.expect("physical");
                let watch = super::PlanProgress::watch(Arc::clone(&plan));
                datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await.expect("collect");
                // One tick past the watcher's interval.
                tokio::time::sleep(Duration::from_secs(20)).await;
                let sampled = progress.load(Relaxed);
                drop(watch);
                assert_eq!(progress.load(Relaxed), sampled, "the final sample must not count previously sampled rows twice");
            })
            .await;
        assert!(progress.load(Relaxed) > 0, "the plan's own row counters must reach the clock the unit is judged by");
    }

    /// A maintenance query reports which operation paid for it, "none" outside one.
    #[tokio::test]
    async fn a_maintenance_query_reports_the_operation_that_ran_it() {
        let reported = || super::UNIT_OPERATION.try_with(|operation| *operation).unwrap_or("none");
        assert_eq!(reported(), "none", "outside a unit there is no operation");
        super::UNIT_OPERATION
            .scope("Dedup", async {
                assert_eq!(reported(), "Dedup");
                tokio::task::yield_now().await;
                assert_eq!(reported(), "Dedup");
            })
            .await;
    }

    /// Pins the DataFusion metric names the pruning instrument reads: a rename
    /// would make every scan report "nothing pruned" instead of failing.
    #[tokio::test]
    async fn the_pruning_metric_names_are_the_ones_datafusion_publishes() {
        use arrow::array::Int64Array;
        use datafusion::prelude::ParquetReadOptions;
        let dir = tempfile::tempdir().expect("tempdir");
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false)]));
        // Two files, disjoint on `a`.
        for (name, lo) in [("a.parquet", 1i64), ("b.parquet", 1000)] {
            let batch =
                arrow::array::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from((lo..lo + 500).collect::<Vec<_>>()))]).expect("batch");
            let file = std::fs::File::create(dir.path().join(name)).expect("create");
            let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None).expect("writer");
            writer.write(&batch).expect("write");
            writer.close().expect("close");
        }
        let ctx = SessionContext::new();
        ctx.register_parquet("t", dir.path().to_str().expect("utf8"), ParquetReadOptions::default()).await.expect("register");
        async fn scanned(ctx: &SessionContext, sql: &str) -> (u64, u64) {
            let plan = ctx.sql(sql).await.expect("sql").create_physical_plan().await.expect("physical");
            datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await.expect("collect");
            (super::plan_metric_sum(plan.as_ref(), "bytes_scanned"), super::plan_metric_sum(plan.as_ref(), "files_processed"))
        }
        let (whole, whole_files) = scanned(&ctx, "SELECT sum(a) FROM t").await;
        let (sliced, sliced_files) = scanned(&ctx, "SELECT sum(a) FROM t WHERE a < 100").await;
        // The disjoint file is dropped during PLANNING, so it never reaches a
        // `*_pruned_*` counter — the cost shows up in bytes_scanned or nowhere.
        assert!(whole > 0 && sliced > 0, "the cost metric must be populated, got {whole} and {sliced}");
        assert!(sliced < whole, "a predicate excluding one of two files must scan fewer bytes: {sliced} vs {whole}");
        // `files_processed` is PER PARTITION, so summed over the tree it never
        // falls when a scan narrows. Only `bytes_scanned` survives summing.
        assert!(sliced_files >= whole_files, "files_processed is per-partition and must not be read as a cost: {sliced_files} vs {whole_files}");
    }

    /// Watching a plan must not change what it returns.
    #[tokio::test]
    async fn a_watched_collect_returns_what_sql_collect_returns() {
        const SQL: &str = "SELECT a, count(*) AS c FROM (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 2) GROUP BY a ORDER BY a";
        let ctx = SessionContext::new();
        let watched = super::collect_watched(&ctx, SQL).await.expect("watched");
        let plain = ctx.sql(SQL).await.expect("sql").collect().await.expect("plain");
        let rows = |batches: &[arrow::array::RecordBatch]| batches.iter().map(arrow::array::RecordBatch::num_rows).sum::<usize>();
        assert_eq!(rows(&watched), rows(&plain));
        assert_eq!(format!("{watched:?}"), format!("{plain:?}"), "watching a plan must not change what it returns");
    }

    /// The watcher must not outlive its guard, or an abandoned unit keeps
    /// reporting progress forever.
    #[tokio::test(start_paused = true)]
    async fn the_plan_watcher_stops_with_its_guard() {
        let progress = Arc::new(AtomicU64::new(0));
        super::UNIT_PROGRESS
            .scope(Arc::clone(&progress), async {
                let ctx = SessionContext::new();
                let plan = ctx.sql("SELECT 1 AS a").await.expect("plan").create_physical_plan().await.expect("physical");
                drop(super::PlanProgress::watch(plan));
                tokio::time::sleep(Duration::from_secs(120)).await;
            })
            .await;
        assert_eq!(progress.load(Relaxed), 0, "a dropped watcher reports nothing");
    }

    #[tokio::test(start_paused = true)]
    async fn work_that_writes_nothing_is_given_up_on() {
        let progress = Arc::new(AtomicU64::new(0));
        let stalled = async { std::future::pending::<&str>().await };
        let result = super::run_until_idle_capped(Duration::from_secs(30), None, progress, stalled).await;
        assert!(result.is_err(), "an idle unit must not hold its worker forever");
    }

    /// The outer loop guard wraps this clock, so it must never be the binding one.
    #[test]
    fn the_loop_guard_sits_above_every_per_unit_window() {
        use crate::maintenance_coordinator::{MAX_OPERATION_DEADLINE_SECS, Operation, operation_deadline_secs};
        for operation in
            [Operation::Dedup, Operation::Repair, Operation::HotPacking, Operation::SealedConsolidation, Operation::BaseRollup, Operation::DerivedRollup]
        {
            assert!(operation_deadline_secs(operation) <= MAX_OPERATION_DEADLINE_SECS, "{operation:?} exceeds the declared maximum");
        }
        assert!(
            crate::database::COORDINATOR_LOOP_TIMEOUT.as_secs() > MAX_OPERATION_DEADLINE_SECS,
            "the outer guard must not fire before a unit's own clock does"
        );
    }
}

#[cfg(test)]
mod batch_rows_tests {
    use super::{batch_rows_for, estimated_decoded_bytes};
    use test_case::test_case;

    /// One case per width regime — one row count cannot serve every row width.
    #[test_case(1_000_000_000, 1_000_000 => 8192 ; "1 KB rows reach the ceiling")]
    #[test_case(estimated_decoded_bytes(1_148_230_580), 1_035_264 => 630 ; "a mid-width bin lands between the clamps")]
    #[test_case(63_000_000, 1_000 => 256 ; "63 KB rows cannot afford more")]
    #[test_case(1_000_000_000, 0 => 256 ; "an unmeasurable bin keeps the old constant")]
    fn batch_rows_for_prices_bytes_not_rows(decoded_bytes: u64, rows: u64) -> usize {
        batch_rows_for(decoded_bytes, rows, 8 << 20)
    }
}

/// How long a unit waits after the pool turned it away for being busy. Backs off
/// without splitting — the unit is the right size, the pool had no room — and is
/// capped so a lane recovers quickly once the pool drains.
fn admission_backoff(attempts: u32) -> std::time::Duration {
    std::time::Duration::from_secs(1u64 << attempts.min(6))
}

/// Order the dedup phase's probe groups so both classes get real budget.
///
/// Every probe in a phase shares ONE deadline, so position is budget: only
/// `certify_only` groups can grant a certification, so interleaving gives each
/// class every other slot. `cap` bounds the total.
fn interleave_probe_groups<T>(dirty: Vec<T>, certify_only: Vec<T>, cap: usize) -> Vec<T> {
    itertools::interleave(dirty, certify_only).take(cap).collect()
}

/// How many batch-probe groups a budget can actually FINISH.
///
/// Admitting a wave that cannot finish is not free: each doomed probe builds a
/// provider and an eager snapshot over a date's files and holds a rewrite permit.
/// `observed = None`/zero means nothing has completed yet — admit the cap. The
/// floor of `permits` keeps a too-small budget at one wave, never zero.
///
/// ```
/// # use std::time::Duration;
/// # use timefusion::database::probe_groups_for_budget as fit;
/// assert_eq!(fit(10, Duration::from_secs(239), Some(Duration::from_secs(159)), 32), 15);
/// // No observation yet: admit the full cap.
/// assert_eq!(fit(10, Duration::from_secs(239), None, 32), 32);
/// // Cheap probes still fill the cap.
/// assert_eq!(fit(10, Duration::from_secs(239), Some(Duration::from_secs(1)), 32), 32);
/// // Too small for even one probe: one wave, never zero.
/// assert_eq!(fit(10, Duration::from_secs(1), Some(Duration::from_secs(159)), 32), 10);
/// ```
pub fn probe_groups_for_budget(permits: usize, budget: std::time::Duration, observed: Option<std::time::Duration>, cap: usize) -> usize {
    let Some(observed) = observed.filter(|cost| !cost.is_zero()) else { return cap };
    let fit = (budget.as_secs_f64() / observed.as_secs_f64() * permits as f64) as usize;
    fit.clamp(permits.min(cap), cap)
}

/// Fold one probe's wall clock into the admission estimate, as a half-weight EMA.
///
/// Keyed PER TABLE: cost varies by orders of magnitude between tables, so one
/// shared figure would be sized by whichever table probed last.
///
/// ```
/// # use timefusion::database::{note_probe_cost_into, probe_groups_for_budget as fit};
/// # use std::time::Duration;
/// let costs = dashmap::DashMap::new();
/// note_probe_cost_into(&costs, "otel_logs_and_spans", 226_882);
/// // Three cheap probes on OTHER tables must not move the big table's estimate.
/// for ms in [482, 1129, 148] {
///     note_probe_cost_into(&costs, "rollup_1h", ms);
/// }
/// let observed = |t: &str| costs.get(t).map(|ms| Duration::from_millis(*ms));
/// assert_eq!(observed("otel_logs_and_spans"), Some(Duration::from_millis(226_882)));
/// assert_eq!(fit(10, Duration::from_secs(239), observed("otel_logs_and_spans"), 32), 10);
/// assert_eq!(fit(10, Duration::from_secs(239), observed("rollup_1h"), 32), 32);
///
/// // A second sample on the same table moves it half way, not all the way.
/// note_probe_cost_into(&costs, "otel_logs_and_spans", 100_000);
/// assert_eq!(observed("otel_logs_and_spans"), Some(Duration::from_millis(163_441)));
/// ```
pub fn note_probe_cost_into(costs: &dashmap::DashMap<String, u64>, table_name: &str, ms: u64) {
    costs.entry(table_name.to_string()).and_modify(|prior| *prior = (*prior + ms) / 2).or_insert(ms);
}

/// Rows an `Add` declares in its Delta statistics, when it declares any.
pub(crate) fn add_row_count(add: &deltalake::kernel::Add) -> Option<u64> {
    serde_json::from_str::<serde_json::Value>(add.stats.as_deref()?).ok()?.get("numRecords")?.as_u64()
}

/// `(min, max)` row-timestamp bounds an `Add`'s statistics declare, when readable.
fn add_ts_bounds(add: &deltalake::kernel::Add) -> (Option<i64>, Option<i64>) {
    add.get_stats().ok().flatten().map_or((None, None), |stats| {
        (
            stats.min_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
            stats.max_values.get("timestamp").and_then(|value| value.as_value()).and_then(delta_stat_micros),
        )
    })
}

/// True when the `Add`'s timestamp statistics PROVE it disjoint from `slice`.
/// Missing bounds prove nothing, so the file stays a candidate.
fn stats_disjoint_from(add: &deltalake::kernel::Add, slice: crate::maintenance_coordinator::TimeSlice) -> bool {
    matches!(add_ts_bounds(add), (Some(min), Some(max)) if min >= slice.end_micros || max < slice.start_micros)
}

/// One live file's contribution to a rollup unit's CONTENT fingerprint.
///
/// Must include the deletion vector: a DV supersedes an `Add` under the SAME
/// path, so a paths-only fingerprint would report "nothing changed" over a
/// partition that just lost rows.
///
/// ```
/// # use deltalake::kernel::{DeletionVectorDescriptor, StorageType};
/// # use timefusion::database::file_content_hash;
/// let dv = |id: &str, cardinality| DeletionVectorDescriptor {
///     storage_type: StorageType::UuidRelativePath,
///     path_or_inline_dv: id.to_owned(),
///     offset: Some(1),
///     size_in_bytes: 32,
///     cardinality,
/// };
/// // The same path with no mask is the same file.
/// assert_eq!(file_content_hash("a.parquet", None), file_content_hash("a.parquet", None));
/// // Attaching a mask, changing WHICH rows it hides, or changing HOW MANY it
/// // hides each make it a different file.
/// assert_ne!(file_content_hash("a.parquet", None), file_content_hash("a.parquet", Some(&dv("u1", 5))));
/// assert_ne!(file_content_hash("a.parquet", Some(&dv("u1", 5))), file_content_hash("a.parquet", Some(&dv("u2", 5))));
/// assert_ne!(file_content_hash("a.parquet", Some(&dv("u1", 5))), file_content_hash("a.parquet", Some(&dv("u1", 6))));
/// ```
pub fn file_content_hash(path: &str, deletion_vector: Option<&deltalake::kernel::DeletionVectorDescriptor>) -> u64 {
    digest_of((path, deletion_vector.map(|dv| (&dv.path_or_inline_dv, dv.offset, dv.size_in_bytes, dv.cardinality))))
}

/// `value`'s `DefaultHasher` digest. FROZEN: `file_content_hash` writes it to a
/// tier file's `TAG_CONTENT_FINGERPRINT`, so changing the hasher makes every
/// already-published file's fingerprint stop matching.
fn digest_of(value: impl std::hash::Hash) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&value, &mut hasher);
    hasher.finish()
}

/// `micros` truncated to unix milliseconds, as the u64 the journal stores.
fn unix_ms(micros: i64) -> u64 {
    u64::try_from(micros.div_euclid(1_000)).unwrap_or_default()
}

/// UTC midnight of `date` in microseconds — the start of every day-sized slice.
fn day_start_micros(date: chrono::NaiveDate) -> Option<i64> {
    Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros())
}

/// `LogicalFileView::add_action()`, centralizing its one `#[allow(deprecated)]` call site.
fn add_action(file: &deltalake::kernel::LogicalFileView) -> deltalake::kernel::Add {
    #[allow(deprecated)]
    file.add_action()
}

tokio::task_local! {
    /// Rows the maintenance unit running on this task has written. A task-local
    /// rather than a parameter: the innermost write loop is several calls below
    /// the thing that measures it.
    static UNIT_PROGRESS: Arc<std::sync::atomic::AtomicU64>;

    /// Which operation the current unit is, for attributing what its queries
    /// cost. Set at the one dispatch site, alongside the progress counter.
    static UNIT_OPERATION: &'static str;
}

/// Report that the current maintenance unit wrote `rows`; no-op outside a unit.
pub(crate) fn note_unit_progress(rows: usize) {
    let _ = UNIT_PROGRESS.try_with(|progress| progress.fetch_add(rows as u64, std::sync::atomic::Ordering::Relaxed));
}

/// Keep the current unit's liveness clock alive while its physical plan is still
/// pulling rows; stops when the guard drops.
///
/// A write loop cannot report progress through a BLOCKING operator such as
/// `ORDER BY`, so the signal is the plan's own `output_rows`, which only move
/// while the plan is being DRIVEN.
pub(crate) struct PlanProgress(Option<(tokio::task::JoinHandle<()>, Arc<PlanProgressState>)>);

struct PlanProgressState {
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    progress: Arc<std::sync::atomic::AtomicU64>,
    last_rows: std::sync::atomic::AtomicU64,
}

impl PlanProgressState {
    fn sample(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let rows = plan_output_rows(self.plan.as_ref());
        // The periodic sample and final sample may race. Each observed row
        // contributes once even if the background task is still unwinding.
        let previous = self.last_rows.fetch_max(rows, Relaxed);
        self.progress.fetch_add(rows.saturating_sub(previous), Relaxed);
    }
}

impl PlanProgress {
    pub(crate) fn watch(plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> Self {
        const TICK: std::time::Duration = std::time::Duration::from_secs(15);
        let Ok(progress) = UNIT_PROGRESS.try_with(Arc::clone) else {
            return Self(None);
        };
        let state = Arc::new(PlanProgressState { plan, progress, last_rows: std::sync::atomic::AtomicU64::new(0) });
        let periodic = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(TICK).await;
                periodic.sample();
            }
        });
        Self(Some((handle, state)))
    }
}

impl Drop for PlanProgress {
    fn drop(&mut self) {
        if let Some((handle, state)) = self.0.take() {
            handle.abort();
            // A sequence of queries shorter than TICK must still keep its unit alive.
            state.sample();
        }
    }
}

/// Run `sql` to completion with the unit's liveness clock watching the plan.
///
/// Every blocking query in a maintenance unit must go through this: a bare
/// `collect()` reports nothing until it returns, so a unit inside a long
/// aggregate is indistinguishable from a stalled one and gets killed.
pub(crate) async fn collect_watched(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Result<Vec<arrow::array::RecordBatch>> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let _progress = PlanProgress::watch(Arc::clone(&plan));
    let started = std::time::Instant::now();
    let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    log_scan_pruning(plan.as_ref(), started.elapsed());
    Ok(batches)
}

fn plan_metric_fold(plan: &dyn datafusion::physical_plan::ExecutionPlan, of: impl Fn(&datafusion::physical_plan::metrics::MetricsSet) -> u64 + Copy) -> u64 {
    plan.children().iter().fold(plan.metrics().as_ref().map_or(0, of), |sum, child| sum.saturating_add(plan_metric_fold(child.as_ref(), of)))
}

fn plan_metric_sum(plan: &dyn datafusion::physical_plan::ExecutionPlan, name: &str) -> u64 {
    plan_metric_fold(plan, |metrics| metrics.sum_by_name(name).map_or(0, |value| value.as_usize() as u64))
}

/// How much of what a maintenance query COULD have read it actually read.
/// `bytes_scanned` is the only usable measure: predicate-excluded files are
/// dropped during PLANNING so they reach no `pruned` counter, and
/// `files_processed` is PER PARTITION so it grows with repartitioning.
fn log_scan_pruning(plan: &dyn datafusion::physical_plan::ExecutionPlan, elapsed: std::time::Duration) {
    let bytes_scanned = plan_metric_sum(plan, "bytes_scanned");
    if bytes_scanned == 0 {
        return;
    }
    info!(
        operation = UNIT_OPERATION.try_with(|operation| *operation).unwrap_or("none"),
        bytes_scanned,
        row_groups_pruned = plan_metric_sum(plan, "row_groups_pruned_statistics") + plan_metric_sum(plan, "row_groups_pruned_bloom_filter"),
        output_rows = plan_metric_sum(plan, "output_rows"),
        elapsed_ms = elapsed.as_millis() as u64,
        event = "maintenance_scan_pruning",
        "how much of the registered partition a maintenance query actually opened"
    );
}

fn plan_output_rows(plan: &dyn datafusion::physical_plan::ExecutionPlan) -> u64 {
    plan_metric_fold(plan, |metrics| metrics.output_rows().unwrap_or_default() as u64)
}

/// Run `work` until it stops making progress for `idle`, or until `cap` of total
/// wall clock has elapsed.
///
/// The idle window is a LIVENESS check, not a budget: killing a unit still
/// writing rows discards uncommitted work and re-queues the same slice. `cap`
/// bounds a unit holding a scarce permit, which the idle window alone does not.
async fn run_until_idle_capped<T>(
    idle: std::time::Duration, cap: Option<std::time::Duration>, progress: Arc<std::sync::atomic::AtomicU64>, work: impl Future<Output = T>,
) -> Result<T, tokio::time::error::Elapsed> {
    use std::sync::atomic::Ordering::Relaxed;
    let started = std::time::Instant::now();
    // BOXED, not `pin!`ed on the stack: `work` is the coordinator's whole
    // dispatch future and holding it inline overflows the worker stack in debug.
    let mut work = Box::pin(UNIT_PROGRESS.scope(Arc::clone(&progress), work));
    let mut last = progress.load(Relaxed);
    loop {
        match tokio::time::timeout(idle, &mut work).await {
            Ok(value) => return Ok(value),
            Err(elapsed) => match progress.load(Relaxed) {
                // Progress moved, so the unit is alive — but a permit-holding
                // lane still owes the rest of the fleet an upper bound.
                moved if moved != last && cap.is_none_or(|cap| started.elapsed() < cap) => last = moved,
                _ => return Err(elapsed),
            },
        }
    }
}

/// (project, slice_start, slice_end, generation, source_fp, source_rows) — the
/// coverage identity `recover_rollup_coverage` reads back off a tier file's tags.
type TaggedSliceIdentity = (String, i64, i64, String, u64, Option<u64>);

/// The same identity WITHOUT `source_rows`. Two files sharing this key and
/// disagreeing about the witness are a partial strip, not history predating the
/// tag.
type SliceKey = (String, i64, i64, String, u64);

use crate::database::rollup_unverifiable::{Tally, UnverifiableFate, UnverifiableReason};

/// Time ranges keyed by `(project, date)` — the untagged files' statistics
/// spans, and separately the tagged files' slice ranges.
type SpansByPartition = HashMap<(String, String), Vec<(i64, i64)>>;

/// One base cell a derived rollup aggregates: `(span, generation, measures it proved)`.
type BaseCell = ((i64, i64), String, Option<HashSet<String>>);

/// Per `(project, date)`: the timestamp spans of its UNTAGGED tier files, and
/// the slice ranges of its tagged ones.
type UntaggedPartitions = HashMap<(String, String), (Vec<(i64, i64)>, Vec<(i64, i64)>)>;

/// Are all of a staged intent's objects actually present, at the size recorded?
///
/// The only thing standing between a process killed mid-PUT and a commit that
/// references a short or absent object — nothing else notices.
async fn staged_objects_complete(store: &dyn object_store::ObjectStore, adds: &[deltalake::kernel::Add]) -> bool {
    use object_store::{ObjectStoreExt, path::Path as OsPath};
    futures::stream::iter(adds)
        .all(|add| async move {
            store.head(&OsPath::from(add.path.as_str())).await.is_ok_and(|meta| i64::try_from(meta.size).is_ok_and(|size| size == add.size))
        })
        .await
}

impl Database {
    /// The busy-pool backoff for `key`. Do NOT inline `self.journal().attempts()`
    /// into a `retry_task` argument: the temporary lives to the end of the
    /// statement and `retry_task` takes the same non-reentrant mutex.
    pub(crate) fn admission_backoff_for(&self, key: &crate::maintenance_coordinator::TaskKey) -> std::time::Duration {
        admission_backoff(self.journal().attempts(key))
    }

    /// Push a coordinator task's next attempt out by `delay`, journaled and checkpointed.
    fn retry_task(&self, key: &crate::maintenance_coordinator::TaskKey, reason: String, delay: std::time::Duration) -> Result<()> {
        let delay_micros = i64::try_from(delay.as_micros()).unwrap_or(i64::MAX);
        let mut journal = self.journal();
        // retry_or_split, not retry: a fast-fail retry (resource admission,
        // memory) repeats identically at the same size, so a unit whose estimate
        // can never be admitted would loop forever without reaching bisection.
        let attempts = journal.attempts(key);
        journal.retry_or_split(key, reason, crate::support::now_micros().saturating_add(delay_micros), attempts);
        journal.checkpoint()
    }

    /// The partition fingerprint the ticket re-check compares against
    /// `coverage.source_fp`.
    ///
    /// Must stay UNBOUNDED, because `source_fp` is RECORDED unbounded at publish
    /// time; a bound here would reject every partition it excluded anything from
    /// as `StaleCoverage`.
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
        self.persist_rollup_journal_bytes(crate::rollup_journal::encode(&self.rollup_journal_entries())?, false)
    }

    /// The journal's current entry set, with the gauges it also feeds.
    fn rollup_journal_entries(&self) -> Vec<crate::rollup_journal::RollupInvalidation> {
        let entries: Vec<_> = self
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
            .sorted_by(|a, b| (&a.source, &a.project_id, &a.date).cmp(&(&b.source, &b.project_id, &b.date)))
            .collect();
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

        entries
    }

    /// How stale the on-disk rollup journal may be — it caps durable writes to
    /// one per second rather than one per commit.
    const ROLLUP_JOURNAL_MAX_STALENESS: std::time::Duration = std::time::Duration::from_secs(1);

    /// Persist the encoded rollup journal, unless unchanged or not yet due.
    /// Safe to defer: the task journal is the source of truth and an absent entry
    /// here already means "full rebuild required". `force` bypasses the staleness
    /// window for shutdown, where no next commit carries the write.
    fn persist_rollup_journal_bytes(&self, bytes: Vec<u8>, force: bool) -> std::io::Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        let digest = digest_of(&bytes);
        let mut persisted = crate::support::lock(&self.rollup_journal_persisted);
        if persisted.digest == Some(digest) {
            stats.rollup_journal_persist_skipped.fetch_add(1, Relaxed);
            return Ok(());
        }
        if !force
            && let Some(at) = persisted.at
            && at.elapsed() < Self::ROLLUP_JOURNAL_MAX_STALENESS
        {
            stats.rollup_journal_persist_deferred.fetch_add(1, Relaxed);
            return Ok(());
        }
        crate::rollup_journal::store_encoded(&self.config.core.timefusion_data_dir, &bytes)?;
        persisted.digest = Some(digest);
        persisted.at = Some(std::time::Instant::now());
        stats.rollup_journal_persists.fetch_add(1, Relaxed);
        Ok(())
    }

    /// Write the rollup journal unconditionally, for shutdown — the one point
    /// where no later commit exists to carry a deferred write.
    pub(crate) fn flush_rollup_journal(&self) -> std::io::Result<()> {
        let _journal_guard = crate::support::lock(&self.rollup_journal_lock);
        self.persist_rollup_journal_bytes(crate::rollup_journal::encode(&self.rollup_journal_entries())?, true)
    }

    /// `mint_dedup=false` only for hours touched EXCLUSIVELY by self-authored
    /// DV-dedup commits (see [`DV_DEDUP_COMMIT_KEY`]); everything else must
    /// pass true.
    pub(crate) fn enqueue_maintenance_hours(&self, project_id: &str, source: &str, date: &str, hours: u32, mint_dedup: bool) -> std::io::Result<()> {
        self.mint_maintenance_hours(project_id, source, date, hours, mint_dedup)?;
        self.commit_journal()
    }

    /// Mint the slice work without making it durable — see
    /// [`Self::commit_journal`] for who pays for the `fsync` and when.
    fn mint_maintenance_hours(&self, project_id: &str, source: &str, date: &str, hours: u32, mint_dedup: bool) -> std::io::Result<()> {
        self.mint_invalidations(project_id, source, date, Some(hours), mint_dedup).map_err(std::io::Error::other)
    }

    fn enqueue_maintenance_partition(&self, project_id: &str, source: &str, date: &str) -> Result<()> {
        self.mint_invalidations(project_id, source, date, None, true)?;
        self.journal().checkpoint()
    }

    /// Invalidate every rollup spec of `source` over `date`: `Some(hours)` mints
    /// the dirty hour ranges precisely, `None` the whole day coarsely. Durability
    /// is the caller's — see [`Self::commit_journal`].
    fn mint_invalidations(&self, project_id: &str, source: &str, date: &str, hours: Option<u32>, mint: bool) -> Result<()> {
        let Some(schema) = get_schema(source) else { return Ok(()) };
        if schema.rollups.is_empty() || hours == Some(0) {
            return Ok(());
        }
        let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
        let day_start = day_start_micros(day).ok_or_else(|| anyhow::anyhow!("invalid maintenance date"))?;
        let observed_at_micros = crate::support::now_micros();
        let mut journal = self.journal();
        for spec in &schema.rollups {
            let target = spec.table_name(source);
            let ranges = hours.map_or_else(|| vec![(day_start, day_start.saturating_add(DAY_MICROS))], |hours| crate::rollup::dirty_ranges(day_start, hours));
            for (start_micros, end_micros) in ranges {
                let invalidation = crate::maintenance_coordinator::Invalidation {
                    source_table: source,
                    rollup_table: &target,
                    source,
                    project_id,
                    start_micros,
                    end_micros,
                    observed_at_micros,
                    derived: spec.derive_from.is_some(),
                    mint_dedup: mint,
                    // A DV-dedup-only hour needs no rollup rebuild either, so
                    // the two flags move together.
                    mint_rollup: mint,
                };
                if hours.is_some() {
                    journal.invalidate(invalidation)?;
                } else {
                    journal.invalidate_coarse(invalidation)?;
                }
            }
        }
        Ok(())
    }

    /// Reconcile the durable task cursor with each live Delta snapshot:
    /// metadata-only, and commits after the cursor contribute only the partitions
    /// named by data-changing Add/Remove actions. A crash before the cursor
    /// checkpoint repeats work; after it, every task checkpoint already happened.
    pub(crate) async fn reconcile_maintenance_task_cursors(&self) -> Result<usize> {
        // Inspect cached handles only: cold-loading a source here puts every
        // foreground reader and ingest writer behind a Delta-log replay.
        let mut queued = 0usize;
        let tables = self.all_tables().await;
        'sources: for (storage_project, source, table_ref) in &tables {
            let Some(schema) = get_schema(source).filter(|schema| !schema.rollups.is_empty()) else { continue };
            let (version, log_store) = {
                let table = table_ref.read().await;
                (table.version().unwrap_or_default(), table.log_store())
            };
            let cursor_key = format!("{storage_project}:{source}");
            // The first coordinator start has no durable cursor: baseline at the
            // loaded snapshot rather than expanding the whole table history.
            // Bound to its own statement so the journal guard is DROPPED before
            // the else block takes it again — see `admission_backoff_for`.
            let current_cursor = self.journal().source_cursor(&cursor_key);
            let Some(cursor) = current_cursor else {
                let mut journal = self.journal();
                journal.set_source_cursor(cursor_key, version);
                journal.checkpoint()?;
                continue;
            };
            if cursor >= version {
                continue;
            }
            // Per partition, the hours the missed commits can have touched.
            // ALL_HOURS per changed partition instead would make every restart
            // re-enqueue the day and reset completed frontier work to Pending.
            let mut partition_hours: HashMap<(String, String), u32> = HashMap::new();
            let mut missing_commit = None;
            // Hours needing a Dedup re-mint, from UNTAGGED commits only: a
            // `DV_DEDUP_COMMIT_KEY` commit adds no rows, so re-minting Dedup from
            // it would upsert already-Complete slices back to Pending forever.
            let mut dedup_hours: HashMap<(String, String), u32> = HashMap::new();
            for commit_version in cursor.saturating_add(1)..=version {
                let Some(bytes) = log_store.read_commit_entry(commit_version).await? else {
                    missing_commit = Some(commit_version);
                    break;
                };
                let actions = deltalake::logstore::get_actions(commit_version, &bytes)?;
                let dv_dedup_commit = actions.iter().any(
                    |action| matches!(action, deltalake::kernel::Action::CommitInfo(ci) if ci.info.get(DV_DEDUP_COMMIT_KEY).and_then(serde_json::Value::as_bool) == Some(true)),
                );
                let mut partitions_with_adds = HashSet::new();
                let mut remove_only: HashSet<(String, String)> = HashSet::new();
                for action in actions {
                    match action {
                        deltalake::kernel::Action::Add(add) if add.data_change => {
                            let Some(partition) = Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default") else { continue };
                            let mask = chrono::NaiveDate::parse_from_str(&partition.1, "%Y-%m-%d")
                                .ok()
                                .and_then(day_start_micros)
                                .and_then(|day_start| add.stats.as_deref().and_then(|stats| crate::rollup::hours_from_stats_json(stats, day_start)))
                                .unwrap_or(crate::rollup::ALL_HOURS);
                            partitions_with_adds.insert(partition.clone());
                            if !dv_dedup_commit {
                                *dedup_hours.entry(partition.clone()).or_insert(0) |= mask;
                            }
                            *partition_hours.entry(partition).or_insert(0) |= mask;
                        }
                        // A Remove carries no stats; in a rewrite commit its span
                        // is covered by the paired Adds. Only a partition with
                        // removes and NO adds needs the conservative day.
                        deltalake::kernel::Action::Remove(remove) if remove.data_change => {
                            let Some(partition) = Self::maintenance_partition_from_action(&remove.path, remove.partition_values.as_ref(), "default") else {
                                continue;
                            };
                            remove_only.insert(partition);
                        }
                        _ => {}
                    }
                }
                // Conservative day regardless of the tag: a tagged commit never
                // removes without a paired Add, so if one does, mint everything.
                for partition in remove_only.difference(&partitions_with_adds).cloned() {
                    dedup_hours.insert(partition.clone(), crate::rollup::ALL_HOURS);
                    partition_hours.insert(partition, crate::rollup::ALL_HOURS);
                }
            }
            if let Some(missing) = missing_commit {
                // A current snapshot replaces unavailable change history only
                // after ALL known source AND output partitions are invalidated:
                // a missed DELETE leaves no source Add but an old aggregate.
                let targets: HashSet<_> = schema.rollups.iter().map(|spec| spec.table_name(source)).collect();
                for (project, name, handle) in &tables {
                    if project != storage_project || (name != source && !targets.contains(name)) {
                        continue;
                    }
                    let table = handle.read().await;
                    for file in table.snapshot()?.log_data().iter() {
                        // A valid Delta file need not use Hive directory labels;
                        // read partition scalars without materializing stats.
                        let values = file.partition_values().map(|values| {
                            values
                                .fields()
                                .iter()
                                .zip(values.values())
                                .map(|(field, value)| {
                                    (field.name().to_owned(), (!value.is_null()).then(|| deltalake::kernel::scalars::ScalarExt::serialize(value)))
                                })
                                .collect::<HashMap<_, _>>()
                        });
                        let Some(partition) = Self::maintenance_partition_from_action(&file.path(), values.as_ref(), "default")
                            .filter(|(_, date)| chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok())
                        else {
                            // Do not advance past unknown data, nor block healthy sources.
                            warn!(source, storage_project, cursor, table = name, path = %file.path(), event = "maintenance_partition_reconcile_failed");
                            continue 'sources;
                        };
                        partition_hours.insert(partition, crate::rollup::ALL_HOURS);
                    }
                }
                // The missing commits make every hour bound uncertain.
                partition_hours.values_mut().for_each(|hours| *hours = crate::rollup::ALL_HOURS);
                warn!(
                    source,
                    storage_project,
                    cursor,
                    missing,
                    version,
                    partitions = partition_hours.len(),
                    event = "maintenance_history_gap_recovery",
                    "requeueing whole partitions from live metadata before advancing an expired cursor"
                );
            }
            for ((partition_project, date), hours) in partition_hours {
                let project = if storage_project.is_empty() { partition_project.clone() } else { storage_project.clone() };
                if missing_commit.is_some() {
                    self.enqueue_maintenance_partition(&project, source, &date)?;
                    queued = queued.saturating_add(1 + schema.rollups.len());
                } else {
                    // The two halves of one partition's hours — mint both, commit once.
                    let with_dedup = dedup_hours.get(&(partition_project, date.clone())).copied().unwrap_or(0) & hours;
                    self.mint_maintenance_hours(&project, source, &date, with_dedup, true)?;
                    self.mint_maintenance_hours(&project, source, &date, hours & !with_dedup, false)?;
                    self.commit_journal()?;
                    queued = queued.saturating_add(usize::try_from(hours.count_ones()).unwrap_or(24) * schema.rollups.len());
                }
                tokio::task::yield_now().await;
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
        let created_unix_ms = unix_ms(now);
        let mut planned = Vec::new();
        // Every table EXCEPT a rollup tier: packing merges files from different
        // slices, which drops the coverage identity tags a tier file must carry.
        let tiers: HashSet<String> = crate::schema::registry()
            .list_tables()
            .into_iter()
            .filter_map(|name| get_schema(&name).map(|schema| (name, schema)))
            .flat_map(|(name, schema)| schema.rollups.iter().map(|spec| spec.table_name(&name)).collect::<Vec<_>>())
            .collect();
        // Retire work for a tier that no longer exists — a spec removal or rename
        // would otherwise leave its queued tasks claimable forever.
        {
            let mut journal = self.journal();
            let retired = journal.retire_undeclared_tiers(&tiers);
            if retired != 0 {
                let _ = journal.compact();
                warn!(retired, event = "maintenance_undeclared_tier_tasks_retired", "queued work for a tier no longer declared");
            }
        }
        // Dirty bins whose source declares rollups have no consumer (the dedup
        // cron skips those tables), so retire them rather than suppress them.
        self.retire_undrainable_dirty_bins();
        // `planned=N` alone cannot distinguish "nothing to do" from "queued but
        // never claimed", so log why the biggest hygiene debt is unclaimed.
        {
            let journal = self.journal();
            for operation in [Operation::SealedConsolidation, Operation::HotPacking] {
                if let Some(refusal) = journal.most_indebted_unclaimed(operation, now) {
                    info!(?operation, refusal, event = "maintenance_hygiene_debt_unclaimed", "the most indebted hygiene cell is not being claimed");
                }
                // `benefit` buckets file counts by 64, so a lane whose cells are
                // all smaller ranks them all at zero and falls through to recency.
                if let Some(spread) = journal.hygiene_debt_spread(operation, now) {
                    info!(?operation, spread, event = "maintenance_hygiene_debt_spread", "how far apart the ranker can tell this lane's cells");
                }
            }
        }
        for (storage_project, source, table_ref) in self.all_tables().await {
            if tiers.contains(&source) {
                continue;
            }
            let schema = schema_or_default(&source);
            // The file count IS the benefit for hygiene: `scheduling_class` ranks
            // sealed hygiene on it, not on which cell sealed first.
            let mk_task = |project_id: &str, slice, operation, files: &[&CompactionDebtFile], created_unix_ms| {
                let estimate = files.iter().fold(0u64, |bytes, file| bytes.saturating_add(estimated_decoded_bytes(file.size)));
                MaintenanceTask {
                    key: TaskKey { physical_table: source.clone(), source: source.clone(), project_id: project_id.to_owned(), slice, operation },
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
                    input: Some(crate::maintenance_coordinator::InputFootprint::new(files.iter().map(|file| &file.path), estimate)),
                    parent_measured_bytes: None,
                    preflight_decoded_bytes: None,
                    backfill_priority_micros: None,
                }
            };
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
                    let rows = file.num_records().and_then(|n| u64::try_from(n).ok());
                    partitions.entry((project, date)).or_default().push(CompactionDebtFile { size: file.size(), path: path.to_string(), rows });
                }
            }
            // Anything seen-but-not-planned is COMPLIANT, which is what retires
            // the stale queue below.
            let mut seen: HashSet<(String, chrono::NaiveDate)> = HashSet::new();
            let mut planned_keys: HashSet<(String, chrono::NaiveDate, Operation)> = HashSet::new();
            for ((project_id, date), files) in partitions {
                seen.insert((project_id.clone(), date));
                // Future event timestamps are neither hot nor sealed; they become
                // eligible normally when their UTC date arrives.
                if date > today {
                    continue;
                }
                let day_start = day_start_micros(date).ok_or_else(|| anyhow::anyhow!("invalid compaction date"))?;
                let slice = TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS))?;
                let small_target = if date == today { COORDINATOR_HOT_TARGET_BYTES } else { COORDINATOR_SEALED_TARGET_BYTES };
                // SIZE only. Sortedness belongs to Repair below: an untagged file
                // is only a *suspect*, and admitting on that would put every
                // flush-written partition permanently out of policy.
                let small = files.iter().filter(|file| file.size < small_target).sorted_by_key(|file| file.size).collect_vec();
                // The same admission test the PACKER applies (sum fits the target
                // on BOTH bytes and rows), so a queued unit can always retire at
                // least one file. Unknown row counts are no objection.
                let mergeable = small.len() >= 2 && packer_admits_pair((small[0].size, small[1].size), (small[0].rows, small[1].rows), small_target);
                if mergeable {
                    let operation = if date == today { Operation::HotPacking } else { Operation::SealedConsolidation };
                    planned_keys.insert((project_id.clone(), date, operation));
                    // Age from when the partition SEALED, so starvation escalation
                    // survives the restarts that re-derive this queue.
                    let sealed_at_ms = unix_ms(slice.end_micros.max(0));
                    let created_unix_ms = if date == today { created_unix_ms } else { sealed_at_ms.min(created_unix_ms) };
                    planned.push(mk_task(&project_id, slice, operation, &small, created_unix_ms));
                }
                if date < today && !schema.sorting_columns.is_empty() {
                    let suspects = files.iter().filter(|file| !self.repair_verified_sorted.contains(&file.path)).collect::<Vec<_>>();
                    if !suspects.is_empty() {
                        planned_keys.insert((project_id.clone(), date, Operation::Repair));
                        planned.push(mk_task(&project_id, slice, Operation::Repair, &suspects, created_unix_ms));
                    }
                }
            }
            // Retire hygiene tasks for partitions this scan proved compliant.
            // Only partitions this pass SAW: absent means unknown, not clean.
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
                // `enqueue_planned`, not `enqueue`: it keeps the footprint this
                // scan measured, which is the benefit term of `scheduling_class`.
                journal.enqueue_planned(&task);
            }
            journal.checkpoint()?;
        }
        Ok(count)
    }

    /// Run coordinator rollup units until the queue stops yielding work or `max_units` have run.
    ///
    /// Does not plan internally: call `plan_rollup_backfill` first, advance the clock past
    /// `FINALIZATION_DELAY`, then drain. Test-only alternative to the concurrent worker pool.
    pub async fn drain_coordinator_rollups(&self, max_units: usize) -> Result<usize> {
        use crate::maintenance_coordinator::Operation;
        let mut ran = 0;
        for _ in 0..max_units {
            let mut progressed = 0;
            for operation in [Operation::BaseRollup, Operation::DerivedRollup] {
                progressed += usize::from(self.run_coordinator_rollup_once(operation).await?);
            }
            ran += progressed;
            if progressed == 0 {
                break;
            }
        }
        Ok(ran)
    }

    /// Enqueue rollup work for sealed days that have source data but no rollup output yet.
    ///
    /// `reconcile_maintenance_task_cursors` only enqueues partitions named by commits after its
    /// durable cursor, so a day written before rollups existed is never enqueued by anything else.
    /// Newest-first, at most `BACKFILL_PARTITIONS_PER_PASS` per pass.
    pub async fn plan_rollup_backfill(&self) -> Result<usize> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskJournal, TaskKey, TaskState, TimeSlice, blocks_rollup_backfill};
        /// Cells admitted newest-first per pass.
        const BACKFILL_PARTITIONS_PER_PASS: usize = 24;
        /// Stop queueing history while the queue is deep: every `claim_next`
        /// scans the task set, so an over-full journal taxes the live frontier.
        const BACKFILL_PENDING_CEILING: usize = 25_000;

        let horizon = i64::from(self.config.maintenance.timefusion_rollup_backfill_days);
        if horizon == 0 {
            return Ok(0);
        }
        // Defers the ENQUEUE, not the pass: the rest of this function still
        // recomputes gauges and proves base tiers, which cost the journal nothing.
        let defer_enqueue = {
            let journal = self.journal();
            let pending = journal.tasks().filter(|task| task.state != TaskState::Complete).count();
            let defer = pending >= BACKFILL_PENDING_CEILING && !coverage_is_short();
            if defer {
                debug!(pending, ceiling = BACKFILL_PENDING_CEILING, event = "rollup_backfill_enqueue_deferred");
            }
            defer
        };
        let today = crate::support::today_utc();
        let earliest = today - chrono::Duration::days(horizon);
        let mut queued = 0usize;
        // The fleet gauge folds every (source, tier) into one worst case: the
        // first tier of a sweep seeds it and the rest minimise into it.
        let mut first_tier_of_sweep = true;
        // Accumulated across EVERY source, published once below — the setters
        // replace wholesale, so a per-source call keeps only the last table.
        let mut all_base_tier_ready: HashSet<(String, String, String)> = HashSet::new();
        let mut all_tier_holes: HashSet<(String, String, String, String)> = HashSet::new();

        for (storage_project, source, table_ref) in self.all_tables().await {
            let Some(schema) = get_schema(&source) else { continue };
            if schema.rollups.is_empty() {
                continue;
            }
            // Metadata only. A partition of EMPTY files is not coverage; missing
            // stats mean UNKNOWN and count as covered.
            let default_project = if storage_project.is_empty() { "default" } else { storage_project.as_str() };

            let source_partitions = {
                let table = table_ref.read().await;
                Self::maintenance_table_partitions(&table, default_project)?
            };
            // Taken from the source, not the tier: asking the tier would let a
            // broken rollup hide the failure this metric exists to catch.
            let active_projects: HashSet<&str> =
                source_partitions.iter().filter(|(_, date)| *date >= today - chrono::Duration::days(1)).map(|(project, _)| project.as_str()).collect();
            let candidates: Vec<(String, chrono::NaiveDate)> = source_partitions
                .iter()
                // Today is the live frontier's job; only sealed days are backfill.
                .filter(|(_, date)| *date < today && *date >= earliest)
                .cloned()
                .collect();
            // Which TIERS each day is missing, per tier — enqueueing every tier
            // for a day that lacks only one rebuilds rollups that already exist.
            let mut covered_per_tier: Vec<(usize, HashSet<(String, chrono::NaiveDate)>)> = Vec::new();

            // A day must be covered by EVERY declared tier: a 30d panel reads the
            // coarse tier, so a hole there refuses it however complete 1m is.
            for (index, spec) in schema.rollups.iter().enumerate() {
                let target = spec.table_name(&source);
                let Ok(target_ref) = self.resolve_table(&storage_project, &target).await else { continue };
                let (covered, tier_created_ms) = {
                    let table = target_ref.read().await;
                    (
                        Self::maintenance_table_partitions(&table, default_project)?,
                        table.snapshot().ok().and_then(|state| state.snapshot().metadata().created_time()),
                    )
                };
                // A tier younger than the horizon cannot hold that many days, so
                // a low number from it is ramp-up and must not move the gauges.
                let horizon_ms = horizon.saturating_mul(24 * 60 * 60 * 1_000);
                let tier_is_ramping =
                    tier_created_ms.is_some_and(|created| crate::support::now_micros().div_euclid(1_000).saturating_sub(created) < horizon_ms);
                // Days back from yesterday covered with NO hole, minimised over projects.
                let (contiguous, worst_project, median_contiguous) = min_contiguous_days(&covered, &source_partitions, today, &active_projects);
                for (gauge, value) in [
                    (&crate::observability::maintenance_stats().rollup_median_contiguous_days, median_contiguous),
                    (&crate::observability::maintenance_stats().rollup_min_contiguous_days, contiguous),
                ] {
                    let previous = gauge.load(std::sync::atomic::Ordering::Relaxed);
                    if let Some(folded) = fold_fleet_gauge(previous, value, !first_tier_of_sweep, tier_is_ramping) {
                        gauge.store(folded, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                // Only a REAL tier consumes the seed; a ramping tier's value is provisional.
                first_tier_of_sweep &= tier_is_ramping;
                // `contiguous_days` counts DATE PARTITIONS and ignores generation;
                // `usable_cells` is the read path's own answer, so a gap between the
                // two means a spec change orphaned coverage.
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
            // Which (project, date) have their BASE tier built; consulted by DAY.
            all_base_tier_ready.extend(
                covered_per_tier
                    .iter()
                    .filter(|(index, _)| schema.rollups[*index].derive_from.is_none())
                    .flat_map(|(_, covered)| covered.iter().map(|(project, date)| (source.clone(), project.clone(), date.to_string()))),
            );
            let mut missing_tiers = tiers_missing_per_day(&candidates, &covered_per_tier);
            // ONE-SHOT REPAIR, bounded to `[ORPHAN_REPAIR_FROM, ORPHAN_REPAIR_BEFORE)`:
            // a spec edit that changes `generation_id` orphans earlier slices, and
            // coverage ignores generation, so force those days back into
            // `missing_tiers` for the ordinary enqueue path below.
            if let Some(cursor_was) = self.journal().repair_orphaned_coverage_once(&source)
                && let (Ok(from), Ok(before)) =
                    (chrono::NaiveDate::parse_from_str(ORPHAN_REPAIR_FROM, "%Y-%m-%d"), chrono::NaiveDate::parse_from_str(ORPHAN_REPAIR_BEFORE, "%Y-%m-%d"))
            {
                let mut forced = 0usize;
                for (project, date) in &candidates {
                    if *date >= from && *date < before {
                        missing_tiers.insert((project.clone(), *date), (0..schema.rollups.len()).collect());
                        forced += 1;
                    }
                }
                warn!(
                    source,
                    forced,
                    cursor_was,
                    from = ORPHAN_REPAIR_FROM,
                    before = ORPHAN_REPAIR_BEFORE,
                    event = "rollup_orphaned_coverage_repair",
                    "re-enqueueing coverage a spec change orphaned and the planner cannot see"
                );
            }
            // A configured damage list, forced the same way: these cells HAVE tier
            // output, so `missing_tiers` never sees them. Offered a PREFIX per pass
            // against a durable cursor; nothing is consumed until it survives truncation.
            let damage_repair = damaged_cells_newest_first(&self.config.maintenance.timefusion_damage_repair_cells);
            let damage_from = self.journal().repair_cursor(TaskJournal::DAMAGE_REPAIR_MIGRATION, &source);
            // An empty candidate set means the source has no partitions at all;
            // burning the cursor against it would silently drop the list.
            let damage_offered: &[BackfillCell] = if candidates.is_empty() { &[] } else { damage_repair.get(damage_from..).unwrap_or_default() };
            let damage_offered = &damage_offered[..damage_offered.len().min(BACKFILL_PARTITIONS_PER_PASS)];
            // Only cells this source has candidates for: a pair outside the
            // retention horizon is not resurrectable, though it is still consumed.
            let damage_forced: HashSet<BackfillCell> = damage_offered.iter().filter(|cell| candidates.contains(cell)).cloned().collect();
            for cell in &damage_forced {
                missing_tiers.insert(cell.clone(), (0..schema.rollups.len()).collect());
            }
            // Publish the holes so `claim_next` ranks them ahead of days that
            // already have tier output.
            let (source_ref, rollups) = (&source, &schema.rollups);
            all_tier_holes.extend(missing_tiers.iter().flat_map(|((project, date), missing)| {
                missing.iter().map(move |index| (source_ref.clone(), project.clone(), rollups[*index].table_name(source_ref), date.to_string()))
            }));
            let mut want: Vec<(String, chrono::NaiveDate)> = missing_tiers.keys().cloned().collect();
            let cells_missing = want.len();
            // Skip work already queued, keyed on (project, date, TABLE) and scoped to
            // rollup operations. Both narrowings are load-bearing: unrelated file debt
            // must not veto rollup coverage, and tiers do not block each other.
            let queued_tables: HashSet<(String, chrono::NaiveDate, String)> = {
                let journal = self.journal();
                journal
                    .tasks()
                    .filter(|task| task.key.source == source && blocks_rollup_backfill(task))
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
            // The days just filtered out most need the proof: a derived unit blocked by
            // `dependencies_complete` stays queued forever, making its day permanently
            // ineligible above. Proving over ALL candidate days mints nothing.
            {
                let mut journal = self.journal();
                let mut proven = 0usize;
                for ((project_id, date), missing) in &missing_tiers {
                    if *date >= today || missing.iter().any(|index| schema.rollups[*index].derive_from.is_none()) {
                        continue;
                    }
                    let Some(day_start) = day_start_micros(*date) else { continue };
                    let Ok(slice) = TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS)) else { continue };
                    for spec in missing.iter().map(|index| &schema.rollups[*index]).filter(|spec| spec.derive_from.is_some()) {
                        proven += journal.prove_base_tier_for_day(
                            &TaskKey {
                                physical_table: spec.table_name(&source),
                                source: source.clone(),
                                project_id: project_id.clone(),
                                slice,
                                operation: Operation::DerivedRollup,
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
            // `cells_missing` is what coverage says is absent, `cells_wanted` what
            // survives the already-queued veto.
            let (derived_pending, derived_sealed, derived_unproven, derived_quarantined, derived_not_due) = {
                let journal = self.journal();
                journal.claimability_census(Operation::DerivedRollup, crate::support::now_micros())
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
                        .first_refused_sealed(Operation::DerivedRollup, crate::support::now_micros())
                        .map_or_else(|| "none_pending".to_owned(), |(project, date, reason)| format!("{reason}:{project:.8}:{date}"))
                },
                cells_admitted = want.len().min(BACKFILL_PARTITIONS_PER_PASS),
                defer_enqueue,
                event = "rollup_backfill_census"
            );
            if want.is_empty() || defer_enqueue {
                continue;
            }
            // Newest first; damage-repair cells outrank that.
            let total = want.len();
            // Forced cells the already-queued veto ate, measured while `want` still
            // distinguishes them from the ones truncation will drop.
            let damage_queue_vetoed = {
                let wanted: HashSet<&BackfillCell> = want.iter().collect();
                damage_forced.iter().filter(|cell| !wanted.contains(cell)).count()
            };
            let (want, damage_consumed) = admit_backfill_pass(want, damage_offered, &damage_forced, BACKFILL_PARTITIONS_PER_PASS);
            let damage_admitted: HashSet<BackfillCell> = want.iter().filter(|cell| damage_forced.contains(*cell)).cloned().collect();
            // DAY-sized units: `split_time_task` splits on OBSERVED bytes until each
            // child fits MAX_DECODED_BYTES, so coarse is safe and fine is merely slow.
            let now = crate::support::now_micros();
            let created_unix_ms = unix_ms(now);
            // Forced cells the QUEUE refused — only measurable here.
            let mut damage_vetoed = 0usize;
            {
                let mut journal = self.journal();
                for (project_id, date) in &want {
                    let Some(day_start) = day_start_micros(*date) else { continue };
                    let Ok(slice) = TimeSlice::new(day_start, day_start.saturating_add(DAY_MICROS)) else { continue };
                    let mut refused = false;
                    let mut enqueue = |physical_table: String, operation, base_tier_present| {
                        journal.enqueue_with_base_tier(
                            TaskKey { physical_table, source: source.clone(), project_id: project_id.clone(), slice, operation },
                            now,
                            MAX_DECODED_BYTES,
                            created_unix_ms,
                            base_tier_present,
                        )
                    };
                    // Only the tiers this day is actually missing.
                    let missing = missing_tiers.get(&(project_id.clone(), *date)).cloned().unwrap_or_default();
                    let needs_source_scan = missing.iter().any(|index| schema.rollups[*index].derive_from.is_none());
                    // Dedup only when something must read RAW anyway: a derived tier
                    // aggregates the base TIER, not the source.
                    if needs_source_scan && !queued_tables.contains(&(project_id.clone(), *date, source.clone())) {
                        refused |= !enqueue(source.clone(), Operation::Dedup, false);
                    }
                    for index in missing {
                        let spec = &schema.rollups[index];
                        let operation = if spec.derive_from.is_some() { Operation::DerivedRollup } else { Operation::BaseRollup };
                        // `!needs_source_scan` means the base tier is already built.
                        // Sealed days only: coverage reports PRESENCE, which is
                        // misleading for a day still being written.
                        let physical_table = spec.table_name(&source);
                        if queued_tables.contains(&(project_id.clone(), *date, physical_table.clone())) {
                            continue;
                        }
                        let base_proven = operation == Operation::DerivedRollup && !needs_source_scan && *date < today;
                        refused |= !enqueue(physical_table, operation, base_proven);
                    }
                    if refused && damage_admitted.contains(&(project_id.clone(), *date)) {
                        damage_vetoed = damage_vetoed.saturating_add(1);
                    }
                    queued = queued.saturating_add(1);
                }
                journal.checkpoint()?;
            }
            // AFTER the enqueue's checkpoint, and only by cells that survived
            // truncation — the other order loses cells on a crash.
            if damage_consumed != 0 {
                self.journal().advance_repair_cursor(TaskJournal::DAMAGE_REPAIR_MIGRATION, &source, damage_from.saturating_add(damage_consumed))?;
            }
            if !damage_offered.is_empty() {
                warn!(
                    source,
                    offered = damage_offered.len(),
                    enqueued = damage_admitted.len().saturating_sub(damage_vetoed),
                    vetoed = damage_vetoed + damage_queue_vetoed,
                    // Held back by the per-pass bound, NOT dropped: the next pass re-offers them.
                    truncated = damage_forced.len().saturating_sub(damage_admitted.len() + damage_queue_vetoed),
                    out_of_horizon = damage_offered.len() - damage_forced.len(),
                    cursor_was = damage_from,
                    cursor = damage_from + damage_consumed,
                    listed = damage_repair.len(),
                    event = "rollup_damaged_cell_repair",
                    "re-enqueueing cells the derived-witness bug published short"
                );
            }
            info!(source, queued = want.len(), remaining = total - want.len(), horizon_days = horizon, event = "rollup_backfill_planned");
        }
        // Published ONCE, after every source has contributed: these setters replace wholesale.
        {
            let mut journal = self.journal();
            journal.set_base_tier_ready(all_base_tier_ready);
            journal.set_tier_holes(all_tier_holes);
        }
        Ok(queued)
    }

    fn maintenance_table_partitions(table: &DeltaTable, default_project: &str) -> Result<HashSet<(String, chrono::NaiveDate)>> {
        Ok(table
            .snapshot()?
            .log_data()
            .iter()
            .filter_map(|file| {
                let add = add_action(&file);
                if partition_file_is_empty(add.get_stats().ok().flatten().map(|stats| stats.num_records)) {
                    return None;
                }
                let (project, date) = Self::maintenance_partition_from_action(&file.path(), None, default_project)?;
                Some((project, date.parse::<chrono::NaiveDate>().ok()?))
            })
            .collect())
    }

    /// One of the tags a maintenance unit stamps on the files it publishes.
    fn add_tag<'a>(add: &'a deltalake::kernel::Add, name: &str) -> Option<&'a str> {
        add.tags.as_ref()?.get(name)?.as_deref()
    }

    /// Reads back the `(start, end)` slice an `Add` was tagged with.
    fn slice_tag_range(add: &deltalake::kernel::Add) -> Option<(i64, i64)> {
        Some((
            Self::add_tag(add, crate::maintenance_coordinator::TAG_SLICE_START)?.parse().ok()?,
            Self::add_tag(add, crate::maintenance_coordinator::TAG_SLICE_END)?.parse().ok()?,
        ))
    }

    fn tag_project(add: &deltalake::kernel::Add) -> Option<&str> {
        Self::add_tag(add, crate::maintenance_coordinator::TAG_PROJECT)
    }

    /// Decoded bytes a unit reads from one file, narrowed to the columns it
    /// projects: `(prorated to the slice, whole file)`. The unprorated figure is
    /// what a sibling slice over the same file would re-read.
    fn projected_slice_bytes(add: &deltalake::kernel::Add, numerator: u64, denominator: u64, slice: crate::maintenance_coordinator::TimeSlice) -> (u64, u64) {
        let projected = estimated_decoded_bytes(add.size).saturating_mul(numerator).div_ceil(denominator);
        let (min, max) = add_ts_bounds(add);
        let (share, whole) = slice_share_of_file(min, max, slice, estimated_row_groups(add.size));
        (projected.saturating_mul(share).div_ceil(whole.max(1)), projected)
    }

    /// Claim one unit, bounding how many quarantined units run at once. The permit
    /// is taken BEFORE the claim (quarantine is only knowable once selected) and
    /// released again when the claim turns out to be ordinary work.
    fn claim_coordinator_task(
        &self, selection: TaskSelection<'_>,
    ) -> Option<(crate::maintenance_coordinator::MaintenanceTask, Option<tokio::sync::OwnedSemaphorePermit>)> {
        let permit = Arc::clone(&self.maintenance_quarantine_slots).try_acquire_owned().ok();
        let task = {
            let mut journal = self.journal();
            let now = crate::support::now_micros();
            match selection {
                TaskSelection::Next(operation) => journal.claim_next(operation, now, permit.is_some()),
                TaskSelection::Exact(key) => journal.claim_exact(key, now, permit.is_some()),
            }?
        };
        let quarantined = crate::maintenance_coordinator::TaskJournal::is_quarantined(&task);
        Some((task, permit.filter(|_| quarantined)))
    }

    fn log_task_started(&self, task: &crate::maintenance_coordinator::MaintenanceTask) {
        let key = &task.key;
        info!(operation = ?key.operation, table = %key.physical_table, project_id = %key.project_id, slice_start = key.slice.start_micros, slice_end = key.slice.end_micros,
            estimated_decoded_bytes = task.estimated_decoded_bytes, attempts = task.attempts,
            input_fp = task.input.map(|input| input.fp), event = "maintenance_task_started");
    }

    pub(crate) async fn run_coordinator_dedup_once(&self) -> Result<bool> {
        self.run_coordinator_dedup_selected(TaskSelection::Next(crate::maintenance_coordinator::Operation::Dedup)).await
    }

    async fn run_coordinator_dedup_selected(&self, selection: TaskSelection<'_>) -> Result<bool> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Resources};
        use std::sync::atomic::Ordering::Relaxed;

        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(selection) else { return Ok(false) };
        let key = task.key.clone();
        self.log_task_started(&task);
        let _lease = crate::maintenance_coordinator::TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        // Parks the unit and reports it as "ran" — every early exit here is a retry.
        let retry = |reason: String, delay: std::time::Duration| -> Result<bool> {
            self.retry_task(&key, reason, delay)?;
            Ok(true)
        };

        if self.buffered_layer().is_some_and(|layer| layer.has_rows_in_range(&key.project_id, &key.source, key.slice.start_micros, key.slice.end_micros)) {
            return retry("source_not_flushed".to_owned(), buffered_source_retry_delay(key.slice, crate::support::now_micros()));
        }
        let Some(date) = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive()) else {
            return retry("invalid_slice_timestamp".to_owned(), std::time::Duration::from_secs(3_600));
        };
        let table = match self.resolve_table(&key.project_id, &key.source).await {
            Ok(table) => table,
            Err(error) => return retry(format!("resolve_source: {error:#}"), std::time::Duration::from_secs(30)),
        };
        // Journal invalidations start with no byte estimate, so estimate the narrow
        // dedup projection from files whose statistics overlap this exact slice.
        let (estimated_bytes, whole_file_bytes, selected_paths, dedup_rows) = {
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
                    let add = add_action(&file);
                    if stats_disjoint_from(&add, key.slice) {
                        return None;
                    }
                    let (share, whole) = Self::projected_slice_bytes(&add, projected_numerator, projected_denominator, key.slice);
                    Some((file.path().to_string(), share, whole, add_row_count(&add).unwrap_or_default()))
                })
                .fold((0u64, 0u64, Vec::new(), 0u64), |(share, whole, mut paths, rows), (path, file_share, file_whole, file_rows)| {
                    paths.push(path);
                    (share.saturating_add(file_share), whole.saturating_add(file_whole), paths, rows.saturating_add(file_rows))
                })
        };
        let input_footprint = crate::maintenance_coordinator::InputFootprint::new(selected_paths, whole_file_bytes);
        // Before the split test: a unit that FITS still needs this, since a later
        // timeout bisect knows only the key.
        if self.journal().record_preflight(&key, Some(input_footprint), estimated_bytes) {
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
        // The unit's OWN size, not the fleet maximum: admission scales its ceiling by
        // pool occupancy, so always asking for `MAX_DECODED_BYTES` starves on a busy pool.
        let request = Resources { cpu: 1, decoded_bytes: estimated_bytes.clamp(1, MAX_DECODED_BYTES), object_reads: 1, object_writes: 1 };
        let Some(_permit) = self.maintenance_admission.try_acquire(request) else {
            // Deliberately NOT `resource_admission`: that reason makes `retry_or_split`
            // split the unit. The request is clamped, so a refusal means only "busy now".
            return retry("admission_busy".to_owned(), admission_backoff(task.attempts));
        };
        let probe_hash_shards = usize::try_from(estimated_bytes.div_ceil(MAX_DECODED_BYTES).clamp(1, DEDUP_BUCKET_COUNT)).unwrap_or(1);
        let limits = DedupExecutionLimits {
            max_decoded_bytes: MAX_DECODED_BYTES,
            max_concurrent_shards: 1,
            probe_hash_shards,
            sort_partitions: dedup_sort_partitions(task.attempts),
            batch_rows: batch_rows_for(estimated_bytes, dedup_rows, self.config.maintenance.timefusion_maintenance_batch_target_bytes),
        };
        // Certification is a property of the whole PARTITION, keyed on
        // (project, table, date). Unit shape must NOT gate the grant: each clean pass
        // records its slice, and `record_clean_slice` certifies once the union covers
        // the UTC day over one unmoved file fingerprint.
        let (pre_files, pre_dv) = {
            let table = table.read().await;
            (
                Self::partition_files_by_pid(&table, &format!("date={date}"))?.remove(&key.project_id).unwrap_or_default(),
                Self::partition_dv_state(&table, &key.project_id, &format!("date={date}"))?,
            )
        };
        match self.dedup_partition_range_limited(&table, &key.source, &key.project_id, date, Some(key.slice), Some(limits)).await {
            Ok((dropped, true, masked)) => {
                // Before the journal lock: `record_clean_slice` awaits and the
                // journal guard is a std Mutex.
                let masked = masked.as_deref().map(|attachments| (&pre_dv, attachments));
                match self.record_clean_slice(&table, &key.physical_table, &key.project_id, date, (key.slice, dropped, masked), &pre_files).await {
                    Ok(Some(_)) => self.persist_certifications(),
                    Ok(None) => {}
                    Err(error) => warn!(%error, project_id = %key.project_id, %date, "certification bookkeeping failed after a clean dedup slice"),
                }
                let mut journal = self.journal();
                journal.complete(&key);
                journal.checkpoint()?;
                crate::observability::maintenance_stats().maintenance_processed_bytes.fetch_add(task.estimated_decoded_bytes, Relaxed);
            }
            Ok((_, false, _)) => {
                retry("dedup_incomplete".to_owned(), std::time::Duration::from_secs(30))?;
            }
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

    /// Execute ONE maintenance unit end-to-end and report where its time went
    /// (backs the `run-unit` CLI). Claims only the requested key; dependency
    /// coverage and admission limits still apply.
    pub async fn run_unit_once(
        &self, source: &str, project_id: &str, date: chrono::NaiveDate, operation: crate::maintenance_coordinator::Operation, slice_hours: i64,
        offset_hours: i64,
    ) -> Result<UnitRunReport> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskKey, TimeSlice};
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
        // Offset from midnight so a day can be TILED; without it the late hours of
        // an oversized day are unreachable.
        let start = day_start.saturating_add(offset_hours.saturating_mul(3_600_000_000));
        let slice = TimeSlice::new(start, start.saturating_add(slice_hours.saturating_mul(3_600_000_000)))?;
        let key = TaskKey { physical_table, source: source.to_owned(), project_id: project_id.to_owned(), slice, operation };
        // Presence permits inspection; the worker still validates generation and
        // coverage. Today's base tier can still grow, so it needs journal proof.
        let base_tier_present = if operation == Operation::DerivedRollup && date < Utc::now().date_naive() {
            let base = base_table().ok_or_else(|| anyhow::anyhow!("{source} declares no base rollup"))?;
            let table = self.resolve_table(project_id, &base).await?;
            let table = table.read().await;
            Self::maintenance_table_partitions(&table, project_id)?.contains(&(project_id.to_owned(), date))
        } else {
            false
        };
        let now = crate::support::now_micros();
        {
            let mut journal = self.journal();
            journal.enqueue_with_base_tier(key.clone(), now, MAX_DECODED_BYTES, unix_ms(now), base_tier_present);
            journal.checkpoint()?;
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
            Operation::Dedup => self.run_coordinator_dedup_selected(TaskSelection::Exact(&key)).await?,
            Operation::BaseRollup | Operation::DerivedRollup => self.run_coordinator_rollup_selected(TaskSelection::Exact(&key)).await?,
            _ => self.run_coordinator_compaction_selected(TaskSelection::Exact(&key)).await?,
        };
        let wall = started.elapsed();
        let after = snapshot();
        let (state, retry_reason) = {
            let journal = self.journal();
            journal.tasks().find(|task| task.key == key).map_or((None, None), |task| (Some(task.state), task.retry_reason.clone()))
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
        self.run_coordinator_rollup_selected(TaskSelection::Next(operation)).await
    }

    async fn run_coordinator_rollup_selected(&self, selection: TaskSelection<'_>) -> Result<bool> {
        let operation = selection.operation();
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

        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(selection) else { return Ok(false) };
        let key = task.key.clone();
        self.log_task_started(&task);
        let lease = crate::maintenance_coordinator::TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        let retry = |reason: String, delay: std::time::Duration| -> Result<bool> { self.retried(&key, reason, delay) };
        let Some(source_schema) = get_schema(&key.source) else { return retry("source_schema_missing".to_owned(), std::time::Duration::from_secs(300)) };
        let Some(spec) = source_schema.rollups.iter().find(|spec| spec.table_name(&key.source) == key.physical_table) else {
            return retry("rollup_spec_missing".to_owned(), std::time::Duration::from_secs(300));
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
            return retry("invalid_slice_timestamp".to_owned(), std::time::Duration::from_secs(3_600));
        };
        if !derived
            && self.buffered_layer().is_some_and(|layer| layer.has_rows_in_range(&key.project_id, &key.source, key.slice.start_micros, key.slice.end_micros))
        {
            return retry("source_not_flushed".to_owned(), buffered_source_retry_delay(key.slice, crate::support::now_micros()));
        }

        // The witness must describe the RAW source partition: the read path verifies
        // it against `route.source`, which is the raw table for EVERY tier.
        let witness_table = match derived {
            false => None,
            true => match self.resolve_table(&key.project_id, &key.source).await {
                Ok(table) => Some(table),
                Err(error) => return retry(format!("resolve_witness_source: {error:#}"), std::time::Duration::from_secs(30)),
            },
        };
        let from_table = match self.resolve_table(&key.project_id, &from).await {
            Ok(table) => table,
            Err(error) => return retry(format!("resolve_input: {error:#}"), std::time::Duration::from_secs(30)),
        };
        let required_columns: HashSet<&str> = ["project_id", "date", "timestamp"]
            .into_iter()
            .chain(source_schema.dedup_keys.iter().map(String::as_str))
            .chain(source_schema.dedup_tiebreak.iter().map(String::as_str))
            .chain(source_schema.tombstone_column.iter().map(String::as_str))
            .chain(spec.dimensions.iter().map(String::as_str))
            .chain(spec.measures.iter().filter_map(|measure| measure.column.as_deref()))
            .chain(
                spec.measures
                    .iter()
                    .filter_map(|measure| measure.filter.as_deref())
                    .flat_map(|filter| filter.split(|character: char| !(character.is_ascii_alphanumeric() || character == '_')))
                    .filter(|token| source_schema.fields.iter().any(|field| field.name == *token)),
            )
            .collect();
        let projected_numerator = u64::try_from(required_columns.len()).unwrap_or(u64::MAX);
        let projected_denominator = u64::try_from(source_schema.fields.len().max(1)).unwrap_or(u64::MAX);
        let mut untagged_inputs = 0u64;
        // Tagged base files the derived selection loop refuses, by reason.
        let (mut skipped_tag_project, mut skipped_tag_range) = (0u64, 0u64);
        let mut skipped_generation = 0u64;
        // Must be read STRICTLY BEFORE the snapshot below: coverage only grows and is
        // inserted after the base commit, so a range collected here is guaranteed
        // present in a later snapshot. The other order publishes short.
        let cells: Vec<BaseCell> = if derived {
            self.rollup_slice_coverage
                .iter()
                .filter(|entry| {
                    let (project, source, table, start, end) = entry.key();
                    *project == key.project_id
                        && *source == key.source
                        && *table == from
                        && key.slice.overlaps(*start, *end)
                        && chrono::DateTime::from_timestamp_micros(*start)
                            .is_some_and(|time| Self::rollup_generation_current(source, table, project, &time.date_naive().to_string(), entry.value()))
                })
                .map(|entry| ((entry.key().3, entry.key().4), entry.value().generation.clone(), entry.value().measures.clone()))
                .collect()
        } else {
            Vec::new()
        };
        let base_generations: HashSet<String> = cells.iter().map(|(_, generation, _)| generation.clone()).collect();
        let base_evidence = crate::rollup::base_measure_evidence(spec, cells.iter().map(|(_, _, measures)| measures.as_ref()));
        let base_covered: Vec<(i64, i64)> = cells.into_iter().map(|(span, _, _)| span).collect();
        let (snapshot, log_store, selected, estimated_bytes, source_rows, partition_identity, whole_file_bytes, content_fp, refused_spans, selected_spans) = {
            let table = from_table.read().await;
            let witness_guard = match &witness_table {
                Some(table) => Some(table.read().await),
                None => None,
            };
            let witness_source: &DeltaTable = witness_guard.as_deref().unwrap_or(&table);
            let snapshot = Arc::new(table.snapshot()?.snapshot().clone());
            let date_string = date.to_string();
            // The witness the read path re-checks this slice against. Must use the SAME
            // call and bound as the read path, over the very snapshot this build aggregates.
            let partition_stats = Self::partition_stats_bounded(witness_source, tiebreak_of(&key.source), &|_, _| i64::MAX).ok().and_then(|mut stats| {
                stats.remove(&(key.project_id.clone(), date_string.clone())).or_else(|| stats.remove(&("default".to_string(), date_string.clone())))
            });
            let source_rows = partition_stats.map(|stats| stats.rows);
            // `(fingerprint, min_ts, max_ts)`: the identity the date-level read path
            // compares. `min_ts` decides whether this slice may claim the day's opening
            // hours; with `max_ts` it bounds where a missing base slice is a real hole.
            let partition_identity = partition_stats.map(|stats| (stats.fingerprint, stats.min_ts, stats.max_ts));
            let partition_paths = dedup_partition_paths(snapshot.log_data().iter().map(|file| file.path().to_string()), &key.project_id, &date_string);
            let mut selected = Vec::new();
            let mut refused_spans: Vec<Option<(i64, i64)>> = Vec::new();
            let mut selected_spans: Vec<(i64, i64)> = Vec::new();
            let mut estimated = 0u64;
            let mut whole_file_bytes = 0u64;
            let mut content_fp = 0u64;
            for file in snapshot.log_data().iter() {
                let path = file.path().to_string();
                if !partition_paths.contains(&path) {
                    continue;
                }
                let add = add_action(&file);
                if !derived && stats_disjoint_from(&add, key.slice) {
                    continue;
                }
                if derived {
                    // OVERLAP, not containment: a base file is tagged with the slice of
                    // the UNIT that wrote it, whose width is unrelated to this one's.
                    // Reading a wider file is safe — the aggregation bounds rows exactly.
                    match Self::slice_tag_range(&add) {
                        Some((start, end)) => {
                            if Self::tag_project(&add) != Some(key.project_id.as_str()) {
                                skipped_tag_project += 1;
                                continue;
                            }
                            if !key.slice.overlaps(start, end) {
                                skipped_tag_range += 1;
                                continue;
                            }
                        }
                        // Without slice tags, prune by timestamp statistics; missing
                        // statistics retain a candidate for generation validation below.
                        _ => {
                            untagged_inputs = untagged_inputs.saturating_add(1);
                            if stats_disjoint_from(&add, key.slice) {
                                continue;
                            }
                        }
                    }
                    // A current coverage range cannot authorize files from an
                    // older materialization generation that overlap that range.
                    if !Self::add_tag(&add, crate::maintenance_coordinator::TAG_GENERATION).is_some_and(|generation| base_generations.contains(generation)) {
                        skipped_generation += 1;
                        // A file with neither tags nor stats is unbounded and can never
                        // be shown reproduced (`None`).
                        refused_spans.push(Self::slice_tag_range(&add).map(|(start, end)| (start, end.saturating_sub(1))).or_else(|| {
                            let (lo, hi) = add_ts_bounds(&add);
                            lo.zip(hi)
                        }));
                        continue;
                    }
                    // The span this ACCEPTED file vouches for, against which a refusal is judged.
                    if let Some(range) = Self::slice_tag_range(&add) {
                        selected_spans.push(range);
                    }
                }
                let (share, projected) = Self::projected_slice_bytes(&add, projected_numerator, projected_denominator, key.slice);
                estimated = estimated.saturating_add(share);
                whole_file_bytes = whole_file_bytes.saturating_add(projected);
                // XOR-folded so file order, which a snapshot does not promise, cannot
                // change the answer.
                content_fp ^= file_content_hash(&path, add.deletion_vector.as_ref());
                selected.push(path);
            }
            (snapshot, table.log_store(), selected, estimated, source_rows, partition_identity, whole_file_bytes, content_fp, refused_spans, selected_spans)
        };
        let input_footprint = crate::maintenance_coordinator::InputFootprint::new(&selected, whole_file_bytes);
        // Record on every claim, not only when this one splits, or a timeout bisect
        // mints footprint-less children.
        if self.journal().record_preflight(&key, Some(input_footprint), estimated_bytes) {
            self.journal().checkpoint()?;
        }
        if skipped_tag_project + skipped_tag_range > 0 {
            let stats = crate::observability::maintenance_stats();
            stats.rollup_base_file_skipped_tag_project.fetch_add(skipped_tag_project, Relaxed);
            stats.rollup_base_file_skipped_tag_range.fetch_add(skipped_tag_range, Relaxed);
        }
        if skipped_generation > 0 {
            crate::observability::maintenance_stats().rollup_base_file_skipped_generation.fetch_add(skipped_generation, Relaxed);
            warn!(source = %key.source, target = %key.physical_table, project_id = %key.project_id, skipped_generation,
                event = "maintenance_rollup_obsolete_inputs", "derived rollup refused obsolete base materializations");
        }
        if untagged_inputs > 0 {
            crate::observability::maintenance_stats().rollup_untagged_inputs.fetch_add(untagged_inputs, std::sync::atomic::Ordering::Relaxed);
            warn!(
                table = %key.physical_table,
                project_id = %key.project_id,
                slice_start = key.slice.start_micros,
                untagged_inputs,
                event = "maintenance_rollup_untagged_input",
                "base files carry no slice tags; checked by timestamp range and materialization generation"
            );
        }
        // Excluding an unverified file is not evidence its rows were empty, so a refusal
        // means: rebuild this base range and retry. A refusal whose span is already
        // reproduced by the CURRENT-generation files selected here is free to exclude,
        // and demanding a rebuild for it livelocks. Judged against `selected_spans` (the
        // live tier's own tags), NOT `base_covered`, which this path distrusts.
        // `ranges_cover`'s `hi` is inclusive, hence the inclusive refused-span ends.
        let unreproduced = crate::rollup::unreproduced_refusals(&refused_spans, &selected_spans);
        if unreproduced > 0 {
            crate::observability::maintenance_stats().rollup_base_refusal_unreproduced.fetch_add(unreproduced, Relaxed);
            let base_spec = source_schema
                .rollups
                .iter()
                .find(|candidate| candidate.table_name(&key.source) == from)
                .ok_or_else(|| anyhow::anyhow!("derived rollup {} has no base spec", key.physical_table))?;
            let base_key = crate::maintenance_coordinator::TaskKey {
                physical_table: from.clone(),
                operation: if base_spec.derive_from.is_some() {
                    crate::maintenance_coordinator::Operation::DerivedRollup
                } else {
                    crate::maintenance_coordinator::Operation::BaseRollup
                },
                ..key.clone()
            };
            let now = crate::support::now_micros();
            {
                let mut journal = self.journal();
                journal.enqueue(base_key, now, MAX_DECODED_BYTES, unix_ms(now));
                journal.checkpoint()?;
            }
            let attempts = self.journal().attempts(&key);
            return retry("base_generation_unverified".to_owned(), std::time::Duration::from_secs(60u64 << attempts.min(5)));
        }
        if skipped_generation > 0 {
            crate::observability::maintenance_stats().rollup_base_refusal_reproduced.fetch_add(skipped_generation, Relaxed);
        }
        // A DERIVED unit may only publish a range its base tier actually covers: its
        // witness is the RAW partition, which agrees forever on a sealed day, so a cell
        // built over a holey base would be published short and trusted permanently.
        // Gaps OUTSIDE the source partition's own span are not holes — base slices begin
        // at the first row, not 00:00. Must stay before `resume_rollup_unit` and
        // `split_time_task`: both would reuse work built over the same incomplete base.
        if derived
            && let Some((hole_start, hole_end)) = crate::rollup::uncovered(key.slice.start_micros, key.slice.end_micros, base_covered)
                .into_iter()
                .find(|(start, end)| partition_identity.is_none_or(|(_, min_ts, max_ts)| *end > min_ts && *start <= max_ts))
        {
            crate::observability::maintenance_stats().rollup_derived_base_incomplete.fetch_add(1, Relaxed);
            warn!(
                table = %key.physical_table, project_id = %key.project_id, base = %from,
                slice_start = key.slice.start_micros, slice_end = key.slice.end_micros, hole_start, hole_end,
                event = "maintenance_rollup_base_tier_incomplete",
                "the base tier does not cover this derived slice; retrying rather than publishing a short cell"
            );
            // Leaves the slice whole: splitting would mint children over the same
            // incomplete base. Backoff caps at ~32 min.
            let attempts = self.journal().attempts(&key);
            return retry("base_tier_incomplete".to_owned(), std::time::Duration::from_secs(60u64 << attempts.min(5)));
        }
        // Everything above is metadata; everything below is the expensive scan and
        // aggregate. Committing a previous process's staged output here skips both.
        if self.resume_rollup_unit(&key, source_rows.and_then(|rows| u64::try_from(rows).ok())).await? {
            return self.completed(&key);
        }
        // A rebuild whose INPUT is unchanged reproduces its own output, and the queue
        // re-mints such units routinely. Three conditions, all necessary:
        //   * `content_fp` — the input file set INCLUDING deletion vectors, so a DV'd
        //     file (path-identical, row-different) is not taken for unchanged. `None`
        //     (coverage recovered from tags at boot) declines the skip.
        //   * the generation is still current, so a spec or measure change rebuilds.
        //   * the OUTPUT still stands (`tier_still_holds_slice`) — an input-only proof is
        //     one-sided, since coverage is in-memory and outlives the files it describes.
        // DERIVED units are excluded: their correctness also depends on `base_covered`.
        // The DashMap guard must be dropped BEFORE the tier check awaits, or this
        // deadlocks against the one caller that also writes this map.
        let reproduces = (!derived && self.config.maintenance.timefusion_rollup_noop_skip_enabled)
            .then(|| {
                let coverage = self.rollup_slice_coverage.get(&(
                    key.project_id.clone(),
                    key.source.clone(),
                    key.physical_table.clone(),
                    key.slice.start_micros,
                    key.slice.end_micros,
                ))?;
                let current = Self::rollup_generation_current(&key.source, &key.physical_table, &key.project_id, &date.to_string(), coverage.value());
                (current && coverage.content_fp == Some(content_fp) && coverage.output_files > 0).then(|| (coverage.generation.clone(), coverage.output_files))
            })
            .flatten();
        if let Some((generation, output_files)) = reproduces
            && self.tier_still_holds_slice(&key, &generation, output_files).await
        {
            crate::observability::maintenance_stats().rollup_noop_rebuild_skipped.fetch_add(1, Relaxed);
            info!(
                table = %key.physical_table, project_id = %key.project_id,
                slice_start = key.slice.start_micros, slice_end = key.slice.end_micros, content_fp,
                event = "maintenance_rollup_noop_skipped",
                "input unchanged since the live coverage was published; completing without rebuilding"
            );
            return self.completed(&key);
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
            // Transient, never "too big to admit": the shard count above was chosen so
            // `per_shard_bytes <= MAX_DECODED_BYTES`.
            return retry("admission_busy".to_owned(), self.admission_backoff_for(&key));
        };
        let mut fingerprint_items = selected.clone();
        fingerprint_items.sort_unstable();
        let mut fingerprint = fnv::FnvHasher::default();
        fingerprint_items.hash(&mut fingerprint);
        let source_fp = fingerprint.finish();

        let unit_started = std::time::Instant::now();
        let ctx = self.bounded_rollup_maintenance_context()?;
        let provider = Self::narrow_provider(log_store, snapshot, selected, None, None).await.map_err(|error| anyhow::anyhow!("slice provider: {error}"))?;
        const RAW: &str = "__maintenance_slice_raw";
        // What the PHYSICAL table has, which is not what the spec declares.
        let present_columns: std::collections::HashSet<String> = provider.schema().fields().iter().map(|field| field.name().clone()).collect();
        ctx.register_table(RAW, provider)?;
        // The schema of whatever is registered as RAW — NOT `source_schema` for a derived
        // tier, which reads the merge-on-read BASE TIER. Without dedup the derived
        // aggregate SUMs every superseded version of a rebuilt bucket.
        let input_schema = if derived { get_schema(&from).unwrap_or(source_schema) } else { source_schema };
        let tier_dedup = derived.then(|| crate::rollup::rollup_tier_dedup(input_schema)).flatten();
        let target_schema = get_schema(&key.physical_table).ok_or_else(|| anyhow::anyhow!("rollup target schema missing"))?;
        // What these files can be READ for, which is not what the spec declares. Taken
        // against the DECLARED target schema, and before the generation, which is
        // computed over exactly this set.
        let materialized = crate::rollup::materialized_measures(spec, derived, &present_columns, &target_schema.schema_ref(), base_evidence.as_ref());
        let generation = crate::rollup::generation_id(spec, &key.source, &key.project_id, &date.to_string(), source_fp, Some(&materialized));
        let default_shard_keys = || ["project_id".to_owned(), "timestamp".to_owned()].into_iter().chain(spec.dimensions.iter().cloned()).collect::<Vec<_>>();
        let shard_keys = if derived || source_schema.dedup_keys.is_empty() { default_shard_keys() } else { source_schema.dedup_keys.clone() };
        let shard_key_sql = shard_keys.iter().map(|field| format!("CAST({} AS VARCHAR)", crate::rollup::quoted(field))).collect::<Vec<_>>().join(", ");
        // An even, stable spread is all this needs; a cryptographic digest is not free.
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
            // Annotated because an unexplained abandonment is BISECTED, and a missing
            // field then fails deterministically in every child.
            let frame = ctx.sql(&input_sql).await.map_err(|error| lease.note_failure(error))?;
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
            let shard_aggregate = collect_watched(&ctx, &aggregate_sql).await.map_err(|error| lease.note_failure(error))?;
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
            aggregate = collect_watched(&ctx, &merge_sql).await?;
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
        // Everything above is read + aggregate; everything below is write.
        let scan_ms = unit_started.elapsed().as_millis() as u64;
        let stage_started = std::time::Instant::now();

        let target_ref = self.get_or_create_table(&key.project_id, &key.physical_table).await?;
        // The writer conforms to the table as it exists, so widen BEFORE it takes its
        // schema or newly declared measures are dropped. Additive, nullable, metadata-only.
        crate::database::evolve_table_columns(&target_ref, target_schema.schema_ref().fields()).await?;
        let staging_table = target_ref.read().await.clone();
        let stage_store = staging_table.log_store().object_store(None);
        let (batches, sorted) = self.sort_flush_group(target_schema, batches, UnsortedFallback::Forbid).await?;
        let mut writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)?.with_writer_properties(self.create_writer_properties(
            target_schema,
            self.config.parquet.timefusion_zstd_compression_level,
            sorted,
        ));
        let arrow_schema = writer.arrow_schema();
        for batch in batches {
            writer.write(deltalake::kernel::schema::cast_record_batch(&batch?, arrow_schema.clone(), true, true)?).await?;
        }
        let mut adds = writer.flush().await?;
        let stage_ms = stage_started.elapsed().as_millis() as u64;
        let commit_started = std::time::Instant::now();
        for add in &mut adds {
            add.data_change = true;
            let mut tags = add.tags.take().unwrap_or_default();
            for (name, value) in [
                (crate::maintenance_coordinator::TAG_SOURCE, key.source.clone()),
                (crate::maintenance_coordinator::TAG_PROJECT, key.project_id.clone()),
                (crate::maintenance_coordinator::TAG_SLICE_START, key.slice.start_micros.to_string()),
                (crate::maintenance_coordinator::TAG_SLICE_END, key.slice.end_micros.to_string()),
                (crate::maintenance_coordinator::TAG_SOURCE_FINGERPRINT, source_fp.to_string()),
                // Persisted so the no-op skip survives a restart.
                (crate::maintenance_coordinator::TAG_CONTENT_FINGERPRINT, content_fp.to_string()),
                (crate::maintenance_coordinator::TAG_GENERATION, generation.clone()),
                // Absent means the read path must refuse this slice, so write the `-1`
                // sentinel rather than omitting the tag when the source reports no count.
                (crate::maintenance_coordinator::TAG_SOURCE_ROWS, source_rows.unwrap_or(-1).to_string()),
                (crate::maintenance_coordinator::TAG_MEASURES, materialized.join(",")),
            ] {
                tags.insert(name.to_owned(), Some(value));
            }
            add.tags = Some(tags);
        }

        let live_adds = staging_table.snapshot()?.log_data().iter().map(|file| add_action(&file)).collect::<Vec<_>>();
        // Containment, not exact equality: slice WIDTH is not stable for a given range,
        // and matching only the identical slice leaves a day-wide file and an hour-wide
        // file inside it both live, which double-counts.
        let date_string = date.to_string();
        let in_partition = |add: &deltalake::kernel::Add| {
            Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default")
                .is_some_and(|(project, date)| project == key.project_id && date == date_string)
        };
        // Every tagged range LIVE here after this commit, this slice included. Ranges
        // CONTAINED in this slice are omitted: those files are the ones being replaced.
        let covered = std::iter::once((key.slice.start_micros, key.slice.end_micros))
            .chain(live_adds.iter().filter(|add| in_partition(add)).filter_map(Self::slice_tag_range))
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
                    slice: Self::slice_tag_range(add),
                    project: Self::tag_project(add),
                    partition: partition.as_ref().map(|(project, date)| (project.as_str(), date.as_str())),
                    stats: add.stats.as_deref().and_then(crate::rollup::stats_time_range),
                };
                crate::rollup::slice_retires(&file, &publish)
            })
            .cloned()
            .collect::<Vec<_>>();
        // Tier-wide, not per-partition; published here because this is the only place
        // that sees a tier's live set without an extra listing.
        let no_identity = |add: &deltalake::kernel::Add| Self::slice_tag_range(add).is_none();
        self.publish_tier_untagged(&key.physical_table, live_adds.iter().filter(|add| no_identity(add)).count() as u64);
        // Applied only AFTER the commit: a unit killed in between retires nothing, and
        // `clear_untagged_cell` would drop the hole boost from an unrepaired partition.
        let retiring = replaced.iter().filter(|add| no_identity(add)).count() as u64;
        let leaves_partition_clean = !live_adds.iter().filter(|add| in_partition(add) && !replaced.iter().any(|gone| gone.path == add.path)).any(no_identity);
        // A slice covered by a STRICTLY WIDER live file must not publish: the replace-set
        // only removes files CONTAINED in this slice, so both would stay live and be
        // summed. Applies to BOTH tiers.
        let covered_by_wider = live_adds.iter().find_map(|add| {
            let (start, end) = Self::slice_tag_range(add)?;
            (Self::tag_project(add) == Some(key.project_id.as_str())
                && (start, end) != (key.slice.start_micros, key.slice.end_micros)
                && start <= key.slice.start_micros
                && end >= key.slice.end_micros)
                .then_some((start, end))
        });
        if let Some((covering_start, covering_end)) = covered_by_wider {
            // ESCALATE rather than silently complete: dropping the unit leaves that hour
            // STALE in the coarse tier. Reopening the covering slice terminates — the
            // wider unit publishes at its own width and never re-enters this branch.
            crate::observability::maintenance_stats().rollup_skipped_covered_by_wider.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut journal = self.journal();
            if let Ok(covering) = crate::maintenance_coordinator::TimeSlice::new(covering_start, covering_end) {
                let now = crate::support::now_micros();
                journal.enqueue(crate::maintenance_coordinator::TaskKey { slice: covering, ..key.clone() }, now, MAX_DECODED_BYTES, unix_ms(now));
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
        // Record the intent HERE — after the tags are stamped and the replace-set is
        // decided, before anything commits — so the next boot can finish this unit
        // instead of repeating the scan. The publication must travel with it: a commit
        // without one is invisible to coverage and would simply be rebuilt.
        let resume_wave = uuid::Uuid::new_v4().to_string();
        let publication = crate::maintenance_coordinator::Publication {
            source_fingerprint: source_fp,
            generation: generation.clone(),
            rows,
            source_rows: source_rows.and_then(|rows| u64::try_from(rows).ok()),
        };
        let mut actions = replaced.iter().map(|add| Action::Remove(remove_for_add(add, true))).collect::<Vec<_>>();
        actions.extend(adds.iter().cloned().map(Action::Add));
        // The staged parquet as commit actions — only the abandon paths need this shape.
        let staged_actions = || adds.iter().cloned().map(Action::Add).collect::<Vec<_>>();

        // Counted before the commit consumes `actions`.
        let (action_count, output_files) = (actions.len() as u64, adds.len() as u64);

        if self.journal().state(&key) != Some(TaskState::Running) {
            Self::cleanup_orphaned_parquet(&stage_store, &staged_actions()).await;
            return Ok(true);
        }
        self.record_staged_intent(StagedIntent {
            wave_id: resume_wave.clone(),
            table_name: key.physical_table.clone(),
            project_id: key.project_id.clone(),
            recorded_at: crate::support::now_secs(),
            paths: adds.iter().map(|add| add.path.clone()).collect(),
            target_paths: target_paths.clone(),
            adds: adds.clone(),
            rollup: Some(crate::database::RollupResume {
                key: key.clone(),
                publication: publication.clone(),
                source_rows: publication.source_rows,
                date: date_string.clone(),
            }),
            instance: None,
        });
        if !actions.is_empty() {
            let commit_lock = self.commit_lock(&key.project_id, &key.physical_table).await;
            let guard = commit_lock.lock().await;
            refresh_table_snapshot(&target_ref, self.config.maintenance.timefusion_incremental_snapshot).await?;
            let mut table = target_ref.read().await.clone();
            let live = table.snapshot()?.log_data().iter().map(|file| file.path().to_string()).collect::<HashSet<_>>();
            if !target_paths.iter().all(|path| live.contains(path)) {
                drop(guard);
                Self::cleanup_orphaned_parquet(&stage_store, &staged_actions()).await;
                // The staged objects are gone; leaving the intent would have the next
                // boot commit Adds whose parquet was deleted.
                self.clear_staged_intent(&[resume_wave.as_str()]);
                return retry("slice_occ_stale".to_owned(), std::time::Duration::from_secs(1));
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

        let _journal_guard = crate::support::lock(&self.rollup_journal_lock);
        let mut journal = self.journal();
        if journal.state(&key) == Some(TaskState::Running) {
            let slice_coverage = RollupCoverage {
                source_fp,
                source_epoch: None,
                generation: generation.clone(),
                source_rows: source_rows.and_then(|rows| u64::try_from(rows).ok()),
                covered_through: key.slice.end_micros,
                measures: Some(materialized.iter().cloned().collect()),
                content_fp: Some(content_fp),
                output_files: u32::try_from(output_files).unwrap_or(u32::MAX),
            };
            self.rollup_slice_coverage.insert(
                (key.project_id.clone(), key.source.clone(), key.physical_table.clone(), key.slice.start_micros, key.slice.end_micros),
                slice_coverage.clone(),
            );
            // DATE-level coverage, the second routing route: it compares the
            // whole-partition fingerprint instead of the per-slice witness. Gated on the
            // partition's EARLIEST ROW — the read path serves `[day_start, covered_through)`
            // from this entry, so a slice beginning mid-day would claim a morning it never
            // aggregated. A midnight test would never fire: units start at the first row.
            if let Some((partition_fp, partition_min_ts, _)) = partition_identity
                && partition_min_ts >= key.slice.start_micros
            {
                self.rollup_coverage.insert(
                    (key.project_id.clone(), key.source.clone(), key.physical_table.clone(), date.to_string()),
                    RollupCoverage {
                        source_fp: partition_fp,
                        source_epoch: Some(
                            self.rollup_source_epochs.get(&(key.project_id.clone(), key.source.clone(), date.to_string())).map_or(0, |epoch| *epoch.value()),
                        ),
                        content_fp: None,
                        output_files: 0,
                        ..slice_coverage
                    },
                );
            }
            journal.publish(&key, publication.clone());
            // A BASE slice just changed under the derived cells built over it, whose
            // witness (the RAW partition) agrees forever on a sealed day. The COVERAGE
            // must be dropped WITH the task and in this order, so that a cell under
            // rebuild falls back to the raw fringe.
            if !derived {
                for child_spec in
                    source_schema.rollups.iter().filter(|candidate| spec.name.is_some() && candidate.derive_from.as_deref() == spec.name.as_deref())
                {
                    let child = child_spec.table_name(&key.source);
                    let reopened = journal.reopen_derived_over(&key.project_id, &child, key.slice.start_micros, key.slice.end_micros);
                    if reopened > 0 {
                        self.rollup_slice_coverage.retain(|(project, _, table, start, end), _| {
                            project != &key.project_id || table != &child || *start >= key.slice.end_micros || *end <= key.slice.start_micros
                        });
                        info!(
                            table = %key.physical_table, project_id = %key.project_id, child = %child, reopened,
                            slice_start = key.slice.start_micros, slice_end = key.slice.end_micros,
                            event = "maintenance_rollup_derived_reopened_after_base_republish"
                        );
                    }
                }
            }
            // An EMPTY publication over a non-empty base freezes the tier above: coverage
            // records it as COVERED, so no hole is ever seen. Reported only — the obvious
            // guard (retry rather than complete) can refuse forever. DERIVED only: the
            // base tier's comparator is DAY-keyed and reads non-empty for an empty hour.
            if derived && rows == 0 && journal.published_rows_overlapping(&key.project_id, &from, key.slice.start_micros, key.slice.end_micros) > 0 {
                crate::observability::maintenance_stats().rollup_published_empty_over_full_base.fetch_add(1, Relaxed);
                warn!(
                    table = %key.physical_table, project_id = %key.project_id, base = %from,
                    slice_start = key.slice.start_micros, slice_end = key.slice.end_micros,
                    event = "maintenance_rollup_published_empty_over_full_base",
                    "published zero rows although the base tier holds rows in this slice"
                );
            }
            journal.checkpoint()?;
            // Cleared AFTER the checkpoint, never before: an intent that outlives its
            // unit is harmlessly retried, while one cleared early and lost to a crash
            // costs the whole scan again.
            self.clear_staged_intent(&[resume_wave.as_str()]);
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
            let commit_ms = commit_started.elapsed().as_millis() as u64;
            let unit_ms = unit_started.elapsed().as_millis() as u64;
            stats.rollup_scan_duration_ms.fetch_add(scan_ms, Relaxed);
            stats.rollup_staging_duration_ms.fetch_add(stage_ms, Relaxed);
            stats.rollup_commit_duration_ms.fetch_add(commit_ms, Relaxed);
            stats.rollup_end_to_end_duration_ms.fetch_add(unit_ms, Relaxed);
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
            // One coordinator unit is one (project, slice) publication.
            stats.rollup_staged_projects.fetch_add(1, Relaxed);
            (if derived { &stats.rollup_rebuilds_incremental } else { &stats.rollup_rebuilds_full }).fetch_add(1, Relaxed);
        }
        Ok(true)
    }

    /// `retry_task` as a coordinator unit's terminal step: push the next attempt
    /// out and report the unit as claimed.
    fn retried(&self, key: &crate::maintenance_coordinator::TaskKey, reason: String, delay: std::time::Duration) -> Result<bool> {
        self.retry_task(key, reason, delay).map(|()| true)
    }

    /// Complete a coordinator task and checkpoint, reporting it as claimed.
    fn completed(&self, key: &crate::maintenance_coordinator::TaskKey) -> Result<bool> {
        let mut journal = self.journal();
        journal.complete(key);
        journal.checkpoint().map(|()| true)
    }

    /// Whether the rollup tier still holds exactly the files a cell published.
    ///
    /// Metadata only; an unresolvable tier declines rather than erroring.
    async fn tier_still_holds_slice(&self, key: &crate::maintenance_coordinator::TaskKey, generation: &str, output_files: u32) -> bool {
        let Ok(target) = self.resolve_table(&key.project_id, &key.physical_table).await else { return false };
        let table = target.read().await;
        let Ok(snapshot) = table.snapshot() else { return false };
        let live = snapshot
            .log_data()
            .iter()
            .map(|file| add_action(&file))
            .filter(|add| {
                Self::tag_project(add) == Some(key.project_id.as_str())
                    && Self::add_tag(add, crate::maintenance_coordinator::TAG_GENERATION) == Some(generation)
                    && Self::slice_tag_range(add) == Some((key.slice.start_micros, key.slice.end_micros))
            })
            .count();
        u32::try_from(live).is_ok_and(|live| live == output_files)
    }

    /// The `RuntimeEnv` a coordinator compaction unit stages under. Repair gets its own
    /// pool: a whole-file rewrite needs one decoded row group as a floor, which a fair
    /// share of the shared pool does not guarantee.
    pub(crate) fn coordinator_compaction_runtime_env(
        &self, operation: crate::maintenance_coordinator::Operation,
    ) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        match operation {
            crate::maintenance_coordinator::Operation::Repair => self.repair_runtime_env(),
            _ => self.coordinator_runtime_env(),
        }
    }

    async fn coordinator_compaction_files(&self, table_ref: &Arc<RwLock<DeltaTable>>, key: &crate::maintenance_coordinator::TaskKey) -> Result<Vec<String>> {
        use crate::maintenance_coordinator::Operation;
        let date = chrono::DateTime::from_timestamp_micros(key.slice.start_micros)
            .map(|time| time.date_naive().to_string())
            .ok_or_else(|| anyhow::anyhow!("invalid compaction slice timestamp"))?;
        let date_marker = format!("date={date}/");
        // Per-stage survivor counts: an empty return retires the unit, so the stage
        // breakdown is what distinguishes "out of policy" from "nothing to do".
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
                    let add = TailAdd::from_stats(
                        path.to_string(),
                        file.size(),
                        is_sorted_run(&file.tags()),
                        file.deletion_vector_descriptor().is_some(),
                        file.stats().as_deref(),
                    );
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
        // Bounded by what ONE SORT CAN DECODE, not only by the desired output size:
        // packing targets are COMPRESSED while sort budgets are DECODED, and a bin that
        // cannot fit its sort stalls indefinitely. See `coordinator_bin_compressed_cap_bytes`.
        let declared_target = match key.operation {
            Operation::HotPacking => COORDINATOR_HOT_TARGET_BYTES,
            Operation::SealedConsolidation => COORDINATOR_SEALED_TARGET_BYTES,
            _ => return Ok(Vec::new()),
        };
        // The pair floor is taken from THIS cell's own two smallest files, not
        // from a constant, because that is exactly what `packer_admits_pair`
        // (the planner/packer agreement test) measures when it decides to queue
        // the cell. Deriving it any other way lets the planner enqueue work this
        // packer must refuse — which is the wedge, not a hypothetical.
        let mut two_smallest: Vec<i64> = candidates.iter().map(|add| add.size).collect();
        two_smallest.sort_unstable();
        let smallest_pair = two_smallest.iter().take(2).sum::<i64>();
        let target = declared_target.min(crate::config::coordinator_packing_cap_bytes(smallest_pair));
        let unsorted_candidates = candidates.iter().filter(|add| !add.is_sorted_run).count();
        let under_target_candidates = candidates.iter().filter(|add| add.size < target).count();
        // A pair that does not fit means planner and packer see different candidate sets
        // (the planner does not apply the packer's range filter).
        let two_smallest = candidates.iter().map(|add| add.size).filter(|size| *size < target).k_smallest(2).collect_tuple();
        let smallest_pair = two_smallest.map_or(-1, |(smaller, larger): (i64, i64)| smaller.saturating_add(larger));
        // Captured before the move: the packer takes `candidates` by value.
        let ranges_by_path: HashMap<String, (i64, i64)> = candidates.iter().filter_map(|add| add.event_range.map(|range| (add.path.clone(), range))).collect();
        let selected = select_coordinator_compaction_candidates(candidates, target);
        // Span of the output: merging unions the inputs' ranges and dedup reads a file
        // once per 10-minute bin it touches. Reported only — deliberately not enforced.
        if selected.len() >= 2 {
            let ranges: Vec<(i64, i64)> = selected.iter().filter_map(|path| ranges_by_path.get(path).copied()).collect();
            if let Some((lo, hi)) = ranges.iter().copied().reduce(|(lo, hi), (start, end)| (lo.min(start), hi.max(end))) {
                let bins = (hi - lo) / (10 * 60 * 1_000_000) + 1;
                info!(
                    operation = ?key.operation,
                    project_id = %key.project_id,
                    table = %key.physical_table,
                    date = %date,
                    files = selected.len(),
                    span_secs = (hi - lo) / 1_000_000,
                    dedup_bins_spanned = bins,
                    with_ranges = ranges.len(),
                    event = "compaction_unit_span",
                    "the time span a compaction unit is about to union into one output"
                );
            }
        }
        // Only when the unit will do nothing: one file is a 1:1 rewrite and retires none.
        if selected.len() < 2 {
            info!(
                operation = ?key.operation,
                project_id = %key.project_id,
                table = %key.physical_table,
                date = %date,
                snapshot_files = seen,
                after_date_filter = after_date,
                after_project_filter = after_project,
                after_range_filter = after_range,
                unsorted_candidates,
                under_target = under_target_candidates,
                selected = selected.len(),
                target,
                smallest_pair_bytes = smallest_pair,
                smallest_pair_fits = smallest_pair >= 0 && smallest_pair <= target,
                event = "compaction_unit_selected_nothing",
                "a compaction unit selected fewer than two files and will retire none"
            );
        }
        Ok(selected)
    }

    pub(crate) async fn run_coordinator_compaction_once(&self, operation: crate::maintenance_coordinator::Operation) -> Result<bool> {
        self.run_coordinator_compaction_selected(TaskSelection::Next(operation)).await
    }

    /// Retire a compaction unit, or requeue it immediately when its partition still
    /// holds debt — a bin is never by construction the whole cell.
    async fn settle_compaction_unit(&self, table_ref: &Arc<RwLock<DeltaTable>>, key: &crate::maintenance_coordinator::TaskKey) -> Result<()> {
        let remaining = !self.coordinator_compaction_files(table_ref, key).await?.is_empty();
        let mut journal = self.journal();
        if remaining {
            journal.retry(key, "compaction_debt_remaining".to_owned(), crate::support::now_micros());
        } else {
            journal.complete(key);
        }
        journal.checkpoint()
    }

    async fn run_coordinator_compaction_selected(&self, selection: TaskSelection<'_>) -> Result<bool> {
        let operation = selection.operation();
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, Resources, TaskLease, TaskState};
        // Take the rewrite permit BEFORE the claim, never inside `stage_hot_bin`:
        // blocking on it after `claim_next` has stamped the unit Running spends the
        // unit's whole deadline in a queue. Repair is exempt.
        let light_permit = match operation {
            Operation::HotPacking | Operation::SealedConsolidation => {
                let stats = crate::observability::maintenance_stats();
                self.rebalance_repair_holdback(stats);
                stats.light_rewrite_permits_available.store(self.light_rewrite_sem.available_permits() as u64, std::sync::atomic::Ordering::Relaxed);
                match Arc::clone(&self.light_rewrite_sem).try_acquire_owned() {
                    Ok(permit) => {
                        stats.compaction_permits_acquired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        Some(permit)
                    }
                    Err(_) => {
                        stats.compaction_permits_unavailable.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return Ok(false);
                    }
                }
            }
            _ => None,
        };
        // PHASE WATCHDOG over the window in which a light permit is held but no
        // sort has begun. Prod 2026-09-13, on a 54-minute-old process carrying
        // every one of tonight's permit and packer fixes:
        //
        //     light_optimize_bins_committed:   150 -> 150       FROZEN
        //     compaction_permits_acquired:     370 -> 370       FROZEN
        //     compaction_permits_unavailable:  4,026 -> 11,136  climbing
        //     light_rewrite_permits_available: 0                both held
        //     HotPacking + SealedConsolidation claims in 6 min: 0
        //     wave_bin_staging_started in 20 min:               0
        //
        // Both permits were held by units that never released them, and because
        // the permit is taken BEFORE the claim the hygiene lanes then stop
        // claiming at all. The lane does not drain slowly — it WEDGES, and prod's
        // constant redeploys keep clearing and re-forming it, which reads as
        // slowness.
        //
        // The reporter must be a SEPARATE TASK, not a check on the way past each
        // checkpoint. A unit that hangs never reaches the next checkpoint, so a
        // completion-based clock stays silent on precisely the failure it is
        // built to name — the same trap that kept #268's lifetime cap from ever
        // firing, since `tokio::time::timeout` cannot preempt a future that never
        // reaches an await point. Two permits bound this to two watchdogs.
        const PERMIT_PHASES: [&str; 5] = ["claim", "admission", "resolve_table", "compaction_files", "staging"];
        struct AbortOnDrop(tokio::task::JoinHandle<()>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let phase = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let note = |next: usize| phase.store(next, std::sync::atomic::Ordering::Relaxed);
        let _watchdog = AbortOnDrop(tokio::spawn({
            let phase = Arc::clone(&phase);
            async move {
                // Back off doubling. A bin at 1.0x the sort budget legitimately
                // stages for ~1,710 s, so a flat 60 s interval would put ~28 lines
                // per healthy unit into the log of a memory-tight box. Doubling
                // keeps the first report early, where a wedge is still news, and
                // costs a logarithmic number of lines for the long legitimate ones.
                let (mut held_secs, mut wait) = (0u64, 60u64);
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                    held_secs += wait;
                    wait = (wait * 2).min(900);
                    warn!(
                        phase = PERMIT_PHASES[phase.load(std::sync::atomic::Ordering::Relaxed).min(PERMIT_PHASES.len() - 1)],
                        held_secs,
                        event = "compaction_permit_held_without_staging",
                        "a light permit has been held this long without starting a sort"
                    );
                }
            }
        }));
        let Some((task, _quarantine_slot)) = self.claim_coordinator_task(selection) else { return Ok(false) };
        note(1);
        let key = task.key.clone();
        self.log_task_started(&task);
        let _lease = TaskLease::new(Arc::clone(&self.maintenance_tasks), key.clone());
        let retry = |reason: String, seconds: u64| -> Result<bool> { self.retried(&key, reason, std::time::Duration::from_secs(seconds)) };
        // The request must be the unit's own size, or the occupancy-scaled ceiling
        // refuses everything on a busy pool.
        let request = Resources { cpu: 1, decoded_bytes: task.estimated_decoded_bytes.clamp(1, MAX_DECODED_BYTES), object_reads: 1, object_writes: 1 };
        let Some(_permit) = self.maintenance_admission.try_acquire(request) else {
            return self.retried(&key, "admission_busy".to_owned(), admission_backoff(task.attempts));
        };
        note(2);
        let table_ref = match self.resolve_table(&key.project_id, &key.source).await {
            Ok(table) => table,
            Err(error) => return retry(format!("resolve_compaction_source: {error:#}"), 30),
        };
        note(3);
        let files = self.coordinator_compaction_files(&table_ref, &key).await?;
        note(4);
        if files.is_empty() {
            return self.completed(&key);
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
            return self.settle_compaction_unit(&table_ref, &key).await.map(|()| true);
        }
        let Some(schema) = get_schema(&key.source) else { return retry("compaction_schema_missing".to_owned(), 300) };
        let pass = if operation == crate::maintenance_coordinator::Operation::Repair { TailPass::Repair } else { TailPass::Pack };
        // A staged-but-uncommitted rewrite from a previous process is COMMITTED
        // here rather than redone; otherwise the boot-time reconcile deletes the
        // staged parquet and a whole rewrite is thrown away.
        let date_marker =
            chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| format!("date={}/", time.date_naive())).unwrap_or_default();
        if let Some(bin) = self.resumable_staged_bin(&table_ref, &key.source, &key.project_id, &files).await {
            let result = self.commit_wave(&table_ref, &key.source, std::slice::from_ref(&date_marker), false, vec![bin], 0).await;
            let landed = result.failed.is_empty() && !result.landed.is_empty();
            info!(table_name = %key.source, project_id = %key.project_id, landed, event = "resumed_bin_committed_early");
            if landed {
                return self.settle_compaction_unit(&table_ref, &key).await.map(|()| true);
            }
            // The resume lost its race (inputs no longer live); stage normally.
        }
        let runtime = self.coordinator_compaction_runtime_env(operation);
        let outcome = self
            .stage_hot_bin(&table_ref, &key.source, schema, &key.project_id, files, HotStageOptions { pass, runtime_env: Some(runtime), light_permit })
            .await;
        let completed = match outcome {
            Ok(BinOutcome::Staged(unit)) => {
                let result = self.commit_wave(&table_ref, &key.source, std::slice::from_ref(&date_marker), false, vec![unit], 0).await;
                result.failed.is_empty() && !result.landed.is_empty()
            }
            Ok(BinOutcome::Converged) => true,
            Ok(BinOutcome::Retry) => false,
            // The repair byte budget is held by another long-running rewrite; requeue
            // on that clock rather than spinning on re-claims.
            Ok(BinOutcome::BudgetBusy) => {
                let mut journal = self.journal();
                journal.retry(&key, "repair_budget_busy".to_owned(), crate::support::now_micros().saturating_add(300 * 1_000_000));
                journal.checkpoint()?;
                return Ok(true);
            }
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
            match (completed, remaining) {
                (true, true) => journal.retry(&key, "compaction_debt_remaining".to_owned(), crate::support::now_micros()),
                (true, false) => journal.complete(&key),
                (false, _) => journal.retry(&key, "compaction_incomplete".to_owned(), crate::support::now_micros().saturating_add(30_000_000)),
            };
            journal.checkpoint()?;
        }
        Ok(true)
    }

    /// Lend the repair lane's reserved light permits out while repair is idle,
    /// and take them back the moment it has work.
    ///
    /// `light_optimize_k` subtracts `repair_pool_holdback_slices` so a repair
    /// rewrite always has budget. Prod 2026-09-13: `pending_repair` was ZERO for
    /// the whole day while that reservation pinned the hygiene lane at K=2 — and
    /// the sealed backlog is precisely what the box is behind on. At 32 cores the
    /// coordinator share holds 4 slices and the holdback was taking 2.
    ///
    /// SAFE because the holdback is a RESERVATION, not a memory ceiling: `slices`
    /// is already `coordinator_share / COORDINATOR_PER_SORT_BUDGET`, so lending
    /// these uses exactly the share the budget tree computed and no more. The
    /// three "Resources exhausted" incidents this pool has seen came from raising
    /// the per-sort budget or over-committing it; this does neither.
    ///
    /// The RETURN is the load-bearing half. `forget_permits` cannot revoke one
    /// already held, so a repair unit arriving mid-flight waits for the current
    /// hygiene sort rather than running beside it — bounded, and the direction
    /// that cannot over-commit.
    fn rebalance_repair_holdback(&self, stats: &crate::observability::MaintenanceStats) {
        use std::sync::atomic::Ordering::Relaxed;
        let lendable = self.config.derived.repair_holdback_permits();
        if lendable == 0 {
            return;
        }
        let repair_idle = stats.pending_repair.load(Relaxed) == 0;
        let lent = self.repair_holdback_lent.load(Relaxed);
        if repair_idle && lent == 0 {
            self.light_rewrite_sem.add_permits(lendable);
            self.repair_holdback_lent.store(lendable as u64, Relaxed);
            stats.light_rewrite_permits_total.fetch_add(lendable as u64, Relaxed);
            stats.repair_holdback_lends.fetch_add(1, Relaxed);
            info!(lendable, event = "repair_holdback_lent", "repair has no pending work; lending its reserved light permits to the hygiene lane");
        } else if !repair_idle && lent > 0 {
            let taken = self.light_rewrite_sem.forget_permits(lent as usize);
            self.repair_holdback_lent.store((lent as usize).saturating_sub(taken) as u64, Relaxed);
            stats.light_rewrite_permits_total.store(stats.light_rewrite_permits_total.load(Relaxed).saturating_sub(taken as u64), Relaxed);
            stats.repair_holdback_returns.fetch_add(1, Relaxed);
            info!(taken, still_lent = lent as usize - taken, event = "repair_holdback_returned", "repair has work; reclaiming its reserved light permits");
        }
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
            // Historical debt nothing else queues; without it the rollup horizon
            // never grows past the live frontier.
            let backfilled = self.plan_rollup_backfill().await?;
            if backfilled != 0 {
                info!(backfilled, event = "maintenance_rollup_backfill_planned");
            }
            // Collapse a sealed day's ten-minute units, which nothing else retires.
            // `ceilings` is what each partition can actually decode to, so the fit
            // test does not trust estimates frozen at enqueue time.
            let mut ceilings: HashMap<(String, String), u64> = HashMap::new();
            for source in crate::schema::registry().list_tables() {
                let Ok(table_ref) = self.resolve_table("default", &source).await else { continue };
                let table = table_ref.read().await;
                let Ok(stats) = Self::partition_stats_bounded(&table, tiebreak_of(&source), &|_, _| i64::MAX) else { continue };
                ceilings.extend(stats.into_iter().map(|(partition, stat)| (partition, stat.bytes)));
            }
            let report = {
                let mut journal = self.journal();
                // Shed finished work whose slice the scheduler has abandoned;
                // every commit serializes the whole task set.
                journal.prune_retired_history(crate::support::now_micros());
                let report = journal.coarsen_sealed_slices_capped(crate::support::now_micros(), &|project, _source, date| {
                    ceilings.get(&(project.to_string(), date.to_string())).or_else(|| ceilings.get(&("default".to_string(), date.to_string()))).copied()
                });
                if report.total() != 0 {
                    // `checkpoint`, not `compact`: `JournalRecord::Removed` lets the
                    // cheap append express a deletion.
                    journal.checkpoint()?;
                }
                report
            };
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
        // Metadata-only, so throttled by wall clock rather than admission.
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
        // Interleave dependent publication with dedup instead of draining the whole
        // dedup backlog first.
        let cycle = crate::maintenance_coordinator::operation_cycle(coverage_is_short());
        let start = self.maintenance_schedule_cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % cycle.len();
        // `Some(None)` is "exempt, or not gated at all"; `None` is "capped out",
        // and the caller falls through to the next operation in the cycle.
        let gate = |semaphore: &Arc<tokio::sync::Semaphore>, exempt: bool| {
            (exempt || !coverage_is_short()).then_some(None).or_else(|| Arc::clone(semaphore).try_acquire_owned().ok().map(Some))
        };
        let mut attempted = [false; <crate::maintenance_coordinator::Operation as strum::EnumCount>::COUNT];
        for offset in 0..cycle.len() {
            let operation = cycle[(start + offset) % cycle.len()];
            if std::mem::replace(&mut attempted[operation as usize], true) {
                continue;
            }
            // Debt work holds a worker for minutes while a rollup unit holds one for
            // seconds, so cap concurrent debt workers while coverage is short. Failing
            // to acquire falls through to the next operation, which is work-conserving.
            let Some(_debt_slot) = gate(&self.maintenance_debt_slots, matches!(operation, Operation::BaseRollup | Operation::DerivedRollup)) else {
                continue;
            };
            // Keep workers free for derived work while coverage is short: everything
            // else must leave `maintenance_derived_reserve` permits unclaimed; derived
            // itself never takes one.
            let Some(_derived_reserve) = gate(&self.maintenance_derived_reserve, operation == Operation::DerivedRollup) else { continue };
            let timeout = coordinator_operation_timeout(operation);
            // What the unit has written so far; the deadline below fires only when
            // this stops moving.
            let progress = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let label: &'static str = operation.into();
            let work = UNIT_OPERATION.scope(label, async {
                match operation {
                    Operation::Dedup => self.run_coordinator_dedup_once().await,
                    Operation::BaseRollup | Operation::DerivedRollup => self.run_coordinator_rollup_once(operation).await,
                    Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair => self.run_coordinator_compaction_once(operation).await,
                }
            });
            let started = std::time::Instant::now();
            let completed = match run_until_idle_capped(timeout, coordinator_operation_lifetime_cap(operation), Arc::clone(&progress), work).await {
                Ok(result) => {
                    // Log only the slow tail; this runs on every claim.
                    let elapsed = started.elapsed();
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
                    // Dropping the operation future drops its TaskLease, durably
                    // requeueing the unit and releasing its resource tokens.
                    let capped = coordinator_operation_lifetime_cap(operation).is_some_and(|cap| started.elapsed() >= cap);
                    if capped {
                        crate::observability::maintenance_stats().maintenance_unit_lifetime_capped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    warn!(
                        ?operation,
                        timeout_seconds = timeout.as_secs(),
                        ran_secs = started.elapsed().as_secs(),
                        capped,
                        event = "maintenance_coordinator_unit_timed_out"
                    );
                    // `killed_secs` is a strict subset of `worker_secs`: capacity that
                    // produced nothing.
                    crate::observability::count_maintenance_work(label, "killed_secs", started.elapsed().as_secs());
                    return Ok(true);
                }
            };
            if completed {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Drive up to `max_units` already-eligible durable maintenance units,
    /// returning how many ran.
    pub async fn run_maintenance_units(&self, max_units: usize) -> Result<usize> {
        let mut completed = 0usize;
        while completed < max_units && self.run_maintenance_coordinator_once().await? {
            completed += 1;
        }
        Ok(completed)
    }

    /// Durable half of the invalidation path: flush the task journal and the
    /// rollup journal, coalescing with any concurrent caller.
    ///
    /// Must run BEFORE the write is acknowledged — nothing else re-seeds a lost
    /// maintenance record (WAL replay does not run this path).
    ///
    /// Must be called with `rollup_journal_lock` RELEASED: holding it across the
    /// commit serialises every writer behind the `fsync`. Durability is unaffected —
    /// the ticket is taken after this caller's mutations are applied.
    pub(crate) fn commit_journal(&self) -> std::io::Result<()> {
        let wait = crate::observability::BlockWatch::new("journal_commit_wait");
        crate::support::without_blocking_the_worker(|| {
            self.journal_group_commit.commit(|| {
                self.journal().checkpoint().map_err(std::io::Error::other)?;
                self.persist_rollup_journal()
            })
        })?;
        drop(wait);
        let (performed, coalesced) = self.journal_group_commit.counts();
        let stats = crate::observability::maintenance_stats();
        stats.journal_commits.store(performed, std::sync::atomic::Ordering::Relaxed);
        stats.journal_commits_coalesced.store(coalesced, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// In-memory half: mark the partition dirty and mint its slice work. Not
    /// durable on its own — the caller must reach [`Self::commit_journal`]
    /// before acknowledging the write that caused it. Split out so a caller
    /// touching several partitions pays for ONE commit rather than one per partition.
    pub(crate) fn apply_rollup_hours(&self, project_id: &str, source: &str, date: &str, hours: u32) -> std::io::Result<()> {
        let _journal_guard = crate::support::lock(&self.rollup_journal_lock);
        let source_key = (project_id.to_string(), source.to_string(), date.to_string());
        // Only the hours the mutation actually touched: expanding to a full day mints
        // hundreds of durable units per project, including future and empty slices.
        // Source-wide changes pass ALL_HOURS explicitly instead.
        self.rollup_invalidated_at.entry(source_key.clone()).or_insert_with(crate::storage::now_unix_ms);
        self.rollup_dirty.entry(source_key.clone()).and_modify(|dirty| *dirty |= hours).or_insert(hours);
        self.rollup_source_epochs.entry(source_key).and_modify(|epoch| *epoch = epoch.saturating_add(1)).or_insert(1);
        if let Some(schema) = get_schema(source) {
            for spec in &schema.rollups {
                let key = (project_id.to_string(), source.to_string(), spec.table_name(source), date.to_string());
                self.rollup_coverage.remove(&key);
                self.rollup_backoff.remove(&key);
            }
        }
        if let Some(day_start) = date_start_micros(date) {
            let ranges = crate::rollup::dirty_ranges(day_start, hours);
            self.rollup_slice_coverage.retain(|(project, table, _, start, end), _| {
                project != project_id || table != source || !ranges.iter().any(|(dirty_start, dirty_end)| *start < *dirty_end && *end > *dirty_start)
            });
        }
        // Becomes durable in `commit_journal`, which the caller MUST reach before
        // acknowledging the write.
        self.mint_maintenance_hours(project_id, source, date, hours, true)
    }

    /// Invalidate only the partitions a non-MOR UPDATE/DELETE statement can have changed.
    ///
    /// Merge-on-read callers skip this — re-appended rows invalidate their own dates.
    /// Narrows to a timestamp window when the predicate confines the statement to a date
    /// range and does not assign `timestamp`; otherwise wipes the whole source. Narrowing
    /// is safe because coverage is re-proved against the partition fingerprint before use.
    pub(crate) fn invalidate_rollup_dml(
        &self, project_id: &str, source: &str, predicate: Option<&datafusion::logical_expr::Expr>, assignments: &[(String, datafusion::logical_expr::Expr)],
    ) -> std::io::Result<()> {
        let moves_rows = assignments.iter().any(|(column, _)| column == "timestamp");
        let masks = (!moves_rows).then(|| predicate.and_then(crate::rollup::timestamp_window)).flatten().and_then(|(lo, hi)| window_hour_masks(lo, hi));
        let Some(masks) = masks else { return self.invalidate_rollup_source(project_id, source) };
        for (date, hours) in masks {
            self.apply_rollup_hours(project_id, source, &date, hours)?;
        }
        self.commit_journal()
    }

    pub(crate) fn invalidate_rollup_source(&self, project_id: &str, source: &str) -> std::io::Result<()> {
        if get_schema(source).is_none_or(|schema| schema.rollups.is_empty()) {
            return Ok(());
        }
        let _journal_guard = crate::support::lock(&self.rollup_journal_lock);
        let keys: Vec<_> =
            self.rollup_source_epochs.iter().filter(|entry| entry.key().0 == project_id && entry.key().1 == source).map(|entry| entry.key().clone()).collect();
        for key in &keys {
            self.rollup_source_epochs.entry(key.clone()).and_modify(|epoch| *epoch = epoch.saturating_add(1));
            self.rollup_dirty.insert(key.clone(), crate::rollup::ALL_HOURS);
            self.rollup_invalidated_at.entry(key.clone()).or_insert_with(crate::storage::now_unix_ms);
            self.mint_maintenance_hours(&key.0, &key.1, &key.2, crate::rollup::ALL_HOURS, true)?;
        }
        self.rollup_coverage.retain(|(project, table, _, _), _| project != project_id || table != source);
        self.rollup_slice_coverage.retain(|(project, table, ..), _| project != project_id || table != source);
        self.rollup_backoff.retain(|(project, table, _, _), _| project != project_id || table != source);
        drop(_journal_guard);
        self.commit_journal()
    }

    /// Walks the batches' dates, so it is gated on the master switch as well as
    /// the schema: with rollups off this runs on every inbound write for nothing.
    pub(crate) fn invalidate_rollup_batches(&self, project_id: &str, source: &str, batches: &[RecordBatch]) -> std::io::Result<()> {
        if get_schema(source).is_none_or(|schema| schema.rollups.is_empty()) {
            return Ok(());
        }
        let mut dates: HashMap<String, u32> = HashMap::new();
        for batch in batches {
            let Some(batch_dates) = batch_hours(batch) else {
                // A batch that cannot say which partitions it touches forces the
                // source-wide wipe; warn loudly rather than doing it silently.
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
        // ONE commit for the whole batch, not one per date.
        for (date, hours) in dates {
            self.apply_rollup_hours(project_id, source, &date, hours)?;
        }
        self.commit_journal()
    }

    /// Exclusive upper bound on row timestamps a file may contain to be folded into a slice.
    ///
    /// Bounding to the part the build actually read lets a day still being written hold stable
    /// coverage: files landing above the bound change nothing, while a rewrite or late file
    /// below it moves the fingerprint and correctly invalidates.
    fn partition_fingerprints_bounded(
        table: &DeltaTable, tiebreak: Option<&str>, bound_for: &dyn Fn(&str, &str) -> i64,
    ) -> Result<HashMap<(String, String), u64>> {
        Ok(Self::partition_stats_bounded(table, tiebreak, bound_for)?.into_iter().map(|(key, stats)| (key, stats.fingerprint)).collect())
    }

    /// Split a window's live Delta files into the ones that may skip
    /// `DedupExec` and the ones that may not.
    ///
    /// The two sets partition the window's in-window files exactly once, letting the
    /// caller union the certified leg ABOVE the dedup. Both are RELATIVE paths
    /// (`parquet_rel_of_uri`).
    ///
    /// Returns `(empty, empty)` when the feature is off or the window cannot be
    /// enumerated; the caller must read that as "no split", never as "nothing
    /// certified".
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
                let bucket = if skippable.contains(&rel) { &mut certified } else { &mut uncertified };
                bucket.insert(rel);
            }
        }
        // A populated `uncertified` alone would be read as a restriction.
        if certified.is_empty() { empty } else { (certified, uncertified) }
    }

    /// The live files of one date partition that a certification still vouches
    /// for AND that no uncertified file could hold another version of.
    ///
    /// The additive half of certification: `dedup_window_certified` is all-or-nothing
    /// and any new file makes it false, whereas this asks WHICH proved files are still
    /// live and still isolated, so a new file costs only the files it overlaps.
    ///
    /// Returns the RELATIVE paths (`parquet_rel_of_uri`) that may skip, or an empty set
    /// when nothing qualifies. Soundness lives in `read::skippable_certified_files`.
    pub(crate) fn certified_files_in_partition(&self, table: &DeltaTable, project_id: &str, table_name: &str, date: &str) -> HashSet<String> {
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        let Some(cert) = self.dedup_clean_fp.get(&key).map(|entry| entry.value().clone()) else { return HashSet::new() };
        let Ok(spans) = Self::partition_file_spans(table, &format!("date={date}")) else { return HashSet::new() };
        // A path the certification names but that `spans` no longer holds was compacted
        // away; it cannot vouch for its replacement, so it drops out of the certified side.
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
    /// Relative because the two sides of the join spell files differently: a
    /// certification's stored list holds full object-store URIs while the add-actions
    /// table holds relative paths, so callers must apply `parquet_rel_of_uri` to the
    /// certification side too.
    ///
    /// A file with missing statistics maps to `None`, which callers must treat as
    /// overlapping everything — never as empty.
    pub(crate) fn partition_file_spans(table: &DeltaTable, date_marker: &str) -> Result<HashMap<String, crate::read::FileSpan>> {
        let snapshot = table.snapshot()?.snapshot();
        let actions = snapshot.add_actions_table(true)?;
        let Some(paths) = actions.column_by_name("path").cloned() else { return Ok(HashMap::new()) };
        let min_ts = crate::read::ts_micros_column(&actions, "min.timestamp");
        let max_ts = crate::read::ts_micros_column(&actions, "max.timestamp");
        Ok((0..actions.num_rows())
            .filter_map(|row| {
                let path = crate::support::test_helpers::array_get_str(paths.as_ref(), row);
                let rel = crate::tantivy::search::parquet_rel_of_uri(&path)?.to_string();
                rel.contains(date_marker).then(|| (rel, Self::valid_at(&min_ts, row).zip(Self::valid_at(&max_ts, row))))
            })
            .collect())
    }

    /// `column[row]`, when the column is present and the row is not null.
    fn valid_at(column: &Option<arrow::array::Int64Array>, row: usize) -> Option<i64> {
        column.as_ref().and_then(|column| column.is_valid(row).then(|| column.value(row)))
    }

    /// As [`Self::partition_fingerprints_bounded`], but also carrying the row
    /// timestamps each partition's files span, in one pass.
    pub(crate) fn partition_stats_bounded(
        table: &DeltaTable, tiebreak: Option<&str>, bound_for: &dyn Fn(&str, &str) -> i64,
    ) -> Result<HashMap<(String, String), PartitionStats>> {
        use std::hash::{Hash, Hasher};
        let snapshot = table.snapshot()?.snapshot();
        let actions = snapshot.add_actions_table(true)?;
        let column = |name: &str| actions.column_by_name(name).cloned();
        // A table without `numRecords` stats cannot be fingerprinted; returning empty
        // leaves every partition uncovered, which costs a raw scan rather than a wrong one.
        let (Some(records), Some(dates)) = (column("num_records"), column("partition.date")) else { return Ok(HashMap::new()) };
        let Some(records) = records.as_any().downcast_ref::<arrow::array::Int64Array>() else { return Ok(HashMap::new()) };
        let projects = column("partition.project_id");
        // Partition values arrive typed (`date` is a Date32); render it as `YYYY-MM-DD`
        // so these keys match the spelling used everywhere else.
        let string_at = |array: &Option<arrow::array::ArrayRef>, row: usize| -> Option<String> {
            let array = array.as_ref().filter(|array| array.is_valid(row))?;
            match array.data_type() {
                arrow::datatypes::DataType::Date32 => {
                    let days = arrow::array::AsArray::as_primitive_opt::<arrow::datatypes::Date32Type>(array.as_ref())?.value(row);
                    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::Duration::days(days as i64)).map(|date| date.to_string())
                }
                _ => Some(crate::support::test_helpers::array_get_str(array.as_ref(), row)),
            }
        };
        let dates = Some(dates);
        let min_ts = crate::read::ts_micros_column(&actions, "min.timestamp");
        let max_ts = crate::read::ts_micros_column(&actions, "max.timestamp");
        // The stamp is a timestamp on every current schema; an integer one is read too
        // so a schema change does not silently drop the tightening.
        let stamp = tiebreak.map(|tiebreak| format!("max.{tiebreak}")).and_then(|name| {
            crate::read::ts_micros_column(&actions, &name)
                .or_else(|| actions.column_by_name(&name)?.as_any().downcast_ref::<arrow::array::Int64Array>().cloned())
        });
        // Delta spells file size `size_bytes` in the flattened add-actions batch; the
        // `size` fallback avoids reporting a zero ceiling that caps every estimate to 0.
        let sizes = actions
            .column_by_name("size_bytes")
            .or_else(|| actions.column_by_name("size"))
            .and_then(|c| c.as_any().downcast_ref::<arrow::array::Int64Array>().cloned());
        // (rows, min_ts, max_ts, max_stamp, bytes) per partition.
        type Identity = (i64, i64, i64, i64, i64);
        let by_partition: HashMap<(String, String), Identity> = (0..actions.num_rows()).fold(HashMap::new(), |mut acc, row| {
            let Some(date) = string_at(&dates, row) else { return acc };
            // Custom-project tables carry no `project_id` partition; group under "default".
            let project = string_at(&projects, row).unwrap_or_else(|| "default".to_string());
            // A file whose rows all sit at or above this partition's bound is not
            // part of what was aggregated, so it must not perturb the fingerprint.
            if Self::valid_at(&max_ts, row).is_some_and(|hi| hi >= bound_for(&project, &date)) {
                return acc;
            }
            let entry = acc.entry((project, date)).or_insert((0, i64::MAX, i64::MIN, i64::MIN, 0));
            entry.0 += if records.is_valid(row) { records.value(row) } else { 0 };
            entry.4 += Self::valid_at(&sizes, row).unwrap_or(0);
            entry.1 = entry.1.min(Self::valid_at(&min_ts, row).unwrap_or(i64::MAX));
            entry.2 = entry.2.max(Self::valid_at(&max_ts, row).unwrap_or(i64::MIN));
            entry.3 = entry.3.max(Self::valid_at(&stamp, row).unwrap_or(i64::MIN));
            acc
        });
        Ok(by_partition
            .into_iter()
            .map(|(key, identity)| {
                let mut hasher = fnv::FnvHasher::default();
                // (rows, min_ts, max_ts, stamp) ONLY. `identity.4` (bytes) must NOT enter
                // the fingerprint: every stored `source_fp` and slice witness was recorded
                // against this 4-tuple, so a 5th field invalidates all existing coverage.
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

    /// Record one tier's untagged-file count and re-export the FLEET sum. Kept per
    /// TABLE and summed on export, so a clean tier cannot mask another tier's damage.
    fn publish_tier_untagged(&self, table_name: &str, untagged: u64) {
        self.rollup_tier_untagged.insert(table_name.to_owned(), untagged);
        crate::observability::maintenance_stats()
            .rollup_tier_untagged_found
            .store(self.rollup_tier_untagged.iter().map(|entry| *entry.value()).sum(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Write the damage set through to its sidecar. Best-effort: a lost or stale file
    /// costs one mis-ranked claim, never correctness.
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

    /// Queue a day-wide rebuild for every partition still holding a tier file
    /// with no identity tags.
    ///
    /// `slice_retires` can retire such a file, but only when something PUBLISHES that
    /// partition, which nothing does for a sealed day that already has coverage.
    ///
    /// Queued day-wide on purpose: the coordinator splits it as needed, and each child
    /// that publishes widens the union `slice_retires` uses as proof. Self-limiting —
    /// the set is re-read from the log each recovery, so it shrinks to zero.
    fn enqueue_untagged_rebuilds(&self, source: &str, spec: &crate::schema::RollupSpec, target: &str, partitions: &UntaggedPartitions) {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskKey, TimeSlice};
        // Published before the empty check, so a tier that has just converged CLEARS
        // its cells instead of ranking a clean partition forever.
        self.journal().set_untagged_cells(source, target, partitions.keys().cloned());
        self.persist_untagged_cells();
        if partitions.is_empty() {
            return;
        }
        let operation = if spec.derive_from.is_some() { Operation::DerivedRollup } else { Operation::BaseRollup };
        let now = crate::support::now_micros();
        let created = unix_ms(now);
        let mut journal = self.journal();
        let mut queued = 0usize;
        for ((project_id, date), (untagged, tagged)) in partitions {
            let Some(day_start) = date.parse::<chrono::NaiveDate>().ok().and_then(day_start_micros) else { continue };
            let day_end = day_start.saturating_add(crate::maintenance_coordinator::DAY_MICROS);
            // No gaps means the proofs already hold and the file is live only because
            // nothing has republished the partition; republishing the untagged spans
            // themselves fires the retire, bounded by those files rather than the day.
            let slices = crate::rollup::uncovered_gaps(untagged, tagged).tap_mut(|gaps| {
                if gaps.is_empty() {
                    // Merged: one span per untagged file would be one unit per file.
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
    /// `None` when the spans leave a hole: the read path serves
    /// `[day_start, covered_through)`, so the answer must be a gap-free run — a union
    /// that merely SPANS the day would claim hours no build aggregated.
    ///
    /// Anchored on the partition's earliest row rather than midnight, matching the
    /// producer: a day's first slice begins at its first row.
    fn contiguous_coverage_end(partition_min_ts: i64, spans: &mut [(i64, i64)]) -> Option<i64> {
        spans.sort_unstable();
        let (first_start, first_end) = *spans.first()?;
        if first_start > partition_min_ts {
            return None;
        }
        spans.iter().skip(1).try_fold(first_end, |cursor, &(start, end)| (start <= cursor).then(|| cursor.max(end)))
    }

    /// Subtract a landed dedup's dropped rows from every rollup slice witness over that
    /// partition, instead of letting the rewrite invalidate them.
    ///
    /// Sound only when the dedup winner is DETERMINISTIC: without a declared
    /// `dedup_tiebreak` both sides fall back to keep-FIRST, which depends on scan order,
    /// so the two could keep different versions of a row. Hence the gate on the
    /// declaration.
    ///
    /// IN-MEMORY ONLY, and therefore incomplete: recovery's authority is the tier files'
    /// `TAG_SOURCE_ROWS`, which a restart re-reads, so this repair does not survive one.
    pub(crate) fn carry_dedup_witness(&self, table_name: &str, project_id: &str, date: &str, dropped: u64) {
        if get_schema(table_name).is_none_or(|schema| schema.dedup_tiebreak.is_none()) {
            return;
        }
        let Some(day_start) = date_start_micros(date) else { return };
        let day_end = day_start.saturating_add(DAY_MICROS);
        let mut carried = 0u64;
        self.rollup_slice_coverage.iter_mut().for_each(|mut entry| {
            let (project, source, _, start, _) = entry.key();
            if project != project_id || source != table_name || *start < day_start || *start >= day_end {
                return;
            }
            if let Some(rows) = entry.value().source_rows {
                entry.value_mut().source_rows = Some(rows.saturating_sub(dropped));
                carried = carried.saturating_add(1);
            }
        });
        if carried > 0 {
            crate::observability::maintenance_stats().rollup_witness_carried.fetch_add(carried, std::sync::atomic::Ordering::Relaxed);
            debug!(table_name, project_id, date, dropped, carried, event = "rollup_witness_carried_across_dedup");
        }
    }

    async fn recover_date_coverage(&self, source: &str, target: &str) {
        let Ok(table_ref) = self.resolve_table("default", source).await else { return };
        let Ok(stats) = ({
            let table = table_ref.read().await;
            Self::partition_stats_bounded(&table, tiebreak_of(source), &|_, _| i64::MAX)
        }) else {
            return;
        };
        let by_date: HashMap<(String, String), Vec<(i64, RollupCoverage)>> = self
            .rollup_slice_coverage
            .iter()
            .filter(|entry| entry.key().1 == source && entry.key().2 == target)
            .filter_map(|entry| {
                let ((project, _, _, start, _), coverage) = (entry.key(), entry.value());
                let date = chrono::DateTime::from_timestamp_micros(*start)?.date_naive().to_string();
                Some(((project.clone(), date), (*start, coverage.clone())))
            })
            .into_group_map();
        let mut recovered = 0u64;
        let mut moved: Vec<(String, crate::maintenance_coordinator::TimeSlice)> = Vec::new();
        for ((project, date), slices) in by_date {
            let Some(partition) = stats.get(&(project.clone(), date.clone())).or_else(|| stats.get(&("default".to_string(), date.clone()))) else {
                continue;
            };
            let Ok(current_rows) = u64::try_from(partition.rows) else { continue };
            // Every slice must be verifiable AND agree: one unverifiable slice means the
            // date cannot be proven current, and an unproven date must not be stamped.
            if !slices.iter().all(|(_, coverage)| coverage.source_rows == Some(current_rows)) {
                // STALE UNTIL REBUILT, not stale forever: the read path refuses an
                // overtaken slice, so queue the disagreeing ones rather than leaving them
                // with no path back to `proven`. `enqueue_unverifiable_rebuilds` is
                // newest-first and bounded, so this cannot flood the queue.
                //
                // SEALED DAYS ONLY: a rebuild cannot make the witness agree on a day that
                // is still ingesting, because the partition moves again before the next
                // recovery pass looks -- requeuing a live day cycles instead of catching
                // up. `date < today` is the cheap form of "no longer live"; the precise
                // predicate is the dedup certification, so a partition still taking late
                // arrivals for yesterday reads as sealed here and can be requeued twice.
                if date < chrono::Utc::now().date_naive().to_string() {
                    moved.extend(slices.iter().filter(|(_, coverage)| coverage.source_rows != Some(current_rows)).map(|(start, coverage)| {
                        (project.clone(), crate::maintenance_coordinator::TimeSlice { start_micros: *start, end_micros: coverage.covered_through })
                    }));
                }
                continue;
            }
            let mut spans: Vec<(i64, i64)> = slices.iter().map(|(start, coverage)| (*start, coverage.covered_through)).collect();
            let Some(covered_through) = Self::contiguous_coverage_end(partition.min_ts, &mut spans) else { continue };
            let newest = slices.iter().map(|(_, coverage)| coverage).max_by_key(|coverage| coverage.covered_through).expect("non-empty");
            // A date can be read for a measure only if EVERY slice covering it
            // materialized it: the date route never consults the slices again, so it
            // cannot be more optimistic than the weakest one.
            let measures = slices.iter().try_fold(None::<HashSet<String>>, |folded, (_, coverage)| {
                let slice = coverage.measures.as_ref()?;
                Some(Some(folded.map_or_else(|| slice.clone(), |folded| &folded & slice)))
            });
            self.rollup_coverage.insert(
                (project.clone(), source.to_string(), target.to_string(), date.clone()),
                RollupCoverage {
                    source_fp: partition.fingerprint,
                    source_epoch: Some(self.rollup_source_epochs.get(&(project, source.to_string(), date)).map_or(0, |epoch| *epoch.value())),
                    generation: newest.generation.clone(),
                    source_rows: Some(current_rows),
                    covered_through,
                    measures: measures.flatten(),
                    content_fp: None,
                    output_files: 0,
                },
            );
            recovered += 1;
        }
        if !moved.is_empty()
            && let Some(spec) = get_schema(source).and_then(|schema| schema.rollups.iter().find(|spec| spec.table_name(source) == target).cloned())
        {
            let queued = moved.len();
            self.enqueue_unverifiable_rebuilds(source, &spec, target, RollupRebuildReason::WitnessMoved, &moved);
            info!(source, target, queued, event = "rollup_moved_slices_requeued", "slices whose witness was overtaken are queued to be re-proven");
        }
        if recovered != 0 {
            info!(source, target, recovered, event = "rollup_date_coverage_recovered", "date-level coverage rebuilt from witnessed slices");
        }
    }

    /// Republish the slices whose coverage can never be verified.
    ///
    /// Work the coordinator will not reach on its own: nothing about a sealed,
    /// fully-covered day says "republish me" — the claim is unverifiable, not missing.
    /// Enqueue is keyed and idempotent, so re-running re-queues only what has not drained.
    fn enqueue_unverifiable_rebuilds(
        &self, source: &str, spec: &crate::schema::RollupSpec, target: &str, reason: RollupRebuildReason,
        slices: &[(String, crate::maintenance_coordinator::TimeSlice)],
    ) {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Operation, TaskKey};
        if slices.is_empty() {
            return;
        }
        let operation = if spec.derive_from.is_some() { Operation::DerivedRollup } else { Operation::BaseRollup };
        let now = crate::support::now_micros();
        let created = unix_ms(now);
        // NEWEST FIRST, and bounded: queueing these flat produces a backlog that cannot
        // drain and makes the coordinator's ranking meaningless. Re-running hourly
        // advances the frontier — a republished slice carries a witness, leaves this
        // list, and the next pass takes the next `BOUND`.
        let ordered =
            slices.iter().collect::<HashSet<_>>().into_iter().sorted_unstable_by_key(|(_, slice)| std::cmp::Reverse(slice.start_micros)).collect_vec();
        const BOUND: usize = 512;
        let total = ordered.len();
        let queued = total.min(BOUND);
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
            deferred = total.saturating_sub(queued),
            ?reason,
            event = "rollup_unverifiable_rebuild_queued",
            "unverifiable slices queued for republish, newest first"
        );
    }

    /// How many DATE-level coverage entries exist.
    pub fn rollup_coverage_entries(&self) -> usize {
        self.rollup_coverage.len()
    }

    /// Seed routing coverage from the durable ledger, returning how many slices
    /// it published.
    ///
    /// `recover_rollup_coverage` rebuilds the same map by replaying every tier's Delta
    /// log, which takes minutes; the ledger holds that answer durably, so routing works
    /// from the first query after boot. Generations are revalidated before restoring —
    /// a ledger accepted by an older reader does not prove the current semantics — and
    /// the Delta tag recovery still runs and replaces the restored evidence.
    pub fn seed_routing_from_ledger(&self) -> usize {
        use crate::storage::CoverageLedger as _;
        let mut seeded = 0usize;
        for cell in self.coverage_ledger.cells() {
            let (source, project_id, table_name, date) = cell.clone();
            for entry in self.coverage_ledger.coverage(&cell) {
                let coverage = RollupCoverage {
                    source_fp: entry.source_fingerprint,
                    source_epoch: None,
                    generation: entry.generation.clone(),
                    source_rows: entry.source_rows.and_then(|rows| u64::try_from(rows).ok()),
                    covered_through: entry.end_micros,
                    measures: entry.measures.as_ref().map(|names| names.iter().cloned().collect()),
                    content_fp: None,
                    output_files: 0,
                };
                if !Self::rollup_generation_current(&source, &table_name, &project_id, &date, &coverage) {
                    crate::observability::maintenance_stats().rollup_ledger_seed_rejected_generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                self.rollup_slice_coverage.insert((project_id.clone(), source.clone(), table_name.clone(), entry.start_micros, entry.end_micros), coverage);
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
    /// Callers MUST pass slices that already passed every filter the read path applies,
    /// so the ledger claims exactly what a query would be served — never more.
    ///
    /// `coverage_ledger_disagreements` is the standing alarm against ledger drift; it
    /// must read zero before any read path trusts the ledger.
    fn record_readable_coverage(&self, source: &str, target: &str, readable: HashMap<crate::storage::CoverageCell, Vec<crate::storage::CoverageEntry>>) {
        use crate::storage::CoverageLedger as _;
        let mut disagreements = 0u64;
        let seen: std::collections::HashSet<crate::storage::CoverageCell> = readable.keys().cloned().collect();
        // Written ONCE: a per-cell write re-serializes the whole ledger.
        let mut batch = readable
            .into_iter()
            .map(|(cell, entries)| {
                let proved = crate::storage::merge_coverage(entries);
                let held = self.coverage_ledger.coverage(&cell);
                // An empty `held` is the FIRST replay for that cell, not a disagreement.
                if !held.is_empty() && held != proved {
                    disagreements += 1;
                    warn!(
                        table = %target, project_id = %cell.1, date = %cell.3,
                        held = held.len(), proved = proved.len(),
                        event = "coverage_ledger_disagreement",
                        "the ledger and the Delta tags disagree about this partition's coverage"
                    );
                }
                (cell, proved)
            })
            .collect_vec();
        // Cells this tier no longer covers. Scoped to (source, target) because this
        // replay proves nothing about a tier it did not read; an empty entry list drops
        // the cell.
        batch.extend(
            self.coverage_ledger.cells().into_iter().filter(|cell| cell.0 == source && cell.2 == target && !seen.contains(cell)).map(|cell| (cell, Vec::new())),
        );
        self.coverage_ledger.replace_many(batch);
        if disagreements > 0 {
            crate::observability::maintenance_stats().coverage_ledger_disagreements.fetch_add(disagreements, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Drop ledger cells whose date fell out of the rollup horizon, returning how
    /// many went.
    ///
    /// The tier replay's orphan sweep only retires cells of a tier it just read, so a
    /// tier that stops being replayed leaves its history behind forever.
    ///
    /// Bounded by `timefusion_rollup_backfill_days`, the same horizon the planner
    /// enumerates. `0` disables the backfill and must therefore retire NOTHING, not
    /// everything. Dates come from the virtual clock, so e2e time travel drives this.
    fn retire_aged_out_coverage(&self) -> usize {
        let horizon = self.config.maintenance.timefusion_rollup_backfill_days;
        if horizon == 0 {
            return 0;
        }
        let Some(now) = chrono::DateTime::from_timestamp_micros(crate::support::now_micros()) else { return 0 };
        let keep_from = (now.date_naive() - chrono::Duration::days(i64::from(horizon))).to_string();
        let retired = self.coverage_ledger.retire_before(&keep_from);
        if retired > 0 {
            info!(retired, keep_from, event = "rollup_coverage_ledger_retired", "coverage cells past the rollup horizon dropped from the ledger");
        }
        retired
    }

    /// Re-adopt rollup coverage that previous processes durably wrote.
    ///
    /// The rollup table is the record; there is no sidecar. Each stored
    /// `(date, generation)` is re-proved against the current source partition's
    /// generation, so a moved source leaves the partition uncovered rather than
    /// claiming coverage a rewrite would read zero rows from.
    pub async fn recover_rollup_coverage(&self, source: &str) -> Result<usize> {
        let Some(schema) = get_schema(source).filter(|schema| !schema.rollups.is_empty()) else { return Ok(0) };
        self.retire_aged_out_coverage();
        let mut recovered = 0;
        // Summed over every declared tier, then stored ONCE — a per-tier store would
        // make the last spec's count the whole reading.
        let mut unverifiable = 0u64;
        // The same population split two ways; both must sum to `unverifiable`.
        let (mut reasons, mut fates) = (Tally::new(), Tally::new());
        // One sampled slice per (tier, reason): identity, not just a count.
        let mut examples: std::collections::HashSet<(String, UnverifiableReason)> = std::collections::HashSet::new();
        // Coverage that exists on disk and cannot be used: stored generation no
        // longer matches the current spec.
        let mut stale_generation = 0u64;
        for spec in &schema.rollups {
            let target = spec.table_name(source);
            let (mut untagged_spans, mut tagged_spans): (SpansByPartition, SpansByPartition) = Default::default();
            // Counted separately from `untagged_spans`, which drops a file with
            // no statistics — the gauge must count FILES, including those.
            let mut untagged_files = 0u64;
            // Recovery reads only the Delta transaction log (coverage identity lives
            // in Add tags); no rollup data scan competes with startup queries.
            let (tagged, paths_by_identity, measures_by_identity, content_fp_by_identity, witness_reasons, witnessed) =
                match self.resolve_table("default", &target).await {
                    Ok(table) => {
                        let table = table.read().await;
                        // `source_rows` is part of the KEY so a partition rebuilt against a
                        // different source count cannot merge with the older evidence.
                        let mut groups: std::collections::HashSet<TaggedSliceIdentity> = std::collections::HashSet::new();
                        // Paths per tagged identity, keyed exactly like `groups`. The ledger
                        // must be written only from the FILTERED loop below: the filters
                        // there decide whether a slice is READABLE, and a ledger written
                        // before them claims coverage the read path refuses.
                        let mut paths_by_identity: HashMap<TaggedSliceIdentity, (String, Vec<String>)> = HashMap::new();
                        // Keyed WITHOUT `source_rows`, so a slice whose files disagree (a
                        // rewrite that stripped the tag off some of them) stays
                        // distinguishable from one that never had it — different repair.
                        let mut witness_reasons: HashMap<SliceKey, UnverifiableReason> = HashMap::new();
                        let mut witnessed: std::collections::HashSet<SliceKey> = std::collections::HashSet::new();
                        // Not part of the identity: two files of one slice that
                        // disagree about their measures are still the same slice,
                        // and folding measures into the key would instead make them
                        // two entries racing for one coverage slot.
                        let mut measures_by_identity: HashMap<TaggedSliceIdentity, Option<BTreeSet<String>>> = HashMap::new();
                        // The input set each identity was aggregated from: a property of
                        // the unit, so all its files carry the same value and any
                        // disagreement collapses to `None`, declining the skip.
                        let mut content_fp_by_identity: HashMap<TaggedSliceIdentity, Option<u64>> = HashMap::new();
                        for add in table.snapshot()?.log_data().iter() {
                            let action = add_action(&add);
                            // An untagged file proves no coverage; remember its partition
                            // so a rebuild can be enqueued below (a sealed day that
                            // already has coverage is otherwise never republished).
                            // Both arms feed `uncovered_gaps`. `hi + 1` because
                            // statistics bounds are inclusive while a slice end is not.
                            let file_partition = Self::maintenance_partition_from_action(&action.path, Some(&action.partition_values), "default");
                            if let Some(partition) = file_partition.clone() {
                                let tags = action.tags.as_ref();
                                let tag = |name: &str| tags?.get(name).and_then(Option::as_deref)?.parse::<i64>().ok();
                                match (tag(crate::maintenance_coordinator::TAG_SLICE_START), tag(crate::maintenance_coordinator::TAG_SLICE_END)) {
                                    (Some(start), Some(end)) => tagged_spans.entry(partition).or_default().push((start, end)),
                                    _ => {
                                        untagged_files = untagged_files.saturating_add(1);
                                        untagged_spans.entry(partition).or_default().extend(
                                            action.stats.as_deref().and_then(crate::rollup::stats_time_range).map(|(lo, hi)| (lo, hi.saturating_add(1))),
                                        );
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
                                // Tagged for THIS source, yet not identifiable: the file
                                // is dropped from the tagged set and counted nowhere
                                // else, so without this it is an invisible population.
                                crate::database::rollup_unverifiable::IDENTITY_TAG_INCOMPLETE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                continue;
                            };
                            // Absent on pre-tag generations; `-1` is the sentinel for "source
                            // reported no count". Both become `None`, which the read path
                            // refuses to verify.
                            let witness = crate::database::rollup_unverifiable::classify_witness(tag(crate::maintenance_coordinator::TAG_SOURCE_ROWS));
                            let source_rows = witness.ok();
                            let slice_key = (project.to_owned(), slice_start, slice_end, generation.to_owned(), source_fp);
                            match witness {
                                Ok(_) => {
                                    witnessed.insert(slice_key);
                                }
                                // Lowest variant wins, so a slice seen under two
                                // reasons attributes deterministically whatever order
                                // the log lists its files in.
                                Err(reason) => {
                                    witness_reasons.entry(slice_key).and_modify(|held| *held = (*held).min(reason)).or_insert(reason);
                                }
                            }
                            // Absent means "no evidence", NOT "no measures": an empty tag
                            // value must still parse as `Some(∅)`. A slice can only be
                            // read for a measure ALL its files carry, hence the intersection.
                            let measures = tag(crate::maintenance_coordinator::TAG_MEASURES)
                                .map(|value| value.split(',').filter(|name| !name.is_empty()).map(str::to_owned).collect::<BTreeSet<String>>());
                            let identity = (project.to_owned(), slice_start, slice_end, generation.to_owned(), source_fp, source_rows);
                            let merged = match measures_by_identity.remove(&identity) {
                                Some(seen) => seen.zip(measures).map(|(left, right)| &left & &right),
                                None => measures,
                            };
                            measures_by_identity.insert(identity.clone(), merged);
                            let content_fp = tag(crate::maintenance_coordinator::TAG_CONTENT_FINGERPRINT).and_then(|value| value.parse::<u64>().ok());
                            let agreed = match content_fp_by_identity.remove(&identity) {
                                Some(seen) if seen == content_fp => seen,
                                Some(_) => None,
                                None => content_fp,
                            };
                            content_fp_by_identity.insert(identity.clone(), agreed);
                            groups.insert(identity);
                            // The date comes from the file's own partition, not from
                            // `slice_start`: a file in `date=D` cannot hold rows outside
                            // `D`, which is the stronger statement.
                            if let Some((partition_project, _date)) = file_partition.as_ref() {
                                let entry = paths_by_identity
                                    .entry((project.to_owned(), slice_start, slice_end, generation.to_owned(), source_fp, source_rows))
                                    .or_insert_with(|| ((*partition_project).to_owned(), Vec::new()));
                                entry.1.push(action.path.clone());
                            }
                        }
                        (groups, paths_by_identity, measures_by_identity, content_fp_by_identity, witness_reasons, witnessed)
                    }
                    Err(_) => Default::default(),
                };
            // Filled by the FILTERED loop below, then written once per tier. Nothing
            // is recorded for a slice the read path would refuse: the ledger is the
            // authority, and one that over-claims serves wrong results.
            let mut readable: HashMap<crate::storage::CoverageCell, Vec<crate::storage::CoverageEntry>> = HashMap::new();
            // Published from the WHOLE tier, before any `continue` below, so the gauge
            // distinguishes "none" from "unmeasured".
            self.publish_tier_untagged(&target, untagged_files);
            let untagged_partitions: UntaggedPartitions = untagged_spans
                .drain()
                .map(|(partition, untagged)| {
                    let tagged = tagged_spans.remove(&partition).unwrap_or_default();
                    (partition, (untagged, tagged))
                })
                .collect();
            self.enqueue_untagged_rebuilds(source, spec, &target, &untagged_partitions);
            // Slices recovered WITHOUT a row witness: they predate `TAG_SOURCE_ROWS`
            // and carry no evidence any read-side rule can verify, so they are refused
            // as `stale_coverage` until republished. Queued explicitly rather than left
            // to compete in the general queue.
            let mut witnessless: Vec<(String, crate::maintenance_coordinator::TimeSlice)> = Vec::new();
            let mut obsolete_generations = Vec::new();
            // Counted from the durable Delta tags, NOT from `published_rollups`:
            // enqueueing a slice flips its task off Complete, so a journal-derived
            // count reads zero exactly when the backlog is being worked.
            // A slice whose siblings carry a witness is `MixedWitness` whatever its
            // own tag said, because a partial strip is repaired, not republished.
            let mut spec_unverifiable = 0u64;
            for identity @ (project_id, slice_start, slice_end, generation, source_fp, source_rows) in &tagged {
                if source_rows.is_some() {
                    continue;
                }
                let key = (project_id.clone(), *slice_start, *slice_end, generation.clone(), *source_fp);
                let reason = if witnessed.contains(&key) {
                    UnverifiableReason::MixedWitness
                } else {
                    // Never `TagAbsent` as a default: a lost classification would then
                    // hide inside the largest legitimate bucket.
                    witness_reasons.get(&key).copied().unwrap_or(UnverifiableReason::Unattributed)
                };
                reason.bump(&mut reasons);
                spec_unverifiable += 1;
                // ONE offending slice per (tier, reason): bounded by the number of
                // reasons, not by the slice population.
                if examples.insert((target.clone(), reason)) {
                    warn!(
                        source,
                        target,
                        project_id = %project_id,
                        slice_start,
                        slice_end,
                        generation = %generation,
                        source_fp,
                        reason = reason.as_str(),
                        date = ?chrono::DateTime::from_timestamp_micros(*slice_start).map(|time| time.date_naive().to_string()),
                        files = paths_by_identity.get(identity).map(|(_, paths)| paths.len()).unwrap_or_default(),
                        event = "rollup_unverifiable_example",
                        "a sampled slice that can never be verified"
                    );
                }
            }
            unverifiable += spec_unverifiable;
            let published = self.journal().published_rollups(source, &target);
            for (key, publication) in &published {
                if publication.source_rows.is_none() {
                    witnessless.push((key.project_id.clone(), key.slice));
                }
                let Some(date) = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive().to_string()) else { continue };
                let identity = (
                    key.project_id.clone(),
                    key.slice.start_micros,
                    key.slice.end_micros,
                    publication.generation.clone(),
                    publication.source_fingerprint,
                    publication.source_rows,
                );
                let coverage = RollupCoverage {
                    source_fp: publication.source_fingerprint,
                    source_epoch: None,
                    generation: publication.generation.clone(),
                    source_rows: publication.source_rows,
                    covered_through: key.slice.end_micros,
                    // The journal alone has no measure proof. Use matching file
                    // evidence when available, including partial measure sets.
                    measures: measures_by_identity.get(&identity).and_then(Option::as_ref).map(|names| names.iter().cloned().collect()),
                    content_fp: None,
                    output_files: 0,
                };
                if !Self::rollup_generation_current(source, &target, &key.project_id, &date, &coverage) {
                    // The tagged loop queues identities it sees. A journal-only
                    // publication still needs rebuilding when the tags are absent.
                    if !measures_by_identity.contains_key(&identity) {
                        obsolete_generations.push((key.project_id.clone(), key.slice));
                    }
                    continue;
                }
                self.rollup_slice_coverage
                    .insert((key.project_id.clone(), source.to_string(), target.clone(), key.slice.start_micros, key.slice.end_micros), coverage);
                recovered += 1;
            }
            if !tagged.is_empty() {
                for (project_id, slice_start, slice_end, generation, source_fp, source_rows) in tagged {
                    // Attributes what this pass did with the slice; every exit below
                    // would otherwise be a silent `continue`.
                    let mut fate = |bucket: UnverifiableFate| {
                        if source_rows.is_none() {
                            bucket.bump(&mut fates);
                        }
                    };
                    let Ok(slice) = crate::maintenance_coordinator::TimeSlice::new(slice_start, slice_end) else {
                        fate(UnverifiableFate::InvalidSlice);
                        continue;
                    };
                    let complete = self.journal().rollup_slice_complete(source, &project_id, &target, slice);
                    let Some(date) = chrono::DateTime::from_timestamp_micros(slice_start).map(|time| time.date_naive().to_string()) else {
                        fate(UnverifiableFate::InvalidSlice);
                        continue;
                    };
                    // `generation_id` is taken over the measures this cell actually
                    // MATERIALIZED, so it rejects a redefined measure but tolerates
                    // additive spec change; a rejection is still unusable coverage and
                    // is counted rather than skipped silently.
                    if !complete {
                        fate(UnverifiableFate::JournalIncomplete);
                        continue;
                    }
                    let identity = (project_id.clone(), slice_start, slice_end, generation.clone(), source_fp, source_rows);
                    let measures = measures_by_identity.get(&identity).cloned().flatten();
                    let restrict = measures.as_ref().map(|names| names.iter().cloned().collect::<Vec<_>>());
                    if crate::rollup::generation_id(spec, source, &project_id, &date, source_fp, restrict.as_deref()) != generation {
                        fate(UnverifiableFate::StaleGeneration);
                        stale_generation += 1;
                        obsolete_generations.push((project_id.clone(), slice));
                        continue;
                    }
                    if source_rows.is_none() {
                        fate(UnverifiableFate::QueuedForRepublish);
                        witnessless.push((project_id.clone(), slice));
                    }
                    // Past every readability filter, so this slice is exactly what the
                    // read path will serve, and therefore what the ledger may claim.
                    if let Some((partition_project, paths)) = paths_by_identity.get(&identity) {
                        readable.entry((source.to_owned(), partition_project.clone(), target.clone(), date.clone())).or_default().push(
                            crate::storage::CoverageEntry {
                                start_micros: slice_start,
                                end_micros: slice_end,
                                generation: generation.clone(),
                                source_fingerprint: source_fp,
                                source_rows: source_rows.and_then(|rows| i64::try_from(rows).ok()),
                                files: paths.clone(),
                                measures: measures.as_ref().map(|names| names.iter().cloned().collect()),
                            },
                        );
                    }
                    // The no-op skip's two halves. An absent tag yields `None`/`0`, and
                    // either alone declines the skip, so a pre-tag cell costs one
                    // rebuild rather than freezing.
                    let content_fp = content_fp_by_identity.get(&identity).copied().flatten();
                    let output_files = paths_by_identity.get(&identity).map_or(0, |(_, paths)| u32::try_from(paths.len()).unwrap_or(u32::MAX));
                    self.rollup_slice_coverage.insert(
                        (project_id, source.to_string(), target.clone(), slice_start, slice_end),
                        RollupCoverage {
                            source_fp,
                            source_epoch: None,
                            generation,
                            source_rows,
                            covered_through: slice_end,
                            measures: measures.map(|names| names.into_iter().collect()),
                            content_fp,
                            output_files,
                        },
                    );
                    recovered += 1;
                }
                // Only from the FILTERED loop above: with no tagged files
                // `readable` is empty, and recording an empty set would retire
                // every ledger cell this tier has as an orphan.
                self.record_readable_coverage(source, &target, readable);
            }
            self.enqueue_unverifiable_rebuilds(source, spec, &target, RollupRebuildReason::MissingRowWitness, &witnessless);
            self.enqueue_unverifiable_rebuilds(source, spec, &target, RollupRebuildReason::ObsoleteGeneration, &obsolete_generations);
            self.recover_date_coverage(source, &target).await;
            // Untagged legacy generations are left uncovered: recovering them needs a
            // rollup data scan, which starves pgwire at startup. Reads fall back to
            // raw data until the coordinator republishes them with tags.
        }
        crate::observability::maintenance_stats().rollup_witnessless_slices.store(unverifiable, std::sync::atomic::Ordering::Relaxed);
        // Each dimension attributes the WHOLE population: a split that stops summing
        // to the aggregate is a lost bucket.
        debug_assert_eq!(reasons.iter().sum::<u64>(), unverifiable, "every unverifiable slice must carry a reason");
        debug_assert_eq!(fates.iter().sum::<u64>(), unverifiable, "every unverifiable slice must carry a fate");
        crate::database::rollup_unverifiable::publish(source, &reasons, &fates);
        info!(
            source,
            recovered,
            unverifiable,
            stale_generation,
            by_reason = %UnverifiableReason::render(&reasons),
            by_fate = %UnverifiableFate::render(&fates),
            event = "rollup_coverage_recovered"
        );
        Ok(recovered)
    }

    /// Fold one completed coordinator dedup unit into per-day clean-slice coverage,
    /// granting certification once the accumulated slices cover the whole UTC day.
    ///
    /// A slice counts as clean evidence only when the pass dropped nothing AND the partition's
    /// file fingerprint did not move across it — or when it dropped losers by MASKING them in
    /// place (DV-dedup), whose post-state is byte-for-byte what the pass proved clean.
    /// Evidence is per-fingerprint: observing a slice under a different fp resets the
    /// accumulation to just that slice. A dirty pass resets coverage and voids any existing
    /// certification. The grant goes through `record_certification` so the rule cannot drift
    /// from the sweep/backfill paths.
    async fn record_clean_slice(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
        (slice, dropped, masked): (crate::maintenance_coordinator::TimeSlice, u64, Option<MaskedPass<'_>>), pre: &[String],
    ) -> Result<Option<u64>> {
        let day_start = day_start_micros(date).unwrap_or_default();
        let day_end = day_start.saturating_add(crate::maintenance_coordinator::DAY_MICROS);
        let (start, end) = (slice.start_micros.max(day_start), slice.end_micros.min(day_end));
        if start >= end {
            metrics::counter!(scan_metric_names::CERT_SLICE_OUTSIDE_DAY).increment(1);
            return Ok(None); // a slice outside the day proves nothing about it
        }
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        let (post, post_dv) = {
            let table = table_ref.read().await;
            (
                Self::partition_files_by_pid(&table, &format!("date={date}"))?.remove(project_id).unwrap_or_default(),
                masked.map(|_| Self::partition_dv_state(&table, project_id, &format!("date={date}"))).transpose()?,
            )
        };
        let fp = partition_file_fp(&post);
        // DV-visibility guard, masked arm only: the URI fp cannot see a foreign
        // same-path DV commit (DML DELETE/UPDATE write DVs too), so compare the
        // live (path, dv_unique_id) set instead — fail-closed, like fp_moved.
        let dv_moved = masked.zip(post_dv.as_ref()).is_some_and(|((pre_dv, attachments), live)| dv_visibility_moved(pre_dv, attachments, live));
        if dv_moved {
            metrics::counter!(scan_metric_names::CERT_SLICE_DV_MOVED).increment(1);
        }
        if slice_pass_dirty(dropped, masked.is_some(), post.is_empty(), partition_file_fp(pre) != fp, dv_moved) {
            metrics::counter!(scan_metric_names::CERT_SLICE_DIRTY).increment(1);
            if self.dedup_slice_coverage.remove(&key).is_some() {
                self.persist_slice_coverage();
            }
            return self.record_certification(table_ref, table_name, project_id, date, pre, (dropped, true)).await;
        }
        // Bind in its own block: the RefMut must drop before the remove/await below
        // (DashMap-shard self-deadlock).
        // `intervals` is the ACCUMULATED clean coverage, not just this slice. Every
        // interval was proved under the same `fp` (the entry resets wholesale when the
        // fingerprint moves), so the union is exactly as sound as one slice.
        let (covered, intervals) = {
            let mut entry = self.dedup_slice_coverage.entry(key.clone()).or_insert_with(|| SliceCoverage { fp, intervals: Vec::new() });
            if entry.fp != fp {
                *entry = SliceCoverage { fp, intervals: vec![(start, end)] };
            } else {
                merge_clean_interval(&mut entry.intervals, (start, end));
            }
            (entry.intervals.iter().any(|&(s, e)| s <= day_start && e >= day_end), entry.intervals.clone())
        };
        // Write-through on every mutation: the journal durably marks this slice
        // Complete (it will never re-run), so its evidence must be equally
        // durable or a restart strands the day at partial coverage forever.
        self.persist_slice_coverage();
        if !covered {
            metrics::counter!(scan_metric_names::CERT_SLICE_PARTIAL).increment(1);
            // The day is not proved but the SLICE is, which is a durable fact about
            // the files whose whole span lies inside it. Banking it is what makes the
            // per-FILE skip reachable: whole-day coverage under an unchanged
            // fingerprint essentially never happens on a live table.
            self.certify_files_within_slice(table_ref, table_name, project_id, date, &intervals).await;
            return Ok(None);
        }
        metrics::counter!(scan_metric_names::CERT_SLICE_DAY_COVERED).increment(1);
        self.dedup_slice_coverage.remove(&key);
        self.persist_slice_coverage();
        self.record_certification(table_ref, table_name, project_id, date, &post, (0, true)).await
    }

    /// Bank the files a clean slice proved, as evidence for the per-FILE skip.
    ///
    /// A clean pass over `[start, end)` is a statement about ROWS IN A TIME RANGE. It
    /// upgrades to a statement about a FILE only when the file's entire span lies
    /// inside the slice; a file crossing the boundary holds rows the pass never saw.
    ///
    /// Two further conditions, both fail-closed:
    /// - **`timestamp` must be a dedup key**, so a duplicate group shares one exact
    ///   timestamp and no group straddles the boundary.
    /// - **A file with no statistics is never certified**, since its span cannot be
    ///   shown to be contained.
    ///
    /// Entries written here are `stale`: they vouch for the files they name and may
    /// NEVER grant the whole-partition skip.
    async fn certify_files_within_slice(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, intervals: &[(i64, i64)],
    ) {
        if !schema_or_default(table_name).dedup_keys.iter().any(|key| key == "timestamp") {
            return;
        }
        let spans = {
            let table = table_ref.read().await;
            Self::partition_file_spans(&table, &format!("date={date}"))
        };
        let Ok(spans) = spans else { return };
        let (proved, unproven): (Vec<_>, Vec<_>) =
            spans.into_iter().partition(|&(_, span)| span.is_some_and(|(min, max)| intervals.iter().any(|&(start, end)| min >= start && max < end)));
        metrics::counter!(scan_metric_names::CERT_SLICE_FILES_UNPROVEN).increment(unproven.len() as u64);
        if proved.is_empty() {
            return;
        }
        metrics::counter!(scan_metric_names::CERT_SLICE_FILES_PROVED).increment(proved.len() as u64);
        let key = (project_id.to_string(), table_name.to_string(), date.to_string());
        // Never DOWNGRADE: a live whole-day grant outranks anything a slice can say.
        // Bound in a `let`, NOT inlined as the scrutinee: a DashMap `Ref` temporary in
        // a match scrutinee lives until the end of the MATCH, and every arm below
        // writes to the same shard — a self-deadlock.
        let existing = self.dedup_clean_fp.get(&key).map(|entry| entry.value().stale);
        match existing {
            Some(false) => return,
            Some(true) => self.dedup_clean_fp.alter(&key, |_, mut live| {
                live.files = live.files.iter().chain(proved.iter().map(|(rel, _)| rel)).cloned().sorted_unstable().dedup().collect();
                live
            }),
            None => {
                let files: Arc<[String]> = proved.into_iter().map(|(rel, _)| rel).collect();
                self.dedup_clean_fp.insert(key, Certification { fp: 0, since: std::time::Instant::now(), files, stale: true });
            }
        }
        self.persist_certifications_debounced();
    }

    /// Write slice evidence at most once a minute.
    ///
    /// Durability is required — the journal marks a slice Complete forever, so evidence
    /// lost to a restart is never re-proved — but re-serializing the whole ledger per
    /// slice is heavy write amplification. A minute bounds the loss.
    fn persist_certifications_debounced(&self) {
        const FLOOR_MICROS: i64 = 60 * 1_000_000;
        let now = crate::support::now_micros();
        let last = self.dedup_certification_persist_at.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(last) < FLOOR_MICROS {
            return;
        }
        self.dedup_certification_persist_at.store(now, std::sync::atomic::Ordering::Relaxed);
        self.persist_certifications();
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
        let fp_post = partition_file_fp(&post);
        if dropped == 0 && complete && !post.is_empty() && partition_file_fp(pre) == fp_post {
            // Re-certifying at the SAME fingerprint continues the existing
            // certification: keeping the original `since` stops the sweep cadence from
            // capping every dwell at one tick.
            let prior = self.dedup_clean_fp.get(&key).map(|e| e.value().clone()).filter(|prev| prev.fp == fp_post);
            // The file list the pass proved clean, kept so the per-FILE skip can ask
            // which of them are still live after the partition gains one.
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
        // Name the failing conjunct — each is a different cause.
        metrics::counter!(match () {
            _ if dropped != 0 => scan_metric_names::CERT_REFUSED_DROPPED,
            _ if !complete => scan_metric_names::CERT_REFUSED_INCOMPLETE,
            _ if post.is_empty() => scan_metric_names::CERT_REFUSED_EMPTY,
            _ => scan_metric_names::CERT_REFUSED_FP_MOVED,
        })
        .increment(1);
        // Keep-and-mark-stale when the entry carries per-file evidence; only a
        // certification with nothing to vouch for is removed. Sound because a duplicate
        // group touching a certified file forces that file's rewrite, and a path that
        // is no longer live drops out of `certified_files_in_partition`.
        // Bind first, or the arms deadlock against the Ref.
        let existing = self.dedup_clean_fp.get(&key).map(|entry| (entry.value().files.is_empty(), entry.value().stale, entry.value().since));
        match existing {
            Some((false, was_stale, since)) => {
                self.dedup_clean_fp.alter(&key, |_, live| Certification { stale: true, ..live });
                if !was_stale {
                    self.scan_metrics.record_cert_dwell(since);
                }
            }
            Some((true, ..)) => {
                if let Some((_, prev)) = self.dedup_clean_fp.remove(&key) {
                    self.scan_metrics.record_cert_dwell(prev.since);
                }
            }
            None => {}
        }
        Ok(None)
    }

    /// Is every partition in the window certified duplicate-free, and if not, why not?
    ///
    /// `FpMoved` outranks `NeverCertified`: one written-since-sweep date definitively denies the
    /// window, regardless of what is stored. Reporting the first denial in date order would
    /// over-report `NeverCertified`, since `window_dates` runs oldest-first.
    pub(crate) fn dedup_window_clean(&self, table: &DeltaTable, project_id: &str, table_name: &str, window: (i64, i64)) -> DedupSkipVerdict {
        self.dedup_window_certified(table, project_id, table_name, window).0
    }

    /// As `dedup_window_clean`, but also returns the `date=` values whose
    /// certification still matches the live file set.
    ///
    /// The set is what makes a PER-DATE skip possible on a window that is only partly
    /// certified. Per date is sound because `date` is derived from `timestamp` and DML
    /// re-appends preserve the original timestamp, so every version and tombstone of a
    /// row shares one date partition — no dedup key can span dates.
    ///
    /// This does NOT return early on the first stale certification: the whole window
    /// must be walked to collect the certified set. `FpMoved` still outranks
    /// `NeverCertified` in the verdict.
    pub(crate) fn dedup_window_certified(
        &self, table: &DeltaTable, project_id: &str, table_name: &str, (lo, hi): (i64, i64),
    ) -> (DedupSkipVerdict, HashSet<String>) {
        let mut certified_dates: HashSet<String> = HashSet::new();
        let Some(dates) = window_dates(lo, hi) else { return (DedupSkipVerdict::NoWindow, certified_dates) };
        let mut verdict = DedupSkipVerdict::Granted;
        let mut saw_fp_moved = false;
        // Did any partition actually produce evidence? `Granted` is the loop's seed and
        // every `continue` leaves it untouched, so without this a window where EVERY
        // date is skipped would grant from an absence of evidence — and the skip it
        // authorises removes DedupExec from the whole scan, including the MemBuffer and
        // hot-tier legs, which can hold superseded merge-on-read versions.
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
            // Bound in a `let`, NOT inlined as the match scrutinee: the `Ref` would live
            // until the end of the match while the stale arm removes from the same
            // shard — a self-deadlock.
            let certified = self.dedup_clean_fp.get(&fp_key).map(|entry| entry.value().clone());
            match certified {
                // `!cert.stale` is required, not decorative: a slice-derived
                // certification proves one time window, never the day, and its
                // fingerprint can still match the live partition.
                Some(cert) if !cert.stale && cert.fp == partition_file_fp(&files) => {
                    certified_any = true;
                    certified_dates.insert(date.to_string());
                    continue;
                }
                Some(cert) => {
                    // Provably stale: this fingerprint can never match again until a
                    // sweep re-certifies. Removal is conditional on the value being
                    // unchanged so a sweep that re-certified in the gap keeps its fresh
                    // entry; with the per-FILE skip on the entry is kept and merely
                    // marked stale, since its file list stays true. Dwell is recorded
                    // exactly once, on the transition.
                    match self.config.maintenance.timefusion_read_dedup_skip_per_file {
                        false => {
                            if self.dedup_clean_fp.remove_if(&fp_key, |_, live| live.fp == cert.fp).is_some() {
                                self.scan_metrics.record_cert_dwell(cert.since);
                            }
                        }
                        true if !cert.stale => {
                            self.dedup_clean_fp.alter(&fp_key, |_, live| Certification { stale: true, ..live });
                            self.scan_metrics.record_cert_dwell(cert.since);
                        }
                        true => {}
                    }
                    saw_fp_moved = true;
                }
                None => verdict = DedupSkipVerdict::NeverCertified,
            }
        }
        // `FpMoved` outranks `NeverCertified` INCLUDING the no-evidence fallback below;
        // it is applied after the full walk only so the certified set is complete.
        // Ordering it after `certified_any` reports a written-to partition as
        // never-certified.
        if saw_fp_moved {
            return (DedupSkipVerdict::FpMoved, certified_dates);
        }
        match certified_any {
            true => (verdict, certified_dates),
            false => (DedupSkipVerdict::NeverCertified, HashSet::new()),
        }
    }

    pub(crate) fn logical_count_partition_snapshot(table: &DeltaTable, project_id: &str, date: &str) -> Result<(u64, crate::read::CountFiles)> {
        let snapshot = table.snapshot()?.snapshot();
        let paths: HashSet<_> = dedup_partition_paths(snapshot.log_data().iter().map(|file| file.path().to_string()), project_id, date).into_iter().collect();
        let mut files = crate::read::CountFiles::new();
        for file in snapshot.log_data().iter().filter(|file| paths.contains(file.path().as_ref())) {
            anyhow::ensure!(
                files.insert(file.path().into_owned(), file.deletion_vector_descriptor()).is_none(),
                "logical-count snapshot has multiple active entries for one Parquet path"
            );
        }
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&serde_json::to_vec(&files)?, &mut hasher);
        Ok((std::hash::Hasher::finish(&hasher), files))
    }

    /// Memory-only lookup for a base whose files are all present in the table
    /// snapshot the caller holds. Newly appended files are returned for a
    /// narrow overlay; any removal/rewrite declines. Filesystem IO is forbidden
    /// on this query path.
    pub(crate) fn logical_count_memory_for_files(
        &self, project_id: &str, table_name: &str, date: &str, files: &crate::read::CountFiles,
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
            Self::narrow_provider(log_store, snapshot, files, None, None).await.map_err(|error| anyhow::anyhow!("logical-count overlay provider: {error}"))?;
        let context = SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.shared_runtime_env()));
        context.register_table("__logical_count_overlay", provider)?;
        Ok(context
            .table("__logical_count_overlay")
            .await?
            .select_columns(&[&[columns.timestamp], columns.keys, &[columns.tiebreak, columns.deleted]].concat())?
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
        let root = log_store.root_url().clone();

        // Try the persistent Arrow tier first: a valid file installs the memory front
        // without scanning Delta.
        let cache = Arc::clone(&self.logical_count_cache);
        let disk_key = key.clone();
        let current_files = files.clone();
        if !force_refresh
            && let Some(added_files) = tokio::task::spawn_blocking(move || cache.load_appendable(&disk_key, &current_files)).await?
            && added_files <= crate::read::MAX_APPEND_OVERLAY_FILES
        {
            return Ok(());
        }

        let declared = get_schema(&key.table_name).ok_or_else(|| anyhow::anyhow!("logical-count table is not registered"))?;
        let keys = crate::read::logical_count_keys(declared)
            .ok_or_else(|| anyhow::anyhow!("logical-count requires dedup keys leading with `timestamp`, got {:?}", declared.dedup_keys))?;
        let tiebreak = declared.dedup_tiebreak.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no dedup tiebreak"))?;
        let deleted = declared.tombstone_column.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no tombstone column"))?;
        let columns = crate::read::LogicalCountColumns { timestamp: "timestamp", keys: &keys, tiebreak, deleted };
        let mut index = crate::read::LogicalCountIndex::new();

        if !files.is_empty() {
            let provider = Self::narrow_provider(log_store, eager_snapshot, files.keys().cloned().collect(), None, None)
                .await
                .map_err(|error| anyhow::anyhow!("logical-count provider: {error}"))?;
            let context =
                SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()));
            context.register_table("__logical_count_src", provider)?;
            let frame = context
                .table("__logical_count_src")
                .await?
                .select_columns(&[&[columns.timestamp], columns.keys, &[columns.tiebreak, columns.deleted]].concat())?;
            let mut stream = frame.execute_stream().await?;
            loop {
                let batch = tokio::select! {
                    batch = stream.try_next() => batch?,
                    () = self.maintenance_shutdown.cancelled() => return Ok(()),
                };
                let Some(batch) = batch else { break };
                index.apply_batch(&batch, columns)?;
                // The mutable builder costs more than the packed resident form, so it
                // gets half the cache budget: the four-way resident limit applied here
                // would stop large days ever reaching `finalize`, where the overhead
                // is released.
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

        // Release the allocation-heavy mutable hash map before cache admission.
        index.finalize()?;
        // A three-day dashboard window can touch four UTC partitions; reserve room for
        // all four so valid daily indexes cannot evict one another into a rebuild loop.
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
            Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?.1
        };
        anyhow::ensure!(files.iter().all(|(path, dv)| current_files.get(path) == Some(dv)), "logical-count partition was rewritten during build");

        let physical_keys = index.physical_keys();
        let logical_rows = index.logical_rows();
        let estimated_bytes = index.estimated_heap_bytes();
        let file_count = files.len();
        let proof = if self.tantivy_indexer().is_some()
            && declared
                .fields
                .iter()
                .any(|field| field.tantivy.as_ref().is_some_and(|config| config.indexed && config.list_mode == crate::schema::TantivyListMode::Elements))
        {
            let date = key.date.parse::<chrono::NaiveDate>()?;
            let lo = date.and_hms_opt(0, 0, 0).context("invalid proof date")?.and_utc().timestamp_micros();
            let hi = lo.checked_add(86_400_000_000).context("proof date overflow")?;
            Some((date, crate::tantivy::visibility::PartitionCountProof::new(root, files.clone(), declared, index.count(lo, hi))?))
        } else {
            None
        };
        let cache = Arc::clone(&self.logical_count_cache);
        let install_key = key.clone();
        tokio::task::spawn_blocking(move || cache.install(install_key, fingerprint, files, index)).await??;
        if let Some((date, proof)) = proof
            && let Some(indexer) = self.tantivy_indexer()
            && let Err(error) = indexer.publish_count_proof(&key.table_name, &key.project_id, date, proof).await
        {
            warn!(%error, project_id = key.project_id, table_name = key.table_name, %date, "histogram count proof publication failed");
        }
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
    /// The sweep is O(dates × projects) with real IO per item; without a deadline it holds
    /// `maintenance_job_sem` past its schedule and starves the dirty-bin drain. Items are
    /// independent and idempotent, so truncation is safe, and the cursor rotates so the next
    /// tick resumes where this one stopped.
    async fn dedup_sweep(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str, deadline: Option<std::time::Instant>) -> Result<()> {
        let schema = schema_or_default(table_name);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        // Today plus a lookback window: a cross-flush dupe landing in a prior-day
        // partition (late replay crossing midnight UTC) never collapses under a
        // today-only scope. The version skip below bounds the cost.
        let today = Utc::now().date_naive();
        let lookback = self.config.maintenance.timefusion_dedup_lookback_days as i64;
        let dates: Vec<chrono::NaiveDate> = (0..=lookback).rev().map(|d| today - chrono::Duration::days(d)).collect();

        let pre_version = table_ref.read().await.version().unwrap_or(0);
        let needs_rollup_retry = get_schema(table_name).is_some_and(|schema| !schema.rollups.is_empty());
        if !needs_rollup_retry && self.last_dedup_versions.read().await.get(dedup_key).copied() == Some(pre_version) {
            debug!("dedup sweep: table={} version={} unchanged — skipping", table_name, pre_version);
            return Ok(());
        }

        let mut total_dropped = 0u64;
        let mut any_ok = false;
        // One (date, project) work list, so the deadline can cut the pass at any point
        // and the cursor can resume there.
        let mut work: Vec<(chrono::NaiveDate, String, Vec<String>)> = Vec::new();
        for date in dates {
            // Per-project live file lists for this date. Custom-project tables
            // don't embed project_id in the path; sweep "default".
            let files_by_pid = {
                let table = table_ref.read().await;
                Self::partition_files_by_pid(&table, &format!("date={date}"))?
            };
            match files_by_pid.is_empty() {
                true => work.push((date, "default".to_string(), Vec::new())),
                false => work.extend(files_by_pid.into_iter().map(|(pid, files)| (date, pid, files))),
            }
        }
        // Stable order (newest date first), then rotate: a truncated tick must not
        // re-serve the same prefix on the next one.
        work.sort_by(|(da, pa, _), (db, pb, _)| db.cmp(da).then_with(|| pa.cmp(pb)));
        let total_work = work.len();
        // Today never rotates out: rotation exists so a truncated tick resumes into
        // UNSEEN sealed work, but today is re-dirtied by every flush and is what the
        // hot queries read, so it must be swept on every tick.
        let sealed_from = work.partition_point(|(date, _, _)| *date >= today);
        rotate_sealed_tail(&mut work, sealed_from, self.dedup_sweep_cursor.load(std::sync::atomic::Ordering::Relaxed));
        for (swept, (date, pid, cur_files)) in work.iter().enumerate() {
            let (date, pid) = (*date, pid);
            // A mid-sweep tick must not run against a closing Foyer cache and hang the
            // graceful drain.
            if self.maintenance_shutdown.is_cancelled() {
                debug!("dedup sweep: shutdown requested, aborting table={}", table_name);
                return Ok(());
            }
            if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                // Advance by the SEALED items covered, since only the sealed tail
                // rotates; counting today's prefix would skip never-swept sealed work.
                self.dedup_sweep_cursor.fetch_add(swept.saturating_sub(sealed_from), std::sync::atomic::Ordering::Relaxed);
                info!(table_name, swept, remaining = total_work - swept, event = "dedup_sweep_truncated");
                break;
            }
            // Incremental skip: a partition certified clean whose live file set is
            // unchanged since that pass cannot have gained dupes — they only arrive in
            // NEW files. Keeps the sweep O(partitions-changed), which the whole-table
            // version guard above cannot do under continuous ingest.
            let fp_key = (pid.clone(), table_name.to_string(), date.to_string());
            let current_fp = partition_file_fp(cur_files);
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
            // BOUND the partition by what is left of the sweep. The deadline check
            // above only gates ADMISSION: without this a single slow partition runs
            // unbounded past it, and since `spawn_cron_job` drops overlapping ticks
            // that wedges the whole dedup job, dirty-bin drain included. A partition
            // abandoned here is re-swept next tick — the pass is idempotent and a
            // truncated one certifies nothing.
            let swept = match deadline.map(|d| d.saturating_duration_since(std::time::Instant::now())) {
                Some(budget) => tokio::time::timeout(budget, self.dedup_partition(table_ref, table_name, pid, date))
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("dedup of {pid}/{date} exceeded the sweep's remaining {budget:?}"))),
                None => self.dedup_partition(table_ref, table_name, pid, date).await,
            };
            match swept {
                Ok((d, complete)) => {
                    self.dedup_backoff.remove(&backoff_key);
                    total_dropped += d;
                    any_ok = true;
                    // Clean-partition fingerprint for the read-side dedup skip: a 0-drop
                    // pass over a file set that is STILL live proves the partition
                    // duplicate-free. A >0 pass marks nothing; the next 0-drop pass
                    // confirms the rewrite held.
                    self.record_certification(table_ref, table_name, pid, date, cur_files, (d, complete)).await?;
                }
                Err(e) => {
                    // Exponential backoff, 10min doubling to a 6h cap, so a failing
                    // partition does not re-fail on every sweep tick.
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
        // Record the version only when a partition ran cleanly AND nothing was
        // rewritten. Recording it after a rewriting pass would make the guard at the
        // top of this function skip the confirming 0-drop pass until some unrelated
        // commit moves the version — leaving exactly the partitions that HAD duplicates
        // as the ones that never get certified.
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

    /// Mirror of `persist_certifications` for the accumulating half of the evidence:
    /// same flag and same lock (one writer at a time across both sidecars).
    fn persist_slice_coverage(&self) {
        if !self.config.maintenance.timefusion_dedup_certification_persist {
            return;
        }
        let _persist = crate::support::lock(&self.dedup_certification_persist_lock);
        let mut entries: Vec<_> = self
            .dedup_slice_coverage
            .iter()
            .map(|entry| {
                let ((project_id, table_name, date), cov) = (entry.key().clone(), entry.value().clone());
                crate::storage::StoredSliceCoverage {
                    proof_version: crate::storage::DedupProofVersion::PhysicalRowOrderV1,
                    project_id,
                    table_name,
                    date,
                    fp: cov.fp,
                    intervals: cov.intervals,
                }
            })
            .collect();
        entries.truncate(crate::storage::PERSIST_CAP);
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::SLICE_COVERAGE, &entries);
    }

    /// Snapshot `dedup_clean_fp` to the data dir, once per sweep rather than per
    /// certification. Best-effort: this is a cache, so a lost write costs a cold start,
    /// never a wrong answer.
    fn persist_certifications(&self) {
        if !self.config.maintenance.timefusion_dedup_certification_persist {
            return;
        }
        let _persist = crate::support::lock(&self.dedup_certification_persist_lock);
        let now_ms = crate::storage::now_unix_ms();
        let mut entries: Vec<_> = self
            .dedup_clean_fp
            .iter()
            .map(|entry| {
                let ((project_id, table_name, date), cert) = (entry.key().clone(), entry.value().clone());
                crate::storage::StoredCertification {
                    proof_version: crate::storage::DedupProofVersion::PhysicalRowOrderV1,
                    project_id,
                    table_name,
                    date,
                    fp: cert.fp,
                    granted_unix_ms: now_ms.saturating_sub(cert.since.elapsed().as_millis() as u64),
                    files: cert.files.to_vec(),
                    stale: cert.stale,
                }
            })
            .collect();
        // Newest first, so the cap drops the oldest — likeliest to be invalidated already.
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.granted_unix_ms));
        entries.truncate(crate::storage::PERSIST_CAP);
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::CERTIFICATIONS, &entries);
    }

    fn persist_dirty_bins(&self) {
        let mut bins: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .map(|entry| {
                let (project_id, table_name, date, bin) = entry.key();
                crate::storage::DirtyBin {
                    project_id: project_id.clone(),
                    table_name: table_name.clone(),
                    date: date.clone(),
                    bin: *bin,
                    width_minutes: self.config.buffer.timefusion_dedup_bin_minutes,
                }
            })
            .collect();
        bins.sort_by(|a, b| (&a.table_name, &a.project_id, &a.date, a.bin).cmp(&(&b.table_name, &b.project_id, &b.date, b.bin)));
        crate::storage::store_sidecar(&self.config.core.timefusion_data_dir, crate::storage::DIRTY_BINS, &bins);
        crate::observability::maintenance_stats().dirty_bin_queue_depth.store(bins.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drop queued dirty bins for tables whose dedup the coordinator owns, since the
    /// cron that drains this queue skips exactly those tables. Returns the number retired.
    pub(crate) fn retire_undrainable_dirty_bins(&self) -> usize {
        let undrainable: Vec<DirtyBinKey> = self
            .dedup_dirty_bins
            .iter()
            .filter(|entry| get_schema(&entry.key().1).is_some_and(|schema| !schema.rollups.is_empty()))
            .map(|entry| entry.key().clone())
            .collect();
        if undrainable.is_empty() {
            return 0;
        }
        for key in &undrainable {
            self.dedup_dirty_bins.remove(key);
        }
        self.persist_dirty_bins();
        warn!(retired = undrainable.len(), event = "dirty_bins_retired_undrainable", "dropped dirty bins for tables the dedup cron does not serve");
        undrainable.len()
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
    /// Reuses the compaction brake's signal, which trips both on an over-threshold
    /// unflushed backlog and while a recent flush FAILURE is inside the brake window.
    fn dedup_flush_healthy(&self) -> bool {
        // The drain's chunk loop never crosses a wave boundary, so it needs the
        // memory brake here; it runs on the HEAVY pool, so a repair sort is not its rival.
        !self.buffered_layer().is_some_and(|layer| layer.is_wal_backlog_over_threshold()) && self.light_optimize_brake().is_none()
    }

    /// Order one drain pass and split off the work it will not do.
    ///
    /// Newest-first (recent partitions are what queries read); cold bins (`date_is_cold`) go last
    /// but are never dropped — this drain is their only physical dedup. Returns
    /// `(ready, deferred_cold)`; `deferred_cold` stays on the queue.
    pub(crate) fn select_drain_bins(mut candidates: Vec<DrainBin>, today: chrono::NaiveDate, after_days: u64, batch: usize) -> (Vec<DrainBin>, Vec<DrainBin>) {
        candidates.sort_by(|a, b| (&b.1, b.2).cmp(&(&a.1, a.2)));
        // An unparseable date sorts cold: it can't be shown to be hot, and the
        // staging call will surface the parse error when it is finally served.
        let (hot, mut cold): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, date, _)| chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok_and(|d| !Self::date_is_cold(today, d, after_days)));
        // Cold bins get a RESERVED half of the batch: hot-first is right, but giving hot the
        // WHOLE batch starves cold forever whenever hot work is continuous.
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
        // Dedup is an OPTIMIZATION (read-side DedupExec keeps results correct), so it must
        // never compete with the persistence path for the per-table commit lock.
        if !flush_healthy() {
            crate::observability::maintenance_stats().dedup_passes_flush_yields.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(table_name, event = "dedup_drain_flush_yield");
            return Ok(());
        }
        use crate::database::compact::bin_micros;
        // Eligible bins classified per table per tick. Large on purpose: per-pass cost is
        // bounded by the (project, date) GROUP count (one batch probe each), not by this.
        const DIRTY_BIN_DRAIN_BATCH: usize = 16384;
        let sealed_before = (Utc::now() - chrono::Duration::hours(2)).timestamp_micros();
        // Today's SEALED bins are eligible; `sealed_before` keeps staging away from the live
        // MemBuffer/late-arrival window.
        let today_date = Utc::now().date_naive();
        let candidates: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .filter_map(|entry| {
                let (project, name, date, bin) = entry.key();
                (name == table_name && (*bin + 1) * bin_micros() <= sealed_before).then(|| (project.clone(), date.clone(), *bin))
            })
            .collect();
        let (ready, deferred) = Self::select_drain_bins(
            candidates,
            today_date,
            self.config.parquet.cold_optimize_after_days(),
            // Fixed, not a knob: the drain self-regulates via flush-health yields, the memory
            // brake and the rewrite semaphore.
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
        // NOT an early return on an empty queue: the certification probes below exist
        // precisely for dates nothing enqueues.
        let has_timestamp_key = schema.dedup_keys.iter().any(|k| k == "timestamp");
        let certify_only = match has_timestamp_key {
            true => self.uncertified_window_dates(&*table.read().await, table_name),
            false => Vec::new(),
        };
        if ready.is_empty() && certify_only.is_empty() {
            return Ok(());
        }
        // One whole-date probe classifies every queued bin of a (project, date) at once; only
        // dup-bearing bins continue into per-bin staging. Probe failure or timeout fails OPEN
        // to the per-bin path.
        let mut ready = if has_timestamp_key {
            // An INSTANT shared by the whole phase, not a per-probe duration: probes run in
            // waves of `rewrite_permits`, so a per-probe ceiling would multiply the budget by
            // the wave count. `checked_add` because `stage_deadline` is `Duration::MAX`
            // wherever the caller means "no per-probe ceiling", which overflows an Instant.
            let probe_deadline = std::time::Instant::now().checked_add(stage_deadline).map_or(pass_deadline, |ceiling| pass_deadline.min(ceiling));
            self.batch_probe_classify(table, table_name, ready, certify_only, probe_deadline).await
        } else {
            ready
        };
        if ready.is_empty() {
            self.persist_dirty_bins();
            return Ok(());
        }
        // STAGING admission is capped SEPARATELY from classification: staging carries a
        // pass-scoped RSS cost, so keeping passes short keeps it bounded. Overflow goes
        // straight back on the queue for the next tick.
        const DIRTY_BIN_STAGE_BATCH: usize = 64;
        // Recent dates get a small RESERVED quota (a dup-bearing bin on today's partition taxes
        // every live query) but must not monopolize the pass; the rest go OLDEST-first so the
        // backlog keeps draining.
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
        // Bins STAGE in parallel and commit in WAVES. The bound on rewrite parallelism is
        // memory: `stage_dedup_chunk` takes a `maintenance_rewrite_sem` permit around its
        // (pool-invisible) Arrow materialization, so `buffer_unordered(permits)` keeps
        // in-flight staging matched to it rather than unbounded.
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        // A wave's units all sit in memory as Delta actions only (their parquet
        // is already in R2), so the cap is about commit size, not memory.
        const DEDUP_WAVE_UNITS: usize = 8;
        let mut staging = futures::stream::iter(ready.into_iter().map(|(project_id, date, bin)| async move {
            let key: DirtyBinKey = (project_id.clone(), table_name.to_string(), date.clone(), bin);
            // STOP ADMITTING once the pass is out of budget, and bound each bin by what is left
            // rather than by `stage_deadline` alone — the pass holds `maintenance_job_sem`, so
            // over-running costs every tick behind it. Bailing here leaves the bin QUEUED (the
            // dequeue below has not run yet), so the next tick serves it.
            let remaining = pass_deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            // No persist here — it would make the drain O(queue x batch) in fsync I/O. Crash
            // direction is safe: an unpersisted dequeue reappears after restart and re-dedups
            // (idempotent). End-of-pass persists.
            self.dedup_dirty_bins.remove(&key);
            crate::observability::maintenance_stats().dirty_bin_eligible.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(project_id, table_name, date, bin, event = "dirty_bin_dequeued");
            let started = std::time::Instant::now();
            let staged = match chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d") {
                // Timing out a bin discards its staged work (uploaded parquet is uncommitted
                // and falls to VACUUM) and retries it next pass; the Err lands in the
                // ordinary failure arm below (requeue + warn).
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
                        // A bin with nothing to rewrite never enters a wave, so count its
                        // drain here or the processed metric reads 0 while the queue empties.
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

    /// `(project, date)` pairs inside the read window that hold files but carry no
    /// live certification — the dates a dirty-bin-driven probe can never reach.
    ///
    /// A scan only sheds `DedupExec` when EVERY date it reads is granted, and the batch probe
    /// only visits groups with queued bins, so fully-processed sealed dates need this path.
    /// TODAY is excluded: a live partition's fingerprint moves under ingest, so
    /// `record_certification` would refuse by construction.
    pub(crate) fn uncertified_window_dates(&self, table: &DeltaTable, table_name: &str) -> Vec<(String, String)> {
        const CERTIFY_WINDOW_DAYS: i64 = 14;
        // Each probe is a whole-date key-only scan sharing the pass deadline, and a group
        // reached with no budget left returns without probing.
        const CERTIFY_PROBES_PER_PASS: usize = 64;
        let today = Utc::now().date_naive();
        let mut by_project: HashMap<String, (usize, Vec<String>)> = Default::default();
        for back in 1..=CERTIFY_WINDOW_DAYS {
            let date = (today - chrono::Duration::days(back)).to_string();
            for (project, files) in Self::partition_files_by_pid(table, &format!("date={date}")).into_iter().flatten().filter(|(_, files)| !files.is_empty()) {
                let key = (project.clone(), table_name.to_string(), date.clone());
                // Stale entries ARE re-probed: staleness means per-file evidence
                // outlived its fingerprint, not that the date is dirty.
                let uncertified = self.dedup_clean_fp.get(&key).is_none_or(|entry| entry.value().stale);
                // A date already probed dirty at THIS exact file set cannot become clean
                // without a commit, and a commit moves the fingerprint — re-probing is waste.
                let known_dirty = self.dedup_probe_declined.get(&key).is_some_and(|entry| *entry.value() == partition_file_fp(&files));
                if uncertified && !known_dirty {
                    let entry = by_project.entry(project).or_default();
                    entry.0 += files.len();
                    entry.1.push(date.clone());
                }
            }
        }
        // PROJECT-MAJOR, busiest first by file count: a scan sheds `DedupExec` only when EVERY
        // date in its window is granted, so grants scattered one-per-project buy nothing.
        // Sorting by fewest-remaining-dates instead starves exactly the busy projects whose
        // queries hurt, because more data looks further from done.
        let mut projects: Vec<_> = by_project.into_iter().collect();
        projects.sort_by(|(a_project, (a_files, _)), (b_project, (b_files, _))| b_files.cmp(a_files).then(a_project.cmp(b_project)));
        projects
            .into_iter()
            .flat_map(|(project, (_, dates))| dates.into_iter().map(move |date| (project.clone(), date)))
            .take(CERTIFY_PROBES_PER_PASS)
            .collect()
    }

    /// Certification-only pass: probe uncertified window dates and grant, with no
    /// dirty-bin drain involved.
    ///
    /// Separate from the drain because the drain's cron skips every rollup-declaring table.
    /// Certification is read-only and key-only, so it is safe for coordinator-owned tables
    /// even though their rewrites are not this cron's business.
    pub(crate) async fn run_certification_pass(&self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, deadline: std::time::Instant) {
        if !schema_or_default(table_name).dedup_keys.iter().any(|key| key == "timestamp") {
            return;
        }
        let candidates = self.uncertified_window_dates(&*table.read().await, table_name);
        if candidates.is_empty() {
            return;
        }
        self.batch_probe_classify(table, table_name, Vec::new(), candidates, deadline).await;
    }

    fn note_probe_cost(&self, table_name: &str, elapsed: std::time::Duration) {
        note_probe_cost_into(&self.dedup_probe_cost_ms, table_name, u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX));
    }

    /// Runs the batch probe over each (project, date) with ≥2 queued bins and
    /// strips the probe-clean bins out of `ready`, consuming them. Group keys
    /// are dequeued BEFORE the probe so dirtiness enqueued while it runs
    /// re-queues the bin (the same ordering the per-bin path relies on). A
    /// singleton keeps the per-bin path — its bin-scoped probe prunes to ten
    /// minutes of files where the whole-date probe scans them all.
    pub(crate) async fn batch_probe_classify(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, ready: Vec<(String, String, i64)>, certify_only: Vec<(String, String)>,
        deadline: std::time::Instant,
    ) -> Vec<(String, String, i64)> {
        use std::sync::atomic::Ordering::Relaxed;
        // No deadline left classifies nothing; returning leaves every bin queued for a tick
        // that can afford it.
        let budget = deadline.saturating_duration_since(std::time::Instant::now());
        if budget.is_zero() {
            return ready;
        }
        use itertools::Itertools;
        let groups = ready.iter().map(|(project, date, bin)| ((project.clone(), date.clone()), *bin)).into_group_map();
        // Probes run concurrently, bounded by the rewrite permits. Results are key-only
        // aggregates, but each probe builds a provider + eager snapshot over a whole date's
        // files (allocator churn jemalloc retains as RSS), so groups per pass are capped too.
        // The rest keep their queue entries and classify on later ticks.
        const BATCH_PROBE_GROUPS: usize = 16;
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        let mut groups: Vec<((String, String), Vec<i64>)> =
            groups.into_iter().filter(|((_, date), bins)| bins.len() >= 2 && chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok()).collect();
        // Recent dates first (dup-bearing bins there force read-side DedupExec on every query);
        // largest group as the tiebreak so each provider built retires the most bins.
        groups.sort_by(|((_, da), a), ((_, db), b)| db.cmp(da).then(b.len().cmp(&a.len())));
        groups.truncate(BATCH_PROBE_GROUPS);
        // Then the dates NOTHING enqueues: they carry no bins and are probed purely for the
        // certification (the same closure handles them — an empty bin list clears nothing).
        //
        // INTERLEAVED, not appended: every probe here shares ONE deadline and takes whatever
        // is left of it, so position IS budget, and certify-only groups are the only ones that
        // can GRANT (a group with dirty bins declines by construction). `2 *` keeps the dirty
        // class's full `BATCH_PROBE_GROUPS` volume while giving certify-only an equal share.
        let certify_only: Vec<_> = certify_only.into_iter().map(|group| (group, Vec::new())).collect();
        let observed = self.dedup_probe_cost_ms.get(table_name).map(|ms| std::time::Duration::from_millis(*ms));
        let groups = interleave_probe_groups(groups, certify_only, probe_groups_for_budget(permits, budget, observed, 2 * BATCH_PROBE_GROUPS));
        // BEFORE the probes: every other line in this phase is emitted on completion, so a
        // phase that does not complete would print nothing at all.
        info!(
            table_name,
            groups = groups.len(),
            budget_secs = deadline.saturating_duration_since(std::time::Instant::now()).as_secs(),
            // `probe_cost_ms=0` means the estimate is still cold and `groups` is the fixed cap.
            probe_cost_ms = self.dedup_probe_cost_ms.get(table_name).map_or(0, |ms| *ms),
            event = "dedup_batch_probe_start"
        );
        let clean: HashSet<(String, String, i64)> = futures::stream::iter(groups.into_iter().map(|((project, date), bins)| async move {
            // What is left of the PHASE at the moment this probe starts, so waves behind the
            // permit limit share one budget instead of each claiming it whole. Zero left means
            // the group was never examined: leave its bins queued rather than dequeuing them.
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Vec::new();
            }
            for bin in &bins {
                self.dedup_dirty_bins.remove(&(project.clone(), table_name.to_string(), date.clone(), *bin));
            }
            // Captured BEFORE the probe so `record_certification`'s fingerprint compare can
            // reject the grant if a commit lands while it runs. Reading the file set
            // afterwards would compare the new set against itself and agree unconditionally.
            let pre = {
                let table = table.read().await;
                Self::partition_files_by_pid(&table, &format!("date={date}")).ok().and_then(|mut by_pid| by_pid.remove(&project)).unwrap_or_default()
            };
            let started = std::time::Instant::now();
            match tokio::time::timeout(remaining, self.probe_dup_bins(table, table_name, &project, &date)).await {
                Ok(Ok(dup_bins)) => {
                    self.note_probe_cost(table_name, started.elapsed());
                    let stats = crate::observability::maintenance_stats();
                    let cleared: Vec<_> = bins.iter().filter(|b| !dup_bins.contains(b)).map(|b| (project.clone(), date.clone(), *b)).collect();
                    stats.dirty_bin_processed.fetch_add(cleared.len() as u64, Relaxed);
                    stats.dirty_bin_batch_probe_clean.fetch_add(cleared.len() as u64, Relaxed);
                    info!(project, table_name, date, queued = bins.len(), clean = cleared.len(), event = "dedup_batch_probe");
                    // A probe that finds NO duplicate-bearing bin has proved the whole
                    // partition duplicate-free — the SAME predicate a zero-drop rewrite
                    // establishes — so it goes through `record_certification` rather than a
                    // second rule that could drift from it.
                    if dup_bins.is_empty()
                        && let Ok(parsed) = chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d")
                    {
                        match self.record_certification(table, table_name, &project, parsed, &pre, (0, true)).await {
                            // Debounced: up to BATCH_PROBE_GROUPS grants land per pass and
                            // `store_sidecar` re-serializes the whole ledger each time.
                            Ok(Some(_)) => self.persist_certifications_debounced(),
                            Ok(None) => {}
                            Err(error) => warn!(project, table_name, date, %error, event = "dedup_batch_probe_certify_failed"),
                        }
                    } else if !dup_bins.is_empty() && !pre.is_empty() {
                        // Memoise the DECLINE against the file set that produced it: the date
                        // cannot become clean without a commit, and a commit moves the
                        // fingerprint, so re-probing before then would crowd out unexamined
                        // candidates.
                        self.dedup_probe_declined.insert((project.clone(), table_name.to_string(), date.clone()), partition_file_fp(&pre));
                        metrics::counter!(scan_metric_names::CERT_PROBE_DECLINED).increment(1);
                        // HOW dirty, not just that it is dirty: this decides the removal
                        // mechanism and cannot be measured from outside, because a psql probe
                        // reads through `DedupExec` and sees duplicates already collapsed.
                        metrics::counter!(scan_metric_names::CERT_DECLINED_DIRTY_BINS).increment(dup_bins.len() as u64);
                        info!(project, table_name, date, dirty_bins = dup_bins.len(), event = "dedup_certify_declined");
                    }
                    cleared
                }
                Ok(Err(error)) => {
                    warn!(project, table_name, date, %error, event = "dedup_batch_probe_failure");
                    Vec::new()
                }
                Err(_) => {
                    // Feed the cost EMA on timeout too: completions are biased toward cheap
                    // dates (the expensive ones time out), so an EMA fed only by them
                    // under-estimates and re-admits the wave this exists to prevent. The
                    // elapsed time is only a LOWER bound, but a less-wrong one than omitting it.
                    self.note_probe_cost(table_name, started.elapsed());
                    crate::observability::maintenance_stats().dedup_probe_timeouts.fetch_add(1, Relaxed);
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
    /// A pass can outlive its start-of-pass health check, and dedup must not compete with
    /// persistence for the commit lock — but one transient unhealthy sample must not forfeit the
    /// whole batch. If flush does not recover, requeue the wave and return false so the pass
    /// stops committing.
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
        use itertools::Itertools;
        use std::sync::atomic::Ordering::Relaxed;
        let markers: Vec<String> = units.iter().filter_map(|u| u.dedup.as_ref()).map(|d| format!("date={}/", d.date)).sorted().dedup().collect();
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
            let (project_id, _, date, bin) = &key;
            stats.dirty_bin_requeued.fetch_add(1, Relaxed);
            warn!(project_id, table_name, date, bin, event = "dirty_bin_requeued");
            self.dedup_dirty_bins.insert(key, ());
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
        // Deadline per bin STAGING attempt, not per pass. Sized for the WORST legitimate bin,
        // not the typical one — too tight and such bins time out every pass forever — while
        // still bounding a hung object-store read.
        const DEDUP_BIN_STAGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(3600);
        let note = |what: &str, elapsed: std::time::Duration, result: Result<()>| {
            let stats = crate::observability::maintenance_stats();
            match result {
                Ok(()) if elapsed > DEDUP_WARN => {
                    warn!("{what} for {label} took {elapsed:?} (exceeds {DEDUP_WARN:?} warning threshold)");
                    stats.dedup_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(()) => {}
                Err(e) => {
                    stats.dedup_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error!("{what} failed for {label}: {e}");
                }
            }
        };
        let drained = self.dedup_dirty_bins_for_table(table, table_name, &|| self.dedup_flush_healthy(), DEDUP_BIN_STAGE_DEADLINE, drain_deadline).await;
        note("Dirty-bin dedup", t0.elapsed(), drained);
        if self.config.maintenance.timefusion_dedup_sweep_fallback {
            let t0 = std::time::Instant::now();
            // The sweep is the pass's unbounded half (see `dedup_sweep`); the
            // drain above is bounded per bin and is the work worth finishing.
            let swept = self.dedup_sweep(table, table_name, dedup_key, Some(sweep_deadline)).await;
            note("Dedup fallback sweep", t0.elapsed(), swept);
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

    /// The taper: full target at or below half full, falling linearly to HALF the
    /// target when the lane's pool is full.
    ///
    /// The clamp makes an out-of-range or NaN occupancy safe — the input is a live pool
    /// reading divided by a configured size.
    fn pressure_factor(occupancy: f64) -> f64 {
        (1.0 - (occupancy - 0.5).max(0.0)).clamp(0.5, 1.0)
    }

    /// Shrink a unit's target as its lane's pool fills. SHADOW BY DEFAULT: the reduction is
    /// always computed and counted, but only returned when
    /// `timefusion_maintenance_pressure_scaling` is on.
    fn pressure_scaled_target(&self, base: i64, pass: TailPass) -> i64 {
        let (env, pool) = match pass {
            TailPass::Pack => (self.light_optimize_runtime_env(), self.pack_pool_bytes()),
            TailPass::Repair => (self.repair_runtime_env(), self.repair_pool_bytes()),
        };
        let occupancy = env.memory_pool.reserved() as f64 / (pool.max(1) as f64);
        let factor = Self::pressure_factor(occupancy);
        if factor >= 1.0 {
            return base;
        }
        let scaled = ((base as f64) * factor) as i64;
        let stats = crate::observability::maintenance_stats();
        stats.pressure_scale_engaged.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        stats.pressure_scale_bytes_withheld.fetch_add((base - scaled).max(0) as u64, std::sync::atomic::Ordering::Relaxed);
        let applied = self.config.maintenance.timefusion_maintenance_pressure_scaling;
        debug!(?pass, occupancy, factor, base, scaled, applied, event = "maintenance_pressure_scale");
        if applied { scaled } else { base }
    }

    /// The admission policy for one tail pass. `budget` is an input, not a
    /// detail: a repair pass's reach is derived from how long it may run.
    fn tail_pass_policy<'a>(&'a self, pass: TailPass, budget: std::time::Duration, repair_dates: &'a [String]) -> HotBinPolicy<'a> {
        // A packing bin is only worth assembling if the tick can finish it. Repair takes ONE
        // file and is bounded by `repair_max_bytes` below, so its target stays the configured
        // value (where it only sets the converged/sorted-run thresholds).
        let target_size = match pass {
            TailPass::Pack => pack_target_bytes(self.config.maintenance.timefusion_light_optimize_target_size, budget),
            TailPass::Repair => self.config.maintenance.timefusion_light_optimize_target_size,
        };
        let target_size = self.pressure_scaled_target(target_size, pass);
        HotBinPolicy {
            repair_dates,
            target_size,
            min_files: self.config.maintenance.timefusion_compact_min_files,
            sorted_run_cap: target_size / 2,
            // On a REPAIR pass the reach is what the budget can actually finish, not the
            // hot-tick knob: `timefusion_repair_max_file_bytes` sizes a five-minute tick, and
            // a file the pass will not admit is never a candidate at any budget or cadence.
            repair_max_bytes: match pass {
                TailPass::Pack => self.config.maintenance.timefusion_repair_max_file_bytes as i64,
                TailPass::Repair => repair_reach_bytes(self.config.maintenance.timefusion_repair_max_file_bytes as i64, budget),
            },
            pass,
            verified_sorted: &self.repair_verified_sorted,
            failures: &self.repair_failures,
        }
    }

    /// One metadata walk under the read lock — the three planning sites' shared shape.
    async fn select_hot_bins(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, schema: &crate::schema::TableSchema, today_str: &str, policy: &HotBinPolicy<'_>,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let table = table_ref.read().await;
        Self::select_all_hot_bins(&table, schema, today_str, policy)
    }

    /// Drop verified-sorted suspects and re-select until what remains is real repair work.
    ///
    /// Load-bearing: admission offers every un-verified sealed file as a suspect because the
    /// sorted-run tag can lie, so only the footer decides. Without re-selection a project whose
    /// selected bin turns out to be correctly sorted drops out for the whole pass.
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
            let next = self.select_hot_bins(table_ref, schema, today_str, policy).await?;
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
        let mut planned = self.select_hot_bins(table_ref, schema, today_str, policy).await?;
        // Rotation cursor: start where the last truncated tick stopped so the
        // same tail is never skipped twice in a row (a truncated tick otherwise
        // always serves the same debt-ordered prefix).
        let cursor = self.light_optimize_cursor.swap(0, Relaxed);
        if cursor > 0 && cursor < planned.len() {
            planned.rotate_left(cursor);
        }
        // Clearing a project's suspect must not cost it the tick: re-plan after each clear and
        // keep going. `repair_verified_sorted` makes every re-plan skip what was just checked,
        // so a tick walks a project's candidates until it finds real work or runs out.
        let mut planned = self.reselect_until_real_work(table_ref, table_name, schema, today_str, policy, planned).await?;
        // Serve the most RECENT poison first, across projects: one footer-less file voids the
        // scan ordering for every query window that reaches it, so a recent file breaks 14-
        // and 30-day queries while an old one breaks only rarely-run long windows.
        if policy.pass == TailPass::Repair {
            planned.sort_by(|a, b| repair_bin_date(&b.1).cmp(repair_bin_date(&a.1)));
        }
        Ok(planned)
    }

    /// TEST SEAM: plan one tail pass, stage its first bin, and ABANDON it (staged parquet plus
    /// an intent line, no commit) — the state a process killed mid-rewrite leaves behind. Goes
    /// through the real planner because resume matches on input-set equality. Returns the
    /// abandoned bin's `(project_id, input paths)`.
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
        let outcome = self
            .stage_hot_bin(table_ref, table_name, schema, &project_id, files.clone(), HotStageOptions { pass, runtime_env: None, light_permit: None })
            .await?;
        Ok(matches!(outcome, BinOutcome::Staged(_)).then_some((project_id, files)))
    }

    /// Hot-tail compaction for one table: plan-once, rewrite-parallel, commit-once waves.
    ///
    /// One tag-first metadata walk plans a bin for every hot project (`select_all_hot_bins`), each
    /// round's bins are rewritten to staged parquet in parallel, and the whole round lands in one
    /// `CommitBuilder` transaction.
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
        // Take the process-wide repair permit FIRST — before planning, not just before
        // staging — or two tables' repair passes duplicate the suspect walk and both stage
        // into one shared light pool. `try_acquire`, not `acquire`, so the loser SKIPS its
        // tick instead of queueing behind a multi-hour budget.
        let _repair_permit = match pass {
            TailPass::Pack => None,
            TailPass::Repair => {
                let Ok(permit) = Arc::clone(&self.repair_pass_permit).try_acquire_owned() else {
                    crate::observability::maintenance_stats().repair_ticks_yielded.fetch_add(1, Relaxed);
                    info!(table_name, event = "repair_pass_yielded_to_another_table");
                    return Ok(());
                };
                Some(permit)
            }
        };
        // `crate::support`, not `Utc::now()`: the hot tail scopes itself to TODAY's partition
        // and an event-time seal window, so a wall-clock read makes the pass unreachable from
        // the virtual-time e2e harness. In production the clock IS the wall clock.
        let today = chrono::DateTime::from_timestamp_micros(crate::support::now_micros()).map(|d| d.date_naive()).unwrap_or_else(|| Utc::now().date_naive());
        let today_str = today.to_string();
        let repair_dates = self.repair_dates(today, pass);
        let budget = self.tail_pass_tick_budget(pass);
        let policy = self.tail_pass_policy(pass, budget, &repair_dates);
        let schema = schema_or_default(table_name);
        // Scope the warm/evict diff to the dates this pass actually rewrote — handing a repair
        // wave today's marker would diff the wrong partition. Shared by every commit site.
        let markers: Vec<String> = match pass {
            TailPass::Pack => vec![format!("date={today_str}/")],
            TailPass::Repair => repair_dates.iter().map(|d| format!("date={d}/")).collect(),
        };
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

        // COMMIT RECOVERED REWRITES FIRST, outside the round. A resumed bin is already staged
        // and verified row-exact, so it needs nothing but a commit — and `round_robin_bins`
        // awaits the whole round's staging before committing anything, which under deploy
        // churn means the recovered rewrite dies with the round every pass.
        let resumed: Vec<(String, StagedBin)> = {
            let plan_guard = plan.lock().await;
            let mut found = Vec::new();
            for (project_id, files) in plan_guard.iter() {
                found.extend(self.resumable_staged_bin(table_ref, table_name, project_id, files).await.map(|bin| (project_id.clone(), bin)));
            }
            found
        };
        for (project_id, bin) in resumed {
            // Drop it from the plan: its inputs are about to stop being live, so
            // re-staging them this tick would rewrite files the commit removed.
            plan.lock().await.remove(&project_id);
            let failed = self.commit_wave(table_ref, table_name, &markers, false, vec![bin], 0).await.failed.len();
            info!(table_name, project_id, failed, event = "resumed_bin_committed_early");
        }

        // Repair takes at most HALF the rewrite slots, never the pool. Both passes share
        // `light_rewrite_sem`, so an uncapped repair wave holding every permit for its whole
        // (much longer) budget starves the 5-minute packing tick — and packing is continuous
        // and latency-critical where repair is a finite background backlog.
        let k = self.config.derived.light_optimize_k(project_ids.len());
        let concurrency = match pass {
            TailPass::Pack => k,
            TailPass::Repair => (k / 2).max(1),
        };
        // Bound total rounds so a large backlog can't wedge the tick even if the
        // wall-clock budget is raised.
        let max_waves = max_waves(pass);
        // Both bounds matter: the nominal budget sizes a pass in isolation, the tick deadline
        // stops N tables from each claiming a full budget.
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
                    // Log WHICH file: a repair bin is one file, and diagnosing a stalled
                    // repair needs the path, not just a count.
                    let selected = files.first().filter(|_| pass == TailPass::Repair).map(String::as_str).unwrap_or("");
                    info!(table_name, project_id, date = %today, selected_files = files.len(), selected, round, event = "light_optimize_tail_selected");
                    // Admission picked this file off the ABSENT sort tag, which is only a
                    // suspicion, so read the footer before spending minutes rewriting it.
                    // `Retry`, NOT `Converged`: clearing a suspect leaves this project's next
                    // candidate unexamined, and `Converged` would drop it for the rest of the
                    // tick — one cleared suspect per tick.
                    if pass == TailPass::Repair && self.repair_bin_already_sorted(table_ref, &files).await {
                        return (project_id, Ok(BinOutcome::Retry));
                    }
                    // Did a previous attempt already WRITE this exact rewrite? Commit it
                    // instead of redoing it. Selection is deterministic given the snapshot, so
                    // the first pass after a restart re-selects the killed pass's files and
                    // finds its own abandoned output. NOT gated on `TailPass::Repair` — a
                    // packing bin is equally data-preserving; it just usually misses.
                    if let Some(bin) = self.resumable_staged_bin(table_ref, table_name, &project_id, &files).await {
                        return (project_id, Ok(BinOutcome::Staged(bin)));
                    }
                    // Bound the bin by what is LEFT of the tick, or one slow bin runs past the
                    // budget and the invocation never returns, so the cron cannot re-plan.
                    // Discarding a timed-out bin is safe: its parquet is uploaded but
                    // uncommitted, so it falls to VACUUM and the next tick re-selects it.
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        // Out of budget before we began: report nothing staged rather than a
                        // failure, so the truncation path accounts for it.
                        return (project_id, Ok(BinOutcome::Converged));
                    }
                    // Publish the sort while it runs so `timefusion_stats` can distinguish a
                    // grinding repair from a wedged one; the guard decrements on every exit
                    // path including the timeout.
                    let _in_flight = (pass == TailPass::Repair).then(|| in_flight_guard(&crate::observability::maintenance_stats().repair_bins_in_flight));
                    let staged = tokio::time::timeout(
                        left,
                        self.stage_hot_bin(table_ref, table_name, schema, &project_id, files, HotStageOptions { pass, runtime_env: None, light_permit: None }),
                    )
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("hot bin staging exceeded the {left:?} left in the tick budget")));
                    (project_id, staged)
                }
            },
            |bins, round| {
                let (plan, today_str, policy, markers) = (&plan, today_str.as_str(), &policy, markers.as_slice());
                async move {
                    let staged = bins.len();
                    let failed = self.commit_wave(table_ref, table_name, markers, false, bins, round).await.failed.len();
                    // Round 0 only: one bin per project, so this stays comparable to
                    // `projects_planned`.
                    if round == 0 {
                        crate::observability::maintenance_stats().light_optimize_projects_completed.fetch_add((staged - failed.min(staged)) as u64, Relaxed);
                    }
                    // Re-plan the NEXT wave from the just-committed snapshot: outputs are
                    // tagged sorted runs and excluded from re-selection, so this yields each
                    // project's next slice, never the run this wave wrote. Skipped entirely
                    // when no further round can run.
                    if round + 1 < max_waves && std::time::Instant::now() < deadline {
                        let next = self.select_hot_bins(table_ref, schema, today_str, policy).await.unwrap_or_default();
                        // Filter the RE-PLAN too, not just round 0, or later rounds re-select
                        // unverified suspects and fall back to one clear per round.
                        let next = self.reselect_until_real_work(table_ref, table_name, schema, today_str, policy, next).await.unwrap_or_default();
                        *plan.lock().await = next.into_iter().collect();
                    }
                    failed
                }
            },
        )
        .await;
        // Checkpoint after the tick's final commit rather than per N versions: wave commits
        // are rare, so a version-count cadence would checkpoint far too seldom, and replay-tail
        // length is a top CPU cost.
        self.checkpoint_after_waves(table_ref, table_name).await;
        anyhow::ensure!(failed == 0, "Light optimize failed for {failed} hot bin(s)");
        Ok(())
    }

    /// Stage one bin's rewrite: read the selected files, sort by the schema keys, write staged
    /// parquet, and return the `Remove+Add` actions for the wave commit. No Delta commit and no
    /// table lock, so waves parallelize instead of serializing behind the log. Uncommitted parquet is
    /// invisible to readers, and failures clean up their own staged files. `Retry` means the bin's
    /// files were rewritten concurrently; the project stays in rotation and the next re-plan serves
    /// a fresh bin. `Converged` means nothing worth staging. `BudgetBusy` means another repair
    /// rewrite holds the byte budget — re-approach on the holder's clock (minutes), not the
    /// stale-selection clock.
    pub(crate) async fn stage_hot_bin(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, schema: &crate::schema::TableSchema, project_id: &str, files: Vec<String>,
        options: HotStageOptions,
    ) -> Result<BinOutcome<StagedBin>> {
        use deltalake::{delta_datafusion::TableProviderBuilder, kernel::Action, writer::DeltaWriter};
        let HotStageOptions { pass, runtime_env, light_permit } = options;
        // One read-lock, one table clone per bin: the pinned scan snapshot and
        // the writer's staging table both derive from it.
        let staging_table = { table_ref.read().await.clone() };
        let (snapshot, log_store) = (Arc::new(staging_table.snapshot()?.snapshot().clone()), staging_table.log_store());
        // Map paths to Add actions in the SAME snapshot the scan reads, so the
        // Remove tombstones carry the exact fields of the files we rewrote.
        let wanted: HashSet<&str> = files.iter().map(String::as_str).collect();
        let targets = dedup_adds_by_path(snapshot.log_data().iter().filter(|f| wanted.contains(f.path().as_ref())).map(|f| add_action(&f)), table_name);
        if targets.len() != files.len() {
            // WARN, not debug: at debug this silent abandonment is invisible at
            // prod's RUST_LOG=info and looks identical to never being selected.
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
        // The wave engine's OWN permit — NEVER maintenance_rewrite_sem, which is
        // for heavy rewrites and would cap waves at its 2 permits. Wave staging
        // is already bounded by K and sized by the light pool slice.
        let permit_wait = std::time::Instant::now();
        let _light_permit = match light_permit {
            Some(permit) => permit,
            // Repair queues on its OWN one-permit semaphore: its bins are whole
            // files, so two at once is a pool exhaustion, and it must not spend
            // one of the hygiene permits it would then starve. It must NOT WAIT
            // on it — waiting parks a coordinator worker for the length of
            // somebody else's multi-minute rewrite; requeueing is work-conserving.
            None if pass == TailPass::Repair => {
                // Priced in decoded MiB, CLAMPED to the whole budget so a bin
                // larger than the budget still takes everything and runs alone.
                let budget_mib = self.config.derived.repair_rewrite_budget_mib();
                let want_mib = u32::try_from(estimated_decoded_bytes(targets.iter().map(|a| a.size).sum::<i64>()) / (1024 * 1024))
                    .unwrap_or(u32::MAX)
                    .clamp(1, u32::try_from(budget_mib).unwrap_or(u32::MAX));
                let Ok(permit) = Arc::clone(&self.repair_rewrite_sem).try_acquire_many_owned(want_mib) else {
                    crate::observability::maintenance_stats().compaction_permits_unavailable.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    info!(
                        table_name,
                        project_id,
                        want_mib,
                        budget_mib,
                        event = "repair_rewrite_permit_busy",
                        "other repair rewrites hold the byte budget; requeueing rather than parking a worker"
                    );
                    return Ok(BinOutcome::BudgetBusy);
                };
                permit
            }
            None => Arc::clone(&self.light_rewrite_sem).acquire_owned().await.map_err(|e| anyhow::anyhow!("light rewrite semaphore closed: {e}"))?,
        };
        let permit_wait_ms = permit_wait.elapsed().as_millis() as u64;
        let stage_started = std::time::Instant::now();
        let bytes_in: i64 = targets.iter().map(|a| a.size).sum();
        let decoded_in = estimated_decoded_bytes(bytes_in);
        let rows_in: u64 = targets.iter().filter_map(add_row_count).sum();
        // THIS bin's measured row width — feeds both the scan batch size and the
        // output's row-group cap.
        let measured_bytes_per_row = decoded_in.checked_div(rows_in).filter(|width| *width > 0);
        let batch_size = batch_rows_for(decoded_in, rows_in, self.config.maintenance.timefusion_maintenance_batch_target_bytes).to_string();
        // Emitted BEFORE the rewrite, because the interesting bins are the ones
        // that never reach `wave_bin_staged`.
        info!(table_name, project_id, selected_files = targets.len(), bytes_in, permit_wait_ms, event = "wave_bin_staging_started");
        let stage_store = staging_table.log_store().object_store(None);
        let mut adds: Vec<Action> = Vec::new();
        // Hoisted out of the staging block so the StagedBin below can carry
        // `sorted` to `mark_written_sorted`; recomputing it there would duplicate
        // the predicate that decides the footer.
        let order_by = schema_order_by_clause(schema);
        let sorted = !order_by.is_empty();
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
            // fails mid-scan. A bin that already exhausted the pool retries with
            // fewer sort partitions — the unspillable merge exec is per-partition.
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
                        Some(&batch_size),
                        Some(UncappedSort { partitions: 1, reservation_bytes: Some(32 * 1024 * 1024) }),
                    )
                },
            );
            let ctx = datafusion::prelude::SessionContext::new_with_state(state);
            // Unique per staging: the cached session state's clone SHARES its
            // catalog, so a fixed name collides across concurrent stagings.
            // Deregistered right after the read so the shared catalog can't
            // accumulate entries.
            let bin_table = format!("hot_bin_{}", uuid::Uuid::new_v4().simple());
            ctx.register_table(&bin_table, Arc::new(provider))?;
            // ORDER BY in the PLAN, streamed — an in-process Arrow lexsort
            // refuses to sort past `SORT_SKIP_BYTES` and silently reports
            // `sorted=false`, leaving the footer's declared ordering dishonest.
            //
            // Slices are emitted in the output's OWN sort direction into one
            // writer, so the concatenation stays globally sorted. Slicing is
            // declined unless the table sorts on a leading timestamp and the bin
            // has a usable non-null range — a NULL would sort outside every
            // slice and be silently dropped.
            let lead = schema.sorting_columns.first();
            let slice_target = coordinator_slice_target(pass, targets.len(), bytes_in);
            let slice_col = lead.filter(|_| sorted && slice_target.is_some()).map(|c| c.name.clone());
            let slices: Vec<String> = match slice_col {
                None => Vec::new(),
                Some(col) => {
                    // DECODED bytes on both sides; sizing in compressed bytes
                    // oversizes every slice by the compression ratio.
                    let want = repair_slice_want(bytes_in, slice_target.expect("slice column requires a target"));
                    let probe = format!(
                        "SELECT min(\"{col}\") AS lo, max(\"{col}\") AS hi, sum(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS nulls FROM {bin_table}"
                    );
                    // Bounds come back as raw i64 micros but the column is a
                    // timestamp, and DataFusion will not coerce a bare integer.
                    // `{:?}` on the Arrow type is `arrow_cast`'s type syntax; no
                    // type in hand means decline slicing.
                    let cast_ty = ctx
                        .table_provider(bin_table.as_str())
                        .await
                        .ok()
                        .and_then(|p| p.schema().field_with_name(&col).ok().map(|f| format!("{:?}", f.data_type())));
                    match (want > 1).then_some(()).and(cast_ty).zip(bin_time_range(&ctx, &probe).await) {
                        Some((ty, (lo, hi))) if hi > lo => {
                            // Equal-ROW cuts where they can be had; the equal-TIME
                            // split only bounds memory when rows are spread evenly.
                            let cuts = repair_slice_cuts(&ctx, bin_table.as_str(), &col, want).await;
                            let mut bounds = if cuts.is_empty() { repair_slice_bounds(lo, hi, want) } else { repair_bounds_from_cuts(lo, hi, &cuts) };
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
            // The writer gets this bin's measured row width so its row groups
            // are capped by decoded bytes rather than by an estimate.
            let writer_properties =
                self.create_writer_properties_measured(schema, self.config.parquet.timefusion_zstd_compression_level, sorted, measured_bytes_per_row);
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
            // Phase decomposition: `upstream` is scan+sort (time blocked in
            // `stream.next()`), `write` is encode+upload, `plan` is optimisation.
            let (mut t_plan, mut t_upstream, mut t_write) = (std::time::Duration::ZERO, std::time::Duration::ZERO, std::time::Duration::ZERO);
            for predicate in &passes {
                let planned_at = std::time::Instant::now();
                let plan = ctx.sql(&format!("SELECT * FROM {bin_table}{predicate}{order_by}")).await?.create_physical_plan().await?;
                t_plan += planned_at.elapsed();
                // Held for the life of the stream: the sort below it can run for
                // most of the unit without emitting a row.
                let _progress = PlanProgress::watch(Arc::clone(&plan));
                let mut stream = datafusion::physical_plan::execute_stream(plan, ctx.task_ctx())?;
                loop {
                    let pulled_at = std::time::Instant::now();
                    let next = stream.next().await;
                    t_upstream += pulled_at.elapsed();
                    let Some(batch) = next else { break };
                    let batch = cast_variant_columns_to_binary(batch?)?;
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    rows_staged += batch.num_rows();
                    // The unit is alive as long as this moves; `run_until_idle`
                    // reads it instead of a fixed budget.
                    note_unit_progress(batch.num_rows());
                    let casted = deltalake::kernel::schema::cast_record_batch(&batch, target_schema.clone(), true, true)?;
                    let wrote_at = std::time::Instant::now();
                    writer.write(casted).await.map_err(|e| anyhow::anyhow!("hot bin stage: {e}"))?;
                    t_write += wrote_at.elapsed();
                    // Cut the file at the ceiling instead of buffering the whole bin
                    // into one Add. The cut is on a contiguous slice of the sorted
                    // stream, so each piece keeps an honest footer and the pieces
                    // stay event-time disjoint.
                    if writer.buffer_len() >= max_file_bytes {
                        let flushed_at = std::time::Instant::now();
                        adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("hot bin flush: {e}"))?.into_iter().map(tag_sorted));
                        t_write += flushed_at.elapsed();
                    }
                }
            }
            let _ = ctx.deregister_table(&bin_table);
            if rows_staged == 0 {
                // The other silent exit: staging nothing and returning Ok is
                // otherwise indistinguishable from success in the logs.
                warn!(table_name, project_id, files = files.len(), event = "light_optimize_bin_no_rows");
                return Ok(());
            }
            // A sliced rewrite must reproduce EVERY input row — a value outside
            // every range would be silently dropped — so the count is checked
            // before anything commits. Only comparable when no input carries a
            // deletion vector, since a DV makes the scan return fewer rows than
            // `numRecords`; declining to check is safe, falsely aborting is not.
            if passes.len() > 1 && targets.iter().all(|a| a.deletion_vector.is_none()) && rows_in > 0 && rows_staged != rows_in as usize {
                anyhow::bail!("sliced repair staged {rows_staged} rows but the inputs hold {rows_in} — refusing to commit a lossy rewrite");
            }
            let final_flush_at = std::time::Instant::now();
            adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("hot bin flush: {e}"))?.into_iter().map(tag_sorted));
            t_write += final_flush_at.elapsed();
            info!(
                table_name,
                project_id,
                ?pass,
                files = files.len(),
                rows_staged,
                outputs = adds.len(),
                plan_secs = t_plan.as_secs_f64(),
                upstream_secs = t_upstream.as_secs_f64(),
                write_secs = t_write.as_secs_f64(),
                event = "unit_phase_timing",
                "where a maintenance unit's wall clock went"
            );
            Ok(())
        }
        .await;
        if let Err(e) = staged {
            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
            warn!(
                project_id,
                table_name,
                ?pass,
                files = files.len(),
                capacity = crate::maintenance_coordinator::is_capacity_failure(&e.to_string()),
                event = "hot_bin_staging_failed",
                "Light optimize staging failed: {e}"
            );
            // Count it against the candidate so a deterministically-impossible
            // file stops being re-offered. A pool exhaustion counts for the WHOLE
            // quarantine threshold — it recurs on every attempt; the 3-strike
            // rule is for genuinely transient failures. Use THE SHARED
            // classifier: a spilling sort dies with "Not enough memory to
            // continue external sort", which a "Resources exhausted" match misses.
            let exhausted = crate::maintenance_coordinator::is_capacity_failure(&e.to_string());
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
        // crash in the staging→commit window leaves a trail to clean up.
        let wave_id = uuid::Uuid::new_v4().to_string();
        self.record_staged_intent(StagedIntent {
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
            rollup: None,
            instance: None,
        });
        // Data-preserving compaction: BOTH sides carry data_change=false so the
        // snapshot-isolation downgrade applies and concurrent ingest appends
        // can't veto the wave.
        let (removes, adds) = staged_actions(&targets, adds, false);
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
        Ok(BinOutcome::Staged(StagedBin {
            project_id: project_id.to_string(),
            wave_id,
            targets,
            removes,
            adds,
            stage_store,
            discardable_paths: Vec::new(),
            dedup: None,
            sorted,
        }))
    }

    /// Commit one WAVE: every staged unit's Remove+Add in a single transaction.
    ///
    /// Before committing, each unit's target files are verified still live in the refreshed
    /// snapshot; a unit whose target was rewritten concurrently has only its own actions
    /// dropped and the rest of the wave still commits. Shared by hot-tail compaction and
    /// dirty-bin dedup; the per-physical-table commit lock prevents delete-delete aborts
    /// between them. `data_change` is the one real difference between the engines.
    pub(crate) async fn commit_wave(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date_markers: &[String], data_change: bool, mut bins: Vec<StagedBin>, round: usize,
    ) -> WaveResult {
        use deltalake::kernel::{Action, transaction::TableReference};
        debug_assert!(bins.iter().all(|b| b.data_change() == data_change), "a wave must not mix data-preserving and row-dropping units");
        let engine = if data_change { "dedup" } else { "light optimize" };
        // Names the waves on the way in, so a bin that never lands is traceable.
        info!(
            table_name,
            engine,
            round,
            bins = bins.len(),
            wave_ids = ?bins.iter().map(|b| b.wave_id.as_str()).collect::<Vec<_>>(),
            event = "wave_commit_enter"
        );
        let mut failed: Vec<StagedBin> = Vec::new();
        // Bins already CONFIRMED landed by an earlier attempt of this wave (see
        // the self-landed split below). Carried across OCC retries so their
        // credit — and their dirty-bin certification — is never lost.
        let mut carried: Vec<StagedBin> = Vec::new();
        // Key on "" explicitly: the wave spans MULTIPLE projects of one physical
        // table and every other unified-log writer serializes under ("", table).
        // Keying on bins[0].project_id would pick a DIFFERENT lock for a project
        // with custom storage, racing dedup's Removes.
        let commit_lock = self.commit_lock("", table_name).await;
        // Same key as the lock above — flush/ingest committers queued on it.
        let flush_waiters = self.flush_waiters("", table_name).await;
        // Warm/evict diff scoped to the wave's dates rather than the whole table.
        let markers: Vec<&str> = date_markers.iter().map(String::as_str).collect();
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        const MAX_RETRIES: usize = 4;
        for attempt in 0..MAX_RETRIES {
            // FLUSH PRIORITY. The lock is FIFO, so enqueueing ahead of a waiting
            // flush costs it our whole (minutes-long) commit. Durability outranks
            // maintenance: don't enqueue at all while a flush waits, bounding
            // flush latency by ONE in-flight wave commit. Not a wave-starvation
            // risk — flush is periodic and its commit is a short log append.
            if flush_waiters.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                crate::observability::maintenance_stats().wave_commits_yielded_to_flush.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(table_name, engine, round, attempt = attempt + 1, bins = bins.len(), event = "wave_commit_flush_yield");
                // Nothing committed: targets are still live and the staged
                // parquet is referenced by nothing. VACUUM cannot see
                // uncommitted staged files, so leaving them would leak forever.
                self.discard_bins(table_ref, &bins, None).await;
                failed.extend(bins);
                return WaveResult { landed: carried, failed };
            }
            let commit_guard = commit_lock.lock().await;
            // Bounded: this reads the log over the network with the commit lock
            // held. A timeout only means building on a possibly-stale snapshot,
            // which the liveness check and OCC retry ladder below handle.
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
            let active = match new_table.snapshot() {
                Ok(s) => ActiveFiles::from_snapshot(s),
                Err(e) => {
                    drop(commit_guard);
                    error!("{engine} wave: no snapshot for {table_name}: {e}");
                    self.discard_bins(table_ref, &bins, None).await;
                    failed.extend(bins);
                    return WaveResult { landed: carried, failed };
                }
            };
            let live = active.referenced_paths();
            let (fresh, stale) = split_live_bins(bins, &active);
            // SELF-LANDED SPLIT — do not remove. A "stale" bin (targets gone
            // from the snapshot) is usually a concurrent rewrite, whose staged
            // parquet is safe to delete. But our OWN previous attempt landing
            // and then erroring looks identical by targets alone, and deleting
            // then would leave the landed commit with dangling Adds. The exact
            // Adds settle it; a DV update reuses its path, so the deletion
            // vector must match too.
            let (self_landed, stale): (Vec<StagedBin>, Vec<StagedBin>) = stale.into_iter().partition(|b| bin_adds_live(b, &active));
            stale.iter().for_each(|bin| debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_stale_at_commit"));
            self.discard_bins(table_ref, &stale, Some(&live)).await;
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
                self.record_wave_landed(&self_landed, data_change, table_name);
                carried.extend(self_landed);
            }
            // Two dirty bins can share one parquet file, and each staged unit is
            // a full-file replacement — committing both would remove the file
            // twice and duplicate its rows. Land only a target-disjoint subset
            // per wave; the rest are requeued. Delta action validity requires it.
            let mut claimed_targets = HashSet::new();
            let (fresh, overlapping): (Vec<_>, Vec<_>) = fresh.into_iter().partition(|bin| {
                let disjoint = bin.targets.iter().all(|add| !claimed_targets.contains(&add.path));
                if disjoint {
                    claimed_targets.extend(bin.targets.iter().map(|add| add.path.clone()));
                }
                disjoint
            });
            overlapping.iter().for_each(|bin| debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_overlapping_target"));
            self.discard_bins(table_ref, &overlapping, Some(&live)).await;
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
            // Tag a wave composed ENTIRELY of in-place DV-dedup bins so
            // reconcile can skip re-minting Dedup from it; a mixed/CoW wave
            // stays untagged and fails toward minting. Only ONE `with_metadata`
            // call is allowed — it REPLACES the map rather than extending it.
            let mut commit_props = incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot);
            if data_change && fresh.iter().all(StagedBin::masked_in_place) {
                commit_props = commit_props.with_metadata([(DV_DEDUP_COMMIT_KEY.to_string(), serde_json::Value::Bool(true))]);
            }
            // Bounded: one slow object-store request here pins the commit lock
            // and stalls every committer on the table.
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "wave_commit",
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(commit_props).with_actions(actions).build(Some(snapshot_ref), new_table.log_store(), op),
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
                    // evicting first cold-starts the hottest query window.
                    let live_uris = self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &markers).await;
                    self.reindex_wave_outputs(table_ref, table_name, &fresh, &live_uris).await;
                    self.record_wave_landed(&fresh, data_change, table_name);
                    return WaveResult { landed: carried.tap_mut(|landed| landed.extend(fresh)), failed };
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    // Released BEFORE the probe: the probe is another log read,
                    // and on a timeout the store is already slow.
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
                            self.record_wave_landed(&fresh, data_change, table_name);
                            return WaveResult { landed: carried.tap_mut(|landed| landed.extend(fresh)), failed };
                        }
                        CommitProbe::NotLanded => {
                            crate::observability::record_optimize_failed();
                            error!("{engine} wave commit failed for '{}': {}", table_name, e);
                            self.discard_bins(table_ref, &fresh, None).await;
                            failed.extend(fresh);
                            return WaveResult { landed: carried, failed };
                        }
                        CommitProbe::Inconclusive => {
                            // Staged files stay in place (possibly the only live
                            // copy); the units still count as failed. The next
                            // wave's `refresh_table_snapshot` converges: a commit
                            // that landed unobserved makes these targets stale,
                            // so re-staged bins drop out instead of
                            // double-applying, leaking only a staged file that
                            // boot-time reconcile reclaims.
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
    /// committed, so the rewrite does not leave a search-coverage hole.
    /// Failure is correctness-safe: hybrid reads fall back to raw data.
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
        // rows under the same ids (dedup only DROPS versions, leaving the index a
        // superset — false positives the scan filters, never false negatives), so
        // a fully-covered input set is a manifest edit, not a rebuild.
        let mut carried: HashSet<String> = HashSet::new();
        for bin in bins {
            let for_bin: Vec<String> = files.iter().filter(|(project, _, _)| *project == bin.project_id).map(|(_, _, uri)| uri.clone()).collect();
            if for_bin.is_empty() {
                continue;
            }
            let target_paths: Vec<_> = bin.targets.iter().map(|add| add.path.clone()).collect();
            match svc.carry_forward_after_compaction(table_name, &bin.project_id, &target_paths, &for_bin).await {
                Ok(true) => carried.extend(for_bin),
                Ok(false) => {}
                Err(error) => warn!(table_name, %error, event = "tantivy_wave_carry_forward_failed"),
            }
        }
        let files: Vec<_> = files.into_iter().filter(|(_, _, uri)| !carried.contains(uri)).collect();
        // Log whenever there was work, not only when something carried —
        // otherwise every all-rebuild wave is invisible.
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

    /// Per-engine counters for a landed wave. Dedup's dropped-row accounting belongs HERE and
    /// nowhere else: a unit that loses the liveness check or the commit dropped zero rows.
    fn record_wave_landed(&self, landed: &[StagedBin], data_change: bool, table_name: &str) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        // Marked HERE because this is the single point both landing branches
        // agree the commit is real.
        let schema = schema_or_default(table_name);
        landed.iter().for_each(|bin| self.mark_written_sorted(schema, bin.sorted, &bin.adds));
        if data_change {
            for dropped in landed.iter().filter_map(|b| b.dedup.as_ref()).map(DedupUnit::dropped).filter(|d| *d > 0) {
                crate::observability::record_compaction_dedup_dropped(dropped);
            }
            stats.dedup_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.dedup_waves_committed.fetch_add(1, Relaxed);
        } else {
            stats.light_optimize_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.light_optimize_waves_committed.fetch_add(1, Relaxed);
        }
    }

    /// Cleanup + intent-clear for bins leaving the wave uncommitted. The pair IS the
    /// crash-safety invariant — cleaning without clearing (or vice versa) breaks the
    /// manifest's meaning.
    ///
    /// The delete is gated on LIVENESS: a concurrent instance can resume our staged
    /// intent and commit these very objects. `live` is the caller's set when it holds
    /// one under the commit lock; otherwise the log is re-read here, since a stale
    /// snapshot cannot see a commit that just landed.
    ///
    /// Liveness unknown ⇒ delete nothing AND clear nothing: the intent is what lets
    /// boot-time reconcile reclaim the files later.
    pub(crate) async fn discard_bins(&self, table_ref: &Arc<RwLock<DeltaTable>>, bins: &[StagedBin], live: Option<&HashSet<String>>) {
        let refreshed;
        let live = match live {
            Some(live) => live,
            None => {
                let ok =
                    tokio::time::timeout(COMMIT_LOCK_OP_TIMEOUT, refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot))
                        .await
                        .is_ok_and(|r| r.is_ok());
                let fresh = if ok { table_ref.read().await.snapshot().ok().map(|s| ActiveFiles::from_snapshot(s).referenced_paths()) } else { None };
                let Some(fresh) = fresh else {
                    warn!("wave discard: no fresh snapshot — leaving {} bins' staged parquet AND intents for boot reconcile", bins.len());
                    return;
                };
                refreshed = fresh;
                &refreshed
            }
        };
        discard_bin_parquet(bins, live).await;
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

    /// Record files THIS process just wrote with an honest `sorting_columns` footer, so footer
    /// repair never offers them as suspects at all. Without this, admission is O(every file
    /// ever written), since every untagged flushed file buys its exoneration with a ranged read.
    ///
    /// **The predicate must mirror [`build_writer_properties`] exactly.** The footer is stamped
    /// only when `declare_sorted` AND the parquet CONVERSION is non-empty —
    /// `schema.sorting_columns()` drops names it cannot map to physical leaf indices, so a schema
    /// can declare an order whose conversion is empty. Marking on the schema field alone would
    /// make such a file permanently invisible to repair.
    ///
    /// Paths are DECODED before insertion: Delta stores `Add.path` URL-encoded while admission
    /// and the probe key on `LogicalFile::path()`, which decodes.
    pub(crate) fn mark_written_sorted(&self, schema: &crate::schema::TableSchema, sorted: bool, adds: &[deltalake::kernel::Action]) {
        if !sorted || schema.sorting_columns().is_empty() || !self.config.maintenance.timefusion_repair_mark_sorted_at_write {
            return;
        }
        let paths: Vec<String> = adds
            .iter()
            .filter_map(|action| match action {
                deltalake::kernel::Action::Add(add) => Some(percent_encoding::percent_decode_str(&add.path).decode_utf8_lossy().into_owned()),
                _ => None,
            })
            .collect();
        if paths.is_empty() {
            return;
        }
        crate::observability::maintenance_stats().repair_sorted_at_write.fetch_add(paths.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.remember_verified_sorted(&paths);
    }

    /// Remember footers known to be sorted, in memory and on disk.
    fn remember_verified_sorted(&self, paths: &[String]) {
        for path in paths {
            self.repair_verified_sorted.insert(path.clone());
        }
        self.persist_verified_sorted(paths);
    }

    /// Persist footers already probed as sorted, so a restart does not re-probe them.
    ///
    /// Sound because a Delta object path is immutable: a rewrite always produces a new path, so
    /// "this object carries a `sorting_columns` footer" is a permanent fact. A stale entry for a
    /// tombstoned path is harmless because admission never sees that path again. Best-effort:
    /// a write failure costs re-probing, never correctness.
    pub(crate) fn persist_verified_sorted(&self, paths: &[String]) {
        let _guard = crate::support::lock(&self.repair_verified_lock);
        let file_path = self.repair_verified_path();
        let write = crate::support::without_blocking_the_worker(|| append_state_lines(&file_path, paths));
        if let Err(e) = write {
            warn!("verified-sorted append failed ({:?}): {} — repair will re-probe these footers after a restart", file_path, e);
        }

        // RUNTIME compaction, amortized to one rewrite per cap's worth of appends: write-time
        // marking feeds this file on every commit, so bounding it only at boot would let it grow
        // without limit in a long-lived process. The in-memory set is trimmed to match — a
        // dropped entry costs one footer probe, never correctness.
        let appended = self.repair_verified_appends.fetch_add(paths.len(), std::sync::atomic::Ordering::Relaxed) + paths.len();
        if appended >= REPAIR_VERIFIED_PERSIST_CAP {
            self.repair_verified_appends.store(0, std::sync::atomic::Ordering::Relaxed);
            let (kept, dropped) = self.truncate_verified_file_locked();
            let keep: HashSet<&str> = kept.iter().map(String::as_str).collect();
            self.repair_verified_sorted.retain(|path| keep.contains(path.as_str()));
            info!(kept = kept.len(), dropped, event = "footer_repair_verified_compacted");
        }
    }

    /// Truncate the persisted verified-sorted list to its most recent
    /// [`REPAIR_VERIFIED_PERSIST_CAP`] entries; returns `(kept, dropped)`. Newest entries win —
    /// the tail of the file is the most recently written or probed. Caller holds
    /// `repair_verified_lock`. The read+rewrite is multi-MB once the cap is reached and is
    /// reachable from the FLUSH commit path, so it must go through
    /// `without_blocking_the_worker` rather than blocking a tokio worker inline.
    fn truncate_verified_file_locked(&self) -> (Vec<String>, usize) {
        let file_path = self.repair_verified_path();
        crate::support::without_blocking_the_worker(|| {
            let Ok(contents) = std::fs::read_to_string(&file_path) else { return (Vec::new(), 0) };
            let all: Vec<&str> = contents.lines().filter(|line| !line.is_empty()).collect();
            let kept: Vec<String> = all[all.len().saturating_sub(REPAIR_VERIFIED_PERSIST_CAP)..].iter().map(|line| (*line).to_string()).collect();
            let dropped = all.len() - kept.len();
            if dropped > 0
                && let Err(e) = std::fs::write(&file_path, kept.iter().map(|path| format!("{path}\n")).collect::<String>())
            {
                warn!("verified-sorted compaction failed ({:?}): {e}", file_path);
            }
            (kept, dropped)
        })
    }

    /// Load the persisted verified-sorted paths at boot, compacting the file if
    /// it has grown past [`REPAIR_VERIFIED_PERSIST_CAP`]. Newest entries win: the
    /// tail of the file is the most recently probed.
    pub fn load_verified_sorted(&self) {
        let _guard = crate::support::lock(&self.repair_verified_lock);
        let (kept, dropped) = self.truncate_verified_file_locked();
        for path in &kept {
            self.repair_verified_sorted.insert(path.clone());
        }
        info!(loaded = kept.len(), dropped, event = "footer_repair_verified_loaded");
    }

    pub(crate) fn staged_intent_path(&self) -> PathBuf {
        self.maintenance_state_path("staged_intent.jsonl")
    }

    /// Every recorded staged intent. A missing/unreadable manifest is an empty
    /// list: every caller treats "no entries" and "no manifest" identically.
    fn staged_intents(&self) -> Vec<StagedIntent> {
        let _manifest_guard = crate::support::lock(&self.staged_intent_manifest_lock);
        parse_staged_intents(&std::fs::read_to_string(self.staged_intent_path()).unwrap_or_default())
    }

    /// Append one bin's staged paths. Best-effort: a manifest write failure
    /// must never fail the compaction, only widen the VACUUM backstop's job.
    pub(crate) fn record_staged_intent(&self, entry: StagedIntent) {
        // Stamped HERE, never by callers: a site that forgot would write an
        // entry indistinguishable from a legacy one and lose its resume.
        let entry = StagedIntent { instance: Some(crate::observability::instance_id().to_owned()), ..entry };
        let _manifest_guard = crate::support::lock(&self.staged_intent_manifest_lock);
        let path = self.staged_intent_path();
        let write = crate::support::without_blocking_the_worker(|| -> std::io::Result<()> { append_state_lines(&path, &[serde_json::to_string(&entry)?]) });
        if let Err(e) = write {
            warn!("staged-intent manifest append failed ({:?}): {} — orphan cleanup falls back to VACUUM", path, e);
        }
    }

    /// Drop one wave's entries, rewrite-compacting the append-only file. Called
    /// after the wave commits or after its staged parquet is cleaned up, i.e.
    /// once the entry can no longer describe an orphan.
    fn clear_staged_intent(&self, wave_ids: &[&str]) {
        let _manifest_guard = crate::support::lock(&self.staged_intent_manifest_lock);
        let path = self.staged_intent_path();
        let Some(write) = crate::support::without_blocking_the_worker(|| {
            let contents = std::fs::read_to_string(&path).ok()?;
            let kept: Vec<String> = parse_staged_intents(&contents)
                .into_iter()
                .filter(|e| !wave_ids.contains(&e.wave_id.as_str()))
                .filter_map(|e| serde_json::to_string(&e).ok())
                .collect();
            Some(if kept.is_empty() { std::fs::write(&path, b"") } else { std::fs::write(&path, kept.join("\n") + "\n") })
        }) else {
            return;
        };
        if let Err(e) = write {
            warn!("staged-intent manifest compaction failed ({:?}): {}", path, e);
        }
    }

    /// Publish a resumed rollup and retire its intent — the bookkeeping both
    /// success arms owe, and which the commit alone does not satisfy.
    fn publish_resumed_rollup(&self, rollup: &crate::database::RollupResume, wave_id: &str) -> Result<()> {
        let mut journal = self.journal();
        journal.publish(&rollup.key, rollup.publication.clone());
        journal.checkpoint()?;
        drop(journal);
        self.clear_staged_intent(&[wave_id]);
        Ok(())
    }

    /// Commit a rollup unit whose output was staged before a restart, instead of
    /// re-running its scan. `Ok(true)` means this unit is DONE and the caller
    /// must not build anything.
    ///
    /// Called after the source metadata is read (so the witness is in hand) and before the
    /// aggregate, which is the expensive part.
    ///
    /// Every refusal path leaves the intent alone: the orphan sweep already collects unowned
    /// staged parquet, and deleting on a transient read failure would discard good output.
    pub(crate) async fn resume_rollup_unit(&self, key: &crate::maintenance_coordinator::TaskKey, current_source_rows: Option<u64>) -> Result<bool> {
        use deltalake::protocol::{DeltaOperation, SaveMode};
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        let candidates: Vec<(StagedIntent, crate::database::RollupResume)> = self
            .staged_intents()
            .into_iter()
            .filter_map(|entry| entry.rollup.clone().filter(|rollup| &rollup.key == key).map(|rollup| (entry, rollup)))
            .collect();
        if candidates.is_empty() {
            stats.rollup_resume_no_intent.fetch_add(1, Relaxed);
            return Ok(false);
        }
        let Some(date_string) = chrono::DateTime::from_timestamp_micros(key.slice.start_micros).map(|time| time.date_naive().to_string()) else {
            return Ok(false);
        };
        let target_ref = self.get_or_create_table(&key.project_id, &key.physical_table).await?;
        // The WHOLE partition's live files, not just the ones named by an
        // intent: the double-count test asks whether anything else already
        // covers this slice, and a file the intent never heard of is the
        // dangerous case. Slice range and Add travel together — the verdict
        // needs the first, the commit the second.
        let (partition, store) = {
            let table = target_ref.read().await;
            let partition: HashMap<String, (Option<(i64, i64)>, deltalake::kernel::Add)> = table
                .snapshot()?
                .log_data()
                .iter()
                .map(|file| add_action(&file))
                .filter(|add| {
                    Self::maintenance_partition_from_action(&add.path, Some(&add.partition_values), "default")
                        .is_some_and(|(project, date)| project == key.project_id && date == date_string)
                })
                .map(|add| {
                    let tag = |name: &str| add.tags.as_ref().and_then(|tags| tags.get(name)).and_then(Option::as_deref)?.parse::<i64>().ok();
                    let slice = tag(crate::maintenance_coordinator::TAG_SLICE_START).zip(tag(crate::maintenance_coordinator::TAG_SLICE_END));
                    (add.path.clone(), (slice, add))
                })
                .collect();
            (partition, table.log_store().object_store(None))
        };
        let live_view: HashMap<&str, Option<(i64, i64)>> = partition.iter().map(|(path, (slice, _))| (path.as_str(), *slice)).collect();
        let now_secs = crate::support::now_secs();
        for (entry, rollup) in &candidates {
            let verdict = classify_rollup_resume(entry, &key.physical_table, now_secs, &live_view, current_source_rows);
            match verdict {
                // `classify_rollup_resume` cannot return RowMismatch — a rollup
                // aggregates — so this arm is the ownership guard and nothing else.
                ResumeVerdict::Skip | ResumeVerdict::RowMismatch { .. } => {
                    stats.rollup_resume_skipped.fetch_add(1, Relaxed);
                    continue;
                }
                ResumeVerdict::AlreadyLanded => {
                    // The Delta commit landed and only the bookkeeping was lost.
                    // PUBLISH anyway: without a journal publication the coverage
                    // replay refuses the slice and the planner re-enqueues the
                    // whole scan.
                    self.publish_resumed_rollup(rollup, &entry.wave_id)?;
                    stats.rollup_resume_already_landed.fetch_add(1, Relaxed);
                    info!(table = %key.physical_table, project_id = %key.project_id, wave_id = %entry.wave_id, event = "rollup_resume_already_landed");
                    return Ok(true);
                }
                ResumeVerdict::Stale | ResumeVerdict::SourceMoved | ResumeVerdict::WouldDoubleCount => {
                    stats.rollup_resume_declined.fetch_add(1, Relaxed);
                    info!(
                        table = %key.physical_table, project_id = %key.project_id, wave_id = %entry.wave_id,
                        verdict = ?verdict, event = "rollup_resume_declined",
                        "a staged rollup output no longer describes reality — rebuilding it instead"
                    );
                }
                ResumeVerdict::Commit => {
                    if !staged_objects_complete(store.as_ref(), &entry.adds).await {
                        stats.rollup_resume_declined.fetch_add(1, Relaxed);
                        info!(table = %key.physical_table, wave_id = %entry.wave_id, event = "rollup_resume_incomplete");
                        continue;
                    }
                    let actions: Vec<deltalake::kernel::Action> = entry
                        .target_paths
                        .iter()
                        .filter_map(|path| partition.get(path))
                        .map(|(_, add)| deltalake::kernel::Action::Remove(remove_for_add(add, true)))
                        .chain(entry.adds.iter().cloned().map(deltalake::kernel::Action::Add))
                        .collect();
                    let commit_lock = self.commit_lock(&key.project_id, &key.physical_table).await;
                    let guard = commit_lock.lock().await;
                    let mut table = target_ref.read().await.clone();
                    let target_schema = get_schema(&key.physical_table).ok_or_else(|| anyhow::anyhow!("rollup target schema missing"))?;
                    let op = DeltaOperation::Write { mode: SaveMode::Overwrite, partition_by: Some(target_schema.partitions.clone()), predicate: None };
                    let finalized = deltalake::kernel::transaction::CommitBuilder::from(incremental_commit_properties(
                        self.config.maintenance.timefusion_incremental_snapshot,
                    ))
                    .with_actions(actions)
                    .build(Some(table.snapshot()? as &dyn deltalake::kernel::transaction::TableReference), table.log_store(), op)
                    .await?;
                    table.state = Some(finalized.snapshot());
                    drop(guard);
                    self.swap_and_refresh_cache(&target_ref, table, None, &[&format!("date={}", rollup.date)]).await;
                    self.publish_resumed_rollup(rollup, &entry.wave_id)?;
                    stats.rollup_resumed.fetch_add(1, Relaxed);
                    info!(
                        table = %key.physical_table, project_id = %key.project_id, wave_id = %entry.wave_id,
                        rows = rollup.publication.rows, event = "rollup_resumed",
                        "committed a rollup staged before a restart instead of rebuilding it"
                    );
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Resume a repair bin that a previous attempt already wrote.
    ///
    /// Returns the staged output as a `StagedBin` instead of rewriting the same file again.
    /// Hooked at bin selection, keyed on an intent whose inputs exactly match the bin we were
    /// about to stage; the same lookup covers a restart and a stage whose commit lost an OCC
    /// race. `None` is always safe: the caller stages normally and the declined entry's parquet
    /// falls to boot-time reconcile / VACUUM.
    async fn resumable_staged_bin(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, files: &[String]) -> Option<StagedBin> {
        use deltalake::kernel::Action;
        use std::sync::atomic::Ordering::Relaxed;
        if !self.config.maintenance.timefusion_repair_resume_enabled {
            return None;
        }
        let wanted: HashSet<&str> = files.iter().map(String::as_str).collect();
        // Set equality, not order: the bin is a SET of inputs, and admission may
        // legitimately hand them over in a different order than last time.
        let candidates: Vec<StagedIntent> = self
            .staged_intents()
            .into_iter()
            .filter(|e| e.project_id == project_id && e.target_paths.len() == files.len())
            .filter(|e| e.target_paths.iter().all(|p| wanted.contains(p.as_str())))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        // Only the paths in play: the snapshot can hold tens of thousands of
        // files and this must not parse every one of their stats blobs.
        let interest: HashSet<&str> = candidates.iter().flat_map(|e| e.target_paths.iter().chain(e.adds.iter().map(|a| &a.path))).map(String::as_str).collect();
        let (live, target_adds, store) = {
            let table = table_ref.read().await;
            let snapshot = table.snapshot().ok()?;
            let (live, target_adds): (HashMap<String, Option<i64>>, HashMap<String, deltalake::kernel::Add>) = snapshot
                .log_data()
                .iter()
                .filter(|file| interest.contains(&*file.path()))
                .map(|file| {
                    let file_path = file.path().into_owned();
                    // A deletion vector makes the file's LOGICAL row count smaller
                    // than its `numRecords`, and the rewrite read logical rows — so
                    // comparing against `numRecords` would flag every DV'd input
                    // as a mismatch.
                    let dropped = file.deletion_vector_descriptor().map_or(0, |dv| dv.cardinality);
                    let rows = file.num_records().and_then(|n| i64::try_from(n).ok()).map(|n| n - dropped);
                    ((file_path.clone(), rows), (file_path, add_action(&file)))
                })
                .unzip();
            (live, target_adds, table.log_store().object_store(None))
        };
        let live_view: HashMap<&str, Option<i64>> = live.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        let now_secs = crate::support::now_secs();
        let stats = crate::observability::maintenance_stats();
        for entry in &candidates {
            match classify_resume(entry, table_name, now_secs, &live_view) {
                // A repair intent never carries rollup evidence, so the latter
                // two verdicts are unreachable here by construction.
                ResumeVerdict::Skip | ResumeVerdict::SourceMoved | ResumeVerdict::WouldDoubleCount => {
                    stats.repair_resume_skipped.fetch_add(1, Relaxed);
                    continue;
                }
                ResumeVerdict::AlreadyLanded => {
                    // The commit landed and only the bookkeeping was lost. Never
                    // re-commit: that would Remove the files it just Added.
                    stats.repair_resume_already_landed.fetch_add(1, Relaxed);
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
                    if !staged_objects_complete(store.as_ref(), &entry.adds).await {
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
                        targets,
                        removes,
                        adds,
                        stage_store: Arc::clone(&store),
                        discardable_paths: Vec::new(),
                        dedup: None,
                        // The manifest does not record whether the earlier
                        // process's output declared a sorted footer, so let the
                        // probe answer it rather than guessing.
                        sorted: false,
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
        let entries = self.staged_intents();
        if entries.is_empty() {
            return;
        }
        let (referenced, store) = {
            let table = table_ref.read().await;
            let Ok(snapshot) = table.snapshot() else {
                warn!("staged-intent reconcile skipped for '{table_name}': no snapshot loaded");
                return;
            };
            // A committed deletion-vector `.bin` is not a log_data path; treat any `.bin`
            // referenced by a live Add's DV descriptor as live or the masked rows resurrect.
            let referenced: HashSet<String> = snapshot
                .log_data()
                .iter()
                .flat_map(|f| {
                    let dv = f.deletion_vector_descriptor().as_ref().and_then(deltalake::operations::deletion_vectors::dv_object_store_relative_path);
                    std::iter::once(f.path().into_owned()).chain(dv)
                })
                .collect();
            (referenced, table.log_store().object_store(None))
        };
        let now_secs = crate::support::now_secs();
        let orphans = staged_orphan_deletions(&entries, table_name, now_secs, &referenced);
        let orphan_count = orphans.len();
        let deleted = futures::stream::iter(orphans)
            .map(|orphan| {
                let store = &store;
                async move {
                    match store.delete(&object_store::path::Path::from(orphan.as_str())).await {
                        // NotFound = already gone.
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
        // Clear ONLY the entries this reconcile judged: this table's, old enough to be
        // unambiguous. Other tables' entries (and young ones) stay for their own pass.
        let ids: Vec<&str> = entries
            .iter()
            .filter(|e| e.table_name == table_name && now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
            .map(|e| e.wave_id.as_str())
            .collect();
        self.clear_staged_intent(&ids);
    }

    /// Checkpoint after a tick's waves when the log has advanced enough since the
    /// last checkpoint.
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
    /// Two levels because the failure modes differ: WAL backlog can be sustained, so it degrades to
    /// a service floor rather than starving compaction; memory near the cgroup limit is a hard stop.
    pub(crate) fn light_optimize_brake(&self) -> Option<Brake> {
        use std::sync::atomic::Ordering::Relaxed;
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
        // HOST pressure, not just our cgroup: /proc/meminfo is the host's inside a container,
        // and the kernel's global OOM killer can fire long before the memcg limit.
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
    /// Must be applied to every plan, including each round's re-plan, or later rounds
    /// re-select the same suspects.
    async fn drop_verified_sorted_bins(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, pass: TailPass, planned: Vec<(String, Vec<String>)>,
    ) -> Vec<(String, Vec<String>)> {
        if pass != TailPass::Repair || planned.is_empty() {
            return planned;
        }
        use futures::StreamExt;
        let before = planned.len();
        let kept: Vec<(String, Vec<String>)> = futures::stream::iter(planned)
            .map(|(project_id, files)| async move { (!self.repair_bin_already_sorted(table_ref, &files).await).then_some((project_id, files)) })
            .buffer_unordered(REPAIR_VERIFY_CONCURRENCY)
            .filter_map(std::future::ready)
            .collect()
            .await;
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
        let object_store = { table_ref.read().await.log_store().object_store(None) };
        for path in files {
            if !Self::footer_declares_sorted(&object_store, path).await {
                return false;
            }
        }
        self.remember_verified_sorted(files);
        info!(files = files.len(), event = "footer_repair_suspect_cleared");
        true
    }

    /// True if every row group of this object carries a non-empty `sorting_columns` footer.
    /// An unreadable footer returns false (a needless rewrite beats a silent wrong answer).
    async fn footer_declares_sorted(object_store: &Arc<dyn object_store::ObjectStore>, path: &str) -> bool {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};
        let os_path = OsPath::from(path);
        let Ok(meta) = object_store.head(&os_path).await else { return false };
        let mut reader = ParquetObjectReader::new(object_store.clone(), os_path).with_file_size(meta.size);
        let Ok(pq) = reader.get_metadata(None).await else { return false };
        !pq.row_groups().iter().any(|rg| rg.sorting_columns().is_none_or(|sc| sc.is_empty()))
    }

    /// Seed the verified-sorted set from files that already exist (write-time marking only
    /// covers files this build writes).
    ///
    /// Bounded by `limit` files per call, `REPAIR_VERIFY_CONCURRENCY` in flight, and only files
    /// not already known. Negatives are deliberately NOT recorded — an unsorted file leaves the
    /// population by being rewritten, and a durable "known bad" set could outlive the fix.
    ///
    /// Returns `(tables_read, verified)`; `tables_read` distinguishes "the fleet is already
    /// seeded" from "this ran before the tables existed", which both yield `verified = 0`.
    pub(crate) async fn seed_verified_sorted(&self, limit: usize) -> (usize, usize) {
        use futures::StreamExt;
        // Gated with the marking, not separately, so the sweep cannot re-derive from footers
        // what the kill switch just turned off.
        if !self.config.maintenance.timefusion_repair_mark_sorted_at_write {
            return (0, 0);
        }
        // Grouped BY TABLE with the store taken once under the enumeration lock, so the probe
        // stage below takes NO table locks: per-file re-locking would starve
        // `refresh_table_snapshot`'s writer and leave queries on a stale snapshot.
        let mut unknown: Vec<(Arc<dyn object_store::ObjectStore>, Vec<String>)> = Vec::new();
        let mut tables_read = 0usize;
        let mut candidates = 0usize;
        for (_, source, table_ref) in self.all_tables().await {
            // A table declaring no sort order has no footer to lose, so it has no suspects.
            if schema_or_default(&source).sorting_columns.is_empty() {
                continue;
            }
            {
                let table = table_ref.read().await;
                let Ok(snapshot) = table.snapshot() else { continue };
                tables_read += 1;
                let paths: Vec<String> = snapshot
                    .log_data()
                    .iter()
                    .map(|file| file.path().into_owned())
                    .filter(|path| !self.repair_verified_sorted.contains(path))
                    .take(limit - candidates)
                    .collect();
                candidates += paths.len();
                if !paths.is_empty() {
                    unknown.push((table.log_store().object_store(None), paths));
                }
            }
            if candidates >= limit {
                break;
            }
        }
        if candidates == 0 {
            // Logged even with nothing to report: `tables_read = 0` (ran before the tables
            // loaded) is otherwise indistinguishable from a fully-seeded fleet.
            info!(tables_read, candidates = 0, verified = 0, event = "footer_repair_seed_swept");
            return (tables_read, 0);
        }
        // Deliberately NOT flattened into one stream over `(store, paths)`: a closure whose
        // PARAMETER carries the store leaves the trait object's lifetime to inference, which
        // then demands an unsatisfiable higher-ranked `FnOnce`.
        let mut verified: Vec<String> = Vec::new();
        for (object_store, paths) in unknown {
            let found: Vec<String> = futures::stream::iter(paths)
                .map(|path| {
                    // Cloned INSIDE the closure so its parameter is a plain `String`.
                    let object_store = Arc::clone(&object_store);
                    async move { Self::footer_declares_sorted(&object_store, &path).await.then_some(path) }
                })
                .buffer_unordered(REPAIR_VERIFY_CONCURRENCY)
                .filter_map(std::future::ready)
                .collect()
                .await;
            verified.extend(found);
        }
        self.remember_verified_sorted(&verified);
        info!(tables_read, candidates, verified = verified.len(), unsorted = candidates - verified.len(), event = "footer_repair_seed_swept");
        (tables_read, verified.len())
    }

    /// Each pass is budgeted from its own cron period: their units are orders of magnitude apart
    /// (a packing bin is a few small files, a repair bin is one whole-file rewrite), and
    /// `stage_hot_bin` discards an over-budget bin outright.
    pub(crate) fn tail_pass_tick_budget(&self, pass: TailPass) -> std::time::Duration {
        match pass {
            TailPass::Pack => self.config.derived.tick_budget(cron_period(&self.config.maintenance.timefusion_light_optimize_schedule)),
            // Set outright, not derived from the period: repair needs both frequent
            // attempts and a long run.
            TailPass::Repair => std::time::Duration::from_secs(self.config.maintenance.timefusion_footer_repair_budget_secs),
        }
    }

    /// Inner optimize loop for the COLD consolidate path. Caller is expected to hold the flush
    /// lock when a `BufferedWriteLayer` is active; the retry loop is a safety net against
    /// bursts from `flush_all_now` or shutdown flushes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn optimize_table_light_inner(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, today: chrono::NaiveDate, project_id: &str, partition_filters: &[PartitionFilter],
        selected_files: &[String], target_size: i64, writer_properties: &WriterProperties, optimize_type: deltalake::operations::optimize::OptimizeType,
        min_files: usize, start_time: std::time::Instant,
    ) -> Result<()> {
        const MAX_RETRIES: usize = 4;
        // Compaction materializes Arrow like dedup, and that footprint is pool-invisible —
        // hold a rewrite permit so it can't stack with a concurrent dedup and blow the cgroup.
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
        let mut last_err: Option<deltalake::DeltaTableError> = None;
        // Pre-state file set for deriving the files this optimize adds (to warm) and removes
        // (to evict), scoped to the one `(project_id, today)` partition it is filtered to.
        // Safe outside the retry loop: only a successful commit (which returns) changes it.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let (pid_marker, date_marker) = (format!("project_id={project_id}/"), format!("date={today}/"));
        let scope = [pid_marker.as_str(), date_marker.as_str()];
        let pre_uris: Option<HashSet<String>> = if track_files { Some(scoped_file_uris(&*table_ref.read().await, &scope).into_iter().collect()) } else { None };
        for attempt in 0..MAX_RETRIES {
            let table_clone = { table_ref.read().await.clone() };
            if attempt == 0 {
                info!(table_name, project_id, date = %today, target_size, max_concurrent_tasks = self.config.derived.optimize_merge_tasks(), event = "light_optimize_started");
            } else {
                debug!("Light optimize retry {}/{} after OCC conflict", attempt + 1, MAX_RETRIES);
            }
            let optimize_result = table_clone
                .optimize()
                .with_filters(partition_filters)
                // Restrict the rewrite to the pre-selected sealed files, so live appends after
                // selection stay out of the commit's file set (OCC race on the hot partition).
                .with_binned_files(selected_files)
                .with_type(optimize_type.clone())
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::<u64>::MIN))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(self.config.derived.optimize_merge_tasks())
                .with_writer_properties(writer_properties.clone())
                .with_min_commit_interval(tokio::time::Duration::from_secs(30))
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                // Variant columns are Struct{Binary, Binary} on disk; delta-rs's default
                // `schema_force_view_types=true` reads them as BinaryView and the rewrite
                // fails mid-scan, so the session state must disable it.
                .with_session_state(Arc::new(self.light_optimize_session_state()))
                .await;
            match optimize_result {
                Ok((new_table, metrics)) => {
                    if metrics.total_considered_files < min_files {
                        debug!(
                            "Skipping light optimization commit for table={table_name} project={project_id} date={today}: {} files < min threshold {min_files}",
                            metrics.total_considered_files
                        );
                        return Ok(());
                    }
                    let duration = start_time.elapsed();
                    info!(
                        "Light optimization completed for table={table_name} project={project_id} date={today} in {duration:?} (attempt {}): {} files considered, {} removed, {} added",
                        attempt + 1,
                        metrics.total_considered_files,
                        metrics.num_files_removed,
                        metrics.num_files_added
                    );
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &scope).await;
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_conflict = is_occ_conflict_err(&msg);
                    if is_conflict {
                        crate::observability::record_optimize_conflict();
                    }
                    // "Found unmasked nulls for non-nullable StructArray" surfaces when
                    // delta-rs is mid-rewrite; it clears on a fresh re-scan.
                    let is_transient_schema = msg.contains("Found unmasked nulls");
                    if (is_conflict || is_transient_schema) && attempt + 1 < MAX_RETRIES {
                        tokio::time::sleep(occ_backoff(attempt)).await;
                        last_err = Some(e);
                        continue;
                    }
                    crate::observability::record_optimize_failed();
                    error!("Light optimization operation failed for table={table_name} project={project_id} date={today} (attempt {}): {e}", attempt + 1);
                    return Err(anyhow::anyhow!("Light table optimization failed: {}", e));
                }
            }
        }
        let err = last_err.map(|e| e.to_string()).unwrap_or_else(|| "exhausted retries".into());
        warn!(
            "Light optimization gave up for table={table_name} project={project_id} date={today} after {MAX_RETRIES} OCC conflicts; will retry next tick: {err}"
        );
        Ok(())
    }

    /// On-demand vacuum of a single unified table (pgwire `VACUUM <table>`).
    /// `retention_hours = None` uses the configured default.
    pub async fn vacuum_named(&self, table_name: &str, retention_hours: Option<u64>) -> Result<usize> {
        let retention = retention_hours.unwrap_or(self.config.maintenance.timefusion_vacuum_retention_hours);
        let table_ref = self.get_or_create_unified_table(table_name).await?;
        Ok(self.vacuum_table("", table_name, &table_ref, retention).await)
    }

    /// Returns the number of files deleted (0 on failure — the error is logged).
    pub(crate) async fn vacuum_table(&self, project_id: &str, table_name: &str, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) -> usize {
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {retention_hours} hours");

        // Full vacuum also lists unreferenced parquet, so the classification must be
        // serialized with every local writer and refreshed INSIDE the critical section — else
        // a concurrent flush lands a file Full vacuum mistakes for an orphan. The table RwLock
        // alone is insufficient: commit paths clone-update-swap without holding it across IO.
        let commit_lock = self.commit_lock(project_id, table_name).await;
        let _commit_guard = commit_lock.lock().await;
        if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
            error!("Vacuum aborted: failed to refresh '{}' before Full orphan sweep: {}", Self::table_label(project_id, table_name), e);
            return 0;
        }

        // Get a clone so the table RwLock is not held across object-store IO.
        // The per-physical-table commit lock above keeps this snapshot stable.
        let table_clone = { table_ref.read().await.clone() };
        match table_clone
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            // Full also sweeps orphaned parquet whose tombstones already left the retained
            // log; without it, bounding the log turns those files into a permanent leak.
            .with_mode(deltalake::operations::vacuum::VacuumMode::Full)
            .await
        {
            Ok((_, metrics)) => {
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed in {:?}, deleted {} files", start_time.elapsed(), files_deleted);
                if !metrics.files_deleted.is_empty() {
                    debug!("Vacuum operation details: {:?}", metrics.files_deleted);
                }
                if refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await.is_ok() {
                    info!("Table state updated after vacuum");
                } else {
                    error!("Failed to update table state after vacuum");
                }
                files_deleted
            }
            Err(e) => {
                error!("Vacuum operation failed: {e}");
                0
            }
        }
    }

    /// Out-of-band checkpoint + expired-log cleanup for one table. Runs on the maintenance
    /// schedule rather than the delta-rs commit hook (which `base_commit_properties` disables),
    /// so a checkpoint PUT or log delete failure cannot fail a landed commit. Best-effort.
    /// Checkpoints only when the version advanced by at least `checkpoint_interval`, tracked
    /// in-memory per table URL, so idle tables are skipped.
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
        // Gauge: max lag seen this tick (the job resets it to 0 first).
        crate::observability::maintenance_stats().checkpoint_lag_versions.fetch_max(lag, Relaxed);
        if lag < interval {
            return;
        }
        // Each store-heavy op is individually bounded so one wedged call can't starve the
        // sweep. 600s is far above any real catch-up, so hitting it means a stuck backend.
        // Dropping the future mid-checkpoint is safe: the PUT is atomic and retried next tick.
        const CHECKPOINT_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
        match tokio::time::timeout(CHECKPOINT_OP_TIMEOUT, deltalake::checkpoints::create_checkpoint(&table, None)).await {
            Ok(Ok(())) => {
                // Verify the checkpoint is readable Parquet before advancing the boundary or
                // letting cleanup prune JSON behind it: a foreign/corrupt checkpoint object
                // must never gate cleanup, since the JSON log is the only recovery source.
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
                            "checkpoint for '{table_name}' at v{version} is unreadable after write (foreign/corrupt object) — withholding log cleanup to preserve the JSON recovery log; PAGE"
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
        // Log cleanup prunes only up to a checkpoint boundary, so it must follow a successful
        // checkpoint. Uses the table's logRetentionDuration.
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
    /// The `Remove` only stops queries 404-ing on dead paths; a nonzero count means committed
    /// data was destroyed elsewhere, so it is logged and counted.
    async fn reconcile_dangling_adds(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let table = { table_ref.read().await.clone() };
        match table.filesystem_check().with_commit_properties(base_commit_properties()).await {
            Ok((_, metrics)) => {
                let n = metrics.files_removed.len();
                if n > 0 {
                    crate::observability::record_dangling_removed(n as u64);
                    warn!("reconcile: '{table_name}' had {n} dangling Add(s) (committed parquet missing from store) — Remove'd: {:?}", metrics.files_removed);
                    let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
                }
            }
            Err(e) => {
                crate::observability::maintenance_stats().reconcile_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!("reconcile filesystem_check failed for '{}': {} (retry next tick)", table_name, e);
            }
        }
    }

    /// One out-of-band checkpoint + log-cleanup tick across every registered table.
    pub async fn run_checkpoint_maintenance(&self) {
        // Reset the lag gauge so it reflects THIS tick's worst table.
        crate::observability::maintenance_stats().checkpoint_lag_versions.store(0, std::sync::atomic::Ordering::Relaxed);
        for (_project_id, name, table) in self.all_tables().await {
            self.checkpoint_and_cleanup_table(&table, &name).await;
        }
    }

    /// One dangling-Add reconcile tick across every registered table.
    pub async fn run_reconcile_maintenance(&self) {
        for (_project_id, name, table) in self.all_tables().await {
            self.reconcile_dangling_adds(&table, &name).await;
        }
    }

    /// Test-only: run `probe_commit_landed` against the table's current active files.
    /// True iff the probe reports `Landed` (every active file's object is present).
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_probe_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let adds: Vec<deltalake::kernel::Action> = {
            let guard = table_ref.read().await;
            guard.snapshot()?.log_data().iter().map(|f| deltalake::kernel::Action::Add(add_action(&f))).collect()
        };
        Ok(matches!(self.probe_commit_landed(&table_ref, &adds).await, CommitProbe::Landed))
    }

    /// Test-only: probe with a fabricated Add whose path was never committed; the probe must
    /// report NOT landed.
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_probe_bogus_not_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let bogus = deltalake::kernel::Action::Add(deltalake::kernel::Add {
            path: "project_id=nope/date=1970-01-01/part-never-committed.parquet".to_string(),
            size: 1,
            data_change: true,
            ..Default::default()
        });
        Ok(matches!(self.probe_commit_landed(&table_ref, &[bogus]).await, CommitProbe::NotLanded))
    }

    /// Test-only: number of `.checkpoint.parquet` objects in the table's `_delta_log`.
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

    /// Test-only: delete the first active parquet object of a table directly from the store
    /// (no Delta commit), leaving a dangling Add. Returns the deleted relative path.
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
    /// list — `project_id` empty for unified tables (shared by all default projects).
    ///
    /// A SNAPSHOT by design: maintenance passes must iterate this rather than hold a table-map
    /// read guard across awaits, since tokio's write-preferring RwLock would let one queued
    /// writer block every later reader and wedge all maintenance jobs.
    pub(crate) async fn all_tables(&self) -> Vec<(String, String, Arc<RwLock<DeltaTable>>)> {
        let mut out: Vec<(String, String, Arc<RwLock<DeltaTable>>)> =
            self.unified_tables.read().await.iter().map(|(n, t)| (String::new(), n.clone(), t.clone())).collect();
        out.extend(self.custom_project_tables.read().await.iter().map(|((p, n), t)| (p.clone(), n.clone(), t.clone())));
        out
    }

    /// Human label for a table from `all_tables`, as used in per-job log lines.
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

    /// Foyer cache handle (None if Foyer is disabled).
    pub fn object_store_cache(&self) -> Option<&Arc<SharedFoyerCache>> {
        self.object_store_cache.as_ref()
    }

    /// Invalidate statistics for a specific table
    pub async fn invalidate_table_statistics(&self, project_id: &str, table_name: &str) {
        self.statistics_extractor.invalidate(project_id, table_name).await
    }

    /// Signal maintenance/background tasks (scheduler, dedup sweep, coalescer) to stop.
    /// Idempotent; `shutdown()` also fires it.
    pub fn cancel_maintenance(&self) {
        self.maintenance_shutdown.cancel();
    }

    /// True once maintenance/background tasks have been told to stop.
    pub fn is_maintenance_cancelled(&self) -> bool {
        self.maintenance_shutdown.is_cancelled()
    }

    /// Clone for long-lived background tasks: omits the cancel guard, so a task waiting on
    /// `maintenance_shutdown` cannot keep its own kill-switch alive and block last-drop
    /// cancellation.
    pub(crate) fn background_clone(&self) -> Self {
        Self { _maintenance_cancel_guard: None, ..self.clone() }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_by(tokio::time::Instant::now() + self.config.buffer.stop_grace()).await
    }

    /// Graceful shutdown. Every phase that can block on a slow Delta/S3 backend is bounded by
    /// `deadline` — the remainder of the process-wide stop grace shared with
    /// `BufferedWriteLayer::shutdown_by`. Un-drained deferred Delta legs are the coalescer's
    /// documented crash-equivalent loss (mem-leg values survive in the WAL).
    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> Result<()> {
        info!("Shutting down TimeFusion database...");

        // Flush deferred DML merges before anything is torn down. Drains are serialized and
        // idempotent, so doing it here cannot race the drain task's own final drain.
        if let Some(coalescer) = self.dml_coalescer()
            && tokio::time::timeout_at(deadline, coalescer.drain(self)).await.is_err()
        {
            warn!("DML coalescer drain exceeded shutdown deadline — un-drained deferred Delta legs lost (crash-equivalent; mem-leg values survive in WAL)");
        }

        // The rollup journal's durable write is throttled on the commit path, so shutdown is
        // the one point with no later commit to carry a deferred one. Best-effort: it is
        // scheduling state, and losing it only costs a conservative rebuild.
        if let Err(error) = self.flush_rollup_journal() {
            warn!(%error, event = "rollup_journal_shutdown_flush_failed");
        }

        self.maintenance_shutdown.cancel();

        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            if tokio::time::timeout_at(deadline, queue.shutdown()).await.is_err() {
                warn!("Batch queue shutdown exceeded shutdown deadline — proceeding with process teardown");
            }
        }

        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown_by(deadline).await?;
        }

        if let Some(ref pool) = self.config_pool
            && tokio::time::timeout_at(deadline, pool.close()).await.is_err()
        {
            warn!("PostgreSQL pool close exceeded shutdown deadline — dropping connections on process exit");
        }

        info!("Database shutdown complete");
        Ok(())
    }
}

/// Append newline-terminated records to a maintenance state file, creating its parent
/// directory if needed. Callers must hold the file's lock and wrap this in
/// `without_blocking_the_worker`; these files are cleanup aids, never correctness boundaries.
fn append_state_lines(path: &std::path::Path, lines: &[String]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    for line in lines {
        writeln!(file, "{line}")?;
    }
    Ok(())
}

/// A masked pass's evidence for the DV-visibility guard: the pre-pass live
/// `(path, dv_id)` set and the pass's own committed DV attachments.
type MaskedPass<'a> = (&'a HashSet<DvEntry>, &'a [DvEntry]);

/// Dirty verdict for one completed dedup pass folding into clean-slice coverage.
///
/// A masked-in-place (DV) pass may drop rows and stay CLEAN: its commit is
/// `Remove(old) + Add(same path, +DV)`, so the post-state is exactly what it just proved
/// duplicate-free. A CoW pass that dropped rows rewrote files and proves nothing about the
/// result. `dv_moved` is the masked arm's own fail-closed check ([`dv_visibility_moved`]).
fn slice_pass_dirty(dropped: u64, masked: bool, post_empty: bool, fp_moved: bool, dv_moved: bool) -> bool {
    (!masked && dropped != 0) || post_empty || fp_moved || dv_moved
}

/// DV-visibility guard for the masked arm: has the live `(path, dv_unique_id)`
/// set diverged from the EXPECTED post-state — `pre` with THIS pass's own
/// committed DV attachments applied?
///
/// NOT a naive pre-vs-post compare: the pass's own commit replaces the dv_id of every file it
/// touched, so that would decline every masked pass with losers. Anything live beyond the
/// expected set is a foreign interleaved same-path DV commit ⇒ dirty. Sets, not a path-keyed
/// map: Delta replay keys file actions on the (path, dv_id) PAIR, so a stale Remove can leave
/// TWO live adds for one path and a map would collapse them.
fn dv_visibility_moved(pre: &HashSet<DvEntry>, attachments: &[DvEntry], live: &HashSet<DvEntry>) -> bool {
    let touched: HashSet<&str> = attachments.iter().map(|(p, _)| p.as_str()).collect();
    let expected: HashSet<DvEntry> = pre.iter().filter(|(p, _)| !touched.contains(p.as_str())).cloned().chain(attachments.iter().cloned()).collect();
    expected != *live
}

#[cfg(test)]
mod pressure_scaling_tests {
    use super::Database;

    /// The taper must be flat until the lane's pool is half full, then fall linearly to HALF —
    /// never below, never above, and never on an out-of-range reading.
    #[test_case::test_case(0.0, 1.0; "empty pool: full target")]
    #[test_case::test_case(0.5, 1.0; "half full: still full target")]
    #[test_case::test_case(0.75, 0.75; "three-quarters: three-quarter target")]
    #[test_case::test_case(1.0, 0.5; "full pool: half target, the floor")]
    #[test_case::test_case(1.5, 0.5; "over-committed reading clamps at the floor")]
    #[test_case::test_case(-0.2, 1.0; "negative reading clamps at full")]
    fn taper_is_flat_then_linear_to_half(occupancy: f64, expected: f64) {
        let got = Database::pressure_factor(occupancy);
        assert!((got - expected).abs() < 1e-9, "occupancy {occupancy} -> {got}, expected {expected}");
        assert!((0.5..=1.0).contains(&got), "the factor must never leave [0.5, 1.0]: {got}");
    }

    /// A NaN pool reading must not produce a NaN target: `clamp` panics on a NaN bound but
    /// propagates a NaN value, so this pins the actual behaviour.
    #[test]
    fn a_nan_reading_does_not_silently_zero_the_target() {
        let got = Database::pressure_factor(f64::NAN);
        assert!(got.is_nan() || (0.5..=1.0).contains(&got), "unexpected NaN handling: {got}");
    }
}

#[cfg(test)]
mod date_coverage_recovery_tests {
    use super::*;

    /// The read path serves `[day_start, covered_through)` from a date entry, so a hole
    /// anywhere in the run would claim hours no build aggregated: a gap must refuse outright
    /// rather than fall back to the maximum end.
    ///
    /// Cases are in whole HOURS; the body scales to the micros the function takes and back.
    #[test_case::test_case(1, vec![(1, 2), (2, 5)] => Some(5) ; "contiguous from the first row: answers the end of the run")]
    #[test_case::test_case(1, vec![(1, 3), (2, 4)] => Some(4) ; "overlapping slices are still a run")]
    #[test_case::test_case(1, vec![(2, 5), (1, 2)] => Some(5) ; "out of order input is sorted, not rejected")]
    #[test_case::test_case(1, vec![(1, 2), (3, 5)] => None ; "a HOLE between 2h and 3h: refuse, do not answer 5h")]
    #[test_case::test_case(1, vec![(2, 5)] => None ; "coverage starts AFTER the partition's first row, so the opening rows were never aggregated")]
    #[test_case::test_case(2, vec![(1, 5)] => Some(5) ; "starting at or before the first row is fine; the read path clamps")]
    #[test_case::test_case(1, vec![] => None ; "no coverage at all")]
    fn a_gap_refuses_and_only_a_true_run_answers(partition_min_hour: i64, hour_spans: Vec<(i64, i64)>) -> Option<i64> {
        const HOUR: i64 = 3_600_000_000;
        let mut spans: Vec<(i64, i64)> = hour_spans.into_iter().map(|(s, e)| (s * HOUR, e * HOUR)).collect();
        Database::contiguous_coverage_end(partition_min_hour * HOUR, &mut spans).map(|end| end / HOUR)
    }

    /// A ramping tier must not pin the fleet gauge; a starved one must. Coverage-short mode
    /// OVERRIDES the journal ceiling fleet-wide, so one auxiliary ramping tier must not put
    /// the whole cluster into it.
    ///
    /// The fold must also be ORDER-INDEPENDENT — a ramping tier seen first seeds only
    /// provisionally and a real tier OVERWRITES it — and an all-ramping fresh deployment must
    /// still publish something rather than leave the gauge at its start value.
    #[test_case::test_case(0, 30, false, false => Some(30) ; "a real tier always counts and seeds the gauge")]
    #[test_case::test_case(30, 2, true, true => None ; "a ramping tier must not drag an established fleet value down")]
    #[test_case::test_case(0, 2, false, true => Some(2) ; "with nothing real yet, even a ramping tier is better than no gauge")]
    #[test_case::test_case(2, 30, false, false => Some(30) ; "the first real tier REPLACES a provisional value")]
    #[test_case::test_case(u64::MAX, 3, false, true => Some(3) ; "an all-ramping fresh deployment still publishes something")]
    #[test_case::test_case(30, 5, true, false => Some(5) ; "a genuinely starved tier still pins the fleet")]
    #[test_case::test_case(5, 30, true, false => Some(5) ; "min, not last-writer")]
    fn a_ramping_tier_abstains_from_the_fleet_gauge_but_a_starved_one_pins_it(previous: u64, value: u64, seeded_by_real: bool, ramping: bool) -> Option<u64> {
        fold_fleet_gauge(previous, value, seeded_by_real, ramping)
    }

    /// The probe phase must not queue the only groups that can GRANT behind the
    /// ones that provably cannot.
    ///
    /// Every probe in the phase shares ONE deadline and takes what is left of it, so POSITION
    /// IS BUDGET. `certify_only` groups carry no dirty bins and are the only source of a
    /// certification; groups that carry dirty bins decline by construction.
    #[test]
    fn certification_probes_are_not_queued_behind_probes_that_cannot_certify() {
        let dirty: Vec<&str> = vec!["d0", "d1", "d2", "d3"];
        let certify: Vec<&str> = vec!["c0", "c1"];

        let order = super::interleave_probe_groups(dirty.clone(), certify.clone(), 32);
        let first_certify = order.iter().position(|g| g.starts_with('c')).expect("a certify-only group must be scheduled");
        assert!(first_certify <= 1, "a certify-only probe must reach the front, got position {first_certify} in {order:?}");
        assert_eq!(order.len(), 6, "interleaving must not drop groups when under the cap");

        // The dirty class keeps its full volume — this changes order, not volume.
        let many_certify: Vec<&str> = (0..50).map(|_| "c").collect();
        let order = super::interleave_probe_groups(dirty.clone(), many_certify, 2 * 16);
        assert_eq!(order.iter().filter(|g| g.starts_with('d')).count(), dirty.len(), "every dirty group must survive");

        // A cap still bounds the total, since an unprobeable tail is waste.
        let flood: Vec<&str> = (0..100).map(|_| "d").collect();
        assert_eq!(super::interleave_probe_groups(flood, vec!["c0"], 32).len(), 32, "the cap bounds the phase");
    }

    /// Every maintenance SQL that can hold a unit's whole deadline must be
    /// WATCHED, or the unit looks stalled while it works.
    ///
    /// `collect()` reports nothing until it returns and the liveness clock reads the plan's own
    /// `output_rows`, so an unwatched aggregate looks stalled and gets killed mid-work. A
    /// source-level guard because the failure is the ABSENCE of a call.
    #[test]
    fn maintenance_aggregates_are_collected_watched() {
        let source = include_str!("maintain.rs");
        let unwatched: Vec<_> = source
            .lines()
            .enumerate()
            // Built by `concat!` so this line does not match itself; `expect(` excludes test
            // code, since production maintenance SQL uses `?`/`map_err`, never `expect`.
            .filter(|(_, line)| line.contains(concat!("ctx.sql", "(")) && line.contains(concat!(".collect", "().await")) && !line.contains("expect("))
            .map(|(n, line)| format!("{}: {}", n + 1, line.trim()))
            .collect();
        assert!(
            unwatched.is_empty(),
            "these run a maintenance SQL to completion without a liveness watcher — use `collect_watched`:\n{}",
            unwatched.join("\n")
        );
    }
}

#[cfg(test)]
mod certify_on_completion_tests {
    use serial_test::serial;

    use super::*;
    use crate::support::test_helpers::{TestConfigBuilder, delta_physical_row_count, json_to_batch, test_span_ts};

    /// The dirty verdict: which completed passes may still certify their slice.
    #[test_case::test_case(3, true, false, false, false => false; "masked fp-stable complete pass is CLEAN despite drops")]
    #[test_case::test_case(3, false, false, false, false => true; "CoW pass that dropped rows stays DIRTY")]
    #[test_case::test_case(3, true, false, false, true => true; "masked pass with a foreign DV in live-post is DIRTY")]
    #[test_case::test_case(0, false, false, false, false => false; "zero-drop clean pass is unchanged")]
    #[test_case::test_case(3, true, false, true, false => true; "fp movement still declines a masked pass")]
    #[test_case::test_case(0, true, true, false, false => true; "an empty partition proves nothing, masked or not")]
    fn slice_dirty_cases(dropped: u64, masked: bool, post_empty: bool, fp_moved: bool, dv_moved: bool) -> bool {
        slice_pass_dirty(dropped, masked, post_empty, fp_moved, dv_moved)
    }

    fn e(p: &str, dv: Option<&str>) -> DvEntry {
        (p.into(), dv.map(Into::into))
    }

    /// The guard applies THIS pass's own attachments before comparing; a naive pre-vs-post
    /// compare would decline every masked pass with losers.
    ///
    /// `pre` is `{(a,-), (b,-)}` and this pass attaches `dv1` to `a`; each case is the LIVE
    /// post-state. The last case is the stale-Remove double-add shape (both (a,dv1) and (a,dvX)
    /// live), which a path-keyed map would collapse but the set compare must not.
    #[test_case::test_case(vec![("a", Some("dv1")), ("b", None)] => false ; "exactly our commit: clean")]
    #[test_case::test_case(vec![("a", Some("dv1")), ("b", Some("dvX"))] => true ; "a foreign DV on an untouched file: dirty")]
    #[test_case::test_case(vec![("a", Some("dvX")), ("b", None)] => true ; "a foreign replacement of our own attachment: dirty")]
    #[test_case::test_case(vec![("a", Some("dv1")), ("a", Some("dvX")), ("b", None)] => true ; "stale-Remove double-add: two live adds for one path")]
    fn dv_guard_expects_own_attachments_and_declines_foreign_ones(live: Vec<(&str, Option<&str>)>) -> bool {
        let pre: HashSet<DvEntry> = [e("a", None), e("b", None)].into();
        dv_visibility_moved(&pre, &[e("a", Some("dv1"))], &live.into_iter().map(|(p, dv)| e(p, dv)).collect())
    }

    const TABLE: &str = "otel_logs_and_spans";

    struct Pass {
        db: Database,
        table_ref: Arc<RwLock<DeltaTable>>,
        project_id: String,
        date: chrono::NaiveDate,
        pre_files: Vec<String>,
        pre_dv: HashSet<DvEntry>,
        dropped: u64,
        masked: Option<Vec<DvEntry>>,
    }

    /// Seeds a cross-file duplicate on a sealed past day (two Delta commits → two files),
    /// snapshots the pre-state the certification guard compares against, and runs ONE dedup
    /// pass, asserting the single drop so each arm below states only what is distinct.
    async fn dedup_pass(cfg: Arc<crate::config::AppConfig>) -> Result<Pass> {
        let db = Database::with_config(cfg).await?;
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
        let row = |name: &str| json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]);
        db.insert_records_batch(&project_id, TABLE, vec![row("first")?], true, None).await?;
        db.insert_records_batch(&project_id, TABLE, vec![row("second")?], true, None).await?;
        let table_ref = db.unified_tables().read().await.get(TABLE).expect("table created").clone();
        let date = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive();
        let (pre_files, pre_dv) = {
            let (table, marker) = (table_ref.read().await, format!("date={date}"));
            let files = Database::partition_files_by_pid(&table, &marker)?.remove(&project_id).unwrap_or_default();
            (files, Database::partition_dv_state(&table, &project_id, &marker)?)
        };
        let (dropped, complete, masked) = db.dedup_partition_range_limited(&table_ref, TABLE, &project_id, date, None, None).await?;
        assert_eq!((dropped, complete), (1, true), "expected one duplicate dropped in a complete pass");
        Ok(Pass { db, table_ref, project_id, date, pre_files, pre_dv, dropped, masked })
    }

    impl Pass {
        /// `None` is a CoW pass (no masked arm); `Some(a)` is a masked pass declaring `a` as the DV attachments it committed.
        async fn certify(&self, attachments: Option<&[DvEntry]>) -> Result<Option<u64>> {
            let masked: Option<MaskedPass<'_>> = attachments.map(|a| (&self.pre_dv, a));
            self.db.record_clean_slice(&self.table_ref, TABLE, &self.project_id, self.date, (day_slice(self.date), self.dropped, masked), &self.pre_files).await
        }

        /// Is a LIVE (non-stale) whole-day certification recorded for the day?
        fn certified(&self) -> bool {
            self.db.dedup_clean_fp.get(&(self.project_id.clone(), TABLE.to_string(), self.date.to_string())).is_some_and(|c| !c.stale)
        }
    }

    fn day_slice(date: chrono::NaiveDate) -> crate::maintenance_coordinator::TimeSlice {
        let start = date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
        crate::maintenance_coordinator::TimeSlice { start_micros: start, end_micros: start + crate::maintenance_coordinator::DAY_MICROS }
    }

    /// A masked (DV) pass that dropped a duplicate, completed, and left the URI set unmoved
    /// GRANTS certification in the SAME pass, and the read-side gate turns Granted.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_masked_dv_pass_certifies_the_day_in_the_same_pass() -> Result<()> {
        let mut cfg = (*TestConfigBuilder::new("certify_same_pass").build()).clone();
        cfg.maintenance.timefusion_read_dedup_skip_swept = true;
        let pass = dedup_pass(Arc::new(cfg)).await?;
        let atts = pass.masked.as_deref().expect("a DV-dedup pass must report its masked attachments");

        assert!(pass.certify(Some(atts)).await?.is_some(), "a masked, complete, fp-stable pass must certify the day in the SAME pass");
        assert!(pass.certified(), "the grant must be a live whole-day certification");

        // The read-side gate the planner consults must turn Granted inside the certified day.
        let slice = day_slice(pass.date);
        let verdict = {
            let table = pass.table_ref.read().await;
            pass.db.dedup_window_clean(&table, &pass.project_id, TABLE, (slice.start_micros, slice.end_micros - 1))
        };
        assert_eq!(verdict, DedupSkipVerdict::Granted, "the same-pass certification must reach the read-side skip");
        Ok(())
    }

    /// A masked pass whose live post `(path, dv)` set holds a DV beyond `pre` + its OWN
    /// attachments must DECLINE, fail-closed.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_foreign_dv_in_live_post_declines_the_masked_pass() -> Result<()> {
        let pass = dedup_pass(TestConfigBuilder::new("certify_foreign_dv").build()).await?;
        assert!(pass.masked.is_some(), "precondition: the pass took the DV path");

        // Withhold the attachments: expected post = pre, so the pass's own committed DV is
        // indistinguishable from a foreign interleaved one.
        assert!(pass.certify(Some(&[])).await?.is_none(), "a DV the pass cannot account for must decline certification");
        assert!(!pass.certified(), "no live certification may survive the decline");
        Ok(())
    }

    /// A CoW rewrite that dropped rows still declines (its post-state is new files the pass
    /// never re-verified). What this uniquely pins is the plumbing: a CoW pass reports
    /// `masked = None` end-to-end and never takes the masked exemption. The decline itself is
    /// over-determined here (both `dropped` and `fp_moved` fire).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_cow_pass_that_dropped_rows_still_declines() -> Result<()> {
        let pass = dedup_pass(TestConfigBuilder::new("certify_cow_declines").without_deletion_vectors().build()).await?;
        assert!(pass.pre_dv.iter().all(|(_, dv)| dv.is_none()), "CoW setup must start DV-free");
        assert!(pass.masked.is_none(), "a CoW rewrite must NOT report masked attachments");

        assert!(pass.certify(None).await?.is_none(), "a row-dropping CoW pass must not certify");
        Ok(())
    }

    /// The concurrent-DML race: a DML DELETE writes a DV to the same file BETWEEN dedup's DV
    /// staging and its wave commit. The commit must decline/retry rather than clobber — the
    /// DML's deletion must survive, and no path may end up with two live adds (the
    /// stale-Remove double-add Delta replay permits, since it keys on the (path, dv_id) pair).
    /// The guard is `split_live_bins`' DV-exact target match (`ActiveFiles::matches`).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dml_dv_landing_between_staging_and_commit_is_not_clobbered() -> Result<()> {
        let cfg = TestConfigBuilder::new("certify_dml_race").build();
        let db = Database::with_config(cfg).await?;
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
        // File 1 holds the duplicate AND the row the DML will delete.
        let b1 = json_to_batch(vec![test_span_ts("dup_id", "first", &project_id, ts), test_span_ts("victim", "extra", &project_id, ts)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![b1], true, None).await?;
        let b2 = json_to_batch(vec![test_span_ts("dup_id", "second", &project_id, ts)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![b2], true, None).await?;
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        let date = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive();
        assert_eq!(delta_physical_row_count(&table_ref).await?, 3);

        // Stage (but do not commit) the DV-dedup wave.
        let options = DedupRangeOptions { slice: None, dirty_key: None, limits: None };
        let (units, complete) = db.stage_dedup_partition_range(&table_ref, "otel_logs_and_spans", &project_id, date, options).await?;
        assert!(complete && !units.is_empty(), "expected a staged DV unit");

        // Foreign DML-DV interleaves: delete the victim via the delta DeleteBuilder directly,
        // masking a row in the very file the staged unit targets.
        let dt = { table_ref.read().await.clone() };
        let (dt, metrics) = dt.delete().with_predicate(format!("id = 'victim' and project_id = '{project_id}'")).with_deletion_vectors(true).await?;
        assert_eq!(metrics.num_deleted_rows, Some(1), "the DML DV delete must land");
        *table_ref.write().await = dt;

        let markers = vec![format!("date={date}/")];
        let result = db.commit_wave(&table_ref, "otel_logs_and_spans", &markers, true, units, 0).await;

        // Either outcome is acceptable — landed against the fresh state, or declined — but
        // never a clobber:
        let live: Vec<String> = {
            let table = table_ref.read().await;
            table.snapshot()?.log_data().iter().map(|f| f.path().into_owned()).collect()
        };
        let unique: HashSet<&String> = live.iter().collect();
        assert_eq!(live.len(), unique.len(), "no path may have two live adds (stale-Remove double-add clobber): {live:?}");
        let victims: i64 = {
            let batches = db.query_delta_only(&format!("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'victim'")).await?;
            use datafusion::arrow::array::AsArray;
            batches[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0)
        };
        assert_eq!(victims, 0, "the DML's deletion must survive the dedup wave (landed={}, failed={})", result.landed.len(), result.failed.len());
        Ok(())
    }
}

/// The no-op-rebuild skip: a rollup unit whose input has not moved since the
/// live coverage was published must complete without touching the tier.
#[cfg(test)]
mod rollup_noop_skip_tests {
    use serial_test::serial;

    use super::*;
    use crate::{
        maintenance_coordinator::{MAX_DECODED_BYTES, TaskState},
        support::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span_ts},
    };

    const TIER: &str = "otel_logs_and_spans_rollup_dashboard_1m_v3";

    fn skips() -> u64 {
        crate::observability::maintenance_stats().rollup_noop_rebuild_skipped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `None` until the tier exists — which is itself the "nothing published"
    /// answer, and must not be confused with a version.
    async fn tier_version(db: &Database) -> Option<u64> {
        let table = db.resolve_table("default", TIER).await.ok()?;
        table.read().await.version()
    }

    /// The re-mint the derived tier's `skipped_generation` branch performs: `journal.enqueue`
    /// on the BASE key it could not verify, upserting an already-Complete unit back to Pending
    /// over the SAME slice. Coverage is untouched — unlike the CONTENT-change path
    /// (`apply_rollup_hours`) — so the rebuild it asks for must be proved needless.
    fn remint_published_base_slices(db: &Database) -> Result<()> {
        let published: Vec<_> = {
            let journal = db.journal();
            journal
                .tasks()
                .filter(|task| task.key.operation == crate::maintenance_coordinator::Operation::BaseRollup && task.state == TaskState::Complete)
                .map(|task| task.key.clone())
                .collect()
        };
        assert!(!published.is_empty(), "no BaseRollup unit completed, so there is no published slice to re-mint");
        let now = crate::support::now_micros();
        let mut journal = db.journal();
        for key in published {
            journal.enqueue(key, now, MAX_DECODED_BYTES, unix_ms(now));
        }
        journal.checkpoint()
    }

    /// A rollup-enabled config whose backfill window reaches the `date` these scenarios build
    /// on. The restart test needs the SAME config (and data dir) for two successive `Database`s.
    fn rollup_cfg(name: &str) -> Arc<crate::config::AppConfig> {
        let mut cfg = (*TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
        cfg.maintenance.timefusion_rollup_backfill_days = 7;
        Arc::new(cfg)
    }

    /// A rollup-enabled db, plus the project and date it writes under.
    async fn rollup_db(name: &str) -> Result<(Arc<Database>, String, chrono::NaiveDate)> {
        let db = Arc::new(Database::with_config(rollup_cfg(name)).await?);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        Ok((db, project_id, chrono::Utc::now().date_naive() - chrono::Duration::days(3)))
    }

    /// One re-mint cycle: re-mint every published slice, then drain.
    async fn remint_and_drain(db: &Database) -> Result<()> {
        remint_published_base_slices(db)?;
        crate::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await?;
        Ok(())
    }

    /// Insert one span on `date` and drive every rollup unit the day produces.
    async fn build_day(db: &Database, project_id: &str, date: chrono::NaiveDate, id: &str) -> Result<usize> {
        let ts = date.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();
        let batch = json_to_batch(vec![test_span_ts(id, "op", project_id, ts)])?;
        db.insert_records_batch(project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;
        db.plan_rollup_backfill().await?;
        crate::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await
    }

    /// The skip must survive a restart: coverage rebuilt from tier tags at boot must carry a
    /// `content_fp` and a nonzero `output_files`, or nothing can be skipped until a slice has
    /// published once more in the new process. The restart is a second `Database` over the
    /// same data dir.
    #[serial]
    #[tokio::test]
    async fn a_restart_recovers_the_proof_the_skip_needs() -> Result<()> {
        let cfg = rollup_cfg("rollup_noop_restart");
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let date = chrono::Utc::now().date_naive() - chrono::Duration::days(3);

        {
            let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
            assert!(build_day(&db, &project_id, date, "seed").await? > 0, "no rollup unit ran, so nothing here says anything about restarts");
        }

        // A brand-new Database over the same data dir; its coverage comes only from the tags.
        let db = Arc::new(Database::with_config(cfg).await?);
        db.recover_rollup_coverage("otel_logs_and_spans").await?;
        let published_at = tier_version(&db).await.expect("the tier must exist after the first process published into it");

        let before = skips();
        remint_published_base_slices(&db)?;
        crate::support::advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(64).await?;
        assert!(skips() > before, "a slice re-minted after a restart must still be proved redundant from its tags");
        assert_eq!(tier_version(&db).await, Some(published_at), "and must not write to the tier");
        Ok(())
    }

    /// A landed dedup must CARRY the rollup witness, not invalidate it.
    ///
    /// The rollup builds from a deduplicated read and the sweep drops the same losers, so the
    /// numbers are unchanged and only the physical witness moved — by exactly `dropped`.
    /// Subtracting it keeps the slice provable instead of forcing a needless rebuild.
    /// Asserts the witness ACTUALLY MOVED by that amount, so a no-op cannot pass.
    #[serial]
    #[tokio::test]
    async fn a_landed_dedup_carries_the_rollup_witness() -> Result<()> {
        let cfg = Arc::new((*TestConfigBuilder::new("rollup_witness_carry").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone());
        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let date = chrono::Utc::now().date_naive() - chrono::Duration::days(3);
        assert!(build_day(&db, &project_id, date, "seed").await? > 0, "no rollup unit ran, so there is no witness to carry");
        db.recover_rollup_coverage("otel_logs_and_spans").await?;

        let witness = |db: &Database| -> Option<u64> {
            db.rollup_slice_coverage.iter().find(|e| e.key().0 == project_id && e.key().1 == "otel_logs_and_spans").and_then(|e| e.value().source_rows)
        };
        let before = witness(&db).expect("recovery must stamp a row witness to carry");

        // Carry a plausible drop through the same entry point the commit site uses.
        db.carry_dedup_witness("otel_logs_and_spans", &project_id, &date.to_string(), 3);
        assert_eq!(witness(&db), Some(before.saturating_sub(3)), "the witness must fall by exactly the rows the dedup dropped");

        // Soundness gate: a table with no declared tiebreak has a non-deterministic winner,
        // so its witness must NOT be carried.
        let untouched = witness(&db);
        db.carry_dedup_witness("no_such_table_without_tiebreak", &project_id, &date.to_string(), 5);
        assert_eq!(witness(&db), untouched, "a table without a declared dedup_tiebreak must never have its witness carried");
        Ok(())
    }

    /// A slice whose witness was OVERTAKEN must be queued to be re-proven.
    ///
    /// `recover_date_coverage` compares each slice's witness against the partition's live row
    /// count; on disagreement the read path independently refuses the slice
    /// (`rollup_stale_moved`), so unless recovery also QUEUES it the cell stays unreadable
    /// until some unrelated invalidation happens along.
    #[serial]
    #[tokio::test]
    async fn an_overtaken_slice_is_queued_to_be_re_proven() -> Result<()> {
        let mut cfg = (*TestConfigBuilder::new("rollup_moved_requeue").with_buffer_mode(BufferMode::Enabled).with_rollups().build()).clone();
        cfg.maintenance.timefusion_rollup_backfill_days = 7;
        let cfg = Arc::new(cfg);
        let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let date = chrono::Utc::now().date_naive() - chrono::Duration::days(3);

        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        assert!(build_day(&db, &project_id, date, "seed").await? > 0, "no rollup unit ran, so there is no witnessed slice to overtake");
        db.recover_rollup_coverage("otel_logs_and_spans").await?;
        assert!(db.rollup_slice_coverage.iter().next().is_some(), "precondition: recovery must have stamped slice coverage to overtake");

        // OVERTAKE it WITHOUT rebuilding: a second row moves the partition's live
        // `num_records` past every stamped witness. Deliberately not `build_day`, which ends in
        // `drain_coordinator_rollups` and would re-publish the slice, restamping the witness.
        let ts = date.and_hms_opt(13, 0, 0).expect("1pm").and_utc().timestamp_micros();
        let batch = json_to_batch(vec![test_span_ts("overtakes-the-witness", "op", &project_id, ts)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

        let before = pending_base_rollups(&db);
        db.recover_rollup_coverage("otel_logs_and_spans").await?;
        assert!(pending_base_rollups(&db) > before, "an overtaken slice must be queued to be re-proven; before={before} after={}", pending_base_rollups(&db));
        Ok(())
    }

    /// BaseRollup tasks in a state that will run — the observable for
    /// "something is queued to re-prove this".
    fn pending_base_rollups(db: &Database) -> usize {
        let journal = db.journal();
        journal
            .tasks()
            .filter(|task| {
                task.key.operation == crate::maintenance_coordinator::Operation::BaseRollup && matches!(task.state, TaskState::Pending | TaskState::Retry)
            })
            .count()
    }

    /// A re-mint puts a Complete task back to Pending over an unmoved input; without the skip
    /// the slice is rebuilt to a byte-identical answer. Both directions are asserted, because
    /// a skip that never rebuilds is the silent failure this guards against.
    ///
    /// The skip has three conjuncts, each pinned by a DIFFERENT test: the switch here,
    /// `content_fp` by the deletion-vector test below (this one's rebuild case clears coverage
    /// outright, so it cannot reach the fingerprint), and `tier_still_holds_slice` by
    /// `rollup_routing_rejects_legacy_materialization_generations`.
    #[serial]
    #[tokio::test]
    async fn a_rollup_whose_input_has_not_moved_completes_without_rebuilding() -> Result<()> {
        let (db, project_id, date) = rollup_db("rollup_noop_skip").await?;

        // A drain of zero units would make every assertion below vacuous.
        assert!(build_day(&db, &project_id, date, "seed").await? > 0, "no rollup unit ran, so nothing here says anything about rebuilding");
        let published_at = tier_version(&db).await.expect("the rollup tier must exist once a unit has published into it");

        let before = skips();
        remint_and_drain(&db).await?;
        assert!(skips() > before, "a re-pended unit over an unmoved input must be proved redundant, not rebuilt");
        assert_eq!(tier_version(&db).await, Some(published_at), "the skip must not write to the tier at all");

        // The other direction: a real row lands, the write path invalidates the hour and drops
        // its coverage, so the proof is gone and the day rebuilds.
        assert!(build_day(&db, &project_id, date, "late-arrival").await? > 0, "the changed day must still produce units");
        assert!(tier_version(&db).await > Some(published_at), "a day whose source actually changed must be rebuilt, not skipped");
        Ok(())
    }

    /// A deletion vector changes a partition's CONTENT without changing a single file PATH,
    /// and a dedup wave does not drop the rollup's coverage either — so a paths-only
    /// fingerprint would report "nothing moved" over a partition that just lost rows and
    /// freeze a rollup that still counts the duplicates. This is what makes `content_fp`
    /// load-bearing.
    #[serial]
    #[tokio::test]
    async fn a_deletion_vector_that_masks_rows_in_place_still_forces_a_rebuild() -> Result<()> {
        let (db, project_id, date) = rollup_db("rollup_noop_dv").await?;
        let ts = date.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();

        // The same id twice, in two commits, so the duplicate spans two files.
        for name in ["first", "second"] {
            let batch = json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        }
        db.plan_rollup_backfill().await?;
        crate::support::advance_micros(16 * 60 * 1_000_000);
        assert!(db.drain_coordinator_rollups(64).await? > 0, "no rollup unit ran, so nothing here says anything about masking");
        let before_dedup = tier_version(&db).await.expect("the rollup tier must exist once a unit has published into it");

        // Mask the loser. Paths do not move; `numRecords` does not move either.
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

        remint_and_drain(&db).await?;
        assert!(
            tier_version(&db).await > Some(before_dedup),
            "a deletion vector masked a duplicate this rollup had already counted; a paths-only fingerprint would have called that unchanged"
        );
        Ok(())
    }
}

/// The rollup journal's two `fsync`s must not be paid on every commit.
#[cfg(test)]
mod rollup_journal_persist_tests {
    use serial_test::serial;

    use super::*;
    use crate::support::test_helpers::TestConfigBuilder;

    fn counts() -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let s = crate::observability::maintenance_stats();
        (s.rollup_journal_persists.load(Relaxed), s.rollup_journal_persist_skipped.load(Relaxed), s.rollup_journal_persist_deferred.load(Relaxed))
    }

    /// `store` costs TWO `fsync`s (temp file, then the parent directory after the rename) and
    /// sat on the pre-ack commit path. Deferring is sound because `rollup_journal` is
    /// scheduling state: `maintenance_tasks` is the finer-grained source of truth and is
    /// fsynced in the SAME commit.
    ///
    /// A content hash alone would not help — `apply_rollup_hours` bumps the source epoch on
    /// every call, so the encoded journal really does change on every ingest invalidation.
    /// Hence the TIME throttle, without which the skip assertion below passes vacuously.
    #[serial]
    #[tokio::test]
    async fn repeated_commits_do_not_each_rewrite_the_rollup_journal() -> Result<()> {
        let cfg = TestConfigBuilder::new("rollup_journal_persist").with_rollups().build();
        let db = Database::with_config(cfg).await?;
        let project = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
        let date = (chrono::Utc::now() - chrono::Duration::days(2)).date_naive().to_string();

        // The first write of a process always happens: there is no stamp yet, and an empty one
        // must not be mistaken for "already persisted".
        db.apply_rollup_hours(&project, "otel_logs_and_spans", &date, 1 << 12)?;
        db.commit_journal()?;
        assert!(counts().0 > 0, "the first persist of a process must write");

        // Every call bumps the source epoch, so the CONTENT differs every time — only the
        // staleness window stops these.
        let before = counts();
        for hour in 0..13u32 {
            db.apply_rollup_hours(&project, "otel_logs_and_spans", &date, 1 << hour)?;
            db.commit_journal()?;
        }
        let after = counts();
        assert_eq!(after.0, before.0, "13 commits inside the staleness window must not each pay two fsyncs");
        assert_eq!(after.2 - before.2, 13, "and each must be counted as deferred, not silently dropped");

        // Deferred is not dropped: shutdown has no later commit to carry the write, so it
        // forces one and the journal on disk holds the hours those commits marked.
        db.flush_rollup_journal()?;
        assert_eq!(counts().0 - after.0, 1, "shutdown must flush what the window deferred");
        let persisted = crate::rollup_journal::load(&db.config.core.timefusion_data_dir);
        let entry =
            persisted.iter().find(|entry| entry.project_id == project && entry.date == date).expect("the partition must be on disk after a forced flush");
        assert_eq!(entry.dirty_hours & 0x1fff, 0x1fff, "every hour marked during the deferred window must have reached disk");

        // Nothing changed since that flush, so the next commit writes nothing even though the
        // window has no say — the idle case.
        let before = counts();
        db.commit_journal()?;
        assert_eq!(counts().1 - before.1, 1, "an unchanged journal must be skipped on content, not merely deferred");
        assert_eq!(counts().0, before.0, "and must not write");
        Ok(())
    }
}

/// The absolute lifetime cap on permit-holding maintenance units.
#[cfg(test)]
mod unit_lifetime_cap_tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::Duration,
    };

    use super::{coordinator_operation_lifetime_cap, run_until_idle_capped};
    use crate::maintenance_coordinator::Operation;

    /// A unit that keeps reporting progress but never finishes must still be
    /// stopped when it holds a permit the rest of the fleet is waiting on.
    ///
    /// `run_until_idle` is an IDLE window by design, which is right when a unit costs only its
    /// worker and wrong when it holds one of the few `light_rewrite_sem` permits — there it
    /// bounds the hold at nothing at all. Both directions, because a cap that fires on
    /// converging work is its own outage.
    #[tokio::test(start_paused = true)]
    async fn a_progressing_unit_that_never_converges_is_still_stopped() {
        let idle = Duration::from_millis(900);
        let cap = Duration::from_millis(3_600);
        let progress = Arc::new(AtomicU64::new(0));
        let ticker = Arc::clone(&progress);
        // Never returns, but reports progress inside every idle window.
        let forever = async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                ticker.fetch_add(1, Relaxed);
            }
        };
        let started = tokio::time::Instant::now();
        assert!(run_until_idle_capped(idle, Some(cap), progress, forever).await.is_err(), "a unit past its lifetime cap must be stopped");
        assert!(started.elapsed() >= cap, "and not before the cap");

        // The other direction: work that FINISHES inside the cap is untouched,
        // including work slower than a single idle window.
        let progress = Arc::new(AtomicU64::new(0));
        let ticker = Arc::clone(&progress);
        let converging = async move {
            for _ in 0..12 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                ticker.fetch_add(1, Relaxed);
            }
            7u32
        };
        assert_eq!(run_until_idle_capped(idle, Some(cap), progress, converging).await.ok(), Some(7), "converging work must not be killed");
    }

    /// Only the lanes that hold a permit get a cap: a rollup unit costs its worker and nothing
    /// else, and its cost is set by input FILE COUNT, so bisecting it converges on nothing.
    #[test]
    fn only_the_permit_holding_lanes_are_capped() {
        for operation in [Operation::HotPacking, Operation::SealedConsolidation, Operation::Repair] {
            let cap = coordinator_operation_lifetime_cap(operation).expect("permit-holding lanes are capped");
            assert_eq!(cap, crate::database::coordinator_operation_timeout(operation) * 4, "{operation:?}");
        }
        for operation in [Operation::BaseRollup, Operation::DerivedRollup, Operation::Dedup] {
            assert!(coordinator_operation_lifetime_cap(operation).is_none(), "{operation:?} holds no permit and must keep idle-only semantics");
        }
    }
}
