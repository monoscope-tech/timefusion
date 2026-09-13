//! Replays a `TaskJournal` through the real scheduler on virtual time.
//!
//! Task selection, timeout handling, cycle switching, invalidation and the
//! claim-time byte preflight are real. Durations, ingest cadence and the bytes
//! a slice decodes are modeled ([`ByteModel`]). Memory admission and intra-call
//! operation order are outside the model.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering::Relaxed;

use anyhow::Context as _;
use itertools::Itertools;
use serde::Serialize;

use crate::database::{coverage_is_short_for, median_contiguous_days};
use crate::maintenance_coordinator::{
    DAY_MICROS, InputFootprint, Invalidation, LIVE_FRONTIER_WINDOW_MICROS, MAX_DECODED_BYTES, MIN_SLICE_MICROS, MaintenanceTask, NORMAL_SLICE_MICROS,
    Operation, STARVATION_HORIZON_MICROS, TaskJournal, TaskKey, TaskState, TimeSlice, operation_cycle, operation_deadline_secs, split_sheds_enough_at,
};

const MICROS: i64 = 1_000_000;
const HOUR_MICROS: i64 = 3_600_000_000;
/// Matches the write path's bucket cadence.
const MINT_INTERVAL_MICROS: i64 = NORMAL_SLICE_MICROS;
/// Lets idle workers notice newly mature deadlines.
const IDLE_POLL_MICROS: i64 = 5 * MICROS;
/// Coarsening cadence, matching `run_maintenance_coordinator_once`. It is the
/// only mechanism that SHRINKS the queue, so the sim cannot omit it.
const COARSEN_INTERVAL_MICROS: i64 = 60 * MICROS;
/// A claim, a scan estimate and a journal write. Charged whether the preflight
/// splits or dispatches, so a bisection ladder costs throughput in the model.
const PREFLIGHT_COST_MICROS: i64 = MICROS;

/// Decoded bytes of one parquet file, and the least a slice can read of a file
/// it overlaps (row groups are the pruning unit — a slice cannot read less than
/// one).
const FILE_DECODED_BYTES: u64 = 9_200_000;
const ROW_GROUP_BYTES: u64 = 5_000_000;
/// How much of a day one file's rows span. Files are not time-sorted, so a
/// narrow slice still overlaps a large fraction of them — this is the term that
/// stops the cost falling with the width, i.e. the floor itself.
const FILE_SPAN_MICROS: i64 = DAY_MICROS / 20;

/// One (project, day) partition's shape, in the only two numbers the cost
/// model needs.
#[derive(Clone, Copy, Debug)]
pub struct DayShape {
    pub decoded_bytes: u64,
    pub files: u64,
}

impl DayShape {
    pub fn new(decoded_bytes: u64) -> Self {
        Self { decoded_bytes, files: decoded_bytes.div_ceil(FILE_DECODED_BYTES).max(1) }
    }

    /// What a slice of `width_micros` over this day decodes.
    ///
    /// `floored`: a slice reads at least one row group of every file it
    /// overlaps, and the overlapping count bottoms out because files span time.
    /// Floorless prices bytes strictly proportional to width (the control).
    ///
    /// ```
    /// # use timefusion::maintenance_sim::DayShape;
    /// let day = DayShape::new(9_200_000_000);
    /// assert_eq!(day.files, 1_000);
    /// let five_minutes = day.bytes(300 * 1_000_000, true);
    /// assert!((five_minutes as i64 - 302_000_000).abs() < 3_000_000, "{five_minutes}");
    /// assert!(day.bytes(300 * 1_000_000, false) < 32_000_000);
    /// ```
    pub fn bytes(&self, width_micros: i64, floored: bool) -> u64 {
        let width = width_micros.max(0) as u128;
        let proportional = (u128::from(self.decoded_bytes) * width / u128::from(DAY_MICROS as u64)) as u64;
        if floored { proportional.saturating_add(self.files_overlapping(width_micros).saturating_mul(ROW_GROUP_BYTES)) } else { proportional }
    }

    fn files_overlapping(&self, width_micros: i64) -> u64 {
        let span = FILE_SPAN_MICROS.saturating_add(width_micros.max(0)) as u128;
        ((u128::from(self.files) * span).div_ceil(u128::from(DAY_MICROS as u64)) as u64).clamp(1, self.files)
    }
}

/// The claim-time cost model: what the preflight measures, per
/// (project, day) partition.
#[derive(Clone, Debug, Default)]
pub struct ByteModel {
    pub floored: bool,
    pub days: HashMap<(String, i64), DayShape>,
}

impl ByteModel {
    pub fn insert(&mut self, project_id: &str, day_start_micros: i64, decoded_bytes: u64) {
        self.days.insert((project_id.to_owned(), day_start_micros), DayShape::new(decoded_bytes));
    }

    fn shape(&self, project_id: &str, day_start_micros: i64) -> Option<DayShape> {
        self.days.get(&(project_id.to_owned(), day_start_micros)).copied()
    }

    /// Unmodelled partitions measure 0, so they never split.
    fn bytes(&self, key: &TaskKey) -> u64 {
        self.shape(&key.project_id, day_start(key.slice.start_micros)).map_or(0, |day| day.bytes(key.slice.width(), self.floored))
    }

    /// The file set the slice overlaps. Siblings of equal width over the same
    /// partition share an `fp`, so fusion charges them once.
    fn footprint(&self, key: &TaskKey) -> Option<InputFootprint> {
        let day_start = day_start(key.slice.start_micros);
        let day = self.shape(&key.project_id, day_start)?;
        let files = day.files_overlapping(key.slice.width());
        let whole = (u128::from(day.decoded_bytes) * u128::from(files) / u128::from(day.files)) as u64;
        Some(InputFootprint::new([format!("{}/{day_start}/{files}", key.project_id)], whole))
    }

    /// `coarsen_sealed_slices_capped`'s ceiling: no unit over one partition can
    /// decode more than the partition holds.
    fn partition_ceiling(&self, project_id: &str, date: &str) -> Option<u64> {
        let day = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        self.shape(project_id, day).map(|shape| shape.decoded_bytes)
    }
}

/// Which split guard the preflight runs under. `Shipped` is the real
/// `split_time_task` predicate; the others exist to answer "compared to what".
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SplitGuard {
    /// The real `split_time_task` predicate, as shipped.
    #[default]
    Shipped,
    /// No guard: every over-budget unit bisects.
    Off,
    /// Sweep an alternative shed threshold (numerator, denominator), keeping
    /// the shipped rule's two-sided shape — a child measuring MORE than its
    /// parent must still split, or the synthetic-stamp lineage freezes.
    Ratio(u64, u64),
}

#[derive(Clone, Debug, educe::Educe)]
#[educe(Default)]
pub struct SimConfig {
    #[educe(Default = 16)]
    pub workers: usize,
    /// Virtual time to simulate.
    #[educe(Default(expression = 24 * 60 * 60 * MICROS))]
    pub horizon_micros: i64,
    /// Model ongoing ingest invalidations for the streams found in the journal.
    #[educe(Default = true)]
    pub mint_frontier: bool,
    /// Override the number of INGESTING streams. Extra streams clone a real
    /// active stream's tables under synthetic project ids.
    pub streams: Option<usize>,
    #[educe(Default = 1.0)]
    pub duration_scale: f64,
    /// Model restarts: re-invalidate the CURRENT HOUR for every stream, which
    /// is what `reconcile_maintenance_task_cursors` does on boot.
    ///
    /// `restart_every_micros` repeats on an interval (0 = no periodic restarts);
    /// `restart_at_micros` fires ONE restart at a fixed offset.
    pub restart_every_micros: i64,
    pub restart_at_micros: Option<i64>,
    #[educe(Default = 0x5EED)]
    pub seed: u64,
    /// Model what a claimed slice decodes, and run the claim-time preflight
    /// against it. Without one the sim never splits on bytes.
    pub byte_model: Option<ByteModel>,
    pub split_guard: SplitGuard,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SimSample {
    pub hour: f64,
    pub pending: usize,
    pub frontier_lag_secs: u64,
    pub min_contiguous_days: u64,
    /// Cumulative, and the live unit count of the worst cell beside it.
    pub split_declined_at_floor: u64,
    pub max_cell_pending: usize,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SimReport {
    pub hours: f64,
    pub completions: HashMap<String, u64>,
    pub timeouts: HashMap<String, u64>,
    pub splits: u64,
    pub executions: u64,
    pub pending_start: usize,
    pub pending_end: usize,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub tasks_end: HashMap<String, usize>,
    /// What coarsening did over the run: `subsumed` + `fused` is queue removed;
    /// `over_budget` against `candidates` is the fusion that was refused.
    pub coarsen_subsumed: usize,
    pub coarsen_fused: usize,
    pub coarsen_candidates: usize,
    pub coarsen_blocked: usize,
    pub coarsen_over_budget: usize,
    pub frontier_lag_secs_max: u64,
    pub min_contiguous_days_end: u64,
    pub hours_to_contiguous_14: Option<f64>,
    pub hours_to_contiguous_30: Option<f64>,
    /// Byte preflight, cumulative. `byte_splits` are units the preflight
    /// bisected before dispatch; `split_declined_at_floor` is the shipped
    /// counter's delta plus the sweep's own declines.
    pub preflight_measures: u64,
    pub byte_splits: u64,
    pub split_declined_at_floor: u64,
    /// Units that ran over budget and were divided by the runner's internal
    /// hash sharding instead of by another journal unit.
    /// `narrowest_sharded_run_micros` must stay above `MIN_SLICE_MICROS`:
    /// reaching the floor is the shred.
    pub sharded_runs: u64,
    pub sharded_runs_above_min_slice: u64,
    /// Claims bucketed by the DATA AGE of the slice, which is what `starved`
    /// ranks on. `starved` improves monotonically past
    /// `STARVATION_HORIZON_MICROS`, so a very old cohort that keeps failing and
    /// requeueing can hold the lane and starve the middle band —
    /// `claims_mid_band == 0` is that livelock.
    pub claims_frontier: u64,
    pub claims_mid_band: u64,
    pub claims_privileged: u64,
    /// Claims whose slice is a full day or wider, regardless of age — the
    /// population that carries the queue's bytes.
    pub claims_day_wide: u64,
    pub narrowest_sharded_run_micros: i64,
    /// Contiguity outcome at sim end: completed sub-day Dedup slices merged
    /// into runs per (project, source, day) inside the certify window.
    /// `islands_total / island_cells` near 1.0 is the goal.
    pub dedup_island_cells: usize,
    pub dedup_islands_total: usize,
    /// Cells whose completed-dedup runs merge into ONE interval covering the
    /// whole day — the shape certification grants on.
    pub dedup_cells_day_covered: usize,
    /// The most any single execution decoded, after runtime sharding. Above
    /// `MAX_DECODED_BYTES` means the memory bound was broken.
    pub max_run_bytes: u64,
    /// Units ever minted per `project/operation/day` cell, in every state,
    /// superseded parents included. `max_cell*` is the worst cell by that count.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub units_per_cell: BTreeMap<String, usize>,
    pub max_cell: String,
    pub max_cell_units: usize,
    pub units_at_min_slice: usize,
    /// Minimum-width tasks per cell, separating new splits from unrelated debris.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub min_slice_units_per_cell: BTreeMap<String, usize>,
    pub samples: Vec<SimSample>,
}

/// Measured duration ranges in seconds, per operation and width class.
///
/// The distributions are deliberately BIMODAL: most units find no work and
/// finish at ~0s, and the rest are very expensive. A single uniform range
/// cannot express that, and the mean alone hides the shape that sets capacity.
fn duration_range_secs(operation: Operation, width_micros: i64, rng: &mut Rng) -> u64 {
    let frontier = width_micros < DAY_MICROS;
    let pct = rng.next() % 100;
    let (lo, hi) = match (operation, frontier, pct) {
        // 70% no-op, 20% cheap, 10% the long tail that actually costs.
        (Operation::Dedup, _, ..70) => (0, 6),
        (Operation::Dedup, _, ..90) => (6, 300),
        (Operation::Dedup, ..) => (1_910, 7_203),
        // 65% no-op, then a wide and frequent expensive mode.
        (Operation::BaseRollup, true, ..65) => (0, 5),
        (Operation::BaseRollup, true, _) => (1_227, 2_368),
        (Operation::BaseRollup, false, _) => (0, 11),
        (Operation::DerivedRollup, ..) => (0, 3),
        (Operation::HotPacking, ..) => (0, 13),
        (Operation::SealedConsolidation, ..) => (0, 26),
        // Half no-op, half a long tail.
        (Operation::Repair, _, ..50) => (0, 49),
        (Operation::Repair, ..) => (49, 5_678),
    };
    rng.uniform_secs(lo, hi)
}

/// Debt work = file rewrites that cannot advance rollup coverage: the
/// operations the occupancy cap applies to. Spelled as the COMPLEMENT of the
/// rollup tiers, like the server's `_debt_slot` predicate, so an operation
/// added later is debt on both sides rather than silently exempt here.
fn is_debt_op(operation: Operation) -> bool {
    !matches!(operation, Operation::BaseRollup | Operation::DerivedRollup)
}

/// Deterministic SplitMix64 generator for simulation.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn uniform_secs(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

/// One ingest stream: the tables a project invalidates per flush.
#[derive(Clone, Debug)]
struct Stream {
    source_table: String,
    base_rollup_table: String,
    derived_rollup_table: Option<String>,
    source: String,
    project_id: String,
    /// Newest `created_unix_ms` this stream ever produced. See
    /// `STREAM_IDLE_MICROS`.
    last_created_ms: u64,
}

/// How recently a stream must have produced work to count as INGESTING: a
/// stream mints only if it produced a task within this window of the journal's
/// newest record. Production invalidates on actual writes, so minting for every
/// discovered stream would vastly over-state the arrival rate.
const STREAM_IDLE_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Contiguity model mirroring `min_contiguous_days`: a day counts for a tier
/// once that tier covers its full width, and contiguity counts back from
/// yesterday. The MIN over active (source, project) pairs is the reported goal
/// metric; the MEDIAN is what `coverage_is_short` steers by.
struct Coverage {
    /// (source, project) -> day_start -> [base_width, derived_width] micros.
    days: HashMap<(String, String), BTreeMap<i64, [i64; 2]>>,
    /// The active set: every stream found in the journal, with whether it has a
    /// derived tier at all.
    pairs: Vec<(String, String, bool)>,
}

impl Coverage {
    fn record(&mut self, key: &TaskKey) {
        let tier = match key.operation {
            Operation::BaseRollup => 0,
            Operation::DerivedRollup => 1,
            _ => return,
        };
        // A slice can straddle midnight after time-bisection; credit each day
        // it overlaps, clamped to that day.
        let days = self.days.entry((key.source.clone(), key.project_id.clone())).or_default();
        for day in (day_start(key.slice.start_micros)..key.slice.end_micros).step_by(DAY_MICROS as usize) {
            days.entry(day).or_default()[tier] += (key.slice.end_micros.min(day + DAY_MICROS) - key.slice.start_micros.max(day)).max(0);
        }
    }

    /// Contiguous covered days back from yesterday, per project of ONE
    /// (source, tier). Per sweep rather than per pair-AND because the fleet
    /// gauge is a MEDIAN within the sweep and a MIN across sweeps
    /// (`fold_fleet_gauge`), and a median does not survive being AND-ed first.
    fn contiguous_days(&self, now_micros: i64, source: &str, tier: usize) -> Vec<u64> {
        let yesterday = (now_micros.div_euclid(DAY_MICROS) - 1) * DAY_MICROS;
        self.pairs
            .iter()
            .filter(|(pair_source, _, has_derived)| pair_source == source && (tier == 0 || *has_derived))
            .map(|(_, project, _)| {
                let days = self.days.get(&(source.to_owned(), project.clone()));
                (0i64..).take_while(|back| days.and_then(|d| d.get(&(yesterday - back * DAY_MICROS))).is_some_and(|widths| widths[tier] >= DAY_MICROS)).count()
                    as u64
            })
            .collect()
    }

    /// One entry per (source, tier) sweep, skipping sweeps with no projects:
    /// a tier no stream declares does not exist to fold.
    fn per_sweep(&self, now_micros: i64) -> Vec<Vec<u64>> {
        let sources = self.pairs.iter().map(|(source, ..)| source.as_str()).collect::<std::collections::BTreeSet<_>>();
        sources.into_iter().flat_map(|source| (0..2).map(move |tier| self.contiguous_days(now_micros, source, tier))).filter(|days| !days.is_empty()).collect()
    }

    /// The goal metric: days every pair can answer, every tier. Reported, never
    /// steered by — see `coverage_is_short`.
    fn min_contiguous_days(&self, now_micros: i64) -> u64 {
        self.per_sweep(now_micros).into_iter().filter_map(|days| days.into_iter().min()).min().unwrap_or(0)
    }

    /// The control signal, computed the way production computes it: the MEDIAN
    /// over each (source, tier) sweep's projects, folded by MIN, compared
    /// through the shared `database::coverage_is_short_for`. Steering by the MIN
    /// instead would put the sim in coverage-short mode on states the server
    /// calls healthy.
    ///
    /// Known gap: production lets a tier younger than the backfill horizon
    /// abstain from the fleet fold; the sim has no tier creation time, so no
    /// simulated tier ever abstains.
    fn coverage_is_short(&self, now_micros: i64) -> bool {
        let fleet = self.per_sweep(now_micros).into_iter().map(|mut days| median_contiguous_days(&mut days)).min().unwrap_or(0);
        coverage_is_short_for(fleet)
    }
}

/// One ingest flush (or one restart reconciliation) for one stream: the base
/// invalidation mints Dedup + BaseRollup over the range's 10-minute slices;
/// the derived invalidation mints the hour-aligned DerivedRollup units.
/// `observed_at` sets the finalization deadline — the flush time for minting,
/// the boot time for a restart reconcile.
fn mint_stream(journal: &mut TaskJournal, stream: &Stream, start_micros: i64, end_micros: i64, observed_at_micros: i64) {
    for (derived, rollup_table) in [(false, stream.base_rollup_table.as_str())].into_iter().chain(stream.derived_rollup_table.as_deref().map(|t| (true, t))) {
        // An invalidation error means a skipped window, not a crash.
        let _ = journal.invalidate(Invalidation {
            source_table: &stream.source_table,
            rollup_table,
            source: &stream.source,
            project_id: &stream.project_id,
            start_micros,
            end_micros,
            observed_at_micros,
            derived,
            mint_dedup: true,
            mint_rollup: true,
        });
    }
}

fn streams_from_journal(journal: &TaskJournal) -> Vec<Stream> {
    // Dedup tasks name the source table, rollup tasks name the tier tables.
    let mut streams: Vec<Stream> = Vec::new();
    for task in journal.tasks() {
        let key = &task.key;
        let position = streams.iter().position(|s| s.source == key.source && s.project_id == key.project_id).unwrap_or_else(|| {
            streams.push(Stream {
                source_table: key.source.clone(),
                base_rollup_table: key.physical_table.clone(),
                derived_rollup_table: None,
                source: key.source.clone(),
                project_id: key.project_id.clone(),
                last_created_ms: 0,
            });
            streams.len() - 1
        });
        let stream = &mut streams[position];
        stream.last_created_ms = stream.last_created_ms.max(task.created_unix_ms);
        match key.operation {
            Operation::Dedup | Operation::HotPacking => stream.source_table = key.physical_table.clone(),
            Operation::BaseRollup => stream.base_rollup_table = key.physical_table.clone(),
            Operation::DerivedRollup => stream.derived_rollup_table = Some(key.physical_table.clone()),
            _ => {}
        }
    }
    streams
}

/// A brief restart reconciles the touched hour from each stream, not its day.
fn reconcile_restart(journal: &mut TaskJournal, streams: &[Stream], now: i64) {
    let hour_start = now.div_euclid(HOUR_MICROS) * HOUR_MICROS;
    for stream in streams {
        mint_stream(journal, stream, hour_start, hour_start + HOUR_MICROS, now);
    }
}

/// The claim-time preflight: measure what the claimed slice reads, record it on
/// the unit whether or not it splits, and split before dispatch when it is over
/// budget and still wide enough to divide. `None` means the unit was superseded
/// by children.
fn preflight(journal: &mut TaskJournal, model: &ByteModel, guard: SplitGuard, task: &MaintenanceTask, report: &mut SimReport) -> Option<u64> {
    let key = &task.key;
    let observed = model.bytes(key);
    let footprint = model.footprint(key);
    report.preflight_measures += 1;
    journal.record_preflight(key, footprint, observed);
    if observed <= MAX_DECODED_BYTES || key.slice.width() <= MIN_SLICE_MICROS {
        return Some(observed);
    }
    match guard {
        SplitGuard::Shipped => {}
        SplitGuard::Off => defeat_guard(journal, task),
        SplitGuard::Ratio(numerator, denominator) => {
            // Call the REAL predicate at the swept ratio; an inline
            // transcription here would drift from the shipped rule.
            if !split_sheds_enough_at(task.parent_measured_bytes, observed, numerator, denominator) {
                report.split_declined_at_floor += 1;
                return Some(observed);
            }
            defeat_guard(journal, task);
        }
    }
    let stats = crate::observability::maintenance_stats();
    let declined_before = stats.split_declined_at_floor.load(Relaxed);
    let split = journal.split_time_task(key, observed, footprint);
    report.split_declined_at_floor += stats.split_declined_at_floor.load(Relaxed) - declined_before;
    if split {
        report.byte_splits += 1;
        return None;
    }
    Some(observed)
}

/// Clear the parent's measurement so `split_sheds_enough` takes its `None` arm.
/// Re-reads the current task so preflight evidence recorded since the claim is
/// retained.
fn defeat_guard(journal: &mut TaskJournal, task: &MaintenanceTask) {
    let mut task = journal.tasks().find(|current| current.key == task.key).cloned().expect("preflight task remains in the journal");
    task.parent_measured_bytes = None;
    journal.upsert(task);
}

/// Midnight (in micros) of the day `micros` falls in.
fn day_start(micros: i64) -> i64 {
    micros.div_euclid(DAY_MICROS) * DAY_MICROS
}

/// A task with work left: not yet complete, and not superseded by a split.
fn is_open(state: TaskState) -> bool {
    !matches!(state, TaskState::Complete | TaskState::Superseded)
}

/// Earliest future deadline among still-claimable tasks, optionally scoped to
/// one operation. Backs both the per-operation "known empty until" memo and the
/// idle-worker wakeup.
fn next_deadline(journal: &TaskJournal, now: i64, operation: Option<Operation>) -> Option<i64> {
    journal
        .tasks()
        .filter(|t| operation.is_none_or(|op| t.key.operation == op) && matches!(t.state, TaskState::Pending | TaskState::Retry) && t.deadline_micros > now)
        .map(|t| t.deadline_micros)
        .min()
}

/// `project/operation/day` — the cell unit counts are bucketed in.
fn cell_of(key: &TaskKey) -> String {
    format!("{}/{:?}/{}", key.project_id, key.operation, key.slice.start_micros.div_euclid(DAY_MICROS))
}

fn max_cell_pending(journal: &TaskJournal) -> usize {
    journal.tasks().filter(|task| is_open(task.state)).map(|task| cell_of(&task.key)).counts().into_values().max().unwrap_or_default()
}

struct Worker {
    busy_until: i64,
    current: Option<(TaskKey, u64)>,
    cycle_pos: usize,
}

fn hours(micros: i64) -> f64 {
    micros as f64 / HOUR_MICROS as f64
}

/// Record the first hour each contiguity milestone is crossed.
fn note_contiguity_milestones(report: &mut SimReport, contiguous: u64, elapsed_micros: i64) {
    if contiguous >= 14 {
        report.hours_to_contiguous_14.get_or_insert(hours(elapsed_micros));
    }
    if contiguous >= 30 {
        report.hours_to_contiguous_30.get_or_insert(hours(elapsed_micros));
    }
}

/// Replayed over `[start_micros, start_micros + horizon)`. The journal's own
/// deadlines are in real time, so `start_micros` should be real "now" for a
/// freshly fetched prod journal.
pub fn run(mut journal: TaskJournal, cfg: &SimConfig, start_micros: i64) -> anyhow::Result<SimReport> {
    let mut rng = Rng(cfg.seed);
    let end = start_micros.saturating_add(cfg.horizon_micros);

    let mut streams = streams_from_journal(&journal);
    // Only streams that are actually INGESTING mint. Discovery walks every task
    // the journal ever held, so without this an account that stopped writing
    // weeks ago still generates frontier work forever.
    let newest_created_ms = streams.iter().map(|s| s.last_created_ms).max().unwrap_or_default();
    let idle_cutoff_ms = newest_created_ms.saturating_sub(STREAM_IDLE_MICROS as u64 / 1_000);
    anyhow::ensure!(!streams.is_empty() || !cfg.mint_frontier, "no streams found in journal; pass --no-mint");
    if let Some(target) = cfg.streams {
        // `--streams N` means N INGESTING streams, not N total: minting is
        // activity-gated, so the template must itself be an active stream.
        let Some(template) = streams.iter().find(|s| s.last_created_ms >= idle_cutoff_ms).or_else(|| streams.first()).cloned() else {
            anyhow::bail!("--streams needs at least one real stream in the journal")
        };
        let active = streams.iter().filter(|s| s.last_created_ms >= idle_cutoff_ms).count();
        // Down: retire the excess actives rather than dropping streams, so the
        // journal's existing backlog for them is preserved.
        streams.iter_mut().filter(|s| s.last_created_ms >= idle_cutoff_ms).take(active.saturating_sub(target)).for_each(|s| s.last_created_ms = 0);
        // Up: synthetic clones of a stream that is genuinely ingesting.
        let len = streams.len();
        streams.extend((0..target.saturating_sub(active)).map(|extra| Stream {
            project_id: format!("synth-{}", len + extra),
            last_created_ms: newest_created_ms,
            ..template.clone()
        }));
    }
    let ingesting = streams.iter().filter(|s| s.last_created_ms >= idle_cutoff_ms).count();

    if cfg.mint_frontier {
        eprintln!("sim: minting from {ingesting} INGESTING streams of {} in the journal", streams.len());
    }
    let mut coverage =
        Coverage { days: HashMap::new(), pairs: streams.iter().map(|s| (s.source.clone(), s.project_id.clone(), s.derived_rollup_table.is_some())).collect() };
    // Seed coverage from already-complete rollup tasks so a fetched journal
    // starts with the coverage prod actually has.
    journal.tasks().filter(|t| t.state == TaskState::Complete).for_each(|t| coverage.record(&t.key));

    let mut report =
        SimReport { hours: hours(cfg.horizon_micros), pending_start: journal.tasks().filter(|t| t.state != TaskState::Complete).count(), ..Default::default() };
    let mut workers = (0..cfg.workers).map(|_| Worker { busy_until: start_micros, current: None, cycle_pos: 0 }).collect::<Vec<_>>();
    let mut next_mint = start_micros + MINT_INTERVAL_MICROS;
    let mut next_restart = match (cfg.restart_at_micros, cfg.restart_every_micros) {
        (Some(at), _) => start_micros + at,
        (None, every) if every > 0 => start_micros + every,
        _ => i64::MAX,
    };
    // 48 samples over the horizon, floored at five minutes.
    let tick = (cfg.horizon_micros / 48).max(300 * MICROS);
    let mut next_tick = start_micros + tick;
    let mut next_coarsen = start_micros + COARSEN_INTERVAL_MICROS;
    let mut now = start_micros;
    // The `.max(1)` floor keeps debt work possible at all on a one-worker box.
    let debt_cap = (cfg.workers * 3 / 4).max(1);
    // Per-op "known empty until" memo. `claim_next` is deterministic given
    // (journal state, now), so a None at time T holds until the op's next
    // future deadline or any state change — every mutation below refills this.
    // Side effect: the skipped calls would have bumped `claim_tick`, so
    // sealed-reservation parity shifts slightly (share over time is unchanged).
    let mut none_until = [0i64; <Operation as strum::EnumCount>::COUNT];
    // Evaluated before any claim, so the initial cycle matches the journal's
    // seeded coverage.
    let mut coverage_short = coverage.coverage_is_short(now);

    while now < end {
        let next_worker_free = workers.iter().map(|w| w.busy_until).min().unwrap_or(end);
        now = next_worker_free.min(next_mint).min(next_tick).min(next_restart).min(next_coarsen).min(end);
        if now >= end {
            break;
        }

        if now >= next_mint {
            if cfg.mint_frontier {
                for stream in streams.iter().filter(|s| s.last_created_ms >= idle_cutoff_ms) {
                    mint_stream(&mut journal, stream, next_mint - MINT_INTERVAL_MICROS, next_mint, next_mint);
                }
            }
            none_until.fill(0);
            // The cadence advances whether or not minting is on — otherwise
            // `next_mint` pins `now` here forever.
            next_mint += MINT_INTERVAL_MICROS;
        }

        if now >= next_coarsen {
            // With a byte model the CAPPED variant is the one prod runs:
            // footprint-less debris carrying an inflated estimate only fuses
            // once the partition ceiling bounds what that day can decode.
            let coarsen = match cfg.byte_model.as_ref() {
                Some(model) => journal.coarsen_sealed_slices_capped(now, &|project, _source, date| model.partition_ceiling(project, date)),
                None => journal.coarsen_sealed_slices_reporting(now),
            };
            report.coarsen_subsumed += coarsen.subsumed;
            report.coarsen_fused += coarsen.fused;
            report.coarsen_candidates += coarsen.candidates;
            report.coarsen_blocked += coarsen.blocked;
            report.coarsen_over_budget += coarsen.over_budget;
            if coarsen.total() != 0 {
                none_until.fill(0);
            }
            next_coarsen += COARSEN_INTERVAL_MICROS;
        }

        if now >= next_restart {
            reconcile_restart(&mut journal, &streams, now);
            none_until.fill(0);
            next_restart = if cfg.restart_every_micros > 0 { next_restart + cfg.restart_every_micros } else { i64::MAX };
        }

        let mut debt_busy = workers.iter().filter(|w| w.current.as_ref().is_some_and(|(key, _)| is_debt_op(key.operation))).count();
        for worker in &mut workers {
            if worker.busy_until > now {
                continue;
            }
            if let Some((key, duration_secs)) = worker.current.take() {
                if is_debt_op(key.operation) {
                    debt_busy -= 1;
                }
                let deadline_secs = operation_deadline_secs(key.operation);
                if duration_secs <= deadline_secs {
                    journal.complete(&key);
                    none_until.fill(0);
                    if matches!(key.operation, Operation::BaseRollup | Operation::DerivedRollup) {
                        coverage.record(&key);
                        let contiguous = coverage.min_contiguous_days(now);
                        coverage_short = coverage.coverage_is_short(now);
                        // At the crossing event, not just at report ticks: a
                        // tick can land short of a day boundary and read one
                        // day less.
                        note_contiguity_milestones(&mut report, contiguous, now - start_micros);
                    }
                    *report.completions.entry(format!("{:?}", key.operation)).or_default() += 1;
                } else {
                    // Timeout: the worker burned the whole deadline, then the
                    // lease drop abandons the unit — bisect on repeat, else
                    // deadline-floored backoff.
                    journal.abandon_running(&key, now, None);
                    none_until.fill(0);
                    match journal.state(&key) {
                        Some(TaskState::Superseded) => report.splits += 1,
                        _ => *report.timeouts.entry(format!("{:?}", key.operation)).or_default() += 1,
                    }
                }
                report.executions += 1;
            }
            // Claim the next unit, rotating through the shared cycle like
            // `run_coordinator_maintenance_once`. While coverage is short, debt
            // work may occupy at most 3/4 of workers, so long file rewrites
            // cannot starve short rollups.
            let cycle = operation_cycle(coverage_short);
            let mut claimed: Option<MaintenanceTask> = None;
            for offset in 0..cycle.len() {
                let position = (worker.cycle_pos + offset) % cycle.len();
                let operation = cycle[position];
                if coverage_short && is_debt_op(operation) && debt_busy >= debt_cap {
                    continue;
                }
                if now < none_until[operation as usize] {
                    continue;
                }
                if let Some(task) = journal.claim_next(operation, now, false) {
                    // Bucketed on the same quantity `starved` ranks on: how long
                    // ago the slice's DATA ended, not when the record was made.
                    let waited = now.saturating_sub(task.key.slice.end_micros);
                    let band = if waited > STARVATION_HORIZON_MICROS {
                        &mut report.claims_privileged
                    } else if waited > 3 * DAY_MICROS {
                        &mut report.claims_mid_band
                    } else {
                        &mut report.claims_frontier
                    };
                    *band += 1;
                    if task.key.slice.width() >= DAY_MICROS {
                        report.claims_day_wide += 1;
                    }
                    if is_debt_op(operation) {
                        debt_busy += 1;
                    }
                    worker.cycle_pos = position + 1;
                    claimed = Some(task);
                    break;
                }
                // A None holds until this op's next deadline matures or any
                // state change (all of which reset the memo above).
                none_until[operation as usize] = next_deadline(&journal, now, Some(operation)).unwrap_or(i64::MAX);
            }
            // The byte preflight runs between the claim and the dispatch, where
            // prod runs it. A split leaves the worker free after the cost of the
            // measurement — no unit ran, so the debt slot goes back too.
            if let (Some(model), Some(task)) = (cfg.byte_model.as_ref(), claimed.as_ref()) {
                let Some(observed) = preflight(&mut journal, model, cfg.split_guard, task, &mut report) else {
                    if is_debt_op(task.key.operation) {
                        debt_busy -= 1;
                    }
                    none_until.fill(0);
                    worker.busy_until = now + PREFLIGHT_COST_MICROS;
                    continue;
                };
                let shards = observed.div_ceil(MAX_DECODED_BYTES).max(1);
                report.max_run_bytes = report.max_run_bytes.max(observed.div_ceil(shards));
                if shards > 1 {
                    let width = task.key.slice.width();
                    report.sharded_runs += 1;
                    report.sharded_runs_above_min_slice += u64::from(width > MIN_SLICE_MICROS);
                    let narrowest = &mut report.narrowest_sharded_run_micros;
                    *narrowest = if *narrowest == 0 { width } else { (*narrowest).min(width) };
                }
            }
            match claimed {
                Some(task) => {
                    let duration_secs = (duration_range_secs(task.key.operation, task.key.slice.width(), &mut rng) as f64 * cfg.duration_scale) as u64;
                    let burn_secs = duration_secs.min(operation_deadline_secs(task.key.operation));
                    worker.current = Some((task.key, duration_secs));
                    worker.busy_until = now + (burn_secs as i64) * MICROS;
                }
                None => {
                    // Nothing claimable: jump to the next eligibility instant
                    // rather than re-scanning the journal on a poll interval.
                    worker.busy_until = next_deadline(&journal, now, None).unwrap_or(now + IDLE_POLL_MICROS).max(now + 1);
                }
            }
        }

        if now >= next_tick {
            let lag = frontier_lag_secs(&journal, now);
            report.frontier_lag_secs_max = report.frontier_lag_secs_max.max(lag);
            let contiguous = coverage.min_contiguous_days(now);
            note_contiguity_milestones(&mut report, contiguous, now - start_micros);
            report.samples.push(SimSample {
                hour: hours(now - start_micros),
                pending: journal.tasks().filter(|t| is_open(t.state)).count(),
                frontier_lag_secs: lag,
                min_contiguous_days: contiguous,
                split_declined_at_floor: report.split_declined_at_floor,
                max_cell_pending: max_cell_pending(&journal),
            });
            next_tick += tick;
        }
    }

    report.min_contiguous_days_end = coverage.min_contiguous_days(now);
    report.pending_end = journal.tasks().filter(|t| is_open(t.state)).count();
    for task in journal.tasks() {
        *report.tasks_end.entry(format!("{:?}/{:?}", task.key.operation, task.state)).or_default() += 1;
        *report.units_per_cell.entry(cell_of(&task.key)).or_default() += 1;
        if task.key.slice.width() <= MIN_SLICE_MICROS {
            report.units_at_min_slice += 1;
            *report.min_slice_units_per_cell.entry(cell_of(&task.key)).or_default() += 1;
        }
    }
    if let Some((cell, units)) = report.units_per_cell.iter().max_by_key(|(_, units)| **units) {
        (report.max_cell, report.max_cell_units) = (cell.clone(), *units);
    }
    // Completed sub-day Dedup slices merged into runs, per (project, source,
    // day) cell inside the 14-day certify window. Certification grants only
    // when the merged runs cover the WHOLE day.
    {
        let by_cell: HashMap<(String, String, i64), Vec<(i64, i64)>> = journal
            .tasks()
            .filter(|task| task.key.operation == Operation::Dedup && task.state == TaskState::Complete)
            .filter(|task| (0..=14 * DAY_MICROS).contains(&now.saturating_sub(task.key.slice.end_micros)))
            .map(|task| {
                let slice = &task.key.slice;
                ((task.key.project_id.clone(), task.key.source.clone(), slice.start_micros.div_euclid(DAY_MICROS)), (slice.start_micros, slice.end_micros))
            })
            .into_group_map();
        report.dedup_island_cells = by_cell.len();
        (report.dedup_islands_total, report.dedup_cells_day_covered) = by_cell
            .into_iter()
            .map(|((_, _, day), mut slices)| {
                slices.sort_unstable();
                let (runs, open_end) = slices.iter().fold((0usize, i64::MIN), |(runs, open), &(start, end)| (runs + usize::from(start > open), open.max(end)));
                // The grant-relevant outcome: ONE run covering the whole day.
                // `open_end` is the union's max end — with runs == 1 the union
                // is one interval from the first start to `open_end`.
                let day_start = day * DAY_MICROS;
                let covered = runs == 1 && slices.first().is_some_and(|&(s, _)| s <= day_start) && open_end >= day_start + DAY_MICROS;
                (runs, usize::from(covered))
            })
            .fold((0, 0), |(islands, covered), (runs, day_covered)| (islands + runs, covered + day_covered));
    }
    Ok(report)
}

/// `eligible_watermark_lag_seconds`, simplified: the oldest eligible,
/// unfinished frontier task's lateness. The production gauge adds a per-stream
/// watermark; the sim needs only the trend.
fn frontier_lag_secs(journal: &TaskJournal, now_micros: i64) -> u64 {
    journal
        .tasks()
        .filter(|task| {
            is_open(task.state) && task.deadline_micros <= now_micros && task.key.slice.end_micros >= now_micros.saturating_sub(LIVE_FRONTIER_WINDOW_MICROS)
        })
        .map(|task| u64::try_from(now_micros.saturating_sub(task.deadline_micros).div_euclid(MICROS)).unwrap_or_default())
        .max()
        .unwrap_or_default()
}

/// A synthetic queue with the shape a shred happens in, built through the
/// `TaskJournal` API — never a hand-written `maintenance_tasks.json`, whose
/// on-disk form is an internal serde detail that would rot silently.
pub struct SynthQueue {
    pub journal: TaskJournal,
    pub model: ByteModel,
    /// The cell that shreds: one day-wide unit over a day of ~100x
    /// `MAX_DECODED_BYTES`.
    pub whale_cell: String,
    /// A lineage carrying `retry_or_split`'s synthetic `MAX_DECODED_BYTES + 1`
    /// stamp, which is a "does not fit" signal and not a measurement.
    pub stamped_cell: String,
    /// Keeps the journal's directory alive for the caller's run.
    pub dir: tempfile::TempDir,
}

/// Skewed cell sizes, many cells, and pre-existing shred debris — the three
/// properties a uniform queue cannot reproduce. Everything starts with
/// `parent_measured_bytes: None` except the deliberately stamped lineage, so
/// the first split of each lineage is unconditional and the guard engages only
/// from the second level down.
///
/// `debris_slice_minutes` varies unit COUNT at constant total work: `600 / n`
/// units of `n` minutes carry the same bytes over the same window.
///
/// Not usable for bin-width questions: widening a bin pays off in READ BYTES,
/// and this IO-free model cannot see bytes.
pub fn synthetic_whale_queue(start_micros: i64, floored: bool, whale_x_max: u64, debris_slice_minutes: i64) -> SynthQueue {
    let dir = tempfile::tempdir().expect("sim fixture tempdir");
    let mut journal = TaskJournal::load(dir.path()).expect("sim fixture journal");
    let mut model = ByteModel { floored, ..Default::default() };
    let day = |back: i64| (start_micros.div_euclid(DAY_MICROS) - back) * DAY_MICROS;
    let cell = |journal: &mut TaskJournal, model: &mut ByteModel, project: &str, back: i64, decoded_bytes: u64| {
        let day_start = day(back);
        model.insert(project, day_start, decoded_bytes);
        let key = rollup_key(project, day_start, DAY_MICROS);
        journal.enqueue(key.clone(), start_micros, decoded_bytes, 0);
        cell_of(&key)
    };

    let whale_cell = cell(&mut journal, &mut model, "whale", 1, whale_x_max * MAX_DECODED_BYTES);
    cell(&mut journal, &mut model, "mid", 1, 5 * MAX_DECODED_BYTES);
    let stamped_cell = cell(&mut journal, &mut model, "stamped", 1, 10 * MAX_DECODED_BYTES);
    // The `retry_or_split` stamp, applied after enqueue so the unit is
    // otherwise ordinary.
    let stamped_key = rollup_key("stamped", day(1), DAY_MICROS);
    let mut stamped = journal.tasks().find(|task| task.key == stamped_key).cloned().expect("stamped unit");
    stamped.parent_measured_bytes = Some(MAX_DECODED_BYTES + 1);
    journal.upsert(stamped);

    // The long tail: 70 projects x 3 sealed days, each fitting in one unit, so
    // `claim_next` ordering, the debt cap and coarsening interact at scale.
    for (project, back) in itertools::iproduct!(0u64..70, 1..=3) {
        cell(&mut journal, &mut model, &format!("tail-{project:02}"), back, 60_000_000 + project * 2_000_000);
    }

    // Pre-existing shred debris: 600 one-minute units on ONE partition, no
    // `InputFootprint` and each claiming far more bytes than the 0.36 GB
    // partition holds. Fusion can only rescue them via the partition ceiling.
    let debris_day = day(2);
    model.insert("debris", debris_day, 360_000_000);
    let slice = debris_slice_minutes.max(1);
    let debris_units = 600 / slice;
    for unit in 0..debris_units {
        // Bytes scale with the slice so total work is constant: the knob must
        // vary unit COUNT alone, or a sweep confounds count with cost.
        journal.enqueue(
            rollup_key("debris", debris_day + unit * slice * MIN_SLICE_MICROS, slice * MIN_SLICE_MICROS),
            start_micros,
            4_466_185_462u64.saturating_mul(slice as u64),
            0,
        );
    }
    SynthQueue { journal, model, whale_cell, stamped_cell, dir }
}

fn rollup_key(project_id: &str, start_micros: i64, width_micros: i64) -> TaskKey {
    TaskKey {
        physical_table: "otel_logs_and_spans_rollup_dashboard_1m_v3".to_owned(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: project_id.to_owned(),
        slice: TimeSlice::new(start_micros, start_micros + width_micros).expect("fixture slice"),
        operation: Operation::BaseRollup,
    }
}

/// Load a journal from a copied-out prod file or data dir WITHOUT ever being
/// able to write back to the source: the inputs are copied into a tempdir and
/// the journal is loaded from there. Returns the journal and the tempdir
/// guard (dropped last -> files cleaned up).
pub fn load_sandboxed(input: &std::path::Path) -> anyhow::Result<(TaskJournal, tempfile::TempDir)> {
    let dir = tempfile::tempdir().context("create sim sandbox")?;
    let meta = dir.path().join(".timefusion_meta");
    std::fs::create_dir_all(&meta)?;
    let (json, wal) = if input.is_dir() {
        (input.join(".timefusion_meta/maintenance_tasks.json"), input.join(".timefusion_meta/maintenance_tasks.wal"))
    } else {
        (input.to_path_buf(), input.with_extension("wal"))
    };
    std::fs::copy(&json, meta.join("maintenance_tasks.json")).with_context(|| format!("copy {}", json.display()))?;
    if wal.exists() {
        std::fs::copy(&wal, meta.join("maintenance_tasks.wal")).with_context(|| format!("copy {}", wal.display()))?;
    }
    let journal = TaskJournal::load(dir.path())?;
    Ok((journal, dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maintenance_coordinator::FRONTIER_LAG_BUDGET_SECS;

    fn key(project: &str, op: Operation, start: i64, width: i64) -> TaskKey {
        let table = match op {
            Operation::BaseRollup => "otel_logs_and_spans_rollup_dashboard_1m_v3",
            Operation::DerivedRollup => "otel_logs_and_spans_rollup_dashboard_1h_v2",
            _ => "otel_logs_and_spans",
        };
        TaskKey {
            physical_table: table.to_owned(),
            source: "otel_logs_and_spans".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(start, start + width).unwrap(),
            operation: op,
        }
    }

    fn empty_journal() -> TaskJournal {
        let dir = tempfile::tempdir().unwrap();
        TaskJournal::load(dir.path()).unwrap()
    }

    /// One completed frontier task per project, so stream extraction sees the
    /// requested number of streams. The derived twin deliberately keeps the
    /// BASE physical table; only the operation is flipped.
    fn journal_with_streams(projects: usize) -> TaskJournal {
        let mut journal = empty_journal();
        for i in 0..projects {
            let base = key(&format!("p{i}"), Operation::BaseRollup, 60 * DAY_MICROS, NORMAL_SLICE_MICROS);
            journal.enqueue(base.clone(), 0, 0, 0);
            let mut task = journal.tasks().find(|task| task.key == base).cloned().expect("enqueued frontier task");
            task.state = TaskState::Complete;
            journal.upsert(task.clone());
            task.key.operation = Operation::DerivedRollup;
            journal.upsert(task);
        }
        journal
    }

    fn cfg(hours: i64) -> SimConfig {
        SimConfig { horizon_micros: hours * HOUR_MICROS, ..Default::default() }
    }

    #[test]
    fn an_idle_journal_stays_idle() {
        let idle = SimConfig { mint_frontier: false, ..cfg(2) };
        let report = run(empty_journal(), &idle, 100 * DAY_MICROS).unwrap();
        assert_eq!(report.executions, 0, "nothing to do means nothing done");
        assert_eq!(report.pending_end, 0);
        // Minting with no streams to mint from is a caller error, not silence.
        let report = run(empty_journal(), &cfg(1), 100 * DAY_MICROS);
        assert!(report.is_err(), "no streams + minting must say so");
    }

    #[test]
    fn the_frontier_already_lags_at_13_projects_and_diverges_further_at_10x() {
        // Pins the SHAPE, not a level: 13 projects already exceed the lag
        // budget, and more load is strictly worse.
        let start = 100 * DAY_MICROS;
        let report_13 = run(journal_with_streams(13), &cfg(6), start).unwrap();
        let pending_13 = report_13.pending_end;
        assert!(
            report_13.frontier_lag_secs_max > FRONTIER_LAG_BUDGET_SECS,
            "13 projects already exceed the lag budget under measured durations, lag {}s",
            report_13.frontier_lag_secs_max
        );

        let cfg_10x = SimConfig { streams: Some(260), ..cfg(2) };
        let report_10x = run(journal_with_streams(13), &cfg_10x, start).unwrap();
        assert!(report_10x.pending_end > 10 * pending_13.max(1), "10x must diverge: pending {} vs {} at 13 projects", report_10x.pending_end, pending_13);
        // Do NOT add a lag comparison here: the two runs use different horizons
        // and max lag is bounded by run length, so the worse configuration can
        // score better. `pending_end` is the horizon-fair divergence metric.
    }

    /// The sim and the server must make the SAME coverage-short decision from
    /// the same coverage state. Each case is the per-project count of contiguous
    /// covered days; the rows where MIN and MEDIAN disagree are the point.
    #[test_case::test_case(&[30, 30, 30], &[30, 30, 30] => false; "fleet covered")]
    #[test_case::test_case(&[2, 3, 4], &[2, 3, 4] => true; "fleet short")]
    #[test_case::test_case(&[0, 20, 25], &[0, 20, 25] => false; "one laggard cannot pin the fleet")]
    #[test_case::test_case(&[13, 15], &[13, 15] => false; "even count takes the upper median")]
    #[test_case::test_case(&[0, 13], &[0, 13] => true; "even count upper median still short")]
    #[test_case::test_case(&[30, 30, 30], &[2, 2, 2] => true; "a lagging derived tier is short on its own")]
    fn sim_and_server_agree_on_coverage_short(base: &[u64], derived: &[u64]) -> bool {
        use std::collections::HashSet;

        let today = chrono::NaiveDate::from_ymd_opt(2026, 8, 25).unwrap();
        let now = today.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
        let source_table = "otel_logs_and_spans".to_owned();
        let projects = (0..base.len()).map(|i| format!("p{i}")).collect::<Vec<_>>();
        // Every project's SOURCE holds all 30 days back; each tier covers its
        // own count of them, at full width.
        let (mut covered, mut source) = ([HashSet::new(), HashSet::new()], HashSet::new());
        let mut coverage = Coverage { days: HashMap::new(), pairs: Vec::new() };
        for (index, project) in projects.iter().enumerate() {
            coverage.pairs.push((source_table.clone(), project.clone(), true));
            for back in 1..=30u64 {
                let date = today - chrono::Duration::days(back as i64);
                source.insert((project.clone(), date));
                for (tier, days) in [base, derived].into_iter().enumerate() {
                    if back <= days[index] {
                        covered[tier].insert((project.clone(), date));
                        coverage.days.entry((source_table.clone(), project.clone())).or_default().entry(now - back as i64 * DAY_MICROS).or_default()[tier] =
                            DAY_MICROS;
                    }
                }
            }
        }
        // Production sweeps each (source, tier), takes the MEDIAN over that
        // sweep's projects, and folds the sweeps by MIN.
        let active = projects.iter().map(String::as_str).collect::<HashSet<_>>();
        let fleet = covered.iter().map(|covered| crate::database::min_contiguous_days(covered, &source, today, &active).2).min().unwrap();
        let server = coverage_is_short_for(fleet);
        assert_eq!(coverage.coverage_is_short(now), server, "sim must decide as the server does (fleet median {fleet})");
        server
    }

    #[test]
    fn a_sealed_backlog_builds_contiguous_coverage() {
        // 2 projects x 30 sealed days x (base + derived day units), no minting.
        let mut journal = journal_with_streams(0);
        let start = 100 * DAY_MICROS;
        // Contiguity counts back from yesterday at sim END (start + 24h
        // = day 101), so the window is days 100..=71 relative to start.
        for (p, day, op) in itertools::iproduct!(["a", "b"], 0..30i64, [Operation::BaseRollup, Operation::DerivedRollup]) {
            journal.enqueue(key(p, op, start - day * DAY_MICROS, DAY_MICROS), start, MAX_DECODED_BYTES, 0);
        }
        let cfg = SimConfig { mint_frontier: false, ..cfg(25) };
        let report = run(journal, &cfg, start).unwrap();
        assert_eq!(report.min_contiguous_days_end, 30, "all 30 sealed days built: {report:#?}");
        assert!(report.hours_to_contiguous_30.is_some(), "the moment of arrival is recorded");
        assert_eq!(report.pending_end, 0, "nothing left pending");
    }

    #[test]
    fn a_unit_that_overruns_its_deadline_twice_is_bisected() {
        // A repeat timeout must split via the REAL `abandon_running`. Twenty
        // units, not one: ~70% of dedup samples finish near 0s and never
        // approach the deadline, so one unit would be a coin flip.
        let mut journal = journal_with_streams(0);
        let start = 100 * DAY_MICROS;
        for unit in 0..20 {
            let slice_start = start - DAY_MICROS * (unit + 1);
            journal.enqueue(key("a", Operation::Dedup, slice_start, DAY_MICROS), start, MAX_DECODED_BYTES, 0);
        }
        let cfg = SimConfig { mint_frontier: false, duration_scale: 10.0, workers: 1, ..cfg(12) };
        let report = run(journal, &cfg, start).unwrap();
        assert!(report.splits >= 1, "repeat overruns must bisect the unit: {report:#?}");
    }

    #[test]
    fn a_restart_reconciles_only_the_touched_hour() {
        let mut journal = journal_with_streams(4);
        let before: HashMap<_, _> = journal.tasks().map(|task| (task.key.clone(), serde_json::to_value(task).unwrap())).collect();
        let streams = streams_from_journal(&journal);
        let hour_start = 100 * DAY_MICROS + 7 * HOUR_MICROS;
        reconcile_restart(&mut journal, &streams, hour_start + HOUR_MICROS / 2);
        let added: Vec<_> = journal.tasks().filter(|task| !before.contains_key(&task.key)).collect();
        assert_eq!(added.len(), 13 * streams.len(), "each stream needs six dedup slices, six base slices, and one derived hour");
        assert!(
            added.iter().all(|task| task.key.slice.start_micros >= hour_start && task.key.slice.end_micros <= hour_start + HOUR_MICROS),
            "restart must only reconcile the touched hour"
        );
        for (key, prior) in before {
            let current = journal.tasks().find(|task| task.key == key).expect("unrelated task preserved");
            assert_eq!(serde_json::to_value(current).unwrap(), prior, "unrelated history stays unchanged");
        }
    }

    /// The whale fixture: 6 virtual hours, 16 workers, no minting — the queue
    /// under study is the fixture, not the frontier.
    fn synth_run(floored: bool, guard: SplitGuard) -> (SimReport, String, String) {
        synth_run_at(floored, guard, 100, 1.0)
    }

    fn synth_run_at(floored: bool, guard: SplitGuard, whale_x_max: u64, duration_scale: f64) -> (SimReport, String, String) {
        let start = 100 * DAY_MICROS;
        let queue = synthetic_whale_queue(start, floored, whale_x_max, 1);
        let cfg = SimConfig {
            mint_frontier: false,
            workers: 16,
            horizon_micros: 6 * HOUR_MICROS,
            byte_model: Some(queue.model),
            split_guard: guard,
            duration_scale,
            ..Default::default()
        };
        let report = run(queue.journal, &cfg, start).unwrap();
        (report, queue.whale_cell, queue.stamped_cell)
    }

    fn cell_units(report: &SimReport, cell: &str) -> usize {
        report.units_per_cell.get(cell).copied().unwrap_or_default()
    }

    /// Walk the whale lineage one preflight at a time, printing what each level
    /// measured against what its parent measured.
    #[test]
    fn whale_lineage_trace() {
        let start = 100 * DAY_MICROS;
        let queue = synthetic_whale_queue(start, true, 100, 1);
        let (mut journal, model) = (queue.journal, queue.model);
        let mut report = SimReport::default();
        // Deep enough to reach the DECLINE, not just the splits above it.
        for level in 0..9 {
            let Some(task) = journal
                .tasks()
                .filter(|task| task.key.project_id == "whale" && task.state == TaskState::Pending)
                .min_by_key(|task| task.key.slice.width())
                .cloned()
            else {
                break;
            };
            let width = task.key.slice.width();
            let observed = model.bytes(&task.key);
            let before = report.byte_splits;
            let split = preflight(&mut journal, &model, SplitGuard::Shipped, &task, &mut report).is_none();
            println!(
                "level {level}: width={:>6}s observed={:>6}MB parent_stamp={:>8} split={split} declined={} children_now={}",
                width / MICROS,
                observed / 1_000_000,
                task.parent_measured_bytes.map_or("none".to_owned(), |bytes| format!("{}MB", bytes / 1_000_000)),
                report.split_declined_at_floor,
                journal.tasks().filter(|t| t.key.project_id == "whale" && t.state == TaskState::Pending).count(),
            );
            assert!(report.byte_splits >= before);
        }
    }

    /// With the floor modelled and the guard defeated, the whale cell shreds
    /// all the way to the one-minute floor.
    #[test]
    fn a_floored_whale_shreds_to_the_minute_without_the_guard() {
        let (report, whale, _) = synth_run(true, SplitGuard::Off);
        assert!(cell_units(&report, &whale) > 500, "the shred must reproduce: {} units", cell_units(&report, &whale));
        assert!(report.units_at_min_slice >= 500, "and it must reach MIN_SLICE_MICROS: {}", report.units_at_min_slice);
    }

    /// The control: bytes strictly proportional to width. Without independent
    /// execution timeouts, byte-driven splitting must stop above the floor
    /// under either guard.
    #[test_case::test_case(SplitGuard::Off; "guard defeated")]
    #[test_case::test_case(SplitGuard::Shipped; "shipped guard")]
    fn a_floorless_whale_never_reaches_the_floor(guard: SplitGuard) {
        // `duration_scale: 0.0` isolates byte physics: random timeouts can
        // legitimately split even a small task, and are covered elsewhere.
        let (report, whale, _) = synth_run_at(false, guard, 100, 0.0);
        assert_eq!(report.timeouts.values().sum::<u64>(), 0);
        // ~255 units: 128 leaves plus their 127 intermediate parents, which are
        // journal rows because bisection descends one level per measurement.
        assert!(cell_units(&report, &whale) < 300, "{guard:?}: {} units", cell_units(&report, &whale));
        assert_eq!(report.min_slice_units_per_cell.get(&whale).copied().unwrap_or_default(), 0, "{guard:?}: the floorless whale must not reach the floor");
    }

    /// The floor guard must be CONSULTED at every level, and declined units run
    /// hash-sharded above the floor instead of shredding to it.
    ///
    /// `split_declined_at_floor > 0` is the regression assertion: bisection has
    /// to descend ONE level per measurement, so every level is a journal level.
    /// If a single call ever descends a whole subtree again the guard is never
    /// asked and this counter is exactly 0.
    #[test]
    fn the_floor_guard_declines_above_the_floor_and_the_shred_stops() {
        let (fixed, whale, _) = synth_run(true, SplitGuard::Shipped);
        let (unfixed, _, _) = synth_run(true, SplitGuard::Off);
        assert!(fixed.split_declined_at_floor > 0, "the guard must be CONSULTED, which is the whole defect");
        // Not 0: a few units still reach the floor against ~800 unguarded. The
        // bound is tight enough to catch a regression and honest about the
        // residual leak; driving it to 0 is open work.
        assert!(fixed.units_at_min_slice <= 16, "the guard must collapse the shred to a trickle: {} reached the floor", fixed.units_at_min_slice);
        assert!(
            cell_units(&fixed, &whale) * 4 < cell_units(&unfixed, &whale),
            "the shred must collapse: {} against {}",
            cell_units(&fixed, &whale),
            cell_units(&unfixed, &whale)
        );
        // Declining is only safe because the runner hash-shards internally, so
        // memory stays bounded.
        assert!(fixed.max_run_bytes <= MAX_DECODED_BYTES, "{} bytes decoded in one run", fixed.max_run_bytes);
        assert!(fixed.sharded_runs_above_min_slice > 0, "a declined unit must run hash-sharded ABOVE the floor");
        // `>=`, not `>`: the residual floor units also run sharded AT the floor.
        assert!(fixed.narrowest_sharded_run_micros >= MIN_SLICE_MICROS, "narrowest sharded run {}", fixed.narrowest_sharded_run_micros);
        // Declines must rise while the queue still DRAINS; the small residue is
        // the horizon expiring, not a wedge.
        assert!(fixed.pending_end <= 16, "declining must not stall the queue: {} left", fixed.pending_end);
    }

    /// A lineage carrying `retry_or_split`'s synthetic `MAX_DECODED_BYTES + 1`
    /// stamp must still split at journal scale: a child measuring MORE than its
    /// parent is evidence the parent's number was never a measurement.
    #[test]
    fn a_synthetic_stamp_still_splits_at_scale() {
        let (report, _, stamped) = synth_run(true, SplitGuard::Shipped);
        assert!(cell_units(&report, &stamped) > 1, "the stamped lineage must not freeze: {} units", cell_units(&report, &stamped));
    }

    /// Footprint-less debris claims far more bytes than its partition holds, so
    /// nothing can fuse it on its own prices — only the partition ceiling can.
    /// Covers fusion and the floor guard interacting, which no unit test does.
    #[test]
    fn the_footprintless_debris_fuses_under_the_partition_ceiling() {
        let (report, _, _) = synth_run(true, SplitGuard::Shipped);
        assert!(report.coarsen_fused > 0, "the ceiling must rescue the debris: fused {} of {} candidates", report.coarsen_fused, report.coarsen_candidates);
    }

    /// Threshold sweep over three floor shapes. Printed rather than asserted:
    /// the constant can only be argued from the table.
    #[test]
    fn threshold_sweep() {
        for whale_x_max in [100, 20, 5] {
            for guard in [SplitGuard::Ratio(1, 2), SplitGuard::Ratio(2, 3), SplitGuard::Ratio(3, 4), SplitGuard::Ratio(4, 5), SplitGuard::Off] {
                let (report, whale, _) = synth_run_at(true, guard, whale_x_max, 1.0);
                println!(
                    "whale={whale_x_max:>3}x guard={guard:?} whale_units={:>5} at_min={:>5} declined={:>4} completed={:>5} sharded_above_min={} pending_end={}",
                    cell_units(&report, &whale),
                    report.units_at_min_slice,
                    report.split_declined_at_floor,
                    report.completions.values().sum::<u64>(),
                    report.sharded_runs_above_min_slice,
                    report.pending_end,
                );
            }
        }
    }

    #[test]
    fn synth_criteria() {
        for (floored, guard) in [(true, SplitGuard::Off), (false, SplitGuard::Off), (false, SplitGuard::Shipped), (true, SplitGuard::Shipped)] {
            let (report, whale, stamped) = synth_run(floored, guard);
            println!(
                "floored={floored} guard={guard:?} whale={} stamped={} at_min={} declined={} splits={} sharded={}/{} narrowest={}s max_run={}MB pending {}->{} execs={}\n  samples={:?}",
                cell_units(&report, &whale),
                cell_units(&report, &stamped),
                report.units_at_min_slice,
                report.split_declined_at_floor,
                report.byte_splits,
                report.sharded_runs_above_min_slice,
                report.sharded_runs,
                report.narrowest_sharded_run_micros / MICROS,
                report.max_run_bytes / 1_000_000,
                report.pending_start,
                report.pending_end,
                report.executions,
                report.samples.iter().map(|s| (s.max_cell_pending, s.split_declined_at_floor)).collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn the_sim_is_deterministic_per_seed() {
        let start = 100 * DAY_MICROS;
        let a = run(journal_with_streams(4), &cfg(3), start).unwrap();
        let b = run(journal_with_streams(4), &cfg(3), start).unwrap();
        assert_eq!(a.executions, b.executions);
        assert_eq!(a.pending_end, b.pending_end);
        assert_eq!(a.frontier_lag_secs_max, b.frontier_lag_secs_max);
    }
}
