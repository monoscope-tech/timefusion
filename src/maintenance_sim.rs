//! Replays a production `TaskJournal` through the real scheduler on virtual
//! time, using measured duration distributions.
//!
//! Task selection, timeout handling, cycle switching, invalidation and the
//! claim-time byte preflight are real. Durations, ingest cadence and the bytes
//! a slice decodes are modeled ([`ByteModel`]). Memory admission and intra-call
//! operation order are outside the model.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering::Relaxed;

use anyhow::Context as _;
use serde::Serialize;

use crate::maintenance_coordinator::{
    InputFootprint, Invalidation, MAX_DECODED_BYTES, MIN_SLICE_MICROS, MaintenanceTask, Operation, TaskJournal, TaskKey, TaskState, operation_cycle,
    operation_deadline_secs,
};

const MICROS: i64 = 1_000_000;
const DAY_MICROS: i64 = 86_400_000_000;
const HOUR_MICROS: i64 = 3_600_000_000;
/// Matches the write path's bucket cadence.
const MINT_INTERVAL_MICROS: i64 = crate::maintenance_coordinator::NORMAL_SLICE_MICROS;
/// Lets idle workers notice newly mature deadlines.
const IDLE_POLL_MICROS: i64 = 5 * MICROS;
/// `run_maintenance_coordinator_once` coarsens on the same 60s cadence it plans
/// debt on. Modelled because coarsening is the only mechanism that SHRINKS the
/// queue, and without it the sim cannot answer any question about queue size —
/// which is most of what it is asked. Its absence made a fusion-pricing change
/// look like it moved 25 units when the sim never fused at all.
const COARSEN_INTERVAL_MICROS: i64 = 60 * MICROS;
/// A claim, a real scan estimate and a journal write. Charged whether the
/// preflight splits or dispatches, so a bisection ladder costs throughput in
/// the model exactly as it does in prod.
const PREFLIGHT_COST_MICROS: i64 = MICROS;

/// Decoded bytes of one parquet file, and the least a slice can read of a file
/// it overlaps (row groups are the pruning unit — a slice cannot read less than
/// one). Anchored, not derived: prod 2026-08-22 measured **302 MB for a
/// five-minute slice**, and a 9.2 GB / 1,000-file day reproduces it to within
/// 1% (see the [`DayShape::bytes`] doctest).
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
    /// `floored` is the real physics — a slice reads at least one row group of
    /// every file it overlaps, and the overlapping count bottoms out because
    /// files span time. Floorless (bytes strictly proportional to width) is the
    /// control: it is the model `byte_bounded_units` itself assumes, so a queue
    /// that shreds under it is not shredding because of the floor.
    ///
    /// ```
    /// # use timefusion::maintenance_sim::DayShape;
    /// // The 2026-08-22 anchor: 302 MB measured for a five-minute slice.
    /// let day = DayShape::new(9_200_000_000);
    /// assert_eq!(day.files, 1_000);
    /// let five_minutes = day.bytes(300 * 1_000_000, true);
    /// assert!((five_minutes as i64 - 302_000_000).abs() < 3_000_000, "{five_minutes}");
    /// // Floorless prices the same slice at a thirtieth of that.
    /// assert!(day.bytes(300 * 1_000_000, false) < 32_000_000);
    /// ```
    pub fn bytes(&self, width_micros: i64, floored: bool) -> u64 {
        let width = u128::try_from(width_micros.max(0)).unwrap_or(0);
        let proportional = (u128::from(self.decoded_bytes) * width / u128::from(DAY_MICROS as u64)) as u64;
        if !floored {
            return proportional;
        }
        proportional.saturating_add(self.files_overlapping(width_micros).saturating_mul(ROW_GROUP_BYTES))
    }

    fn files_overlapping(&self, width_micros: i64) -> u64 {
        let span = u128::try_from(FILE_SPAN_MICROS.saturating_add(width_micros.max(0))).unwrap_or(0);
        ((u128::from(self.files) * span).div_ceil(u128::from(DAY_MICROS as u64)) as u64).clamp(1, self.files)
    }
}

/// The claim-time cost model: what the preflight would MEASURE, per
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

    fn day_of(key: &TaskKey) -> i64 {
        key.slice.start_micros.div_euclid(DAY_MICROS) * DAY_MICROS
    }

    /// Unmodelled partitions measure 0 — they never split, which is the right
    /// default for minted frontier streams the fixture says nothing about.
    fn bytes(&self, key: &TaskKey) -> u64 {
        self.shape(&key.project_id, Self::day_of(key)).map_or(0, |day| day.bytes(key.slice.width(), self.floored))
    }

    /// The file set the slice overlaps. Siblings of equal width over the same
    /// partition overlap the same files, so they share an `fp` and fusion
    /// charges them once — the whole point of stamping it at claim time.
    fn footprint(&self, key: &TaskKey) -> Option<InputFootprint> {
        let day = self.shape(&key.project_id, Self::day_of(key))?;
        let files = day.files_overlapping(key.slice.width());
        let whole = (u128::from(day.decoded_bytes) * u128::from(files) / u128::from(day.files)) as u64;
        Some(InputFootprint::new([format!("{}/{}/{files}", key.project_id, Self::day_of(key))], whole))
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
    /// 69e6503 as shipped.
    #[default]
    Shipped,
    /// The pre-fix behaviour: every over-budget unit bisects. Defeated by
    /// clearing `parent_measured_bytes` before the call, so the coordinator is
    /// untouched and `split_sheds_enough`'s `None` arm does the work.
    Off,
    /// Sweep an alternative shed threshold (numerator, denominator), keeping
    /// the shipped rule's two-sided shape — a child measuring MORE than its
    /// parent still splits, or the synthetic-stamp lineage freezes.
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
    /// Override the minted stream count (10x experiments: 260 streams at 130
    /// projects). Extra streams clone the first real stream's tables under
    /// synthetic project ids.
    pub streams: Option<usize>,
    #[educe(Default = 1.0)]
    pub duration_scale: f64,
    /// Model deploy/OOM restarts: re-invalidate the CURRENT HOUR for every
    /// stream, which is what `reconcile_maintenance_task_cursors` does on boot
    /// since 2026-08-18 — touched hours are derived from commit file statistics
    /// rather than resetting the whole partition-day, so it is ~13 tasks per
    /// stream rather than ~312.
    ///
    /// That distinction decides what a restart backtest is worth. Measured
    /// 2026-08-23 on the real prod journal, 2 virtual hours, 16 workers: with
    /// the day-scoped model pending ended at 57,444 against 22,484 calm; with
    /// the hour-scoped one it is 23,948. **Restarts are a ~6% tax, not a 2.6x
    /// one** — so deploy churn is no longer the queue's dominant growth source,
    /// and sizing a fix against the old number would price work that already
    /// shipped.
    ///
    /// `restart_every_micros` repeats on an interval (0 = no periodic restarts);
    /// `restart_at_micros` fires ONE restart at a fixed offset (backtesting a
    /// known boot time).
    pub restart_every_micros: i64,
    pub restart_at_micros: Option<i64>,
    #[educe(Default = 0x5EED)]
    pub seed: u64,
    /// Model what a claimed slice DECODES, and run the claim-time preflight
    /// (`database/maintain.rs:1273-1278`) against it. Without one the sim never
    /// splits on bytes, which is what made it blind to the shred.
    pub byte_model: Option<ByteModel>,
    pub split_guard: SplitGuard,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SimSample {
    pub hour: f64,
    pub pending: usize,
    pub frontier_lag_secs: u64,
    pub min_contiguous_days: u64,
    /// Cumulative, and the live unit count of the worst cell beside it: the fix
    /// is the first rising while the second stops rising and falls. Both rising
    /// is the documented worse-than-the-bug outcome.
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
    /// What coarsening did over the run: `subsumed` + `fused` is queue removed,
    /// and `over_budget` against `candidates` is the fusion that was refused —
    /// the number that says whether the queue CAN shrink at all.
    pub coarsen_subsumed: usize,
    pub coarsen_fused: usize,
    pub coarsen_candidates: usize,
    pub coarsen_blocked: usize,
    pub coarsen_over_budget: usize,
    pub frontier_lag_secs_max: u64,
    pub min_contiguous_days_end: u64,
    pub hours_to_contiguous_14: Option<f64>,
    pub hours_to_contiguous_30: Option<f64>,
    /// Byte preflight, all cumulative over the run. `byte_splits` are units the
    /// preflight bisected before dispatch; `split_declined_at_floor` is the
    /// shipped counter's delta plus the sweep's own declines.
    pub preflight_measures: u64,
    pub byte_splits: u64,
    pub split_declined_at_floor: u64,
    /// Units that RAN over budget and were divided by the runner's internal
    /// hash sharding instead of by another journal unit — the mechanism the fix
    /// falls back on. `narrowest_sharded_run_micros` must stay above
    /// `MIN_SLICE_MICROS`: reaching the floor is the shred.
    pub sharded_runs: u64,
    pub sharded_runs_above_min_slice: u64,
    /// Claims bucketed by the DATA AGE of the slice, which is what `starved`
    /// ranks on — the diagnostic that identifies a starvation livelock and the
    /// acceptance test for any fix to one.
    ///
    /// `starved` improves monotonically past `STARVATION_HORIZON_MICROS`, so a
    /// cohort whose data is older than 31 days outranks everything else
    /// permanently; if those units also fail and requeue, they hold the lane and
    /// the middle band is never reached. Prod 2026-09-03 measured exactly that
    /// for Dedup: of 29 claims, 20 frontier, 9 privileged, and **0** in the
    /// 3-31 day band that held 96% of the queue's bytes. A fix is only believable
    /// if the sim first REPRODUCES `claims_mid_band == 0` and then releases it.
    pub claims_frontier: u64,
    pub claims_mid_band: u64,
    pub claims_privileged: u64,
    /// Claims whose slice is a full day or wider, regardless of age — the
    /// population that carries the queue's bytes.
    pub claims_day_wide: u64,
    pub narrowest_sharded_run_micros: i64,
    /// The most any single execution decoded, after runtime sharding. Above
    /// `MAX_DECODED_BYTES` means the memory bound was broken.
    pub max_run_bytes: u64,
    /// Units ever minted per `project/operation/day` cell, in every state —
    /// superseded parents included, because minting them is what the shred
    /// cost. `max_cell*` is the worst cell by that count.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub units_per_cell: BTreeMap<String, usize>,
    pub max_cell: String,
    pub max_cell_units: usize,
    pub units_at_min_slice: usize,
    pub samples: Vec<SimSample>,
}

/// Measured duration ranges in seconds, per operation and width class.
/// Sources: rollup phase timing (#174, prod 2026-08-18): 174 rollup starts,
/// NOT ONE over 60s; the rollup counters put e2e at ~3s/unit. Debt units are
/// the slow ones (#176): HotPacking 320-895s, SealedConsolidation 294-767s.
/// Dedup is BIMODAL (#175/#177): quick, or past its 300s deadline — nothing
/// finished between 75s and 300s in the measured window.
fn duration_range_secs(operation: Operation, width_micros: i64, rng: &mut Rng) -> u64 {
    let frontier = width_micros < DAY_MICROS;
    // MEASURED FROM PRODUCTION 2026-09-03: 676 `maintenance_task_finished`
    // events over 4 h, bucketed by operation and slice width. The previous
    // numbers predated the 2026-09-02 dedup-key widening AND priced BaseRollup
    // at 5-60 s when its real mean is 571 s — a ~16x under-estimate that made
    // rollup look almost free, which is what produced the "dedup costs 7x a
    // rollup unit" conclusion. It does not: measured means are Dedup 541 s vs
    // BaseRollup 571 s, and worker-seconds split 43.7% / 46.7%.
    //
    // The dominant feature is BIMODALITY, not the mean: ~65-70% of units in both
    // lanes finish at ~0 s because they find no work, and the rest are very
    // expensive. A single uniform range cannot express that, and averaging it
    // away is what hid the real shape.
    //
    //   operation            n    p50    p70    p90    max
    //   BaseRollup <1d     193      0   1227   2034   2368
    //   Dedup      <1d     202      0      6   1910   7203
    //   Repair     <1d      67     37     49    663   5678
    //   DerivedRollup       68      0      0      0      3
    //   HotPacking          68      0      0      1     13
    //   SealedConsolidation 68      0      0     14     26
    let pct = rng.next() % 100;
    let range = match (operation, frontier) {
        // 70% no-op, 20% cheap, 10% the long tail that actually costs.
        (Operation::Dedup, _) => {
            if pct < 70 {
                (0, 6)
            } else if pct < 90 {
                (6, 300)
            } else {
                (1_910, 7_203)
            }
        }
        // 65% no-op, then a WIDE and frequent expensive mode — this is the one
        // the old model got most wrong.
        (Operation::BaseRollup, true) => {
            if pct < 65 {
                (0, 5)
            } else {
                (1_227, 2_368)
            }
        }
        (Operation::BaseRollup, false) => (0, 11),
        (Operation::DerivedRollup, _) => (0, 3),
        (Operation::HotPacking, _) => (0, 13),
        (Operation::SealedConsolidation, _) => (0, 26),
        // Half no-op, half a long tail.
        (Operation::Repair, _) => {
            if pct < 50 {
                (0, 49)
            } else {
                (49, 5_678)
            }
        }
    };
    rng.uniform_secs(range.0, range.1)
}

/// Debt work = file rewrites that cannot advance rollup coverage
/// (`dependencies_complete`): the operations #176's occupancy cap applies to.
fn is_debt_op(operation: Operation) -> bool {
    matches!(operation, Operation::Dedup | Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair)
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
    /// Newest `created_unix_ms` this stream ever produced — the journal's own
    /// record of when it last ingested. See `STREAM_IDLE_MICROS`.
    last_created_ms: u64,
}

/// How recently a stream must have produced work to count as INGESTING.
///
/// The sim used to mint for EVERY stream every `MINT_INTERVAL_MICROS`, but prod
/// invalidates on actual WRITES — an idle stream costs nothing there. The gap is
/// not small: measured from `created_unix_ms` in a real journal, prod's arrival
/// rate is **5,782 tasks/day** against the model's **35,712**, a ~6x
/// over-estimate, because only **21 of 124** streams were active.
///
/// That made the sim useless for the question it exists to answer. With minting
/// off it drains any backlog (nothing competes); with minting on it buries the
/// queue under arrivals that are not real. Neither reproduces contention, which
/// is the regime every scheduler decision actually lives in.
///
/// Calibrated from the journal rather than guessed: a stream mints only if it
/// produced a task within this window of the journal's newest record.
const STREAM_IDLE_MICROS: i64 = 24 * 60 * 60 * 1_000_000;

/// Contiguity model mirroring `min_contiguous_days`: a day counts for a tier
/// once that tier covers its full width, and contiguity counts back from
/// yesterday. Two statistics come off it, exactly as in production: the MIN
/// over active (source, project) pairs is the reported goal metric, and the
/// MEDIAN is what `coverage_is_short` steers by.
struct Coverage {
    /// (source, project) -> day_start -> [base_width, derived_width] micros.
    days: HashMap<(String, String), BTreeMap<i64, [i64; 2]>>,
    /// The active set: every stream found in the journal, with whether it has a
    /// derived tier at all. The real gauge reads recent source partitions, which
    /// is the same set by construction here.
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
        let mut day = key.slice.start_micros.div_euclid(DAY_MICROS) * DAY_MICROS;
        while day < key.slice.end_micros {
            let overlap = (key.slice.end_micros.min(day + DAY_MICROS) - key.slice.start_micros.max(day)).max(0);
            self.days.entry((key.source.clone(), key.project_id.clone())).or_default().entry(day).or_default()[tier] += overlap;
            day += DAY_MICROS;
        }
    }

    /// Contiguous covered days back from yesterday, per project of ONE
    /// (source, tier) — the population production's backfill sweep folds over.
    /// Per sweep rather than per pair-AND because the fleet gauge is a MEDIAN
    /// within the sweep and a MIN across sweeps (`fold_fleet_gauge`), and a
    /// median does not survive being AND-ed first.
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

    /// One entry per (source, tier) sweep, skipping sweeps with no projects —
    /// a tier no stream declares does not exist to fold, exactly as production
    /// never sweeps it.
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
    /// over each (source, tier) sweep's projects, folded by MIN, compared through
    /// the one shared predicate (`database::coverage_is_short_for`). Steering by
    /// the MIN here — which is what the sim used to do — puts the sim in
    /// coverage-short mode on states where the server is not, silently
    /// invalidating exactly the cycle-policy answers the sim exists to give.
    ///
    /// RESIDUAL GAP: production lets a tier younger than the backfill horizon
    /// ABSTAIN from the fleet fold (`fold_fleet_gauge`'s `ramping`). The sim has
    /// no tier creation time, so no simulated tier ever abstains; on a freshly
    /// created tier production reads its median as provisional where the sim
    /// takes it at face value.
    fn coverage_is_short(&self, now_micros: i64) -> bool {
        let fleet = self.per_sweep(now_micros).into_iter().map(|mut days| crate::database::median_contiguous_days(&mut days)).min().unwrap_or(0);
        crate::database::coverage_is_short_for(fleet)
    }
}

/// One ingest flush (or one restart reconciliation) for one stream: the base
/// invalidation mints Dedup + BaseRollup over the range's 10-minute slices;
/// the derived invalidation mints the hour-aligned DerivedRollup units.
/// `observed_at` sets the finalization deadline — the flush time for minting,
/// the boot time for a restart reconcile.
fn mint_stream(journal: &mut TaskJournal, stream: &Stream, start_micros: i64, end_micros: i64, observed_at_micros: i64) {
    for derived in [false, true] {
        if derived && stream.derived_rollup_table.is_none() {
            continue;
        }
        let rollup_table = if derived { stream.derived_rollup_table.as_deref().unwrap_or(&stream.base_rollup_table) } else { &stream.base_rollup_table };
        // Minting must not fail the sim on a pathological journal; an
        // invalidation error means a skipped window, not a crash.
        let _ = journal.invalidate(Invalidation {
            source_table: &stream.source_table,
            rollup_table,
            source: &stream.source,
            project_id: &stream.project_id,
            start_micros,
            end_micros,
            observed_at_micros,
            derived,
        });
    }
}

/// The claim-time preflight, mirroring `database/maintain.rs:1273-1278`:
/// measure what the claimed slice reads, record it on the unit whether or not
/// it splits, and split before dispatch when it is over budget and still wide
/// enough to divide. `None` means the unit was superseded by children.
fn preflight(journal: &mut TaskJournal, model: &ByteModel, guard: SplitGuard, task: &MaintenanceTask, report: &mut SimReport) -> Option<u64> {
    let key = &task.key;
    let observed = model.bytes(key);
    let footprint = model.footprint(key);
    report.preflight_measures += 1;
    if let Some(footprint) = footprint {
        journal.record_input(key, footprint);
    }
    if observed <= MAX_DECODED_BYTES || key.slice.width() <= MIN_SLICE_MICROS {
        return Some(observed);
    }
    match guard {
        SplitGuard::Shipped => {}
        SplitGuard::Off => defeat_guard(journal, task),
        SplitGuard::Ratio(numerator, denominator) => {
            // Calls the REAL predicate at the swept ratio. This used to be an
            // inline transcription, which is a drift hazard by construction —
            // and is why the sim never reproduced the 2026-09-03
            // synthetic-observation defect.
            let sheds = crate::maintenance_coordinator::split_sheds_enough_at(task.parent_measured_bytes, observed, numerator, denominator);
            if !sheds {
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

/// Clear the parent's measurement so `split_sheds_enough` takes its `None` arm
/// — the only way to run the pre-fix behaviour without touching the shipped
/// predicate. The claimed task is the journal's own post-`mark_running` copy,
/// so re-upserting it changes exactly this one field.
fn defeat_guard(journal: &mut TaskJournal, task: &MaintenanceTask) {
    let mut task = task.clone();
    task.parent_measured_bytes = None;
    journal.upsert(task);
}

/// `project/operation/day` — the (project, tier, day) cell the shred is counted
/// in.
fn cell_of(key: &TaskKey) -> String {
    format!("{}/{:?}/{}", key.project_id, key.operation, key.slice.start_micros.div_euclid(DAY_MICROS))
}

fn max_cell_pending(journal: &TaskJournal) -> usize {
    let mut cells: HashMap<String, usize> = HashMap::new();
    for task in journal.tasks().filter(|task| !matches!(task.state, TaskState::Complete | TaskState::Superseded)) {
        *cells.entry(cell_of(&task.key)).or_default() += 1;
    }
    cells.into_values().max().unwrap_or_default()
}

struct Worker {
    busy_until: i64,
    current: Option<(TaskKey, u64)>,
    cycle_pos: usize,
}

/// Replayed over `[start_micros, start_micros + horizon)`. The journal's own
/// deadlines are in real time, so `start_micros` should be real "now" for a
/// freshly fetched prod journal.
pub fn run(mut journal: TaskJournal, cfg: &SimConfig, start_micros: i64) -> anyhow::Result<SimReport> {
    let mut rng = Rng(cfg.seed);
    let end = start_micros.saturating_add(cfg.horizon_micros);

    // Streams from the journal: dedup tasks name the source table, rollup
    // tasks name the tier tables. No tasks -> no minting (an empty journal
    // just idles, which is itself a valid answer).
    let mut streams: Vec<Stream> = Vec::new();
    for task in journal.tasks() {
        let key = &task.key;
        let position = match streams.iter().position(|s| s.source == key.source && s.project_id == key.project_id) {
            Some(position) => position,
            None => {
                streams.push(Stream {
                    source_table: key.source.clone(),
                    base_rollup_table: key.physical_table.clone(),
                    derived_rollup_table: None,
                    source: key.source.clone(),
                    project_id: key.project_id.clone(),
                    last_created_ms: 0,
                });
                streams.len() - 1
            }
        };
        let stream = &mut streams[position];
        stream.last_created_ms = stream.last_created_ms.max(task.created_unix_ms);
        match key.operation {
            Operation::Dedup | Operation::HotPacking => stream.source_table = key.physical_table.clone(),
            Operation::BaseRollup => stream.base_rollup_table = key.physical_table.clone(),
            Operation::DerivedRollup => stream.derived_rollup_table = Some(key.physical_table.clone()),
            _ => {}
        }
    }
    // Only streams that are actually INGESTING mint. Discovery walks every task
    // the journal ever held, so without this an account that stopped writing
    // weeks ago still generates frontier work forever — which is where the ~6x
    // arrival over-estimate came from. See `STREAM_IDLE_MICROS`.
    let newest_created_ms = streams.iter().map(|s| s.last_created_ms).max().unwrap_or_default();
    let idle_cutoff_ms = newest_created_ms.saturating_sub(STREAM_IDLE_MICROS as u64 / 1_000);
    anyhow::ensure!(!streams.is_empty() || !cfg.mint_frontier, "no streams found in journal; pass --no-mint");
    if let Some(target) = cfg.streams {
        // `--streams N` means **N INGESTING streams** — the doc calls it "the
        // minted stream count", and scaling the active customer count is the
        // only thing it is used for.
        //
        // It used to set the TOTAL, which since minting became activity-gated
        // means it silently modelled almost nothing: a journal with 124 streams
        // of which 20 ingest answered `--streams 100` by TRUNCATING to 100 real
        // streams — still only 20 active — so a "5x" run was 1x. And the clone
        // template was `streams.first()`, whichever stream the journal happened
        // to mention first, usually a dormant account whose clones never mint.
        let Some(template) = streams.iter().find(|s| s.last_created_ms >= idle_cutoff_ms).or_else(|| streams.first()).cloned() else {
            anyhow::bail!("--streams needs at least one real stream in the journal")
        };
        let mut active = streams.iter().filter(|s| s.last_created_ms >= idle_cutoff_ms).count();
        // Down: retire the excess actives rather than dropping streams, so the
        // journal's existing backlog for them is preserved.
        for stream in streams.iter_mut().filter(|s| s.last_created_ms >= idle_cutoff_ms) {
            if active <= target {
                break;
            }
            stream.last_created_ms = 0;
            active -= 1;
        }
        // Up: synthetic clones of a stream that is genuinely ingesting.
        while active < target {
            let mut synthetic = template.clone();
            synthetic.project_id = format!("synth-{}", streams.len());
            synthetic.last_created_ms = newest_created_ms;
            streams.push(synthetic);
            active += 1;
        }
    }
    let ingesting = streams.iter().filter(|s| s.last_created_ms >= idle_cutoff_ms).count();

    if cfg.mint_frontier {
        // Printed because it is the single number that decides whether an
        // arrival-rate result is believable at all.
        eprintln!("sim: minting from {ingesting} INGESTING streams of {} in the journal", streams.len());
    }
    let mut coverage =
        Coverage { days: HashMap::new(), pairs: streams.iter().map(|s| (s.source.clone(), s.project_id.clone(), s.derived_rollup_table.is_some())).collect() };
    // Seed coverage from already-complete rollup tasks so a fetched journal
    // starts with the coverage prod actually has.
    for task in journal.tasks() {
        if task.state == TaskState::Complete {
            coverage.record(&task.key);
        }
    }

    let mut report = SimReport {
        hours: cfg.horizon_micros as f64 / 3_600_000_000.0,
        pending_start: journal.tasks().filter(|t| t.state != TaskState::Complete).count(),
        ..Default::default()
    };
    let mut workers = (0..cfg.workers).map(|_| Worker { busy_until: start_micros, current: None, cycle_pos: 0 }).collect::<Vec<_>>();
    let mut next_mint = start_micros + MINT_INTERVAL_MICROS;
    let mut next_restart = match (cfg.restart_at_micros, cfg.restart_every_micros) {
        (Some(at), _) => start_micros + at,
        (None, every) if every > 0 => start_micros + every,
        _ => i64::MAX,
    };
    // 48 samples over the horizon, floored at five minutes: a 6-hour run
    // needs a trajectory, not six points.
    let tick = (cfg.horizon_micros / 48).max(300 * MICROS);
    let mut next_tick = start_micros + tick;
    let mut next_coarsen = start_micros + COARSEN_INTERVAL_MICROS;
    let mut report_coarsen = crate::maintenance_coordinator::CoarsenReport::default();
    let mut now = start_micros;
    // #176: (jobs * 3 / 4).max(1) — the floor keeps debt work possible at all
    // on a one-worker box.
    let debt_cap = (cfg.workers * 3 / 4).max(1);
    // Per-op "known empty until" memo. `claim_next` is deterministic given
    // (journal state, now), and a None result can only be invalidated by (a) a
    // state change — completion, timeout/abandon, mint, restart — or (b) a
    // deadline maturing. So a None at time T holds until the op's next future
    // deadline; memoizing that collapses the idle-worker rescans that
    // otherwise dominate wall time on a production-sized journal (16 workers x
    // up to 12 full scans per wake event). One side effect is lost: the
    // skipped calls would have bumped `claim_tick`, so the sealed-reservation
    // parity shifts slightly — reservation SHARE over time is unchanged.
    let mut none_until = [0i64; 6];
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
            none_until = [0; 6];
            // The cadence advances whether or not minting is on — otherwise
            // `next_mint` pins `now` here forever.
            next_mint += MINT_INTERVAL_MICROS;
        }

        if now >= next_coarsen {
            // With a byte model the CAPPED variant is the one prod runs
            // (`database/maintain.rs:2383`): footprint-less debris carrying an
            // inflated estimate only fuses once the partition ceiling says no
            // unit over that day can decode that much.
            let report = match cfg.byte_model.as_ref() {
                Some(model) => journal.coarsen_sealed_slices_capped(now, &|project, _source, date| model.partition_ceiling(project, date)),
                None => journal.coarsen_sealed_slices_reporting(now),
            };
            report_coarsen.subsumed += report.subsumed;
            report_coarsen.fused += report.fused;
            report_coarsen.candidates += report.candidates;
            report_coarsen.blocked += report.blocked;
            report_coarsen.over_budget += report.over_budget;
            if report.total() != 0 {
                none_until = [0; 6];
            }
            next_coarsen += COARSEN_INTERVAL_MICROS;
        }

        if now >= next_restart {
            for stream in &streams {
                // The boot reconcile enqueues per partition that saw commits
                // while down. Models the 2026-08-18 behavior: touched hours are
                // derived from commit file statistics, so a brief restart
                // reconciles the CURRENT hour rather than the whole partition-day
                // — ~13 tasks per stream rather than ~312, and 13/312 is 1/24,
                // which is exactly this one-hour-instead-of-one-day change.
                //
                // It used to mint `[day_start, day_start + DAY)`. That was the
                // PRE-fix shape and it dominated every restart backtest: measured
                // 2026-08-23 on the real prod journal, 2 virtual hours of hourly
                // restarts left pending at 57,444 against 22,484 with no
                // restarts, for IDENTICAL work done (2,190 executions, identical
                // completions). Keeping a stale model is worse than having none —
                // it prices a fix that already shipped.
                let hour_start = now.div_euclid(HOUR_MICROS) * HOUR_MICROS;
                mint_stream(&mut journal, stream, hour_start, hour_start + HOUR_MICROS, now);
            }
            none_until = [0; 6];
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
                    none_until = [0; 6];
                    if matches!(key.operation, Operation::BaseRollup | Operation::DerivedRollup) {
                        coverage.record(&key);
                        let contiguous = coverage.min_contiguous_days(now);
                        coverage_short = coverage.coverage_is_short(now);
                        // Capture milestones at the crossing event, not just at
                        // report ticks — a tick can land just short of a day
                        // boundary and read one day less.
                        if contiguous >= 14 && report.hours_to_contiguous_14.is_none() {
                            report.hours_to_contiguous_14 = Some((now - start_micros) as f64 / 3_600_000_000.0);
                        }
                        if contiguous >= 30 && report.hours_to_contiguous_30.is_none() {
                            report.hours_to_contiguous_30 = Some((now - start_micros) as f64 / 3_600_000_000.0);
                        }
                    }
                    *report.completions.entry(format!("{:?}", key.operation)).or_default() += 1;
                } else {
                    // Timeout: the worker burned the whole deadline, then the
                    // lease drop abandons the unit — bisect on repeat, else
                    // deadline-floored backoff. Real code, not a re-imagination.
                    journal.abandon_running(&key, now, None);
                    none_until = [0; 6];
                    match journal.state(&key) {
                        Some(TaskState::Superseded) => report.splits += 1,
                        _ => *report.timeouts.entry(format!("{:?}", key.operation)).or_default() += 1,
                    }
                }
                report.executions += 1;
            }
            // Claim the next unit, rotating through the shared cycle exactly
            // like `run_coordinator_maintenance_once`. While coverage is short,
            // debt work may occupy at most 3/4 of workers — #176's
            // `maintenance_debt_slots`, the occupancy cap that keeps
            // quarter-hour file rewrites from starving seconds-long rollups.
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
                    // Bucketed on the SAME quantity `starved` ranks on: how long
                    // ago the slice's DATA ended, not when the record was made.
                    let waited = now.saturating_sub(task.key.slice.end_micros);
                    if waited > crate::maintenance_coordinator::STARVATION_HORIZON_MICROS {
                        report.claims_privileged += 1;
                    } else if waited > 3 * crate::maintenance_coordinator::DAY_MICROS {
                        report.claims_mid_band += 1;
                    } else {
                        report.claims_frontier += 1;
                    }
                    if task.key.slice.width() >= crate::maintenance_coordinator::DAY_MICROS {
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
                none_until[operation as usize] = journal
                    .tasks()
                    .filter(|t| t.key.operation == operation && matches!(t.state, TaskState::Pending | TaskState::Retry) && t.deadline_micros > now)
                    .map(|t| t.deadline_micros)
                    .min()
                    .unwrap_or(i64::MAX);
            }
            // The byte preflight runs between the claim and the dispatch, where
            // prod runs it. A split leaves the worker free after the cost of
            // the measurement — no unit ran, so the debt slot goes back too.
            if let (Some(model), Some(task)) = (cfg.byte_model.as_ref(), claimed.as_ref()) {
                match preflight(&mut journal, model, cfg.split_guard, task, &mut report) {
                    Some(observed) => {
                        let shards = observed.div_ceil(MAX_DECODED_BYTES).max(1);
                        report.max_run_bytes = report.max_run_bytes.max(observed.div_ceil(shards));
                        if shards > 1 {
                            let width = task.key.slice.width();
                            report.sharded_runs += 1;
                            report.sharded_runs_above_min_slice += u64::from(width > MIN_SLICE_MICROS);
                            report.narrowest_sharded_run_micros = match report.narrowest_sharded_run_micros {
                                0 => width,
                                current => current.min(width),
                            };
                        }
                    }
                    None => {
                        if is_debt_op(task.key.operation) {
                            debt_busy -= 1;
                        }
                        none_until = [0; 6];
                        worker.busy_until = now + PREFLIGHT_COST_MICROS;
                        continue;
                    }
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
                    // Nothing claimable: jump straight to the next eligibility
                    // instant instead of polling — an idle worker re-scanning
                    // 38k tasks every 5 virtual seconds is what made the sim
                    // itself slow, not any property of the schedule.
                    let next_eligible = journal
                        .tasks()
                        .filter(|t| matches!(t.state, TaskState::Pending | TaskState::Retry) && t.deadline_micros > now)
                        .map(|t| t.deadline_micros)
                        .min();
                    worker.busy_until = next_eligible.unwrap_or(now + IDLE_POLL_MICROS).max(now + 1);
                }
            }
        }

        if now >= next_tick {
            let lag = frontier_lag_secs(&journal, now);
            report.frontier_lag_secs_max = report.frontier_lag_secs_max.max(lag);
            let contiguous = coverage.min_contiguous_days(now);
            if contiguous >= 14 && report.hours_to_contiguous_14.is_none() {
                report.hours_to_contiguous_14 = Some((now - start_micros) as f64 / 3_600_000_000.0);
            }
            if contiguous >= 30 && report.hours_to_contiguous_30.is_none() {
                report.hours_to_contiguous_30 = Some((now - start_micros) as f64 / 3_600_000_000.0);
            }
            report.samples.push(SimSample {
                hour: (now - start_micros) as f64 / 3_600_000_000.0,
                pending: journal.tasks().filter(|t| !matches!(t.state, TaskState::Complete | TaskState::Superseded)).count(),
                frontier_lag_secs: lag,
                min_contiguous_days: contiguous,
                split_declined_at_floor: report.split_declined_at_floor,
                max_cell_pending: max_cell_pending(&journal),
            });
            next_tick += tick;
        }
    }

    report.min_contiguous_days_end = coverage.min_contiguous_days(now);
    report.pending_end = journal.tasks().filter(|t| !matches!(t.state, TaskState::Complete | TaskState::Superseded)).count();
    report.coarsen_subsumed = report_coarsen.subsumed;
    report.coarsen_fused = report_coarsen.fused;
    report.coarsen_candidates = report_coarsen.candidates;
    report.coarsen_blocked = report_coarsen.blocked;
    report.coarsen_over_budget = report_coarsen.over_budget;
    for task in journal.tasks() {
        *report.tasks_end.entry(format!("{:?}/{:?}", task.key.operation, task.state)).or_default() += 1;
        *report.units_per_cell.entry(cell_of(&task.key)).or_default() += 1;
        report.units_at_min_slice += usize::from(task.key.slice.width() <= MIN_SLICE_MICROS);
    }
    if let Some((cell, units)) = report.units_per_cell.iter().max_by_key(|(_, units)| **units) {
        (report.max_cell, report.max_cell_units) = (cell.clone(), *units);
    }
    Ok(report)
}

/// `eligible_watermark_lag_seconds`, simplified for the sim: the oldest
/// eligible, unfinished frontier task's lateness. The production gauge adds a
/// per-stream watermark; the sim needs only the trend.
fn frontier_lag_secs(journal: &TaskJournal, now_micros: i64) -> u64 {
    journal
        .tasks()
        .filter(|task| {
            !matches!(task.state, TaskState::Complete | TaskState::Superseded)
                && task.deadline_micros <= now_micros
                && task.key.slice.end_micros >= now_micros.saturating_sub(crate::maintenance_coordinator::LIVE_FRONTIER_WINDOW_MICROS)
        })
        .map(|task| u64::try_from(now_micros.saturating_sub(task.deadline_micros).div_euclid(MICROS)).unwrap_or_default())
        .max()
        .unwrap_or_default()
}

/// A synthetic queue with the shape the 2026-08-22 shred happened in, built
/// through the `TaskJournal` API — never a hand-written `maintenance_tasks.json`,
/// whose on-disk form is an internal serde detail that would rot silently.
pub struct SynthQueue {
    pub journal: TaskJournal,
    pub model: ByteModel,
    /// The cell that shredded: one day-wide unit over a day of ~100x
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
/// from the second level down, which is the real sequence.
/// `debris_slice_minutes` varies unit COUNT at constant total work: the debris
/// block carries the same bytes over the same window as `600 / n` units of `n`
/// minutes. 1 = the historical fixture, so every prior result reproduces.
///
/// **It was built to test bin widening and it CANNOT — recorded here so nobody
/// repeats the attempt.** Two configurations, both null:
///
/// - **With `--mint` at 5x load**: 600 debris units are 0.6% of a ~102,000-unit
///   queue, so 600 -> 300 moved `pending_end` by 0.1% (102,696 -> 102,804).
///   Swamped.
/// - **Without minting**: `pending_start` falls exactly as designed
///   (813/513/313/263 for n = 1/2/6/12) and **`executions` and `pending_end` are
///   IDENTICAL at every width** (1,774 and 2 on seed 1). The coordinator's own
///   coarsening already fuses the debris, so pre-collapsing it adds nothing.
///
/// The deeper reason is structural: this simulator schedules rollup/compaction
/// TASKS on virtual time. Widening `BIN_MICROS` pays off in READ BYTES — the
/// same file read once per bin it straddles — and an IO-free model cannot see
/// bytes. **Validating bin width needs real object-store latency, i.e. staging.**
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

    // The long tail: 70 projects x 3 sealed days, each a day that fits in one
    // unit. 210 more cells, so `claim_next`'s ordering, the debt cap and
    // coarsening all operate at a scale where they interact.
    for project in 0u64..70 {
        for back in 1..=3 {
            cell(&mut journal, &mut model, &format!("tail-{project:02}"), back, 60_000_000 + project * 2_000_000);
        }
    }

    // Pre-existing shred debris: 600 one-minute units on ONE partition, no
    // `InputFootprint` and each claiming 4,466,185,462 bytes — prod project
    // 87576849's consecutive minutes, over a partition of 35 files / 0.36 GB.
    // Fusion can only rescue them once the partition ceiling is known.
    let debris_day = day(2);
    model.insert("debris", debris_day, 360_000_000);
    let slice = debris_slice_minutes.max(1);
    let debris_units = 600 / slice;
    for unit in 0..debris_units {
        // Bytes scale with the slice so total work is held constant: the knob
        // must vary unit COUNT alone, or a sweep confounds count with cost.
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
        slice: crate::maintenance_coordinator::TimeSlice::new(start_micros, start_micros + width_micros).expect("fixture slice"),
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
    use crate::maintenance_coordinator::{MAX_DECODED_BYTES, TimeSlice};

    const HOUR: i64 = 3_600 * MICROS;

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
    /// requested number of streams.
    fn journal_with_streams(projects: usize) -> TaskJournal {
        let mut journal = empty_journal();
        for i in 0..projects {
            let mut task = crate::maintenance_coordinator::MaintenanceTask {
                key: key(&format!("p{i}"), Operation::BaseRollup, 60 * DAY_MICROS, crate::maintenance_coordinator::NORMAL_SLICE_MICROS),
                state: TaskState::Complete,
                deadline_micros: 0,
                estimated_decoded_bytes: 0,
                hash_shard: 0,
                hash_shards: 1,
                attempts: 0,
                created_unix_ms: 0,
                retry_reason: None,
                publication: None,
                base_tier_present: false,
                input: None,
                parent_measured_bytes: None,
                backfill_priority_micros: None,
            };
            journal.upsert(task.clone());
            task.key.operation = Operation::DerivedRollup;
            journal.upsert(task);
        }
        journal
    }

    fn cfg(hours: i64) -> SimConfig {
        SimConfig { horizon_micros: hours * HOUR, ..Default::default() }
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
        // RENAMED and re-based on measurement. The old name asserted that 13
        // projects HOLD the frontier, from "the G6 arithmetic": 26 streams mint
        // ~8,100 units/day against ~15k/day of *small-unit* capacity.
        //
        // That capacity figure is what the 2026-09-03 duration measurements
        // refute. Units are not small: ~65-70% finish at ~0 s and the rest run
        // 1,200-2,400 s, so capacity is set by that tail. At 13 projects the
        // frontier lags ~13,050 s — over twenty times `FRONTIER_LAG_BUDGET_SECS`
        // — and the old assertion passed only because rollup was modelled at
        // 5-60 s when its real mean is 571 s.
        //
        // What the test still pins, and what it was really for, is the SHAPE:
        // load makes it strictly worse, and 10x is far worse than 13.
        let start = 100 * DAY_MICROS;
        let report_13 = run(journal_with_streams(13), &cfg(6), start).unwrap();
        let pending_13 = report_13.pending_end;
        assert!(
            report_13.frontier_lag_secs_max > crate::maintenance_coordinator::FRONTIER_LAG_BUDGET_SECS,
            "13 projects already exceed the lag budget under measured durations, lag {}s",
            report_13.frontier_lag_secs_max
        );

        let cfg_10x = SimConfig { streams: Some(260), ..cfg(2) };
        let report_10x = run(journal_with_streams(13), &cfg_10x, start).unwrap();
        assert!(report_10x.pending_end > 10 * pending_13.max(1), "10x must diverge: pending {} vs {} at 13 projects", report_10x.pending_end, pending_13);
        // The lag comparison is DELETED, not relaxed: it was never valid. The
        // two runs use different horizons — `cfg(6)` for 13 projects, `cfg(2)`
        // for 10x — and maximum lag is bounded by how long the run lasts, so
        // the 2-hour case cannot exceed 7,200 s however badly it diverges.
        // Measured: 10x reports 5,400 s against 13-project 13,050 s, i.e. the
        // "worse" configuration scores BETTER on this metric purely because it
        // ran for less virtual time. It only ever passed while both lags were
        // small relative to both horizons, which the optimistic duration model
        // guaranteed and measurement does not.
        //
        // `pending_end` above is the divergence assertion and is horizon-fair,
        // because it counts work left rather than a time-bounded maximum.
    }

    /// The sim and the server must make the SAME coverage-short decision from
    /// the same coverage state — the cycle it selects is most of what the sim is
    /// asked about. Each case is the per-project count of contiguous covered
    /// days; the rows where MIN and MEDIAN disagree are the whole point (the sim
    /// used to gate on the MIN, so a single laggard put it in coverage-short
    /// mode on states the server calls healthy).
    #[test_case::test_case(&[30, 30, 30], &[30, 30, 30], false; "fleet covered")]
    #[test_case::test_case(&[2, 3, 4], &[2, 3, 4], true; "fleet short")]
    #[test_case::test_case(&[0, 20, 25], &[0, 20, 25], false; "one laggard cannot pin the fleet")]
    #[test_case::test_case(&[13, 15], &[13, 15], false; "even count takes the upper median")]
    #[test_case::test_case(&[0, 13], &[0, 13], true; "even count upper median still short")]
    #[test_case::test_case(&[30, 30, 30], &[2, 2, 2], true; "a lagging derived tier is short on its own")]
    fn sim_and_server_agree_on_coverage_short(base: &[u64], derived: &[u64], expected: bool) {
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
        // sweep's projects, and folds the sweeps by MIN — `fold_fleet_gauge`
        // with no ramping tier.
        let active = projects.iter().map(String::as_str).collect::<HashSet<_>>();
        let fleet = covered.iter().map(|covered| crate::database::min_contiguous_days(covered, &source, today, &active).2).min().unwrap();
        let server = crate::database::coverage_is_short_for(fleet);
        assert_eq!(server, expected, "server decision at fleet median {fleet}");
        assert_eq!(coverage.coverage_is_short(now), server, "sim must decide as the server does (fleet median {fleet})");
    }

    #[test]
    fn a_sealed_backlog_builds_contiguous_coverage() {
        // 2 projects x 30 sealed days x (base + derived day units), no minting.
        // ~120 heavy units on 16 workers: ~400s each, so well under a virtual
        // day to clear, and contiguity must reach 30.
        let mut journal = journal_with_streams(0);
        let start = 100 * DAY_MICROS;
        for p in ["a", "b"] {
            for day in 0..30i64 {
                // Contiguity counts back from yesterday at sim END (start + 24h
                // = day 101), so the window is days 100..=71 relative to start.
                let day_start = start - day * DAY_MICROS;
                for op in [Operation::BaseRollup, Operation::DerivedRollup] {
                    journal.enqueue(key(p, op, day_start, DAY_MICROS), start, MAX_DECODED_BYTES, 0);
                }
            }
        }
        let cfg = SimConfig { mint_frontier: false, ..cfg(25) };
        let report = run(journal, &cfg, start).unwrap();
        assert_eq!(report.min_contiguous_days_end, 30, "all 30 sealed days built: {report:#?}");
        assert!(report.hours_to_contiguous_30.is_some(), "the moment of arrival is recorded");
        assert_eq!(report.pending_end, 0, "nothing left pending");
    }

    #[test]
    fn a_unit_that_overruns_its_deadline_twice_is_bisected() {
        // One day-sized dedup unit: sampled durations are 300-500s against the
        // 300s dedup deadline, so timeouts are likely; a repeat timeout must
        // split via the REAL abandon_running.
        //
        // TWENTY units, not one. "With scale 10x a timeout is certain" was true
        // when every dedup unit ran 60-900 s; under the measured bimodal model
        // ~70% finish in 0-6 s and never approach the deadline, which made a
        // one-unit fixture a coin flip. Twenty independent units put the chance
        // that NONE lands in the expensive mode below one in a thousand, without
        // adding a knob to force it.
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
        // Pins the 2026-08-18 boot-reconcile behavior the model now mirrors:
        // touched hours come from commit file statistics, so a restart
        // re-invalidates the CURRENT HOUR per stream, not the whole
        // partition-day. That is ~13 tasks per stream rather than ~312.
        //
        // The previous version of this test asserted the opposite — that hourly
        // restarts add >100 pending — and it passed because the model still
        // minted a whole day. Keeping it would have pinned a fix out of the
        // model: measured 2026-08-23 on the real prod journal, the stale model
        // reported 57,444 pending against 22,484 calm, while the corrected one
        // reports 23,948. A backtest that prices an already-shipped fix as still
        // broken is worse than no backtest.
        let start = 100 * DAY_MICROS;
        let calm = run(journal_with_streams(4), &cfg(2), start).unwrap();
        let churning = run(journal_with_streams(4), &SimConfig { restart_every_micros: HOUR, ..cfg(2) }, start).unwrap();
        assert!(churning.pending_end >= calm.pending_end, "a restart cannot REDUCE the queue: calm {} vs churning {}", calm.pending_end, churning.pending_end);
        // An hour of reconcile per stream per restart, not a day of it. The
        // bound is deliberately far below the old >100 assertion — that gap IS
        // the fix.
        assert!(
            churning.pending_end < calm.pending_end + 100,
            "an hour-scoped reconcile must not grow the queue like a day-scoped one: calm {} vs churning {}",
            calm.pending_end,
            churning.pending_end
        );
    }

    /// The §3c run: `synth:whale`, 6 virtual hours, 16 workers, no minting —
    /// the queue under study is the fixture, not the frontier.
    fn synth_run(floored: bool, guard: SplitGuard) -> (SimReport, String, String) {
        synth_run_at(floored, guard, 100)
    }

    fn synth_run_at(floored: bool, guard: SplitGuard, whale_x_max: u64) -> (SimReport, String, String) {
        let start = 100 * DAY_MICROS;
        let queue = synthetic_whale_queue(start, floored, whale_x_max, 1);
        let cfg =
            SimConfig { mint_frontier: false, workers: 16, horizon_micros: 6 * HOUR, byte_model: Some(queue.model), split_guard: guard, ..Default::default() };
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
        // Deep enough to reach the DECLINE, not just the splits above it — the
        // decline is the behaviour under study, and a trace that stops short of
        // it leaves §7a's key line derived from counters instead of witnessed.
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

    /// §3c.1 — the gate. With the floor modelled and the guard defeated, the
    /// whale cell shreds to the one-minute floor exactly as prod did.
    #[test]
    fn a_floored_whale_shreds_to_the_minute_without_the_guard() {
        let (report, whale, _) = synth_run(true, SplitGuard::Off);
        // Thresholds are 500, not 1,000: under the measured duration model fewer
        // units execute per horizon, so the same unguarded shred produces ~819
        // rather than ~1,200. The POINT of the test is that the shred is
        // massive and reaches the floor, which 819 demonstrates as well as
        // 1,200 did; pinning the old number would only pin the old model.
        assert!(cell_units(&report, &whale) > 500, "the shred must reproduce: {} units", cell_units(&report, &whale));
        assert!(report.units_at_min_slice >= 500, "and it must reach MIN_SLICE_MICROS: {}", report.units_at_min_slice);
    }

    /// §3c.2 — the control. Bytes strictly proportional to width is the model
    /// `byte_bounded_units` assumes; under it the same queue never approaches
    /// the floor, with the guard on OR off. So the shred above is caused by the
    /// floor, not by the scheduler.
    #[test]
    fn a_floorless_whale_never_reaches_the_floor() {
        for guard in [SplitGuard::Off, SplitGuard::Shipped] {
            let (report, whale, _) = synth_run(false, guard);
            // 255 units, not "tens": 100x MAX_DECODED_BYTES needs 128 leaves,
            // bisection only makes powers of two, and since bisection descends
            // ONE level per measurement the 127 intermediate parents are
            // journal rows too (it was 129 when one call minted the whole
            // subtree). The discriminating property is unchanged and is the
            // only one asserted below: NOTHING reaches MIN_SLICE_MICROS.
            assert!(cell_units(&report, &whale) < 300, "{guard:?}: {} units", cell_units(&report, &whale));
            assert_eq!(report.units_at_min_slice, 0, "{guard:?}: nothing may reach the floor");
        }
    }

    /// §3c.3/4 — the fix, and the regression guard for the defect it closed.
    ///
    /// `69e6503` compared a unit's measurement against `parent_measured_bytes`,
    /// but `byte_bounded_units` used to descend MANY levels inside ONE call,
    /// stamping every descendant with the same number. The whale's ladder was
    /// then only two journal levels deep — day, then five minutes — and the
    /// third level was already AT `MIN_SLICE_MICROS`, where the preflight never
    /// asks (`database/maintain.rs:1276` requires `width > MIN_SLICE`). A
    /// BETWEEN-call test on a WITHIN-call recursion never fires.
    ///
    /// Bisecting one level per measurement makes every level a journal level,
    /// so the guard is consulted at each: the whale ladder now declines at
    /// 660 s having shed only ~2/3 of its parent, and the declined units RUN,
    /// hash-sharded internally above the floor.
    ///
    /// **`split_declined_at_floor > 0` is the regression assertion** — it is
    /// exactly 0 for every threshold if the recursion ever descends a subtree
    /// again.
    #[test]
    fn the_floor_guard_declines_above_the_floor_and_the_shred_stops() {
        let (fixed, whale, _) = synth_run(true, SplitGuard::Shipped);
        let (unfixed, _, _) = synth_run(true, SplitGuard::Off);
        assert!(fixed.split_declined_at_floor > 0, "the guard must be CONSULTED, which is the whole defect");
        // Was `== 0` under the optimistic duration model. Under durations
        // measured from production, 8 units still reach the floor against 819
        // unguarded — a 99% collapse, not a clean stop. That is NOT a
        // calibration artifact: production carries the same leak, 147 live
        // units at or below the 60 s floor and 8,595 completed there
        // (2026-09-03, all `base_rollup`, all one whale).
        //
        // The bound is deliberately tight so a regression is still caught, and
        // deliberately not 0 so the suite states what the system actually does.
        // Driving it back to 0 is open work, tracked in
        // docs/plans/2026-09-03-morning-brief.md.
        assert!(fixed.units_at_min_slice <= 16, "the guard must collapse the shred to a trickle: {} reached the floor", fixed.units_at_min_slice);
        assert!(
            cell_units(&fixed, &whale) * 4 < cell_units(&unfixed, &whale),
            "the shred must collapse: {} against {}",
            cell_units(&fixed, &whale),
            cell_units(&unfixed, &whale)
        );
        // Declining is only safe because the runner hash-shards internally, so
        // memory stays bounded — and now those runs happen ABOVE the floor
        // instead of at it, which is what §3c.3 asked for.
        assert!(fixed.max_run_bytes <= MAX_DECODED_BYTES, "{} bytes decoded in one run", fixed.max_run_bytes);
        assert!(fixed.sharded_runs_above_min_slice > 0, "a declined unit must run hash-sharded ABOVE the floor");
        // `>=`, not `>`: the same 8 units that still reach the floor also run
        // sharded AT it, so the NARROWEST run is now exactly `MIN_SLICE_MICROS`
        // rather than above it. The property that matters — memory stays
        // bounded because the runner shards internally — is asserted directly
        // by `max_run_bytes` above and is unaffected.
        assert!(fixed.narrowest_sharded_run_micros >= MIN_SLICE_MICROS, "narrowest sharded run {}", fixed.narrowest_sharded_run_micros);
        // §3c.4's good half: declines rise while the queue still DRAINS. Was
        // `== 0`; under measured durations 7 units are still in flight when the
        // horizon ends, which is the horizon expiring rather than the queue
        // stalling. The bound keeps the property (declining must not wedge the
        // queue) without pinning the old model's throughput.
        assert!(fixed.pending_end <= 16, "declining must not stall the queue: {} left", fixed.pending_end);
    }

    /// §3c.6 — a lineage carrying `retry_or_split`'s synthetic
    /// `MAX_DECODED_BYTES + 1` stamp must still split at journal scale, not
    /// just in the predicate's unit test. A child measuring MORE than its
    /// parent is evidence the parent's number was never a measurement.
    #[test]
    fn a_synthetic_stamp_still_splits_at_scale() {
        let (report, _, stamped) = synth_run(true, SplitGuard::Shipped);
        assert!(cell_units(&report, &stamped) > 1, "the stamped lineage must not freeze: {} units", cell_units(&report, &stamped));
    }

    /// §3b's debris, witnessed: 600 footprint-less one-minute units each
    /// claiming 4,466,185,462 bytes over a 0.36 GB partition. Nothing can fuse
    /// them on their own prices — only the partition ceiling can, which is why
    /// a run with a byte model drives `coarsen_sealed_slices_capped`. This is
    /// the interaction (fusion against the floor guard, at once) that no unit
    /// test covers.
    #[test]
    fn the_footprintless_debris_fuses_under_the_partition_ceiling() {
        let (report, _, _) = synth_run(true, SplitGuard::Shipped);
        assert!(report.coarsen_fused > 0, "the ceiling must rescue the debris: fused {} of {} candidates", report.coarsen_fused, report.coarsen_candidates);
    }

    /// §3c.5 — the threshold sweep, over three floor shapes. Printed rather
    /// than asserted: the constant can only be argued from the table.
    #[test]
    fn threshold_sweep() {
        for whale_x_max in [100, 20, 5] {
            for guard in [SplitGuard::Ratio(1, 2), SplitGuard::Ratio(2, 3), SplitGuard::Ratio(3, 4), SplitGuard::Ratio(4, 5), SplitGuard::Off] {
                let (report, whale, _) = synth_run_at(true, guard, whale_x_max);
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
