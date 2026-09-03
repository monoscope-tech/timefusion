//! Durable, byte-bounded work units shared by background maintenance.
//!
//! This module deliberately contains no scan implementation.  It is the
//! correctness boundary between write/Delta reconciliation and the workers:
//! work is journaled before it can be selected, and a worker can only receive
//! a unit whose decoded-byte reservation fits the configured ceiling.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

pub const NORMAL_SLICE_MICROS: i64 = 10 * 60 * 1_000_000;
pub const DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
/// Widths `coarsen_sealed_slices` fuses sealed units to, widest first. It takes
/// the widest whose summed estimate fits `MAX_DECODED_BYTES`; below the finest,
/// the mint width stands. Each divides the one above, so an aligned unit at any
/// width sits inside exactly one bucket at every coarser width.
pub const COARSEN_WIDTHS: [i64; 3] = [DAY_MICROS, 6 * 60 * 60 * 1_000_000, 60 * 60 * 1_000_000];
/// Widths a unit can be SUBSUMED at, finest first — the mint width plus every
/// fusion width. Each divides the next, so an aligned unit at any of them sits
/// wholly inside one bucket of every coarser one.
/// What one `coarsen_sealed_slices` pass actually did, per stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CoarsenReport {
    /// Units dropped because a wider live unit already covers them.
    pub subsumed: usize,
    /// Units replaced by a wider fused unit.
    pub fused: usize,
    /// Sealed units eligible to fuse, before any group was rejected.
    pub candidates: usize,
    /// Candidates whose bucket a Running / equal-or-wider / superseded unit held.
    pub blocked: usize,
    /// Candidates in a group whose priced estimate exceeded MAX_DECODED_BYTES.
    pub over_budget: usize,
    /// Buckets that fit ONLY because members sharing a file set were charged
    /// once. Zero means [`InputFootprint`] pricing is changing nothing — either
    /// no unit carries a footprint yet, or fusion was never the constraint. It
    /// is the one number that says whether the fix is doing work.
    pub priced_by_footprint: usize,
}

impl CoarsenReport {
    pub const fn total(self) -> usize {
        self.subsumed + self.fused
    }
}

/// Is this operation fully re-derivable from a storage scan, and therefore not
/// worth persisting?
///
/// `plan_compaction_debt` scans the real file list of every (project, date)
/// every 60 s and mints HotPacking for today and SealedConsolidation once the
/// day seals, from the files themselves — `small.len() >= 2 || any !sorted`. The
/// scan is authoritative; a durable record of it is a second, weaker copy that
/// can only disagree.
///
/// And it did. Prod 2026-08-19 carried `pending_sealed_consolidation = 2,218`
/// while an audit of object storage found 877 of 1,033 partitions already
/// compliant and only 108 sealed ones out of policy — a queue 20x inflated with
/// work already done, draining at -0.27/min forever. Persisting derived state
/// buys nothing and costs staleness.
///
/// Repair is deliberately NOT here. Its units are day-wide rewrites that run for
/// 12-15 minutes and stage output before committing, so a durable record is what
/// `TIMEFUSION_REPAIR_RESUME_ENABLED` resumes against rather than redoing the
/// work.
pub const fn is_derived_operation(operation: Operation) -> bool {
    matches!(operation, Operation::HotPacking | Operation::SealedConsolidation)
}

pub const SUBSUME_WIDTHS: [i64; 4] = [NORMAL_SLICE_MICROS, 60 * 60 * 1_000_000, 6 * 60 * 60 * 1_000_000, DAY_MICROS];
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const DERIVED_SLICE_MICROS: i64 = 60 * 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;

/// Frontier lag the sealed reservation is still affordable at.
///
/// Above this, `claim_next` stops reserving a share for sealed work until the
/// live frontier catches up. Ten minutes is one `NORMAL_SLICE_MICROS`: a
/// frontier that is a whole slice behind is not keeping up, and every hybrid
/// query is paying for it through `raw_tail_duration_secs`
/// (`FINALIZATION_DELAY + lag`).
pub const FRONTIER_LAG_BUDGET_SECS: u64 = 600;

/// Whether a failure means "this did not fit" rather than "this went wrong".
///
/// Matched on the message, not the type: these errors originate in DataFusion,
/// cross the delta-rs and `anyhow` boundaries on the way back, and arrive
/// type-erased. The two strings are DataFusion's own — `ResourcesExhausted`'s
/// `Display` and the `ExternalSorter`'s message — and both are asserted against
/// verbatim prod text in `capacity_failures_are_recognised_from_prod_text`.
/// Operation deadlines also bound retry backoff so oversized units cannot
/// monopolize a worker.
/// Since `run_until_idle`, this is an IDLE window, not a budget: it fires only
/// after this long with no rows written. That makes a longer window nearly free
/// on healthy units and changes what the number has to cover — the longest
/// stretch a working unit can go without producing a row.
///
/// Repair gets an hour for exactly that reason. `ORDER BY` is blocking, so a
/// repair unit produces its first row only after the whole input is downloaded,
/// decoded and spilled; on the table's largest file (2.3 GB compressed, ~28 GB
/// decoded, uncached because it is past `cache_recent_days`) that silent
/// stretch can exceed 15 minutes on its own. The cost of the longer window is
/// that a genuinely hung repair holds a worker and one of ~2 `light_rewrite_sem`
/// permits for an hour; that is affordable for a `take(1)` lane with a finite
/// backlog, and it is strictly better than killing a unit that was working.
/// The longest per-unit idle window any operation gets. `COORDINATOR_LOOP_TIMEOUT`
/// is derived from this so the outer guard can never quietly become the real
/// deadline again.
pub const MAX_OPERATION_DEADLINE_SECS: u64 = operation_deadline_secs(Operation::Repair);

pub const fn operation_deadline_secs(operation: Operation) -> u64 {
    match operation {
        // Dedup's 300s was chosen when this was a BUDGET on total time. Under
        // idle semantics it is the wrong number, and prod 2026-09-01 measured
        // the cost: of 288 dedup units finishing in 25 minutes, 249 ran 0s
        // (claim-and-refuse churn) and **33 burned the full 300s and were
        // killed** — 9,900 worker-seconds, ~6.6 of 16 workers producing nothing.
        //
        // Those 33 made NO progress in 300s, which for dedup means the probe
        // (`GROUP BY` over a whole partition) had not yet emitted a row. Units
        // that do get a second window finish: the same log shows completions at
        // 599s, 600s and 887s. 900s covers them, and matches every other
        // non-repair operation — the exposure argument the old comment made is
        // now carried by `run_until_idle`, which only fires on ZERO progress.
        Operation::Dedup => 15 * 60,
        Operation::Repair => 60 * 60,
        Operation::HotPacking | Operation::SealedConsolidation | Operation::BaseRollup | Operation::DerivedRollup => 15 * 60,
    }
}

pub fn is_capacity_failure(message: &str) -> bool {
    // "resource_admission" belongs here by definition: the unit's ESTIMATE
    // exceeds what admission can ever grant, so it fails identically every
    // pass. Before it was included, a 1.1TB-estimate day-wide Repair looped
    // at its 1s admission-retry delay for DAYS (prod 2026-08-21, attempts
    // 140-211): never claimed a worker, never timed out, so neither
    // abandon_running's split nor its backoff floor ever fired.
    message.contains("Resources exhausted") || message.contains("Not enough memory to continue external sort") || message.contains("resource_admission")
}

/// Whether a failure is a DETERMINISTIC PLAN error — the SQL could not be built
/// at all, so every retry, and every CHILD of a bisection, fails identically.
///
/// The opposite verdict to [`is_capacity_failure`]: shrinking the slice cannot
/// make a missing column appear, it only multiplies the number of units failing
/// on it. Prod 2026-08-24: base files written before a rollup spec change lacked
/// `duration_digest`, and bisecting the units that named it turned one bad spec
/// into 477 of 658 claims failing in eight minutes — the 1h tier froze and every
/// other operation starved behind the storm. Same string-matching contract as
/// its sibling, pinned by `schema_failures_are_recognised_from_prod_text`.
pub fn is_schema_failure(message: &str) -> bool {
    message.contains("Schema error") || message.contains("SchemaError") || message.contains("No field named")
}
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const INVALIDATION_DEADLINE_BUCKET_MICROS: i64 = 30 * 1_000_000;
pub const LIVE_FRONTIER_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
const PRIORITY_BUCKET_MICROS: i64 = 60 * 1_000_000;
/// File-count band for hygiene benefit ranking — see `scheduling_class`.
const BENEFIT_BUCKET_FILES: u32 = 64;
pub const TAG_SOURCE: &str = "timefusion.source";
pub const TAG_PROJECT: &str = "timefusion.project";
pub const TAG_SLICE_START: &str = "timefusion.slice_start_micros";
pub const TAG_SLICE_END: &str = "timefusion.slice_end_micros";
pub const TAG_SOURCE_FINGERPRINT: &str = "timefusion.source_fingerprint";
/// How many rows the SOURCE DATE PARTITION held when this slice was built —
/// the `num_records` sum, exactly as `partition_stats_bounded` computes it.
///
/// Read coverage is refused unless every slice covering a date still agrees
/// with the partition's present count (`rollup::slice_coverage_agrees`). A row
/// count rather than a fingerprint on purpose: a fingerprint folds file
/// identity, so bin-packing would void it and recreate the certification-vs-
/// churn race inside the rollup tier. Row counts survive compaction.
pub const TAG_SOURCE_ROWS: &str = "timefusion.source_rows";
pub const TAG_GENERATION: &str = "timefusion.generation";
/// Which declared measures this slice's files actually MATERIALIZED, comma
/// separated — not what the spec declares.
///
/// The generation does not imply it. `duration_digest` was declared 2026-08-22
/// and first written 2026-08-24 10:30 UTC, so slices carrying the current
/// generation hold no digest at all, and Delta null-fills the column on scan.
/// Every merge then SKIPS those nulls — `tdigest_merge` and SQL `SUM` alike —
/// so the answer is a plausible number computed from the covered fraction
/// rather than a visible NULL: prod measured a p95 over 08-22..08-25 built from
/// 3,438 of 14,830 rows. The read path refuses a cell that cannot prove the
/// measure a query needs; see `RoutedRollup::measures_available`.
pub const TAG_MEASURES: &str = "timefusion.measures";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_COMPACT_BYTES: u64 = 64 * 1024 * 1024;
static THROUGHPUT_SAMPLE: std::sync::OnceLock<Mutex<(i64, u64)>> = std::sync::OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Dedup,
    BaseRollup,
    DerivedRollup,
    HotPacking,
    SealedConsolidation,
    Repair,
}

impl Operation {
    const fn priority(self) -> u8 {
        match self {
            Self::Dedup => 0,
            Self::BaseRollup => 1,
            Self::DerivedRollup => 2,
            Self::HotPacking => 3,
            Self::SealedConsolidation => 4,
            Self::Repair => 5,
        }
    }
}

/// The operation mix a maintenance worker rotates through. One definition for
/// the server loop (`run_coordinator_maintenance_once`) and the journal-replay
/// simulator (`maintenance_sim`) — the sim exists to evaluate changes to this
/// mix, so the two must never be able to drift apart. The SIGNAL that selects
/// the cycle is shared too: both sides decide through
/// `database::coverage_is_short_for` over `database::median_contiguous_days`.
///
/// BALANCED interleaves dependent publication with dedup: dedup/base receive
/// three slots each; derived and file work each receive one. `claim_next`
/// still applies deadline, recent-slice, dependency, and project fairness.
///
/// COVERAGE_SHORT gives the rollup chain the slots while
/// `rollup_median_contiguous_days` is below goal (the MEDIAN, not the goal
/// gauge — one negligible tenant must not pin the fleet):
/// `dependencies_complete` makes BaseRollup depend on NOTHING, so of the
/// balanced cycle six slots in ten go
/// to work that cannot advance the metric governing 14d/30d latency (measured
/// 2026-08-18). Every operation keeps at least one slot — file debt left at
/// zero is how file counts ran to 2-3k and degraded every query (2026-08-01).
pub const CYCLE_BALANCED: [Operation; 10] = [
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::Repair,
];
pub const CYCLE_COVERAGE_SHORT: [Operation; 10] = [
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::BaseRollup,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Repair,
];

pub fn operation_cycle(coverage_short: bool) -> &'static [Operation; 10] {
    if coverage_short { &CYCLE_COVERAGE_SHORT } else { &CYCLE_BALANCED }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TimeSlice {
    pub start_micros: i64,
    pub end_micros: i64,
}

impl TimeSlice {
    pub fn new(start_micros: i64, end_micros: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(start_micros < end_micros, "maintenance slice must be non-empty");
        Ok(Self { start_micros, end_micros })
    }

    pub const fn width(self) -> i64 {
        self.end_micros - self.start_micros
    }

    /// Half-open intersection. The rollup pipeline asks this in two places —
    /// which base TASKS cover a derived slice, and which base FILES a derived
    /// unit must read — and the two must agree, so they share this.
    pub const fn overlaps(self, start_micros: i64, end_micros: i64) -> bool {
        end_micros > self.start_micros && start_micros < self.end_micros
    }

    pub fn normal_units(start_micros: i64, end_micros: i64) -> anyhow::Result<Vec<Self>> {
        Self::fixed_units(start_micros, end_micros, NORMAL_SLICE_MICROS)
    }

    fn fixed_units(start_micros: i64, end_micros: i64, width_micros: i64) -> anyhow::Result<Vec<Self>> {
        let whole = Self::new(start_micros, end_micros)?;
        anyhow::ensure!(width_micros > 0, "maintenance slice width must be positive");
        let mut result = Vec::new();
        let mut start = whole.start_micros;
        while start < whole.end_micros {
            let end = start.saturating_add(width_micros).min(whole.end_micros);
            result.push(Self { start_micros: start, end_micros: end });
            start = end;
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TaskKey {
    pub physical_table: String,
    pub source: String,
    pub project_id: String,
    pub slice: TimeSlice,
    pub operation: Operation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Retry,
    Complete,
    Superseded,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MaintenanceTask {
    pub key: TaskKey,
    pub state: TaskState,
    pub deadline_micros: i64,
    pub estimated_decoded_bytes: u64,
    pub hash_shard: u32,
    pub hash_shards: u32,
    pub attempts: u32,
    pub created_unix_ms: u64,
    #[serde(default)]
    pub retry_reason: Option<String>,
    #[serde(default)]
    pub publication: Option<Publication>,
    /// The base tier this derived unit aggregates is ALREADY PRESENT, proven
    /// from real rollup coverage by `plan_rollup_backfill` rather than from
    /// journal bookkeeping.
    ///
    /// `dependencies_complete` otherwise requires COMPLETE `BaseRollup` TASKS
    /// contiguously covering the slice. For a frontier hour that is right. For a
    /// historical day whose 1m tier was built weeks ago — possibly by an older
    /// code path, possibly with its journal records long since collapsed — no
    /// such task exists, so the unit is unclaimable forever and `claim_next`
    /// skips it with no counter and no log.
    ///
    /// Prod 2026-08-18 22:30 UTC is that shape exactly: the 1m base tier is 33
    /// days deep on most projects while the 1h derived tier it feeds sits at
    /// 9-17, `pending_derived_rollup` did not move by ONE task across two 240s
    /// windows with workers free, and all 35 derived units claimed in 20 minutes
    /// were frontier slices whose base had completed minutes earlier.
    ///
    /// Only ever set from positive evidence — the planner computes `missing`
    /// tiers from actual coverage, so a derived tier missing while no base tier
    /// is missing means the base data is there. That is strictly better evidence
    /// than the journal's, which is why this overrides rather than supplements.
    #[serde(default)]
    pub base_tier_present: bool,
    /// What this unit's slice actually READS, measured when its estimate was
    /// taken. `default` so older journals deserialize as `None` and price
    /// exactly as they did before.
    #[serde(default)]
    pub input: Option<InputFootprint>,
    /// What the parent MEASURED when it split into this unit, so that the next
    /// preflight can tell whether halving the width actually bought anything.
    ///
    /// `byte_bounded_units` prices children by TIME SHARE, which is a model, not
    /// a measurement — so a split always "fits" on paper however expensive the
    /// slice really is. The true cost has a floor (a slice reads at least one
    /// row group of every file it overlaps; prod 2026-08-22 measured 302 MB for
    /// a FIVE-MINUTE slice), and it surfaces only when the preflight re-measures
    /// at claim time, finds itself over budget and splits again — all the way to
    /// `MIN_SLICE_MICROS`. One (project, tier, day) held 3,455 units that way.
    ///
    /// Comparing against this turns the model into a feedback loop: measure, and
    /// if the child did not get meaningfully cheaper than its parent, stop. The
    /// loop only closes because a split descends ONE level per call — when it
    /// descended a whole subtree, every descendant carried the same stamp and
    /// the ladder reached the floor in two measured levels, so this field was
    /// never once consulted at a width where the floor dominates (sim,
    /// 2026-08-25).
    /// `default` so older journals deserialize as `None` and bisect exactly as
    /// they did before.
    #[serde(default)]
    pub parent_measured_bytes: Option<u64>,
    /// Scheduling weight inherited from the backfill unit this task was split
    /// out of. `None` means "weigh me by my own width".
    ///
    /// Sealed ordering ranks wide units first because width PROXIES BACKFILL
    /// PROVENANCE: a day-sized unit comes from the backfill planner and is the
    /// only kind that advances the horizon, while a ten-minute one is what the
    /// live path mints by the hundred. `split_time_task` breaks that proxy — a
    /// day unit's children are still the backfill work that moves coverage, but
    /// they now measure 180s and rank below every day-wide unit anywhere in
    /// history.
    ///
    /// Prod 2026-08-19 is that shape exactly: `87576849`'s 2026-08-10 day unit
    /// was split into 928 fragments in one burst on 08-17 11:23-11:31, and in
    /// the 40 minutes measured afterwards sealed BaseRollup claims went to
    /// 2026-07-22 — a month older — while 08-10 got none. That single day is the
    /// hole capping `rollup_min_contiguous_days` at 2.
    ///
    /// Ageing does not cover this: both units are starved, and within the
    /// starved set width still decides.
    /// `default` so older journals deserialize as `None` and rank exactly as
    /// they did before.
    #[serde(default)]
    pub backfill_priority_micros: Option<i64>,
}

impl MaintenanceTask {
    /// Width for SCHEDULING only, never for planning or execution.
    pub fn scheduling_width(&self) -> i64 {
        self.backfill_priority_micros.unwrap_or_else(|| self.key.slice.width())
    }
}

/// The file set behind a unit's byte estimate.
///
/// `estimated_decoded_bytes` prorates a file by the share of its time span the
/// slice covers, which is right for ONE unit and wrong the moment
/// `coarsen_to_width` sums siblings: a parquet file is pruned at row-group
/// granularity, so `slice_share_of_file` floors every slice at one row group —
/// and on ~10 MB files that is the WHOLE file. 1,440 one-minute children of the
/// same day therefore each estimated ~282 MB honestly, summed to 391 GB against
/// a 512 MB budget, and fusion refused them at every width (prod 2026-08-23:
/// 92.9% of 5.1M candidates `over_budget`). The queue could not shrink by the
/// one mechanism built to shrink it.
///
/// Pricing a group by DISTINCT `fp` fixes exactly that and nothing else:
/// children that read the same files are one scan, children that read different
/// files still sum. Partial overlap counts twice, which refuses a fusion that
/// would have fit — the safe direction, and the behaviour that already existed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct InputFootprint {
    /// Hash of the live file paths the slice overlaps.
    pub fp: u64,
    /// Decoded, projected bytes of that whole set — one scan, unprorated.
    pub whole_file_bytes: u64,
    /// How many files. For file hygiene this IS the benefit: a consolidation
    /// removes them and leaves one. `default` so journals written between
    /// 5582105 and this change still deserialize, reporting zero benefit —
    /// which orders them last rather than wrongly.
    #[serde(default)]
    pub files: u32,
}

impl InputFootprint {
    /// Fingerprint a selected file set. Order-independent (a snapshot's file
    /// order is not stable), so two units over the same files agree.
    ///
    /// FROZEN HASH: persisted in the task journal, so a change makes every
    /// in-flight unit's footprint stop matching its own journal entry. Not part
    /// of the XXH3 sweep.
    pub fn new<I: IntoIterator<Item = S>, S: AsRef<str>>(paths: I, whole_file_bytes: u64) -> Self {
        use std::hash::{Hash, Hasher};
        let (fp, files) = paths.into_iter().fold((0u64, 0u32), |(acc, count), path| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            path.as_ref().hash(&mut hasher);
            (acc ^ hasher.finish(), count.saturating_add(1))
        });
        Self { fp, whole_file_bytes, files }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Publication {
    pub source_fingerprint: u64,
    pub generation: String,
    pub rows: u64,
    /// The source DATE partition's `num_records` sum when this slice was built,
    /// mirroring [`TAG_SOURCE_ROWS`]. `default` so journals written before this
    /// field deserialize: they yield `None`, the read path cannot verify them,
    /// and those slices read raw until the coordinator republishes.
    #[serde(default)]
    pub source_rows: Option<u64>,
}

/// What one fusion bucket would cost to scan, accumulated member by member.
///
/// Members naming the same [`InputFootprint`] are charged ONCE — they re-read
/// the same row groups, so one wider unit does their work in one scan. Members
/// with no footprint (older journals, planner-minted units) keep the old summed
/// price.
///
/// This prices the fused unit at what it will actually READ, which is not
/// always lower than the old sum: two members prorated at 100 each over a file
/// set worth 400 now price at 400, because that is what one scan of the set
/// costs and the sum under-stated it.
#[derive(Default)]
struct GroupPrice {
    /// Distinct footprints, each charged its unprorated whole-file cost.
    distinct: HashMap<u64, InputFootprint>,
    /// Members with nothing better to say than their prorated share.
    unpriced_bytes: u64,
    unpriced_members: usize,
    /// Every member's own estimate, summed — what the old rule charged, kept
    /// only so a pass can report whether footprint pricing changed anything.
    summed_bytes: u64,
}

impl GroupPrice {
    fn add(&mut self, task: &MaintenanceTask) {
        self.summed_bytes = self.summed_bytes.saturating_add(task.estimated_decoded_bytes);
        match task.input {
            Some(input) => {
                self.distinct.insert(input.fp, input);
            }
            None => {
                self.unpriced_bytes = self.unpriced_bytes.saturating_add(task.estimated_decoded_bytes);
                self.unpriced_members += 1;
            }
        }
    }

    fn bytes(&self) -> u64 {
        self.distinct.values().fold(self.unpriced_bytes, |total, input| total.saturating_add(input.whole_file_bytes))
    }

    /// Bound the price by what the partition can actually decode to.
    ///
    /// Applied to `unpriced_bytes`, because that is the term that double-counts:
    /// footprint-priced members are already charged once each, while unpriced
    /// members are summed and pre-date the footprint entirely. Prod's stuck
    /// backlog is all unpriced.
    fn cap_at(&mut self, ceiling: u64) {
        let priced: u64 = self.distinct.values().fold(0, |total, input| total.saturating_add(input.whole_file_bytes));
        self.unpriced_bytes = self.unpriced_bytes.min(ceiling.saturating_sub(priced.min(ceiling)));
    }

    fn unanimous_input(&self) -> Option<InputFootprint> {
        match (self.unpriced_members, self.distinct.iter().next()) {
            (0, Some((_, &input))) if self.distinct.len() == 1 => Some(input),
            _ => None,
        }
    }
}

/// A child must shed at least this much of what its parent measured for the
/// next bisection to be worth minting units for. Bisection halves the WIDTH, so
/// the model expects ~50%; anything above this is the row-group floor, not a
/// slice that is genuinely too wide.
const SPLIT_MUST_SHED_NUMERATOR: u64 = 3;
const SPLIT_MUST_SHED_DENOMINATOR: u64 = 4;

/// Whether halving the width bought enough to justify halving it again.
///
/// `None` — no parent evidence — always splits: that is the first measurement
/// of this lineage and there is nothing yet to compare against.
///
/// The window is deliberately two-sided. A child that measured MORE than its
/// parent is not evidence of the floor, it is evidence the parent's number was
/// never a measurement — which is exactly what `retry_or_split` stamps when it
/// forces a bisection with a synthetic `MAX_DECODED_BYTES + 1`. Declining there
/// would freeze that lineage forever, the immortal-unit shape this file already
/// has three incidents of. Let it split once more; the next preflight stamps a
/// real number and the guard starts working.
fn split_sheds_enough(parent_measured_bytes: Option<u64>, observed_bytes: u64) -> bool {
    split_sheds_enough_at(parent_measured_bytes, observed_bytes, SPLIT_MUST_SHED_NUMERATOR, SPLIT_MUST_SHED_DENOMINATOR)
}

/// The shed test at an arbitrary ratio, so the simulator's threshold sweep runs
/// THIS function rather than a copy of it.
///
/// `maintenance_sim` used to transcribe the predicate inline to vary the ratio,
/// which is a drift hazard by construction: the copy silently kept its own
/// meaning while this one changed. It is also why the sim never reproduced the
/// 2026-09-03 synthetic-observation defect — the copy was fed a modelled
/// observation and never saw what `retry_or_split` actually passes.
pub fn split_sheds_enough_at(parent_measured_bytes: Option<u64>, observed_bytes: u64, numerator: u64, denominator: u64) -> bool {
    let Some(parent) = parent_measured_bytes else { return true };
    observed_bytes > parent || observed_bytes.saturating_mul(denominator) < parent.saturating_mul(numerator)
}

/// Split a unit that does not fit, **one level per call**.  A one-minute whale
/// is divided by a stable hash of the complete dedup key; callers must apply
/// `hash(key) % hash_shards == hash_shard` before deduplication.
///
/// One level, not a subtree, because every level below the first is priced by
/// TIME SHARE and time share is a model. Descending many levels inside one call
/// stamped every descendant with the same measurement, so a lineage reached
/// `MIN_SLICE_MICROS` in two journal levels and `split_sheds_enough` — a
/// between-call test — was never asked about any level in between. Halving once
/// and re-measuring makes the guard see every level, which is the only way it
/// can observe the floor at the width where the floor starts to dominate.
pub fn byte_bounded_units(task: &MaintenanceTask, observed_or_estimated_bytes: u64) -> Vec<MaintenanceTask> {
    if observed_or_estimated_bytes <= MAX_DECODED_BYTES {
        let mut task = task.clone();
        task.estimated_decoded_bytes = observed_or_estimated_bytes;
        return vec![task];
    }
    // Time-bisection stops at the width where a slice stops shedding FILES, not
    // at the narrowest slice the journal can express.
    //
    // A dedup unit's cost is its PARTITION: `dedup_probe_ctx` builds its
    // provider over every file of the (project, date) and then filters by
    // slice, and on the live frontier those files overlap in time so nothing
    // prunes. Prod 2026-09-01 measured the consequence — 4,000 of 5,028 active
    // dedup units were sub-15-minute slivers with a p50 of 10 input files and a
    // p90 of **76**, and units over a SINGLE file were burning the whole 300s
    // deadline. Every one of those was manufactured by bisection: 3,602 units
    // superseded as `split_into_smaller_slices`, each child costing what its
    // parent cost.
    //
    // Below this floor the answer is to shard by KEY, which does shed work,
    // rather than to halve time ten more times and pay the same scan each way.
    // Repair declines to bisect at all for the same reason (its cost is a
    // file); this is the same argument one level weaker.
    let bisect_floor = if task.key.operation == Operation::Dedup { NORMAL_SLICE_MICROS } else { MIN_SLICE_MICROS };
    if task.key.slice.width() > bisect_floor {
        let midpoint = task.key.slice.start_micros.saturating_add(task.key.slice.width() / 2);
        let midpoint = (midpoint / MIN_SLICE_MICROS) * MIN_SLICE_MICROS;
        if midpoint > task.key.slice.start_micros && midpoint < task.key.slice.end_micros {
            let left_bytes = ((u128::from(observed_or_estimated_bytes) * u128::try_from(midpoint - task.key.slice.start_micros).unwrap_or(0))
                / u128::try_from(task.key.slice.width()).unwrap_or(1)) as u64;
            let mut left = task.clone();
            left.key.slice.end_micros = midpoint;
            left.estimated_decoded_bytes = left_bytes;
            let mut right = task.clone();
            right.key.slice.start_micros = midpoint;
            right.estimated_decoded_bytes = observed_or_estimated_bytes.saturating_sub(left_bytes);
            return vec![left, right];
        }
    }

    let shards_u64 = observed_or_estimated_bytes.div_ceil(MAX_DECODED_BYTES).max(2);
    let shards = u32::try_from(shards_u64).unwrap_or(u32::MAX);
    let per_shard = observed_or_estimated_bytes.div_ceil(u64::from(shards));
    (0..shards)
        .map(|hash_shard| {
            let mut shard = task.clone();
            shard.hash_shard = hash_shard;
            shard.hash_shards = shards;
            shard.estimated_decoded_bytes = per_shard;
            shard
        })
        .collect()
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Snapshot {
    version: u32,
    tasks: Vec<MaintenanceTask>,
    source_cursors: BTreeMap<String, u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum JournalRecord {
    Task(MaintenanceTask),
    SourceCursor {
        source: String,
        delta_version: u64,
    },
    /// This task no longer exists.
    ///
    /// Without it the WAL is upsert-only, and a pass that removes tasks can
    /// persist its work ONLY by rewriting the whole snapshot. Prod 2026-08-19
    /// shows what that costs when a caller forgets: coarsening took
    /// `pending_base_rollup` 88,618 -> 2,294, every gauge agreed, and the
    /// on-disk journal was still byte-identical at 84,734,124 bytes with all
    /// 173,901 tasks. The collapse existed only in memory and the next restart
    /// undid it.
    ///
    /// `compact` remains correct and is still what a large migration should
    /// use; this exists so that the CHEAP path can express a deletion at all,
    /// and so forgetting is no longer possible — `retain_tasks` records the
    /// tombstones for every caller.
    Removed(TaskKey),
}

/// Crash-safe task journal. `checkpoint` uses the same fsync + atomic rename
/// primitive as WAL metadata; a failed completion checkpoint therefore causes
/// redundant work, never missing work.
#[derive(Debug)]
pub struct TaskJournal {
    path: PathBuf,
    wal_path: PathBuf,
    snapshot: Snapshot,
    /// Stable indices into `snapshot.tasks`. Tasks are never removed, so point
    /// updates and WAL replay stay O(1) even with a production-sized backlog.
    task_indices: HashMap<TaskKey, usize>,
    dirty_tasks: HashSet<TaskKey>,
    /// Keys removed since the last write, pending a `Removed` tombstone.
    removed_tasks: HashSet<TaskKey>,
    dirty_cursors: HashSet<String>,
    fair_cursors: HashMap<Operation, String>,
    /// `(source, project_id, date)` whose BASE tier is already built, as read
    /// from real rollup coverage by `plan_rollup_backfill` every 60s.
    ///
    /// `dependencies_complete` consults this instead of requiring COMPLETE
    /// `BaseRollup` TASKS, which a historical day does not have. Three attempts
    /// to carry the same fact as a per-task flag failed (#184, #186, #195),
    /// because the flag had to land on exactly the right `TaskKey` and the
    /// queued work is not the width the planner assumes: prod 2026-08-19 06:30
    /// measured `derived_unproven=674` out of `derived_pending=674` — the flag
    /// had never been set on ONE pending task.
    ///
    /// A day is the right key because that is what the fact is about, so it
    /// cannot miss a task whatever slice that task covers. Runtime only, never
    /// journalled: the planner rebuilds it from coverage each pass, so a restart
    /// costs one pass and it self-heals if coverage changes underneath it.
    base_tier_ready: HashSet<(String, String, String)>,
    /// `(source, project_id, physical_table, date)` the tier is MISSING, from
    /// the same coverage read that fills `base_tier_ready`.
    ///
    /// `scheduling_class` ranks a hole ahead of a re-derive. Without that,
    /// sealed rollup work is strictly newest-first, and recent days are
    /// continuously re-invalidated by ongoing publication — so the claim never
    /// walks backwards far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f` had 1h-tier dates jumping 2026-07-31 -> 08-14 for a second
    /// day running while day-wide derived units for 08-17 were claimed over and
    /// over. Newest-first is right for freshness and wrong for CONTIGUITY, and
    /// 30d coverage is a contiguity goal.
    ///
    /// Runtime only, rebuilt from coverage every 60s, same as `base_tier_ready`.
    tier_holes: HashSet<(String, String, String, String)>,
    /// `(source, project, tier table, date)` for partitions still holding tier
    /// files with NO identity tags — ranked exactly like `tier_holes`, because
    /// they are one. Such a file cannot be certified and cannot be retired until
    /// something republishes the partition, so the partition is missing coverage
    /// however much tagged output sits beside it.
    ///
    /// Kept separate from `tier_holes` rather than merged into it because the
    /// two are published by different passes at different cadences, and a
    /// wholesale replace by either would erase the other's evidence.
    untagged_cells: HashSet<(String, String, String, String)>,
    /// Rotates so a fixed share of claims is reserved for sealed work. Runtime
    /// only — never journalled; losing it across a restart costs nothing.
    claim_tick: u64,
    /// Last observed `eligible_watermark_lag_seconds`, republished by
    /// `publish_statistics`. Read by `claim_next` to decide whether the sealed
    /// reservation can still be afforded. Atomic because `publish_statistics`
    /// takes `&self`; runtime only, never journalled.
    frontier_lag_secs: std::sync::atomic::AtomicU64,
}

/// Ensures a claimed task cannot remain stuck in `Running` when its worker
/// returns early through an unexpected error. Expected retry, split, and
/// completion paths change the state before this guard is dropped.
pub struct TaskLease {
    journal: Arc<Mutex<TaskJournal>>,
    key: TaskKey,
    started_micros: i64,
    /// Why the unit is about to fail, when the failing path knows. Errors leave
    /// a run function through `?`, which reaches [`Drop`] carrying nothing at
    /// all, so without this `abandon_running` must treat a deterministic plan
    /// error exactly like a timeout — and bisect it. Mutex, not `RefCell`, so
    /// the lease stays `Send` across the run functions' awaits.
    failure: Mutex<Option<String>>,
}

impl TaskLease {
    pub fn new(journal: Arc<Mutex<TaskJournal>>, key: TaskKey) -> Self {
        Self { journal, key, started_micros: crate::support::now_micros(), failure: Mutex::new(None) }
    }

    /// Annotate the error on its way out: `lease.note_failure(error)?`.
    pub fn note_failure<E: std::fmt::Display>(&self, error: E) -> E {
        *self.failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error.to_string());
        error
    }
}

impl Drop for TaskLease {
    fn drop(&mut self) {
        let mut journal = self.journal.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        // The ONLY place a unit's end is observable on every path. Prod
        // 2026-08-24, one container's whole 62-minute life: 244
        // `maintenance_task_started` and ZERO completion lines of any kind —
        // because none existed. `TaskJournal::complete` and `publish` set the
        // state and log nothing, so a log-only reading could not tell
        // "succeeded" from "did nothing" from "died", and the first reading of
        // it looked like a total stall when it was an absent instrument. That
        // is the same shape as `retry()` logging nothing, which stalled two
        // earlier diagnoses.
        //
        // Emitted from the lease rather than from `complete()` for two reasons:
        // the lease is RAII so no exit path can skip it (the run functions
        // return early in a dozen places), and `complete()` knows neither how
        // long the unit ran nor that it was the one running it.
        //
        // The state read here IS the outcome, because this runs after the run
        // function has recorded it: `Complete` succeeded, `Retry` is going
        // round again, `Running` means it died without recording anything and
        // is about to be abandoned below.
        let outcome = journal.state(&self.key);
        let ran_micros = crate::support::now_micros().saturating_sub(self.started_micros);
        tracing::info!(
            operation = ?self.key.operation, table = %self.key.physical_table, project_id = %self.key.project_id,
            slice_start = self.key.slice.start_micros, slice_end = self.key.slice.end_micros,
            outcome = ?outcome, ran_secs = ran_micros / 1_000_000,
            // What the unit knows it READ. Not the same as what it changed —
            // a consolidation that merged 200 files and one that merged none
            // are still indistinguishable here, and closing that needs the
            // publish site, not this one. Said plainly so the next reader does
            // not mistake "completed" for "did something".
            input_files = journal.input_files(&self.key),
            event = "maintenance_task_finished"
        );
        if outcome == Some(TaskState::Running) {
            let failure = self.failure.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            journal.abandon_running(&self.key, crate::support::now_micros(), failure.as_deref());
            if let Err(error) = journal.checkpoint() {
                tracing::error!(error = %error, task = ?self.key, "failed to checkpoint maintenance task lease recovery");
            }
        }
    }
}

pub struct Invalidation<'a> {
    pub source_table: &'a str,
    pub rollup_table: &'a str,
    pub source: &'a str,
    pub project_id: &'a str,
    pub start_micros: i64,
    pub end_micros: i64,
    pub observed_at_micros: i64,
    pub derived: bool,
}

impl TaskJournal {
    // v1 removed the original bootstrap expansion, but the then-current
    // reconciliation immediately recreated it before advancing its cursor.
    // v2 removed it in memory but did not force an on-disk snapshot rewrite.
    // v3 runs once with commit-range reconciliation and forced compaction.
    const BOOTSTRAP_BACKLOG_MIGRATION: &'static str = "__maintenance_bootstrap_backlog_v3";
    const BOOTSTRAP_BACKLOG_LIMIT: usize = 100_000;
    /// v2, because the shredded units it exists to remove came BACK by a route
    /// v1 could not see. `split_time_task` bisects a unit that times out, and
    /// each bisect halves again, so a day that kept failing reached the
    /// one-minute floor — prod 2026-08-23 held 1,440 sixty-second units for
    /// `base_rollup / 00000000 / 2026-08-13` alone, and 21,598 pending resolving
    /// to 1,452 real cells (14.9x). Those children predate [`InputFootprint`],
    /// so fusion still prices them by summing, still refuses every width, and
    /// they are stuck exactly as before. Re-running clears them; the planner
    /// re-derives what coverage actually lacks, and the split that follows now
    /// stamps its children so the next fusion collapses them.
    const COARSE_BACKFILL_MIGRATION: &'static str = "__maintenance_coarse_backfill_v2";
    /// v2, because v1 ran and threw its own work away. It cleared 85,047
    /// estimates in memory and persisted only its CURSOR — `checkpoint` writes
    /// dirty cursors but cannot express the task rewrites, and the `compact`
    /// that would have is only there as of f945bf1. So the journal kept every
    /// stale number while the marker said the migration was done, and prod came
    /// back with `over_budget=52,178` and no way left to clear it. A migration
    /// that records completion more durably than its effect is worse than one
    /// that never ran.
    const STALE_ESTIMATE_MIGRATION: &'static str = "__maintenance_stale_estimate_v2";
    /// Bump to re-run the orphaned-coverage repair after a FUTURE spec edit.
    ///
    /// v2: v1 consumed `otel_logs_and_spans`'s cursor without its enqueue taking
    /// effect — prod logged the repair for `otel_metrics` (forced=197) and never
    /// for `otel_logs_and_spans`, which is the source holding the orphaned cells.
    /// The claim persists its cursor BEFORE the caller does the work precisely so
    /// a crash cannot loop; the cost of that choice is that a pass which claims
    /// and then does not enqueue burns the one-shot. Bumping is the intended
    /// recovery, and re-enqueueing is idempotent at the journal.
    const ORPHAN_REPAIR_MIGRATION: &'static str = "__maintenance_orphan_repair_v2";

    /// Re-enqueue of the cells the 2026-08-25 derived-witness bug left SHORT on
    /// disk. Distinct from `ORPHAN_REPAIR_MIGRATION` — that one bounded by a DATE
    /// WINDOW, which re-enqueued every tier of every in-window day. The damage
    /// here is a measured list of 81 (project, date) pairs out of 320 comparable,
    /// so a window would drag 236 clean pairs through a queue that was measured
    /// GROWING at ~+80 units/hr with an 11-day-old starved tail.
    ///
    /// v2, and the cursor now counts CONSUMED PREFIX rather than done/not-done.
    /// v1 was a one-shot that burned its cursor before the caller enqueued, so
    /// the whole list was forced into exactly one planner pass — where
    /// `BACKFILL_PARTITIONS_PER_PASS` (24) truncated it newest-first and dropped
    /// the rest PERMANENTLY, because a cell that HAS tier output can never
    /// re-enter `missing_tiers`. Measured on prod 2026-08-28: eight of eight
    /// sampled pairs dated before 08-22 were still byte-identical at their
    /// damaged values (28f62f01/08-03 at 1,071 rows against a truth of
    /// 3,752,582), while the newest dates had repaired.
    pub const DAMAGE_REPAIR_MIGRATION: &'static str = "__maintenance_damage_repair_v2";
    /// See [`TaskJournal::reset_repair_attempts`].
    const REPAIR_SINGLE_PASS_MIGRATION: &'static str = "__maintenance_repair_single_pass_v1";

    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        let path = crate::write::wal::meta_path(data_dir, "maintenance_tasks.json");
        let wal_path = crate::write::wal::meta_path(data_dir, "maintenance_tasks.wal");
        let mut snapshot = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes)?,
            Err(error) if error.kind() == ErrorKind::NotFound => Snapshot { version: JOURNAL_VERSION, ..Snapshot::default() },
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(snapshot.version == JOURNAL_VERSION, "unsupported maintenance task journal version {}", snapshot.version);
        // Every record ends in a newline. Ignore only a torn final record; all
        // earlier records were fsynced before the caller acknowledged the
        // invalidation or publication that produced them.
        let mut task_indices = snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect::<HashMap<_, _>>();
        if let Ok(bytes) = fs::read(&wal_path) {
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if !line.ends_with(b"\n") {
                    break;
                }
                let record = serde_json::from_slice::<JournalRecord>(&line[..line.len() - 1])?;
                match record {
                    JournalRecord::Task(task) => {
                        if let Some(index) = task_indices.get(&task.key).copied() {
                            snapshot.tasks[index] = task;
                        } else {
                            let index = snapshot.tasks.len();
                            task_indices.insert(task.key.clone(), index);
                            snapshot.tasks.push(task);
                        }
                    }
                    JournalRecord::SourceCursor { source, delta_version } => {
                        snapshot.source_cursors.entry(source).and_modify(|cursor| *cursor = (*cursor).max(delta_version)).or_insert(delta_version);
                    }
                    // `swap_remove` keeps this O(1); the entry swapped into the
                    // hole needs its index corrected. Snapshot ORDER carries no
                    // meaning — every consumer sorts or filters — and `compact`
                    // rewrites it anyway. A later `Task` record for the same key
                    // simply re-inserts, so remove-then-readd replays correctly.
                    JournalRecord::Removed(key) => {
                        if let Some(index) = task_indices.remove(&key) {
                            snapshot.tasks.swap_remove(index);
                            if let Some(moved) = snapshot.tasks.get(index) {
                                task_indices.insert(moved.key.clone(), index);
                            }
                        }
                    }
                }
            }
        }
        Ok(Self {
            path,
            wal_path,
            snapshot,
            task_indices,
            dirty_tasks: HashSet::new(),
            removed_tasks: HashSet::new(),
            dirty_cursors: HashSet::new(),
            fair_cursors: HashMap::new(),
            base_tier_ready: HashSet::new(),
            tier_holes: HashSet::new(),
            untagged_cells: HashSet::new(),
            claim_tick: 0,
            frontier_lag_secs: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Early coordinator builds journaled the one-hour tier in ten-minute
    /// units. Supersede those unpublished fragments and replace them with one
    /// aligned hour task; completed tagged publications remain available for
    /// metadata recovery and are not rewritten by this migration.
    pub fn migrate_derived_slices(&mut self) -> usize {
        let mut replacements: HashMap<TaskKey, (i64, u64, u64)> = HashMap::new();
        let mut migrated = 0usize;
        for task in &mut self.snapshot.tasks {
            // A SPLIT CHILD is not a legacy fragment. This migration exists for
            // the old 10-minute derived units; collapsing a child back to its
            // hour erases the bisection ladder and re-enqueues the parent key,
            // which `enqueue_inner` then resurrects to Pending — the loop that
            // turned one schema break into 4,632 superseded derived records on
            // 2026-08-22/23 (not one survived at width 1.0h; the migrated
            // population peaked at the 1-minute floor). `parent_measured_bytes`
            // is set only by `split_time_task`, so it is the exact discriminator.
            // Recombining children stays `coarsen_to_width`'s job — it prices the
            // fusion, which this pass cannot.
            // NARROWER than an hour, not merely "not an hour". `!=` also matched
            // WIDER slices, and the replacement key below is the single hour
            // containing the slice START — so a day-wide derived unit was
            // superseded and re-enqueued as hour 00 with its other 23 hours
            // silently dropped. Prod 2026-08-28: 265 such collapses, 248 of them
            // day-wide, losing ~5,799 hours of derived work; it is why cell
            // 28f62f01/08-25 has no derived unit at all for hours 21 and 22.
            //
            // Left alone rather than expanded into 24 hour units: expanding mints
            // 24x the journal entries this comment already warns about, and
            // day-wide units are the healthy ones — 0 of 398 published empty over
            // a non-empty base against 14.5% for hour-wide.
            if !(task.key.operation == Operation::DerivedRollup
                && task.key.slice.width() < DERIVED_SLICE_MICROS
                && task.parent_measured_bytes.is_none()
                && !matches!(task.state, TaskState::Complete | TaskState::Superseded))
            {
                continue;
            }
            let start = task.key.slice.start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            let mut key = task.key.clone();
            key.slice = TimeSlice { start_micros: start, end_micros: start.saturating_add(DERIVED_SLICE_MICROS) };
            replacements
                .entry(key)
                .and_modify(|(deadline, estimate, created)| {
                    *deadline = (*deadline).min(task.deadline_micros);
                    *estimate = estimate.saturating_add(task.estimated_decoded_bytes);
                    *created = (*created).min(task.created_unix_ms);
                })
                .or_insert((task.deadline_micros, task.estimated_decoded_bytes, task.created_unix_ms));
            task.state = TaskState::Superseded;
            task.retry_reason = Some("migrated_to_aligned_hour_slice".to_owned());
            self.dirty_tasks.insert(task.key.clone());
            migrated = migrated.saturating_add(1);
        }
        for (key, (deadline, estimate, created)) in replacements {
            self.enqueue(key, deadline, estimate, created);
        }
        migrated
    }

    /// Remove the one-time global backlog produced by the original cursor
    /// bootstrap. That implementation expanded every retained partition into
    /// dedup, rollup, and packing work and produced 730k journal entries in
    /// production. Keeping completed publications preserves recoverable
    /// coverage; dropping unfinished entries is correctness-safe because they
    /// remain uncovered and reads use raw data until normal planners or new
    /// invalidations enqueue bounded work.
    pub fn migrate_bootstrap_backlog(&mut self) -> Option<usize> {
        self.migrate_bootstrap_backlog_with_limit(Self::BOOTSTRAP_BACKLOG_LIMIT)
    }

    fn migrate_bootstrap_backlog_with_limit(&mut self, limit: usize) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::BOOTSTRAP_BACKLOG_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut removed = 0;
        if self.snapshot.tasks.len() > limit {
            removed = self.retain_tasks(|task| task.state == TaskState::Complete);
        }
        self.snapshot.source_cursors.insert(Self::BOOTSTRAP_BACKLOG_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::BOOTSTRAP_BACKLOG_MIGRATION.to_owned());
        Some(removed)
    }

    /// One-shot: drop the fine-grained SEALED backfill tasks so the coarse
    /// planner can re-derive them a day at a time.
    ///
    /// `plan_rollup_backfill` used to enqueue history through `invalidate`,
    /// which expands a day into ~144 ten-minute slices x Dedup/BaseRollup/
    /// HotPacking x each tier — about 450 durable tasks per (project, date).
    /// Prod 2026-08-17 reached 127,536 pending that way and drained ~19/min,
    /// because each unit costs 50-80s of snapshot-refresh and commit regardless
    /// of how little data it covers. That is ~111 hours of pure overhead for
    /// ~600 GB of actual work, and it is why adding concurrency kept not
    /// helping — and why pushing concurrency harder ended in an OOM kill.
    ///
    /// Changing the planner alone does not help, because the already-enqueued
    /// tasks still have to drain. So drop them: they are unpublished backfill
    /// work, and the planner re-derives exactly what is missing from rollup
    /// COVERAGE, so nothing is lost by forgetting the intent.
    ///
    /// Deliberately narrow. Only non-Complete, only sealed slices, and only the
    /// operations the backfill mints. Frontier work is untouched, and
    /// SealedConsolidation/Repair are left alone because `plan_compaction_debt`
    /// already plans those day-sized.
    /// Forget every stored byte estimate once, because they were all measured
    /// with a broken ruler.
    ///
    /// Until `slice_share_of_file` the estimate counted whole files, so a
    /// ten-minute child of a split day carried the WHOLE DAY's estimate — and
    /// `coarsen_to_width` sums its members, so fusing 144 such children summed
    /// 144 whole days and never fit. Prod 2026-08-19, immediately after the
    /// estimate fix deployed:
    ///
    /// ```text
    /// subsumed=0 fused=0 candidates=266530 blocked=24 over_budget=266506
    /// ```
    ///
    /// `blocked` had collapsed from 249,786 to 24 — the superseded trap was
    /// gone — and every single candidate was now refused on a number written
    /// before the fix existed. A correction that only applies to new
    /// measurements cannot repair a durable queue full of old ones.
    ///
    /// Zero is what a freshly minted unit already carries: the claim-time
    /// preflight computes the real estimate and splits if it genuinely must, so
    /// the worst case is one over-sized claim that immediately right-sizes
    /// itself. That is strictly better than a queue that can never fuse.
    pub fn clear_stale_estimates(&mut self) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::STALE_ESTIMATE_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut cleared = 0usize;
        for task in self.snapshot.tasks.iter_mut().filter(|task| task.state != TaskState::Complete && task.estimated_decoded_bytes != 0) {
            task.estimated_decoded_bytes = 0;
            cleared += 1;
        }
        self.dirty_tasks.clear();
        self.snapshot.source_cursors.insert(Self::STALE_ESTIMATE_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::STALE_ESTIMATE_MIGRATION.to_owned());
        Some(cleared)
    }

    /// One-shot: forget the attempt history of every Repair unit.
    ///
    /// `attempts` is evidence about the code that produced it, and for Repair
    /// that code no longer exists. Prod 2026-09-01 carried 432 repair units,
    /// every one `worker_error`, one at `attempts = 100`, under a rewrite that
    /// re-read its input once per event-time slice and could not finish inside
    /// any deadline. With the rewrite single-pass, that history is not evidence
    /// — it is a sentence: `attempts >= 2` makes a unit QUARANTINED (claimable
    /// only through `coordinator_jobs / 8` slots) and floors its retry backoff
    /// at `operation_deadline_secs`, now an hour. 432 units through ~2 slots at
    /// an hour each is over a week before the fix is even attempted once.
    ///
    /// Clearing the deadline too, because the floor is already stamped into
    /// `deadline_micros` on units that were abandoned before this shipped.
    /// Safe in the same way `clear_stale_estimates` is: zero is what a freshly
    /// minted unit carries, and the claim path re-derives everything else.
    pub fn reset_repair_attempts(&mut self) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::REPAIR_SINGLE_PASS_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let mut reset = 0usize;
        for task in self.snapshot.tasks.iter_mut().filter(|task| task.key.operation == Operation::Repair && task.state != TaskState::Complete) {
            task.attempts = 0;
            task.retry_reason = None;
            task.deadline_micros = 0;
            reset += 1;
        }
        // A journal with no repair queue has nothing to forgive, so it must not
        // spend the one-shot cursor — or the migration would be consumed by a
        // boot that happened to precede the queue, and the caller would compact
        // for nothing on every fresh journal.
        if reset == 0 {
            return None;
        }
        self.dirty_tasks.clear();
        self.snapshot.source_cursors.insert(Self::REPAIR_SINGLE_PASS_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::REPAIR_SINGLE_PASS_MIGRATION.to_owned());
        Some(reset)
    }

    /// Drop queued work for a rollup tier that is no longer DECLARED.
    ///
    /// Removing a spec from the schema does not remove the tasks already queued
    /// against it, and those tasks stay claimable forever: prod 2026-08-24 was
    /// still spending ~80 claims per ten minutes on `dashboard_level_1h_v1`
    /// after its spec had been deleted, doing nothing each time. Every tier
    /// rename (`_v2` -> `_v3`) leaves the same residue.
    ///
    /// Conservative on purpose, because the cost of a false positive is deleting
    /// live work:
    ///
    ///   * only rollup-target names — a table that is not `{source}_rollup_*`
    ///     is never a tier and is never considered;
    ///   * only non-Complete tasks, so history is untouched;
    ///   * and NOTHING happens when `declared` is empty, which is what an
    ///     unloaded registry looks like. Without that guard a startup ordering
    ///     change would silently retire the entire queue.
    pub fn retire_undeclared_tiers(&mut self, declared: &HashSet<String>) -> usize {
        if declared.is_empty() {
            return 0;
        }
        self.retain_tasks(|task| {
            let tier = task.key.physical_table.contains("_rollup_");
            !(tier && task.state != TaskState::Complete && !declared.contains(&task.key.physical_table))
        })
    }

    /// How much of `migration`'s ordered repair list `source` has CONSUMED.
    ///
    /// The durable form of a one-shot repair, and the difference is the whole
    /// point: a claim-once cursor gives the caller exactly one planner pass, and
    /// a pass admits a bounded number of cells, so any list longer than that
    /// bound is silently truncated and the tail is lost. A prefix index survives
    /// restarts and every pass forces only what it can actually fit — see
    /// [`Self::DAMAGE_REPAIR_MIGRATION`].
    pub fn repair_cursor(&self, migration: &str, source: &str) -> usize {
        usize::try_from(self.snapshot.source_cursors.get(&format!("{migration}:{source}")).copied().unwrap_or_default()).unwrap_or(usize::MAX)
    }

    /// Record that `source` has consumed `consumed` entries of `migration`'s list.
    ///
    /// Monotonic, because WAL replay folds `SourceCursor` records with `max()`
    /// and because the caller must only ever move forward: a pair that the pass
    /// dropped keeps the cursor where it is, so the next pass re-offers it.
    /// Persist AFTER the enqueue's own checkpoint — a crash in between re-offers,
    /// which is idempotent (`enqueue` upserts by key), while the other order
    /// re-creates the v1 bug.
    pub fn advance_repair_cursor(&mut self, migration: &str, source: &str, consumed: usize) -> anyhow::Result<()> {
        let key = format!("{migration}:{source}");
        let consumed = u64::try_from(consumed).unwrap_or(u64::MAX);
        if self.snapshot.source_cursors.get(&key).copied().unwrap_or_default() >= consumed {
            return Ok(());
        }
        self.snapshot.source_cursors.insert(key.clone(), consumed);
        self.dirty_cursors.insert(key);
        self.checkpoint()
    }

    /// Claim the one-shot orphaned-coverage repair, or `None` if it already ran.
    ///
    /// The caller does the work — it needs the tier partitions, which the journal
    /// does not have. This owns only the once-ness, and it persists the cursor
    /// IMMEDIATELY rather than after the enqueue: a repair that half-ran and then
    /// died to a restart would otherwise re-force every cell on the next boot,
    /// and this box restarts every few minutes. Re-enqueueing is idempotent at
    /// the journal (`enqueue` upserts by key), so the safe failure is running
    /// once and under-repairing, never looping.
    ///
    /// PER SOURCE. The caller runs inside a loop over sources, so a single global
    /// cursor would be consumed by whichever source happens to be processed
    /// first — and if that is `otel_metrics`, the repair fires for a source that
    /// does not need it and NEVER runs for `otel_logs_and_spans`, which is the
    /// one whose spec changed. A silent no-op that looks like success.
    ///
    /// Still one-shot, unlike the damage repair beside it, and deliberately: this
    /// one forces a DATE WINDOW of live candidates rather than a fixed list, so a
    /// prefix index into it has nothing stable to index, and re-firing it at a v3
    /// would rebuild every in-window cell the v2 pass already repaired.
    pub fn repair_orphaned_coverage_once(&mut self, source: &str) -> Option<u64> {
        let key = format!("{}:{source}", Self::ORPHAN_REPAIR_MIGRATION);
        let previous = self.snapshot.source_cursors.get(&key).copied().unwrap_or_default();
        if previous >= 1 {
            return None;
        }
        self.snapshot.source_cursors.insert(key.clone(), 1);
        self.dirty_cursors.insert(key);
        let _ = self.checkpoint();
        Some(previous)
    }

    pub fn migrate_fine_grained_backfill(&mut self, now_micros: i64) -> Option<usize> {
        if self.snapshot.source_cursors.get(Self::COARSE_BACKFILL_MIGRATION).copied().unwrap_or_default() >= 1 {
            return None;
        }
        let removed = self.retain_tasks(|task| {
            let coarse_planned = matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking);
            let drop = task.state != TaskState::Complete
                && coarse_planned
                && !is_live_frontier(task.key.slice, now_micros)
                // A day-sized unit is what replaces these; anything already that
                // wide came from the coarse planner and must survive.
                && task.key.slice.width() < 24 * 60 * 60 * 1_000_000;
            !drop
        });
        self.snapshot.source_cursors.insert(Self::COARSE_BACKFILL_MIGRATION.to_owned(), 1);
        self.dirty_cursors.insert(Self::COARSE_BACKFILL_MIGRATION.to_owned());
        Some(removed)
    }

    /// Collapse a sealed day's leftover ten-minute units into one day unit.
    ///
    /// The live path mints a unit per ten-minute slice per project per tier,
    /// which is right while the day IS the frontier and pure overhead once it
    /// seals: ~144 units where one would do, each paying the same fixed
    /// object-store cost regardless of how little it covers. That is the whole
    /// shape of the backlog — prod 2026-08-17 sat at 18,040 pending with dedup,
    /// base-rollup and hot-packing each around 5,000, draining at roughly the
    /// rate the next midnight refills it. At ten times the projects it does not
    /// merely lag, it diverges.
    ///
    /// One-shot migration `migrate_fine_grained_backfill` did this once for the
    /// historical backlog; this is the recurring form, because every midnight
    /// creates another day of it.
    ///
    /// Fusion is a cascade over `COARSEN_WIDTHS`, not a day-or-nothing choice:
    /// a span lands at the widest width whose estimate fits the decode budget.
    /// Day-or-nothing left an over-budget day holding all 144 of its ten-minute
    /// slices, and on an uncompacted sealed partition each of those re-reads the
    /// whole day regardless — so the fallback for "too big to scan once" was to
    /// scan it 144 times.
    ///
    /// Anti-loop guard: a span already covered by a non-complete unit at least
    /// that wide is skipped. `split_time_task` leaves the parent `Superseded`
    /// when a unit is too big, so without this a whale's day would be split into
    /// children, fused back into a day, split again, forever. The guard is
    /// per-width, so a superseded day blocks only the day — its children still
    /// fuse at six hours, which is the point of the cascade.
    pub fn coarsen_sealed_slices(&mut self, now_micros: i64) -> usize {
        self.coarsen_sealed_slices_reporting(now_micros).total()
    }

    /// `coarsen_sealed_slices`, with the per-stage breakdown.
    ///
    /// The totals alone cannot say why a pass is small, and that is the only
    /// question worth asking of it. Prod 2026-08-19: subsume took `pending_dedup`
    /// from 14,519 to 3,753 in one pass while `pending_base_rollup` moved
    /// 88,104 → 85,287 and the fuse pass settled at ~76 units/tick — at which
    /// rate the base queue needs ~18 hours. Whether that is candidates being
    /// blocked, groups over budget, or simply few candidates is invisible from
    /// a single collapsed count, and every answer implies a different fix.
    pub fn coarsen_sealed_slices_reporting(&mut self, now_micros: i64) -> CoarsenReport {
        self.coarsen_sealed_slices_capped(now_micros, &|_, _, _| None)
    }

    /// `coarsen_sealed_slices_reporting`, with a ceiling on what a partition can
    /// possibly decode to.
    ///
    /// The fit test sums the children's stored `estimated_decoded_bytes`, and
    /// those are WHOLE-FILE figures frozen at enqueue time. On an uncompacted
    /// sealed partition every child re-reads the same files, so the sum counts
    /// the same bytes once per child and grows with the shredding — the test is
    /// most certain to refuse exactly where fusing is worth the most.
    ///
    /// Prod 2026-08-23: `base_rollup / 00000000 / 2026-08-13` held 1,440
    /// one-minute units claiming 391 GB between them, over a partition holding
    /// **35 files totalling 0.36 GB**. Every width was refused and the cell was
    /// stuck. Separately, project 87576849's consecutive one-minute units each
    /// reported an IDENTICAL 4,466,185,462 bytes — the signature of re-reading
    /// one file set.
    ///
    /// `partition_bytes(project, source, date)` returns what that partition
    /// actually holds. The fused estimate is capped by it, because no unit over
    /// one partition can decode more than the partition contains. Returning
    /// `None` keeps the old summed behaviour, so a caller without storage access
    /// — every unit test — is unaffected.
    pub fn coarsen_sealed_slices_capped(&mut self, now_micros: i64, partition_bytes: &dyn Fn(&str, &str, &str) -> Option<u64>) -> CoarsenReport {
        // SUBSUME before fusing. Fusion cannot touch a bucket that a wider
        // pending unit already covers — it would duplicate claimed work — so on
        // its own it leaves exactly the redundancy it exists to remove.
        let subsumed = self.subsume_covered_units(now_micros);
        let mut report = CoarsenReport { subsumed, ..Default::default() };
        for &width in COARSEN_WIDTHS.iter() {
            let stage = self.coarsen_to_width_reporting(width, now_micros, partition_bytes);
            report.fused += stage.fused;
            report.candidates += stage.candidates;
            report.blocked += stage.blocked;
            report.over_budget += stage.over_budget;
            report.priced_by_footprint += stage.priced_by_footprint;
        }
        report
    }

    /// Drop sealed units wholly covered by a WIDER non-complete unit for the
    /// same (table, source, project, operation).
    ///
    /// A rollup or dedup unit rebuilds its entire slice, so a ten-minute unit
    /// sitting inside a queued day-wide unit for the same cell is not work — it
    /// is the same work, listed 144 times. It still costs a full scan when
    /// claimed (a sealed partition's files span the whole day, so nothing
    /// prunes), and it costs `claim_next` a scan of the task set on every tick.
    ///
    /// This is the other half of `coarsen_to_width`, and without it that half
    /// converges to nothing. Fusion refuses any bucket already covered by a
    /// pending unit at least as wide — correctly, since fusing there would
    /// duplicate work — but a pending day-wide unit is *precisely* the condition
    /// under which the narrow units are redundant. So the fuse pass collapsed
    /// only the cells that had no day unit and then went quiet, which is exactly
    /// what prod showed: `maintenance_sealed_slices_coarsened` logged 12, 6, 3
    /// and then nothing for the rest of the process's life while
    /// `pending_base_rollup` sat at 88,100.
    ///
    /// The scale of the redundancy is measured, not assumed. The same process's
    /// backfill census reported `cells_missing=260 cells_wanted=0` — 260 real
    /// (project, date) cells behind 88,100 queued units, about 339 units per
    /// cell, and the planner declining to add more because every cell was
    /// already queued.
    ///
    /// Only NON-COMPLETE covering units subsume. A complete one is not evidence
    /// the span is still queued — it was built once, and a narrower unit inside
    /// it is a later invalidation that must run.
    fn subsume_covered_units(&mut self, now_micros: i64) -> usize {
        type Group = (String, String, String, Operation);
        let group_of =
            |task: &MaintenanceTask| -> Group { (task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation) };
        // For each subsume width, the buckets wholly inside some non-complete
        // unit STRICTLY wider than it. Membership therefore means "contained in
        // a wider live unit", which is the whole predicate.
        let mut covered: [HashSet<(Group, i64)>; SUBSUME_WIDTHS.len()] = Default::default();
        // Pending/Retry/Running only. NOT Superseded, and that exclusion is
        // load-bearing: `split_time_task` supersedes a unit too big to finish
        // and replaces it with children that tile its range, so a superseded
        // parent subsuming its own children would delete exactly the work the
        // split just created and leave the cell with nothing. NOT Complete
        // either — built once is not still queued, and a narrower unit inside a
        // completed span is a later invalidation that must run.
        for task in self.snapshot.tasks.iter().filter(|task| matches!(task.state, TaskState::Pending | TaskState::Retry | TaskState::Running)) {
            let group = group_of(task);
            for (index, &width) in SUBSUME_WIDTHS.iter().enumerate() {
                if task.key.slice.width() <= width {
                    continue;
                }
                let mut bucket = task.key.slice.start_micros.div_euclid(width) * width;
                while bucket.saturating_add(width) <= task.key.slice.end_micros {
                    if bucket >= task.key.slice.start_micros {
                        covered[index].insert((group.clone(), bucket));
                    }
                    bucket = bucket.saturating_add(width);
                }
            }
        }
        // Damage repairs are exempt here for the same reason they are exempt
        // from fusion: the repair unit is sized to one file's uncovered span,
        // and the wider unit that subsumes it does NOT replace it — the
        // preflight measures that one over budget and shreds it back down.
        // Prod 2026-08-23: after exempting only the fuse pass, five cells still
        // had NO unit covering their hole at all — `dcad860a` 08-15's units
        // began at 18:11 against a hole of 18:00-18:11 — because this pass had
        // deleted them first.
        let damaged = self.untagged_cells.clone();
        let is_damage = |task: &MaintenanceTask| {
            chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).is_some_and(|time| {
                damaged.contains(&(task.key.source.clone(), task.key.project_id.clone(), task.key.physical_table.clone(), time.date_naive().to_string()))
            })
        };
        self.retain_tasks(|task| {
            if !matches!(task.state, TaskState::Pending | TaskState::Retry) || is_live_frontier(task.key.slice, now_micros) || is_damage(task) {
                return true;
            }
            // The NARROWEST ladder width this unit fits inside, so that every
            // unit recorded against it is strictly wider than this one.
            //
            // Taking the widest that fits instead is subtly wrong and reads as
            // correct: `split_time_task` BISECTS, so it produces 12-hour
            // children, and 12h is not a ladder width. Mapped down to the 6h
            // bucket, a 12-hour unit finds the entry its OWN expansion wrote —
            // a unit subsuming itself, deleting both halves of every split and
            // leaving the cell with nothing queued.
            let Some(index) = SUBSUME_WIDTHS.iter().position(|&width| width >= task.key.slice.width()) else { return true };
            let width = SUBSUME_WIDTHS[index];
            let bucket = task.key.slice.start_micros.div_euclid(width) * width;
            // An unaligned unit can straddle two buckets; one bucket covering it
            // is the claim being made, so check it rather than assume alignment.
            if bucket > task.key.slice.start_micros || task.key.slice.end_micros > bucket.saturating_add(width) {
                return true;
            }
            !covered[index].contains(&(group_of(task), bucket))
        })
    }

    /// One pass of `coarsen_sealed_slices` at a single width.
    ///
    /// Fuses every strictly-narrower sealed unit in a bucket into one unit of
    /// `width`, when the bucket's summed estimate fits `MAX_DECODED_BYTES` and
    /// nothing at least that wide already covers it.
    fn coarsen_to_width_reporting(&mut self, width: i64, now_micros: i64, partition_bytes: &dyn Fn(&str, &str, &str) -> Option<u64>) -> CoarsenReport {
        let bucket_of = |start: i64| start.div_euclid(width) * width;
        // DAMAGE REPAIR IS NEVER COARSENED. A repair unit is deliberately sized
        // to one file's uncovered span, so fusing it destroys the only work that
        // can close that hole — and the fused unit does not replace it, because
        // the preflight measures it over budget and shreds it back down. That is
        // the split/fuse cycle the comment below argues cannot happen: fusion is
        // gated on the CHILDREN'S summed estimate, but a slice claims at least
        // one row group of every overlapping file, so those estimates are floor
        // inflated and their sum does not predict the fused unit's real cost.
        //
        // Measured on prod 2026-08-23: the three units covering `dcad860a`
        // 08-15's eleven-minute hole (5m, 6m, 8m — Pending, eligible,
        // attempts=0) had VANISHED from the journal a few hours later, leaving
        // only completed units on either side of the hole. Five such cells sat
        // at 3-11 minutes for an entire day while their neighbours converged.
        // Cloned, not borrowed: this pass mutates `self` further down. The set is
        // the damaged cells only — 24 entries when this shipped, and zero once
        // they converge.
        let untagged_cells = self.untagged_cells.clone();
        let coarsenable = |task: &MaintenanceTask| {
            matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup | Operation::HotPacking)
                && matches!(task.state, TaskState::Pending | TaskState::Retry)
                && !is_live_frontier(task.key.slice, now_micros)
                && task.key.slice.width() < width
                && !chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).is_some_and(|time| {
                    untagged_cells.contains(&(
                        task.key.source.clone(),
                        task.key.project_id.clone(),
                        task.key.physical_table.clone(),
                        time.date_naive().to_string(),
                    ))
                })
        };
        // Which buckets may not be fused at this width, and why. Three distinct
        // reasons, and collapsing them into one rule is what made the old
        // day-or-nothing version dead-end:
        //
        //   Running          — claimed work. Never race it, at any width.
        //   Superseded, <= W — proven too big at a width no larger than this
        //                      one, so this one cannot fit either.
        //   Pending/Retry, >= W — the span is already queued at least this wide;
        //                      a narrower unit inside it is duplicate work.
        //
        // The middle rule is the cascade. `split_time_task` supersedes a unit
        // that did not fit and replaces it with children; without the width
        // comparison a superseded DAY would block its children at every width
        // and they would sit at ten minutes forever, which is the state prod was
        // in. With it, a superseded day frees six hours, a superseded six hours
        // frees one, and each supersede strictly lowers the ceiling — so the
        // split/fuse loop the guard exists to prevent still cannot run.
        let mut blocked: HashSet<(String, String, String, Operation, i64)> = HashSet::new();
        for task in self.snapshot.tasks.iter().filter(|task| match task.state {
            TaskState::Running => true,
            TaskState::Pending | TaskState::Retry => task.key.slice.width() >= width,
            // Superseded does NOT block, and that reversal is the point.
            //
            // It used to block every width at or above its own, so a cell split
            // all the way down carried superseded ancestors at day, 12h, 6h and
            // 1h — which between them blocked every fusion width while none of
            // them could subsume (a superseded parent must not delete the
            // children that replaced it). Its descendants were then permanently
            // stuck: prod 2026-08-19 reported `fused=0 candidates=257,535
            // blocked=249,786` on every tick, a queue that could not shrink by
            // any mechanism it had.
            //
            // The anti-loop guard it was written for is really the budget test
            // below, which is stronger: fusion happens only when the CHILDREN'S
            // summed estimate fits `MAX_DECODED_BYTES`, and a unit that fits
            // does not split, so split/fuse cannot cycle. Superseded only ever
            // meant "did not fit under the estimate of the day", and with
            // `slice_share_of_file` that estimate has changed — refusing on it
            // forever would pin the queue to a measurement already known wrong.
            TaskState::Superseded | TaskState::Complete => false,
        }) {
            let mut bucket = bucket_of(task.key.slice.start_micros);
            while bucket < task.key.slice.end_micros {
                blocked.insert((task.key.physical_table.clone(), task.key.source.clone(), task.key.project_id.clone(), task.key.operation, bucket));
                bucket = bucket.saturating_add(width);
            }
        }

        let mut report = CoarsenReport::default();
        let mut groups: HashMap<(String, String, String, Operation, i64), GroupPrice> = HashMap::new();
        let mut members: HashMap<(String, String, String, Operation, i64), usize> = HashMap::new();
        // The OLDEST member's creation time, because the fused unit inherits its
        // members' work and must inherit their age with it.
        let mut oldest: HashMap<(String, String, String, Operation, i64), u64> = HashMap::new();
        for task in self.snapshot.tasks.iter().filter(|task| coarsenable(task)) {
            report.candidates += 1;
            let group = (
                task.key.physical_table.clone(),
                task.key.source.clone(),
                task.key.project_id.clone(),
                task.key.operation,
                bucket_of(task.key.slice.start_micros),
            );
            if blocked.contains(&group) {
                report.blocked += 1;
                continue;
            }
            groups.entry(group.clone()).or_default().add(task);
            *members.entry(group.clone()).or_default() += 1;
            oldest.entry(group).and_modify(|at| *at = (*at).min(task.created_unix_ms)).or_insert(task.created_unix_ms);
        }
        // Only fuse a span that will actually FIT. Coarsening is a win because
        // one unit does one scan where 144 slices each did the same scan — but a
        // unit that cannot finish inside its deadline does none of them, and
        // that is strictly worse than the slices it replaced.
        //
        // Measured after #178 shipped: BaseRollup began timing out at 900s for
        // the first time (4 in a 10-minute window), and rollup output collapsed
        // from ~9,000 rows/min to 10 — 469 rows in 46 minutes. The split-on-claim
        // and abandon-bisect paths were expected to right-size those units and did
        // not, or not nearly fast enough.
        //
        // What this test must NOT do is give up. Day-or-nothing meant an
        // over-budget day kept all 144 of its ten-minute slices, and on an
        // uncompacted sealed partition every one of those slices re-reads the
        // WHOLE day anyway — no timestamp-stat pruning can skip a file that
        // spans it. So the fallback for a day too big to scan once was to scan
        // it 144 times. That is why this is a cascade: the day that does not fit
        // is offered six hours, then one hour, and lands at the widest span it
        // can actually finish. Prod 2026-08-19 sat at 84,834 pending base
        // rollups with coarsening logging 12, 6, 3, then nothing — everything
        // collapsible had collapsed and the rest was over budget forever.
        //
        // The children's own estimates are already summed here, so the test is
        // free.
        // ...and then bound the result by what the partition can actually hold.
        //
        // `GroupPrice` charges members sharing an `InputFootprint` once, which is
        // the exact de-duplication. But members with NO footprint keep the old
        // summed price, and prod's backlog is entirely of that kind: those units
        // were enqueued before footprints existed, so they are the ones still
        // stuck. 2026-08-23: `base_rollup / 00000000 / 2026-08-13` held 1,440
        // one-minute units summing to 391 GB over a partition of 35 files /
        // 0.36 GB, and project 87576849's consecutive minutes each carried an
        // IDENTICAL 4,466,185,462 bytes.
        //
        // No unit over one partition can decode more than the partition holds,
        // so that is a sound ceiling on any price however it was computed. It
        // only ever removes double-counting — it never argues a big partition is
        // small.
        let mut priced_by_partition: HashSet<(String, String, String, Operation, i64)> = HashSet::new();
        for ((table, source, project_id, op, bucket), price) in groups.iter_mut() {
            let Some(date) = chrono::DateTime::from_timestamp_micros(*bucket).map(|time| time.date_naive().to_string()) else { continue };
            if let Some(ceiling) = partition_bytes(project_id, source, &date) {
                price.cap_at(ceiling);
                // Dedup ONLY. The argument below applies to any partition-scoped
                // cost, but the escape hatch does not: a fused over-budget unit
                // is only safe where the runner honours `hash_shard`, and dedup
                // is the path where it demonstrably does (`dedup_shard_count`
                // and the probe's `hash_bucket` filter). Widening this to the
                // rollup lanes needs that check first.
                if *op == Operation::Dedup {
                    priced_by_partition.insert((table.clone(), source.clone(), project_id.clone(), *op, *bucket));
                }
            }
        }
        groups.retain(|group, price| {
            // A group priced against its PARTITION may exceed the decode budget
            // and still be worth fusing, because its members do not avoid that
            // cost by staying apart — they each pay it. A dedup or rollup slice
            // reads at least one row group of every file it overlaps, and on a
            // sealed partition the files span the day, so 144 ten-minute units
            // are 144 scans of exactly what one day-wide unit would scan once.
            //
            // Refusing on `MAX_DECODED_BYTES` therefore preserved the shape it
            // was meant to prevent. Prod 2026-09-01, every coarsening pass:
            // `candidates=7452 fused=0 over_budget=6967` — the queue's ~4,000
            // sub-15-minute dedup slivers were re-evaluated and re-refused every
            // 60 seconds while `pending_dedup` sat at 5,000.
            //
            // The budget is enforced where it can still be honoured: the claim's
            // preflight measures the fused unit and `byte_bounded_units` shards
            // it BY KEY, which is k scans instead of 144. Without a partition
            // ceiling (no storage access — every unit test) the old rule stands,
            // because then the price really is a sum over possibly-disjoint files.
            let fits = price.bytes() <= MAX_DECODED_BYTES || priced_by_partition.contains(group);
            report.priced_by_footprint += usize::from(fits && price.summed_bytes > MAX_DECODED_BYTES);
            if !fits {
                report.over_budget += members.get(group).copied().unwrap_or(0);
            }
            fits
        });
        if groups.is_empty() {
            return report;
        }
        report.fused = self.retain_tasks(|task| {
            !coarsenable(task)
                || !groups.contains_key(&(
                    task.key.physical_table.clone(),
                    task.key.source.clone(),
                    task.key.project_id.clone(),
                    task.key.operation,
                    bucket_of(task.key.slice.start_micros),
                ))
        });
        for ((physical_table, source, project_id, operation, bucket), price) in groups {
            let Ok(slice) = TimeSlice::new(bucket, bucket.saturating_add(width)) else { continue };
            let oldest_member = oldest
                .get(&(physical_table.clone(), source.clone(), project_id.clone(), operation, bucket))
                .copied()
                .unwrap_or_else(|| u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default());
            self.upsert(MaintenanceTask {
                key: TaskKey { physical_table, source, project_id, slice, operation },
                state: TaskState::Pending,
                deadline_micros: now_micros,
                estimated_decoded_bytes: price.bytes(),
                hash_shard: 0,
                hash_shards: 1,
                attempts: 0,
                // NOT `now`. `scheduling_class` escalates a task that has waited
                // past STARVATION_MICROS, so stamping the fused unit with the
                // current time makes it permanently fresh — and the narrow
                // leftovers it did not fuse keep their real, older creation time
                // and therefore outrank it forever.
                //
                // Prod 2026-08-19, over 316 claims in 35 minutes: Dedup and
                // BaseRollup claimed ZERO day-wide units while Repair (34/34),
                // HotPacking (29/41) and SealedConsolidation (19/20) claimed
                // almost nothing else. Those three are planned day-wide and
                // never fused; the two that fuse were starving their own output.
                //
                // Inheriting the oldest member's age is what makes the fused unit
                // represent the work rather than the moment of fusion. Same
                // defect as ageing a re-derived hygiene task from the rescan.
                created_unix_ms: oldest_member,
                retry_reason: None,
                publication: None,
                base_tier_present: false,
                // Only when every member agreed. A fused unit over several file
                // sets reads their union, which no scalar here can state, and
                // guessing one would let the next width up under-price itself.
                input: price.unanimous_input(),
                parent_measured_bytes: None,
                backfill_priority_micros: None,
            });
        }
        report
    }

    /// Record that the base tier a queued derived unit aggregates already
    /// exists. Returns whether anything changed.
    ///
    /// The planner cannot do this through `enqueue`, because it SKIPS every day
    /// that already has rollup work queued (`want.retain(|key| !queued...)`) —
    /// and a day with a stuck derived task is exactly such a day. So the tasks
    /// that most need the proof are the ones `enqueue` can never reach: prod
    /// 2026-08-18 22:50 UTC, `pending_derived_rollup` still did not move after
    /// #184 shipped, because all 759 of them predated it.
    /// Applies to EVERY derived task in the day, at whatever width.
    ///
    /// Keyed on one exact day-wide `TaskKey`, this proved nothing on prod. The
    /// queued historical work is not day-wide: `invalidate` mints derived units
    /// at `DERIVED_SLICE_MICROS` (one hour), and `coarsen_sealed_slices` refuses
    /// to fuse a day whose day-wide unit already exists in ANY state — including
    /// `Complete`, which is exactly what a legacy rows=0 publication left behind.
    /// So the day carries hour-wide pending tasks plus a completed day-wide one,
    /// and a proof aimed at the day-wide key landed on the completed task, which
    /// is never claimed.
    ///
    /// Measured 2026-08-19 03:00 UTC by #194's census:
    ///
    /// ```text
    /// cells_missing=264  cells_wanted=0  defer_enqueue=false
    /// ```
    ///
    /// — the planner saw every hole, every one was vetoed as already-queued, and
    /// `rollup_derived_base_tier_proven` had not fired once. The tasks existed,
    /// were pending, and could not be claimed because `dependencies_complete`
    /// still had no proof for THEM.
    ///
    /// The fact being recorded is a property of the DAY — "the tier this derives
    /// from is already built" — so it belongs on every task covering that day.
    /// Why is queued work for `operation` not being claimed?
    ///
    /// Every counter this system has reports how much work EXISTS. None reports
    /// why a task that exists is passed over, and `claim_next` decides that
    /// inside filter predicates that leave no trace. That gap has now cost five
    /// fixes shipped against wrong models of the queue (#186, #189, #190, #192,
    /// #195), each individually correct.
    ///
    /// Returns `(pending, sealed, unproven, quarantined, not_yet_due)`, which
    /// between them cover every reason `claim_next` skips a pending task.
    ///
    /// `unproven` counts `!base_tier_present` rather than calling
    /// `dependencies_complete`, deliberately. That predicate is itself a scan of
    /// the whole task set, so calling it per task would make this census O(n^2)
    /// under the journal lock — ~900 x 55,000 every 60s on prod. The flag is the
    /// actionable half anyway: for a historical derived unit it is exactly what
    /// decides the dependency, and it is O(1).
    pub fn claimability_census(&self, operation: Operation, now_micros: i64) -> (usize, usize, usize, usize, usize) {
        let (mut pending, mut sealed, mut unproven, mut quarantined, mut not_due) = (0, 0, 0, 0, 0);
        for task in self.snapshot.tasks.iter().filter(|task| task.key.operation == operation) {
            if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
                continue;
            }
            pending += 1;
            sealed += usize::from(!is_frontier_task(task, now_micros));
            unproven += usize::from(!task.base_tier_present);
            quarantined += usize::from(Self::is_quarantined(task));
            not_due += usize::from(task.deadline_micros > now_micros);
        }
        (pending, sealed, unproven, quarantined, not_due)
    }

    /// The full claim order for one task: `(class, damaged, starved, hole,
    /// width, benefit, recency)`. Smaller wins.
    ///
    /// `hole_rank` orders WITHIN a class: a cell whose tier output is missing
    /// outranks one that already has output and is merely being re-derived.
    /// Without it, sealed rollup work is strictly newest-first, and recent days
    /// are re-invalidated continuously by ongoing publication — so the claim
    /// never walks back far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f`'s 1h tier jumped 2026-07-31 -> 08-14 for a second day running,
    /// while day-wide derived units for 08-17 were claimed repeatedly.
    /// Newest-first is right for FRESHNESS and wrong for CONTIGUITY, and 30
    /// contiguous days is a contiguity goal.
    ///
    /// DAMAGE leads its class, ahead of `starved`, because the starvation window
    /// is a FRESHNESS heuristic and damage is not a freshness question.
    /// `starved` grades AGE and is compared first, so whatever `hole_rank` says
    /// about a damaged cell is only reached once age has spoken: every untagged
    /// file left on prod 2026-08-23 was 32 to 37 days old, which under the old
    /// hard horizon put it below the entire ~12,000-unit backfill queue.
    ///
    /// Damage does not order by width OR recency — every damage unit ties, so the
    /// per-project cursor in `claim_next` rotates across the damaged CELLS
    /// instead of draining one to exhaustion. Both width orderings starve.
    /// `-width` (widest first) buried the narrow repair units: prod 2026-08-23
    /// had three 5-8 minute units Pending at attempts=0 with deadlines nine hours
    /// past, behind 800-1400 minute ladders of the same rank. Reversing it
    /// starved the opposite end — the selection loop matches the winning tuple
    /// EXACTLY, so ordering by width makes the single narrowest unit win every
    /// claim, and one whale cell's shredded ladder always contains a narrower
    /// child than another project's 3-minute hole. Five cells with 3-11 minute
    /// holes sat untouched for eight hours while that one cell ground down its
    /// ladder. Tying them puts every damage unit in one rank group, which is what
    /// `fair_cursors` is for. `benefit` is carried through untouched — it is 0
    /// for the rollup operations damage repair uses, so it cannot disturb the tie.
    ///
    /// A method rather than a closure inside `claim_next` so that a read-only
    /// caller can ask why a unit is losing. See `most_indebted_unclaimed`.
    fn rank(&self, task: &MaintenanceTask, now_micros: i64) -> Rank {
        let (class, starved, width, benefit, order) = scheduling_class(task, now_micros);
        let hole = self.hole_rank(task);
        let (width, order) = if hole == 0 { (0, 0) } else { (width, order) };
        (class, u8::from(hole > 0), starved, hole, width, benefit, order)
    }

    /// The unit holding the most DEBT that is not being claimed, and why.
    ///
    /// `claimability_census` counts why tasks are skipped; `first_refused_sealed`
    /// names one. Neither can answer the question prod actually poses, for two
    /// reasons: both sample the first 64 tasks in journal order, and when the
    /// answer comes back `CLAIMABLE` — eligibility is fine, the refusal is in
    /// ordering — neither says WHAT outranked it.
    ///
    /// That is the branch the worst cell on prod sits in. Measured 2026-08-24:
    /// `87576849 / 2026-08-19` held 238 small files in 1.9 GB, the single largest
    /// file-debt cell in the fleet, and read exactly 238 at four object-storage
    /// censuses spanning a day. Searching 73 minutes of retained logs across
    /// three containers for its slice returned nothing at all — not a start, not
    /// a timeout, not a funnel event. `680acac` instrumented "a unit was claimed
    /// and selected nothing"; nothing instrumented "a cell was planned and never
    /// claimed", so the fleet's worst debt was invisible rather than explained.
    ///
    /// Selects by `input.files` — for hygiene that IS the debt, and it is what
    /// the planner already counted — so the answer is about the cell that matters
    /// rather than whichever one the journal happens to hold first.
    pub fn most_indebted_unclaimed(&self, operation: Operation, now_micros: i64) -> Option<String> {
        let eligible =
            || self.snapshot.tasks.iter().filter(|task| task.key.operation == operation && matches!(task.state, TaskState::Pending | TaskState::Retry));
        let date_of = |task: &MaintenanceTask| {
            chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map_or_else(|| "?".to_owned(), |time| time.date_naive().to_string())
        };
        let worst = eligible().max_by_key(|task| task.input.map_or(0, |input| input.files))?;
        let files = worst.input.map_or(0, |input| input.files);
        // The reasons that live on the task itself, in the order `claim_next`
        // applies them. Anything else means ordering, which is the case no
        // existing instrument could name.
        let reason = if worst.deadline_micros > now_micros {
            "not_due".to_owned()
        } else if Self::is_quarantined(worst) {
            "quarantined".to_owned()
        } else if !self.dependencies_complete(worst) {
            "dependencies".to_owned()
        } else {
            let winner = eligible()
                .filter(|task| task.deadline_micros <= now_micros && !Self::is_quarantined(task) && self.dependencies_complete(task))
                .min_by_key(|task| self.rank(task, now_micros))?;
            if winner.key == worst.key {
                // It wins its own claims, so it is not being starved — whatever
                // is wrong is downstream of selection, not in it.
                return None;
            }
            format!("outranked_by:{:.8}:{}", winner.key.project_id, date_of(winner))
        };
        Some(format!("{reason}:{:.8}:{}:files={files}", worst.key.project_id, date_of(worst)))
    }

    /// The first SEALED task of `operation` that `claim_next` would refuse, and
    /// why — as `(project, date, reason)`.
    ///
    /// Every count in `claimability_census` is a property of the task in
    /// isolation. None of them answers the question that actually matters, which
    /// is why `best_class(sealed_only = true)` returns None while sealed tasks
    /// sit pending. Prod 2026-08-19 has been in exactly that state through four
    /// fixes: 141 sealed derived units, ~60 neither quarantined nor future-dated,
    /// and not one claimed — every derived claim an hour-wide slice of today.
    ///
    /// Bounded to `LIMIT` evaluations because `dependencies_complete` is a scan;
    /// a sample is enough to name the reason, and naming it is the whole point.
    pub fn first_refused_sealed(&self, operation: Operation, now_micros: i64) -> Option<(String, String, &'static str)> {
        const LIMIT: usize = 64;
        let why = |task: &MaintenanceTask| -> &'static str {
            if task.deadline_micros > now_micros {
                "not_due"
            } else if Self::is_quarantined(task) {
                "quarantined"
            } else if !self.dependencies_complete(task) {
                "dependencies"
            } else {
                // Nothing about the task refuses it, so the refusal is upstream —
                // in class ordering or the operation cycle, not in eligibility.
                "CLAIMABLE"
            }
        };
        let describe = |task: &MaintenanceTask| {
            let date = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros)
                .map(|time| time.date_naive().to_string())
                .unwrap_or_else(|| "?".to_owned());
            (task.key.project_id.clone(), date, why(task))
        };
        let mut sealed = self.snapshot.tasks.iter().filter(|task| {
            task.key.operation == operation && matches!(task.state, TaskState::Pending | TaskState::Retry) && !is_frontier_task(task, now_micros)
        });
        // A claimable one is the interesting answer: it means eligibility is fine
        // and the refusal is in ordering. Otherwise report the first task's reason.
        let sample: Vec<_> = sealed.by_ref().take(LIMIT).collect();
        sample.iter().find(|task| why(task) == "CLAIMABLE").or_else(|| sample.first()).map(|task| describe(task))
    }

    /// Publish which `(source, project, date)` have their BASE tier built, read
    /// from real rollup coverage. Replaces the set wholesale: coverage can go
    /// backwards (a rewrite, a vacuum), and a stale "ready" is the direction
    /// that derives from a tier that is not there.
    pub fn set_base_tier_ready(&mut self, ready: HashSet<(String, String, String)>) {
        self.base_tier_ready = ready;
    }

    pub fn base_tier_ready_len(&self) -> usize {
        self.base_tier_ready.len()
    }

    /// Which sources contributed to the published coverage and holes. Exists so
    /// a test can prove the sets are not merely the last source planned.
    pub fn base_tier_ready_sources(&self) -> HashSet<String> {
        self.base_tier_ready.iter().map(|(source, ..)| source.clone()).collect()
    }

    pub fn tier_hole_sources(&self) -> HashSet<String> {
        self.tier_holes.iter().map(|(source, ..)| source.clone()).collect()
    }

    /// Publish which `(source, project, tier table, date)` are MISSING, so
    /// `claim_next` can rank holes ahead of re-derives. Replaced wholesale for
    /// the same reason as `base_tier_ready`.
    pub fn set_tier_holes(&mut self, holes: HashSet<(String, String, String, String)>) {
        self.tier_holes = holes;
    }

    pub fn tier_holes_len(&self) -> usize {
        self.tier_holes.len()
    }

    /// Replace the untagged set for ONE `(source, tier table)`, leaving every
    /// other producer's cells alone — see the field.
    pub fn set_untagged_cells(&mut self, source: &str, table: &str, cells: impl IntoIterator<Item = (String, String)>) {
        self.untagged_cells.retain(|(cell_source, _, cell_table, _)| cell_source != source || cell_table != table);
        self.untagged_cells.extend(cells.into_iter().map(|(project, date)| (source.to_owned(), project, table.to_owned(), date)));
    }

    pub fn untagged_cells_len(&self) -> usize {
        self.untagged_cells.len()
    }

    /// Seed the set from the sidecar at boot, so the damage rank is live from
    /// the first claim instead of from the first recovery pass ~40 minutes in.
    ///
    /// The repair UNITS were already durable — they live in this journal — but
    /// their priority was not. Prod 2026-08-23 restarted four times in one hour,
    /// so the rank was never once active and queued repairs drained at the slow
    /// unprioritised rate.
    ///
    /// Additive, and safe if stale: `set_untagged_cells` replaces its own tier's
    /// slice on the next recovery, and a publish that leaves a partition clean
    /// clears its cell. A stale entry costs one mis-ranked claim, never
    /// correctness.
    pub fn restore_untagged_cells(&mut self, cells: impl IntoIterator<Item = (String, String, String, String)>) {
        self.untagged_cells.extend(cells);
    }

    /// Every cell currently ranked as damaged, for persisting.
    pub fn untagged_cells(&self) -> impl Iterator<Item = &(String, String, String, String)> {
        self.untagged_cells.iter()
    }

    /// Forget one cell, for a publish that has just left the partition clean.
    ///
    /// `recover_rollup_coverage` is the only producer and it runs ONCE at
    /// startup, so without this a converged cell keeps out-ranking real work
    /// until the next restart — the ranking would spend claims re-deriving the
    /// day it already fixed.
    pub fn clear_untagged_cell(&mut self, source: &str, table: &str, project: &str, date: &str) -> bool {
        self.untagged_cells.remove(&(source.to_owned(), project.to_owned(), table.to_owned(), date.to_owned()))
    }

    /// How badly this cell needs the work: 0 repairs DAMAGE, 1 fills a missing
    /// day, 2 re-derives a day that already has output. Smaller runs first.
    ///
    /// Damage leads because a repair unit is narrow by construction — it targets
    /// the uncovered span of one file, 39 to 308 minutes on prod 2026-08-22 —
    /// and the `-width` tiebreak below therefore ranks it BELOW every day-wide
    /// backfill hole. Sharing one rank with those made the damaged cells drain
    /// at ~2.4 files/hour behind ~12,000 wider units, which is the slow tail
    /// this ordering exists to prevent.
    ///
    /// Bounded, so it cannot starve the backfill: the set comes from files that
    /// actually exist and shrinks as they are retired — 39 cells when this
    /// shipped, and zero is the terminal state.
    fn hole_rank(&self, task: &MaintenanceTask) -> u8 {
        if !matches!(task.key.operation, Operation::BaseRollup | Operation::DerivedRollup) {
            return 2;
        }
        let Some(date) = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map(|time| time.date_naive().to_string()) else {
            return 2;
        };
        let cell = (task.key.source.clone(), task.key.project_id.clone(), task.key.physical_table.clone(), date);
        if self.untagged_cells.contains(&cell) {
            0
        } else if self.tier_holes.contains(&cell) {
            1
        } else {
            2
        }
    }

    pub fn prove_base_tier_for_day(&mut self, key: &TaskKey, day_start: i64, day_end: i64) -> usize {
        let mut proven = 0;
        for task in &mut self.snapshot.tasks {
            // Only work that can still run. A completed task matches its own
            // day and proving it is a no-op that would make `proven` read as
            // progress when nothing became claimable.
            if !matches!(task.state, TaskState::Pending | TaskState::Retry)
                || task.base_tier_present
                || task.key.operation != key.operation
                || task.key.source != key.source
                || task.key.project_id != key.project_id
                || task.key.physical_table != key.physical_table
                || task.key.slice.start_micros < day_start
                || task.key.slice.end_micros > day_end
            {
                continue;
            }
            task.base_tier_present = true;
            self.dirty_tasks.insert(task.key.clone());
            proven += 1;
        }
        proven
    }

    /// Remember what a unit reads, measured by the claim-time preflight.
    ///
    /// Recorded on EVERY claim, not only when the preflight splits. The split
    /// that shredded prod was not the byte one — a unit that fits its estimate
    /// and then times out is bisected by `abandon_running`, which knows only a
    /// key. Without a footprint already on the parent, those children carry
    /// none, fusion sums them, and the bisect ladder is one-way again.
    pub fn record_input(&mut self, key: &TaskKey, input: InputFootprint) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        if task.input == Some(input) {
            return false;
        }
        task.input = Some(input);
        self.dirty_tasks.insert(key.clone());
        true
    }

    pub fn upsert(&mut self, task: MaintenanceTask) {
        // A key removed earlier in this write window and re-created now must not
        // still carry a tombstone; the upsert is the later truth. Every path
        // that re-creates a task — enqueue, invalidate, the fused unit
        // `coarsen_to_width` writes over the members it just dropped — goes
        // through here, so this is the one place it needs saying.
        self.removed_tasks.remove(&task.key);
        self.dirty_tasks.insert(task.key.clone());
        if let Some(index) = self.task_indices.get(&task.key).copied() {
            self.snapshot.tasks[index] = task;
        } else {
            let index = self.snapshot.tasks.len();
            self.task_indices.insert(task.key.clone(), index);
            self.snapshot.tasks.push(task);
        }
    }

    pub fn enqueue(&mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64) {
        self.enqueue_with_base_tier(key, deadline_micros, estimated_decoded_bytes, created_unix_ms, false);
    }

    /// `base_tier_present` records that the tier this unit aggregates already
    /// exists — see the field on [`MaintenanceTask`]. Only `plan_rollup_backfill`
    /// can prove it, because only it reads actual tier coverage.
    ///
    /// Returns whether the queue accepted the unit. `false` is one of the two
    /// structural vetoes in `enqueue_inner` — a Superseded parent with a live
    /// descendant, or a Retry parked on a worker/schema failure — both of which
    /// are correct and both of which are otherwise INVISIBLE to the caller. The
    /// damage repair counts them, because its whole failure mode is pairs that
    /// look forced and are not.
    pub fn enqueue_with_base_tier(
        &mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64, base_tier_present: bool,
    ) -> bool {
        self.enqueue_inner(key, deadline_micros, estimated_decoded_bytes, created_unix_ms, base_tier_present, None)
    }

    /// Queue a unit the planner has already MEASURED, carrying its footprint.
    ///
    /// `scheduling_class` ranks file hygiene by `input.files` and
    /// `most_indebted_unclaimed` picks the worst cell on the same field, but the
    /// only writer was `record_input` — at CLAIM time. So every never-claimed
    /// cell scored zero, which is precisely the population both exist to order:
    /// a 139-file cell tied a 2-file one and ordering fell through to recency.
    /// Prod 2026-08-25, one container over 5h49m: `small_files_in_them` 746 ->
    /// 447 with `out_of_policy_cells` at 51 for nine consecutive samples —
    /// hygiene rotated across cells instead of finishing any of them.
    ///
    /// Deliberately NOT `upsert`: that replaces the whole task, resetting
    /// `state`/`attempts` and bypassing the Superseded/live-descendant veto
    /// below, which exists because resurrecting a parent beside its children
    /// starved them for days.
    pub fn enqueue_planned(&mut self, task: &MaintenanceTask) {
        self.enqueue_inner(task.key.clone(), task.deadline_micros, task.estimated_decoded_bytes, task.created_unix_ms, task.base_tier_present, task.input);
    }

    /// Precedence between the two footprints, since both are honest:
    ///
    /// * `None` NEVER erases. Most callers cannot measure anything, and stripping
    ///   a claim-time footprint would break the bisect ladder `record_input`
    ///   documents — children of a fused or split unit would carry none.
    /// * The claim-time measurement wins while the unit is `Running` (the guard
    ///   below already refuses to touch a running task) and while it sits in the
    ///   quarantine early-return: those are the windows `abandon_running` reads
    ///   it in, and it is a real measurement of what the unit actually read.
    /// * Between claims the PLANNER wins. It re-derives the live file set every
    ///   60 s, so it is the fresher observation, and nothing is lost — every
    ///   claim records its own footprint unconditionally. This is also what
    ///   heals the backlog: cells enqueued before this change gain a count on
    ///   the next planner tick instead of waiting for a first claim.
    ///
    /// Returns `true` when the unit is queued afterwards — minted now, or already
    /// present and deliberately left alone. Only the two structural vetoes below
    /// return `false`.
    fn enqueue_inner(
        &mut self, key: TaskKey, deadline_micros: i64, estimated_decoded_bytes: u64, created_unix_ms: u64, base_tier_present: bool,
        input: Option<InputFootprint>,
    ) -> bool {
        // Same rule as `upsert`, and it needs stating twice because this path
        // does not go through it: a key removed earlier in this write window and
        // enqueued again is CREATED, not removed. `coarsen_to_width` does
        // exactly that in a single pass — it drops a bucket's members and writes
        // a fused unit whose key can be one of them — so without this the pass
        // would delete the unit it exists to create.
        self.removed_tasks.remove(&key);
        if let Some(index) = self.task_indices.get(&key).copied() {
            // A superseded parent's work lives on in its children; re-noticing
            // the same debt is not new information and must not resurrect the
            // parent beside them. Prod 2026-08-21, project 87576849: ~6 whole-day
            // Repair units sat at attempts 140-204 for days because every timeout
            // split them and the next 60s `plan_compaction_debt` tick flipped the
            // Superseded parent back to Pending — where its day width outranked
            // its own 12h children, so they never ran once. Only when no live
            // descendant remains is the enqueue fresh debt, and then it is a NEW
            // unit: attempts start over.
            if self.snapshot.tasks[index].state == TaskState::Superseded {
                let parent = &self.snapshot.tasks[index].key;
                let live_descendant = self.snapshot.tasks.iter().any(|task| {
                    task.key != *parent
                        && task.key.operation == parent.operation
                        && task.key.source == parent.source
                        && task.key.project_id == parent.project_id
                        && task.key.physical_table == parent.physical_table
                        && task.key.slice.start_micros >= parent.slice.start_micros
                        && task.key.slice.end_micros <= parent.slice.end_micros
                        && matches!(task.state, TaskState::Pending | TaskState::Retry | TaskState::Running)
                });
                if live_descendant {
                    return false;
                }
                let task = &mut self.snapshot.tasks[index];
                task.state = TaskState::Pending;
                task.deadline_micros = deadline_micros;
                task.estimated_decoded_bytes = estimated_decoded_bytes;
                task.attempts = 0;
                task.retry_reason = None;
                task.publication = None;
                task.base_tier_present |= base_tier_present;
                task.input = input.or(task.input);
                self.dirty_tasks.insert(key);
                return true;
            }
            // `abandon_running`'s verdict outlives a planner tick. Its deadline
            // floor is the only bound on a doomed unit's duty cycle and its
            // `worker_error` reason is what routes the unit through the small
            // quarantine permit; the reset below erased both every 60s, which is
            // how day-wide units re-claimed every 5-8 minutes against a >=900s
            // floor. The debt it re-notices is already queued — nothing is lost.
            // `schema_error` for the same reason, and more sharply: a re-mint
            // would clear the park and hand the unit straight back to a worker
            // whose plan cannot build, every 60 seconds.
            if self.snapshot.tasks[index].state == TaskState::Retry
                && matches!(self.snapshot.tasks[index].retry_reason.as_deref(), Some(Self::WORKER_FAILURE_REASON | Self::SCHEMA_FAILURE_REASON))
            {
                return false;
            }
            let task = &mut self.snapshot.tasks[index];
            if task.state != TaskState::Running {
                let new_deadline = task.deadline_micros.min(deadline_micros);
                let changed = task.state != TaskState::Pending
                    || task.deadline_micros != new_deadline
                    || task.estimated_decoded_bytes != estimated_decoded_bytes
                    || task.retry_reason.is_some()
                    || task.publication.is_some()
                    // Latching, never clearing: the planner proves presence, and
                    // a later enqueue that cannot prove it (the frontier's) is
                    // silence, not evidence of absence.
                    || (base_tier_present && !task.base_tier_present)
                    // The re-plan that HEALS the backlog: a cell queued before
                    // the planner carried a footprint, or one whose debt grew,
                    // must not have to be claimed once to become rankable.
                    || (input.is_some() && input != task.input);
                if changed {
                    task.state = TaskState::Pending;
                    task.deadline_micros = new_deadline;
                    task.estimated_decoded_bytes = estimated_decoded_bytes;
                    task.retry_reason = None;
                    task.publication = None;
                    task.base_tier_present |= base_tier_present;
                    task.input = input.or(task.input);
                    self.dirty_tasks.insert(key);
                }
            }
            return true;
        }
        self.dirty_tasks.insert(key.clone());
        let index = self.snapshot.tasks.len();
        self.task_indices.insert(key.clone(), index);
        self.snapshot.tasks.push(MaintenanceTask {
            key,
            state: TaskState::Pending,
            deadline_micros,
            estimated_decoded_bytes,
            hash_shard: 0,
            hash_shards: 1,
            attempts: 0,
            created_unix_ms,
            retry_reason: None,
            publication: None,
            base_tier_present,
            input,
            parent_measured_bytes: None,
            backfill_priority_micros: None,
        });
        true
    }

    /// Drop every task the predicate rejects, recording a tombstone for each.
    ///
    /// THE way to remove tasks. Four call sites used to hand-roll
    /// `snapshot.tasks.retain(..)` plus an index rebuild, and none of them
    /// recorded anything — the WAL had no way to say "this task is gone", so
    /// the removals lived only in memory and came back on the next load. Prod
    /// 2026-08-19: `pending_base_rollup` 88,618 -> 2,294 with the on-disk
    /// journal still byte-identical at 84,734,124 bytes.
    ///
    /// Centralising it is the actual fix. A caller can no longer forget,
    /// because there is nothing left to remember.
    fn retain_tasks(&mut self, mut keep: impl FnMut(&MaintenanceTask) -> bool) -> usize {
        let before = self.snapshot.tasks.len();
        let removed = &mut self.removed_tasks;
        let dirty = &mut self.dirty_tasks;
        self.snapshot.tasks.retain(|task| {
            if keep(task) {
                return true;
            }
            // A key that was pending a write and is now gone must not also be
            // upserted; the tombstone is the whole story.
            dirty.remove(&task.key);
            removed.insert(task.key.clone());
            false
        });
        let dropped = before - self.snapshot.tasks.len();
        if dropped != 0 {
            self.task_indices = self.snapshot.tasks.iter().enumerate().map(|(index, task)| (task.key.clone(), index)).collect();
        }
        dropped
    }

    /// Record all maintenance consequences of a source mutation. Repeated
    /// invalidations are idempotent by `TaskKey`; an already-complete slice is
    /// made pending again and its quiet-period deadline moves forward.
    pub fn invalidate(&mut self, invalidation: Invalidation<'_>) -> anyhow::Result<()> {
        let Invalidation { source_table, rollup_table, source, project_id, start_micros, end_micros, observed_at_micros, derived } = invalidation;
        // Round up, never down: a bucket can delay eligibility slightly but
        // can never publish before the full quiet period. High-rate ingest
        // then journals one deadline extension per bucket rather than one full
        // task record per event batch.
        let deadline = observed_at_micros.saturating_add(FINALIZATION_DELAY_MICROS);
        let deadline_micros = deadline
            .saturating_add(INVALIDATION_DEADLINE_BUCKET_MICROS - 1)
            .div_euclid(INVALIDATION_DEADLINE_BUCKET_MICROS)
            .saturating_mul(INVALIDATION_DEADLINE_BUCKET_MICROS);
        let created_unix_ms = u64::try_from(observed_at_micros.div_euclid(1_000)).unwrap_or_default();
        let normal_slices = TimeSlice::normal_units(start_micros, end_micros)?;
        let rollup_slices = if derived {
            let aligned_start = start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            let aligned_end = end_micros.saturating_add(DERIVED_SLICE_MICROS - 1).div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
            TimeSlice::fixed_units(aligned_start, aligned_end, DERIVED_SLICE_MICROS)?
        } else {
            normal_slices.clone()
        };
        // HotPacking is deliberately NOT here. File hygiene is planned by DEBT,
        // not by the calendar: `plan_compaction_debt` scans the actual file list
        // per (project, date) every 60s and mints ONE day-wide unit when the
        // partition really has small or unsorted files
        // (`small.len() >= 2 || any !sorted`) — HotPacking for today,
        // SealedConsolidation once the day seals.
        //
        // Minting one per ten-minute slice here duplicated that with 144 units
        // per project per day whose count tracked ingest rather than
        // fragmentation. It was 5,367 pending on prod 2026-08-17 — 22% of the
        // whole journal — for an operation that produces no rollup coverage,
        // and it is a third of everything the live frontier creates. The
        // frontier wanted ~7.8 units/min against a total drain of ~11.6.
        //
        // The rest of the codebase already treated these as waste:
        // `migrate_fine_grained_backfill` and `coarsen_sealed_slices` both list
        // HotPacking as coarsenable, and the comment on the former names it in
        // the "~450 durable tasks per (project, date)" expansion that coarse
        // planning exists to undo. This stops minting them rather than
        // collapsing them afterwards.
        for (operation, slices) in
            [(Operation::Dedup, normal_slices.as_slice()), (if derived { Operation::DerivedRollup } else { Operation::BaseRollup }, rollup_slices.as_slice())]
        {
            for &slice in slices {
                let key = TaskKey {
                    physical_table: match operation {
                        Operation::Dedup | Operation::HotPacking => source_table,
                        _ => rollup_table,
                    }
                    .to_owned(),
                    source: source.to_owned(),
                    project_id: project_id.to_owned(),
                    slice,
                    operation,
                };
                if let Some(index) = self.task_indices.get(&key).copied() {
                    let task = &mut self.snapshot.tasks[index];
                    let new_deadline = task.deadline_micros.max(deadline_micros);
                    let changed =
                        task.state != TaskState::Pending || task.deadline_micros != new_deadline || task.retry_reason.is_some() || task.publication.is_some();
                    if changed {
                        task.state = TaskState::Pending;
                        task.deadline_micros = new_deadline;
                        task.retry_reason = None;
                        task.publication = None;
                        self.dirty_tasks.insert(key);
                    }
                } else {
                    let index = self.snapshot.tasks.len();
                    self.task_indices.insert(key.clone(), index);
                    self.snapshot.tasks.push(MaintenanceTask {
                        key: key.clone(),
                        state: TaskState::Pending,
                        deadline_micros,
                        estimated_decoded_bytes: 0,
                        hash_shard: 0,
                        hash_shards: 1,
                        attempts: 0,
                        created_unix_ms,
                        retry_reason: None,
                        publication: None,
                        base_tier_present: false,
                        input: None,
                        parent_measured_bytes: None,
                        backfill_priority_micros: None,
                    });
                    self.dirty_tasks.insert(key);
                }
            }
        }
        Ok(())
    }

    pub fn mark_running(&mut self, key: &TaskKey) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        if !matches!(task.state, TaskState::Pending | TaskState::Retry) {
            return false;
        }
        task.state = TaskState::Running;
        task.attempts = task.attempts.saturating_add(1);
        true
    }

    /// Attempts after which a unit has PROVEN it does not fit its deadline.
    ///
    /// One timeout is a blip — a FairSpillPool squeeze, an object-store stall.
    /// Two is the slice itself, and is exactly the threshold `abandon_running`
    /// already uses to decide a unit is oversized rather than unlucky.
    pub const QUARANTINE_ATTEMPTS: u32 = 2;

    /// The `retry_reason` `abandon_running` writes when a WORKER gave a unit
    /// back — a deadline it could not meet, or an error inside the unit.
    pub const WORKER_FAILURE_REASON: &'static str = "worker_error";

    /// The `retry_reason` a unit parked on a DETERMINISTIC plan error carries —
    /// a state token, never the error text, because three places match it
    /// exactly: [`Self::is_quarantined`], the planner re-mint guard in
    /// `enqueue`, and the claim gate that reads the first. The text is logged
    /// once at the park site instead.
    pub const SCHEMA_FAILURE_REASON: &'static str = "schema_error";
    /// How long a schema-parked unit waits. Deliberately as long as the file's
    /// other never-going-to-change-soon retries (`invalid_slice_timestamp`): the
    /// thing that fixes it is a rebuild or a deploy, not another attempt.
    const SCHEMA_PARK_MICROS: i64 = 3_600 * 1_000_000;

    /// Park a unit whose SQL cannot be planned. Never bisected: the children
    /// would name the same missing column and fail identically, which is the
    /// 2026-08-24 retry storm.
    fn park_schema_failure(&mut self, key: &TaskKey, now_micros: i64, error: &str) {
        crate::observability::maintenance_stats().maintenance_schema_parked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(?key, %error, event = "maintenance_task_schema_parked", "parked a maintenance unit on a deterministic plan error instead of bisecting it");
        self.retry(key, Self::SCHEMA_FAILURE_REASON.to_owned(), now_micros.saturating_add(Self::SCHEMA_PARK_MICROS));
    }

    /// Has this unit PROVEN it cannot fit its deadline?
    ///
    /// Attempts alone is not that proof, and using it as the proxy quarantined
    /// the wrong work. A task is claimed and its attempt counted before anything
    /// about its cost is known, so a unit that was handed back for a reason
    /// unrelated to cost — `source_not_flushed`, `resolve_input`, a dependency
    /// that was not yet satisfiable — accumulates attempts identically to one
    /// that burned a 900s deadline.
    ///
    /// Measured on prod 2026-08-19: of 162 SEALED derived units — precisely the
    /// historical backfill the 30d goal needs — **109 were quarantined**, held
    /// to 2 of 16 workers. They had failed repeatedly while their base-tier
    /// dependency was unprovable, which #197/#202 then fixed. Their attempt
    /// count was stale evidence about a condition that no longer existed, and
    /// the quarantine kept punishing them for it.
    ///
    /// So require the worker's own verdict: only a unit `abandon_running` gave
    /// back is evidence about cost. Anything else retries normally.
    /// A schema-parked unit qualifies immediately: one deterministic plan error
    /// is already proof, and rationing its claims is the point of the tag.
    pub fn is_quarantined(task: &MaintenanceTask) -> bool {
        task.retry_reason.as_deref() == Some(Self::SCHEMA_FAILURE_REASON)
            || (task.attempts >= Self::QUARANTINE_ATTEMPTS && task.retry_reason.as_deref() == Some(Self::WORKER_FAILURE_REASON))
    }

    /// Claim one unit. `allow_quarantined` admits units that have already timed
    /// out [`Self::QUARANTINE_ATTEMPTS`] times; the caller gates it on a small
    /// occupancy permit so proven-unfittable work cannot hold the whole pool.
    ///
    /// Measured on prod 2026-08-18 21:00 UTC over 60 minutes of logs: 47 units
    /// timed out (BaseRollup 24, HotPacking 12, SealedConsolidation 11) against
    /// a 900s deadline each. That is 42,300 of 57,600 available worker-seconds —
    /// 73% of ALL maintenance capacity — spent committing nothing, while
    /// ~40,000 pending BaseRollup units could not get a slot. Over 180s
    /// `tasks_complete` rose by 2 with `tasks_running` pinned at 16, and the one
    /// rollup that completed took 812ms.
    ///
    /// Neither existing lever reaches this. `abandon_running`'s backoff decides
    /// how OFTEN a doomed unit runs, never what it costs when it does; and its
    /// bisection makes the total worse, because halving a slice cannot halve a
    /// per-file cost — measured at ~3.2s per parquet file the same day — it only
    /// doubles the number of units paying it. #176's cap bounds occupancy, which
    /// is the right lever, but exempts BaseRollup, and BaseRollup is half the
    /// timeouts: the exemption assumed rollup units advance coverage, which is
    /// true only of one that COMPLETES.
    pub fn claim_next(&mut self, operation: Operation, now_micros: i64, allow_quarantined: bool) -> Option<MaintenanceTask> {
        // The winning scheduling class, as a streaming minimum.
        //
        // This used to call `fair_ready_tasks`, which builds a BTreeMap of every
        // ready task plus a fully materialised Vec, and then took `.find()` —
        // discarding all of it to keep one `(class, order_key)` tuple. That is
        // O(n log n) and two large allocations PER CLAIM, on every worker.
        // Tolerable at 18k tasks; at 128k (2026-08-17, after the rollup backfill
        // queued real history) it started showing up as live-frontier lag, and
        // it scales the wrong way for the 10x-projects target.
        //
        // The minimum over the same predicate is identical: groups are ordered
        // by `(class, operation.priority(), order_key)` and the operation is
        // fixed here, so the first match `find` returned was exactly the
        // smallest `(class, order_key)` among eligible tasks for this operation.
        //
        // `dependencies_complete` is itself a scan, so it is evaluated ONLY when
        // a task would actually improve the current best — otherwise this would
        // be quadratic for `DerivedRollup`.
        //
        // One claim in three is also RESERVED for sealed work. Class is strict
        // priority and ingest generates live-frontier work continuously, so
        // without a reservation class 1 never runs at all. Prod 2026-08-17, over
        // 278 consecutive task starts: every one was today or yesterday, not a
        // single sealed day. Rollup coverage for a live tenant stayed pinned at
        // two days for hours while the frontier was perfectly healthy
        // (`eligible_watermark_lag_seconds` 0) — and a 7d/14d/30d query needs
        // exactly the sealed days that never ran. Falls back to any class when
        // there is no sealed work, so quiet history never idles a worker.
        self.claim_tick = self.claim_tick.wrapping_add(1);
        // Every OTHER claim, not every third. Measured after the reservation
        // shipped: sealed work went from 0 of 278 task starts to 8 of 131
        // (6.1%), far short of the intended third, because a sealed turn falls
        // back to any class whenever the operation being claimed has no eligible
        // sealed task — and rollup work sits behind its own dedup. Raising the
        // share is the direct lever on how fast coverage walks backward.
        // Three claims in four, not one in two. Measured 2026-08-17 with the
        // per-operation gauges (#120): of 127,798 pending tasks, 118,794 are
        // sealed AND eligible right now — including 39,072 eligible BaseRollup —
        // while only ~9,000 are frontier. Yet the frontier was taking ~90% of
        // claims, because class 0 is strict priority and ingest regenerates it
        // continuously. A one-in-two reservation delivered ~10% sealed starts in
        // practice, so rollup coverage sat frozen at 2026-08-11 for hours with
        // 39k claimable rollup tasks queued behind a much smaller frontier.
        //
        // The frontier is small in volume, so one claim in four still clears it;
        // if it stops keeping up, `eligible_watermark_lag_seconds` says so and
        // this is the dial to turn back.
        // Back to one in two after an OOM. Prod 2026-08-17 09:39:02:
        //
        //   maintenance-wor invoked oom-killer
        //   Killed process (timefusion) anon-rss: 124,921,780 kB  (124.9 GB)
        //
        // RSS was 11.9 GB two minutes earlier, so this was a fan-in spike, not
        // drift. Three-in-four routes far more SEALED partitions through the
        // heavy path at once, and historical partitions are much larger than
        // frontier slices — so the same permit count admits far more bytes.
        // permits=10 + jobs=16 had run 1.5h clean before this landed; the share
        // is the variable that changed immediately before the kill.
        //
        // One in two ran ~6h without an OOM. Raising it again needs the per-sort
        // budget cut to pay for it (the documented 2026-07-04 pairing), not just
        // a bigger share.
        // ...and halve it while the frontier is behind. The comment
        // above names `eligible_watermark_lag_seconds` as the dial to turn when
        // the frontier stops keeping up; this turns it automatically, in both
        // directions, instead of waiting for someone to notice.
        //
        // Frontier lag is not just staleness, it is a per-query cost:
        // `raw_tail_duration_secs` is `FINALIZATION_DELAY + lag`, and EVERY
        // hybrid rollup query scans that tail. Prod 2026-08-17 sat at 62
        // minutes of raw tail, so each query paid an hour of raw scan no matter
        // how good the rollup coverage was.
        //
        // Safe in the direction that matters: the 3-in-4 sealed share
        // OOM-killed prod at 124.9 GB because sealed partitions are far larger
        // than frontier slices. This moves share AWAY from sealed, so it cannot
        // reproduce that.
        //
        // HALVED to one-in-four, not withdrawn. The comment above calls the
        // frontier "small in volume", but `LIVE_FRONTIER_WINDOW_MICROS` is 24
        // HOURS — a full day of ten-minute slices across every stream, which on
        // prod 2026-08-17 was ~7.8 units/min of creation against 3.8 claimed,
        // hence a lag climbing past 98 minutes. Withdrawing the reservation
        // entirely would let the frontier catch up fastest but would stop the
        // sealed backfill that builds 30d coverage, and coverage is the other
        // half of the same goal. One in four leaves both moving.
        //
        // This is not merely a freshness knob: `rollup_min_contiguous_days`
        // counts back from YESTERDAY, so a frontier that never finishes today
        // guarantees tomorrow's yesterday is holed, and the coverage metric can
        // never leave zero. Frontier health is a PREREQUISITE for the coverage
        // goal, not a competitor to it.
        //
        // DerivedRollup always takes the sealed turn, because for THIS operation
        // the reservation's premise does not hold. The frontier mints derived
        // work at `DERIVED_SLICE_MICROS` — one unit per stream per HOUR, ~24 a
        // day — against 144 Dedup plus 144 BaseRollup for the same stream-day.
        // Derived is roughly 3% of frontier creation, so preferring sealed for
        // it cannot meaningfully starve the frontier, which is the entire reason
        // the share is throttled.
        //
        // What it does buy is the goal. The derived tier is the one 14d/30d
        // dashboards read, and it is the coverage gap: prod 2026-08-19 00:10 UTC
        // had the 1m base tier 33 days deep against a 1h tier at 9-17, with 387
        // historical day-wide derived units freshly unblocked by #186/#188 — and
        // EVERY derived unit claimed in the following 12 minutes was a one-hour
        // frontier slice for today or yesterday. Class is strict priority, the
        // frontier regenerates continuously, so without a reservation the
        // historical units never run at all.
        //
        // Costs nothing when there is no sealed derived work: the caller already
        // falls back to any class.
        let sealed_turn = operation == Operation::DerivedRollup
            || if self.frontier_lag_secs.load(std::sync::atomic::Ordering::Relaxed) > FRONTIER_LAG_BUDGET_SECS {
                self.claim_tick.is_multiple_of(4)
            } else {
                self.claim_tick.is_multiple_of(2)
            };
        let claimable = |task: &MaintenanceTask| {
            task.key.operation == operation
                && matches!(task.state, TaskState::Pending | TaskState::Retry)
                && task.deadline_micros <= now_micros
                && (allow_quarantined || !Self::is_quarantined(task))
        };
        // `(class, hole_rank, width, recency)`. Class still leads, so the live
        // frontier keeps strict priority over sealed work. `hole_rank` orders
        // WITHIN a class: a cell whose tier output is missing outranks one that
        // already has output and is merely being re-derived.
        //
        // Without it, sealed rollup work is strictly newest-first, and recent
        // days are re-invalidated continuously by ongoing publication — so the
        // claim never walks back far enough to reach an old hole. Prod
        // 2026-08-19 09:00: `94c5dc1f`'s 1h tier jumped 2026-07-31 -> 08-14 for
        // a second day running, while day-wide derived units for 08-17 were
        // claimed repeatedly. Newest-first is right for FRESHNESS and wrong for
        // CONTIGUITY, and 30 contiguous days is a contiguity goal.
        // DAMAGE leads its class, ahead of `starved`, because the starvation
        // window is a FRESHNESS heuristic and damage is not a freshness
        // question. `starved` grades AGE and is compared first: every untagged
        // file left on prod 2026-08-23 was 32 to 37 days old, which under the
        // old hard horizon put it below the entire ~12,000-unit backfill queue,
        // unreachable whatever `hole_rank` said.
        //
        // Bounded and self-terminating: the set comes from files that exist and
        // empties as they are retired.
        // One claim in four is RESERVED for work inside the window dashboards
        // read, chosen WITHOUT reference to `starved`.
        //
        // `starved` is `u8::MAX` when a task is not starved and smaller (better)
        // the longer it has waited, so any starved task outranks any non-starved
        // one. With `STARVATION_MICROS` at 3 days, days 4-14 of every dashboard
        // window sit in that lane and are outranked by months of history starved
        // by a wider margin — so capacity goes to data nobody queries before
        // reaching the window everybody does. Prod 2026-09-01: `pending_dedup`
        // ~2,250 while a 1.7M-row/day project had 0 of 8 sampled dates certified,
        // and certification cannot grant until those dates are deduped.
        //
        // Raising `STARVATION_MICROS` is the WRONG fix and was refuted locally
        // (9 test failures) — it evicts the window from the privileged lane
        // instead of protecting it. Reserving a SHARE is the same shape the
        // sealed reservation above already uses, and it is bounded: three claims
        // in four still go to the existing order, so the backlog keeps draining.
        //
        // Memory-safe by construction, unlike the sealed share that OOM-killed
        // prod at 124.9 GB in 2026-08-17: that moved capacity TOWARD large
        // historical partitions, while this moves it toward RECENT ones, which
        // are smaller. Same permit count therefore admits fewer bytes, not more.
        // An ODD residue, so it can never collide with a sealed turn. Sealed
        // fires on multiples of 2 (or 4 when the frontier is behind), so taking
        // `% 4 == 0` would have PREEMPTED it rather than sharing with it — the
        // local suite caught exactly that (`sealed_work_gets_claims_while_the_
        // frontier_is_busy`, `the_sealed_reservation_yields_while_the_frontier_
        // is_behind`). Ticks 3, 7, 11 ... are odd and therefore never sealed
        // turns, so both reservations keep their guarantees.
        //
        // Residue 3 rather than 1 so the reservation never takes the FIRST
        // claims: `claim_tick` is incremented before use, so `% 4 == 1` fired
        // immediately and changed what a freshly-built journal hands out — which
        // is what every rank-ordering test asserts, and what an operator reads
        // when debugging a stalled queue by hand.
        let window_turn = self.claim_tick % 4 == 3;
        let rank = |journal: &Self, task: &MaintenanceTask| -> Rank { journal.rank(task, now_micros) };
        let best_class = |journal: &Self, sealed_only: bool, window_only: bool| -> Option<Rank> {
            let mut class: Option<Rank> = None;
            for task in journal.snapshot.tasks.iter().filter(|task| {
                claimable(task)
                    && !(sealed_only && is_frontier_task(task, now_micros))
                    && !(window_only && now_micros.saturating_sub(task.key.slice.end_micros) > QUERY_WINDOW_MICROS)
            }) {
                let candidate = rank(journal, task);
                if class.is_none_or(|best| candidate < best) && journal.dependencies_complete(task) {
                    class = Some(candidate);
                }
            }
            class
        };
        // Falls through to the normal order when the window is already clean, so
        // a quiet window never idles a worker.
        let class = if window_turn {
            best_class(self, false, true).or_else(|| best_class(self, sealed_turn, false))
        } else if sealed_turn {
            best_class(self, true, false).or_else(|| best_class(self, false, false))
        } else {
            best_class(self, false, false)
        }?;
        let cursor = self.fair_cursors.get(&operation).map(String::as_str).unwrap_or("");
        let mut fallback: Option<&MaintenanceTask> = None;
        let mut next: Option<&MaintenanceTask> = None;
        for task in self.snapshot.tasks.iter().filter(|task| claimable(task) && rank(self, task) == class && self.dependencies_complete(task)) {
            if fallback.is_none_or(|current| {
                (&task.key.project_id, task.deadline_micros, &task.key) < (&current.key.project_id, current.deadline_micros, &current.key)
            }) {
                fallback = Some(task);
            }
            if task.key.project_id.as_str() > cursor
                && next.is_none_or(|current| {
                    (&task.key.project_id, task.deadline_micros, &task.key) < (&current.key.project_id, current.deadline_micros, &current.key)
                })
            {
                next = Some(task);
            }
        }
        let selected = next.or(fallback)?;
        let key = selected.key.clone();
        self.fair_cursors.insert(operation, key.project_id.clone());
        self.mark_running(&key);
        self.task_indices.get(&key).map(|index| self.snapshot.tasks[*index].clone())
    }

    fn dependencies_complete(&self, task: &MaintenanceTask) -> bool {
        let required = match task.key.operation {
            // Base publication performs its own bounded complete-key/tiebreak
            // dedup before aggregation. Physical source consolidation is
            // independent debt and must not block exact rollup coverage.
            Operation::BaseRollup => None,
            Operation::DerivedRollup => Some(Operation::BaseRollup),
            _ => None,
        };
        // Proven from real tier coverage, which is strictly better evidence than
        // the journal's own record of who built what. See the field's comment.
        //
        // The day-keyed set is checked first and is the one that actually works:
        // the per-task flag has to be set on exactly the right `TaskKey`, and
        // prod measured 674 of 674 pending derived tasks without it. Keying the
        // fact on the DAY it is a fact about cannot miss a task.
        if task.base_tier_present {
            return true;
        }
        if let Some(date) = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map(|time| time.date_naive().to_string())
            && self.base_tier_ready.contains(&(task.key.source.clone(), task.key.project_id.clone(), date))
        {
            return true;
        }
        required.is_none_or(|required| {
            let mut intervals = self
                .snapshot
                .tasks
                .iter()
                .filter(|candidate| {
                    candidate.key.source == task.key.source
                        && candidate.key.project_id == task.key.project_id
                        && candidate.key.operation == required
                        && candidate.state == TaskState::Complete
                        && task.key.slice.overlaps(candidate.key.slice.start_micros, candidate.key.slice.end_micros)
                })
                .map(|candidate| candidate.key.slice)
                .collect::<Vec<_>>();
            intervals.sort_unstable();
            let mut covered_through = task.key.slice.start_micros;
            for interval in intervals {
                if interval.start_micros > covered_through {
                    break;
                }
                covered_through = covered_through.max(interval.end_micros);
                if covered_through >= task.key.slice.end_micros {
                    return true;
                }
            }
            false
        })
    }

    pub fn attempts(&self, key: &TaskKey) -> u32 {
        self.task_indices.get(key).and_then(|index| self.snapshot.tasks.get(*index)).map_or(0, |task| task.attempts)
    }

    pub fn retry(&mut self, key: &TaskKey, reason: String, not_before_micros: i64) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Retry;
        tracing::debug!(?key, %reason, attempts = task.attempts, not_before_micros, "maintenance task retry");
        crate::observability::set_maintenance_retry_reason(&reason);
        crate::observability::count_maintenance_retry(&format!("{:?}", key.operation), &reason);
        task.retry_reason = Some(reason);
        task.deadline_micros = not_before_micros;
        self.dirty_tasks.insert(key.clone());
        true
    }

    pub fn complete(&mut self, key: &TaskKey) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Complete;
        task.retry_reason = None;
        self.dirty_tasks.insert(key.clone());
        true
    }

    pub fn publish(&mut self, key: &TaskKey, publication: Publication) -> bool {
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let task = &mut self.snapshot.tasks[index];
        task.state = TaskState::Complete;
        task.retry_reason = None;
        task.publication = Some(publication);
        self.dirty_tasks.insert(key.clone());
        true
    }

    /// Supersede an oversized time unit with smaller durable children. The
    /// parent remains as a completed audit record. Hash shards intentionally
    /// stay inside a one-minute task because `TaskKey` identifies logical
    /// slice work; the worker merges all shard states before publication.
    /// A worker gave the task back without finishing it — an error, or a
    /// deadline it could not meet.
    ///
    /// Once is a blip: back off and retry it whole. Twice says the slice itself
    /// does not fit its deadline, and requeueing it unchanged is an infinite
    /// loop that holds a worker for the full deadline every pass and never
    /// produces anything — the failure this codebase has hit repeatedly, most
    /// recently as five Dedup timeouts in twelve minutes permanently occupying
    /// ~2 of 16 workers while the rollup horizon they starved sat days behind.
    /// So bisect instead, and let the halves face the same test.
    ///
    /// Byte-based splitting does not cover this. It fires on decoded bytes,
    /// while what overran was WALL TIME — a day-sized slice with modest bytes
    /// still pays an object-store round trip per file.
    ///
    /// `failure` is the worker's own account of what went wrong, when it had
    /// one — a timeout drops the future and carries nothing, and that unnamed
    /// case is exactly the one bisection exists for. Positive evidence of a
    /// DETERMINISTIC plan error is the one thing that suppresses it.
    pub fn abandon_running(&mut self, key: &TaskKey, now_micros: i64, failure: Option<&str>) {
        if let Some(error) = failure.filter(|failure| is_schema_failure(failure)) {
            self.park_schema_failure(key, now_micros, error);
            return;
        }
        let attempts = self.task_indices.get(key).and_then(|index| self.snapshot.tasks.get(*index)).map_or(1, |task| task.attempts);
        // `MAX_DECODED_BYTES + 1` is the smallest input that makes
        // `byte_bounded_units` bisect, and a bisect is one halving: this asks
        // for one split, never a shred down to the minimum slice.
        if attempts >= 2 && self.split_time_task(key, MAX_DECODED_BYTES.saturating_add(1), None) {
            return;
        }
        // Floored at this operation's OWN deadline. A unit that cannot be split
        // — repair is the standing case, since its cost is the file it rewrites
        // and time-bisection cannot shrink a file set — otherwise burns the full
        // deadline, waits out a backoff that tops out at 256s, and burns it
        // again: a ~78% duty cycle on a worker, forever, for a unit that has
        // never once produced anything.
        //
        // Measured on prod 2026-08-18: 7 Repair units timed out at 900s inside a
        // 15-minute window, which is 6,300 of the 14,400 available slot-seconds
        // — 44% of ALL maintenance capacity, spent on units that complete
        // nothing. Rollup coverage could not advance behind that no matter how
        // the scheduling cycle was weighted.
        let backoff_micros = i64::try_from((1u64 << attempts.min(8)).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
        // The floor applies only after a REPEAT, for the same reason the split
        // above does: a FairSpillPool can squeeze out a perfectly sized unit that
        // would succeed untouched next pass, and making that unit wait a full
        // deadline would be a 15-minute penalty for someone else's memory spike.
        // A unit that has now failed twice is the one that is actually oversized.
        let delay_micros = if attempts >= 2 {
            let floor_micros = i64::try_from(operation_deadline_secs(key.operation).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            backoff_micros.max(floor_micros)
        } else {
            backoff_micros
        };
        self.retry(key, Self::WORKER_FAILURE_REASON.to_owned(), now_micros.saturating_add(delay_micros));
    }

    /// A unit that failed because its input did not FIT will fail identically
    /// on every retry — `retry` re-runs the same slice at the same size, so the
    /// backoff only decides how slowly it never finishes. This is
    /// [`Self::abandon_running`]'s argument, and byte-splitting answers it even
    /// more directly here: what overran was BYTES, which is exactly what
    /// `byte_bounded_units` divides.
    ///
    /// Still tolerate one: the maintenance pool is a FairSpillPool, so a unit
    /// can be squeezed out by whatever else happened to be running and succeed
    /// untouched next pass. Only a repeat says the slice itself is too big.
    ///
    /// Prod 2026-08-17 held ~5 units in this loop, each burning a worker and
    /// 50-80s of object-store work per pass, and each guarding a partition that
    /// stayed uncertified — which keeps `DedupExec` in every query plan over it.
    pub fn retry_or_split(&mut self, key: &TaskKey, reason: String, when_micros: i64, attempts: u32) {
        // A reason that names a missing field is the opposite verdict: the
        // input does not FIT nothing — it cannot be PLANNED, at any width.
        if is_schema_failure(&reason) {
            self.park_schema_failure(key, crate::support::now_micros(), &reason);
            return;
        }
        // Price the guard on the unit's OWN estimate, not a bare synthetic.
        //
        // This passed `MAX_DECODED_BYTES + 1`, which made `split_sheds_enough`
        // compare a CONSTANT against the parent's real measured bytes: against
        // any parent above ~683 MiB, 512 MiB always satisfies
        // `observed * 4 < parent * 3`, so the shed test passed at EVERY level
        // and the lineage bisected to `MIN_SLICE_MICROS`. The guard was
        // structurally blind here — it cannot compare a measurement to a
        // constant. Prod 2026-09-03: 147 live units at or below the 60 s floor
        // (all `base_rollup`, one whale) and 8,595 completed there.
        //
        // `max(estimate, MAX + 1)` because the synthetic does TWO jobs and only
        // one was wrong. It also forces `byte_bounded_units` past its
        // `<= MAX_DECODED_BYTES` early return, so a unit that OVERRAN ITS
        // DEADLINE still splits though its estimate claims it fits
        // (`a_unit_that_overruns_its_deadline_twice_is_bisected`). Taking the
        // max keeps that: a large real estimate reaches the guard intact, a
        // small or unpriced one falls back to the synthetic and splits as before.
        let estimate = self.task_indices.get(key).map_or(0, |index| self.snapshot.tasks[*index].estimated_decoded_bytes);
        let observed = estimate.max(MAX_DECODED_BYTES.saturating_add(1));
        if attempts >= 2 && is_capacity_failure(&reason) && self.split_time_task(key, observed, None) {
            return;
        }
        // Split refused (already at minimum width, or would hash-shard) on a
        // REPEATED capacity failure: the caller's delay is tuned for transient
        // contention (1s for admission), which turns an unfittable unit into a
        // hot loop that increments attempts every second. Escalate the delay
        // with the evidence instead — same exponential abandon_running uses.
        let when_micros = if attempts >= 2 && is_capacity_failure(&reason) {
            let backoff = i64::try_from((1u64 << attempts.min(8)).saturating_mul(1_000_000)).unwrap_or(i64::MAX);
            when_micros.max(crate::support::now_micros().saturating_add(backoff))
        } else {
            when_micros
        };
        self.retry(key, reason, when_micros);
    }

    pub fn split_time_task(&mut self, key: &TaskKey, observed_bytes: u64, input: Option<InputFootprint>) -> bool {
        // A Repair unit's cost is the FILE it rewrites, and time-bisection
        // cannot shrink a file. `coordinator_compaction_files` hands Repair
        // `take(1)` of a whole file whatever the slice width is, so every child
        // of a split fights over the same file and pays the same cost — the
        // split only multiplies the number of units paying it. Prod 2026-08-31:
        // the 432 active repair units had been bisected to 12h/0.75h/0.38h
        // widths over four dates, all in `worker_error`, one at `attempts=100`.
        if key.operation == Operation::Repair {
            crate::observability::maintenance_stats().split_declined_at_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        let Some(index) = self.task_indices.get(key).copied() else { return false };
        let mut parent = self.snapshot.tasks[index].clone();
        // Children of a split read the parent's files — every one of them, since
        // a row group cannot be pruned below. Stamping them here is what lets
        // `coarsen_to_width` put them back together later for the price of one
        // scan instead of N.
        parent.input = input.or(parent.input);
        // Bisecting halves TIME. It only halves BYTES while the slice is wide
        // enough that narrowing it drops whole files and row groups; a slice
        // reads at least one row group of every file it still overlaps, so below
        // some width the cost stops falling and only the model keeps shrinking.
        //
        // `byte_bounded_units` cannot see that — it prices children by time
        // share — so a split always "fits" on paper and the truth arrives one
        // preflight later, over budget, splitting again. That cycle is what
        // minted 3,455 units for one (project, tier, day).
        //
        // The parent's measurement is the evidence: if this unit came back
        // costing most of what its parent cost, the floor has been reached and
        // there is nothing left for bisection to win. Decline, and let the unit
        // RUN — the runner already hash-shards internally at any width
        // (`database/maintain.rs:1623`), which bounds memory without minting a
        // single journal unit.
        if !split_sheds_enough(parent.parent_measured_bytes, observed_bytes) {
            crate::observability::maintenance_stats().split_declined_at_floor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        let children = byte_bounded_units(&parent, observed_bytes);
        if children.len() <= 1 || children.iter().any(|child| child.hash_shards > 1) {
            // Counted for the same reason the floor decline is: a unit that can
            // neither finish nor shrink is invisible otherwise. See
            // `split_declined_no_width`.
            crate::observability::maintenance_stats().split_declined_no_width.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        }
        if let Some(index) = self.task_indices.get(key).copied() {
            let task = &mut self.snapshot.tasks[index];
            task.state = TaskState::Superseded;
            task.retry_reason = Some("split_into_smaller_slices".to_owned());
            self.dirty_tasks.insert(key.clone());
        }
        for mut child in children {
            // What the PARENT measured, not the child's modelled share — the
            // modelled share is the very number that cannot be trusted.
            child.parent_measured_bytes = Some(observed_bytes);
            child.state = TaskState::Pending;
            child.attempts = 0;
            child.retry_reason = None;
            child.publication = None;
            // A split narrows the WORK, not the priority. Without this the
            // children rank by their own 180s width and fall behind every
            // day-wide unit in history, so the day never completes and the
            // coverage hole it leaves is permanent.
            child.backfill_priority_micros = Some(parent.scheduling_width());
            self.upsert(child);
        }
        true
    }

    /// A process may die after selecting work. Running is not a durable lease;
    /// requeue it at boot so recovery produces redundant work, never a hole.
    pub fn requeue_running(&mut self, now_micros: i64) -> usize {
        let mut count = 0;
        for task in &mut self.snapshot.tasks {
            if task.state == TaskState::Running {
                task.state = TaskState::Retry;
                task.deadline_micros = now_micros;
                task.retry_reason = Some("coordinator_restart".to_owned());
                self.dirty_tasks.insert(task.key.clone());
                count += 1;
            }
        }
        count
    }

    pub fn tasks(&self) -> impl Iterator<Item = &MaintenanceTask> {
        self.snapshot.tasks.iter()
    }

    pub fn state(&self, key: &TaskKey) -> Option<TaskState> {
        self.task_indices.get(key).map(|index| self.snapshot.tasks[*index].state)
    }

    /// Files this unit's footprint says it reads, if it has one yet.
    ///
    /// `None` is the common case for a unit that has never been claimed —
    /// `record_input` writes the footprint at claim time — so a `None` here is
    /// information, not a gap: it says the scheduler was ordering this unit
    /// without knowing its debt.
    pub fn input_files(&self, key: &TaskKey) -> Option<u32> {
        self.task_indices.get(key).and_then(|index| self.snapshot.tasks[*index].input).map(|input| input.files)
    }

    pub fn rollup_slice_complete(&self, source: &str, project_id: &str, target: &str, slice: TimeSlice) -> bool {
        self.snapshot.tasks.iter().any(|task| {
            task.key.source == source
                && task.key.project_id == project_id
                && task.key.physical_table == target
                && task.key.slice == slice
                && matches!(task.key.operation, Operation::BaseRollup | Operation::DerivedRollup)
                && task.state == TaskState::Complete
        })
    }

    pub fn published_rollups(&self, source: &str, target: &str) -> Vec<(TaskKey, Publication)> {
        self.snapshot
            .tasks
            .iter()
            .filter(|task| task.state == TaskState::Complete && task.key.source == source && task.key.physical_table == target)
            .filter_map(|task| task.publication.clone().map(|publication| (task.key.clone(), publication)))
            .collect()
    }

    /// Rows this project has PUBLISHED into `target` over slices overlapping
    /// `[start, end)`.
    ///
    /// The comparator for an empty publication. Day-level `source_rows` cannot
    /// serve: it is keyed on (project, date) while a unit is an hour, so a
    /// genuinely empty hour of a busy day reads as non-empty. Joining the base
    /// tier's own publications over the SAME slice separates the two — offline
    /// against the 2026-08-28 prod journal it split 285 empty derived
    /// completions into 276 with base rows available and 9 legitimately empty.
    pub fn published_rows_overlapping(&self, project_id: &str, target: &str, start: i64, end: i64) -> u64 {
        self.snapshot
            .tasks
            .iter()
            .filter(|task| {
                task.key.project_id == project_id && task.key.physical_table == target && task.key.slice.start_micros < end && task.key.slice.end_micros > start
            })
            .filter_map(|task| task.publication.as_ref().map(|publication| publication.rows))
            .sum()
    }

    /// Reopen the COMPLETE derived cells built over a base range that has just
    /// been republished. Returns how many were reopened.
    ///
    /// A derived cell's input is the base tier, but its witness is the RAW
    /// partition — which agrees forever on a sealed day. So when the base is
    /// rebuilt underneath it, the cell keeps serving a faithful aggregate of a
    /// base that no longer exists, and never revisits it because its unit is
    /// `Complete`. Prod 2026-08-28: `87576849`/08-01's 1h cell was written
    /// 09:44:32, its 1m base was rebuilt 09:49:13-14:46:12, and the cell reads
    /// +70.6% over that base to this day.
    ///
    /// Deliberately NOT `invalidate`: that mints `Dedup` work over the same
    /// range, and dedup is the largest backlog in the queue (3,592 pending on
    /// 2026-08-28). Rebuilding a rollup tier says nothing about whether the RAW
    /// partition needs deduplicating.
    ///
    /// Only `Complete` cells are touched, so the live frontier — where the
    /// derived task is already Pending — costs nothing, and the new work is
    /// exactly the republication case. It cannot loop: this only ever moves
    /// Complete to Pending and mints no task, and a derived publish does not
    /// call it.
    pub fn reopen_derived_over(&mut self, project_id: &str, rollup_table: &str, start_micros: i64, end_micros: i64) -> usize {
        let mut reopened = 0;
        for task in &mut self.snapshot.tasks {
            if task.state != TaskState::Complete
                || task.key.operation != Operation::DerivedRollup
                || task.key.project_id != project_id
                || task.key.physical_table != rollup_table
                || task.key.slice.start_micros >= end_micros
                || task.key.slice.end_micros <= start_micros
            {
                continue;
            }
            task.state = TaskState::Pending;
            task.retry_reason = None;
            // Dropped with the state: `Publication` is what coverage is recovered
            // from at boot, so leaving it would have the next process re-adopt
            // the very cell this reopen exists to replace.
            task.publication = None;
            self.dirty_tasks.insert(task.key.clone());
            reopened += 1;
        }
        reopened
    }

    pub fn source_cursor(&self, source: &str) -> Option<u64> {
        self.snapshot.source_cursors.get(source).copied()
    }

    pub fn set_source_cursor(&mut self, source: String, delta_version: u64) {
        let cursor = self.snapshot.source_cursors.entry(source.clone()).or_default();
        if delta_version > *cursor {
            *cursor = delta_version;
            self.dirty_cursors.insert(source);
        }
    }

    /// Append the pending journal records and fsync them.
    ///
    /// Called on every claim, complete and retry — 46 sites, all synchronous,
    /// all reached from tasks running on the shared runtime. The `fsync` here
    /// is the blocking syscall that `block.journal_hold.max_ms = 2,380` on prod
    /// was measuring (2026-08-24), and a held worker stalls every unrelated task
    /// queued behind it, which is what `SELECT 1` costing seconds looks like
    /// from the client. The IO therefore runs through
    /// `without_blocking_the_worker`; the mutex is still held across it, so
    /// durability ordering is unchanged.
    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.wal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !self.dirty_tasks.is_empty() || !self.dirty_cursors.is_empty() || !self.removed_tasks.is_empty() {
            let mut wal = OpenOptions::new().create(true).append(true).open(&self.wal_path)?;
            let mut records = Vec::new();
            for key in self.dirty_tasks.drain() {
                if is_derived_operation(key.operation) {
                    continue;
                }
                if let Some(index) = self.task_indices.get(&key).copied() {
                    let task = &self.snapshot.tasks[index];
                    serde_json::to_writer(&mut records, &JournalRecord::Task(task.clone()))?;
                    records.push(b'\n');
                }
            }
            for source in self.dirty_cursors.drain() {
                if let Some(delta_version) = self.snapshot.source_cursors.get(&source).copied() {
                    serde_json::to_writer(&mut records, &JournalRecord::SourceCursor { source, delta_version })?;
                    records.push(b'\n');
                }
            }
            // AFTER the upserts, so a key removed and re-created in the same
            // window keeps the re-creation. `retain_tasks` already drops such a
            // key from `removed_tasks` when it reappears, but ordering makes the
            // record stream correct on its own terms rather than by convention.
            for key in self.removed_tasks.drain() {
                if is_derived_operation(key.operation) {
                    continue;
                }
                serde_json::to_writer(&mut records, &JournalRecord::Removed(key))?;
                records.push(b'\n');
            }
            crate::support::without_blocking_the_worker(|| {
                wal.write_all(&records)?;
                wal.sync_all()
            })?;
        }
        if fs::metadata(&self.wal_path).is_ok_and(|metadata| metadata.len() >= JOURNAL_COMPACT_BYTES) {
            self.compact()?;
        }
        self.publish_statistics();
        Ok(())
    }

    /// Rewrite the authoritative snapshot even when the WAL is below its
    /// normal size threshold. Migrations that remove tasks cannot represent
    /// those deletions as append-only WAL records, so they must force this
    /// compaction before startup continues.
    pub fn compact(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Derived work is left out of the authoritative snapshot too, so a
        // reload starts with none of it and `plan_compaction_debt` re-derives
        // exactly what storage says is needed within its next 60 s pass.
        let durable = Snapshot {
            version: self.snapshot.version,
            tasks: self.snapshot.tasks.iter().filter(|task| !is_derived_operation(task.key.operation)).cloned().collect(),
            source_cursors: self.snapshot.source_cursors.clone(),
        };
        // Serialize AND write off the worker: the snapshot is every live task
        // (thousands on prod), so the `to_vec` is as costly as the fsync that
        // follows it, and both hold the journal mutex.
        crate::support::without_blocking_the_worker(|| -> anyhow::Result<()> {
            let bytes = serde_json::to_vec(&durable)?;
            crate::write::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
            let wal = OpenOptions::new().create(true).write(true).truncate(true).open(&self.wal_path)?;
            wal.sync_all()?;
            Ok(())
        })?;
        self.dirty_tasks.clear();
        self.removed_tasks.clear();
        self.dirty_cursors.clear();
        Ok(())
    }

    pub fn publish_statistics(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::observability::maintenance_stats();
        let mut counts = [0u64; 4];
        let mut backlog_bytes = 0u64;
        let mut sealed_debt_bytes = 0u64;
        let mut oldest_created = u64::MAX;
        let mut beyond_horizon = 0u64;
        let mut latest_frontier_rollup: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
        let mut per_operation = [0u64; 6];
        let (mut eligible_base_rollup, mut eligible_sealed) = (0u64, 0u64);
        let now_micros = crate::support::now_micros();
        for task in &self.snapshot.tasks {
            let index = match task.state {
                TaskState::Pending => 0,
                TaskState::Running => 1,
                TaskState::Retry => 2,
                TaskState::Complete => 3,
                TaskState::Superseded => 3,
            };
            counts[index] = counts[index].saturating_add(1);
            if !matches!(task.state, TaskState::Complete | TaskState::Superseded) {
                backlog_bytes = backlog_bytes.saturating_add(task.estimated_decoded_bytes);
                // Only work the scheduler still intends to do. Past
                // `STARVATION_HORIZON_MICROS` a task is deliberately abandoned
                // (see the constant), and an age gauge that counts abandoned
                // work is pinned red: prod read 83 days on 2026-08-23 and 85.6 on
                // 08-25, which is the seal time of one 2026-05-31 hygiene
                // partition and says nothing about queue health inside the goal
                // window.
                //
                // Aged from `created_unix_ms`, which for an invalidation-minted
                // task is `observed_at` and can precede the slice end — so the
                // gauge is bounded by the horizon plus one slice width, not by
                // the horizon exactly.
                //
                // Counted instead, so the abandonment is sized rather than
                // hidden — a definition change that only made a number smaller
                // would be indistinguishable from the fix working.
                if now_micros.saturating_sub(task.key.slice.end_micros) > STARVATION_HORIZON_MICROS {
                    beyond_horizon = beyond_horizon.saturating_add(1);
                } else {
                    oldest_created = oldest_created.min(task.created_unix_ms);
                }
                if task.key.operation == Operation::SealedConsolidation {
                    sealed_debt_bytes = sealed_debt_bytes.saturating_add(task.estimated_decoded_bytes);
                }
            }
            // Per-operation split, plus what is actually claimable now. When
            // coverage stalls the question is always: is the rollup work
            // absent, present-but-not-eligible, or present-and-eligible but
            // out-competed? Only these three views together answer it.
            if !matches!(task.state, TaskState::Complete | TaskState::Superseded) {
                per_operation[task.key.operation as usize] = per_operation[task.key.operation as usize].saturating_add(1);
                if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
                    if task.key.operation == Operation::BaseRollup {
                        eligible_base_rollup = eligible_base_rollup.saturating_add(1);
                    }
                    if !is_frontier_task(task, now_micros) {
                        eligible_sealed = eligible_sealed.saturating_add(1);
                    }
                }
            }
            track_latest_frontier_rollup(&mut latest_frontier_rollup, task, now_micros);
        }
        stats.pending_dedup.store(per_operation[Operation::Dedup as usize], Relaxed);
        stats.pending_base_rollup.store(per_operation[Operation::BaseRollup as usize], Relaxed);
        stats.pending_derived_rollup.store(per_operation[Operation::DerivedRollup as usize], Relaxed);
        stats.pending_hot_packing.store(per_operation[Operation::HotPacking as usize], Relaxed);
        stats.pending_sealed_consolidation.store(per_operation[Operation::SealedConsolidation as usize], Relaxed);
        stats.pending_repair.store(per_operation[Operation::Repair as usize], Relaxed);
        stats.eligible_base_rollup.store(eligible_base_rollup, Relaxed);
        stats.eligible_sealed_total.store(eligible_sealed, Relaxed);
        stats.maintenance_tasks_pending.store(counts[0], Relaxed);
        stats.maintenance_tasks_running.store(counts[1], Relaxed);
        stats.maintenance_tasks_retry.store(counts[2], Relaxed);
        stats.maintenance_tasks_complete.store(counts[3], Relaxed);
        stats.maintenance_backlog_bytes.store(backlog_bytes, Relaxed);
        stats.sealed_compaction_debt_bytes.store(sealed_debt_bytes, Relaxed);
        let eligible_lag_secs = frontier_lag_secs(latest_frontier_rollup.values().copied(), now_micros);
        stats.maintenance_eligible_watermark_lag_secs.store(eligible_lag_secs, Relaxed);
        self.frontier_lag_secs.store(eligible_lag_secs, Relaxed);
        stats
            .maintenance_raw_tail_duration_secs
            .store(u64::try_from(FINALIZATION_DELAY_MICROS / 1_000_000).unwrap_or_default().saturating_add(eligible_lag_secs), Relaxed);
        let processed = stats.maintenance_processed_bytes.load(Relaxed);
        let mut sample = THROUGHPUT_SAMPLE.get_or_init(|| Mutex::new((now_micros, processed))).lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = now_micros.saturating_sub(sample.0);
        if elapsed >= 1_000_000 {
            let rate = processed.saturating_sub(sample.1).saturating_mul(1_000_000) / u64::try_from(elapsed).unwrap_or(u64::MAX).max(1);
            stats.maintenance_processed_bytes_per_sec.store(rate, Relaxed);
            *sample = (now_micros, processed);
        }
        let now = u64::try_from(now_micros.div_euclid(1_000)).unwrap_or_default();
        let oldest_age_secs = if oldest_created != u64::MAX { now.saturating_sub(oldest_created) / 1_000 } else { 0 };
        stats.maintenance_oldest_task_age_secs.store(oldest_age_secs, Relaxed);
        stats.maintenance_beyond_horizon_tasks.store(beyond_horizon, Relaxed);
    }
}

/// `(class, operation priority, -width, -recency)` -> project -> that project's
/// queue. Ordered so smaller tuples run first; see `scheduling_class`.
type ReadyGroups<'a> = BTreeMap<(u8, u8, u8, i64, i64, i64), HashMap<&'a str, VecDeque<&'a MaintenanceTask>>>;

/// Deadline ordering with round-robin selection among projects at the same
/// operation priority. This prevents one whale from consuming an entire pass.
pub fn fair_ready_tasks<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> Vec<&'a MaintenanceTask> {
    let mut groups: ReadyGroups<'_> = BTreeMap::new();
    for task in tasks {
        if matches!(task.state, TaskState::Pending | TaskState::Retry) && task.deadline_micros <= now_micros {
            let (class, starved, width_key, benefit_key, order_key) = scheduling_class(task, now_micros);
            groups
                .entry((class, starved, task.key.operation.priority(), width_key, benefit_key, order_key))
                .or_default()
                .entry(&task.key.project_id)
                .or_default()
                .push_back(task);
        }
    }
    let mut ready = Vec::new();
    for projects in groups.values_mut() {
        let mut names: Vec<_> = projects.keys().copied().collect();
        names.sort_unstable();
        loop {
            let mut progressed = false;
            for name in &names {
                if let Some(task) = projects.get_mut(name).and_then(VecDeque::pop_front) {
                    ready.push(task);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
    }
    ready
}

/// Keep the live finalized frontier ahead of historical debt. Within the
/// frontier, newest eligible slices run first so sustained backfill cannot make
/// the raw tail grow without bound; tasks in the same minute still rotate by
/// project in `claim_next`. Once the frontier is caught up, historical work
/// returns to oldest-deadline order.
///
/// This is intentionally based on event time, not mutation deadline. A late
/// correction to old data is a bounded historical hole; treating its recent
/// mutation as live-tail work would let backfill displace current coverage.
/// A sealed task that has waited this long outranks newer sealed work.
///
/// Newest-first is right for freshness and starves anything old, because newer
/// sealed days keep arriving and never let the head of the queue advance.
/// Prod 2026-08-19, from the compaction dashboard: 2026-08-13 sat at 167 files
/// for FOUR DAYS — the same count, the same four tenants — while six younger
/// sealed days converged past it. Six successors overtaking a partition is not a
/// queue draining slowly; it is a partition the order never reaches.
///
/// Oldest-first is not the answer either, and this codebase has already tried
/// it: `scheduling_class`'s own comment records 10 of 10 historical starts
/// landing on data months old while the last 30 days went untouched. So age
/// tasks instead — the standard fix for exactly this. Normal ordering stays
/// newest-first; anything that has waited past the threshold escalates ahead of
/// it, and the escalated set is finite, so it cannot swallow the budget.
/// How long past SEALING a partition may still carry debt before it is overdue.
///
/// This measures the data, not the task record (see `scheduling_class`), so the
/// value has to mean something about days rather than about queue latency. At 24
/// h every sealed day but yesterday qualified, and a flag everything sets is a
/// flag that sorts nothing — which is how 2026-08-13 stayed at exactly 167 files
/// through three ordering changes.
///
/// Three days leaves the window dashboards actually read (today via the live
/// frontier, plus the last two sealed days) ordered newest-first for freshness,
/// and treats everything behind it as backlog to be drained oldest-first for
/// contiguity.
///
/// 2026-09-01, REFUTED CHANGE — do not repeat it. Raising this to 15 days, to put
/// the measured 7d/14d dashboard windows back in newest-first order, is WRONG and
/// makes the problem worse. `starved` is `u8::MAX` for NON-starved work and
/// smaller (better) the longer something has waited, so **any starved task
/// outranks any non-starved task**. Raising the threshold does not protect the
/// window — it EVICTS the window from the privileged lane, so it loses to old
/// history by more. The local suite caught it: 9 failures, including
/// `damage_outranks_work_inside_the_starvation_window` and
/// `sealed_work_ages_out_of_starvation_without_becoming_oldest_first`.
///
/// The real defect is still real (see
/// `docs/plans/2026-09-01-certification-coverage.md`): months-old history
/// outranks the dates dashboards read. But the lever is NOT this threshold — it
/// is bounding how much of the claim budget the starved lane may take, the way
/// `claim_next` already reserves one claim in two for sealed work.
/// The claim-order tuple `claim_next` minimises: see `TaskJournal::rank`.
type Rank = (u8, u8, u8, u8, i64, i64, i64);

const STARVATION_MICROS: i64 = 3 * 24 * 60 * 60 * 1_000_000;
/// The window dashboards actually read, and therefore the window maintenance has
/// to keep clean. Measured, not assumed: the latency matrix covers 1h/6h/24h/7d
/// /14d and monoscope's charts are 7d/14d
/// (`docs/plans/2026-09-01-certification-coverage.md`).
const QUERY_WINDOW_MICROS: i64 = 14 * 24 * 60 * 60 * 1_000_000;
/// ...and an UPPER bound, because an escape valve everything fits through is
/// not an escape valve.
///
/// Ageing sealed hygiene from its seal time (e8645a6) is correct, and it made
/// every sealed partition older than a day qualify — at which point "starved"
/// stops discriminating and ordering falls back to newest-first, leaving
/// 2026-08-13 exactly where it was. Making everything starved is the same as
/// making nothing starved.
///
/// Beyond this bound a task stops gaining escalation from the WINDOW and starts
/// gaining it one step per further day instead — see the graded term in
/// `scheduling_class`. It was a hard cut-off until 2026-08-25, which is a
/// different thing entirely: past the bound a task ranked below every task
/// inside it, forever. The bound exists because plain oldest-first sent 10 of 10
/// historical starts to data months old while the last 30 days went untouched;
/// flattening the escalation across the goal window keeps that from recurring,
/// whereas cutting it off merely made the far tail unreachable.
///
/// 31 days, because the flat part of the escalation must cover the GOAL window
/// and no more.
/// At 45 it did not: prod 2026-08-19 had hygiene claiming 2026-07-17, 07-19 and
/// 07-20 — correctly oldest-first, and 30+ days back, so outside the window any
/// 30d panel reads. The source table spans 2023-01-01 to 2026-08-19 with 75 of
/// its 89 partition-days holding 10 or more files, so there is always older
/// debt to find; without a bound tied to the goal, oldest-first spends the whole
/// escalation there and the days a 30d query actually needs wait behind it.
///
/// Outside the window a partition is ABANDONED, and that is the intent — the
/// earlier claim here that it "is deprioritised, not abandoned" was wrong.
/// Nominally it still ranks by newest-first inside its class, but that class is
/// replenished continuously by ingest and by the 60 s hygiene planner, so a
/// beyond-horizon task is reachable only when the 3-31 day band is empty of
/// eligible work of its operation. It never is: `pending_sealed_consolidation`
/// walked 76 -> 99 across 2026-08-25 while `out_of_policy_cells` held at 51 for
/// thirteen consecutive censuses.
///
/// Prod paid for that phrasing. `oldest_task_age_seconds` read 83 days on
/// 2026-08-23 and 85.6 on 2026-08-25 — the seal time of ONE 2026-05-31 hygiene
/// partition — and was read as a scheduling stall rather than as this
/// deliberate policy. `publish_statistics` now counts
/// beyond-horizon work separately for exactly that reason. See
/// `docs/plans/2026-08-25-oldest-task-tail.md`.
const STARVATION_HORIZON_MICROS: i64 = 31 * 24 * 60 * 60 * 1_000_000;

fn scheduling_class(task: &MaintenanceTask, now_micros: i64) -> (u8, u8, i64, i64, i64) {
    if is_frontier_task(task, now_micros) {
        // Smaller tuples run first. Negating makes the newest minute the most
        // urgent while keeping all projects in that minute deadline-equivalent.
        // Width is not a frontier concern — these are all one slice wide.
        (0, 0, 0, 0, -task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS))
    } else {
        // Newest slice first here too, for the same reason the dedup drain and
        // the rollup backfill are newest-first: recent days are what dashboards
        // read, and history is debt nobody is querying.
        //
        // This was `deadline_micros` ascending — oldest-FIRST — which is the
        // opposite. Prod 2026-08-17, over 84 task starts: 74 went to the live
        // frontier (correct), and every one of the other 10 landed on
        // 2026-08-01, 07-22, 07-16, 07-15, 06-29, 06-15, 06-02, 06-01, 05-28.
        // All of the historical capacity was spent on data months old while the
        // last 30 days — the window a 30d dashboard query actually needs
        // certified and rolled up — was never reached. Coverage sat at two days.
        //
        // Eligibility is still deadline-gated in `claim_next`
        // (`deadline_micros <= now`), so retry backoff is unaffected; this only
        // orders the tasks that are already runnable.
        //
        // WIDTH outranks recency, though. A day-sized unit comes from the
        // backfill planner and is the only kind that advances the horizon; a
        // ten-minute unit is what the live path mints, and a day that has just
        // sealed carries ~144 of them per project per tier. Newest-first alone
        // therefore spends every sealed claim on yesterday's leftovers before
        // reaching the day before it — and midnight mints another day's worth,
        // so the horizon never moves. Prod 2026-08-17, 65 rollup starts in 25
        // minutes: 46 on today, 18 on yesterday, ZERO older, while 7d/14d
        // queries were refused for want of exactly those older days.
        //
        // Newest-first still breaks ties, so among backfill units recent
        // history is still built first.
        //
        // Starved work leads, then width, then recency — see `STARVATION_MICROS`.
        //
        // A SEALED task's age is how long its DATA has been sealed, not how long
        // its record has existed. Three separate bugs came from the latter, and
        // all three are the same mistake — a task record's birthday is a
        // property of the process, and this system re-creates records constantly:
        //
        //   * `plan_compaction_debt` re-derives hygiene every 60 s, and since
        //     hygiene stopped being persisted it re-creates it on every restart,
        //     so `created_unix_ms` reset several times a day and the threshold
        //     was unreachable.
        //   * `coarsen_to_width` stamped each fused unit with `now`, so the
        //     day-wide unit it produced was permanently fresher than the narrow
        //     leftovers it did not fuse — coarsening starved its own output.
        //   * Correcting only NEW fusions left 255 already-fused day-wide Dedup
        //     units carrying a recent stamp, and prod claimed ZERO of them
        //     across 466 claims in 10 minutes while narrow sealed units ran.
        //     Certification needs a day-wide dedup unit, so it stayed at 0%.
        //
        // Seal time fixes all three at once and cannot regress the same way: it
        // is derived from the slice, so every task covering a day — narrow or
        // day-wide, freshly minted or fused an hour ago — reports the same age,
        // and `-width` then breaks the tie in favour of the day-wide unit, which
        // is exactly the desired order.
        let waited = now_micros.saturating_sub(task.key.slice.end_micros);
        // The horizon is a SLOPE, not a cliff. `starved` used to be 0 only inside
        // [3d, 31d], and it is compared before `hole`, `width` and `benefit` in a
        // strict-priority tuple — so a slice one day past the horizon lost to
        // everything inside it and was never compared on any other term. Prod
        // 2026-08-25: 1,237 units older than 31 days were permanently
        // unclaimable and `oldest_task_age_seconds` sat at 85 days, unable to
        // decrease; 40 minutes of `maintenance_task_started` showed ZERO claims
        // in 05-30..07-20. Nor could it heal: the 3-31d band refills every
        // midnight and its timed-out residue crosses the cliff (207 of the tail
        // at attempts=1 — tried inside the window, then aged out).
        //
        // So: below the floor is worst (fresh slices still settle before being
        // processed), the whole [floor, horizon] band ties, and each further DAY
        // past the horizon is one step better, saturating at 0 (~285 d). Ties
        // across the band are the point — every ordering the band has learned
        // (`hole`, `-width`, `benefit`, oldest-first) still decides inside it,
        // and only work the horizon had abandoned gains rank.
        //
        // DAYS, not raw age, for the reason `PRIORITY_BUCKET_MICROS` and
        // `BENEFIT_BUCKET_FILES` exist: `claim_next` matches the winning tuple
        // EXACTLY, so a continuous key makes one unit the sole winner of every
        // claim and defeats the per-project rotation in `fair_cursors`.
        let starved = if waited < STARVATION_MICROS {
            u8::MAX
        } else {
            (u8::MAX - 1).saturating_sub(u8::try_from(waited.saturating_sub(STARVATION_HORIZON_MICROS).max(0) / DAY_MICROS).unwrap_or(u8::MAX))
        };
        // Starved work drains OLDEST-first; fresh work stays newest-first.
        //
        // This is the half that was missing. Escalating a starved task above
        // newer sealed work does nothing if, once escalated, it is ordered by
        // recency again — it merely joins a larger set that is still drained
        // newest-first, which is why 2026-08-13 stayed at 167 files for five
        // days while seven younger sealed days converged past it.
        //
        // Oldest-first is right HERE and wrong in general: the starved set is
        // the backlog, and a backlog has to drain from its old end to restore
        // contiguity, while fresh work is what dashboards read and belongs
        // newest-first. `STARVATION_HORIZON_MICROS` is what keeps the two apart.
        let recency = task.key.slice.end_micros.div_euclid(PRIORITY_BUCKET_MICROS);
        // File hygiene ranks by BENEFIT, not by date. Every hygiene unit is
        // day-wide, so `-width` is constant among them and the tie-break was
        // pure recency — which says nothing about how much debt a claim
        // retires. Prod 2026-08-23: four sealed cells held ~850 removable files
        // in 4.9 GB, and capacity went to whichever happened to be newest.
        //
        // `files` is what the planner already counted to decide the partition
        // was out of policy, so this costs nothing to know. Zero (a journal
        // written before the field, or a unit from another path) orders LAST,
        // which is the safe reading of "benefit unknown".
        //
        // Starvation still leads the tuple, so this cannot strand a small cell
        // forever — it only decides the order among work nothing has escalated.
        // And benefit is self-limiting: a consolidated partition becomes
        // compliant and is retired, so the queue drains toward the small cells
        // by itself.
        let benefit = match task.key.operation {
            // BUCKETED, for the same reason recency is. `claim_next` matches the
            // winning tuple EXACTLY, so a raw file count makes one cell the sole
            // winner of every claim and defeats the per-project rotation
            // `fair_cursors` exists for. A band of 64 lets comparable cells tie
            // and rotate while a 200-file cell still outranks a 3-file one.
            Operation::SealedConsolidation | Operation::HotPacking | Operation::Repair => {
                -i64::from(task.input.map_or(0, |input| input.files) / BENEFIT_BUCKET_FILES)
            }
            _ => 0,
        };
        // Keyed on the AGE, not on `starved`: the graded term is 254 in-band, not
        // 0, so testing the rank value here would silently flip the whole backlog
        // to newest-first.
        // Width is read through `scheduling_width` so that SPLITTING a backfill
        // unit does not demote its children out of reach. See
        // `backfill_priority_micros`.
        (1, starved, -task.scheduling_width(), benefit, if waited >= STARVATION_MICROS { recency } else { -recency })
    }
}

/// Does this task mean a day's ROLLUP work is already queued?
///
/// The precondition of a backfill is "nothing has rolled this day up yet", and
/// the planner enqueues exactly `Dedup` + the rollup tiers — so only those may
/// veto it. Matching every non-complete task on the source instead let
/// unrelated file debt disqualify a day forever: prod 2026-08-17 had the
/// whale's Aug 15 `SealedConsolidation` on attempt 370, which alone kept that
/// day out of every backfill pass.
/// Does this task make its (project, date, tier) cell ineligible for re-planning?
///
/// Only work that will actually RUN may veto. `Superseded` is what
/// `split_time_task` leaves on a parent and is never claimable, so letting it
/// block meant every day that was ever split had its cell marked "already
/// queued" permanently — the planner could not re-admit it, and the tier stayed
/// holed forever. Its children are `Pending` and still veto on their own, so
/// dropping the parent's veto cannot cause duplicate work.
///
/// Prod 2026-08-20 01:35, `otel_logs_and_spans`: `cells_missing=210` against
/// `cells_wanted=0` with `derived_pending=22`. `claimability_census` counts only
/// Pending/Retry, so the superseded parents doing the blocking appeared in no
/// gauge at all. The 1h tier — what 30d dashboards read — held 22 days with its
/// oldest date frozen at 2026-07-25 while the 1m tier held 31.
pub fn blocks_rollup_backfill(task: &MaintenanceTask) -> bool {
    matches!(task.key.operation, Operation::Dedup | Operation::BaseRollup | Operation::DerivedRollup)
        && !matches!(task.state, TaskState::Complete | TaskState::Superseded)
}

fn is_live_frontier(slice: TimeSlice, now_micros: i64) -> bool {
    slice.end_micros >= now_micros.saturating_sub(LIVE_FRONTIER_WINDOW_MICROS) && slice.start_micros <= now_micros
}

/// Whether a TASK is live-frontier work, which is not the same question as
/// whether its slice is inside the frontier window.
///
/// `SealedConsolidation` never is, whatever its slice says. `plan_compaction_debt`
/// chooses the operation from the calendar — `date == today` mints HotPacking,
/// anything older mints SealedConsolidation — while `is_live_frontier` stays true
/// for a full `LIVE_FRONTIER_WINDOW_MICROS` (24 h) after a slice ENDS. Between
/// midnight and midnight+24h the two disagreed about the same date, and class is
/// STRICT priority, so yesterday's consolidation outranked every genuinely sealed
/// cell in the fleet for a whole day.
///
/// Prod 2026-08-24, a mature container over 68 minutes: **26 of 27**
/// SealedConsolidation claims went to 2026-08-23 while 48 out-of-policy cells got
/// none — one of them holding 238 small files that read exactly 238 at four
/// object-storage censuses spanning a day. `scheduling_class` promises "a cell
/// worth 200 files outranks one worth 3"; class silently overrode it.
///
/// HotPacking is deliberately NOT included: it is minted for today, so its slice
/// and its operation agree, and it is genuinely the frontier.
fn is_frontier_task(task: &MaintenanceTask, now_micros: i64) -> bool {
    task.key.operation != Operation::SealedConsolidation && is_live_frontier(task.key.slice, now_micros)
}

fn track_latest_frontier_rollup<'a>(latest: &mut HashMap<(&'a str, &'a str, &'a str), &'a MaintenanceTask>, task: &'a MaintenanceTask, now_micros: i64) {
    if task.key.operation != Operation::BaseRollup || !is_live_frontier(task.key.slice, now_micros) {
        return;
    }
    let stream = (task.key.source.as_str(), task.key.project_id.as_str(), task.key.physical_table.as_str());
    let active = u8::from(!matches!(task.state, TaskState::Complete | TaskState::Superseded));
    if latest.get(&stream).is_none_or(|current| {
        let current_active = u8::from(!matches!(current.state, TaskState::Complete | TaskState::Superseded));
        (task.key.slice.end_micros, active, task.deadline_micros) > (current.key.slice.end_micros, current_active, current.deadline_micros)
    }) {
        latest.insert(stream, task);
    }
}

fn frontier_lag_secs<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> u64 {
    tasks
        .into_iter()
        .filter(|task| !matches!(task.state, TaskState::Complete | TaskState::Superseded) && task.deadline_micros <= now_micros)
        .map(|task| u64::try_from(now_micros.saturating_sub(task.deadline_micros).div_euclid(1_000_000)).unwrap_or_default())
        .max()
        .unwrap_or_default()
}

/// Additional delay beyond the 15-minute quiet-period watermark for the live
/// rollup frontier. Historical holes remain visible through backlog and oldest
/// task age, but do not inflate the raw-tail gauge.
#[cfg(test)]
fn live_frontier_lag_secs<'a>(tasks: impl IntoIterator<Item = &'a MaintenanceTask>, now_micros: i64) -> u64 {
    let mut latest: HashMap<(&str, &str, &str), &MaintenanceTask> = HashMap::new();
    for task in tasks {
        track_latest_frontier_rollup(&mut latest, task, now_micros);
    }
    frontier_lag_secs(latest.values().copied(), now_micros)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Resources {
    pub cpu: u32,
    pub decoded_bytes: u64,
    pub object_reads: u32,
    pub object_writes: u32,
}

impl Resources {
    fn fits(self, available: Self) -> bool {
        self.cpu <= available.cpu
            && self.decoded_bytes <= available.decoded_bytes
            && self.object_reads <= available.object_reads
            && self.object_writes <= available.object_writes
    }

    fn checked_sub(self, request: Self) -> Option<Self> {
        Some(Self {
            cpu: self.cpu.checked_sub(request.cpu)?,
            decoded_bytes: self.decoded_bytes.checked_sub(request.decoded_bytes)?,
            object_reads: self.object_reads.checked_sub(request.object_reads)?,
            object_writes: self.object_writes.checked_sub(request.object_writes)?,
        })
    }

    fn saturating_add(self, released: Self) -> Self {
        Self {
            cpu: self.cpu.saturating_add(released.cpu),
            decoded_bytes: self.decoded_bytes.saturating_add(released.decoded_bytes),
            object_reads: self.object_reads.saturating_add(released.object_reads),
            object_writes: self.object_writes.saturating_add(released.object_writes),
        }
    }
}

#[derive(Debug)]
struct AdmissionState {
    capacity: Resources,
    available: Resources,
}

/// Non-queuing multi-resource admission. Workers that cannot reserve every
/// resource return to the durable queue instead of sleeping on a semaphore.
#[derive(Clone, Debug)]
pub struct AdmissionController(Arc<Mutex<AdmissionState>>);

/// The largest single unit a pool at this occupancy will admit.
///
/// ClickHouse's rule, and the most transferable idea in the prior-art survey
/// (`docs/plans/2026-08-31-how-other-systems-schedule-maintenance.md`, rule 2):
/// scale the size cap by how much of the pool is free, so a busy pool admits
/// only SMALL work. Its documented rationale is ours exactly — *"to allow small
/// merges to process, not filling the pool with long running merges"* — and it
/// subsumes a deadline, because an oversized unit is never admitted into a
/// position where it would have to be killed.
///
/// Linear rather than ClickHouse's geometric interpolation: monotone, trivially
/// explainable, and the floor is what actually matters (a busy pool must still
/// admit the small hygiene bins that keep file counts down).
///
/// Always on, and deliberately not a knob: one admission rule is easier to
/// reason about than two, and a flag that is never flipped is a second
/// architecture nobody tests.
fn occupancy_scaled_ceiling(available: u64, capacity: u64) -> u64 {
    /// A busy pool must still admit work this small, or hygiene starves.
    const FLOOR: u64 = MAX_DECODED_BYTES / 16;
    /// Free fraction at which the FULL cap is granted, as a divisor: 2 = half
    /// free. ClickHouse grants its whole 150 GB merge cap at **8 free pool
    /// entries of 16** — half free, not idle — and that detail is the whole
    /// rule. Ours reached `MAX_DECODED_BYTES` only at `available == capacity`
    /// exactly, which no working pool ever is, so every unit priced at exactly
    /// `MAX_DECODED_BYTES` was refused forever. That is precisely what the
    /// splitter produces (`byte_bounded_units` halves until a unit FITS the
    /// constant) and what both admission sites clamp their request to, so the
    /// gate excluded the design's own intended shape: reserve the maximum, then
    /// hash-shard internally. Prod 2026-09-02 carried 365 dedup units at a
    /// median of exactly 512.0 MiB, median age 14.6 days.
    ///
    /// Honest about the practical effect: at prod's ratios — at most
    /// `coordinator_jobs x MAX_DECODED_BYTES` = 8 GiB reserved against a 60 GiB
    /// capacity — this ceiling is now INERT until the pool is half reserved. It
    /// is a backstop for a genuinely full pool, not a live throttle, and that is
    /// the right division of labour: the job count already bounds the
    /// monopolization the ceiling was written to stop, and it bounds it by
    /// construction rather than by a race.
    const FULL_CAP_AT_FREE_FRACTION: u128 = 2;
    let scaled = (u128::from(MAX_DECODED_BYTES) * FULL_CAP_AT_FREE_FRACTION * u128::from(available) / u128::from(capacity.max(1))) as u64;
    scaled.clamp(FLOOR, MAX_DECODED_BYTES)
}

impl AdmissionController {
    pub fn new(cpu: u32, cgroup_memory_bytes: u64, object_reads: u32, object_writes: u32) -> Self {
        // At most 75% is trackable maintenance decode. The remainder is an
        // unconditional foreground/untracked-allocation reserve.
        let decoded_bytes = cgroup_memory_bytes.saturating_mul(3) / 4;
        let capacity = Resources { cpu, decoded_bytes, object_reads, object_writes };
        Self(Arc::new(Mutex::new(AdmissionState { capacity, available: capacity })))
    }

    pub fn try_acquire(&self, request: Resources) -> Option<AdmissionPermit> {
        if request.decoded_bytes > MAX_DECODED_BYTES {
            return None;
        }
        let mut state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if request.decoded_bytes > occupancy_scaled_ceiling(state.available.decoded_bytes, state.capacity.decoded_bytes) {
            return None;
        }
        if !request.fits(state.available) {
            return None;
        }
        state.available = state.available.checked_sub(request)?;
        Self::publish_utilization(&state);
        Some(AdmissionPermit { controller: self.clone(), resources: request })
    }

    pub fn utilization(&self) -> Resources {
        let state = self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        Resources {
            cpu: state.capacity.cpu.saturating_sub(state.available.cpu),
            decoded_bytes: state.capacity.decoded_bytes.saturating_sub(state.available.decoded_bytes),
            object_reads: state.capacity.object_reads.saturating_sub(state.available.object_reads),
            object_writes: state.capacity.object_writes.saturating_sub(state.available.object_writes),
        }
    }

    fn publish_utilization(state: &AdmissionState) {
        use std::sync::atomic::Ordering::Relaxed;
        let used = Resources {
            cpu: state.capacity.cpu.saturating_sub(state.available.cpu),
            decoded_bytes: state.capacity.decoded_bytes.saturating_sub(state.available.decoded_bytes),
            object_reads: state.capacity.object_reads.saturating_sub(state.available.object_reads),
            object_writes: state.capacity.object_writes.saturating_sub(state.available.object_writes),
        };
        let stats = crate::observability::maintenance_stats();
        stats.maintenance_cpu_tokens_used.store(u64::from(used.cpu), Relaxed);
        stats.maintenance_decoded_bytes_used.store(used.decoded_bytes, Relaxed);
        stats.maintenance_object_read_tokens_used.store(u64::from(used.object_reads), Relaxed);
        stats.maintenance_object_write_tokens_used.store(u64::from(used.object_writes), Relaxed);
    }
}

#[must_use]
pub struct AdmissionPermit {
    controller: AdmissionController,
    resources: Resources,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = self.controller.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.available = state.available.saturating_add(self.resources);
        debug_assert!(state.available.fits(state.capacity));
        AdmissionController::publish_utilization(&state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim prod text, 2026-08-24: base files written before a rollup spec
    /// change lacked `duration_digest`, and the DataFusion `SchemaError` that
    /// raised arrives type-erased, so the strings are the contract.
    #[test]
    fn schema_failures_are_recognised_from_prod_text() {
        assert!(is_schema_failure("Schema error: No field named duration_digest. Valid fields are __maintenance_slice_input_0.project_id, ..."));
        assert!(is_schema_failure("Error during planning: SchemaError(FieldNotFound { field: Column { name: \"duration_digest\" } })"));
        for benign in ["dedup: Object at location ... not found", "compaction: transaction failed: version 2667 already exists", "resource_admission"] {
            assert!(!is_schema_failure(benign), "must not park a unit that would succeed on a retry: {benign}");
        }
    }

    /// A missing column cannot be halved away: every CHILD of a bisection names
    /// it too and fails identically, so splitting turns one bad spec into a
    /// retry storm. Prod 2026-08-24: 477 of 658 claims failed in eight minutes,
    /// the 1h rollup tier stopped ingesting entirely, and every other operation
    /// starved behind the churn. Both entry points must park instead — and the
    /// park has to survive the 60s planner re-mint, or it is a hot loop with a
    /// fresh face.
    #[test_case::test_case(true ; "the worker hands back a plan error")]
    #[test_case::test_case(false ; "a fast-fail retry reason carries one")]
    fn a_deterministic_plan_error_parks_instead_of_shredding_the_slice(via_abandon: bool) {
        const ERROR: &str = "Schema error: No field named duration_digest.";
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Day-wide: splittable, so nothing but the guard stops the bisection.
        let mut unit = task("p", 0, DAY_MICROS, Operation::BaseRollup);
        unit.attempts = 2;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 1_000_000_000;
        if via_abandon {
            journal.abandon_running(&key, now, Some(ERROR));
        } else {
            journal.retry_or_split(&key, format!("resolve_input: {ERROR}"), now, 2);
        }

        assert_eq!(journal.tasks().count(), 1, "a deterministic failure must not be bisected into children that fail identically");
        let parked = journal.tasks().find(|candidate| candidate.key == key).expect("requeued").clone();
        assert_eq!(parked.state, TaskState::Retry);
        assert_eq!(parked.retry_reason.as_deref(), Some(TaskJournal::SCHEMA_FAILURE_REASON), "the operator must see a park, not a backoff");
        assert!(TaskJournal::is_quarantined(&parked), "a parked unit is rationed to the quarantine permit, not the whole pool");
        assert!(parked.deadline_micros >= now + 3_600_000_000, "a park waits for a deploy or a rebuild, not for the next tick");

        journal.enqueue(key.clone(), now + 60_000_000, 1_000, 0);
        let after = journal.tasks().find(|candidate| candidate.key == key).expect("still queued");
        assert_eq!(after.deadline_micros, parked.deadline_micros, "the planner tick must not hand an unplannable unit straight back");
        assert_eq!(after.retry_reason.as_deref(), Some(TaskJournal::SCHEMA_FAILURE_REASON));
    }

    /// A unit that cannot be split — repair is the standing case, since its cost
    /// is the file it rewrites and time-bisection cannot shrink a file set — must
    /// not come straight back. Burning a 900s deadline and returning 256s later
    /// is a ~78% duty cycle on a worker, forever, for a unit that has never
    /// produced anything.
    ///
    /// Prod 2026-08-18: 7 Repair units timed out at 900s inside a 15-minute
    /// window — 6,300 of 14,400 available slot-seconds, 44% of all maintenance
    /// capacity. Rollup coverage could not advance behind that whatever the
    /// scheduling cycle did.
    #[test]
    fn a_unit_that_burned_its_deadline_waits_at_least_that_long_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A single-slice repair unit: `byte_bounded_units` cannot divide it, so
        // the split path declines and the backoff is what bounds the waste.
        let mut unit = task("p", 0, 1, Operation::Repair);
        unit.attempts = 5;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 1_000_000_000;
        journal.abandon_running(&key, now, None);

        let deadline_micros = i64::try_from(operation_deadline_secs(Operation::Repair) * 1_000_000).expect("fits");
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        assert!(
            not_before >= now + deadline_micros,
            "a repair unit that burned {}s must wait at least that long again, waited {}s",
            deadline_micros / 1_000_000,
            (not_before - now) / 1_000_000
        );
    }

    /// A FIRST abandonment must still come back fast. The pool is a FairSpillPool,
    /// so a correctly sized unit can be squeezed out by someone else's memory
    /// spike and succeed untouched next pass — making that wait a full deadline
    /// would penalise the innocent case. Only a repeat says the unit is oversized,
    /// which is the same threshold the split above already uses.
    #[test]
    fn a_first_abandonment_is_not_floored() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, 1, Operation::Repair);
        unit.attempts = 1;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        journal.abandon_running(&key, 0, None);
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        assert_eq!(not_before, 2 * 1_000_000, "one failure retries on plain exponential backoff, not the deadline floor");
    }

    /// A fast-fail retry (resource admission) repeats identically at the same
    /// size: the estimate exceeds what admission can ever grant. This was the
    /// ONE retry path with no split — prod 2026-08-21, a 1.1TB-estimate
    /// day-wide Repair looped at its 1s admission delay for days (attempts
    /// 140-211), never claiming a worker, so neither abandon_running's split
    /// nor its floor ever fired. Routed through retry_or_split, a repeated
    /// admission failure bisects like any other capacity failure.
    #[test]
    fn a_repeated_admission_failure_splits_instead_of_hot_looping() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let day = 86_400_000_000;
        // NOT Repair: its cost is a whole file, so it declines to split by
        // construction (see `a_repair_unit_is_never_bisected_...`). The
        // hot-loop invariant this test guards belongs to every other operation.
        let mut unit = task("p", 0, day, Operation::SealedConsolidation);
        unit.attempts = 3;
        unit.estimated_decoded_bytes = 1_100_000_000_000; // the observed 1.1TB
        let key = unit.key.clone();
        journal.upsert(unit);

        journal.retry_or_split(&key, "resource_admission".into(), 1_000_000, 3);

        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "an unadmittable unit must split, not requeue whole");
        let children: Vec<_> = journal.tasks().filter(|t| t.key != key && t.state == TaskState::Pending).collect();
        assert_eq!(children.len(), 2, "one bisection: two half-day children");
        assert!(children.iter().all(|t| t.attempts == 0));
    }

    /// REPRODUCES the 2026-09-03 split-floor leak: `retry_or_split` handed the
    /// guard a SYNTHETIC `MAX_DECODED_BYTES + 1` instead of the unit's own
    /// estimate, so `split_sheds_enough` compared a CONSTANT against the
    /// parent's real measured bytes. Against any parent above ~683 MiB that
    /// constant always satisfies `observed * 4 < parent * 3`, so the shed test
    /// passed at every level and the lineage bisected to `MIN_SLICE_MICROS`.
    ///
    /// Prod 2026-09-03: **147 live units at or below the 60 s floor** — all
    /// `base_rollup`, all one whale project — plus 8,595 completed there
    /// historically, with `parent_measured_bytes` 512 MiB..17,020 MiB against
    /// own estimates of 171 MiB..8,510 MiB. The guard was consulted at every
    /// level and said yes at every level.
    ///
    /// The unit below shed NOTHING: its own estimate equals its parent's
    /// measurement, so bisecting again cannot help and must be declined.
    #[test]
    fn a_lineage_that_did_not_shed_is_not_split_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY: i64 = 86_400_000_000;
        const MEASURED: u64 = 8 * 1024 * 1024 * 1024;
        let mut unit = task("whale", 0, DAY, Operation::BaseRollup);
        unit.attempts = 3;
        unit.parent_measured_bytes = Some(MEASURED);
        unit.estimated_decoded_bytes = MEASURED;
        let key = unit.key.clone();
        journal.upsert(unit);

        journal.retry_or_split(&key, "resource_admission".into(), 1_000_000, 3);

        assert_eq!(
            journal.state(&key),
            Some(TaskState::Retry),
            "a unit that shed NOTHING against its parent must be retried, not bisected again — \
             splitting it only mints children that each pay the same scan"
        );
        assert_eq!(journal.tasks().filter(|t| t.key != key).count(), 0, "and it must mint no children");
    }

    /// When the split is REFUSED (already at minimum width), a repeated
    /// capacity failure must not keep the caller's transient-tuned delay: a 1s
    /// admission retry on an unfittable unit is a hot loop that increments
    /// attempts every second forever. The delay escalates with the evidence.
    #[test]
    fn a_split_refused_capacity_retry_escalates_its_delay() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Single MIN_SLICE unit: byte_bounded_units would hash-shard, so the
        // split declines.
        let mut unit = task("p", 0, MIN_SLICE_MICROS, Operation::Repair);
        unit.attempts = 6;
        unit.estimated_decoded_bytes = 1_100_000_000_000;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = crate::support::now_micros();
        journal.retry_or_split(&key, "resource_admission".into(), now + 1_000_000, 6);

        let not_before = journal.tasks().find(|t| t.key == key).map(|t| t.deadline_micros).expect("requeued");
        assert!(
            not_before >= now + (1 << 6) * 1_000_000,
            "sixth identical capacity failure must wait 2^6s, not the 1s admission delay (waited {}s)",
            (not_before - now) / 1_000_000
        );
    }

    /// The planner re-derives file debt every 60s and enqueues the same day-wide
    /// key while a partition stays out of policy. That re-mint must not resurrect
    /// a parent `split_time_task` superseded: prod 2026-08-21, project 87576849,
    /// held ~6 whole-day Repair units at attempts 140-204 for days — each timeout
    /// split the parent, the next 60s tick flipped it back to Pending with its
    /// attempts intact, and the day-wide width outranked its own 12h children in
    /// `scheduling_class`, so across 24h of logs not one child ever started.
    #[test]
    fn a_replanned_day_does_not_resurrect_a_superseded_parent() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, DAY_MICROS, Operation::SealedConsolidation);
        unit.attempts = 2;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        journal.abandon_running(&key, 0, None);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "two failures split a day-wide unit");
        let children: Vec<_> = journal.tasks().filter(|child| child.key != key).map(|child| child.key.clone()).collect();
        assert_eq!(children.len(), 2, "bisection leaves two live children");

        // The 60s planner tick re-mints the day key while children are live.
        journal.enqueue(key.clone(), 60_000_000, 1_000, 0);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "the children carry the work; the parent must stay down");
        assert!(!journal.mark_running(&key), "a superseded parent is not claimable");

        // Once every child is done, new debt on the day is new work: recreate fresh.
        for child in &children {
            journal.complete(child);
        }
        journal.enqueue(key.clone(), 120_000_000, 1_000, 0);
        let revived = journal.tasks().find(|candidate| candidate.key == key).expect("revived");
        assert_eq!(revived.state, TaskState::Pending, "a childless superseded key revives for fresh debt");
        assert_eq!(revived.attempts, 0, "revival is a new unit, not attempt 205 of the old one");
    }

    /// The same 60s re-mint must not erase what `abandon_running` recorded: its
    /// deadline floor is the only bound on a doomed unit's duty cycle, and its
    /// `worker_error` reason is what routes the unit through the small quarantine
    /// permit. Prod 2026-08-21: day-wide Repair units were re-claimed every 5-8
    /// minutes against a >=900s floor, because every planner tick pulled the
    /// deadline back to `now` and cleared the reason.
    #[test]
    fn a_replanned_debt_does_not_erase_worker_failure_backoff() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Single-slice: unsplittable, so abandonment falls to the backoff floor.
        let mut unit = task("p", 0, MIN_SLICE_MICROS, Operation::Repair);
        unit.attempts = 5;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 1_000_000_000;
        journal.abandon_running(&key, now, None);
        let floored = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");

        journal.enqueue(key.clone(), now + 60_000_000, 1_000, 0);
        let after = journal.tasks().find(|candidate| candidate.key == key).expect("still queued");
        assert_eq!(after.deadline_micros, floored, "a re-noticed debt must not cancel the abandonment backoff");
        assert_eq!(after.retry_reason.as_deref(), Some(TaskJournal::WORKER_FAILURE_REASON), "the quarantine tag survives the planner tick");
    }

    /// The floor must not REPLACE exponential backoff, only raise it. A unit that
    /// has failed many times should keep backing off past the deadline, or a
    /// permanently broken unit still returns every 15 minutes forever.
    #[test]
    fn the_deadline_floor_does_not_cap_exponential_backoff() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, 1, Operation::Dedup);
        unit.attempts = 8;
        unit.state = TaskState::Running;
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = 0;
        journal.abandon_running(&key, now, None);
        let not_before = journal.tasks().find(|candidate| candidate.key == key).map(|candidate| candidate.deadline_micros).expect("requeued");
        let exponential = 256 * 1_000_000i64;
        let floor = i64::try_from(operation_deadline_secs(Operation::Dedup) * 1_000_000).expect("fits");
        assert_eq!(not_before, now + exponential.max(floor), "the delay is the greater of the two, never the lesser");
    }

    /// Splitting a backfill unit must narrow the WORK, not the priority.
    ///
    /// Sealed ordering ranks wide units first because width PROXIES backfill
    /// provenance: a day-sized unit comes from the backfill planner and is the
    /// only kind that advances the horizon, while a ten-minute one is what the
    /// live path mints by the hundred. `split_time_task` breaks that proxy — the
    /// children are still the backfill work that moves coverage, but they now
    /// measure 180s and rank below every day-wide unit anywhere in history.
    ///
    /// Prod 2026-08-19: `87576849`'s 2026-08-10 day unit was split into 928
    /// fragments on 08-17, and over the following 40 minutes sealed BaseRollup
    /// claims went to 2026-07-22 — a month older — while 08-10 got none. That
    /// day stayed at zero rollup rows and capped `rollup_min_contiguous_days`
    /// at 2. Ageing does not cover it: both are starved, and width decides
    /// within the starved set.
    ///
    /// Asserted on the WIDTH TERM of `scheduling_class` rather than on the order
    /// `fair_ready_tasks` returns. The original fixture compared a recent split
    /// day against an older day-wide one and asserted the recent won; that has
    /// since become the wrong expectation for a reason unrelated to this fix —
    /// `starved` is now a per-day SLOPE past the 31-day horizon, so a 39-day
    /// unit legitimately outranks a 9-day one before width is ever consulted.
    /// Comparing the two units at the same seal time isolates the one term this
    /// change actually moves.
    #[test]
    fn a_split_backfill_child_keeps_its_parents_scheduling_width() {
        const DAY: i64 = 24 * 60 * 60 * 1_000_000;
        let now = 60 * DAY;
        let (start, end) = (now - 10 * DAY, now - 9 * DAY);
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");

        let split = task("split", start, end, Operation::BaseRollup);
        let key = split.key.clone();
        journal.upsert(split);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None), "the day unit splits");

        // The same day, same seal time, never split — what the children must not
        // be demoted below.
        let peer = task("peer", start, end, Operation::BaseRollup);
        let peer_width = scheduling_class(&peer, now).2;

        let child = journal.tasks().find(|task| task.key != key && task.key.project_id == "split").expect("a split child");
        assert!(child.key.slice.width() < end - start, "the child really is narrower: {}", child.key.slice.width());
        assert_eq!(scheduling_class(child, now).2, peer_width, "a split child must rank at its PARENT's width, not its own {}", child.key.slice.width());
    }

    /// THE DASHBOARD-WINDOW ORDERING DEFECT, pinned as a measurement.
    ///
    /// `STARVATION_MICROS` is 3 days and starved work drains OLDEST-first, so of
    /// the 14 days a dashboard reads, only days 1-3 stay newest-first. Days 4-14
    /// are all "starved" and join the oldest-first backlog lane, where they
    /// compete with MONTHS of history that is starved by a wider margin and
    /// therefore outranks them.
    ///
    /// Prod 2026-09-01 is why this matters: certification cannot grant on
    /// `otel_logs_and_spans` until duplicates are physically removed (0.0004% of
    /// rows but spread over ~50 of 144 bins per date), so dedup order decides
    /// when a queried window becomes certifiable. `pending_dedup` sat at ~2,250
    /// while a 1.7M-row/day project measured 0 of 8 sampled dates certified.
    ///
    /// The query-window reservation must not preempt the sealed one, and must
    /// not change what the first claims hand out.
    ///
    /// Both were caught by the suite while building it. `% 4 == 0` is also a
    /// multiple of 2, so it STOLE sealed turns — and the sealed reservation
    /// exists because without it sealed work never runs at all (prod 2026-08-17:
    /// 278 consecutive starts, every one today or yesterday). `% 4 == 1` fired on
    /// the very first claim, changing what a freshly-built journal returns.
    #[test]
    fn the_window_reservation_composes_with_the_sealed_one() {
        // Sealed fires on multiples of 2, or of 4 while the frontier is behind.
        for tick in 1u64..=64 {
            let window_turn = tick % 4 == 3;
            if window_turn {
                assert!(!tick.is_multiple_of(2), "tick {tick} would steal a sealed turn");
                assert!(!tick.is_multiple_of(4), "tick {tick} would steal a frontier-behind sealed turn");
            }
        }
        // `claim_tick` is incremented BEFORE use, so the first claim sees 1.
        assert!(![1u64, 2].into_iter().any(|first| first % 4 == 3), "the reservation must not take the first claims");
        // And it is bounded: one claim in four, so three in four still drain the
        // backlog in the existing order.
        assert_eq!((1u64..=64).filter(|tick| tick % 4 == 3).count(), 16, "one claim in four, no more");
    }

    /// Months-old history OUTRANKS the dates dashboards read, and the threshold
    /// is not the lever.
    ///
    /// `STARVATION_MICROS` is 3 days and starved work drains oldest-first, so of
    /// the 14 days a dashboard reads only days 1-3 escape the starved lane — and
    /// `starved` is `u8::MAX` when NOT starved, so those three lose to everything
    /// in it. Prod 2026-09-01: `pending_dedup` ~2,250 while a 1.7M-row/day project
    /// had 0 of 8 sampled dates certified.
    ///
    /// Raising the threshold is REFUTED (see `STARVATION_MICROS`): it evicts the
    /// window from the privileged lane and it loses by more. This pins the real
    /// shape so a future fix — bounding the starved lane's share of claims, as
    /// `claim_next` already does for sealed work — has a baseline to move.
    #[test]
    fn months_old_history_outranks_the_dates_dashboards_read() {
        const DAY: i64 = 24 * 60 * 60 * 1_000_000;
        let now = 400 * DAY;
        let rank = |days_ago: i64| {
            let end = now - days_ago * DAY;
            scheduling_class(&task("p", end - DAY, end, Operation::Dedup), now)
        };
        let (day2, day4, day10, day90) = (rank(2), rank(4), rank(10), rank(90));

        // Only the first three days of a 14-day window escape the starved lane.
        assert_eq!(day2.1, u8::MAX, "day 2 is inside the 3d floor, so it is NOT starved");
        assert!(day4.1 < u8::MAX, "day 4 already IS starved");
        assert!(day10.1 < u8::MAX, "and so is day 10");

        // `starved` is compared before every other term, and smaller wins — so
        // the further past the horizon, the better the rank.
        assert!(day90 < day4, "months-old history outranks a date the dashboard reads");
        assert!(day90 < day10, "and outranks the middle of the window");
        // And the unstarved days lose to ALL of it: u8::MAX is the worst value,
        // which is why raising the threshold cannot be the fix.
        assert!(day4 < day2, "an in-window starved date still outranks an unstarved newer one");
    }

    fn task(project: &str, start: i64, end: i64, operation: Operation) -> MaintenanceTask {
        MaintenanceTask {
            key: TaskKey {
                physical_table: "table".into(),
                source: "source".into(),
                project_id: project.into(),
                slice: TimeSlice::new(start, end).expect("valid slice"),
                operation,
            },
            state: TaskState::Pending,
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
        }
    }

    /// Only outstanding ROLLUP work may veto a rollup backfill.
    ///
    /// This predicate used to be "any non-complete task on the source", so a
    /// day carrying unrelated file debt was disqualified forever. Prod
    /// 2026-08-17: the whale's Aug 15 `SealedConsolidation` sat on attempt 370,
    /// and once the coarse-backfill migration retired the fine-grained
    /// historical tasks there was nothing left to claim and nothing allowed to
    /// replace it — every task start for 30 minutes was today's, rollup
    /// coverage froze at three days, and 7d/14d queries fell back to a full raw
    /// scan (`rollup_miss_not_built`).
    #[test]
    fn only_outstanding_rollup_work_blocks_a_backfill() {
        for operation in [Operation::SealedConsolidation, Operation::HotPacking, Operation::Repair] {
            let debt = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(!blocks_rollup_backfill(&debt), "{operation:?} is file debt, not rollup coverage; it must not veto a backfill");
        }
        for operation in [Operation::Dedup, Operation::BaseRollup, Operation::DerivedRollup] {
            let outstanding = task("whale", 0, NORMAL_SLICE_MICROS, operation);
            assert!(blocks_rollup_backfill(&outstanding), "{operation:?} is the work a backfill would queue; re-queueing it pushes its deadline out");
            let mut done = outstanding.clone();
            done.state = TaskState::Complete;
            assert!(!blocks_rollup_backfill(&done), "a completed {operation:?} leaves the day open to backfill again");
            // A SUPERSEDED parent is what `split_time_task` leaves behind, and it
            // is never claimable. Letting it veto meant every day that was ever
            // split had its cell marked "already queued" forever, so the planner
            // could not re-admit it and the tier stayed holed permanently.
            //
            // Prod 2026-08-20 01:35, otel_logs_and_spans: `cells_missing=210`
            // with `cells_wanted=0` and `derived_pending=22` — the census counts
            // only Pending/Retry, so the superseded parents doing the blocking
            // were invisible in every gauge. The 1h tier (what 30d dashboards
            // read) sat at 22 days with its oldest date frozen at 2026-07-25
            // across 32 minutes while the 1m tier held 31 days.
            //
            // Safe because a split parent's CHILDREN are Pending and still veto
            // on their own; this only stops an unclaimable record from standing
            // in for work nobody is going to do. Same reversal `can_fuse` already
            // makes for coarsening.
            let mut split = outstanding;
            split.state = TaskState::Superseded;
            assert!(!blocks_rollup_backfill(&split), "a superseded {operation:?} is unclaimable, so it must not veto the backfill that would replace it");
        }
    }

    /// One call halves ONCE. Descending further would price the extra levels by
    /// time share and stamp them all with this one measurement, which is what
    /// made `split_sheds_enough` unreachable; the next level is minted only
    /// after its own preflight has measured it.
    #[test]
    fn halves_a_whale_once_and_hash_shards_at_the_floor() {
        // A 10-minute slice is Dedup's floor now (see `byte_bounded_units`), so
        // the halving property is asserted on an operation that still bisects
        // there. The property under test is "one level per call", not the floor.
        let input = task("whale", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let units = byte_bounded_units(&input, 10 * MAX_DECODED_BYTES);
        assert_eq!(units.len(), 2, "one level per measurement, not a subtree");
        assert_eq!(units.iter().map(|unit| unit.estimated_decoded_bytes).sum::<u64>(), 10 * MAX_DECODED_BYTES, "the halves must price the whole parent");
        assert!(units.iter().all(|unit| unit.key.slice.width() == NORMAL_SLICE_MICROS / 2 && unit.hash_shards <= 1));
        assert_eq!(units[0].key.slice.end_micros, units[1].key.slice.start_micros, "and they must tile the parent");

        let minute = task("whale", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let shards = byte_bounded_units(&minute, MAX_DECODED_BYTES * 3);
        assert_eq!(shards.len(), 3);
        assert!(shards.iter().all(|unit| unit.hash_shards == 3));
    }

    /// Bisection prices children by TIME SHARE (`byte_bounded_units:444`), so it
    /// always "fits" on paper. The real cost floors out — a slice reads at least
    /// one row group of every file it overlaps — and the disagreement surfaces
    /// only at the next preflight, which re-measures, finds itself over budget
    /// and splits again. Prod 2026-08-22 held 3,455 units for a single
    /// (project, tier, day), 1,423 of them pending.
    ///
    /// A child that measured essentially what its parent measured IS that floor,
    /// observed. Splitting again cannot help, so the split must decline —
    /// declining is safe because the runner already hash-shards INTERNALLY at
    /// any width (`database/maintain.rs:1623`), which bounds memory without
    /// minting a single journal unit.
    #[test]
    fn a_child_no_cheaper_than_its_parent_stops_bisecting() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A 12h child of a day-wide parent: the model promised half the bytes,
        // the measurement came back with 96% of them.
        let mut unit = task("whale", 0, DAY_MICROS / 2, Operation::BaseRollup);
        unit.parent_measured_bytes = Some(100 * MAX_DECODED_BYTES);
        let key = unit.key.clone();
        journal.upsert(unit);

        assert!(
            !journal.split_time_task(&key, 96 * MAX_DECODED_BYTES, None),
            "halving the width bought 4% — the row-group floor dominates, so bisecting again only mints units"
        );
        assert_eq!(journal.state(&key), Some(TaskState::Pending), "a declined split must leave the unit runnable, not superseded");
    }

    /// Section 4 of the 08-24 plan proposed BATCHING escalations: when several
    /// holes inside one covering slice each escalate, rebuild the covering slice
    /// once rather than N times. That needs no code, and this pins why —
    /// `enqueue` is keyed by `TaskKey`, so N escalations naming the same
    /// covering slice collapse into a single pending unit by construction.
    ///
    /// Worth a test rather than a comment because the claim is what justifies
    /// NOT building the batching layer, and a future change to enqueue's
    /// identity rules would silently reintroduce the N-rebuild cost.
    #[test]
    fn escalations_to_one_covering_slice_collapse_into_a_single_unit() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let day = 3 * DAY_MICROS;
        let covering = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;

        // Five separate narrow units discover holes and each escalates to the
        // same covering slice, exactly as `covered_by_wider` does.
        for _ in 0..5 {
            journal.enqueue(covering.clone(), 0, MAX_DECODED_BYTES, 0);
        }

        let covering_units = journal.tasks().filter(|t| t.key == covering).count();
        assert_eq!(covering_units, 1, "the covering rebuild is queued ONCE however many holes escalated to it");
    }

    /// Declining a split must leave the unit RUNNABLE, not merely un-superseded.
    ///
    /// This is the failure mode that would be worse than the shred it replaces:
    /// a unit declined at the floor and then never claimed does no work at all,
    /// where the shred at least made progress expensively. `split_declined_at_floor`
    /// rising alongside `pending_base_rollup` is what that would look like in
    /// prod; this pins it at the source instead of waiting to read it there.
    #[test]
    fn a_unit_declined_at_the_floor_is_still_claimable() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("whale", 0, DAY_MICROS / 2, Operation::BaseRollup);
        unit.parent_measured_bytes = Some(100 * MAX_DECODED_BYTES);
        let key = unit.key.clone();
        journal.upsert(unit);

        let now = crate::support::now_micros();
        assert!(!journal.split_time_task(&key, 96 * MAX_DECODED_BYTES, None), "the floor declines the split");

        let claimed = journal.claim_next(Operation::BaseRollup, now, true);
        assert_eq!(claimed.map(|task| task.key), Some(key), "the declined unit is the one a worker picks up, so the work still happens");
    }

    /// BOTH ways a split can be refused must be counted, or a pinned unit is
    /// invisible.
    ///
    /// Prod 2026-08-28: day-wide sealed units carrying 22-25 GB estimates were
    /// abandoned 9/9 at the 900 s deadline and re-claimed forever. That was only
    /// attributable because `split_declined_at_floor` happened to be counted —
    /// the sibling branch (`children.len() <= 1 || hash_shards > 1`) returned
    /// `false` silently, so had the decline gone that way instead, nothing in the
    /// process would have named it. Counters are per-process under nextest, so
    /// these assertions are exact.
    #[test]
    fn both_split_declines_are_counted() {
        let stats = crate::observability::maintenance_stats();
        let floor0 = stats.split_declined_at_floor.load(std::sync::atomic::Ordering::Relaxed);
        let width0 = stats.split_declined_no_width.load(std::sync::atomic::Ordering::Relaxed);

        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");

        // Floor: the unit came back costing nearly what its parent cost.
        let mut at_floor = task("whale", 0, DAY_MICROS / 2, Operation::BaseRollup);
        at_floor.parent_measured_bytes = Some(100 * MAX_DECODED_BYTES);
        let floor_key = at_floor.key.clone();
        journal.upsert(at_floor);
        assert!(!journal.split_time_task(&floor_key, 96 * MAX_DECODED_BYTES, None), "floor declines");

        // No width: already at the minimum slice, so bisection yields no children.
        let narrow = task("whale", DAY_MICROS, DAY_MICROS + MIN_SLICE_MICROS, Operation::BaseRollup);
        let narrow_key = narrow.key.clone();
        journal.upsert(narrow);
        assert!(!journal.split_time_task(&narrow_key, 96 * MAX_DECODED_BYTES, None), "a minimum-width unit has nothing to split into");

        assert_eq!(stats.split_declined_at_floor.load(std::sync::atomic::Ordering::Relaxed), floor0 + 1, "the floor decline is counted");
        assert_eq!(
            stats.split_declined_no_width.load(std::sync::atomic::Ordering::Relaxed),
            width0 + 1,
            "the no-width decline must be counted too — this is the branch that was silent"
        );
    }

    /// The guard above is only as good as the evidence it reads, and that
    /// evidence has to be the parent's MEASUREMENT — the modelled per-child
    /// number is the very thing that cannot be trusted.
    #[test]
    fn a_split_stamps_children_with_what_the_parent_measured() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let unit = task("whale", 0, DAY_MICROS, Operation::BaseRollup);
        let key = unit.key.clone();
        journal.upsert(unit);

        let measured = 4 * MAX_DECODED_BYTES;
        assert!(journal.split_time_task(&key, measured, None), "a day-wide unit with no floor evidence still bisects");

        let children: Vec<_> = journal.tasks().filter(|t| t.state == TaskState::Pending).collect();
        assert!(!children.is_empty(), "the split produced children");
        assert!(
            children.iter().all(|child| child.parent_measured_bytes == Some(measured)),
            "children carry the parent's measurement, not their own modelled share"
        );
    }

    /// `retry_or_split` forces a bisection with a synthetic
    /// `MAX_DECODED_BYTES + 1` — it is a "does not fit" signal, not a
    /// measurement. Stamping it and then comparing the next REAL measurement
    /// against it would decline every split in that lineage forever, which is
    /// the immortal-unit shape this file already carries three incidents of.
    #[test]
    fn a_synthetic_stamp_does_not_freeze_a_lineage() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let unit = task("whale", 0, DAY_MICROS, Operation::BaseRollup);
        let key = unit.key.clone();
        journal.upsert(unit);

        // The retry path's synthetic value, verbatim.
        assert!(journal.split_time_task(&key, MAX_DECODED_BYTES.saturating_add(1), None));
        let child = journal.tasks().find(|t| t.state == TaskState::Pending).expect("a child").clone();
        assert_eq!(child.parent_measured_bytes, Some(MAX_DECODED_BYTES + 1));

        // A real preflight now measures far more than the synthetic seed. That
        // is not the row-group floor, so the child must still be splittable.
        assert!(
            journal.split_time_task(&child.key, 8 * MAX_DECODED_BYTES, None),
            "a measurement ABOVE the parent's stamp is evidence the stamp was never a measurement"
        );
    }

    #[test]
    fn scheduler_rotates_projects_within_a_deadline() {
        let tasks =
            [task("a", 0, 1, Operation::Dedup), task("a", 1, 2, Operation::Dedup), task("b", 0, 1, Operation::Dedup), task("b", 1, 2, Operation::Dedup)];
        let order: Vec<_> = fair_ready_tasks(&tasks, 0).into_iter().map(|task| task.key.project_id.as_str()).collect();
        assert_eq!(order, ["a", "b", "a", "b"]);
    }

    #[test]
    fn journal_claims_rotate_projects_instead_of_restarting_at_first() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        for input in
            [task("a", 0, 1, Operation::Dedup), task("a", 1, 2, Operation::Dedup), task("b", 0, 1, Operation::Dedup), task("b", 1, 2, Operation::Dedup)]
        {
            journal.upsert(input);
        }
        assert_eq!(journal.claim_next(Operation::Dedup, 0, true).expect("first").key.project_id, "a");
        assert_eq!(journal.claim_next(Operation::Dedup, 0, true).expect("second").key.project_id, "b");
    }

    #[test]
    fn bootstrap_backlog_migration_keeps_publications_and_runs_once() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Production already carries the v1 marker. It must not suppress the
        // corrective cleanup after commit-range reconciliation ships.
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v1".to_owned(), 1);
        journal.set_source_cursor("__maintenance_bootstrap_backlog_v2".to_owned(), 1);
        journal.upsert(task("pending-a", 0, 1, Operation::Dedup));
        journal.upsert(task("pending-b", 1, 2, Operation::BaseRollup));
        let mut complete = task("published", 2, 3, Operation::BaseRollup);
        complete.state = TaskState::Complete;
        journal.upsert(complete.clone());

        assert_eq!(journal.migrate_bootstrap_backlog_with_limit(2), Some(2));
        assert_eq!(journal.snapshot.tasks, vec![complete]);
        assert_eq!(journal.migrate_bootstrap_backlog_with_limit(0), None, "migration marker makes cleanup one-shot");
        journal.compact().expect("migration snapshot");
        let reloaded = TaskJournal::load(dir.path()).expect("reloaded journal");
        assert_eq!(reloaded.snapshot.tasks.len(), 1, "removed bootstrap tasks must not resurrect after restart");
        assert_eq!(reloaded.snapshot.tasks[0].key.project_id, "published");
    }

    #[test]
    fn repeated_pending_invalidation_does_not_rewrite_the_wal() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("customer", 0, 1, Operation::BaseRollup).key;
        journal.enqueue(key.clone(), 10, 512, 1);
        journal.checkpoint().expect("first checkpoint");
        let first_size = fs::metadata(&journal.wal_path).expect("wal").len();

        journal.enqueue(key.clone(), 20, 512, 2);
        journal.checkpoint().expect("idempotent checkpoint");
        assert_eq!(fs::metadata(&journal.wal_path).expect("wal").len(), first_size);

        journal.enqueue(key, 5, 512, 3);
        journal.checkpoint().expect("earlier deadline checkpoint");
        assert!(fs::metadata(&journal.wal_path).expect("wal").len() > first_size);
    }

    #[test]
    fn recent_slices_precede_overdue_historical_work() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let old = task("old", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let recent = task("recent", now - MIN_SLICE_MICROS, now, Operation::BaseRollup);
        let ready = fair_ready_tasks([&old, &recent], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "recent");
    }

    #[test]
    fn newest_eligible_frontier_slice_precedes_older_frontier_debt() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let older_start = now - 12 * 60 * 60 * 1_000_000;
        let older = task("older", older_start, older_start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let newest = task("newest", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&older, &newest], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "newest");
    }

    #[test]
    fn frontier_minute_rotates_fairly_across_projects() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let start = now - 2 * MIN_SLICE_MICROS;
        let a = task("a", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let b = task("b", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let c = task("c", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&c, &a, &b], now);
        assert_eq!(ready.iter().map(|task| task.key.project_id.as_str()).collect::<Vec<_>>(), ["a", "b", "c"]);
    }

    #[test]
    fn recently_mutated_historical_hole_does_not_displace_frontier() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut correction = task("correction", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        correction.deadline_micros = now - 1;
        let mut frontier = task("frontier", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        frontier.deadline_micros = now - 60 * 1_000_000;
        let ready = fair_ready_tasks([&correction, &frontier], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "frontier");
    }

    #[test]
    fn future_clock_slice_does_not_displace_live_frontier() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let future = task("future", now + 24 * 60 * 60 * 1_000_000, now + 24 * 60 * 60 * 1_000_000 + MIN_SLICE_MICROS, Operation::BaseRollup);
        let frontier = task("frontier", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        let ready = fair_ready_tasks([&future, &frontier], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "frontier");
    }

    /// Historical debt runs newest SLICE first, not oldest deadline first.
    ///
    /// This asserted the opposite until 2026-08-17, with no stated reason.
    /// Oldest-first is FIFO-fair, but it spends the whole non-frontier budget on
    /// the oldest data in the store — which is the data nobody queries. Prod,
    /// over 84 task starts: 74 went to the live frontier (correct) and every one
    /// of the other 10 landed on 2026-08-01, 07-22, 07-16, 07-15, 06-29, 06-15,
    /// 06-02, 06-01, 05-28. Meanwhile rollup coverage for a live tenant sat at
    /// two days and every 7d/14d/30d query fell back to a raw scan.
    ///
    /// Newest-first does not starve old debt: frontier work is rate-limited by
    /// ingest and recent slices are finite, so once the recent window drains the
    /// ordering walks backward on its own. What it guarantees is that the window
    /// a dashboard actually reads is reached FIRST.
    /// Sealed work must get a share of claims even while the frontier is busy.
    ///
    /// Class is strict priority and ingest never stops, so before the
    /// reservation class 1 simply never ran: prod 2026-08-17 went 278
    /// consecutive task starts without a single sealed day, while rollup
    /// coverage for a live tenant sat at two days and every 7d/14d/30d query
    /// fell back to a raw scan. A frontier that is healthy on its own metric
    /// (`eligible_watermark_lag_seconds` 0) tells you nothing about this.
    /// Pending work must be reportable per operation, and split by whether it
    /// is claimable right now.
    ///
    /// `tasks_pending` alone cannot distinguish "no rollup work queued" from
    /// "queued but not eligible" from "eligible but out-competed" — three states
    /// with three different fixes. Prod 2026-08-17 sat at ~128k pending with
    /// rollup coverage frozen for hours, and every attempt to tune it was a
    /// guess because these three were indistinguishable from outside.
    #[test]
    fn pending_work_is_reported_per_operation_and_by_eligibility() {
        use std::sync::atomic::Ordering::Relaxed;
        // publish_statistics reads the real clock, so eligibility has to be
        // expressed against it — an epoch-relative `now` makes every deadline
        // look long past and the eligible/pending distinction vanishes.
        let now = crate::support::now_micros();
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = |op, start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: op,
        };
        // One eligible sealed rollup, one sealed rollup not yet due, one repair.
        // Sealed slices: well before now, so they are not live-frontier.
        let old_start = now - 30 * 24 * 60 * 60 * 1_000_000;
        journal.enqueue(key(Operation::BaseRollup, old_start, old_start + MIN_SLICE_MICROS), now - 1, 1, 1);
        journal.enqueue(key(Operation::BaseRollup, old_start + MIN_SLICE_MICROS, old_start + 2 * MIN_SLICE_MICROS), now + 3_600_000_000, 1, 1);
        journal.enqueue(key(Operation::Repair, old_start, old_start + MIN_SLICE_MICROS), now - 1, 1, 1);
        journal.publish_statistics();

        let stats = crate::observability::maintenance_stats();
        assert_eq!(stats.pending_base_rollup.load(Relaxed), 2, "both rollup tasks are pending regardless of eligibility");
        assert_eq!(stats.pending_repair.load(Relaxed), 1);
        assert_eq!(stats.eligible_base_rollup.load(Relaxed), 1, "only the due rollup is claimable now — this is the distinction tasks_pending cannot make");
        assert_eq!(stats.eligible_sealed_total.load(Relaxed), 2, "both due sealed tasks (rollup + repair) are claimable");
    }

    /// `oldest_task_age` reports work the scheduler intends to do; abandoned
    /// work is COUNTED, not aged.
    ///
    /// Prod read 85.6 days for months. That was the seal time of one 2026-05-31
    /// hygiene partition — `plan_compaction_debt` stamps sealed units with their
    /// slice end — and past `STARVATION_HORIZON_MICROS` such a unit is
    /// deliberately never scheduled. An age gauge dominated by abandoned work is
    /// pinned red and reports nothing about the goal window.
    ///
    /// Both halves are asserted together on purpose: narrowing the age alone
    /// would look identical to hiding the debt.
    #[test]
    fn the_age_gauge_skips_abandoned_work_and_counts_it_instead() {
        use std::sync::atomic::Ordering::Relaxed;
        const DAY: i64 = 24 * 60 * 60 * 1_000_000;
        let now = crate::support::now_micros();
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = |start: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, start + DAY).expect("slice"),
            operation: Operation::SealedConsolidation,
        };
        let stamp = |micros: i64| u64::try_from(micros.div_euclid(1_000)).unwrap_or_default();
        // The prod shape: a hygiene unit aged from a seal time 85 days back...
        journal.enqueue(key(now - 86 * DAY), now, 1, stamp(now - 85 * DAY));
        // ...beside one inside the window, five days old.
        journal.enqueue(key(now - 6 * DAY), now, 1, stamp(now - 5 * DAY));
        journal.publish_statistics();

        let stats = crate::observability::maintenance_stats();
        assert_eq!(stats.maintenance_beyond_horizon_tasks.load(Relaxed), 1, "the abandoned unit must be sized, not silently dropped");
        let age_days = stats.maintenance_oldest_task_age_secs.load(Relaxed) / 86_400;
        assert_eq!(age_days, 5, "the gauge must report the oldest unit the scheduler will still escalate, not the 85-day tail");
    }

    /// The coarse-backfill migration must be narrow: fine sealed backfill goes,
    /// everything else stays.
    ///
    /// It exists because ~450 tasks per (project, date) drained at ~19/min on
    /// prod — 111 hours for ~600 GB of real work. Dropping tasks is safe only
    /// because `plan_rollup_backfill` re-derives from rollup COVERAGE, but a
    /// migration that over-reaches would silently cancel live frontier work or
    /// the separately-planned compaction debt.
    #[test]
    fn coarse_backfill_migration_only_drops_fine_sealed_backfill() {
        let now = crate::support::now_micros();
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let day = 24 * 60 * 60 * 1_000_000i64;
        let sealed_start = now - 10 * day;
        let key = |op, start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: op,
        };
        // Dropped: fine-grained sealed rollup/dedup work.
        journal.enqueue(key(Operation::BaseRollup, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);
        journal.enqueue(key(Operation::Dedup, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);
        // Kept: live frontier, day-sized (already coarse), and compaction debt.
        journal.enqueue(key(Operation::BaseRollup, now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS), now, 1, 1);
        journal.enqueue(key(Operation::BaseRollup, sealed_start, sealed_start + day), now, 1, 1);
        journal.enqueue(key(Operation::SealedConsolidation, sealed_start, sealed_start + day), now, 1, 1);
        journal.enqueue(key(Operation::Repair, sealed_start, sealed_start + MIN_SLICE_MICROS), now, 1, 1);

        let removed = journal.migrate_fine_grained_backfill(now).expect("migration runs once");
        assert_eq!(removed, 2, "only the two fine-grained sealed backfill tasks should go");
        let kept: Vec<_> = journal.tasks().map(|task| (task.key.operation, task.key.slice.width())).collect();
        assert_eq!(kept.len(), 4, "frontier, day-sized rollup, consolidation and repair must all survive");
        assert!(kept.iter().any(|(op, _)| *op == Operation::SealedConsolidation), "compaction debt is planned elsewhere and must not be cancelled");
        assert!(kept.iter().any(|(op, width)| *op == Operation::BaseRollup && *width == day), "an already-coarse rollup unit must survive");

        // One-shot: a second call must not re-run and drop the coarse re-plan.
        assert!(journal.migrate_fine_grained_backfill(now).is_none(), "migration must be guarded by its cursor");
    }

    /// The comparator for an empty publication must be the base tier's rows in
    /// THIS slice, not the day.
    ///
    /// `source_rows` is keyed on (project, date) while a unit is an hour, so a
    /// genuinely empty hour of a busy day reads as non-empty and a guard built
    /// on it would fire on correct work. Joined offline against the 2026-08-28
    /// prod journal, the per-slice comparator split 285 empty derived
    /// completions into 276 real violations and 9 legitimately empty hours.
    #[test]
    fn published_rows_are_summed_per_slice_and_per_project() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        let key = |project: &str, table: &str, hour: i64| TaskKey {
            physical_table: table.into(),
            source: "src".into(),
            project_id: project.into(),
            slice: TimeSlice::new(hour * HOUR, (hour + 1) * HOUR).expect("slice"),
            operation: Operation::BaseRollup,
        };
        let publish = |journal: &mut TaskJournal, project: &str, table: &str, hour: i64, rows: u64| {
            let key = key(project, table, hour);
            journal.enqueue(key.clone(), 0, 1, 1);
            journal.publish(&key, Publication { source_fingerprint: 0, generation: "g".into(), rows, source_rows: Some(1_000_000) });
        };
        publish(&mut journal, "p", "base", 1, 10);
        publish(&mut journal, "p", "base", 2, 0); // an hour the base itself left empty
        publish(&mut journal, "p", "base", 3, 7);
        publish(&mut journal, "other", "base", 2, 999); // another tenant
        publish(&mut journal, "p", "elsewhere", 2, 999); // another tier

        let rows = |lo: i64, hi: i64| journal.published_rows_overlapping("p", "base", lo * HOUR, hi * HOUR);
        assert_eq!(rows(1, 2), 10, "one slice");
        assert_eq!(rows(1, 4), 17, "overlapping slices sum, and the empty hour adds nothing");
        assert_eq!(rows(2, 3), 0, "an hour the base left empty is NOT a violation — this is the 9-of-285 case");
        assert_eq!(rows(4, 5), 0, "a slice with no base publication at all");
        assert_eq!(rows(0, 24), 17, "the day never picks up another tenant or another tier");
    }

    #[test]
    fn sealed_work_gets_claims_while_the_frontier_is_busy() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");

        // Plenty of frontier work — the production condition, where ingest
        // keeps class 0 permanently non-empty — plus one sealed day.
        let key = |start: i64, end: i64| TaskKey {
            physical_table: "t".into(),
            source: "t".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: Operation::BaseRollup,
        };
        for k in 1..20 {
            journal.enqueue(key(now - (k + 1) * MIN_SLICE_MICROS, now - k * MIN_SLICE_MICROS), now - 1, 1, 1);
        }
        let sealed = key(0, MIN_SLICE_MICROS);
        journal.enqueue(sealed.clone(), now - 1, 1, 1);

        // Enough sealed slices that the share, not the supply, is what limits
        // sealed claims — mirroring prod, where 118,794 of 127,798 pending tasks
        // were sealed and eligible while the frontier took ~90% of claims.
        let sealed_slices: Vec<TimeSlice> = (1..20).map(|k| TimeSlice::new(k * MIN_SLICE_MICROS, (k + 1) * MIN_SLICE_MICROS).expect("sealed slice")).collect();
        for slice in &sealed_slices {
            journal.enqueue(key(slice.start_micros, slice.end_micros), now - 1, 1, 1);
        }

        let mut sealed_claims = 0;
        for _ in 0..12 {
            let claimed = journal.claim_next(Operation::BaseRollup, now, true).expect("a task is always available");
            if !is_live_frontier(claimed.key.slice, now) {
                sealed_claims += 1;
            }
        }
        // "> 0" would pass at any share, including one too small to ever drain a
        // backlog — which is exactly what shipped first and left coverage frozen
        // for hours. Assert sealed work is the MAJORITY when it is the majority
        // of the queue.
        // Was `>= 7` (three in four). Lowered to the one-in-two floor after the
        // 2026-08-17 OOM: sealed work must still get a real share — "> 0" is the
        // bug that let an ineffective share ship — but the higher share cost
        // 124.9 GB of anon RSS and a kill, so the invariant is "a third or
        // better", not "the majority".
        assert!(
            sealed_claims >= 4,
            "sealed work got only {sealed_claims}/12 claims; below a third, a 118k-task sealed backlog never drains and long windows stay unroutable"
        );
    }

    #[test]
    fn historical_debt_runs_newest_slice_first() {
        let now: i64 = 10 * 24 * 60 * 60 * 1_000_000;
        // Older slice, and the older deadline too — under the previous ordering
        // this ran first purely because it had been waiting longest.
        // Both cover days that sealed RECENTLY, so neither is overdue. Age now
        // comes from the slice, and overdue work drains oldest-first by design
        // — a different question from which slice a dashboard needs.
        let base = now - 2 * 24 * 3_600_000_000;
        let mut older_slice = task("a", base, base + MIN_SLICE_MICROS, Operation::BaseRollup);
        older_slice.deadline_micros = now - 2 * 60 * 1_000_000;
        let mut newer_slice = task("b", base + MIN_SLICE_MICROS, base + 2 * MIN_SLICE_MICROS, Operation::BaseRollup);
        newer_slice.deadline_micros = now - 60 * 1_000_000;
        let ready = fair_ready_tasks([&older_slice, &newer_slice], now);
        assert_eq!(ready.first().expect("ready task").key.project_id, "b", "the more recent slice is the one a dashboard query needs");
    }

    #[test]
    fn live_frontier_lag_ignores_historical_holes() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut historical = task("project", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        historical.deadline_micros = now - 4 * 60 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&historical], now), 0);
    }

    #[test]
    fn live_frontier_lag_tracks_only_each_streams_newest_slice() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut older = task("project", now - 3 * MIN_SLICE_MICROS, now - 2 * MIN_SLICE_MICROS, Operation::BaseRollup);
        older.deadline_micros = now - 10 * 60 * 1_000_000;
        let mut newest = task("project", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        newest.deadline_micros = now - 2 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&older, &newest], now), 2 * 60);
        newest.state = TaskState::Complete;
        assert_eq!(live_frontier_lag_secs([&older, &newest], now), 0, "an older hole is not the raw tail once newer coverage landed");
    }

    #[test]
    fn live_frontier_lag_reports_the_slowest_project() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let mut fast = task("fast", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        fast.deadline_micros = now - 60 * 1_000_000;
        let mut slow = task("slow", now - 2 * MIN_SLICE_MICROS, now - MIN_SLICE_MICROS, Operation::BaseRollup);
        slow.deadline_micros = now - 5 * 60 * 1_000_000;
        assert_eq!(live_frontier_lag_secs([&fast, &slow], now), 5 * 60);
    }

    #[test]
    fn derived_invalidations_use_one_aligned_hour() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "derived",
                source: "source",
                project_id: "p",
                start_micros: NORMAL_SLICE_MICROS,
                end_micros: 2 * NORMAL_SLICE_MICROS,
                observed_at_micros: 0,
                derived: true,
            })
            .expect("invalidate");
        let derived = journal.tasks().find(|task| task.key.operation == Operation::DerivedRollup).expect("derived task");
        assert_eq!(derived.key.slice.width(), DERIVED_SLICE_MICROS);
        assert_eq!(derived.key.slice.start_micros % DERIVED_SLICE_MICROS, 0);
        assert!(journal.tasks().filter(|task| task.key.operation == Operation::Dedup).all(|task| task.key.slice.width() == NORMAL_SLICE_MICROS));
    }

    #[test]
    fn restart_migrates_unpublished_derived_fragments_to_hours() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(task("p", 0, NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        journal.checkpoint().expect("old checkpoint");

        let mut journal = TaskJournal::load(dir.path()).expect("journal to migrate");
        assert!(journal.migrate_derived_slices() > 0);
        journal.checkpoint().expect("migration checkpoint");
        let journal = TaskJournal::load(dir.path()).expect("migrated journal");
        assert!(journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup && task.key.slice.width() == DERIVED_SLICE_MICROS));
        assert!(journal.tasks().any(|task| task.key.operation == Operation::DerivedRollup
            && task.key.slice.width() == NORMAL_SLICE_MICROS
            && task.state == TaskState::Superseded));
    }

    /// The hour migration must not touch a slice WIDER than an hour.
    ///
    /// Its guard was `width() != DERIVED_SLICE_MICROS`, which matches wider
    /// slices as well as the ten-minute fragments it was written for — and the
    /// replacement key is the single hour containing the slice START, so a
    /// day-wide unit was superseded and re-enqueued as hour 00 with the other 23
    /// hours silently dropped. The 2026-08-28 prod journal held 265 such
    /// collapses (248 of them day-wide), losing roughly 5,799 hours of derived
    /// work, and is why cell 28f62f01/08-25 has no derived unit at all for hours
    /// 21 and 22.
    ///
    /// Left ALONE rather than expanded into 24 hour units: expanding mints 24x
    /// the journal entries this migration's own comment warns about, and day-wide
    /// derived units are empirically the healthy ones — 0 of 398 published empty
    /// over a non-empty base, against 14.5% for hour-wide units.
    #[test]
    fn the_hour_migration_leaves_a_slice_wider_than_an_hour_alone() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY: i64 = 24 * DERIVED_SLICE_MICROS;
        journal.upsert(task("p", 0, DAY, Operation::DerivedRollup));
        // A genuine legacy fragment alongside it, so the migration is not simply inert.
        journal.upsert(task("p", DAY, DAY + NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        journal.checkpoint().expect("checkpoint");

        let mut journal = TaskJournal::load(dir.path()).expect("journal to migrate");
        assert_eq!(journal.migrate_derived_slices(), 1, "only the sub-hour fragment may migrate");

        let day_wide = journal
            .tasks()
            .find(|task| task.key.operation == Operation::DerivedRollup && task.key.slice.width() == DAY)
            .expect("the day-wide unit must survive the migration");
        assert_ne!(day_wide.state, TaskState::Superseded, "a day-wide derived unit must not be superseded by the hour migration");
        assert!(
            !journal.tasks().any(|task| task.key.slice.start_micros == 0 && task.key.slice.width() == DERIVED_SLICE_MICROS),
            "the day must not be replaced by hour 00, which drops the other 23 hours"
        );
    }

    /// Rebuilding a BASE slice must reopen the DERIVED cell built over it.
    ///
    /// Without this edge a derived cell is a faithful aggregate of a base that no
    /// longer exists, and it never self-corrects because its unit is `Complete`.
    /// Prod 2026-08-28: `87576849`/08-01's 1h cell was written 09:44:32 and its
    /// 1m base was rebuilt 09:49:13-14:46:12 — the cell reads +70.6% over the
    /// base to this day.
    ///
    /// Three properties, because the reopen is the easy one:
    ///   1. a COMPLETE derived unit over the republished range goes back to Pending
    ///   2. its stale publication is dropped, so nothing recovers coverage from it
    ///   3. it TERMINATES — reopening does not itself mint more work. The
    ///      4,632-superseded-record incident came from resurrect logic that did.
    #[test]
    fn republishing_a_base_slice_reopens_the_derived_cell_over_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = DERIVED_SLICE_MICROS;
        let derived = |start: i64, end: i64| TaskKey {
            physical_table: "derived".into(),
            source: "src".into(),
            project_id: "p".into(),
            slice: TimeSlice::new(start, end).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        let publication = || Publication { source_fingerprint: 7, generation: "g".into(), rows: 5, source_rows: Some(9) };
        for (start, end) in [(0, HOUR), (HOUR, 2 * HOUR), (5 * HOUR, 6 * HOUR)] {
            let key = derived(start, end);
            journal.enqueue(key.clone(), 0, 1, 1);
            journal.publish(&key, publication());
            journal.complete(&key);
        }
        let before = journal.tasks().count();

        // A base unit republishes 00:00-02:00 — two of the three derived cells.
        let reopened = journal.reopen_derived_over("p", "derived", 0, 2 * HOUR);
        assert_eq!(reopened, 2, "exactly the derived cells overlapping the republished range");
        assert_eq!(journal.tasks().count(), before, "reopening must not MINT tasks — that is the resurrect loop");

        let state_of = |journal: &TaskJournal, start: i64| {
            journal
                .tasks()
                .find(|task| task.key.slice.start_micros == start && task.key.operation == Operation::DerivedRollup)
                .map(|t| (t.state, t.publication.is_some()))
        };
        assert_eq!(state_of(&journal, 0), Some((TaskState::Pending, false)), "reopened, and its stale publication dropped");
        assert_eq!(state_of(&journal, HOUR), Some((TaskState::Pending, false)), "reopened, and its stale publication dropped");
        assert_eq!(state_of(&journal, 5 * HOUR), Some((TaskState::Complete, true)), "a cell outside the republished range must be untouched");

        // Terminates: a second call over the same range has nothing left to do.
        assert_eq!(journal.reopen_derived_over("p", "derived", 0, 2 * HOUR), 0, "reopening is idempotent");
    }

    #[test]
    fn journal_round_trips_tasks_and_monotonic_cursors() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        journal.set_source_cursor("source".into(), 9);
        journal.set_source_cursor("source".into(), 7);
        journal.checkpoint().expect("checkpoint");
        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        assert_eq!(loaded.tasks().count(), 1);
        assert_eq!(loaded.source_cursor("source"), Some(9));
    }

    #[test]
    fn production_sized_wal_replay_updates_tasks_without_quadratic_scans() {
        const TASKS: i64 = 20_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
        for index in 0..TASKS {
            journal.upsert(task("large-project", index, index + 1, Operation::Dedup));
        }
        journal.checkpoint().expect("initial backlog checkpoint");
        for index in 0..TASKS {
            let key = task("large-project", index, index + 1, Operation::Dedup).key;
            assert!(journal.retry(&key, "restart_test".to_owned(), index));
        }
        journal.checkpoint().expect("updated backlog checkpoint");

        let loaded = TaskJournal::load(dir.path()).expect("replay large journal");
        assert_eq!(loaded.tasks().count(), usize::try_from(TASKS).expect("positive task count"));
        let last = task("large-project", TASKS - 1, TASKS, Operation::Dedup).key;
        assert_eq!(loaded.state(&last), Some(TaskState::Retry));
    }

    #[test]
    fn empty_rollup_publication_survives_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("new journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.publish(&key, Publication { source_fingerprint: 7, generation: "stable".to_owned(), rows: 0, source_rows: None }));
        journal.checkpoint().expect("checkpoint");

        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        let publication = loaded.published_rollups("source", "table").into_iter().next().expect("published empty slice").1;
        assert_eq!(publication.rows, 0);
        assert_eq!(publication.source_fingerprint, 7);
    }

    #[test]
    fn dropping_a_running_lease_durably_requeues_the_task() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.mark_running(&key));
        journal.checkpoint().expect("running checkpoint");
        let journal = Arc::new(Mutex::new(journal));
        let before_drop = crate::support::now_micros();
        drop(TaskLease::new(Arc::clone(&journal), key.clone()));

        let journal_guard = journal.lock().expect("lock");
        assert_eq!(journal_guard.state(&key), Some(TaskState::Retry));
        let task = journal_guard.snapshot.tasks.iter().find(|task| task.key == key).expect("requeued task");
        assert_eq!(task.retry_reason.as_deref(), Some("worker_error"));
        assert!(task.deadline_micros >= before_drop + 2_000_000, "first failed attempt must use the same exponential backoff as explicit worker errors");
        drop(journal_guard);
        let loaded = TaskJournal::load(dir.path()).expect("load checkpoint");
        assert_eq!(loaded.state(&key), Some(TaskState::Retry));
    }

    /// A completed unit must NOT be requeued by its lease, and the lease must
    /// read the outcome the run function recorded.
    ///
    /// This is the invariant behind `maintenance_task_finished`: the event is
    /// emitted from `Drop`, so it reports whatever `state` says at that moment.
    /// If the lease could not distinguish Complete from Running, the new event
    /// would report every successful unit as a death — which is worse than the
    /// silence it replaces, because it looks like data.
    #[test]
    fn a_completed_lease_reports_complete_and_is_not_requeued() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::SealedConsolidation);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.mark_running(&key));
        assert!(journal.complete(&key));
        let journal = Arc::new(Mutex::new(journal));
        drop(TaskLease::new(Arc::clone(&journal), key.clone()));

        let guard = journal.lock().expect("lock");
        assert_eq!(guard.state(&key), Some(TaskState::Complete), "a completed unit must survive its own lease drop");
        // And the debt field the event carries is absent on a unit that was
        // never preflighted — which is the state every unclaimed hygiene cell
        // is in, and the reason `benefit` ordering cannot see their debt.
        assert_eq!(guard.input_files(&key), None);
    }

    #[test]
    fn every_crash_boundary_recovers_to_redundant_work_or_published_coverage() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();

        // WAL/source invalidation checkpoint: a crash leaves pending work and
        // therefore no coverage claim.
        journal.upsert(input);
        journal.checkpoint().expect("invalidation checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after invalidation");
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));
        assert!(recovered.published_rollups("source", "table").is_empty());

        // Rollup staging / target-commit-before-coverage checkpoint: claims are
        // deliberately transient, so restart sees the last durable Pending
        // state directly. An already-landed target commit is safely replaced
        // by that redundant retry without a full-journal claim checkpoint.
        assert!(recovered.mark_running(&key));
        recovered.checkpoint().expect("running checkpoint");
        let mut recovered = TaskJournal::load(dir.path()).expect("recover after staging");
        assert_eq!(recovered.requeue_running(100), 0);
        assert_eq!(recovered.state(&key), Some(TaskState::Pending));

        // Coverage checkpoint is the only boundary that makes the slice
        // readable after restart, including an empty output.
        assert!(recovered.publish(&key, Publication { source_fingerprint: 9, generation: "g".to_owned(), rows: 0, source_rows: None }));
        recovered.checkpoint().expect("coverage checkpoint");
        let recovered = TaskJournal::load(dir.path()).expect("recover publication");
        assert_eq!(recovered.published_rollups("source", "table").len(), 1);
    }

    #[test]
    fn invalidation_is_idempotent_and_extends_the_quiet_period() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: 10,
                derived: false,
            })
            .expect("invalidate");
        journal.checkpoint().expect("first invalidation checkpoint");
        let first_wal_size = fs::metadata(&journal.wal_path).expect("wal").len();
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: 20,
                derived: false,
            })
            .expect("invalidate again");
        journal.checkpoint().expect("same-bucket checkpoint");
        assert_eq!(fs::metadata(&journal.wal_path).expect("wal").len(), first_wal_size, "same deadline bucket must not rewrite tasks");
        journal
            .invalidate(Invalidation {
                source_table: "source",
                rollup_table: "rollup",
                source: "source",
                project_id: "p",
                start_micros: 0,
                end_micros: NORMAL_SLICE_MICROS,
                observed_at_micros: INVALIDATION_DEADLINE_BUCKET_MICROS + 1,
                derived: false,
            })
            .expect("invalidate in next bucket");
        // Two, not three: Dedup and the rollup. HotPacking is planned by DEBT
        // in `plan_compaction_debt` (one day-wide unit per project, only when
        // the partition really has small or unsorted files), never per slice —
        // minting it here made file hygiene 22% of the journal with a count
        // that tracked ingest rather than fragmentation.
        assert_eq!(journal.tasks().count(), 2);
        assert!(
            journal.tasks().all(|task| task.key.operation != Operation::HotPacking),
            "ingest must not mint file-hygiene work per slice; the debt planner owns it"
        );
        assert!(journal.tasks().all(|task| task.deadline_micros == FINALIZATION_DELAY_MICROS + 2 * INVALIDATION_DEADLINE_BUCKET_MICROS));
    }

    #[test]
    fn running_tasks_are_requeued_after_a_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.mark_running(&key));
        assert_eq!(journal.requeue_running(42), 1);
        let task = journal.tasks().next().expect("task");
        assert_eq!(task.state, TaskState::Retry);
        assert_eq!(task.deadline_micros, 42);
    }

    #[test]
    fn claim_is_durable_state_not_an_in_memory_queue_pop() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(task("p", 0, MIN_SLICE_MICROS, Operation::Dedup));
        let claimed = journal.claim_next(Operation::Dedup, 0, true).expect("claim");
        assert_eq!(claimed.state, TaskState::Running);
        assert_eq!(claimed.attempts, 1);
        assert!(journal.claim_next(Operation::Dedup, 0, true).is_none());
    }

    /// Historical derived work must not lose every claim to the frontier.
    ///
    /// Prod 2026-08-19 00:10 UTC: 387 historical day-wide derived units had just
    /// been unblocked (#186/#188), the 1h tier they build was 9-17 days deep
    /// against a 33-day base tier — and EVERY derived unit claimed in the next
    /// 12 minutes was a one-hour frontier slice for today or yesterday. Class is
    /// strict priority and the frontier regenerates continuously, so without a
    /// reservation the historical units never run at all.
    #[test]
    fn a_sealed_derived_unit_is_not_starved_by_the_frontier() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 40 * 24 * 3_600_000_000i64;
        let derived = |project: &str, start: i64, width: i64| {
            let mut task = task(project, start, start + width, Operation::DerivedRollup);
            task.base_tier_present = true;
            task.deadline_micros = 0;
            task
        };
        // A one-hour frontier slice, and a day-wide sealed one ten days back.
        journal.upsert(derived("frontier", now - 3_600_000_000, 3_600_000_000));
        let sealed_start = now - 10 * 24 * 3_600_000_000;
        journal.upsert(derived("sealed", sealed_start, 24 * 3_600_000_000));

        // The frontier is behind, which is exactly when the general reservation
        // shrinks — and exactly when coverage needs the sealed unit most.
        journal.frontier_lag_secs.store(FRONTIER_LAG_BUDGET_SECS + 1, std::sync::atomic::Ordering::Relaxed);
        let claimed = journal.claim_next(Operation::DerivedRollup, now, true).expect("a derived unit is claimable");
        assert_eq!(claimed.key.project_id, "sealed", "historical derived work must win the claim, not today's");
    }

    /// A derived unit whose base TIER already exists must be claimable, even
    /// when no `BaseRollup` journal task records that it was built.
    ///
    /// Prod 2026-08-18 22:30 UTC: the 1m base tier was 33 days deep on most
    /// projects while the 1h derived tier it feeds sat at 9-17,
    /// `pending_derived_rollup` did not move by ONE task across two 240s windows
    /// with workers free, and every derived unit claimed in 20 minutes was a
    /// frontier slice whose base had completed minutes earlier. Historical days
    /// were unclaimable — `claim_next` skips a dependency-blocked task silently,
    /// with no counter and no log, which is the sixth silent refusal in this
    /// family.
    #[test]
    fn a_derived_unit_runs_when_its_base_tier_exists_without_a_journal_record() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let derived = |project: &str| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };

        // The status quo, and correct on its own terms: no completed base task
        // covers this slice, so the unit is refused.
        journal.enqueue(derived("historical"), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "without evidence the dependency gate still holds");

        // The backfill planner reads real tier coverage, so it can prove the
        // base tier is there. That must be enough.
        journal.enqueue_with_base_tier(derived("historical"), 0, 1, 0, true);
        assert_eq!(journal.claim_next(Operation::DerivedRollup, 0, true).expect("proven base tier makes the unit claimable").key.project_id, "historical");
    }

    /// The planner must be able to prove the base tier for a task it can no
    /// longer reach through `enqueue`.
    ///
    /// A blocked derived unit stays queued, which makes its day permanently
    /// ineligible for backfill admission (`want.retain(|key| !queued...)`),
    /// which is the only path that could have carried the proof. Prod
    /// 2026-08-18 22:50 UTC: `pending_derived_rollup` did not move after #184
    /// shipped, because all 759 tasks predated it.
    #[test]
    fn an_already_queued_derived_unit_can_still_be_told_its_base_tier_exists() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: blocked by the dependency gate");

        let day = (0, 24 * 3_600_000_000i64);
        assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 1, "the proof lands on an existing task");
        assert_eq!(journal.prove_base_tier_for_day(&key, day.0, day.1), 0, "and is idempotent");
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "the unit becomes claimable without being re-enqueued");
    }

    /// The claimability census must name each reason `claim_next` skips a task.
    ///
    /// Every counter in this system reports how much work EXISTS; none reported
    /// why an existing task is passed over, and `claim_next` decides that inside
    /// filter predicates that leave no trace. Five correct fixes shipped against
    /// wrong models of the queue before this existed.
    #[test]
    fn the_claimability_census_separates_the_reasons_a_task_is_skipped() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        let now = 40 * 24 * HOUR;
        let at = |project: &str, start: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(start, start + HOUR).expect("slice"),
            operation: Operation::DerivedRollup,
        };

        // Sealed and dependency-blocked: no completed BaseRollup covers it.
        journal.enqueue(at("blocked", now - 10 * 24 * HOUR), 0, 1, 0);
        // Sealed, proven, but not due yet.
        let later = at("not_due", now - 10 * 24 * HOUR);
        journal.enqueue(later.clone(), now + HOUR, 1, 0);
        journal.prove_base_tier_for_day(&later, now - 10 * 24 * HOUR, now - 9 * 24 * HOUR);
        // Sealed, proven, due, but has burned its attempts.
        let doomed = at("doomed", now - 11 * 24 * HOUR);
        journal.enqueue(doomed.clone(), 0, 1, 0);
        journal.prove_base_tier_for_day(&doomed, now - 11 * 24 * HOUR, now - 10 * 24 * HOUR);
        for _ in 0..TaskJournal::QUARANTINE_ATTEMPTS {
            assert!(journal.mark_running(&doomed));
            journal.retry(&doomed, TaskJournal::WORKER_FAILURE_REASON.to_owned(), 0);
        }

        let (pending, sealed, unproven, quarantined, not_due) = journal.claimability_census(Operation::DerivedRollup, now);
        assert_eq!(pending, 3, "every pending derived task is counted");
        assert_eq!(sealed, 3, "all three are older than the live frontier window");
        assert_eq!(unproven, 1, "only the one without a base-tier proof is dependency-blocked");
        assert_eq!(quarantined, 1, "only the one that burned its attempts is quarantined");
        assert_eq!(not_due, 1, "only the one with a future deadline is not yet due");
    }

    /// Sealed work that has waited too long must overtake newer sealed work —
    /// and only then.
    ///
    /// Prod 2026-08-19, from the compaction dashboard: 2026-08-13 sat at 167
    /// files for FOUR DAYS, the same count and the same four tenants, while six
    /// younger sealed days converged past it. Six successors overtaking a
    /// partition is not a queue draining slowly; it is one the order never
    /// reaches.
    ///
    /// Both halves matter. Oldest-first was tried and is recorded in
    /// `scheduling_class`'s own comment as sending 10 of 10 historical starts to
    /// data months old while the last 30 days went untouched — so fresh work
    /// must STILL be newest-first, and only genuinely starved tasks escalate.
    /// Sealed hygiene ranks by how much debt a claim RETIRES, not by date.
    ///
    /// Every hygiene unit is day-wide, so `-width` ties among them and the
    /// tie-break was pure recency — which says nothing about benefit. Prod
    /// 2026-08-23: four sealed cells held ~850 removable files in 4.9 GB while
    /// capacity went to whichever sealed most recently.
    #[test]
    fn sealed_hygiene_ranks_by_files_removed_not_by_date() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let now = 400 * DAY;
        let cell = |project: &str, hours_ago: i64, files: u32, operation| {
            let end = now - hours_ago * HOUR;
            let mut unit = task(project, end - DAY, end, operation);
            unit.input = Some(InputFootprint { fp: 1, whole_file_bytes: 1, files });
            unit
        };
        let class = |unit: &MaintenanceTask| super::scheduling_class(unit, now);

        // A big older cell beats a small newer one — the reversal.
        let big_old = cell("a", 60, 200, Operation::SealedConsolidation);
        let small_new = cell("b", 30, 3, Operation::SealedConsolidation);
        assert!(class(&big_old) < class(&small_new), "200 files outrank 3, whichever sealed first");
        // Within a band they TIE, so `fair_cursors` can still rotate projects
        // instead of one cell winning every claim.
        assert_eq!(
            class(&cell("a", 60, 200, Operation::SealedConsolidation)).3,
            class(&cell("b", 30, 210, Operation::SealedConsolidation)).3,
            "comparable cells must tie"
        );
        // Unknown benefit orders LAST, never first: a journal written before the
        // field must not jump the queue.
        let mut unknown = cell("c", 60, 0, Operation::SealedConsolidation);
        unknown.input = None;
        assert!(class(&big_old) < class(&unknown));
        // And benefit is hygiene-only — it must not perturb rollup ordering,
        // which damage repair relies on tying.
        assert_eq!(class(&cell("d", 60, 200, Operation::BaseRollup)).3, class(&cell("e", 60, 3, Operation::BaseRollup)).3);
    }

    /// A `SealedConsolidation` unit is never the live frontier, by construction.
    ///
    /// `plan_compaction_debt` picks the operation from the calendar — `date ==
    /// today` mints HotPacking, anything older mints SealedConsolidation — but
    /// `scheduling_class` asked `is_live_frontier`, which stays true for a full
    /// `LIVE_FRONTIER_WINDOW_MICROS` (24 h) after the slice ENDS. So yesterday's
    /// consolidation unit held class 0 — strict priority over every genuinely
    /// sealed cell — until it was 48 h old, and the two halves of the system
    /// disagreed about the same date for a whole day.
    ///
    /// Measured on prod 2026-08-24, a mature container over 68 minutes: **26 of
    /// 27** SealedConsolidation claims went to 2026-08-23, while 48 out-of-policy
    /// cells — one of them holding 238 small files — took none. Class is strict
    /// priority, so benefit ordering never got a say: `scheduling_class`'s own
    /// comment promises "a cell worth 200 files outranks one worth 3", and class
    /// was silently overriding it.
    #[test]
    fn a_sealed_consolidation_unit_is_never_the_live_frontier() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        // Late morning, so yesterday's slice ended 11 h ago — inside the 24 h
        // frontier window, which is the window the defect lives in.
        let now = 400 * DAY + 11 * HOUR;
        let cell = |project: &str, start: i64, files: u32, operation| {
            let mut unit = task(project, start, start + DAY, operation);
            unit.input = Some(InputFootprint { fp: 1, whole_file_bytes: 1, files });
            unit
        };
        let yesterday = cell("small", 399 * DAY, 3, Operation::SealedConsolidation);
        let five_days_old = cell("bigdebt", 395 * DAY, 238, Operation::SealedConsolidation);

        assert_eq!(super::scheduling_class(&yesterday, now).0, 1, "the planner mints SealedConsolidation only for a date it already treats as sealed");
        assert!(
            super::scheduling_class(&five_days_old, now) < super::scheduling_class(&yesterday, now),
            "238 files of debt must outrank 3 — which class 0 was silently preventing"
        );

        // The blast radius. Today's packing IS frontier work and must stay class
        // 0, or this trades one starvation for another.
        assert_eq!(super::scheduling_class(&cell("today", 400 * DAY, 9, Operation::HotPacking), now).0, 0, "today's packing is genuinely live-frontier work");
    }

    /// "Planned and never claimed" must name what beat it.
    ///
    /// Prod 2026-08-24: `87576849 / 2026-08-19` held 238 small files — the
    /// largest file-debt cell in the fleet — read exactly 238 at four
    /// object-storage censuses spanning a day, and appeared in NO log line of any
    /// kind across 73 minutes and three containers. `680acac` instrumented "a
    /// unit was claimed and selected nothing"; this is the other branch, and it
    /// was invisible rather than explained.
    ///
    /// The `outranked_by` arm is the one no existing instrument could reach:
    /// `claimability_census` counts per-task reasons and `first_refused_sealed`
    /// returns a bare `CLAIMABLE` — true, and useless, because the refusal is in
    /// `benefit` is per UNIT, so any future SPLITTING of a cell divides its debt.
    ///
    /// A LATENT hazard, not the cause of the 2026-08-28 starvation — that was
    /// misdiagnosed twice before the numbers settled it, so both the property and
    /// its limits are pinned here.
    ///
    /// The property is real: `benefit = -(input.files / BENEFIT_BUCKET_FILES)` is
    /// computed per unit, so if one cell is ever cut into slices, each slice
    /// reports a fraction of the debt and the cell sinks in the ordering — and
    /// heavy debt is exactly what makes a cell a split candidate. Any change that
    /// makes hygiene units narrower than one (project, date) must fix this first.
    ///
    /// It is NOT what starved `otel_logs_and_spans` on 2026-08-28. Hygiene units
    /// there are per (project, date): prod's worst cell reported `files=433`,
    /// which is that cell's *entire* file count. Nothing was split.
    #[test]
    fn splitting_a_cell_would_divide_its_benefit_and_invert_the_ordering() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let now = 400 * DAY;
        let unit = |project: &str, days_ago: i64, slice: i64, slices: i64, files: u32| {
            let day_end = now - days_ago * DAY;
            let width = DAY / slices;
            let end = day_end - slice * width;
            let mut t = task(project, end - width, end, Operation::SealedConsolidation);
            t.input = Some(InputFootprint::new((0..files).map(|n| format!("{project}/{days_ago}/{slice}/{n}.parquet")), 1));
            t
        };

        // One cell per day — how hygiene actually runs. Benefit tracks real debt.
        let dir = tempfile::tempdir().expect("temp dir");
        let mut whole = TaskJournal::load(dir.path()).expect("journal");
        whole.enqueue_planned(&unit("metrics", 6, 0, 1, 261));
        whole.enqueue_planned(&unit("logs", 3, 0, 1, 964));
        let first = whole.claim_next(Operation::SealedConsolidation, now, true).expect("a claim");
        assert_eq!(first.key.project_id.as_str(), "logs", "unsplit, the 964-file cell correctly outranks 261");

        // The same debt, split four ways, now loses to a cell holding a quarter
        // as much — the debt did not change, only its packaging.
        let dir2 = tempfile::tempdir().expect("temp dir 2");
        let mut split = TaskJournal::load(dir2.path()).expect("journal");
        split.enqueue_planned(&unit("metrics", 6, 0, 1, 261));
        for slice in 0..4 {
            split.enqueue_planned(&unit("logs", 3, slice, 4, 964 / 4));
        }
        assert_eq!(
            split.claim_next(Operation::SealedConsolidation, now, true).expect("a claim").key.project_id.as_str(),
            "metrics",
            "split four ways, a 964-file day is outranked by a 261-file one"
        );

        // And `most_indebted_unclaimed` is blinded the same way: it ranks by
        // per-unit files, so the largest is now the metrics cell, which wins its
        // own claim — so nothing is reported starved at all.
        assert!(
            split.most_indebted_unclaimed(Operation::SealedConsolidation, now).is_none(),
            "the instrument cannot see a starved cell whose debt has been divided below its rivals"
        );
    }

    /// The age window demotes the fleet's BIGGEST debts, at prod's real numbers.
    ///
    /// `starved` is `0` only for work aged 3-31 days and is compared BEFORE
    /// `benefit`, so a cell that is merely young loses regardless of how much it
    /// holds. Measured 2026-08-28, the three largest cells in the fleet were all
    /// 1-2 days old and therefore all demoted:
    ///
    ///     433 files  otel_logs_and_spans 28f62f01 08-27   1 day   starved=1
    ///     294 files  otel_logs_and_spans 28f62f01 08-26   2 days  starved=1
    ///     283 files  otel_metrics        8100121c 08-27   1 day   starved=1
    ///
    /// while a 238-file cell three days old was eligible. Prod's instrument said
    /// exactly this: `outranked_by:8100121c:2026-08-24:28f62f01:2026-08-27:files=433`.
    #[test]
    fn the_starvation_window_demotes_the_biggest_debt_when_it_is_young() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let now = 400 * DAY;
        let cell = |project: &str, days_ago: i64, files: u32| {
            let end = now - days_ago * DAY;
            let mut t = task(project, end - DAY, end, Operation::SealedConsolidation);
            t.input = Some(InputFootprint::new((0..files).map(|n| format!("{project}/{n}.parquet")), 1));
            t
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.enqueue_planned(&cell("biggest-but-young", 1, 433));
        journal.enqueue_planned(&cell("smaller-but-aged", 3, 238));

        let first = journal.claim_next(Operation::SealedConsolidation, now, true).expect("a claim");
        assert_eq!(first.key.project_id.as_str(), "smaller-but-aged", "a 238-file cell wins over a 433-file one solely because the bigger one is 1 day old");
    }

    /// the ORDERING and neither reports the winner.
    #[test]
    fn the_most_indebted_hygiene_cell_names_what_outranks_it() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let now = 400 * DAY;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Shaped exactly like `plan_compaction_debt`: build the unit with the
        // footprint it selected on, then hand it to `enqueue_planned`. The old
        // fixture set `input` on a task it `upsert`ed — the one path production
        // does not take — so it passed while prod read `files=0` on every sample.
        let cell = |project: &str, hours_ago: i64, files: u32| {
            let end = now - hours_ago * HOUR;
            let mut unit = task(project, end - DAY, end, Operation::SealedConsolidation);
            unit.input = Some(InputFootprint::new((0..files).map(|n| format!("{project}/{n}.parquet")), 1));
            unit
        };
        // The biggest debt sealed only a day ago, so it is NOT in the starvation
        // band; a much smaller cell has been waiting five days and is. `starved`
        // is compared before `benefit`, so the older one legitimately wins and
        // the 238-file cell waits — which is the ordering this instrument exists
        // to make legible.
        //
        // The smaller cell is enqueued FIRST, deliberately: with `files` unset
        // the selection is `max_by_key` over zeroes, which returns whichever the
        // iterator reached first. Naming the right cell then proves nothing —
        // prod named a genuinely indebted cell while reporting `files=0`, purely
        // by journal order. Insertion order must contradict the answer.
        let indebted = cell("bigdebt", 24, 238);
        journal.enqueue_planned(&cell("starved", 120, 10));
        journal.enqueue_planned(&indebted);

        let refusal = journal.most_indebted_unclaimed(Operation::SealedConsolidation, now).expect("the debt is not being claimed");
        assert!(refusal.starts_with("outranked_by:starved"), "it must name the winner, not merely say CLAIMABLE — got {refusal}");
        assert!(refusal.contains("files=238"), "and it must carry the debt that makes it worth reporting — got {refusal}");

        // Eligibility reasons still win over the ordering answer, because they
        // are actionable on the task itself. (A fresh journal, because `enqueue`
        // only ever pulls a deadline EARLIER.)
        let mut later = TaskJournal::load(dir.path()).expect("journal");
        let mut not_due = indebted.clone();
        not_due.deadline_micros = now + DAY;
        later.enqueue_planned(&not_due);
        assert!(
            later.most_indebted_unclaimed(Operation::SealedConsolidation, now).is_some_and(|why| why.starts_with("not_due:")),
            "a future deadline explains it without appealing to ordering"
        );

        // And silence when there is nothing to explain: the biggest debt winning
        // its own claims is the healthy state, not a finding.
        let mut alone = TaskJournal::load(dir.path()).expect("journal");
        alone.enqueue_planned(&indebted);
        assert_eq!(alone.most_indebted_unclaimed(Operation::SealedConsolidation, now), None);
    }

    /// Which footprint wins, and when. Both are honest measurements.
    ///
    /// The planner re-derives the live file set every 60 s; the claim-time
    /// preflight measures what a running unit actually read. `None` must never
    /// erase either — every other `enqueue` caller passes it, and stripping a
    /// claim-time footprint breaks the bisect ladder `record_input` documents.
    #[test]
    fn a_planned_footprint_heals_a_pending_cell_and_never_erases_a_measured_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut unit = task("p", 0, DAY_MICROS, Operation::SealedConsolidation);

        // The backlog heal: a cell queued before the planner carried a footprint
        // must not need a first claim to become rankable.
        journal.enqueue(unit.key.clone(), 0, 1, 0);
        assert_eq!(journal.input_files(&unit.key), None);
        unit.input = Some(InputFootprint::new(["a", "b", "c"], 1));
        journal.enqueue_planned(&unit);
        assert_eq!(journal.input_files(&unit.key), Some(3), "the next planner tick must supply the count");

        // And a plain re-enqueue leaves it alone rather than zeroing it.
        journal.enqueue(unit.key.clone(), 0, 1, 0);
        assert_eq!(journal.input_files(&unit.key), Some(3), "`None` is silence, not evidence of no files");

        // While the unit RUNS, the claim-time measurement is the one that counts:
        // it is what `abandon_running` bisects on.
        let claimed = journal.claim_next(Operation::SealedConsolidation, 0, false).expect("claimable");
        journal.record_input(&claimed.key, InputFootprint::new(["a", "b"], 1));
        journal.enqueue_planned(&unit);
        assert_eq!(journal.input_files(&unit.key), Some(2), "a planner tick must not clobber a running unit's own measurement");
    }

    #[test]
    fn sealed_work_ages_out_of_starvation_without_becoming_oldest_first() {
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let now = 400 * DAY;
        // Age comes from the SLICE, so a case is described by how long ago its
        // day sealed. Anything sealing within 24 h is the live frontier and is
        // class 0 regardless — these are all older than that.
        let sealed_hours_ago = |project: &str, hours: i64| {
            let end = now - hours * HOUR;
            task(project, end - DAY, end, Operation::SealedConsolidation)
        };

        // Inside the overdue threshold: recent sealed days stay newest-first,
        // which is what a dashboard reads.
        let recent_new = super::scheduling_class(&sealed_hours_ago("a", 30), now);
        let recent_old = super::scheduling_class(&sealed_hours_ago("b", 60), now);
        assert!(recent_new < recent_old, "among days not yet overdue the newest still leads");

        // Past it, a day is backlog and overtakes those recent days.
        let overdue = super::scheduling_class(&sealed_hours_ago("c", 10 * 24), now);
        assert!(overdue < recent_new, "a day overdue past the threshold overtakes newer sealed work");

        // The backlog drains from its OLD end — the half that was missing, and
        // why 2026-08-13 sat at 167 files while seven younger days passed it.
        let overdue_older = super::scheduling_class(&sealed_hours_ago("d", 30 * 24), now);
        assert!(overdue_older < overdue, "a backlog drains oldest-first: the older overdue day leads");

        // A day-wide unit still beats a narrow one covering the SAME day, which
        // is what lets certification get the day-wide dedup unit it requires.
        let end = now - 30 * 24 * HOUR;
        let narrow = super::scheduling_class(&task("e", end - NORMAL_SLICE_MICROS, end, Operation::SealedConsolidation), now);
        assert!(overdue_older < narrow, "width breaks the tie: the day-wide unit leads its own day's slices");

        // Past the horizon a day keeps escalating rather than falling off it.
        // This pair asserted the opposite until 2026-08-25 — it codified the
        // defect: the cut-off left 1,237 units permanently unclaimable and
        // `oldest_task_age_seconds` pinned at 85 days. What the horizon still
        // buys is the FLAT band beneath it: every day inside the goal window
        // ties on age, so `hole`, `-width` and `benefit` decide there, and only
        // the tail the cut-off had abandoned gains rank over them.
        let ancient = super::scheduling_class(&sealed_hours_ago("f", 60 * 24), now);
        assert!(ancient < recent_new, "past STARVATION_HORIZON_MICROS a day still escalates, it does not fall off");

        let outside_goal_window = super::scheduling_class(&sealed_hours_ago("h", 40 * 24), now);
        let inside_goal_window = super::scheduling_class(&sealed_hours_ago("i", 25 * 24), now);
        assert!(outside_goal_window < inside_goal_window, "a day outside the window is behind in the drain, not beneath it");
        // ...and inside the window age is FLAT, so the terms the band has learned
        // still order it: 25 days and 10 days tie on `starved`.
        let (_, inside_starved, ..) = inside_goal_window;
        let (_, ten_days_starved, ..) = super::scheduling_class(&sealed_hours_ago("j", 10 * 24), now);
        assert_eq!(inside_starved, ten_days_starved, "the goal window is one band: `hole`/`width`/`benefit` order inside it");

        // And starvation never lets sealed work outrank the live frontier.
        let frontier = super::scheduling_class(&task("g", now - 600_000_000, now, Operation::BaseRollup), now);
        assert!(frontier < overdue_older, "class still leads: the frontier outranks even overdue sealed work");
    }

    /// Age must keep accruing rank past the horizon instead of falling off it.
    ///
    /// The horizon used to be a CLIFF: `starved` was 0 only inside [3d, 31d],
    /// and it sits ahead of `hole`/`width`/`benefit` in a strict-priority tuple,
    /// so a slice one day past the horizon lost to everything inside it and was
    /// never compared on any other term. Prod 2026-08-25: 1,237 units older than
    /// 31 days were permanently unclaimable, `oldest_task_age_seconds` was 85
    /// days and could not decrease — 40 minutes of `maintenance_task_started`
    /// showed ZERO claims anywhere in 05-30..07-20. It cannot self-heal either:
    /// the 3-31d band is refilled every midnight and its timed-out residue
    /// crosses the cliff (1,019 of the tail at attempts=0, but 207 at
    /// attempts=1 — tried inside the window, then aged out).
    #[test]
    fn age_past_the_starvation_horizon_keeps_accruing_rank() {
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 800 * DAY;
        // Day-wide, no footprint: width and benefit tie, so only age can order these.
        let aged = |project: &str, days: i64| {
            let end = now - days * DAY;
            super::scheduling_class(&task(project, end - DAY, end, Operation::SealedConsolidation), now)
        };

        assert!(aged("ancient", 71) < aged("recent", 10), "a 71-day-old unit must not rank behind a 10-day-old one");
        assert!(aged("settling", 2) > aged("recent", 10), "the 3-day floor holds: work still settling does not jump the queue");

        // Monotonic past the floor: older never ranks worse than younger, and the
        // grade saturates so ancient work ties rather than fanning out forever.
        let ladder: Vec<_> = [4, 10, 31, 32, 71, 300, 400].into_iter().map(|days| aged("p", days)).collect();
        assert!(ladder.windows(2).all(|pair| pair[1] <= pair[0]), "rank must be non-increasing in age: {ladder:?}");
        // Saturation ties the GRADED term — the tuple below it still drains them
        // oldest-first, which is what keeps the far tail converging rather than
        // fanning into one sole winner of every claim.
        let (_, at_300, ..) = aged("p", 300);
        let (_, at_400, ..) = aged("p", 400);
        assert_eq!((at_300, at_400), (0, 0), "the grade saturates instead of running out of `u8`");
    }

    /// A hole must be claimed before a day that already has tier output.
    ///
    /// Sealed rollup work is otherwise strictly newest-first, and recent days
    /// are re-invalidated continuously by ongoing publication, so the claim
    /// never walks back far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f`'s 1h tier jumped 2026-07-31 -> 08-14 for a second day running
    /// while day-wide derived units for 08-17 were claimed over and over.
    /// Newest-first is right for FRESHNESS and wrong for CONTIGUITY, and 30
    /// contiguous days is a contiguity goal.
    #[test]
    fn a_hole_outranks_a_day_that_already_has_tier_output() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let at = |project: &str, day: i64| {
            let mut t = task(project, day * DAY, day * DAY + DAY, Operation::DerivedRollup);
            t.key.physical_table = "rollup_1h".to_owned();
            t.deadline_micros = 0;
            // Freshly planned, so neither is starved — this test is about
            // hole-vs-recency, and leaving `created_unix_ms` at 0 would make
            // both maximally starved and decide the order on age instead.
            t.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            t
        };
        // Both are SEALED and both are overdue, so backlog order applies and the
        // OLDER day leads on age alone. The hole is deliberately put on the
        // NEWER day, so `fills_a_hole` has to beat that ordering rather than
        // merely agree with it — a control that agreed would prove nothing.
        journal.upsert(at("recent", 35));
        journal.upsert(at("oldhole", 20));
        journal.set_base_tier_ready(HashSet::from([
            ("source".to_owned(), "recent".to_owned(), "1970-02-05".to_owned()),
            ("source".to_owned(), "oldhole".to_owned(), "1970-01-21".to_owned()),
        ]));

        // With no hole information, the older overdue day leads on age.
        assert_eq!(journal.claim_next(Operation::DerivedRollup, now, true).expect("claim").key.project_id, "oldhole");

        // Told which cell is a hole, the hole goes first instead.
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.upsert(at("recent", 35));
        journal.upsert(at("oldhole", 20));
        journal.set_base_tier_ready(HashSet::from([
            ("source".to_owned(), "recent".to_owned(), "1970-02-05".to_owned()),
            ("source".to_owned(), "oldhole".to_owned(), "1970-01-21".to_owned()),
        ]));
        journal.set_tier_holes(HashSet::from([("source".to_owned(), "recent".to_owned(), "rollup_1h".to_owned(), "1970-02-05".to_owned())]));
        assert_eq!(
            journal.claim_next(Operation::DerivedRollup, now, true).expect("claim").key.project_id,
            "recent",
            "a missing day must outrank an OLDER day that already has output — holes rank above backlog age"
        );
    }

    /// A partition still holding UNTAGGED tier files is a hole, whatever else
    /// is live in it.
    ///
    /// Measured on prod 2026-08-22. Every one of the 08-19 damaged partitions
    /// had its day-wide rebuild already queued, already past its deadline, at
    /// `attempts=0`, and cheap enough to run unsplit (268-537 MB) — and none had
    /// been claimed in three days. The reason is this rank: those cells DO have
    /// tier output, so `fills_a_hole` was false and they sat behind ~12,000
    /// hole-filling units that the backfill mints faster than it drains. The
    /// untagged files themselves cannot be certified, so the partition is
    /// missing coverage no matter how much output sits beside them.
    #[test]
    fn a_partition_holding_untagged_files_outranks_a_re_derive() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let at = |project: &str, day: i64| {
            let mut t = task(project, day * DAY, day * DAY + DAY, Operation::BaseRollup);
            t.key.physical_table = "rollup_1m".to_owned();
            t.deadline_micros = 0;
            t.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            t
        };
        // The damaged cell is deliberately the one that loses every other tie:
        // it is the NEWER day and its project sorts last, so only the untagged
        // rank can put it first.
        let seed = |journal: &mut TaskJournal| {
            journal.upsert(at("aaa-clean", 20));
            journal.upsert(at("zzz-damaged", 35));
        };
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        assert_eq!(journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id, "aaa-clean");

        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        journal.set_untagged_cells("source", "rollup_1m", [("zzz-damaged".to_owned(), "1970-02-05".to_owned())]);
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "zzz-damaged",
            "a partition holding unretirable untagged files must outrank a day that is merely being re-derived"
        );
    }

    /// Damage repair leads a missing day, which leads a re-derive.
    ///
    /// The middle rank is not enough on its own: a repair unit targets one
    /// file's uncovered span — 39 to 308 minutes on prod 2026-08-22 — so the
    /// `-width` tiebreak ranks it below every day-wide backfill hole it shares a
    /// rank with. That is why the damaged cells drained at ~2.4 files/hour.
    #[test]
    fn damage_outranks_a_missing_day_which_outranks_a_re_derive() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        // The damaged unit is deliberately the NARROWEST and the newest, so it
        // loses every other tiebreak and only the rank can put it first.
        let seed = |journal: &mut TaskJournal| {
            let mut narrow = task("damaged", 35 * DAY, 35 * DAY + 600_000_000, Operation::BaseRollup);
            narrow.key.physical_table = "rollup_1m".to_owned();
            narrow.deadline_micros = 0;
            narrow.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            let mut wide_hole = task("missing", 20 * DAY, 21 * DAY, Operation::BaseRollup);
            wide_hole.key.physical_table = "rollup_1m".to_owned();
            wide_hole.deadline_micros = 0;
            wide_hole.created_unix_ms = narrow.created_unix_ms;
            journal.upsert(narrow);
            journal.upsert(wide_hole);
            journal.set_tier_holes(HashSet::from([("source".to_owned(), "missing".to_owned(), "rollup_1m".to_owned(), "1970-01-21".to_owned())]));
        };
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);

        // Sharing one rank, the day-wide hole wins on width — the old behaviour.
        assert_eq!(journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id, "missing");

        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-02-05".to_owned())]);
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "damaged",
            "a ten-minute damage repair must outrank a day-wide backfill hole"
        );
    }

    /// Coarsening must not eat a damage repair.
    ///
    /// A repair unit is sized to one file's uncovered span, so fusing it
    /// destroys the only work that closes that hole — and the fused unit does
    /// not replace it, because the preflight measures it over budget and shreds
    /// it back down. Prod 2026-08-23: the three units covering `dcad860a`
    /// 08-15's eleven-minute hole (5m, 6m, 8m, Pending, eligible, attempts=0)
    /// had VANISHED from the journal hours later, leaving only completed units
    /// on either side of the hole. Five such cells sat at 3-11 minutes all day.
    #[test]
    fn coarsening_leaves_damage_repairs_alone() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let unit = |project: &str, start: i64, width: i64| {
            let mut t = task(project, start, start + width, Operation::BaseRollup);
            t.key.physical_table = "rollup_1m".to_owned();
            t.deadline_micros = 0;
            t.estimated_decoded_bytes = 1;
            t.created_unix_ms = 0;
            t
        };
        // Two narrow sealed units in the same bucket, in different cells: the
        // ordinary one is fair game to fuse, the damaged one is not.
        let seed = |journal: &mut TaskJournal| {
            journal.upsert(unit("ordinary", 10 * DAY, 300_000_000));
            journal.upsert(unit("ordinary", 10 * DAY + 600_000_000, 300_000_000));
            journal.upsert(unit("damaged", 10 * DAY, 300_000_000));
            journal.upsert(unit("damaged", 10 * DAY + 600_000_000, 300_000_000));
        };
        let survives = |journal: &TaskJournal, project: &str| {
            journal.tasks().filter(|task| task.key.project_id == project && task.key.slice.width() == 300_000_000).count()
        };

        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-01-11".to_owned())]);
        journal.coarsen_sealed_slices(now);
        assert_eq!(survives(&journal, "damaged"), 2, "a damaged cell's repair units must survive coarsening");
        assert_eq!(survives(&journal, "ordinary"), 0, "control: ordinary narrow sealed units are still fused");
    }

    /// Nor may SUBSUMPTION eat one, which is the other half of the same pass.
    ///
    /// A wider pending unit does not replace the repair it swallows: the
    /// preflight measures that one over budget and shreds it back down. Prod
    /// 2026-08-23, after exempting only fusion: five cells still had NO unit
    /// covering their hole — `dcad860a` 08-15's units began at 18:11 against a
    /// hole of 18:00-18:11.
    #[test]
    fn subsumption_leaves_damage_repairs_alone() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let unit = |project: &str, start: i64, width: i64| {
            let mut t = task(project, start, start + width, Operation::BaseRollup);
            t.key.physical_table = "rollup_1m".to_owned();
            t.deadline_micros = 0;
            t.estimated_decoded_bytes = 1;
            t
        };
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A narrow repair inside a much wider pending unit for the same cell —
        // exactly what subsumption exists to delete.
        for project in ["damaged", "ordinary"] {
            journal.upsert(unit(project, 10 * DAY, DAY));
            journal.upsert(unit(project, 10 * DAY + 600_000_000, 300_000_000));
        }
        journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-01-11".to_owned())]);
        journal.coarsen_sealed_slices(now);
        let narrow = |project: &str| journal.tasks().filter(|t| t.key.project_id == project && t.key.slice.width() == 300_000_000).count();
        assert_eq!(narrow("damaged"), 1, "the repair must survive a wider pending unit that would subsume it");
        assert_eq!(narrow("ordinary"), 0, "control: an ordinary narrow unit inside a wider one is still subsumed");
    }

    /// Damage ROTATES across cells; one cell's ladder cannot monopolise it.
    ///
    /// Both width orderings starve, because the selection loop matches the
    /// winning rank tuple EXACTLY, so any width ordering makes a single unit win
    /// every claim. Widest-first buried the narrow repair units (three 5-8
    /// minute units Pending at attempts=0, deadlines nine hours past);
    /// narrowest-first buried everyone behind one whale cell whose shredded
    /// ladder always held a narrower child — five cells with 3-11 minute holes
    /// sat untouched for eight hours. Tying every damage unit puts them in one
    /// rank group so `fair_cursors` rotates across projects.
    #[test]
    fn damage_rotates_across_cells_instead_of_draining_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let unit = |project: &str, start: i64, width: i64| {
            let mut t = task(project, start, start + width, Operation::BaseRollup);
            t.key.physical_table = "rollup_1m".to_owned();
            t.deadline_micros = 0;
            t.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            t
        };
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A whale ladder of very narrow children, and ONE other cell holding a
        // slightly wider hole — the prod shape exactly.
        for minute in 0..6 {
            journal.upsert(unit("whale", 35 * DAY + minute * 60_000_000, 60_000_000));
        }
        journal.upsert(unit("small", 35 * DAY, 180_000_000));
        journal.set_untagged_cells("source", "rollup_1m", [("whale".to_owned(), "1970-02-05".to_owned()), ("small".to_owned(), "1970-02-05".to_owned())]);

        // Six claims: the other cell must get a turn, not wait for the ladder.
        let claimed: Vec<String> = (0..6).filter_map(|_| journal.claim_next(Operation::BaseRollup, now, true).map(|task| task.key.project_id)).collect();
        assert!(claimed.iter().any(|project| project == "small"), "one cell's ladder must not monopolise damage repair; claimed {claimed:?}");
    }

    /// A restored cell ranks exactly like a freshly discovered one.
    ///
    /// The repair units are durable in this journal; their PRIORITY was not,
    /// and prod restarted four times an hour on 2026-08-23, so the rank was
    /// never active. `restore_untagged_cells` is what closes that, and it is
    /// only worth anything if the restored entries rank identically.
    #[test]
    fn a_restored_untagged_cell_ranks_like_a_discovered_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut damaged = task("zzz-damaged", 35 * DAY, 35 * DAY + 600_000_000, Operation::BaseRollup);
        damaged.key.physical_table = "rollup_1m".to_owned();
        damaged.deadline_micros = 0;
        damaged.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
        let mut clean = task("aaa-clean", 20 * DAY, 21 * DAY, Operation::BaseRollup);
        clean.key.physical_table = "rollup_1m".to_owned();
        clean.deadline_micros = 0;
        clean.created_unix_ms = damaged.created_unix_ms;
        journal.upsert(damaged);
        journal.upsert(clean);
        // Restored from the sidecar rather than set by a recovery pass.
        journal.restore_untagged_cells([("source".to_owned(), "zzz-damaged".to_owned(), "rollup_1m".to_owned(), "1970-02-05".to_owned())]);
        assert_eq!(journal.untagged_cells().count(), 1, "the restored cell must be readable back for persisting");
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "zzz-damaged",
            "a cell restored from the sidecar must carry the damage rank, or a restart un-prioritises the repair"
        );
    }

    /// Damage outranks starvation, because the starvation window is a
    /// FRESHNESS heuristic and damage is not a freshness question.
    ///
    /// `starved` grades age and is compared BEFORE `hole_rank`, so a damaged
    /// cell whose age loses is unreachable however damaged it is. Every untagged
    /// file left on prod 2026-08-23 was 32 to 37 days old and sorted below the
    /// whole ~12,000-unit backfill queue.
    ///
    /// The age that loses used to be "past the 31-day horizon"; since the
    /// horizon became a slope (2026-08-25) old work no longer loses on age at
    /// all, so the damaged cell here is a SETTLING one — two days sealed, under
    /// `STARVATION_MICROS` — which is now the only way age can rank a cell last.
    /// Its previous shape made the control vacuous: the 36-day cell won on age
    /// alone and the damage flag proved nothing.
    #[test]
    fn damage_outranks_work_inside_the_starvation_window() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAY: i64 = 24 * 3_600_000_000;
        let now = 40 * DAY;
        let seed = |journal: &mut TaskJournal| {
            // Two days sealed: under the floor, so it loses to anything in the
            // band — and past LIVE_FRONTIER_WINDOW_MICROS, so still class 1.
            let mut settling = task("damaged", 37 * DAY, 38 * DAY, Operation::BaseRollup);
            settling.key.physical_table = "rollup_1m".to_owned();
            settling.deadline_micros = 0;
            settling.created_unix_ms = u64::try_from(now.div_euclid(1_000)).unwrap_or_default();
            let mut inside = task("recent", 30 * DAY, 31 * DAY, Operation::BaseRollup);
            inside.key.physical_table = "rollup_1m".to_owned();
            inside.deadline_micros = 0;
            inside.created_unix_ms = settling.created_unix_ms;
            journal.upsert(settling);
            journal.upsert(inside);
        };
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "recent",
            "control: work inside the starvation window leads work still settling under the floor"
        );

        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        seed(&mut journal);
        journal.set_untagged_cells("source", "rollup_1m", [("damaged".to_owned(), "1970-02-07".to_owned())]);
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, now, true).expect("claim").key.project_id,
            "damaged",
            "a damaged cell the age ordering ranks last must still lead, or it is unreachable"
        );
    }

    /// Each (source, tier) owns its own slice of the untagged set.
    ///
    /// `recover_rollup_coverage` runs per source AND per tier, so a wholesale
    /// replace would leave the journal holding only whichever tier ran last —
    /// the exact bug that made `base_tier_ready` and `tier_holes` inert on prod
    /// (`base_tier_ready=374` then `272`, each wiping the other).
    #[test]
    fn setting_untagged_cells_replaces_only_that_sources_tier() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        journal.set_untagged_cells("logs", "logs_1m", [("p".to_owned(), "2026-08-19".to_owned())]);
        journal.set_untagged_cells("metrics", "metrics_1m", [("p".to_owned(), "2026-08-19".to_owned())]);
        assert_eq!(journal.untagged_cells_len(), 2, "a second source must not wipe the first");
        journal.set_untagged_cells("metrics", "metrics_1m", []);
        assert_eq!(journal.untagged_cells_len(), 1, "an emptied tier clears its own cells and only those");
    }

    /// The day-keyed ready set must unblock a derived task of ANY width.
    ///
    /// Three attempts to carry this fact as a per-task flag were inert (#184,
    /// #186, #195) because the flag had to land on exactly the right `TaskKey`
    /// and the queued work is not the width the planner assumes. Prod
    /// 2026-08-19 06:30: `derived_unproven=674` of `derived_pending=674` — the
    /// flag had never been set on ONE pending task. A day is what the fact is
    /// about, so keying on it cannot miss a task.
    #[test]
    fn the_base_tier_ready_set_unblocks_derived_work_of_any_width() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        let at = |start: i64, width: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(start, start + width).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        // An hour-wide task and a day-wide one, both inside 1970-01-01.
        journal.enqueue(at(0, HOUR), 0, 1, 0);
        journal.enqueue(at(5 * HOUR, HOUR), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: dependency-blocked");

        journal.set_base_tier_ready(HashSet::from([("source".to_owned(), "p".to_owned(), "1970-01-01".to_owned())]));
        assert_eq!(journal.base_tier_ready_len(), 1);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "an hour-wide task is unblocked by a DAY-keyed fact");

        // Wholesale replacement: coverage can go backwards, and a stale "ready"
        // would derive from a tier that is no longer there.
        journal.set_base_tier_ready(HashSet::new());
        let key = at(5 * HOUR, HOUR);
        journal.retry(&key, "requeue".to_owned(), 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "clearing the set re-blocks the work");
    }

    /// The proof must reach the tasks that actually exist, at their own width.
    ///
    /// Prod's shape, not a hypothetical: `invalidate` mints derived units one
    /// HOUR wide, and `coarsen_sealed_slices` will not fuse a day whose day-wide
    /// unit already exists in any state — including `Complete`, which is what a
    /// legacy rows=0 publication leaves behind. So the day carries hour-wide
    /// pending tasks *and* a completed day-wide one, and a proof aimed at the
    /// day-wide key lands on the completed task, which is never claimed.
    ///
    /// Measured 2026-08-19 03:00 UTC by #194's census:
    /// `cells_missing=264 cells_wanted=0 defer_enqueue=false` — every hole seen,
    /// every one vetoed as already-queued, and the proof had never fired once.
    #[test]
    fn the_proof_reaches_hour_wide_tasks_under_a_completed_day_unit() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const HOUR: i64 = 3_600_000_000;
        const DAY: i64 = 24 * HOUR;
        let at = |start: i64, width: i64| TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(start, start + width).expect("slice"),
            operation: Operation::DerivedRollup,
        };

        // The legacy rows=0 publication: a COMPLETE day-wide unit.
        let day_unit = at(0, DAY);
        journal.enqueue(day_unit.clone(), 0, 1, 0);
        journal.complete(&day_unit);
        // And the hour-wide work that is actually pending underneath it.
        journal.enqueue(at(0, HOUR), 0, 1, 0);
        journal.enqueue(at(HOUR, HOUR), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none(), "precondition: the hour units are dependency-blocked");

        // Proving the DAY must reach the hour units, not just the completed key.
        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY), 2, "both pending hour units are proven");
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "an hour unit becomes claimable");

        // Scoped to the day: a task in the NEXT day must not be swept up.
        journal.enqueue(at(DAY, HOUR), 0, 1, 0);
        assert_eq!(journal.prove_base_tier_for_day(&day_unit, 0, DAY), 0, "the following day is a different fact and stays unproven");
    }

    /// The proof latches. The frontier re-enqueues the same key without it, and
    /// silence is not evidence that coverage stopped existing.
    #[test]
    fn a_proven_base_tier_is_not_forgotten_by_a_later_enqueue() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = TaskKey {
            physical_table: "rollup_1h".to_owned(),
            source: "source".to_owned(),
            project_id: "p".to_owned(),
            slice: TimeSlice::new(0, 3_600_000_000).expect("slice"),
            operation: Operation::DerivedRollup,
        };
        journal.enqueue_with_base_tier(key.clone(), 0, 1, 0, true);
        journal.enqueue(key.clone(), 0, 1, 0);
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some(), "a later uninformed enqueue must not clear the proof");
    }

    /// Attempts alone must not quarantine. Only the worker's own verdict does.
    ///
    /// A task is claimed and its attempt counted before anything about its cost
    /// is known, so a unit handed back for a reason unrelated to cost —
    /// `source_not_flushed`, `resolve_input`, an unsatisfiable dependency —
    /// accumulates attempts identically to one that burned a 900s deadline.
    ///
    /// Prod 2026-08-19: of 162 SEALED derived units, the historical backfill the
    /// 30d goal needs, **109 were quarantined** and held to 2 of 16 workers.
    /// They had failed while their base-tier dependency was unprovable, which
    /// #197/#202 then fixed — stale evidence about a condition that no longer
    /// existed.
    #[test]
    fn only_a_worker_verdict_quarantines_a_unit() {
        let mut over = task("p", 0, MIN_SLICE_MICROS, Operation::DerivedRollup);
        over.attempts = TaskJournal::QUARANTINE_ATTEMPTS + 3;

        // Handed back for a reason that says nothing about cost.
        over.retry_reason = Some("source_not_flushed".to_owned());
        assert!(!TaskJournal::is_quarantined(&over), "a dependency-shaped failure is not evidence about cost");
        over.retry_reason = None;
        assert!(!TaskJournal::is_quarantined(&over), "no recorded reason is not evidence either");

        // The worker's own verdict, which IS about cost.
        over.retry_reason = Some(TaskJournal::WORKER_FAILURE_REASON.to_owned());
        assert!(TaskJournal::is_quarantined(&over), "a unit the worker gave back, repeatedly, is quarantined");

        // One such failure is still a blip, not proof.
        let mut once = over.clone();
        once.attempts = 1;
        assert!(!TaskJournal::is_quarantined(&once), "a single worker failure is a blip");
    }

    /// A unit that has proven it cannot fit its deadline must not be able to
    /// crowd out work that can.
    ///
    /// Prod 2026-08-18 21:00 UTC, 60 minutes of logs: 47 units timed out at
    /// 900s each — 42,300 of 57,600 available worker-seconds, 73% of ALL
    /// maintenance capacity, committing nothing — while ~40,000 pending
    /// BaseRollup units could not get a slot and `tasks_complete` advanced by 2
    /// per 180s with `tasks_running` pinned at 16. The one rollup that did
    /// complete took 812ms.
    ///
    /// Neither the backoff nor the bisection in `abandon_running` bounds this:
    /// the backoff sets how OFTEN a doomed unit runs, not what it costs, and
    /// halving a slice cannot halve a per-file cost (~3.2s per parquet file,
    /// measured the same day) — it doubles the number of units paying it.
    #[test]
    fn a_unit_that_cannot_fit_its_deadline_does_not_crowd_out_one_that_can() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // Two units, identical but for their history. The doomed one sorts
        // FIRST on every tiebreak in `claim_next` (project id, then key), so a
        // scheduler blind to attempts is guaranteed to pick it.
        let doomed = task("a_doomed", 0, MIN_SLICE_MICROS, Operation::BaseRollup);
        let key = doomed.key.clone();
        journal.upsert(doomed);
        journal.upsert(task("b_fresh", 0, MIN_SLICE_MICROS, Operation::BaseRollup));
        // Two runs that ended in a timeout. `mark_running` is the same call
        // `claim_next` uses to count an attempt, driven directly here so the
        // setup does not also rotate the project-fairness cursor.
        for _ in 0..TaskJournal::QUARANTINE_ATTEMPTS {
            assert!(journal.mark_running(&key));
            journal.retry(&key, TaskJournal::WORKER_FAILURE_REASON.to_owned(), 0);
        }

        // Without a quarantine slot the worker must skip it and run the unit
        // that can still finish.
        assert_eq!(
            journal.claim_next(Operation::BaseRollup, 0, false).expect("fresh work is claimable").key.project_id,
            "b_fresh",
            "a proven-unfittable unit must not be claimed while ordinary work waits"
        );

        // Deprioritised, never abandoned: with a slot it still takes its turn,
        // or a partition whose rollup is genuinely expensive — the one the 30d
        // goal needs most — would never gain coverage at all.
        assert_eq!(journal.claim_next(Operation::BaseRollup, 0, true).expect("quarantined work still runs").key, key);
    }

    /// The stale-estimate migration must run once, and must free fusion.
    ///
    /// Every stored estimate predating `slice_share_of_file` measured whole
    /// files, so a ten-minute child of a split day carried the whole day's
    /// number. `coarsen_to_width` sums its members, so 144 of them summed 144
    /// whole days and could never fit. Prod 2026-08-19, right after the estimate
    /// fix landed: `blocked=24 over_budget=266506` — the superseded trap gone
    /// The debris left by the old preflight shred is footprint-less one-minute
    /// units whose stored estimates are WHOLE-FILE figures, so their sum grows
    /// with the shredding and the fit test refuses hardest exactly where fusing
    /// is worth most. Prod 2026-08-23: `base_rollup / 00000000 / 2026-08-13`
    /// held 1,440 such units claiming 391 GB between them over a partition
    /// holding 0.36 GB.
    ///
    /// This pins that the PARTITION CEILING alone rescues them — no migration,
    /// no deletion of queued work. It is why the "one-shot destructive
    /// migration" this codebase's plan called for should not be written: the
    /// debris fuses back, and fusing preserves the work where deleting it would
    /// not.
    #[test]
    fn a_footprintless_shred_fuses_once_the_partition_ceiling_is_known() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        // One-minute units, each carrying a whole-file estimate and NO
        // footprint — the shape `split_time_task` produced before it recorded
        // one, which is the entire stuck population.
        let minutes = 600;
        for slot in 0..minutes {
            let start = day + slot * MIN_SLICE_MICROS;
            let mut unit = task("p", start, start + MIN_SLICE_MICROS, Operation::BaseRollup);
            unit.estimated_decoded_bytes = 4_466_185_462;
            unit.input = None;
            journal.upsert(unit);
        }
        let pending = |journal: &TaskJournal| {
            journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry)).count()
        };
        assert_eq!(pending(&journal), minutes as usize, "the shred is queued");

        // Without a ceiling the summed price is ~2.7 TB and every width refuses.
        assert_eq!(journal.coarsen_sealed_slices(now), 0, "summed whole-file estimates refuse to fuse — this is the stuck state");

        // The partition actually holds 0.36 GB. No unit over one partition can
        // decode more than the partition contains.
        let report = journal.coarsen_sealed_slices_capped(now, &|_project, _source, _date| Some(360 * 1024 * 1024));

        assert!(report.fused > 0, "the ceiling makes the fused unit's real cost knowable, so the day collapses: {report:?}");
        assert!(
            pending(&journal) < minutes as usize / 10,
            "600 one-minute units collapse to a handful of wide ones, not to nothing and not to 600: {} left",
            pending(&journal)
        );
        assert!(
            journal.tasks().any(|t| t.key.operation == Operation::BaseRollup && t.key.slice.width() > MIN_SLICE_MICROS),
            "the work SURVIVES as wider units — fusing preserves it where a deleting migration would not"
        );
    }

    /// and every candidate refused on a number written before the fix existed.
    #[test]
    fn clearing_stale_estimates_runs_once_and_lets_a_split_day_fuse_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        // 144 children each carrying the WHOLE day's estimate, as a split under
        // the old ruler produced.
        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, MAX_DECODED_BYTES, 0);
        }
        assert_eq!(journal.coarsen_sealed_slices(now), 0, "summed stale estimates must refuse to fuse — that is the bug");

        let cleared = journal.clear_stale_estimates().expect("first run migrates");
        assert_eq!(cleared, 144, "every pending unit's estimate is suspect");
        assert!(journal.clear_stale_estimates().is_none(), "the migration must not run twice");

        assert!(journal.coarsen_sealed_slices(now) > 0, "with the stale numbers gone the day must fuse");
        let widths = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry))
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(widths, vec![DAY_MICROS], "one day-wide unit should remain, got {widths:?}");
    }

    /// A collapse must SURVIVE a reload.
    ///
    /// `checkpoint` appends dirty tasks to a WAL, and `JournalRecord::Task` can
    /// only upsert — there is no record meaning "this task is gone". So a pass
    /// that removes tasks and then checkpoints persists nothing, and every
    /// removed task returns when the journal is next loaded.
    ///
    /// Prod 2026-08-19 ran the whole loop invisibly: coarsening took
    /// `pending_base_rollup` from 88,618 to 2,294, the next deploy restored 81k,
    /// and the on-disk journal was still byte-identical at 84,734,124 bytes with
    /// all 173,901 tasks. On a process that restarts several times a day, an
    /// in-memory-only collapse never happened at all.
    #[test]
    fn a_collapsed_queue_stays_collapsed_across_a_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        let now = 10 * DAY_MICROS;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
                let start = day + slot * NORMAL_SLICE_MICROS;
                journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
            }
            journal.checkpoint().expect("persist the fine slices");
            assert!(journal.coarsen_sealed_slices(now) > 0, "the day must collapse");
            journal.compact().expect("a pass that REMOVES must rewrite the snapshot");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let live = reloaded.tasks().filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry)).count();
        assert_eq!(live, 1, "the collapse must survive the reload; got {live} units back");
    }

    /// Derived hygiene work is never persisted; durable work always is.
    ///
    /// `plan_compaction_debt` scans the real file list every 60 s and mints
    /// HotPacking / SealedConsolidation from the files themselves, so the scan
    /// is authoritative and a durable copy can only go stale. It did: prod
    /// 2026-08-19 carried `pending_sealed_consolidation = 2,218` while an audit
    /// of object storage found 877 of 1,033 partitions already compliant and
    /// only 108 sealed ones out of policy — 20x inflated with work already done.
    ///
    /// Repair must still persist: its units are day-wide rewrites that stage
    /// output before committing, and losing one means redoing 12-15 minutes.
    #[test]
    fn derived_hygiene_is_not_persisted_but_durable_work_is() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        let of = |operation| task("p", day, day + DAY_MICROS, operation).key;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            for operation in [Operation::HotPacking, Operation::SealedConsolidation, Operation::Repair, Operation::BaseRollup] {
                journal.enqueue(of(operation), 0, 16, 0);
            }
            assert_eq!(journal.tasks().count(), 4, "all four exist in memory");
            journal.checkpoint().expect("persist");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        let survived: Vec<_> = reloaded.tasks().map(|t| t.key.operation).collect();
        assert!(survived.contains(&Operation::Repair), "Repair must survive: it stages output before committing");
        assert!(survived.contains(&Operation::BaseRollup), "BaseRollup must survive");
        assert!(!survived.contains(&Operation::HotPacking), "HotPacking is re-derived from the file scan, not reloaded");
        assert!(!survived.contains(&Operation::SealedConsolidation), "SealedConsolidation is re-derived from the file scan");
        assert_eq!(survived.len(), 2, "exactly the durable operations, got {survived:?}");
    }

    /// A removal must survive a reload through `checkpoint` ALONE.
    ///
    /// The WAL used to be upsert-only: `JournalRecord` had `Task` and
    /// `SourceCursor` and nothing meaning "this task is gone", so a pass that
    /// removed tasks could persist its work only by rewriting the entire 84 MB
    /// snapshot. Every caller that forgot silently lost the removal. Prod
    /// 2026-08-19: `pending_base_rollup` 88,618 -> 2,294 while the on-disk
    /// journal stayed byte-identical at 84,734,124 bytes with all 173,901
    /// tasks, and the next restart undid the lot.
    ///
    /// `compact` still exists and is still right for a large migration. This
    /// pins the cheap path, because the cheap path is the one callers reach for.
    #[test]
    fn a_removal_survives_a_reload_without_compacting() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            for slot in 0..6 {
                let start = day + slot * NORMAL_SLICE_MICROS;
                journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
            }
            journal.checkpoint().expect("persist the slices");
            let dropped = journal.retain_tasks(|t| t.key.slice.start_micros != day);
            assert_eq!(dropped, 1, "exactly one unit should have been dropped");
            journal.checkpoint().expect("persist the REMOVAL — no compact");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.tasks().count(), 5, "the removal must survive; a tombstone-less WAL replays all 6");
        assert!(!reloaded.tasks().any(|t| t.key.slice.start_micros == day), "the removed unit specifically must be gone, not merely some unit");
    }

    /// Remove-then-recreate replays as CREATED, not as removed.
    ///
    /// `coarsen_to_width` does exactly this in one pass: it drops a bucket's
    /// members and writes a fused unit over the same span, and a fused unit at
    /// day width can collide with a key that was just dropped. If the tombstone
    /// won, the pass would delete the very unit it exists to create.
    #[test]
    fn a_task_recreated_after_removal_survives_the_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        let day = 3 * DAY_MICROS;
        let key = task("p", day, day + NORMAL_SLICE_MICROS, Operation::BaseRollup).key;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            journal.enqueue(key.clone(), 0, 16, 0);
            journal.checkpoint().expect("persist");
            journal.retain_tasks(|t| t.key != key);
            journal.enqueue(key.clone(), 0, 32, 0);
            journal.checkpoint().expect("persist both the removal and the re-creation");
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.tasks().count(), 1, "the re-created task must be present exactly once");
        assert_eq!(reloaded.state(&key), Some(TaskState::Pending));
    }

    /// A fused unit inherits its members' AGE, not the moment of fusion.
    ///
    /// `scheduling_class` escalates work that has waited past
    /// `STARVATION_MICROS`. Stamping the fused unit with `now` makes it
    /// permanently fresh, so the narrow leftovers that were not fused keep their
    /// real, older creation time and outrank it forever — coarsening starves its
    /// own output.
    ///
    /// Prod 2026-08-19, 316 claims in 35 minutes: Dedup and BaseRollup claimed
    /// ZERO day-wide units, while Repair (34/34), HotPacking (29/41) and
    /// SealedConsolidation (19/20) claimed almost nothing else. Those three are
    /// planned day-wide and never fused; the two that fuse were the two starving.
    #[test]
    fn a_fused_unit_inherits_the_age_of_the_work_it_replaces() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;
        // Members created five days ago, in milliseconds.
        let old_ms = u64::try_from((now - 5 * DAY_MICROS).div_euclid(1_000)).expect("positive");

        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, old_ms + u64::try_from(slot).unwrap_or(0));
        }
        assert!(journal.coarsen_sealed_slices(now) > 0, "the day must fuse");

        let fused: Vec<_> =
            journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry)).collect();
        assert_eq!(fused.len(), 1, "one day-wide unit should remain");
        assert_eq!(
            fused[0].created_unix_ms, old_ms,
            "the fused unit must carry the OLDEST member's age; stamped with `now` it can never escalate past STARVATION_MICROS"
        );
    }

    /// A day-wide unit must SUBSUME the ten-minute units inside it.
    ///
    /// This is the shape prod was actually in on 2026-08-19, and the reason the
    /// fuse pass alone converged to nothing. `coarsen_to_width` refuses any
    /// bucket already covered by a pending unit at least as wide — correctly,
    /// because fusing there would duplicate claimed work — but a pending
    /// day-wide unit is exactly the condition under which the narrow units are
    /// redundant. So fusion collapsed the cells that had no day unit, logged
    /// 12, 6, 3, and went quiet, while `pending_base_rollup` sat at 88,100
    /// against a backfill census of `cells_missing=260 cells_wanted=0` — 260
    /// real (project, date) cells, ~339 queued units each.
    #[test]
    fn a_pending_day_unit_subsumes_the_ten_minute_units_inside_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = day + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);
        }
        journal.enqueue(task("p", day, day + DAY_MICROS, Operation::BaseRollup).key, 0, 16, 0);
        assert_eq!(journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count(), 145, "144 slices plus the day unit");

        journal.coarsen_sealed_slices(now);

        let live = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && matches!(t.state, TaskState::Pending | TaskState::Retry))
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(live, vec![DAY_MICROS], "the day unit must absorb all 144 slices, leaving one unit for the cell; got {live:?}");
    }

    /// ...and a SUPERSEDED day unit subsumes nothing either.
    ///
    /// `split_time_task` supersedes a unit too big to finish and replaces it
    /// with children tiling its range. If a superseded parent could subsume,
    /// it would delete the children the split just created and leave the cell
    /// with no queued work at all — strictly worse than the redundancy this
    /// pass exists to remove.
    #[test]
    fn a_superseded_day_unit_does_not_subsume_the_children_that_replaced_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        let parent = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "the parent must be superseded for this to prove anything");
        let children = journal.tasks().filter(|t| t.state == TaskState::Pending && t.key.operation == Operation::BaseRollup).count();
        assert!(children > 0, "the split must have produced children");

        journal.coarsen_sealed_slices(now);

        assert!(
            journal.tasks().any(|t| matches!(t.state, TaskState::Pending | TaskState::Retry) && t.key.operation == Operation::BaseRollup),
            "the split's children must survive; a superseded parent is not queued work"
        );
    }

    /// ...but a COMPLETE day unit subsumes nothing.
    ///
    /// Built once is not the same as queued. A narrower unit inside a completed
    /// day is a later invalidation — the day is dirty again, which is why the
    /// slice exists — and dropping it would lose that rebuild silently.
    #[test]
    fn a_complete_day_unit_does_not_subsume_a_later_invalidation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let day = 3 * DAY_MICROS;

        let parent = task("p", day, day + DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 16, 0);
        journal.complete(&parent);
        let start = day + 5 * NORMAL_SLICE_MICROS;
        journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 16, 0);

        journal.coarsen_sealed_slices(now);

        assert!(
            journal.tasks().any(|t| t.state == TaskState::Pending && t.key.operation == Operation::BaseRollup),
            "a later invalidation inside a COMPLETE day must survive"
        );
    }

    /// A sealed day too big to scan as one unit lands at a narrower width,
    /// not back at ten minutes.
    ///
    /// This is the state prod was in on 2026-08-19: `pending_base_rollup =
    /// 84,834`, with `maintenance_sealed_slices_coarsened` logging 12, 6, 3 and
    /// then nothing for the rest of the process's life. Everything that fit a
    /// day had already been fused; every remaining day was over
    /// `MAX_DECODED_BYTES` and so kept all 144 of its ten-minute slices.
    ///
    /// That fallback is not merely slower, it is inverted. On an uncompacted
    /// sealed partition every file spans the whole day, so timestamp-stat
    /// pruning skips nothing and a ten-minute slice reads exactly the files a
    /// day unit would — measured at `scan_ms=481682` to publish 142 rows. The
    /// day-or-nothing rule therefore answered "too expensive to scan once" with
    /// "scan it 144 times".
    #[test]
    fn a_day_over_the_decode_budget_lands_at_a_narrower_width_not_at_ten_minutes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // One sealed day of ten-minute slices whose summed estimate busts the
        // day budget but leaves each six-hour quarter comfortably inside it.
        let per_slice = MAX_DECODED_BYTES / 96;
        for slot in 0..(DAY_MICROS / NORMAL_SLICE_MICROS) {
            let start = 3 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, per_slice, 0);
        }
        let minted = journal.tasks().filter(|t| t.key.operation == Operation::BaseRollup).count();
        assert_eq!(minted, 144, "the live path mints one unit per ten-minute slice");

        journal.coarsen_sealed_slices(now);

        let widths = journal
            .tasks()
            .filter(|t| t.key.operation == Operation::BaseRollup && t.state == TaskState::Pending)
            .map(|t| t.key.slice.width())
            .collect::<Vec<_>>();
        assert_eq!(widths.len(), 4, "a day over budget must fuse into its four six-hour quarters, got {widths:?}");
        assert!(widths.iter().all(|width| *width == 6 * 60 * 60 * 1_000_000), "expected six-hour units, got {widths:?}");
    }

    /// A sealed day's ten-minute leftovers collapse into one day unit — but
    /// never one that would undo a split.
    ///
    /// The live path mints a unit per ten-minute slice, which is right while
    /// the day is the frontier and pure overhead once it seals: ~144 units
    /// where one would do, each paying the same fixed object-store cost. Prod
    /// 2026-08-17 sat at 18,040 pending, refilled every midnight at about the
    /// rate it drained. At ten times the projects that diverges.
    ///
    /// The guard matters as much as the collapse: `split_time_task` leaves a
    /// too-big parent `Superseded`, so coarsening its children back into a day
    /// would split, coarsen, split, forever.
    #[test]
    fn a_sealed_days_fine_slices_collapse_but_never_undo_a_split() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 3: ten-minute leftovers, nothing coarse above them.
        for slot in 0..6 {
            let start = 3 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }
        // Day 5: same, but its day unit was already split into these children.
        for slot in 0..6 {
            let start = 5 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }
        let parent = task("p", 5 * DAY_MICROS, 6 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "the parent must be superseded for the guard to be exercised");

        let collapsed = journal.coarsen_sealed_slices(now);
        assert!(collapsed >= 6, "day 3's leftovers must collapse, got {collapsed}");

        let widths = |day: i64| {
            journal
                .tasks()
                .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= day * DAY_MICROS && t.key.slice.start_micros < (day + 1) * DAY_MICROS)
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths(3), vec![DAY_MICROS], "day 3 is now exactly one day-sized unit");
        assert!(
            widths(5).iter().all(|w| *w < DAY_MICROS),
            "day 5 already had a day unit that was SPLIT; recreating it would loop forever, got {:?}",
            widths(5)
        );
    }

    /// Among sealed work, a day-sized unit outranks yesterday's ten-minute
    /// leftovers even though those are newer.
    ///
    /// Day-sized units come from the backfill planner and are the only kind
    /// that advances the rollup horizon. Ten-minute units are what the live
    /// path mints, and a day that has just sealed carries ~144 of them per
    /// project per tier. Ordering sealed work newest-first alone therefore
    /// grinds all of yesterday before reaching the day before — and midnight
    /// mints a fresh day's worth, so the horizon can never move.
    ///
    /// Prod 2026-08-17, 65 rollup starts in 25 minutes: 46 on today, 18 on
    /// yesterday, ZERO on any older day, while 7d/14d queries were refused for
    /// want of exactly those older days.
    /// The sealed reservation is affordable only while the frontier keeps up.
    ///
    /// It exists because strict class-0 priority let the frontier take ~90% of
    /// claims and froze rollup coverage. But frontier lag is a per-query cost —
    /// `raw_tail_duration_secs` is `FINALIZATION_DELAY + lag` and every hybrid
    /// query scans that tail — and prod 2026-08-17 reached 62 minutes of it.
    /// So the reservation yields while the frontier is behind, and returns on
    /// its own once it is not.
    /// Coarsening must not build a day unit that cannot finish. One day unit doing
    /// one full-day scan beats 144 slices each doing the same scan — but only if
    /// it completes. A day over the decode budget publishes nothing and times out
    /// repeatedly, which is strictly worse than the slices it replaced.
    ///
    /// Measured after #178: BaseRollup timed out at 900s for the first time (4 in
    /// a 10-minute window) and rollup output collapsed from ~9,000 rows/min to 10.
    /// The fix is only reachable if it can claim the units it is for.
    /// `attempts >= 2` quarantines a unit and floors its backoff at the
    /// operation deadline, so prod's 432 `worker_error` repair units would
    /// otherwise have drained through ~2 slots at an hour each.
    #[test]
    fn the_repair_migration_unquarantines_the_queue_it_is_for() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut wedged = task("p", 0, DAY_MICROS, Operation::Repair);
        wedged.attempts = 100;
        wedged.state = TaskState::Retry;
        wedged.retry_reason = Some("worker_error".to_owned());
        wedged.deadline_micros = i64::MAX;
        let repair = wedged.key.clone();
        journal.upsert(wedged);
        // A neighbour that the migration must not touch.
        let mut dedup = task("p", 0, DAY_MICROS, Operation::Dedup);
        dedup.attempts = 7;
        let untouched = dedup.key.clone();
        journal.upsert(dedup);

        assert_eq!(journal.reset_repair_attempts(), Some(1), "one repair unit reset");
        let after = journal.tasks().find(|candidate| candidate.key == repair).expect("still queued");
        assert_eq!(after.attempts, 0, "quarantine is keyed on attempts");
        assert_eq!(after.deadline_micros, 0, "and the stamped backoff floor must go with it");
        assert_eq!(journal.attempts(&untouched), 7, "other operations keep their history");
        assert_eq!(journal.reset_repair_attempts(), None, "the migration must not run twice");

        // A journal with no repair queue must not spend the cursor, or a boot
        // that precedes the queue consumes the one shot.
        let empty = tempfile::tempdir().expect("temp dir");
        let mut fresh = TaskJournal::load(empty.path()).expect("journal");
        assert_eq!(fresh.reset_repair_attempts(), None, "nothing to forgive, nothing spent");
        fresh.upsert(task("p", 0, DAY_MICROS, Operation::Repair));
        assert_eq!(fresh.reset_repair_attempts(), Some(1), "and the cursor is still available when the queue arrives");
    }

    /// A busy pool is not an oversized unit. `resource_admission` is classed as
    /// a capacity failure, so `retry_or_split` SPLITS on it — right when
    /// admission's ceiling was a static `MAX_DECODED_BYTES`, wrong once it
    /// scales with occupancy. Prod 2026-09-01: 230,015 such retries in 33
    /// minutes with `pending_dedup` climbing 2,857 -> 3,533, because every
    /// refusal split a unit into shards that were each refused in turn.
    #[test]
    fn a_busy_pool_refusal_must_not_be_classed_as_a_capacity_failure() {
        assert!(is_capacity_failure("resource_admission"), "a static over-budget estimate still splits");
        assert!(!is_capacity_failure("admission_busy"), "but a transient busy pool must back off, not multiply the queue");
    }

    /// The ceiling is only meaningful if the REQUEST is honest. Prod
    /// 2026-09-01, five minutes after the ceiling shipped: 1,339
    /// `resource_admission` retries in one window, because every caller asked
    /// for `MAX_DECODED_BYTES` regardless of its unit's real size, so a busy
    /// pool refused all of them and every lane hot-looped on a 1-second requeue.
    #[test]
    fn a_small_unit_is_admitted_by_a_pool_that_refuses_a_large_one() {
        const CAPACITY: u64 = MAX_DECODED_BYTES * 16;
        // Three quarters full.
        let ceiling = super::occupancy_scaled_ceiling(CAPACITY / 4, CAPACITY);
        assert!(ceiling < MAX_DECODED_BYTES, "a busy pool must refuse a max-size unit");
        assert!(ceiling >= MAX_DECODED_BYTES / 16, "and still admit a small one");
        assert!(MAX_DECODED_BYTES / 16 <= ceiling, "a hygiene-sized unit must fit under the ceiling of a busy pool, or the fleet hot-loops on admission");
    }

    /// REPRODUCES the 2026-09-02 lockout: a unit priced at exactly
    /// `MAX_DECODED_BYTES` must be admissible into a pool that is merely BUSY,
    /// not idle.
    ///
    /// `byte_bounded_units` splits until a unit fits `MAX_DECODED_BYTES`, so its
    /// output piles up AT that constant, and both admission sites clamp their
    /// request to it — deliberately, because an oversized unit is meant to
    /// reserve the maximum and then hash-shard ITSELF internally
    /// (`split_time_task` declines to shard in the journal for exactly that
    /// reason). But `MAX * available / capacity` is strictly less than `MAX`
    /// whenever one byte is reserved, so the request the design intends as
    /// "reserve the maximum and self-shard" was the one request the gate could
    /// never grant.
    ///
    /// Prod 2026-09-02: 365 dedup units at a median of exactly 512.0 MiB, a
    /// median age of 14.6 days, all `hash_shards = 1`; plus 94 base_rollup units
    /// pinned at the 1-minute minimum slice width estimating 7.5 GiB each, hot
    /// looping to 715 attempts on `admission_busy`.
    ///
    /// FAILS before the ceiling reaches `MAX` at a healthy free fraction.
    #[test]
    fn a_max_sized_unit_is_admitted_by_a_busy_pool_not_only_an_idle_one() {
        const CAPACITY: u64 = MAX_DECODED_BYTES * 16;
        // One single unit reserved — the least busy a working pool can be.
        let ceiling = super::occupancy_scaled_ceiling(CAPACITY - MAX_DECODED_BYTES, CAPACITY);
        assert!(
            ceiling >= MAX_DECODED_BYTES,
            "a pool with ONE unit reserved refused a max-sized request (ceiling {ceiling} < {MAX_DECODED_BYTES}); \
             every unit the splitter produces is priced at exactly that, so they can only ever run on a perfectly empty pool"
        );
    }

    /// ClickHouse's rule: a busy pool admits only small work, so an oversized
    /// unit is never admitted into a position where it would have to be killed.
    /// Off by default — a shape for 10x load, not a throughput win today.
    #[test]
    fn the_admission_ceiling_shrinks_as_the_pool_fills() {
        const CAPACITY: u64 = MAX_DECODED_BYTES * 16;
        assert_eq!(super::occupancy_scaled_ceiling(CAPACITY, CAPACITY), MAX_DECODED_BYTES, "an idle pool admits the largest unit");
        assert!(super::occupancy_scaled_ceiling(CAPACITY / 4, CAPACITY) < MAX_DECODED_BYTES, "a three-quarters-full pool admits less");
        assert!(
            super::occupancy_scaled_ceiling(0, CAPACITY) >= MAX_DECODED_BYTES / 16,
            "but a full pool must still admit the small hygiene bins, or file counts run away"
        );
        assert!(
            super::occupancy_scaled_ceiling(CAPACITY / 2, CAPACITY) > super::occupancy_scaled_ceiling(CAPACITY / 4, CAPACITY),
            "and the ceiling must be monotone in free space"
        );
    }

    /// Fusion must not refuse a group priced against its PARTITION just because
    /// the partition is bigger than one unit's decode budget: the members do not
    /// avoid that cost by staying apart, they each pay it. Prod 2026-09-01, every
    /// coarsening pass: `candidates=7452 fused=0 over_budget=6967`.
    #[test]
    fn a_partition_priced_group_fuses_even_when_the_partition_is_large() {
        const DAY: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // A day shredded into ten-minute slices, each modelled at a fraction of
        // a partition that is far bigger than MAX_DECODED_BYTES.
        for slot in 0..24 {
            let start = DAY + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::Dedup).key, 0, MAX_DECODED_BYTES / 2, 0);
        }
        let big_partition = MAX_DECODED_BYTES * 20;

        // The exemption is dedup's alone until the rollup runner is shown to
        // honour `hash_shard` — see `the_partition_ceiling_does_not_fuse_a_genuinely_oversized_day`.
        let mut rollup = TaskJournal::load(tempfile::tempdir().expect("dir").path()).expect("journal");
        for slot in 0..24 {
            let start = DAY + slot * NORMAL_SLICE_MICROS;
            rollup.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, MAX_DECODED_BYTES / 2, 0);
        }
        assert_eq!(rollup.coarsen_sealed_slices_capped(10 * DAY, &|_, _, _| Some(big_partition)).fused, 0, "rollup keeps the old rule");

        // Without storage access the old rule stands — the price is a sum over
        // files that might not overlap.
        let mut blind = TaskJournal::load(tempfile::tempdir().expect("dir").path()).expect("journal");
        for slot in 0..24 {
            let start = DAY + slot * NORMAL_SLICE_MICROS;
            blind.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::Dedup).key, 0, MAX_DECODED_BYTES / 2, 0);
        }
        assert_eq!(blind.coarsen_sealed_slices_capped(10 * DAY, &|_, _, _| None).fused, 0, "a sum over unknown files must still be refused");

        let report = journal.coarsen_sealed_slices_capped(10 * DAY, &|_, _, _| Some(big_partition));
        assert!(report.fused > 0, "a group known to share one partition must fuse: {report:?}");
        assert_eq!(report.over_budget, 0, "and must not be counted against the decode budget it cannot honour");
    }

    /// Bisecting a dedup unit below the width where it stops shedding FILES
    /// manufactures slivers that each pay the same whole-partition scan. Prod
    /// 2026-09-01: 4,000 of 5,028 active dedup units were sub-15-minute slices
    /// with a p90 of 76 input files, all of them made by `split_into_smaller_slices`.
    #[test]
    fn a_dedup_unit_shards_by_key_instead_of_slivering_time() {
        let over_budget = MAX_DECODED_BYTES * 4;
        let ten_minutes = task("p", 0, NORMAL_SLICE_MICROS, Operation::Dedup);
        let children = byte_bounded_units(&ten_minutes, over_budget);
        assert!(children.iter().all(|child| child.hash_shards > 1), "at the floor, dedup sheds work by KEY, not by halving time again");
        assert!(children.iter().all(|child| child.key.slice.width() == NORMAL_SLICE_MICROS), "and the slice must not narrow further");

        // Above the floor it still bisects — the floor is a floor, not a ban.
        let hour = task("p", 0, 60 * 60 * 1_000_000, Operation::Dedup);
        let halves = byte_bounded_units(&hour, over_budget);
        assert_eq!(halves.len(), 2, "an hour-wide unit still halves");
        assert!(halves.iter().all(|child| child.hash_shards <= 1));

        // Other operations keep the old floor: their cost model is different.
        let rollup = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        assert_eq!(byte_bounded_units(&rollup, over_budget).len(), 2, "only dedup's cost is partition-scoped");
    }

    /// Dedup's window was 300s from when the clock was a budget on TOTAL time.
    /// Under idle semantics that killed working units: prod 2026-09-01, 33 of
    /// 288 dedup units in 25 minutes burned the full 300s and were killed —
    /// 9,900 worker-seconds — while units granted a second window finished at
    /// 599s, 600s and 887s.
    #[test]
    fn only_repair_gets_a_window_longer_than_the_fleet_default() {
        for operation in [Operation::Dedup, Operation::HotPacking, Operation::SealedConsolidation, Operation::BaseRollup, Operation::DerivedRollup] {
            assert_eq!(operation_deadline_secs(operation), 15 * 60, "{operation:?} shares the fleet default");
        }
        assert_eq!(operation_deadline_secs(Operation::Repair), 60 * 60, "repair alone is longer: ORDER BY is blocking on a whole file");
        assert!(operation_deadline_secs(Operation::Repair) <= MAX_OPERATION_DEADLINE_SECS);
    }

    /// A Repair unit rewrites ONE whole file, so halving its slice halves
    /// nothing: `coordinator_compaction_files` still hands every child the same
    /// `take(1)`. Prod 2026-08-31 had 432 repair units bisected to 12h/0.75h/
    /// 0.38h widths across four dates, every one of them `worker_error`.
    #[test]
    fn a_repair_unit_is_never_bisected_because_its_cost_is_a_whole_file() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let repair = task("p", DAY_MICROS, DAY_MICROS + DAY_MICROS, Operation::Repair).key;
        journal.enqueue(repair.clone(), 0, MAX_DECODED_BYTES, 0);
        let before = journal.snapshot.tasks.len();

        // Day-wide, and measured far over budget: every condition a split needs.
        assert!(!journal.split_time_task(&repair, MAX_DECODED_BYTES * 8, None), "repair must decline to split");
        assert_eq!(journal.snapshot.tasks.len(), before, "a declined split must mint no children");
        assert_eq!(journal.state(&repair), Some(TaskState::Pending), "and must not supersede the parent");
    }

    #[test]
    fn coarsening_skips_a_day_that_would_not_fit_the_decode_budget() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 1: six slices whose combined estimate fits comfortably. Must fuse.
        let small = MAX_DECODED_BYTES / 12;
        for slot in 0..6 {
            let start = DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, small, 0);
        }
        // Day 4: six slices that together blow the budget. Must stay as slices.
        let big = MAX_DECODED_BYTES / 2;
        for slot in 0..6 {
            let start = 4 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("q", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, big, 0);
        }

        journal.coarsen_sealed_slices(now);

        let widths = |project: &str, day: i64| {
            journal
                .tasks()
                .filter(|t| {
                    t.state == TaskState::Pending
                        && t.key.project_id == project
                        && t.key.slice.start_micros >= day * DAY_MICROS
                        && t.key.slice.start_micros < (day + 1) * DAY_MICROS
                })
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths("p", 1), vec![DAY_MICROS], "a day that fits must fuse into one unit");
        assert_eq!(widths("q", 4).len(), 6, "a day over budget must keep its slices — they finish, a too-big day unit never does");
        assert!(widths("q", 4).iter().all(|w| *w < DAY_MICROS));
    }

    /// The prod shape: a whole day shredded to the one-minute floor, with the
    /// ESTIMATES prod actually stored. Currently fails — this is a live defect.
    ///
    /// Copied out of the real journal 2026-08-23 (94,223 tasks; 21,598 pending
    /// resolving to 1,452 cells, a 14.9x inflation). Worst cell: `base_rollup /
    /// 00000000 / 2026-08-13`, **1,440 pending units of exactly 60 seconds each,
    /// perfectly contiguous, covering exactly 24 hours**, nothing wider on that
    /// table. Nothing wider means `subsume_covered_units` cannot fire, so fusion
    /// is the only way out.
    ///
    /// Fusion cannot fire either, and the stored estimates say why:
    ///
    ///     estimated_decoded_bytes:  282 MB x1220,  188 MB x203,  537 MB x17
    ///     summed per HOUR:          15.3 - 20.1 GB
    ///     MAX_DECODED_BYTES:        512 MB
    ///
    /// An hour is 30-40x over budget, so every width is refused and the cell is
    /// stuck forever. That partition holds **35 files totalling 0.36 GB** while
    /// these 1,440 units claim **391 GB** between them — but each individual
    /// estimate is HONEST, not stale (`__maintenance_stale_estimate_v2` had
    /// already run). A row group cannot be pruned below, and on ~10 MB files one
    /// row group IS the file, so a sixty-second slice really does decode ~282 MB.
    ///
    /// The defect is the SUM. 1,440 children reading the same 35 files are one
    /// scan, and charging them 1,440 times is what blocks the one mechanism that
    /// would collapse the 14.9x inflation.
    ///
    /// The fix is [`InputFootprint`]: children of a split read the parent's
    /// files, so fusion charges that set ONCE instead of once per child.
    #[test]
    fn a_day_shredded_to_the_minute_floor_collapses() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        // The 391 GB prod's 1,440 units claimed between them, against a
        // partition holding 35 files that decode to well under the budget.
        let footprint = InputFootprint::new((0..35).map(|n| format!("part-{n}.parquet")), MAX_DECODED_BYTES / 2);
        // A split stamps its children with what the PARENT read — that is what
        // lets fusion price them as one scan later. Exercised on a different
        // day so the halves it leaves cannot subsume the shred below, which
        // would collapse it by covering rather than by fusion.
        let parent = task("p", 3 * DAY_MICROS, 4 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        assert!(journal.split_time_task(&parent, 391 * 1024 * 1024 * 1024, Some(footprint)));
        assert!(journal.tasks().filter(|t| t.state == TaskState::Pending).all(|t| t.input == Some(footprint)), "children inherit what they read");

        // The shred as prod held it, and as only REPEATED preflights can now
        // produce it: 1,440 contiguous one-minute units over the same file
        // set, nothing wider on the table.
        for minute in 0..1_440 {
            let start = DAY_MICROS + minute * MIN_SLICE_MICROS;
            let key = task("p", start, start + MIN_SLICE_MICROS, Operation::BaseRollup).key;
            journal.enqueue(key.clone(), 0, 282 * 1024 * 1024, 0);
            let index = journal.task_indices[&key];
            journal.snapshot.tasks[index].input = Some(footprint);
        }
        let shredded = |journal: &TaskJournal| {
            journal
                .tasks()
                .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= DAY_MICROS && t.key.slice.start_micros < 2 * DAY_MICROS)
                .count()
        };
        assert!(shredded(&journal) > 1_000, "precondition: the day is shredded to the floor");

        // The cascade lands them at a width they can actually finish. One pass
        // is enough because every child names the same file set.
        journal.coarsen_sealed_slices(now);
        assert!(shredded(&journal) <= 24, "a contiguous shredded day must collapse; {} units remain", shredded(&journal));
    }

    /// Fusion charges a file set once, and only when the members agree it IS
    /// one set. Two children over different files still sum — a fused unit
    /// would read both, and under-pricing that is how a split/fuse loop starts.
    #[test]
    fn fusion_charges_a_shared_file_set_once_and_disjoint_sets_twice() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let now = 10 * DAY_MICROS;
        let half = MAX_DECODED_BYTES / 2 + 1;
        let fuse = |footprints: [InputFootprint; 2]| {
            let dir = tempfile::tempdir().expect("temp dir");
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            for (slot, footprint) in footprints.into_iter().enumerate() {
                let slot = i64::try_from(slot).unwrap_or(0);
                let start = DAY_MICROS + slot * NORMAL_SLICE_MICROS;
                let key = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key;
                journal.enqueue(key.clone(), 0, half, 0);
                let index = journal.task_indices[&key];
                journal.snapshot.tasks[index].input = Some(footprint);
            }
            journal.coarsen_sealed_slices_reporting(now)
        };
        let shared = InputFootprint::new(["a.parquet", "b.parquet"], half);
        // Order-independent: a snapshot lists files in no fixed order, and two
        // units over the same partition must still recognise each other.
        assert_eq!(shared, InputFootprint::new(["b.parquet", "a.parquet"], half));
        assert_eq!(fuse([shared, shared]).fused, 2, "same files, one scan, one charge");
        let disjoint = fuse([shared, InputFootprint::new(["c.parquet"], half)]);
        assert_eq!((disjoint.fused, disjoint.over_budget > 0), (0, true), "different files must still sum");
    }

    /// The prod shape, WITH the partition ceiling that makes it collapsible.
    ///
    /// Same fixture as `a_day_shredded_to_the_minute_floor_collapses` (which is
    /// `#[ignore]`d because it calls the uncapped entry point and therefore still
    /// documents the defect): 1,440 contiguous one-minute units for one day, each
    /// carrying the whole-file estimate prod actually stored.
    ///
    /// Summed they claim 391 GB. The partition holds **35 files totalling
    /// 0.36 GB** — the sum is not pessimistic, it is impossible, because it
    /// counts the same files once per child. Given the real ceiling the day fuses
    /// in one pass.
    #[test]
    fn a_shredded_day_collapses_once_the_estimate_is_capped_by_its_partition() {
        const DAY_MICROS: i64 = 86_400_000_000;
        const MINUTE: i64 = 60_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        let prod_estimate = 282_280_533u64; // one 60-second slice, whole-file accounting
        for slot in 0..1440 {
            let start = DAY_MICROS + slot * MINUTE;
            journal.enqueue(task("p", start, start + MINUTE, Operation::BaseRollup).key, 0, prod_estimate, 0);
        }
        let partition = 386_547_056u64; // 35 files, 0.36 GB — what the day actually holds
        let report = journal.coarsen_sealed_slices_capped(now, &|_, _, _| Some(partition));
        let remaining = journal
            .tasks()
            .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= DAY_MICROS && t.key.slice.start_micros < 2 * DAY_MICROS)
            .count();
        assert!(
            remaining < 1440,
            "the shredded day must collapse once its estimate is bounded by the partition; {remaining} remain (fused={} over_budget={})",
            report.fused,
            report.over_budget
        );
        assert_eq!(remaining, 1, "and it should land as ONE day-wide unit, got {remaining}");
    }

    /// The ceiling must not become a licence to fuse anything. A partition whose
    /// REAL size is over budget still cannot be scanned in one unit, so the day
    /// keeps its slices exactly as before — the cap only ever removes
    /// double-counting, it never argues a big partition is small.
    #[test]
    fn the_partition_ceiling_does_not_fuse_a_genuinely_oversized_day() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;
        for slot in 0..6 {
            let start = DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, MAX_DECODED_BYTES / 2, 0);
        }
        // The partition really is 4 GB; the sum was not double-counting here.
        journal.coarsen_sealed_slices_capped(now, &|_, _, _| Some(4 * 1024 * 1024 * 1024));
        let widths: Vec<i64> = journal
            .tasks()
            .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= DAY_MICROS && t.key.slice.start_micros < 2 * DAY_MICROS)
            .map(|t| t.key.slice.width())
            .collect();
        assert_eq!(widths.len(), 6, "a genuinely oversized day must keep its slices, got {widths:?}");
        assert!(widths.iter().all(|w| *w < DAY_MICROS));
    }

    /// Removing a spec must retire its queued work, and must never touch
    /// anything else.
    ///
    /// Prod 2026-08-24 was still spending ~80 claims per ten minutes on
    /// `dashboard_level_1h_v1` after the spec was deleted, doing nothing each
    /// time. Every `_v2` -> `_v3` rename leaves the same residue.
    #[test]
    fn removing_a_spec_retires_its_queued_work_and_nothing_else() {
        const DAY: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let tiered = |table: &str, slot: i64, operation| {
            let mut unit = task("p", slot * DAY, (slot + 1) * DAY, operation);
            unit.key.physical_table = table.to_owned();
            unit.key
        };
        let gone = tiered("src_rollup_dead_1h_v1", 1, Operation::DerivedRollup);
        let live = tiered("src_rollup_live_1m_v3", 2, Operation::BaseRollup);
        let raw = tiered("src", 3, Operation::Dedup);
        let done = tiered("src_rollup_dead_1h_v1", 4, Operation::DerivedRollup);
        for key in [&gone, &live, &raw, &done] {
            journal.enqueue(key.clone(), 0, 1, 0);
        }
        journal.complete(&done);

        let declared: HashSet<String> = ["src_rollup_live_1m_v3".to_owned()].into_iter().collect();
        assert_eq!(journal.retire_undeclared_tiers(&declared), 1, "exactly the undeclared tier's live work");
        assert_eq!(journal.state(&gone), None, "the undeclared tier's queued unit is gone");
        assert_eq!(journal.state(&live), Some(TaskState::Pending), "a declared tier is untouched");
        assert_eq!(journal.state(&raw), Some(TaskState::Pending), "a non-tier table is never considered");
        assert_eq!(journal.state(&done), Some(TaskState::Complete), "history is untouched");

        // An empty declared set is what an unloaded registry looks like, and it
        // must retire NOTHING — otherwise a startup ordering change silently
        // deletes the whole queue.
        assert_eq!(journal.retire_undeclared_tiers(&HashSet::new()), 0, "an empty registry must never retire anything");
        assert_eq!(journal.state(&live), Some(TaskState::Pending));
    }

    /// The orphan repair must run exactly once, and must persist that BEFORE
    /// the caller does the work — a half-run repair that died to a restart would
    /// re-force every cell on the next boot, and prod restarts every few minutes.
    #[test]
    fn the_orphan_repair_claims_itself_once_and_survives_a_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            assert_eq!(journal.repair_orphaned_coverage_once("otel_logs_and_spans"), Some(0), "first call claims it");
            assert_eq!(journal.repair_orphaned_coverage_once("otel_logs_and_spans"), None, "second call in the same process must not");
            // PER SOURCE: the caller loops over sources, so one global cursor
            // would let whichever source is processed first consume the repair —
            // and if that is otel_metrics, the source that actually needs it
            // never runs. A silent no-op that looks like success.
            assert_eq!(journal.repair_orphaned_coverage_once("otel_metrics"), Some(0), "a different source claims independently");
        }
        let mut reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.repair_orphaned_coverage_once("otel_logs_and_spans"), None, "a restart must not re-run the repair");
    }

    /// The damage repair's cursor is a consumed-PREFIX index, per source, and it
    /// survives a restart. Sharing it with the orphan repair would let claiming
    /// one silently consume the other — and the damage list is what fixes ~211M
    /// rows the derived-witness bug left short, so consuming it as a no-op would
    /// look exactly like success.
    #[test]
    fn the_damage_repair_cursor_is_a_per_source_prefix_that_survives_a_reload() {
        let dir = tempfile::tempdir().expect("temp dir");
        const DAMAGE: &str = TaskJournal::DAMAGE_REPAIR_MIGRATION;
        {
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            assert_eq!(journal.repair_orphaned_coverage_once("otel_logs_and_spans"), Some(0), "orphan repair claims");
            assert_eq!(journal.repair_cursor(DAMAGE, "otel_logs_and_spans"), 0, "the orphan repair's cursor must not consume the damage list");
            journal.advance_repair_cursor(DAMAGE, "otel_logs_and_spans", 24).expect("advance");
            assert_eq!(journal.repair_cursor(DAMAGE, "otel_metrics"), 0, "a different source is consumed independently");
            // Monotonic: `SourceCursor` replay folds with `max()`, and a pass
            // that resolved less than an earlier one must never rewind the list.
            journal.advance_repair_cursor(DAMAGE, "otel_logs_and_spans", 3).expect("advance");
            assert_eq!(journal.repair_cursor(DAMAGE, "otel_logs_and_spans"), 24);
        }
        let reloaded = TaskJournal::load(dir.path()).expect("reload");
        assert_eq!(reloaded.repair_cursor(DAMAGE, "otel_logs_and_spans"), 24, "a restart resumes at the prefix, it does not start over");
    }

    /// A COMPLETE day unit must not block coarsening, or the queue fills with
    /// sub-day slices that can never collapse.
    ///
    /// Once a day was rolled up once, its day task stayed in the journal as
    /// Complete forever, so every later invalidation minted a ten-minute slice
    /// that was skipped here and processed alone. A slice is not cheaper than the
    /// day: an uncompacted sealed partition's files each span the WHOLE day, so a
    /// ten-minute slice reads the same files a day unit would. Prod 2026-08-18,
    /// via the phase timing from #174:
    ///
    ///     scan_ms=481682  stage_ms=30  commit_ms=961  rows=142
    ///
    /// Eight minutes of scanning for 142 rows, 144 times a day where once would
    /// do, while `maintenance_sealed_slices_coarsened` logged ZERO against 37,065
    /// pending base rollups.
    #[test]
    fn a_completed_day_unit_does_not_block_coarsening_but_a_superseded_one_does() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let now = 10 * DAY_MICROS;

        // Day 2: a day unit that already ran to completion, plus fresh slices
        // minted by a later invalidation. These MUST collapse.
        let done = task("p", 2 * DAY_MICROS, 3 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(done.clone(), 0, 0, 0);
        journal.complete(&done);
        assert_eq!(journal.state(&done), Some(TaskState::Complete), "precondition");
        for slot in 0..6 {
            let start = 2 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            journal.enqueue(task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup).key, 0, 10, 0);
        }

        // Day 6: a day unit SPLIT because it was too big. Its children must NOT
        // collapse, or the split is undone and the two fight forever.
        let parent = task("p", 6 * DAY_MICROS, 7 * DAY_MICROS, Operation::BaseRollup).key;
        journal.enqueue(parent.clone(), 0, 0, 0);
        journal.split_time_task(&parent, MAX_DECODED_BYTES.saturating_add(1), None);
        assert_eq!(journal.state(&parent), Some(TaskState::Superseded), "precondition");

        journal.coarsen_sealed_slices(now);

        let widths = |day: i64| {
            journal
                .tasks()
                .filter(|t| t.state == TaskState::Pending && t.key.slice.start_micros >= day * DAY_MICROS && t.key.slice.start_micros < (day + 1) * DAY_MICROS)
                .map(|t| t.key.slice.width())
                .collect::<Vec<_>>()
        };
        assert_eq!(widths(2), vec![DAY_MICROS], "a completed day must re-coarsen to one day unit, not stay as slices");
        assert!(widths(6).iter().all(|w| *w < DAY_MICROS), "a SPLIT day's children must stay split — recoarsening them loops forever");
    }

    #[test]
    fn the_sealed_reservation_yields_while_the_frontier_is_behind() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let now = 10 * DAY_MICROS;
        let claims = |lag: u64| {
            let dir = tempfile::tempdir().expect("temp dir");
            let mut journal = TaskJournal::load(dir.path()).expect("journal");
            // One frontier slice (ends at `now`) and one sealed day, both eligible.
            let frontier = task("p", now - 600_000_000, now, Operation::BaseRollup).key.clone();
            let sealed = task("p", now - 5 * DAY_MICROS, now - 4 * DAY_MICROS, Operation::BaseRollup).key.clone();
            journal.enqueue(frontier, 0, 0, 0);
            journal.enqueue(sealed, 0, 0, 0);
            journal.frontier_lag_secs.store(lag, std::sync::atomic::Ordering::Relaxed);
            let mut sealed_claims = 0;
            for _ in 0..8 {
                let Some(claimed) = journal.claim_next(Operation::BaseRollup, now, true) else { continue };
                if !is_live_frontier(claimed.key.slice, now) {
                    sealed_claims += 1;
                }
                journal.complete(&claimed.key);
                // Re-open both so every tick has a choice to make.
                journal.enqueue(claimed.key.clone(), 0, 0, 0);
            }
            sealed_claims
        };

        assert!(claims(0) > 0, "a keeping-up frontier must still leave sealed work its reserved share");
        assert!(
            claims(FRONTIER_LAG_BUDGET_SECS + 1) < claims(0),
            "past the lag budget the frontier must take a bigger share, because a frontier that never finishes today \
             guarantees tomorrow's yesterday is holed and coverage can never reach thirty contiguous days"
        );
        assert!(
            claims(FRONTIER_LAG_BUDGET_SECS + 1) > 0,
            "but sealed must keep SOME share: withdrawing it entirely stops the backfill that builds 30d coverage"
        );
    }

    #[test]
    fn sealed_backfill_units_outrank_yesterdays_fine_slices() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        // "Now" well past both, so everything is sealed rather than frontier.
        let now = 10 * DAY_MICROS;

        // Yesterday's fine-grained leftovers: newer, and far more numerous.
        for slot in 0..12 {
            let start = 8 * DAY_MICROS + slot * NORMAL_SLICE_MICROS;
            let fine = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup);
            journal.enqueue(fine.key.clone(), 0, 0, 0);
        }
        // An older day, queued whole by the backfill planner.
        let coarse = task("p", 3 * DAY_MICROS, 4 * DAY_MICROS, Operation::BaseRollup);
        journal.enqueue(coarse.key.clone(), 0, 0, 0);

        let claimed = journal.claim_next(Operation::BaseRollup, now, true).expect("a sealed task is claimable");
        assert_eq!(
            claimed.key.slice.width(),
            DAY_MICROS,
            "the horizon-advancing day unit must be claimed before yesterday's ten-minute slices; got a {}s slice starting {}",
            claimed.key.slice.width() / 1_000_000,
            claimed.key.slice.start_micros / DAY_MICROS
        );
    }

    /// A unit that cannot finish inside its deadline must get SMALLER, not be
    /// requeued unchanged.
    ///
    /// The lease requeues whatever the worker abandoned, so a slice too big for
    /// its deadline times out, requeues identical, times out again — forever,
    /// holding a worker for the full deadline each time and never producing
    /// anything. Prod 2026-08-17: five Dedup timeouts in twelve minutes at 300s
    /// apiece, permanently occupying ~2 of 16 coordinator workers, while total
    /// task starts fell to 2.2/min and the rollup horizon it was starving sat
    /// days behind. Byte-based splitting cannot catch this — a day-sized slice
    /// with modest bytes still pays one object-store round trip per file.
    /// Verbatim prod text, 2026-08-17. Classification is string-based because
    /// the DataFusion error arrives type-erased through delta-rs/anyhow, so the
    /// strings are the contract and this test is what pins them.
    #[test]
    fn capacity_failures_are_recognised_from_prod_text() {
        assert!(is_capacity_failure(
            "dedup: Not enough memory to continue external sort. Consider increasing the memory limit config: \
             'datafusion.runtime.memory_limit', or decreasing the config: 'datafusion.execution.sort_spill_reservation_bytes'."
        ));
        assert!(is_capacity_failure("compaction: Resources exhausted: Additional allocation failed for ExternalSorter[1] with top memory consumers"));
        for benign in ["dedup: Object at location ... not found", "compaction: transaction failed: version 2667 already exists", "source_not_flushed"] {
            assert!(!is_capacity_failure(benign), "must not shrink a slice over a fault that has nothing to do with size: {benign}");
        }
    }

    /// A slice too big for the pool fails identically every pass. Back off once
    /// (a FairSpillPool squeeze is transient), then shrink it.
    #[test]
    fn a_unit_that_cannot_fit_bisects_instead_of_retrying_at_the_same_size() {
        const DAY_MICROS: i64 = 86_400_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("whale", 0, DAY_MICROS, Operation::Dedup).key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);
        let oom = "dedup: Not enough memory to continue external sort.".to_owned();

        journal.retry_or_split(&key, oom.clone(), 1, 1);
        assert_eq!(journal.state(&key), Some(TaskState::Retry), "one squeeze may be someone else's fault; retry it whole");

        journal.retry_or_split(&key, oom, 2, 2);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "a repeat says the slice itself does not fit");
        let widths: Vec<i64> = journal.tasks().filter(|t| t.state != TaskState::Superseded).map(|t| t.key.slice.width()).collect();
        assert!(!widths.is_empty() && widths.iter().all(|w| *w < DAY_MICROS), "bisection must leave smaller claimable children; got {widths:?}");
    }

    /// Only capacity failures shrink. A missing object or a commit conflict says
    /// nothing about size, and splitting on it would shred a healthy slice.
    #[test]
    fn an_unrelated_failure_retries_whole_however_often_it_repeats() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let key = task("steady", 0, 86_400_000_000, Operation::Dedup).key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);
        for attempts in 1..6 {
            journal.retry_or_split(&key, "dedup: object not found".to_owned(), i64::from(attempts), attempts);
            assert_eq!(journal.state(&key), Some(TaskState::Retry), "attempt {attempts} must not split");
        }
    }

    /// A derived unit must read the base files that hold its rows, whatever
    /// width the unit that WROTE them happened to use.
    ///
    /// Prod 2026-08-17, exact numbers: derived units are one hour wide, while
    /// the backfill writes base units a whole day wide and tags each file with
    /// its own slice. The old test was CONTAINMENT — `file_end <= slice_end` —
    /// which a day-tagged file can never satisfy against an hour, so every
    /// backfilled day published rows=0 and was marked complete. Project
    /// 87576849 had 17,705 rows in the 1m tier for 08-03 while its 1h unit for
    /// 08-03 produced nothing, and 14d/30d queries never routed as a result.
    #[test]
    fn a_day_wide_base_file_feeds_every_hour_wide_derived_slice_inside_it() {
        const DAY: i64 = 1_785_628_800_000_000; // 2026-08-02T00:00Z
        const HOUR: i64 = 3_600_000_000;
        let day_file = (DAY, DAY + 86_400_000_000);

        for hour in 0..24 {
            let slice = TimeSlice::new(DAY + hour * HOUR, DAY + (hour + 1) * HOUR).expect("slice");
            assert!(slice.overlaps(day_file.0, day_file.1), "hour {hour} must read the day-wide base file that holds its rows");
            // The rule this replaced, stated so the regression is unmistakable.
            let contained = day_file.0 >= slice.start_micros && day_file.1 <= slice.end_micros;
            assert!(!contained, "containment is what broke: hour {hour} could never select a day-tagged file");
        }

        // Overlap must still EXCLUDE what it should: neighbouring days.
        let slice = TimeSlice::new(DAY, DAY + HOUR).expect("slice");
        assert!(!slice.overlaps(DAY - 86_400_000_000, DAY), "the previous day ends exactly at the boundary and must not be read");
        assert!(!slice.overlaps(DAY + 86_400_000_000, DAY + 2 * 86_400_000_000), "a later day must not be read");
        // Ten-minute frontier files — the only shape that ever worked — still do.
        assert!(slice.overlaps(DAY, DAY + 600_000_000));
    }

    #[test]
    fn a_unit_that_keeps_timing_out_bisects_instead_of_retrying_forever() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        const DAY_MICROS: i64 = 86_400_000_000;
        let input = task("whale", 0, DAY_MICROS, Operation::Dedup);
        let key = input.key.clone();
        journal.enqueue(key.clone(), 0, 0, 0);

        // Two abandoned runs: the first is an ordinary blip and must simply
        // retry, the second says the slice itself does not fit.
        for _ in 0..2 {
            assert!(journal.mark_running(&key));
            journal.abandon_running(&key, 0, None);
        }

        let widths: Vec<i64> = journal.tasks().filter(|t| t.state != TaskState::Superseded).map(|t| t.key.slice.width()).collect();
        assert!(!widths.is_empty(), "bisection must leave claimable children behind");
        assert!(widths.iter().all(|width| *width < DAY_MICROS), "a repeatedly-abandoned day must bisect; got widths {widths:?} still at the full day");
        assert_eq!(journal.state(&key), Some(TaskState::Superseded), "the parent stays as an audit record");
    }

    #[test]
    fn replanning_live_debt_reopens_one_idempotent_task() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, MIN_SLICE_MICROS, Operation::SealedConsolidation);
        let key = input.key.clone();
        journal.enqueue(key.clone(), 10, 20, 1);
        journal.complete(&key);
        journal.enqueue(key.clone(), 5, 30, 1);
        journal.enqueue(key.clone(), 5, 30, 1);
        let tasks = journal.tasks().collect::<Vec<_>>();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].state, TaskState::Pending);
        assert_eq!(tasks[0].deadline_micros, 5);
        assert_eq!(tasks[0].estimated_decoded_bytes, 30);
    }

    #[test]
    fn derived_rollup_claim_waits_for_complete_base_hour() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut base_keys = Vec::new();
        for start in (0..DERIVED_SLICE_MICROS).step_by(NORMAL_SLICE_MICROS as usize) {
            let base = task("p", start, start + NORMAL_SLICE_MICROS, Operation::BaseRollup);
            base_keys.push(base.key.clone());
            journal.upsert(base);
        }
        journal.upsert(task("p", 0, DERIVED_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_none());
        for key in base_keys {
            journal.complete(&key);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some());
    }

    #[test]
    fn oversized_task_is_replaced_by_durable_time_children() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let input = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None));
        assert_eq!(journal.tasks().filter(|task| task.state == TaskState::Pending).count(), 2);
        assert_eq!(journal.state(&key), Some(TaskState::Superseded));
    }

    #[test]
    fn live_frontier_lag_prefers_pending_split_child_over_superseded_parent() {
        let now = 10 * 24 * 60 * 60 * 1_000_000;
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let mut input = task("p", now - NORMAL_SLICE_MICROS, now, Operation::BaseRollup);
        input.deadline_micros = now - 2 * 60 * 1_000_000;
        let key = input.key.clone();
        journal.upsert(input);
        assert!(journal.split_time_task(&key, 2 * MAX_DECODED_BYTES, None));
        assert_eq!(live_frontier_lag_secs(journal.tasks(), now), 2 * 60);
    }

    #[test]
    fn completed_children_satisfy_a_larger_derived_dependency() {
        let dir = tempfile::tempdir().expect("temp dir");
        let mut journal = TaskJournal::load(dir.path()).expect("journal");
        let base = task("p", 0, NORMAL_SLICE_MICROS, Operation::BaseRollup);
        let base_key = base.key.clone();
        journal.upsert(base);
        journal.upsert(task("p", 0, NORMAL_SLICE_MICROS, Operation::DerivedRollup));
        assert!(journal.split_time_task(&base_key, 2 * MAX_DECODED_BYTES, None));
        let children = journal
            .tasks()
            .filter(|task| task.key.operation == Operation::BaseRollup && task.state == TaskState::Pending)
            .map(|task| task.key.clone())
            .collect::<Vec<_>>();
        assert_eq!(children.len(), 2);
        for child in children {
            journal.complete(&child);
        }
        assert!(journal.claim_next(Operation::DerivedRollup, 0, true).is_some());
    }

    #[test]
    fn admission_is_all_or_nothing_and_keeps_memory_headroom() {
        let admission = AdmissionController::new(4, 1_000, 8, 2);
        let request = Resources { cpu: 2, decoded_bytes: 600, object_reads: 4, object_writes: 1 };
        let permit = admission.try_acquire(request).expect("first reservation");
        assert!(admission.try_acquire(request).is_none());
        assert_eq!(admission.utilization(), request);
        drop(permit);
        assert_eq!(admission.utilization(), Resources::default());
        assert!(admission.try_acquire(Resources { decoded_bytes: 751, ..Resources::default() }).is_none());
        assert!(admission.try_acquire(Resources { decoded_bytes: MAX_DECODED_BYTES + 1, ..Resources::default() }).is_none());
    }
}
