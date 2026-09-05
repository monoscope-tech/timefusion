use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * 1024 * 1024;

fn read_parsed<T>(path: &str, parse: impl FnOnce(&str) -> Option<T>) -> Option<T> {
    std::fs::read_to_string(path).ok().and_then(|s| parse(&s))
}

fn parse_cgroup_v2_memory_max(content: &str) -> Option<usize> {
    content.trim().parse().ok()
}

/// Parse cgroup v1 `memory.limit_in_bytes` content. v1 reports a huge
/// sentinel (close to `i64::MAX`) for "unlimited" instead of a keyword;
/// treat anything past 2^62 bytes as unlimited.
fn parse_cgroup_v1_memory_limit(content: &str) -> Option<usize> {
    let v = content.trim().parse::<usize>().ok()?;
    (v < (1_usize << 62)).then_some(v)
}

fn parse_meminfo_total_bytes(content: &str) -> Option<usize> {
    content.lines().find(|l| l.starts_with("MemTotal:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|kb| kb.parse::<usize>().ok()).map(|kb| kb * 1024)
}

/// Parse cgroup v2 `cpu.max` content (`"<quota> <period>"` or `"max
/// <period>"`) into a whole-core count, rounded up. `None` for unlimited.
fn parse_cgroup_cpu_max(content: &str) -> Option<usize> {
    let mut parts = content.split_whitespace();
    let (quota, period) = (parts.next()?.parse::<f64>().ok()?, parts.next()?.parse::<f64>().ok()?);
    (quota > 0.0 && period > 0.0).then(|| ((quota / period).ceil() as usize).max(1))
}

/// Detect the effective memory limit in bytes: cgroup v2 → cgroup v1 →
/// `/proc/meminfo` total → a conservative 8 GiB floor. Never panics.
fn detect_memory_limit_bytes() -> usize {
    read_parsed("/sys/fs/cgroup/memory.max", parse_cgroup_v2_memory_max)
        .or_else(|| read_parsed("/sys/fs/cgroup/memory/memory.limit_in_bytes", parse_cgroup_v1_memory_limit))
        // No cgroup limit → unmanaged box: an explicit env override is safe HERE
        // only (off-box CLI / dev boxes) — prod always runs under a cgroup, so
        // the misconfigured-knob OOM loop can't recur through this path.
        .or_else(|| {
            env_memory_override_bytes().inspect(|v| tracing::warn!("budget tree: no cgroup limit; using TIMEFUSION_MEMORY_LIMIT_GB override ({} GiB)", v / GIB))
        })
        // Shared host: budget HALF the machine, loudly — sizing from full host
        // RAM inside a container caused a memcg OOM-loop, so stay conservative.
        .or_else(|| {
            read_parsed("/proc/meminfo", parse_meminfo_total_bytes)
                .map(|v| v / 2)
                .inspect(|v| tracing::warn!("budget tree: no cgroup memory limit; deriving from HALF of host RAM ({} GiB)", v / GIB))
        })
        // macOS (dev / off-box CLI): same shared-host half-the-machine rule.
        .or_else(|| {
            #[cfg(target_os = "macos")]
            let macos = Some(
                sysinfo::System::new_with_specifics(sysinfo::RefreshKind::new().with_memory(sysinfo::MemoryRefreshKind::everything())).total_memory() as usize
                    / 2,
            )
            .filter(|half| *half > 0)
            .inspect(|half| tracing::warn!("budget tree: no cgroup; deriving from HALF of host RAM ({} GiB)", half / GIB));
            #[cfg(not(target_os = "macos"))]
            let macos = None;
            macos
        })
        .unwrap_or_else(|| {
            tracing::warn!("budget tree: could not detect memory limit from cgroup or /proc/meminfo; falling back to 8 GiB");
            8 * GIB
        })
}

/// `TIMEFUSION_MEMORY_LIMIT_GB`, parsed. Consulted ONLY when no cgroup limit
/// exists (see `detect_memory_limit_bytes`) — a containerized deployment can
/// never be resized by env var.
fn env_memory_override_bytes() -> Option<usize> {
    std::env::var("TIMEFUSION_MEMORY_LIMIT_GB").ok()?.parse::<usize>().ok().filter(|gb| *gb > 0).map(|gb| gb * GIB)
}

/// `TIMEFUSION_MEMORY_BUDGET_GB`: sizes the whole tree BELOW the cgroup limit.
///
/// A single input — every budget derives from it, so shares can't drift out of
/// proportion (the failure mode that got the old per-consumer knobs removed).
/// Needed because the tree otherwise budgets 100% of the cgroup, which
/// oversubscribes a shared host (TF's container sits alongside other services
/// on the same box, and growing into its entitlement gets it OOM-killed).
/// Lowering the container limit fixes it too but needs an orchestrator change
/// and a redeploy; this lets the process size itself down instead.
///
/// Only ever LOWERS the effective limit — an over-large value is clamped,
/// never honoured.
fn env_memory_budget_bytes() -> Option<usize> {
    std::env::var("TIMEFUSION_MEMORY_BUDGET_GB").ok()?.parse::<f64>().ok().filter(|gb| *gb > 0.0).map(|gb| (gb * GIB as f64) as usize)
}

fn detect_memory_limit_clamped() -> usize {
    // A v1 "no limit" sentinel or an over-committed cgroup can report more than
    // physical RAM; clamp so the tree never budgets memory the host lacks.
    let detected = detect_memory_limit_bytes();
    read_parsed("/proc/meminfo", parse_meminfo_total_bytes).map_or(detected, |host| detected.min(host))
}

pub(crate) fn detect_cores() -> usize {
    let host = std::thread::available_parallelism().map(NonZeroUsize::get).unwrap_or(4);
    let read_i64 = |p: &str| read_parsed(p, |s| s.trim().parse::<i64>().ok());
    // cgroup v2 cpu.max, then v1 cfs_quota/period; a quota can exceed host
    // parallelism on misconfigured hosts, so clamp. THE process-wide core
    // detector — `config::apply` reads it too, so partitions and the budget
    // tree can never size from different answers.
    read_parsed("/sys/fs/cgroup/cpu.max", parse_cgroup_cpu_max)
        .or_else(|| {
            let (quota, period) = (read_i64("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?, read_i64("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?);
            (quota > 0 && period > 0).then(|| ((quota as f64 / period as f64).ceil() as usize).max(1))
        })
        .map_or(host, |c| c.clamp(1, host))
}

/// Self-sizing memory/concurrency budget derived once at startup from the
/// container's cgroup limits. See docs/compaction-redesign-2026-07-29.md §4.
///
/// Fixed fractions are opinions pinned in code (no override — that's the
/// point of deleting the env vars): a workload needing a different split
/// changes the constants here, not a knob in production.
#[derive(Debug, Clone, Copy)]
pub struct DerivedBudget {
    pub memory_limit_bytes: usize,
    pub cores: usize,
    query_pool_bytes: usize,
    ingest_buffer_bytes: usize,
    foyer_memory_bytes: usize,
    writer_reserve_bytes: usize,
    maintenance_pool_bytes: usize,
    profile: BudgetProfile,
}

/// Reservation shape selected internally for a server or one-shot maintenance CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, strum::EnumString)]
#[strum(serialize_all = "kebab-case")]
pub enum BudgetProfile {
    #[default]
    Server,
    MaintenanceCli,
}

/// Anything unrecognised (including unset) is the server profile.
fn profile_from_env() -> BudgetProfile {
    std::env::var("TIMEFUSION_BUDGET_PROFILE").ok().and_then(|v| v.parse().ok()).unwrap_or_default()
}

// 0.20, down from 0.25: sampled pool usage sat at 0 while the 70% memory
// brake chronically halted light-compaction waves. The freed headroom favors
// maintenance — a query past its pool spills (one slower scan), while a
// stopped hot-tail compaction backlogs the whole table.
const QUERY_POOL_FRACTION: f64 = 0.20;
/// Share reserved for consumers no pool tracks — parquet decode heap, pgwire
/// parse ASTs, allocator overhead (measured ~10-20 GiB on prod). Carved out
/// before maintenance takes the remainder so the tree never sanctions more
/// than the cgroup holds.
const UNTRACKED_SLACK_FRACTION: f64 = 0.15;
/// Ingest MemBuffer share of the limit. Reproduces today's working ratio
/// (24 GiB of a 120 GiB box).
const INGEST_BUFFER_FRACTION: f64 = 0.20;
/// Foyer read-cache share, deliberately larger than the previous ~3.3%: cache
/// hit-rate is what query latency lives on, and it was the most-starved
/// consumer relative to impact.
const FOYER_MEMORY_FRACTION: f64 = 0.10;
/// Per-(rewrite-permit × merge-task) delta-rs writer buffer. Previously
/// budgeted nowhere — the 06-11 OOM was exactly this gap.
const WRITER_RESERVE_PER_TASK_BYTES: usize = 3 * GIB / 2;
/// delta-rs concurrent merge tasks per optimize run (unchanged default).
const OPTIMIZE_MERGE_TASKS: usize = 2;
/// Cap on files per optimize merge bin. Byte-only bins pack hundreds of tiny
/// files into one rewrite whose merge fan-in scales with fragmentation —
/// memory demand peaks exactly when compaction is most needed. 32 × ~35MB
/// batches ≈ ~1GB peak per merge; repeated cron passes converge fragmented
/// partitions to target.
const OPTIMIZE_MAX_FILES_PER_BIN: NonZeroUsize = NonZeroUsize::new(32).unwrap();
/// Concurrent heavy maintenance rewrites (dedup/optimize/recompress).
/// Formerly `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`. 10, at the full
/// 4 GiB per-sort budget: at fewer permits, coordinator jobs spent most of
/// their wall clock BLOCKED on this semaphore rather than working, freezing
/// rollup coverage. Raising coordinator jobs alone doesn't help — it just
/// queues deeper here. `PER_SORT_BUDGET_BYTES` is a spill THRESHOLD on a
/// FairSpillPool, not a hard reservation, so extra permits buy
/// parallelism/spill rather than blowing the bound; the 85% memory brake is
/// the backstop. Watch `permit_wait_ms`, RSS against the brake, and
/// `occ_conflicts_total` before raising further.
const HEAVY_REWRITE_PERMITS: usize = 10;
/// Per-sort budget. A spill THRESHOLD on a FairSpillPool, not a hard
/// reservation — a sort that exceeds it degrades to bounded disk spill rather
/// than failing, so more spilling-capable parallel sorts beat fewer
/// comfortable ones. 2 GiB, HALVED from 4 GiB to pay for
/// `HEAVY_REWRITE_PERMITS` going 4 -> 10: leaving it at 4 GiB while permits
/// rose caused an OOM kill (anon-rss 124.9 GB, two minutes after 11.9 GB) —
/// halving keeps the extra concurrency while landing the fan-in envelope near
/// where it last ran clean (10 x 2 = 20 GiB vs the old 4 x 4 = 16 GiB).
const PER_SORT_BUDGET_BYTES: usize = 2 * GIB;
/// Per-sort budget for the COORDINATOR rewrite path specifically, measured
/// rather than inherited.
///
/// `PER_SORT_BUDGET_BYTES` is 2 GiB partly because a sort could be handed a
/// 2 GB row group; capping row groups by measured decoded bytes removed that,
/// and it also sizes the `HEAVY_REWRITE_PERMITS` envelope that was tuned
/// against an OOM — so it stays where it is and the coordinator gets its own
/// number.
///
/// 1.25 GiB comes from `benches/rewrite_throughput.rs` (`TF_BENCH_FLEET=1`), N
/// concurrent rewrites of a real 204 MB prod file sharing one 8 GiB pool:
///
/// ```text
/// 4 workers  29.16 MB/s  0 failed
/// 5 workers  29.31 MB/s  0 failed
/// 6 workers  33.32 MB/s  0 failed   <- best
/// 8 workers  15.07 MB/s  4 FAILED   <- cliff
/// ```
///
/// Six concurrent sorts fit 8 GiB, so the real footprint is ~1.33 GiB, not 2.
/// At 1.25 GiB `light_optimize_k` yields 5, plus the one repair permit = **6**
/// — the measured optimum, and one rung below the measured cliff.
const COORDINATOR_PER_SORT_BUDGET_BYTES: usize = 5 * GIB / 4;
/// Concurrent target-sized repair rewrites the repair budget must hold.
///
/// A repair unit is exactly ONE file (`coordinator_compaction_files` takes 1
/// for Repair), so unlike every other lane it cannot be split to fit a budget —
/// the budget must fit IT. The prior-art survey's rule 1 is that a compaction
/// size cap is a MULTIPLE of the target file size, read from metadata: RocksDB
/// `max_compaction_bytes = 25 x target_file_size_base`, IOx `max_compact_size`
/// = 3x target, Iceberg `max-file-group-size-bytes`. Ours was **0.42x** the
/// target once decoded, so no correctly-sized file ever fit and every unit's
/// request clamped to the whole semaphore — repair serialized to ~1.2 rewrites
/// an hour with 310 units queued (see `repair_budget_must_fit_one_target_sized_file`).
///
/// 2, not more, because what the 8 GiB coordinator pool is proven to hold is
/// SIX concurrent sorts (`benches/rewrite_throughput.rs`, cliff at 8). The
/// split moves from 5 light + 1 repair to 4 light + 2 repair — same envelope,
/// still two rungs below the measured cliff — so this buys repair throughput
/// without widening total concurrency. Raise it only alongside a bench that
/// moves the cliff.
const REPAIR_REWRITE_TARGET_FILES: usize = 2;
/// Decoded bytes one byte of sort pool can carry before rewrites start failing.
///
/// THE UNIT CONVERSION the repair lane was missing. Its semaphore is priced in
/// DECODED bytes (what admission grants) while the pool it draws from is priced
/// in POOL bytes, and until now one constant stood in for both — so raising
/// repair's concurrency silently re-priced `light_optimize_k`'s holdback, and
/// the two could never be reasoned about separately.
///
/// Measured, `benches/rewrite_throughput.rs` fleet ladder, one shared 8 GiB pool
/// against a real 204 MB prod file at 2,451 MB decoded each:
///
/// ```text
/// 6 workers  14.7 GB decoded / 8 GiB pool = 1.79x   0 failed
/// 8 workers  19.6 GB decoded / 8 GiB pool = 2.39x   4 FAILED
/// ```
///
/// 1.79 is the passing point, not a midpoint guess — the cliff is between these
/// two rungs and this takes the safe side of it.
const SAFE_DECODED_PER_POOL_BYTE: f64 = 1.79;
/// Heavy maintenance's minimum share of the maintenance pool. 0.40, up from
/// 0.25 — a REBALANCE inside the existing pool (total unchanged), following
/// the workload: hot-tail packing (light share) converged once unstarved,
/// while the dedup queue kept growing, so heavy is where the backlog lives.
/// 0.40 specifically because `light_optimize_k` divides light's share by
/// `PER_SORT_BUDGET_BYTES`; going lower still yields the same K for packing
/// while buying heavy little, since heavy's concurrency is bounded by
/// rewrite permits, not bytes.
/// Heavy maintenance's slice of the whole maintenance pool. 0.30 is what the
/// old `0.40 of the residual` came to before the coordinator's share grew; see
/// `heavy_share_bytes` for why it is no longer expressed against the residual.
const HEAVY_POOL_SHARE: f64 = 0.30;

/// Pool slice per in-flight coordinator job. Kept equal to the admission
/// ceiling so a job that is allowed to decode N bytes has N bytes of pool to
/// hold them in before it must spill; see `coordinator_share_bytes`.
const COORDINATOR_JOB_POOL_BYTES: usize = crate::maintenance_coordinator::MAX_DECODED_BYTES as usize;
/// Floor so a tiny box never zeroes the maintenance pool.
const MAINTENANCE_FLOOR_BYTES: usize = GIB;

/// Ceiling on the query session's `target_partitions` — see the long note at
/// the `TIMEFUSION_QUERY_PARTITIONS` tune site. It bounds the sort machinery's
/// per-partition, non-spillable reservations, which is what exhausted the query
/// pool on the 48-core box.
/// 16 -> 24 on 2026-09-04, as a bounded step and NOT a return to 48.
///
/// This is the measured ceiling on a wide query's read concurrency, and wide
/// aggregates are IO-bound: container CPU is identical with and without a 30-day
/// query (maintenance already holds ~17 of 48 cores and ~150-200 MB/s), so reads
/// in flight IS the throughput. Two client sessions — i.e. 2x16 workers — do the
/// same 30 days in 11.8/21.3/14.6 s that one query takes 37.8/43.0/30.2 s to do.
/// One query could not use more, which is why four attempts at CPU parallelism
/// changed nothing.
///
/// 24 is deliberately short of the 48 that caused the incidents this cap was
/// created for (2026-08-28/30/31 `ExternalSorterMerge` / TopK / SPM exhausting
/// the 16 GiB query pool, plus monoscope's `row_number() OVER (…)` as XX000).
/// Those reservations are non-spillable and scale with this number, so it buys
/// +50% scan concurrency for +50% sort reservation rather than +200%.
///
/// JUDGE IT ON BOTH SIDES: 30-day completion rate AND the absence of
/// `Resources exhausted` on sort-bound pages. `TIMEFUSION_QUERY_PARTITIONS`
/// overrides it, so rollback is env-only and needs no rebuild.
const QUERY_PARTITIONS_MAX: usize = 24;

/// The number the whole tree derives from: the detected limit, LOWERED by an
/// operator request — budgeting above the cgroup is never valid, so an
/// over-large request is clamped rather than honoured.
fn effective_limit(detected: usize, requested: Option<usize>) -> usize {
    requested.map_or(detected, |b| b.min(detected))
}

impl Default for DerivedBudget {
    fn default() -> Self {
        Self::from_limits(8 * GIB, 4)
    }
}

impl DerivedBudget {
    /// Pure derivation over an already-detected limit/core count — the seam
    /// unit tests drive directly (simulated boxes) without touching the
    /// filesystem.
    fn from_limits(memory_limit_bytes: usize, cores: usize) -> Self {
        Self::from_limits_with_profile(memory_limit_bytes, cores, BudgetProfile::Server)
    }

    fn from_limits_with_profile(memory_limit_bytes: usize, cores: usize, profile: BudgetProfile) -> Self {
        if profile == BudgetProfile::MaintenanceCli {
            // No queries/ingest: token slices for query pool and foyer, the
            // full writer-reserve cap (delta-rs output buffers are real in a
            // CLI), rest to maintenance. An 8 GiB pod derives ~6 GiB of
            // sort/spill instead of the 1 GiB floor.
            let query_pool_bytes = (memory_limit_bytes as f64 * 0.08) as usize;
            let ingest_buffer_bytes = (memory_limit_bytes as f64 * 0.02) as usize;
            let foyer_memory_bytes = (memory_limit_bytes as f64 * 0.02) as usize;
            let writer_reserve_bytes = (HEAVY_REWRITE_PERMITS * OPTIMIZE_MERGE_TASKS * WRITER_RESERVE_PER_TASK_BYTES).min(memory_limit_bytes / 10);
            let reserved = query_pool_bytes + ingest_buffer_bytes + foyer_memory_bytes + writer_reserve_bytes;
            let maintenance_pool_bytes = memory_limit_bytes.saturating_sub(reserved).max(MAINTENANCE_FLOOR_BYTES);
            return Self {
                memory_limit_bytes,
                cores,
                query_pool_bytes,
                ingest_buffer_bytes,
                foyer_memory_bytes,
                writer_reserve_bytes,
                maintenance_pool_bytes,
                profile,
            };
        }
        // Fixed fraction, not the old TIMEFUSION_MEMORY_FRACTION knob: that
        // 0.75 was calibrated against a hand-set limit and, applied to the
        // real cgroup, would crush maintenance to K=1 — the drift-class bug
        // this tree exists to kill. 0.20 of the real limit is ~1.25x the old
        // effective pool.
        let query_pool_bytes = (memory_limit_bytes as f64 * QUERY_POOL_FRACTION) as usize;
        let ingest_buffer_bytes = (memory_limit_bytes as f64 * INGEST_BUFFER_FRACTION) as usize;
        let foyer_memory_bytes = (memory_limit_bytes as f64 * FOYER_MEMORY_FRACTION) as usize;
        // Capped at 10% of the limit: the full 6 GiB reserve on an 8 GiB dev
        // box budgeted 142% of the container — the drift class this tree kills.
        let writer_reserve_bytes = (HEAVY_REWRITE_PERMITS * OPTIMIZE_MERGE_TASKS * WRITER_RESERVE_PER_TASK_BYTES).min(memory_limit_bytes / 10);
        // UNTRACKED-CONSUMER SLACK, carved out BEFORE maintenance takes the
        // remainder. Without it the tree hands maintenance everything left,
        // and consumers no pool tracks (parquet decode, giant-INSERT parse
        // ASTs, allocator overhead) push the box over the cgroup limit —
        // every subsystem behaved "legally", the sum was the bug. 15% covers
        // the measured untracked peak; the resulting maintenance shrink was
        // proven harmless (512MB bins sort+spill fine on smaller pools).
        let untracked_slack_bytes = (memory_limit_bytes as f64 * UNTRACKED_SLACK_FRACTION) as usize;
        let reserved = query_pool_bytes + ingest_buffer_bytes + foyer_memory_bytes + writer_reserve_bytes + untracked_slack_bytes;
        let maintenance_pool_bytes = memory_limit_bytes.saturating_sub(reserved).max(MAINTENANCE_FLOOR_BYTES);
        Self { memory_limit_bytes, cores, query_pool_bytes, ingest_buffer_bytes, foyer_memory_bytes, writer_reserve_bytes, maintenance_pool_bytes, profile }
    }

    /// Detect the real container limits and derive the tree. The only env input
    /// is `TIMEFUSION_MEMORY_BUDGET_GB`, which can lower (never raise) the one
    /// number the whole tree derives from — see `env_memory_budget_bytes`.
    pub fn compute() -> Self {
        Self::from_limits_with_profile(effective_limit(detect_memory_limit_clamped(), env_memory_budget_bytes()), detect_cores(), profile_from_env())
    }

    pub fn query_pool_bytes(&self) -> usize {
        self.query_pool_bytes
    }

    pub fn buffer_max_bytes(&self) -> usize {
        self.ingest_buffer_bytes
    }

    pub fn foyer_memory_bytes(&self) -> usize {
        self.foyer_memory_bytes
    }

    /// The shared cache reservation is split between raw object bytes and
    /// exact logical-count indexes. Keeping both inside the existing 10%
    /// reservation prevents the derived cache from becoming untracked heap.
    pub fn object_cache_memory_bytes(&self) -> usize {
        self.foyer_memory_bytes / 2
    }

    pub fn logical_count_memory_bytes(&self) -> usize {
        self.foyer_memory_bytes - self.object_cache_memory_bytes()
    }

    pub fn writer_reserve_bytes(&self) -> usize {
        self.writer_reserve_bytes
    }

    /// Formerly `TIMEFUSION_MEMORY_LIMIT_GB * GIB`.
    pub fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes
    }

    /// Formerly `MemoryConfig::maintenance_pool_bytes`. Hands `bytes` back
    /// from the maintenance pool (never below the floor); returns what was
    /// actually surrendered.
    ///
    /// Maintenance is the RESIDUAL claimant (`limit - reserved`), so it
    /// silently absorbed every ceiling `reserved` forgot (MemBuffer overshoot,
    /// tantivy writer peak, DataFusion metadata cache). Shrinking it to match
    /// the audit restores the intended reservation rather than taxing
    /// maintenance arbitrarily. See `config::apply`.
    pub fn reclaim_maintenance_pool(&mut self, bytes: usize) -> usize {
        let before = self.maintenance_pool_bytes;
        self.maintenance_pool_bytes = before.saturating_sub(bytes).max(MAINTENANCE_FLOOR_BYTES);
        before - self.maintenance_pool_bytes
    }

    pub fn maintenance_pool_bytes(&self) -> usize {
        self.maintenance_pool_bytes
    }

    /// The durable coordinator's own pool, carved off before the heavy/light
    /// split since the coordinator is now the primary maintenance path.
    ///
    /// Sized as `jobs x MAX_DECODED_BYTES` (each job may hold that much),
    /// capped at a QUARTER of the maintenance pool, not a half. Formerly a
    /// flat `MAX_DECODED_BYTES` — correct only while `coordinator_jobs` was 1;
    /// at 16 jobs that sliced the pool below `ExternalSorterMerge`'s 32 MB
    /// floor and units failed instead of spilling.
    ///
    /// `jobs x MAX_DECODED_BYTES` is the ceiling a fully-loaded coordinator
    /// could want, and it is now allowed to reach it.
    ///
    /// The quarter cap was written when only rollup units drew on this pool and
    /// "DEDUP units sort on the heavy share". That stopped being true when the
    /// coordinator took ownership of slice maintenance: `compact.rs`'s limited
    /// dedup path, `stage_hot_bin`, Repair and SealedConsolidation all pass
    /// `coordinator_runtime_env()`. So a quarter of the pool — 4.2 GB, split 16
    /// ways by the `FairSpillPool` — gave each rewrite ~265 MB against a 512 MB
    /// admission ceiling, and prod's journal carried 243 dedup units failed
    /// with "Not enough memory to continue external sort" (2026-08-31).
    ///
    /// Meanwhile the light share it was protecting (~7.6 GB) feeds
    /// `light_optimize_session_state` and `repair_session_state`, whose only
    /// callers sit under `stage_hot_bin`'s `runtime_env: None` arm and
    /// `optimize_table_light` — and `COORDINATOR_OWNS_SLICE_MAINTENANCE` left
    /// that function with no callers at all. The cap moves to three fifths so
    /// the ceiling binds instead: the pool that does the work gets the budget.
    pub fn coordinator_share_bytes(&self) -> usize {
        match self.profile {
            // The CLI drives engines directly; no coordinator runs.
            BudgetProfile::MaintenanceCli => 0,
            BudgetProfile::Server => (self.coordinator_jobs() * COORDINATOR_JOB_POOL_BYTES).min(self.maintenance_pool_bytes * 3 / 5),
        }
    }

    /// The decoded-bytes budget shared by concurrent repair rewrites.
    ///
    /// Exactly the one `COORDINATOR_PER_SORT_BUDGET_BYTES` that
    /// `light_optimize_k` already holds back for repair — this only changes how
    /// it is SPENT. A single count-of-1 permit prices the worst case onto every
    /// unit: prod's worst repair bin is 2.3 GB compressed (~28 GB decoded), two
    /// of which exhausted the pool on 2026-09-01, so the permit was set to 1 and
    /// every small bin inherited that limit. At ~20-50 min per rewrite that is
    /// ~2 units/hour against `pending_repair = 358` — 173 `repair_rewrite_permit_busy`
    /// events in 40 minutes, a queue flat by arithmetic.
    ///
    /// Sizing in bytes with a CLAMPED request keeps the property the count was
    /// protecting — a bin larger than the budget takes all of it and still runs
    /// alone — while letting small bins share. It can only ADD concurrency, and
    /// never for two large bins.
    ///
    /// Its OWN constant, no longer `COORDINATOR_PER_SORT_BUDGET_BYTES`. Sharing
    /// that one made the two numbers move together, and the other one is
    /// `light_optimize_k`'s divisor: raising it to fix repair would have cut
    /// hot-tail packing concurrency 2.4x, which is the 2026-09-01 outage in the
    /// opposite direction (K 3 -> 1, zero HotPacking units claimed in 45 minutes
    /// with 17 pending). Repair's holdback in `light_optimize_k` is what keeps
    /// the two in step now.
    ///
    /// Derived from the target file size rather than written as a byte count, so
    /// a change to what compaction produces cannot silently make repair
    /// unrunnable again — which is exactly how it broke.
    pub fn repair_rewrite_budget_bytes(&self) -> usize {
        REPAIR_REWRITE_TARGET_FILES * (crate::database::COORDINATOR_HOT_TARGET_BYTES as usize) * (crate::database::DECODED_BYTES_PER_COMPRESSED as usize)
    }

    /// The same budget in whole MiB, which is the unit the repair semaphore
    /// counts in — a permit per MiB keeps the numbers small enough for
    /// `Semaphore`'s permit ceiling while staying finer than any real bin.
    pub fn repair_rewrite_budget_mib(&self) -> usize {
        (self.repair_rewrite_budget_bytes() / MIB).max(1)
    }

    /// Per-sort budgets `light_optimize_k` must hold back so the repair lane's
    /// DECODED budget has enough POOL behind it.
    ///
    /// Derived rather than hand-set, which is the whole point: the semaphore and
    /// the holdback measure different things (decoded bytes vs pool bytes), and
    /// a single shared constant meant changing repair's concurrency moved
    /// hot-tail packing's permit count as a side effect. That coupling is the
    /// 2026-09-01 HotPacking outage class — K went 3 -> 1 and packing stopped
    /// being claimed at all.
    ///
    /// At today's values: 6,144 MiB decoded / 1.79 = 3,432 MiB of pool, which is
    /// 3 slices of `COORDINATOR_PER_SORT_BUDGET_BYTES`. The previous hand-set
    /// holdback of 2 implied 2.4x decoded-to-pool — past the 2.39x rung that
    /// FAILED on the bench. So this costs one light permit and buys repair's
    /// two-way concurrency an envelope it can actually run in.
    pub fn repair_pool_holdback_slices(&self) -> usize {
        let pool_bytes = self.repair_rewrite_budget_bytes() as f64 / SAFE_DECODED_PER_POOL_BYTE;
        (pool_bytes / COORDINATOR_PER_SORT_BUDGET_BYTES as f64).ceil() as usize
    }

    /// What heavy and light divide, once the coordinator has taken its share.
    fn maintenance_split_bytes(&self) -> usize {
        self.maintenance_pool_bytes - self.coordinator_share_bytes()
    }

    /// Heavy maintenance (dedup/optimize/recompress) share.
    ///
    /// A fraction of the WHOLE maintenance pool, not of what the coordinator
    /// left behind. As a fraction of the residual it moved whenever the
    /// coordinator's share moved — so raising the coordinator's cap would have
    /// quietly cut the heavy pool by 30%, and `stage_dedup_chunk` (the dedup
    /// REWRITE, as opposed to its probe) draws on exactly this pool. The
    /// fraction is chosen to hold the share it had when the coupling was
    /// removed; only the dead light share pays for the coordinator's increase.
    pub fn heavy_share_bytes(&self) -> usize {
        // MaintenanceCli: engines run one command at a time and each engine's
        // pool is a separate FairSpillPool, so both shares may claim ~the whole
        // pool — only the active engine ever allocates.
        match self.profile {
            BudgetProfile::MaintenanceCli => ((self.maintenance_pool_bytes as f64) * 0.85) as usize,
            BudgetProfile::Server => ((self.maintenance_pool_bytes as f64) * HEAVY_POOL_SHARE).min(self.maintenance_split_bytes() as f64 * 0.9) as usize,
        }
    }

    /// Light hot-tail compaction share — the remainder.
    pub fn light_share_bytes(&self) -> usize {
        match self.profile {
            BudgetProfile::MaintenanceCli => self.heavy_share_bytes(),
            BudgetProfile::Server => self.maintenance_split_bytes() - self.heavy_share_bytes(),
        }
    }

    /// Concurrent heavy maintenance rewrites. Formerly
    /// `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`.
    /// PINNED CONSTANT (not box-derived): concurrency caps guard against an
    /// uncapped-rewrite OOM, not a sizing miss.
    pub fn rewrite_permits(&self) -> usize {
        HEAVY_REWRITE_PERMITS
    }

    /// delta-rs concurrent merge tasks per optimize run. Formerly
    /// `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`.
    /// PINNED CONSTANT (not box-derived) — see `rewrite_permits`.
    pub fn optimize_merge_tasks(&self) -> usize {
        OPTIMIZE_MERGE_TASKS
    }

    /// Scan batch size for maintenance sessions. Merge/sort memory has batch
    /// granularity (a batch is indivisible — it must be admitted to the pool
    /// before it can ever spill), and 2048-row otel batches reach ~35-150 MB.
    /// Under the CLI profile's small cgroups the unit drops to 256 rows
    /// (~4-20 MB) so tiny pools can admit, buffer, and spill.
    pub fn maintenance_batch_size(&self) -> &'static str {
        match self.profile {
            BudgetProfile::MaintenanceCli => "256",
            BudgetProfile::Server => "2048",
        }
    }

    /// Files-per-bin cap for every optimize rewrite (see const doc).
    /// PINNED CONSTANT — bounds per-merge memory regardless of box size.
    pub fn optimize_max_files_per_bin(&self) -> NonZeroUsize {
        OPTIMIZE_MAX_FILES_PER_BIN
    }

    /// PINNED CONSTANT (not box-derived): empirical sort peak, tighten after
    /// the sorted-run transition completes.
    pub fn per_sort_budget_bytes(&self) -> usize {
        PER_SORT_BUDGET_BYTES
    }

    /// Concurrent hot-tail light-optimize sorts. Formerly
    /// `TIMEFUSION_LIGHT_OPTIMIZE_CONCURRENCY`: memory-bound by the light
    /// share, CPU-bound to a quarter of cores, and never more than there are
    /// hot projects to compact. Degrades to 1 on small boxes instead of
    /// starving/OOMing (2026-07-23 incident was 2 sorts in a 6 GiB slice).
    pub fn light_optimize_k(&self, hot_project_count: usize) -> usize {
        // Priced against the pool these units ACTUALLY allocate from. Every live
        // caller of the permit is a coordinator unit, and `stage_hot_bin` on
        // that path takes `coordinator_runtime_env()`; the light share feeds
        // `light_optimize_session_state`, whose only callers sit under
        // `optimize_table_light` — which has none.
        //
        // Deriving it from the light share was a latent coupling that fired the
        // moment that share moved: raising the coordinator's cap shrank light
        // from ~7.6 GB to 3 GB, which took this from 3 to 1, and HotPacking —
        // which must take the permit BEFORE it claims — stopped being claimed
        // at all (prod 2026-09-01: zero HotPacking units in 45 minutes with 17
        // pending, and `compaction_permits_unavailable` 23 on a 35-minute-old
        // process against 9 over 5.8h before).
        // MINUS the repair lane's holdback — now TWO budgets, because
        // `repair_rewrite_budget_bytes` holds two target-sized files rather than
        // a fraction of one. The total stays at the bench's measured optimum of
        // six concurrent sorts; only the split moves (5 light + 1 repair ->
        // 4 light + 2 repair). Repair draws on the
        // same coordinator pool but is NOT counted here — and since the liveness
        // clock let its units live for tens of minutes instead of dying at their
        // deadline, they now overlap the hygiene bins instead of being killed
        // before they could. Prod 2026-09-01: `Not enough memory to continue
        // external sort` on repair staging as soon as long-running units and
        // K=4 hygiene bins shared 8 GB sixteen ways.
        let mem_bound = (self.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES).saturating_sub(self.repair_pool_holdback_slices());
        let cpu_bound = self.cores / 4;
        mem_bound.min(cpu_bound).min(hot_project_count).max(1)
    }

    /// Concurrently admitted maintenance coordinator units.
    ///
    /// Was hard-coded at 1 (alongside `(cpu 1, reads 1, writes 1)` admission
    /// tokens), which serialized maintenance hard enough that the queue grew
    /// unbounded at ~99% idle decode budget — the root cause of dead rollups,
    /// not a separate problem: no dedup commits means `record_certification`
    /// never fires, so an uncertified partition keeps `DedupExec` in every
    /// plan and denies rollup routing its certified prefix.
    ///
    /// The cap existed because maintenance shared Tokio workers with pgwire
    /// and starved health checks; the dedicated maintenance runtime fixed that
    /// properly, so this now bounds by the box instead — each unit reserves at
    /// most `MAX_DECODED_BYTES` (512 MiB), making the memory term exact.
    /// `TIMEFUSION_COORDINATOR_JOB_WORKERS=1` restores the old serialized
    /// behavior.
    pub fn coordinator_jobs(&self) -> usize {
        std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").ok().and_then(|v| v.parse::<usize>().ok()).filter(|n| *n > 0).unwrap_or_else(|| {
            let mem_bound = self.maintenance_pool_bytes / (512 * 1024 * 1024);
            // cores/3 (cap 16): jobs are only useful up to the inner
            // rewrite/sort permit pool (HEAVY_REWRITE_PERMITS) — going wider
            // than that just converts coordinator slots into queueing
            // (measured: at a 6:1 job:permit ratio, completions collapsed
            // from ~0.6/s to 0.035/s). 16 jobs against 10 permits leaves
            // headroom for non-rewrite operations without rebuilding that
            // queue. Memory term stays exact: 16 x MAX_DECODED_BYTES = 8 GiB
            // against a ~16.6 GiB maintenance pool, asserted below.
            let cpu_bound = self.cores / 3;
            mem_bound.min(cpu_bound).clamp(1, 16)
        })
    }

    /// Wall-clock budget for one maintenance tick: 80% of the cron period.
    /// Formerly `TIMEFUSION_LIGHT_OPTIMIZE_TICK_BUDGET_SECS`.
    /// K unbounded by project count (memory x CPU terms only) — sizes the
    /// light pool slice, which can't depend on the tick's plan.
    pub fn max_light_optimize_k(&self) -> usize {
        self.light_optimize_k(usize::MAX)
    }

    pub fn tick_budget(&self, cron_period: Duration) -> Duration {
        cron_period.mul_f64(0.8)
    }

    /// Wave-boundary memory brake, as a fraction of the BUDGETED limit. One-way
    /// safety valve only (see doc §5) — never used to size K.
    ///
    /// 80%, up from 70%. The number that matters is not this fraction but
    /// where it lands against the cgroup the OOM killer watches:
    /// `memory_limit_bytes` is itself capped below the cgroup, so 70% left
    /// real headroom the brake would never let maintenance use; 80% reclaims
    /// most of that while still leaving margin.
    ///
    /// Not a return to the 85% that previously failed: that regression was
    /// allocation bursts between wave boundaries outrunning jemalloc purge in
    /// the 85%→100% window during active backlog drain. Raised again once
    /// compaction converged and the backlog was draining — the brake had
    /// started firing on transient dedup peaks well below true steady-state
    /// RSS.
    pub fn memory_brake_limit_bytes(&self) -> usize {
        (self.memory_limit_bytes as f64 * 0.80) as usize
    }

    /// WAL emergency-flush byte threshold, as a fraction of the ingest
    /// buffer rather than a free-standing constant (today's 6 GB threshold
    /// vs a 24 GB buffer had drifted 9× out of proportion — doc §4).
    pub fn wal_flush_byte_threshold(&self) -> u64 {
        // Floor at 4 GiB: the WAL counts PREALLOCATED file bytes (walrus
        // blocks are up to 1 GiB each), so a threshold below a few blocks
        // trips on preallocation alone — on a small box a derived half-buffer
        // value fired early and drained open buckets before hard-limit
        // backpressure could engage (caught by the e2e backpressure test).
        // Small boxes are guarded by the file count + memory pressure; the
        // byte ceiling is a prod-scale replay bound, not a small-box valve.
        ((self.ingest_buffer_bytes / 2) as u64).max(4 * GIB as u64)
    }

    /// WAL emergency-flush file-count threshold: the legacy 200 as a FLOOR,
    /// scaled up on boxes with a bigger ingest buffer than the 24 GiB baseline.
    /// Never derived downward: 200 bounds restart REPLAY, not memory. A lower
    /// floor once tripped early on small boxes, draining the open bucket
    /// before hard-limit backpressure could engage — exactly the preemption
    /// the e2e backpressure test exists to catch.
    pub fn wal_flush_file_threshold(&self) -> usize {
        const BASELINE_BUFFER_BYTES: usize = 24 * GIB;
        const BASELINE_FILES: f64 = 200.0;
        (BASELINE_FILES * (self.ingest_buffer_bytes as f64 / BASELINE_BUFFER_BYTES as f64)).round().max(BASELINE_FILES) as usize
    }
}

/// Startup log of the whole derived tree so a misread cgroup limit is
/// immediately visible (doc §4 hard requirement). `hot_project_count` uses
/// prod's current 11 as the illustrative K.
pub fn log_derived_budget(b: &DerivedBudget) {
    tracing::info!(
        profile = ?b.profile,
        detected_limit_gb = detect_memory_limit_clamped() / GIB,
        effective_limit_gb = b.memory_limit_bytes / GIB,
        cores = b.cores,
        query_pool_gb = b.query_pool_bytes() / GIB,
        ingest_buffer_gb = b.buffer_max_bytes() / GIB,
        cache_memory_gb = b.foyer_memory_bytes() / GIB,
        foyer_memory_gb = b.object_cache_memory_bytes() / GIB,
        logical_count_memory_gb = b.logical_count_memory_bytes() / GIB,
        writer_reserve_gb = b.writer_reserve_bytes() / GIB,
        maintenance_pool_gb = b.maintenance_pool_bytes() / GIB,
        coordinator_share_gb = b.coordinator_share_bytes() / GIB,
        heavy_share_gb = b.heavy_share_bytes() / GIB,
        light_share_gb = b.light_share_bytes() / GIB,
        rewrite_permits = b.rewrite_permits(),
        optimize_merge_tasks = b.optimize_merge_tasks(),
        light_optimize_k_at_11_hot_projects = b.light_optimize_k(11),
        memory_brake_limit_gb = b.memory_brake_limit_bytes() / GIB,
        wal_flush_byte_threshold_gb = b.wal_flush_byte_threshold() / GIB as u64,
        wal_flush_file_threshold = b.wal_flush_file_threshold(),
        "self-sizing budget tree derived at startup"
    );
}

/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Load each sub-config separately to avoid #[serde(flatten)] issues with envy
    // See: https://github.com/softprops/envy/issues/26
    Ok(AppConfig {
        aws: envy::from_env()?,
        core: envy::from_env()?,
        buffer: envy::from_env()?,
        cache: envy::from_env()?,
        parquet: envy::from_env()?,
        maintenance: envy::from_env()?,
        memory: envy::from_env()?,
        telemetry: envy::from_env()?,
        tantivy: envy::from_env()?,
        derived: DerivedBudget::compute(),
    })
}

/// Initialize global config from environment (for production use).
pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    // `&mut` is autotune's API (cross-module), so the mutation stays here.
    let mut cfg = load_config_from_env()?;
    crate::config::apply(&mut cfg);
    let _ = CONFIG.set(cfg);
    Ok(config())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}

/// Global config if initialized, else `None`. For construction paths that may
/// run before `init_config()` (e.g. test-only server factories) and want to
/// fall back to defaults rather than panic.
pub fn try_config() -> Option<&'static AppConfig> {
    CONFIG.get()
}

/// Test-only: seed the global config so construction paths that read
/// `try_config()` (e.g. `PlanCacheHook::default`) see it. No-op if already set.
#[doc(hidden)]
pub fn set_config_for_test(cfg: AppConfig) {
    let _ = CONFIG.set(cfg);
}

/// Whether the operator has opted into open auth for local dev via
/// `TIMEFUSION_ALLOW_INSECURE_AUTH=true`.
/// paths gate their fail-secure defaults on this flag.
pub fn is_insecure_auth_allowed() -> bool {
    std::env::var("TIMEFUSION_ALLOW_INSECURE_AUTH").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

/// Bound on the post-commit cache confirm. It is an optimization, never a
/// durability gate, so a slow warm must not stall the flush loop.
pub const CACHE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrency of the confirm's full-file fetches. Deliberately NOT the 16-way
/// `timefusion_warm_concurrency` (detached, off the flush path): each miss
/// buffers a whole flush-sized parquet body in transient heap no memory pool
/// tracks, ON the flush path — the untracked-consumer shape behind this box's
/// prior OOMs. Peak ≈ this × largest added file.
pub const CACHE_CONFIRM_CONCURRENCY: usize = 4;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub aws: AwsConfig,
    #[serde(flatten)]
    pub core: CoreConfig,
    #[serde(flatten)]
    pub buffer: BufferConfig,
    #[serde(flatten)]
    pub cache: CacheConfig,
    #[serde(flatten)]
    pub parquet: ParquetConfig,
    #[serde(flatten)]
    pub maintenance: MaintenanceConfig,
    #[serde(flatten)]
    pub memory: MemoryConfig,
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
    #[serde(flatten)]
    pub tantivy: TantivyConfig,
    /// Self-sizing budget tree, derived (not deserialized) at construction
    /// from the cgroup limit, optionally lowered by
    /// `TIMEFUSION_MEMORY_BUDGET_GB`. NOT `timefusion_memory_fraction` — dead
    /// since the tree landed.
    #[serde(skip)]
    pub derived: DerivedBudget,
}

/// Tantivy sidecar-index config. Indexing is always-on for any table whose
/// YAML schema declares `tantivy.indexed: true` on at least one field —
/// schema is the single source of truth, no override knob.
#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde_inline_default(64)]
    pub timefusion_tantivy_max_index_size_mb: u64,
    /// Byte budget for the local extracted-index cache
    /// (`<timefusion_data_dir>/tantivy_cache`), enforced LRU-first by the
    /// "Tantivy cache reap" cron — the only thing that deletes from that
    /// tree. Shares a volume with the WAL, so a full volume also fails WAL
    /// appends.
    // Sized against a working set, not a wish: the reaper only evicts what no
    // query has opened recently, and every eviction re-downloads a blob on the
    // next hit. 4 GB (the value this knob carried as dead code) would thrash the
    // hot window at prod scale. Measured working set: ~65 GB across ~6500 leaf
    // index dirs.
    //
    // 200, up from 64. 64 sat *at* the measured working set, which was survivable
    // only while nothing actively repopulated the cache. It no longer is: indexes
    // are now seeded on publish and a cron re-warms `prefetch_days` for every
    // project, so a budget equal to the working set makes the 10-minute reaper
    // evict precisely what the 15-minute warmer re-downloads — a permanent S3
    // churn loop, and one that would show up as a `blob_fetches` counter that
    // never falls, i.e. the metric this work is judged by.
    //
    // Headroom, not a wish: the prod volume has ~1.1 TB free, and the only other
    // large tenant on it is foyer at an env-pinned 600 GB.
    #[serde_inline_default(200)]
    pub timefusion_tantivy_cache_disk_gb: u64,
    /// How often to enforce `timefusion_tantivy_cache_disk_gb`. Each sweep
    /// walks the whole cache tree; empty disables the reap (and the bound).
    #[serde_inline_default("0 */10 * * * *".to_string())]
    pub timefusion_tantivy_cache_reap_schedule: String,
    // Level 3: index packing is on the flush hot path; level 19 cost ~88% of a
    // CPU window per flush for only 10-15% smaller output.
    #[serde_inline_default(3)]
    pub timefusion_tantivy_compression_level: i32,
    #[serde_inline_default(2)]
    pub timefusion_tantivy_min_files_for_pushdown: usize,
    /// If a tantivy prefilter would produce more than this many hits, skip
    /// the `id IN (...)` pushdown entirely — the IN-list itself becomes the
    /// bottleneck above this point. Default 2k: measured 2026-08-22, a
    /// 3,346-literal IN cost ~2.4s of planning and a 59k one ~28s, so the
    /// old 100k cap admitted pushdowns that were strictly slower than the
    /// scan they replaced.
    #[serde_inline_default(2_000)]
    pub timefusion_tantivy_prefilter_max_hits: usize,
    /// If a tantivy prefilter selects more than this percentage of the
    /// indexed rows, the pushdown isn't worth the round-trip; skip it and
    /// let Delta scan with the original predicate. Default 50 (%).
    #[serde_inline_default(50)]
    pub timefusion_tantivy_prefilter_min_selectivity_pct: u32,
    /// Route exact `col = 'lit'` on raw-tokenized high-cardinality columns
    /// (trace_id/span_id/id/parent_id) through the tantivy id-prefilter, not
    /// just LIKE (and IN-lists as OR-of-terms). Correctness-safe under OR:
    /// `collect_text_match_tree` only routes a disjunction when every branch
    /// is fully covered by a text_match, and the original predicate always
    /// stays as the post-filter backstop. Targets the trace/span lookup gap
    /// vs the indexed PG path. Default ON; set false to revert to
    /// bloom/stats-only equality pruning.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_route_equality: bool,
    /// Startup backfill: build partition-mirrored indexes for live parquet
    /// files no manifest entry covers (pre-tantivy history, failed builds,
    /// files landed while the feature was off). Oldest-first, bounded
    /// concurrency. Off by default — reads every uncovered file back from S3.
    #[serde(default)]
    pub timefusion_tantivy_backfill: bool,
    /// Concurrent index builds during backfill/reconcile/post-optimize
    /// reindex. 2 is safe alongside prod query load; the off-box repair CLI
    /// raises it (each 1 GB parquet takes ~2-3 min to index).
    #[serde_inline_default(2)]
    pub timefusion_tantivy_build_concurrency: usize,
    /// Backfill/reconcile skips parquet files larger than this (MB); 0 = no
    /// limit. Memory-tight runners OOM decoding+indexing very large files;
    /// capping lets everything else repair while logging the skips.
    ///
    /// 4096, up from 512: 512 was sized for the whole-file `build_and_pack`
    /// path, but backfill/reconcile actually call the streaming
    /// `build_parquet_and_pack`, whose peak is fixed by construction and
    /// doesn't scale with parquet size — so the cap was excluding files for a
    /// cost this path no longer pays, stalling the reindex. Not removed
    /// outright: `pack_dir` still builds the compressed-index tar in memory,
    /// so 4096 keeps a bound on that one term while clearing the largest
    /// files on record with margin.
    #[serde_inline_default(4096)]
    pub timefusion_tantivy_backfill_max_file_mb: u64,
    /// File-level scan pruning: when the prefilter engages, files whose
    /// covering index returned zero hits are excluded from the Delta scan
    /// entirely. Off switch for instant rollback to id-IN-list-only pruning.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_file_pruning: bool,
    /// Warm the local index cache with blobs whose data is at most this many
    /// days old, at startup AND on `timefusion_tantivy_prefetch_schedule`
    /// (0 = off). Turns the cold-window download cliff into a background cost
    /// after restarts, and keeps the hot window resident thereafter.
    ///
    /// Default 3, up from 0 (the warmer had never run in prod). Sized from the
    /// whale's real blob distribution: recent days carry nearly all the blobs
    /// AND nearly all the bytes (2026-08-21 alone: 483 blobs / 1.4 GB
    /// compressed; days older than a week are single-digit MB). At the
    /// measured 5.66x extraction ratio, 3 days is ~20 GB extracted for the
    /// largest project — affordable against `cache_disk_gb`, where a week
    /// would not be.
    #[serde_inline_default(3)]
    pub timefusion_tantivy_prefetch_days: u32,
    /// Re-warm cadence for `timefusion_tantivy_prefetch_days`. Startup warming
    /// alone decays: the reaper evicts, new indexes land, and a wide historical
    /// query can pull enough cold blobs to push the hot window out. Re-warming
    /// also re-stamps `last_used` on hot dirs, which is what keeps them at the
    /// young end of the reaper's LRU order. Empty disables the periodic pass
    /// (startup warming still runs).
    #[serde_inline_default("0 */15 * * * *".to_string())]
    pub timefusion_tantivy_prefetch_schedule: String,
    /// Seed the local extracted-index cache at publish time, so a freshly
    /// built index is never re-downloaded from S3 to answer the first query
    /// that needs it. The upload still happens either way — S3 remains the
    /// source of truth and this only avoids the round trip back.
    /// Off switch for instant rollback to download-on-first-read.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_seed_cache_on_publish: bool,
    /// Open-index (mmap + reader) LRU capacity. Was a hardcoded 256, which is
    /// smaller than a single query's working set at any window wider than ~6h
    /// (measured: 254 indexes at 6h, 457 at 12h, 913 at 7d), so wide queries
    /// evicted exactly what the next one needed.
    ///
    /// 2048 covers an entire project's manifest (950 entries for the largest)
    /// with headroom for several more. Two things make that affordable, and
    /// both were measured rather than assumed:
    /// - fds: `server::raise_file_limit()` lifts RLIMIT_NOFILE soft to hard at
    ///   startup, and the prod process really is running at **524288**. Read it
    ///   from the CONTAINER's pid: a host-side `pgrep -f timefusion` matches the
    ///   ssh shell instead and reports its 1024, which is how an earlier pass of
    ///   this work talked itself into leaving the cache at 256. Even 2048
    ///   indexes x 28 files is ~57k fds, roughly a ninth of the limit.
    /// - memory: an open index is mmap'd, so its pages are FILE-backed, and
    ///   every recorded OOM on this box is anon-driven (~100 GB anon vs ~170 MB
    ///   file-rss). This does not push the term that actually kills us.
    #[serde_inline_default(2048)]
    pub timefusion_tantivy_reader_cache_entries: usize,
    /// Concurrent per-index download+open+search tasks within one query. Was a
    /// hardcoded 8, which serialized a 7d query's 913 indexes into ~114 rounds.
    /// These tasks are IO-bound (object-store GET or page-cache read), so the
    /// useful ceiling is far above 8.
    #[serde_inline_default(32)]
    pub timefusion_tantivy_search_concurrency: usize,
    /// TTL for the parsed-manifest cache. Was a hardcoded 5s while a routed
    /// query takes 1.7-3.2s, so back-to-back queries routinely straddled it and
    /// re-GET + re-parsed a 745 KB, 950-entry manifest on the planning path.
    /// Safe to lengthen: publishing an index invalidates this process's cached
    /// entry, so our own indexer is never unseen; `gc_after_compaction` drops
    /// it, so our own pruning is never unseen either. It does still bound
    /// staleness against writers we don't observe — other processes, e.g. the
    /// repair CLI. A stale entry only ever costs a wasted lookup against a
    /// deleted blob, which the prefilter treats as "no usable index".
    #[serde_inline_default(300)]
    pub timefusion_tantivy_manifest_ttl_secs: u64,
    /// Files a single backfill pass will attempt.
    ///
    /// Sized against the MEASURED build rate, which is the thing that actually
    /// bounds throughput: prod 2026-08-23, a container up 7 hours had
    /// `tantivy_backfill_built = 30` — **~4 builds/hr**. At the old cap of 150
    /// that is a ~35-HOUR pass, and the consequences were all visible in the
    /// same 7 hours: **1** `tantivy_backfill_started`, **0**
    /// `tantivy_backfill_pass` completions, and **19** ticks dropped as "run
    /// still in progress". A pass that never ends never refreshes its work
    /// list, never reports its end-line, and blocks every later tick.
    ///
    /// A COUNT ceiling only — the real bound is
    /// `timefusion_tantivy_backfill_max_bytes_per_pass`. This exists to stop a
    /// pathological queue of tiny files, not to size the pass.
    ///
    /// It was 8, on the reasoning that "the cap does NOT throttle throughput —
    /// build rate does". That was measured on `otel_logs_and_spans`, where a
    /// build costs 4-5 MINUTES, and it is false for every other indexed table:
    /// on 2026-08-23 a rollup pass logged `built=8` in **14 seconds**, ~1.75s
    /// per build, ~150x cheaper. One count cap across populations that differ
    /// by two orders of magnitude in cost throttles the cheap tables to protect
    /// against the expensive one — which is exactly the rollup tables whose
    /// coverage had been frozen since 08-20.
    /// Sized against WALL CLOCK, because for small files the cost is per-file
    /// overhead rather than bytes: prod measured ~1.75s/build on rollup files,
    /// and a pass at 128 stopped on this ceiling having spent 29 MB of a
    /// 2048 MB budget — 98.6% unused, so the count was binding again exactly as
    /// it was at 8. 320 x 1.75s is ~9 minutes, which fits inside the ~15-minute
    /// gap between prod restarts; the byte budget still bounds the pass when the
    /// queue holds spans whales instead.
    #[serde_inline_default(320)]
    pub timefusion_tantivy_backfill_max_files_per_pass: usize,
    /// The real per-pass bound: total INPUT bytes a backfill pass will read.
    ///
    /// Cost tracks bytes, not files, so this bounds a pass to a predictable
    /// wall-clock regardless of whether the queue holds 4 GB spans whales or
    /// 2 MB rollup files. At least one file is always attempted, so an
    /// over-budget file still makes progress instead of wedging the queue.
    #[serde_inline_default(2048)]
    pub timefusion_tantivy_backfill_max_bytes_per_pass_mb: u64,
    /// Percentage of each backfill pass reserved for the OLDEST uncovered files.
    ///
    /// Without it the pass is pure newest-first and the tail starves. Measured
    /// on prod 2026-08-22 across three consecutive census samples: `week` and
    /// `older` sat at 1723 and 3459 — frozen to within one file — while `today`
    /// climbed 586 → 624 → 643. Today's partition alone held 624 uncovered files
    /// against a 150-file cap, so no pass could ever reach yesterday, and flush
    /// plus hot-tail compaction refill today faster than it drains. Throughput
    /// cannot fix that ordering; only a reservation can.
    ///
    /// Carved OUT of the cap, never added to it, so pass cost is unchanged.
    /// 0 restores pure newest-first.
    #[serde_inline_default(33)]
    pub timefusion_tantivy_backfill_tail_share_pct: u8,
    /// Skip TODAY's date partition in the backfill queue.
    ///
    /// Indexing today's files per-file is work with a half-life of hours: the
    /// hot partition is rewritten continuously by flush and hot-tail
    /// compaction, so an index built for one of its files is usually GC'd
    /// before it is ever consulted. Measured 2026-08-22, `today` was the ONLY
    /// growing class (586 -> 984 across the day) while `week`/`older` sat
    /// frozen at ~1720/3453 — so a newest-first queue spends the whole pass on
    /// files that will not survive, and the 5,171-file backlog never moves.
    ///
    /// The flush callback still covers today's files at birth, so this is not
    /// first coverage. **But be honest about the gap:** today's partition is
    /// owned by `light_optimize_tail`, which has NO tantivy hook — only the full
    /// `optimize_table` path reindexes (or now carries forward) its output. So a
    /// hot-tail merge drops its inputs' coverage and the output stays uncovered
    /// until the date rolls over and the backfill picks it up. That costs the
    /// hottest query window its prefilter for up to a day: correctness is
    /// unaffected (an uncovered file goes to the raw leg with the original
    /// filters), latency is not.
    ///
    /// It is still the right trade while a build costs ~15 minutes and runs at
    /// ~4/hr: spending the entire pass on files that will be rewritten within
    /// hours is what kept the 5,171-file sealed backlog frozen all day. The
    /// principled fix is to give `light_optimize_tail` the same carry-forward
    /// hook `optimize_table` now has, which makes today cheap to cover instead
    /// of skipped — do that and this flag can go back to false.
    ///
    /// Counted and logged as `skipped_today`, never silent, and the coverage
    /// census still counts today so the today/week/older breakdown stays
    /// legible. Set false to restore whole-corpus backfill.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_backfill_skip_today: bool,
    /// Row-selection pushdown: when the prefilter engages, files whose index
    /// was built in parquet row order get a per-file ParquetAccessPlan so the
    /// reader decodes only matching rows. Off switch for instant rollback to
    /// id-IN-list-only filtering inside surviving files.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_row_selection: bool,
}

impl TantivyConfig {
    /// Tables to index: schemas with `tantivy.indexed: true` on any field.
    /// Computed once from the compiled-in registry. `BTreeSet` so
    /// `indexed_tables` is sorted by construction.
    fn indexed_set() -> &'static std::collections::BTreeSet<String> {
        static SET: OnceLock<std::collections::BTreeSet<String>> = OnceLock::new();
        SET.get_or_init(|| {
            let reg = crate::schema::registry();
            reg.list_tables()
                .into_iter()
                .filter(|name| reg.get(name).is_some_and(|s| s.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed))))
                .collect()
        })
    }
    pub fn indexed_tables(&self) -> Vec<String> {
        Self::indexed_set().iter().cloned().collect()
    }
    pub fn is_table_indexed(&self, table: &str) -> bool {
        Self::indexed_set().contains(table)
    }
    pub fn compression_level(&self) -> i32 {
        self.timefusion_tantivy_compression_level
    }
    pub fn prefilter_max_hits(&self) -> usize {
        self.timefusion_tantivy_prefilter_max_hits.max(1)
    }
    pub fn prefilter_min_selectivity_pct(&self) -> u32 {
        self.timefusion_tantivy_prefilter_min_selectivity_pct.min(100)
    }
    pub fn route_equality(&self) -> bool {
        self.timefusion_tantivy_route_equality
    }
    /// Disk budget in bytes. Floored at 1 GB — zero would reap the cache to
    /// nothing every 10 minutes, turning every query into a re-download.
    pub fn cache_disk_bytes(&self) -> u64 {
        self.timefusion_tantivy_cache_disk_gb.max(1) * 1024 * 1024 * 1024
    }
    /// Floored at 1: a zero-capacity LRU would make every open a cold open.
    pub fn reader_cache_entries(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.timefusion_tantivy_reader_cache_entries.max(1)).expect("max(1) is non-zero")
    }
    /// Floored at 1: zero concurrency would deadlock the per-index fan-out.
    pub fn search_concurrency(&self) -> usize {
        self.timefusion_tantivy_search_concurrency.max(1)
    }
    pub fn manifest_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.timefusion_tantivy_manifest_ttl_secs)
    }
    pub fn seed_cache_on_publish(&self) -> bool {
        self.timefusion_tantivy_seed_cache_on_publish
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region: Option<String>,
    #[serde_inline_default("https://s3.amazonaws.com".to_string())]
    pub aws_s3_endpoint: String,
    #[serde(default)]
    pub aws_s3_bucket: Option<String>,
    #[serde(default)]
    pub aws_allow_http: Option<String>,
    /// TCP/TLS connection-establishment bound for the object_store S3 client
    /// (humantime, e.g. "15s"). `Option` so the derived `AwsConfig::default()`
    /// stays valid; see `connect_timeout`/`request_timeout` for effective defaults.
    #[serde(default)]
    pub timefusion_s3_connect_timeout: Option<String>,
    /// Total per-request bound (humantime, e.g. "900s"). Must comfortably
    /// exceed the time to PUT one large multipart part under load — too
    /// short and concurrent big PUTs starve connections. Tunable via
    /// TIMEFUSION_S3_REQUEST_TIMEOUT.
    #[serde(default)]
    pub timefusion_s3_request_timeout: Option<String>,
    /// Per-request bound for the COMMIT-LOG request class (`_delta_log/*.json`,
    /// `_last_checkpoint`, log LISTs) — humantime, default "30s". Split from
    /// `timefusion_s3_request_timeout` because the classes are unrelated: a
    /// data request is a multi-MB part that can legitimately take minutes,
    /// while a log request is a few-KB op that's sub-second when healthy.
    /// Sharing the data bound let one hung commit PUT hold a table's commit
    /// lock for minutes and stall every committer on it.
    ///
    /// Safe to bound tightly: delta-rs's conditional commit PUT isn't marked
    /// idempotent in object_store, so a timeout is never silently re-sent —
    /// it surfaces as an ordinary commit error and the landed-probe
    /// (`probe_commit_landed`) decides whether the commit landed. Tunable via
    /// TIMEFUSION_S3_LOG_REQUEST_TIMEOUT.
    #[serde(default)]
    pub timefusion_s3_log_request_timeout: Option<String>,
}

/// Warm-connection pool size per host, shared by both object-store client
/// construction paths. 128 gives headroom above the query scan fanout so
/// concurrent GETs reuse sockets instead of re-doing TLS.
pub(crate) const S3_POOL_MAX_IDLE_PER_HOST: usize = 128;

/// Coerces a bare number (e.g. "150") to humantime seconds ("150s").
/// object_store's `ClientConfigKey::{ConnectTimeout,Timeout}` parse strictly
/// via humantime and PANIC at boot on a unitless value. Treat an all-digit
/// string as seconds; pass anything with a unit through untouched.
fn normalize_duration(configured: Option<&str>, default: &str) -> String {
    let s = configured.unwrap_or(default);
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) { format!("{s}s") } else { s.to_owned() }
}

impl AwsConfig {
    /// Effective connect timeout. Healthy connections establish in <1s; a
    /// generous bound only matters when something is wrong, trading slower
    /// failure for surviving transient connection refusals.
    pub fn connect_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_connect_timeout.as_deref(), "60s")
    }

    pub fn request_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_request_timeout.as_deref(), "900s")
    }

    /// Effective per-request bound for the commit-log request class — see
    /// `timefusion_s3_log_request_timeout`. Not clamped against
    /// `request_timeout`: an operator raising it above the data bound is a
    /// deliberate act, and the two classes are independent by design.
    pub fn log_request_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_log_request_timeout.as_deref(), "30s")
    }

    pub fn build_storage_options(&self, endpoint_override: Option<&str>) -> HashMap<String, String> {
        [
            ("AWS_ACCESS_KEY_ID", self.aws_access_key_id.clone()),
            ("AWS_SECRET_ACCESS_KEY", self.aws_secret_access_key.clone()),
            ("AWS_REGION", self.aws_default_region.clone()),
            ("AWS_ALLOW_HTTP", self.aws_allow_http.clone()),
            ("AWS_ENDPOINT_URL", Some(endpoint_override.unwrap_or(&self.aws_s3_endpoint).to_string())),
            // Bound connection establishment + total request time. Kept in
            // sync with create_object_store so both paths agree.
            ("connect_timeout", Some(self.connect_timeout())),
            ("timeout", Some(self.request_timeout())),
            // Keep TLS connections warm for the read path: a scan fans out
            // ~target_partitions concurrent GETs; a low idle cap can force
            // mid-fanout TLS re-establishment. Matches create_object_store's client.
            ("pool_max_idle_per_host", Some(S3_POOL_MAX_IDLE_PER_HOST.to_string())),
        ]
        .into_iter()
        .filter_map(|(k, v)| Some((k.to_string(), v?)))
        .collect()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OtelScanGuard {
    #[default]
    Off,
    Observe,
    Enforce,
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde_inline_default(PathBuf::from("./data"))]
    pub timefusion_data_dir: PathBuf,
    #[serde_inline_default(5432)]
    pub pgwire_port: u16,
    #[serde_inline_default("timefusion".to_string())]
    pub timefusion_table_prefix: String,
    #[serde(default)]
    pub timefusion_config_database_url: Option<String>,
    #[serde_inline_default(true)]
    pub enable_batch_queue: bool,
    #[serde_inline_default(100_000_000)]
    pub timefusion_batch_queue_capacity: usize,
    #[serde_inline_default("postgres".to_string())]
    pub pgwire_user: String,
    #[serde(default)]
    pub pgwire_password: Option<String>,
    /// Interactive statement cap. 60 -> 90 on 2026-09-04: a 30-day dashboard
    /// aggregate on the whale project MEASURES 44-50 s and was being cancelled at
    /// 60 s, so the window failed outright while being only marginally over.
    /// Measured single-query 30 d runs: 30.2 / 37.8 / 43.0 / 44.4 / 48.4 / 50.7 s
    /// — the distribution straddles 60 s, which is why the same query sometimes
    /// returned and usually did not.
    ///
    /// This is a CAP RAISE, not a speed-up: 30 d is genuinely ~45 s because the
    /// scan is IO-bound on object-store reads shared with a saturated maintenance
    /// tier (container CPU is identical with and without the query; ~17 of 48
    /// cores and ~150-200 MB/s are consumed by maintenance alone). The real
    /// speed-ups are reading less (dedup skip via certification, or rollups) and
    /// more concurrent reads — two 15 d queries in parallel do the same work in
    /// 11.8-21.3 s because in-query concurrency is bounded by the scan's ~22 file
    /// groups while two sessions get ~44 in flight.
    #[serde_inline_default(90)]
    pub timefusion_pgwire_max_statement_secs: u64,
    /// How far a session may RAISE its statement timeout by asking for one, in
    /// seconds. 0 (the default) means it cannot: the cap stays
    /// `timefusion_pgwire_max_statement_secs` and behaviour is unchanged.
    ///
    /// Exists for batch work that legitimately cannot finish inside the
    /// interactive cap -- monoscope's usage metering splits a billing cycle
    /// into 30 day-sized aggregates and pays 60 sequential round trips for
    /// exactly this reason. Raising is opt-in per session
    /// (`SET statement_timeout = '300s'`) and never implicit, so a dashboard
    /// connection that says nothing is still capped at the interactive value.
    #[serde_inline_default(0)]
    pub timefusion_pgwire_batch_statement_secs: u64,
    #[serde(default)]
    pub timefusion_otel_scan_guard: OtelScanGuard,
}

impl CoreConfig {
    pub fn wal_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("wal")
    }
    pub fn cache_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("cache")
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    // 60s (was 300s): a shorter flush interval bounds how much un-flushed WAL a
    // restart must replay — startup/redeploy downtime is dominated by WAL replay,
    // which scales ~linearly with this interval. Trade-off: ~5x more Delta
    // commits / small files (handled by compaction/OPTIMIZE).
    #[serde_inline_default(60)]
    pub timefusion_flush_interval_secs: u64,
    // Flush dwell: a sealed-but-young bucket waits this long from CREATION before
    // the periodic flush commits it, unless it is already big. -1 = one
    // bucket_duration (the prod default), 0 = off (the test harnesses set this —
    // they assert "sealed => next tick flushes"). See flush_completed_buckets.
    #[serde_inline_default(-1)]
    pub timefusion_flush_dwell_secs: i64,
    #[serde_inline_default(70)]
    pub timefusion_buffer_retention_mins: u64,
    #[serde_inline_default(60)]
    pub timefusion_eviction_interval_secs: u64,
    #[serde_inline_default(4096)]
    pub timefusion_buffer_max_memory_mb: usize,
    // Total graceful-shutdown budget shared by ALL serial shutdown phases
    // (PGWire drain → buffered-layer flush + cursor snapshot).
    // Set to ~80% of the orchestrator's SIGTERM→SIGKILL grace (Docker/CapRover
    // `StopGracePeriod`; prod is 90s) so the clean cursor snapshot always lands
    // before SIGKILL — the previous per-phase 180s ceilings assumed grace nobody
    // configured, and PGWire drain alone could eat the real grace before the
    // flush or snapshot ever started. Anything unflushed at the deadline is
    // durable in the WAL and replays on next boot.
    #[serde_inline_default(70)]
    pub timefusion_stop_grace_secs: u64,
    #[serde_inline_default(10)]
    pub timefusion_wal_corruption_threshold: usize,
    // Concurrent staged flush commits. Parquet encode + S3 upload happen outside
    // the per-table commit lock (see insert_records_batch staged path), so this scales
    // upload throughput directly — the dominant steady-state drain lever under
    // backfill. 8 doubles concurrency over the old 4 while bounding in-flight
    // encode memory; raise further (env) if CPU/R2 headroom allows.
    #[serde_inline_default(8)]
    pub timefusion_flush_parallelism: usize,
    /// Coalesce one tick's per-project flush commits into one commit per
    /// PHYSICAL Delta table (see `d_flush_coalesce_commits`).
    // Cross-project flush commit coalescing (C3). All default-storage projects share
    // ONE physical Delta table, and `table_lock_key` already serializes their commits
    // behind a single mutex — so N per-project commits per tick produce N log entries,
    // N snapshot refreshes and N log-JSON parses where 1 would do. When enabled, one
    // tick produces one commit per PHYSICAL table carrying every project's Add
    // actions; parquet writes still fan out `flush_parallelism`-wide, only the
    // commit is shared. Custom-storage projects have their own `_delta_log` and are
    // never coalesced with default storage.
    // Default OFF: the coalesced path changes the durability-critical commit +
    // watermark shape, so it ships as an operator-enabled lever (env
    // `TIMEFUSION_FLUSH_COALESCE_COMMITS=true`) that needs a soak before default-on.
    #[serde_inline_default(false)]
    pub timefusion_flush_coalesce_commits: bool,
    #[serde(default)]
    pub timefusion_flush_immediately: bool,
    /// EXPERIMENTAL (default OFF): when set, `insert()` admits over the
    /// memory hard limit instead of rejecting a write whose backpressure
    /// budget is exhausted — the WAL append is the durability boundary, so a
    /// slow/over-budget write beats a dropped one. Requires a soak (RSS /
    /// flush throughput) before prod enable — over-budget admission trades a
    /// reject for unbounded growth if flush can't keep up.
    #[serde(default)]
    pub timefusion_wal_admit_decouple: bool,
    #[serde_inline_default(200)]
    pub timefusion_wal_fsync_ms: u64,
    // Durability mode for the WAL. One of:
    //   "sync_each" — fsync after every entry (default; zero data-loss window, ~1ms per write)
    //   "ms"        — async fsync every `wal_fsync_ms` (~200ms loss window; a torn
    //                 mmap tail after OOM/SIGKILL quarantines acked entries)
    //   "none"      — never fsync (test/throwaway data only)
    #[serde_inline_default("sync_each".to_string())]
    pub timefusion_wal_fsync_mode: String,
    /// Fsync the WAL shard before acking DML appends (machine-crash
    /// durability). Batched INSERT appends are always flushed before ack;
    /// only single-entry DML appends defer to the background fsync thread —
    /// this closes that window. Default on: a torn mmap tail after
    /// OOM/SIGKILL can quarantine acked-but-unsynced entries.
    #[serde_inline_default(true)]
    pub timefusion_wal_ack_fsync: bool,
    // 0 = unset → derived (DerivedBudget::wal_flush_file_threshold); env-set wins.
    #[serde_inline_default(0)]
    pub timefusion_wal_max_file_count: usize,
    /// Force-flush backstop on total on-disk (unflushed) WAL bytes. Guards
    /// the case the memory-pressure valve misses: a stuck/retrying commit
    /// pins the WAL GC floor while buffer memory frees post-commit, bloating
    /// the WAL without tripping memory pressure and inflating restart
    /// replay. 0 = derive from the buffer budget.
    #[serde(default)]
    pub timefusion_wal_max_unflushed_mb: usize,
    /// Disk-runaway breaker: HARD cap on total on-disk WAL bytes, past which
    /// INSERTs are rejected (the upstream DLQ absorbs and replays them)
    /// instead of acking writes into unbounded disk growth — soft thresholds
    /// alone let a merge storm grow the WAL past 100GB. Total on-disk
    /// includes flushed segments the age-gated GC still holds plus all
    /// active per-shard files, so keep this well above busy-hour residue.
    /// Checked every ~15s by a dedicated WAL-gate task (`run_wal_gate_task`,
    /// deliberately not the flush loop, which stalls in exactly the overload
    /// this guards against). DML mem legs are exempt: failing an UPDATE
    /// mid-statement would desync mem vs Delta. 0 disables.
    #[serde_inline_default(192)]
    pub timefusion_wal_hard_limit_gb: u64,
    // MemBuffer bucket window (seconds). Smaller windows free RAM sooner because
    // the previous bucket becomes flushable sooner; larger windows amortize into
    // fewer/larger Delta commits. 300s, halved from 600s to cut peak MemBuffer
    // footprint (the current bucket is excluded from flushing, so this is the
    // floor on how long a row accumulates in RAM). Trade-off is ~2× Delta commits
    // / small files; high-throughput tenants can go lower (60–120s), memory-relaxed
    // deployments can raise it back.
    #[serde_inline_default(300)]
    pub timefusion_bucket_duration_secs: u64,
    // Memory pressure threshold (0–100) at which the flush task is woken
    // independently of the periodic flush timer. Triggers an early
    // `flush_completed_buckets` so MemBuffer drains before reservation reaches
    // the hard limit. 0 disables pressure-triggered flushes.
    #[serde_inline_default(75)]
    pub timefusion_pressure_flush_pct: u32,
    // Max seconds an insert applies backpressure (synchronously flushing
    // MemBuffer → Delta to free RAM) before failing, when the memory hard limit
    // is hit. The rows are already durable in the WAL, so this trades a slow
    // write for a rejected one — the right call for a TS DB whose producers DLQ
    // on rejection. 0 restores the old fail-fast behavior. 60s is long enough to
    // ride out a flush cycle / drain a replayed backlog, finite so a genuinely
    // down Delta can't pile blocked writers up without bound.
    #[serde_inline_default(60)]
    pub timefusion_write_backpressure_secs: u64,
    /// See `d_dml_coalesce_secs` — drain interval for deferred UPDATE ... FROM
    /// Delta merges; 0 keeps the synchronous per-statement path.
    // DML coalescing (0 = disabled). When > 0, the Delta leg of `UPDATE ... FROM`
    // statements is deferred and batched: sources accumulate per (project, table,
    // statement shape) and a background task merges them every N seconds, cutting
    // one-Delta-commit-per-statement churn (which starves OPTIMIZE via OCC and
    // piles up small files) down to a few commits per interval. The in-memory leg
    // still applies synchronously, so reads that overlay the buffer stay
    // read-your-writes. CONTRACT: statements must be idempotent under
    // re-application (e.g. guard appends with `NOT (col @> val)`), because a row
    // flushed between the mem leg and the drain sees the assignment applied
    // twice, and a failed drain retries whole groups. Timestamp-range conjuncts
    // are widened to the union across coalesced statements.
    // 3s ON by default: a drain hash-update storm (repeated same-shape UPDATEs,
    // each rewriting most of the hot partition) OOM-looped prod; coalescing
    // collapses a window's statements into one rewrite. 0 restores the
    // synchronous per-statement path.
    #[serde_inline_default(3)]
    pub timefusion_dml_coalesce_secs: u64,
    /// Fold same-shape coalesced groups across projects into one MERGE per
    /// unified table per drain (`project_id` becomes a join key + IN-list
    /// partition filter). Eliminates the per-project metadata-scan +
    /// OCC-commit multiplication that starved flush under a heavy merge
    /// storm. Kill switch: `TIMEFUSION_DML_COALESCE_FOLD=false`.
    #[serde_inline_default(true)]
    pub timefusion_dml_coalesce_fold: bool,
    // Watchdog for a single bucket's Delta commit inside `flush_bucket`. A hung S3
    // commit / commit-lock wait otherwise pins `flush_lock` forever with no log:
    // flushes freeing zero memory while inserts wedge at the hard limit. On
    // timeout the flush errors (counted in flush_failed + flush_stalled),
    // releasing the lock so relief retries; rows stay in MemBuffer + WAL, so it's
    // safe. Must exceed a normal backfill commit but stay well under retention.
    //
    // The CEILING, not the budget: `BufferedWriteLayer::adaptive_flush_timeout`
    // contracts it as the ingest buffer fills. Read that function before changing
    // this — prod has been wedged from both ends of the fixed-value trade (too low
    // aborted legitimate multi-GB drains into a retry loop; too high let a hung
    // commit hold the global flush_lock until every tenant's INSERT was rejected).
    // 600 stays right for the ceiling: with headroom, a slow-but-progressing
    // commit should be allowed to finish, since aborting it wastes the work and
    // the next attempt is no faster.
    #[serde_inline_default(600)]
    pub timefusion_flush_bucket_timeout_secs: u64,
    /// WAL shards per (project, table) topic. Higher = more append parallelism
    /// at the cost of O(shards) recovery memory and more file handles.
    #[serde_inline_default(4)]
    pub timefusion_wal_shards_per_topic: usize,
    /// Max concurrent S3/R2 reads when reconciling per-table Delta watermarks
    /// at boot. Only used when the cursor snapshot is missing or stale.
    // Cold-boot Delta cursor reconciliation. R2 happily takes 64+ concurrent
    // gets per bucket; the original 8 left ~8× headroom. Depth 8 is half the
    // original 16 (the snapshot replaces the bulk of the scan) but keeps a
    // safety margin: if a few snapshot writes failed silently before reboot,
    // depth-2 could miss the legitimate cursor advance. Tune via env if the
    // fallback Delta scan is the bottleneck.
    #[serde_inline_default(64)]
    pub timefusion_delta_scan_concurrency: usize,
    /// Per-table Delta commit history depth scanned at boot. The cursor
    /// snapshot covers the bulk of the watermark; this only needs to catch
    /// a writer that committed after the last snapshot was written.
    #[serde_inline_default(8)]
    pub timefusion_delta_scan_depth: usize,
    /// Decline a flush whose batch set is provably already committed (the
    /// duplicates WAL replay manufactures after an unclean exit — see
    /// `docs/plans/2026-09-02-stop-manufacturing-duplicates.md`). Off until
    /// staging proves it: the skip only fires after an unclean restart, which
    /// cannot be induced on the read-only prod host.
    /// Reject a compaction candidate that would push the merged output's UNION
    /// SPAN past this many dedup bins. **0 disables it, which is the default.**
    ///
    /// A unit's output spans the union of what it merged, and a dedup bin must
    /// rewrite every file overlapping it — so a wide output is read once per bin
    /// it touches, forever. The packer's other budgets are BYTES and ROWS;
    /// neither is even correlated with span. Prod 2026-09-04, **297 units over
    /// 90 minutes** (`docs/plans/2026-09-04-certification-proves-the-wrong-thing.md`):
    ///
    /// |                     |   n | p50 | max | <=16 bins |
    /// |---------------------|-----|-----|-----|-----------|
    /// | `HotPacking`        | 118 |  13 |  20 |    62.7 % |
    /// | `SealedConsolidation` | 179 |  84 | 144 |     4.5 % |
    ///
    /// **Set it from that distribution, not from a guess — and not from 16.** An
    /// earlier n=24 sample showed `HotPacking` 13 of 13 under 16 bins and made
    /// 16 look like a clean separator; at n=297 it is 62.7 %, so 16 would reject
    /// 37 % of hot packing too. **~20-24 spares hot packing (max observed 20)
    /// while still rejecting the bulk of sealed consolidation.**
    ///
    /// **Off by default because the trade is real and unpriced.** Any bound that
    /// bites rejects most sealed consolidation, which cannot pick narrower
    /// inputs — so it effectively disables that lane. Whether that is right depends on the file-count win
    /// it gives up against the read amplification it stops, and that comparison
    /// needs the per-unit cost decomposition (`prep/unit-phase-timers`).
    ///
    /// Candidates with no event range are NO OBJECTION, matching how the row
    /// budget treats an absent `numRecords`.
    #[serde_inline_default(0)]
    pub timefusion_compaction_span_budget_bins: i64,
    /// Width of a dedup bin, in minutes. **10 = today's behaviour, unchanged.**
    ///
    /// A dedup unit rewrites every file overlapping its bin, and file-granular
    /// replacement REQUIRES reading those files whole — so a file straddling N
    /// bins is read N times to sweep them. Files are cut at
    /// `timefusion_writer_max_file_bytes` (512 MiB), which at whale density is
    /// ~45 minutes of event time, and the whale's real 1017 MiB files are ~90 —
    /// against a 10-minute bin. **That ratio IS the read amplification**, and
    /// dedup is ~98% of the heavy maintenance pool.
    ///
    /// Measured from `Add.stats` over 7,702 live files (no data read), total
    /// bytes to sweep the fleet once:
    ///
    /// | width | unit size | total read |
    /// |---:|---:|---:|
    /// | 10 min | 1,469 MiB | 19,530 GiB |
    /// | 60 min | 1,734 MiB (+18%) | 3,847 GiB (**5.1x less**) |
    /// | 120 min | 2,053 MiB (+40%) | 2,280 GiB (8.6x less) |
    ///
    /// Narrow bins do not read less — they read THE SAME FILES once per bin.
    /// `otel_metrics` lands within half a point on both axes (5.5x at 60 min)
    /// on 104 vs 12 B/row and a different sort key, which is why this is
    /// structural rather than an artefact of one shape.
    ///
    /// **What is NOT measured is the soak**: 6x fewer, larger units interact
    /// with the claim/lease/900s-deadline machinery, and that needs real
    /// object-store latency (staging), not MinIO — per-unit cost is round
    /// trips. Ship at 10, flip in staging first.
    ///
    /// Changing this RE-KEYS the dirty-bin queue. Each persisted `DirtyBin`
    /// carries the width it was recorded at and is remapped on load
    /// (`crate::storage::remap_bin`), so a change costs an over-approximation,
    /// never a lost bin. **Rollback hazard:** a binary predating that field
    /// reads a post-flip sidecar's ids at its own width and sweeps the wrong
    /// windows — clear `dedup_dirty_bins.json` when rolling back across a flip.
    #[serde_inline_default(10)]
    pub timefusion_dedup_bin_minutes: i64,
    /// Decline a flush whose batch set provably already committed — the
    /// duplicates WAL replay manufactures after an unclean exit (58% of
    /// duplicate groups in a sampled prod file; see
    /// `docs/plans/2026-09-02-stop-manufacturing-duplicates.md`).
    ///
    /// ON by default since 2026-09-05. The "validate in staging" blocker was
    /// stale: the whole cross-boot chain (flush commit writes
    /// `timefusion.landed_digests` → boot history scan → replay re-inserts →
    /// re-flush DECLINED, zero rows lost) is proven by
    /// `replayed_rows_that_delta_already_holds_are_not_written_again`
    /// (tests/e2e/restart_recovery.rs) against real Delta on real object
    /// storage. Staging was only ever needed to OBSERVE it, not to validate it.
    ///
    /// Only ever ACTIVE on a DIRTY boot — a clean boot skips the Delta history
    /// scan that loads the identities, so a graceful deploy pays nothing and
    /// `wal.landed_skips` reading 0 for days is DORMANCY, not failure. The
    /// counters to watch after the next unclean restart: `wal.landed_skips`
    /// against `wal.replay_rows`.
    #[serde_inline_default(true)]
    pub timefusion_landed_skip_enabled: bool,
}

/// WAL durability mode. See `d_wal_fsync_mode` for the env-var encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFsyncMode {
    Milliseconds(u64),
    SyncEach,
    None,
}

impl BufferConfig {
    pub fn flush_interval_secs(&self) -> u64 {
        self.timefusion_flush_interval_secs.max(1)
    }
    pub fn retention_mins(&self) -> u64 {
        self.timefusion_buffer_retention_mins.max(1)
    }
    /// mtime age past which a WAL file is PRESUMED dead weight. Heuristic,
    /// not a soundness bound: replay is cursor-bounded (no age cutoff), so
    /// GC soundness comes from the un-flushed floor and the drained-gated
    /// boot sweep, NEVER from age alone. Do not tighten or bypass the floor
    /// on the strength of this age.
    pub fn wal_gc_max_age(&self) -> Duration {
        // Fixed 30min, decoupled from buffer retention: GC soundness comes
        // from the un-flushed floor above, never from age — age only delays
        // reclaiming FLUSHED segments. The old ~90min retention held ~3x more
        // dead weight, which under catch-up bursts pushed total-on-disk into
        // the disk-runaway breaker and flapped ingest for no durability benefit.
        Duration::from_secs(30 * 60)
    }
    pub fn eviction_interval_secs(&self) -> u64 {
        self.timefusion_eviction_interval_secs.max(1)
    }
    pub fn max_memory_mb(&self) -> usize {
        self.timefusion_buffer_max_memory_mb.max(64)
    }
    pub fn wal_shards_per_topic(&self) -> usize {
        self.timefusion_wal_shards_per_topic.max(1)
    }
    pub fn wal_corruption_threshold(&self) -> usize {
        self.timefusion_wal_corruption_threshold
    }
    pub fn flush_parallelism(&self) -> usize {
        self.timefusion_flush_parallelism.max(1)
    }
    pub fn flush_coalesce_commits(&self) -> bool {
        self.timefusion_flush_coalesce_commits
    }
    pub fn dml_coalesce_secs(&self) -> u64 {
        self.timefusion_dml_coalesce_secs
    }
    pub fn dml_coalesce_fold(&self) -> bool {
        self.timefusion_dml_coalesce_fold
    }
    pub fn delta_scan_concurrency(&self) -> usize {
        self.timefusion_delta_scan_concurrency.max(1)
    }
    pub fn landed_skip_enabled(&self) -> bool {
        self.timefusion_landed_skip_enabled
    }

    pub fn delta_scan_depth(&self) -> usize {
        self.timefusion_delta_scan_depth.max(1)
    }
    pub fn flush_immediately(&self) -> bool {
        self.timefusion_flush_immediately
    }
    pub fn wal_admit_decouple(&self) -> bool {
        self.timefusion_wal_admit_decouple
    }
    pub fn wal_fsync_ms(&self) -> u64 {
        self.timefusion_wal_fsync_ms.max(1)
    }
    pub fn wal_ack_fsync(&self) -> bool {
        self.timefusion_wal_ack_fsync
    }
    pub fn wal_fsync_mode(&self) -> WalFsyncMode {
        match self.timefusion_wal_fsync_mode.to_ascii_lowercase().as_str() {
            "sync_each" | "synceach" | "each" => WalFsyncMode::SyncEach,
            "none" | "off" | "disabled" => WalFsyncMode::None,
            _ => WalFsyncMode::Milliseconds(self.wal_fsync_ms()),
        }
    }
    pub fn wal_max_file_count(&self) -> usize {
        self.timefusion_wal_max_file_count
    }
    /// Byte ceiling for the unflushed-WAL force-flush backstop. An explicit
    /// value overrides the default quarter-buffer ceiling, bounding the
    /// active bucket's restart replay even without memory pressure.
    /// Env-set bytes only; None = derive (see `AppConfig::effective_wal_max_unflushed_bytes`).
    pub fn wal_max_unflushed_bytes(&self) -> Option<u64> {
        (self.timefusion_wal_max_unflushed_mb > 0).then(|| (self.timefusion_wal_max_unflushed_mb as u64).saturating_mul(MIB as u64))
    }
    pub fn wal_hard_limit_bytes(&self) -> Option<u64> {
        (self.timefusion_wal_hard_limit_gb > 0).then(|| self.timefusion_wal_hard_limit_gb.saturating_mul(GIB as u64))
    }
    pub fn bucket_duration_secs(&self) -> u64 {
        self.timefusion_bucket_duration_secs.max(1)
    }

    /// The flush dwell in micros: -1 = one bucket_duration, 0 = gate off.
    pub fn flush_dwell_micros(&self) -> i64 {
        match self.timefusion_flush_dwell_secs {
            s if s < 0 => (self.bucket_duration_secs() as i64) * 1_000_000,
            s => s * 1_000_000,
        }
    }
    pub fn pressure_flush_pct(&self) -> u32 {
        self.timefusion_pressure_flush_pct.min(100)
    }
    pub fn write_backpressure_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_write_backpressure_secs)
    }
    /// Per-bucket Delta-commit watchdog inside `flush_bucket`. 0 disables it (unbounded wait).
    pub fn flush_bucket_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_flush_bucket_timeout_secs)
    }

    /// Total graceful-shutdown budget — see `d_stop_grace`.
    pub fn stop_grace(&self) -> Duration {
        Duration::from_secs(self.timefusion_stop_grace_secs.max(1))
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde_inline_default(1024)]
    pub timefusion_foyer_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb: Option<usize>,
    // Local disk is cheap and fast relative to S3 GETs, so default the cache large
    // — servers run 500GB–1TB cache volumes. foyer creates the backing file sparse,
    // but this is the logical ceiling at which it starts evicting, so it MUST stay
    // <= the cache volume's free space or writes hit ENOSPC before eviction kicks
    // in. Lower it on smaller disks.
    #[serde_inline_default(500)]
    pub timefusion_foyer_disk_gb: usize,
    // 35 days: measured 2026-08-21 — a 7d ttl put the 2-9d query window
    // exactly outside the cache, turning warm 1.3s lookups into 26s-to-timeout
    // cold ones (docs/plans/2026-08-21-post-hot-tier-speed.md). With the hot
    // tier removed, foyer IS the local tier; its horizon must cover the query
    // mix (30d dashboards), not the flush cadence. Disk stays the real bound
    // (env-pinned GB cap + oldest-first eviction).
    #[serde_inline_default(3_024_000)]
    pub timefusion_foyer_ttl_seconds: u64,
    /// Bounded lifetime of resolved Delta providers. A provider is also
    /// invalidated immediately when its Delta snapshot version changes.
    // 5 minutes
    #[serde_inline_default(300)]
    pub timefusion_provider_cache_ttl_seconds: u64,
    #[serde_inline_default(4_096)]
    pub timefusion_provider_cache_capacity: usize,
    #[serde_inline_default(8)]
    pub timefusion_foyer_shards: usize,
    #[serde_inline_default(32)]
    pub timefusion_foyer_file_size_mb: usize,
    #[serde_inline_default("true".to_string())]
    pub timefusion_foyer_stats: String,
    #[serde_inline_default(MIB)]
    pub timefusion_parquet_metadata_size_hint: usize,
    /// Memory limit (MB) for DataFusion's decoded parquet-metadata cache
    /// (`datafusion.runtime.metadata_cache_limit`). See `d_df_metadata_cache_mb`.
    // DataFusion's in-process decoded-parquet-metadata cache (footer + page index).
    // Distinct from the Foyer footer-BYTES cache: this holds the decoded
    // ParquetMetaData so repeat scans skip re-parsing. Entries larger than the
    // limit are silently dropped, so it must comfortably exceed a single file's
    // metadata; the DataFusion default is only 50MB.
    #[serde_inline_default(512)]
    pub timefusion_df_metadata_cache_mb: usize,
    #[serde_inline_default(512)]
    pub timefusion_foyer_metadata_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb: Option<usize>,
    #[serde_inline_default(5)]
    pub timefusion_foyer_metadata_disk_gb: usize,
    #[serde_inline_default(4)]
    pub timefusion_foyer_metadata_shards: usize,
    /// Disk block size (MB) for the main data cache. The block is foyer's
    /// minimal eviction unit AND caps the largest entry that can land on disk
    /// — must be >= the largest file to cache locally. Acts as a floor:
    /// `from_app_config` auto-raises the effective block size to 2x the
    /// compaction target so the two can't drift apart. Default 256MB.
    ///
    /// Also bounds the transient buffer each multipart-write warm holds in
    /// heap (see `timefusion_warm_inline_max_mb`): up to
    /// `timefusion_warm_concurrency` compactions can run at once, so worst
    /// case is `block_size_mb * warm_concurrency` transient heap. On
    /// smaller-memory instances, cap `timefusion_warm_inline_max_mb` independently.
    #[serde_inline_default(256)]
    pub timefusion_foyer_block_size_mb: usize,
    /// Entries larger than this (MB) are inserted disk-only so warming a big
    /// compaction output doesn't evict the hot small-entry working set from
    /// L1 memory. 0 = always use L1.
    #[serde_inline_default(16)]
    pub timefusion_foyer_l1_max_entry_mb: usize,
    /// Don't admit writes whose `date=` partition is older than this many
    /// days (e.g. cold-tier recompress rewrites) — recent data stays local,
    /// old data serves from S3. 0 = no age limit. Pairs with the cache TTL.
    /// 35 to match: dashboards read 30d, so the whole window must be
    /// admittable or its tail is permanently cold (measured 2026-08-21).
    #[serde_inline_default(35)]
    pub timefusion_cache_recent_days: usize,
    /// Optional extra cap (MB) on the in-flight buffer used to warm the cache
    /// directly from a multipart write (skip re-downloading what we just
    /// streamed to S3). Always bounded by the disk block size; 0 = bound
    /// only by the block size.
    #[serde_inline_default(0)]
    pub timefusion_warm_inline_max_mb: usize,
    /// Per-upload cap (MB) on the heap buffer a multipart write tees into to
    /// warm the cache. Uploads that grow past this abandon capture and
    /// stream through untouched — never blocked, never failed.
    ///
    /// Sized for flush outputs, which actually benefit: a flush bucket's
    /// parquet is small and read back within seconds by dashboards, so
    /// teeing it saves a real S3 GET. Compaction/optimize outputs are much
    /// larger and already warmed post-commit through the read path
    /// (`timefusion_warm_after_compaction`), so they're not worth teeing.
    /// Before this cap, capture was bounded only by the block size, which
    /// prod heap profiles attributed a large share of heap to. 0 = bounded
    /// only by the block size, further clamped to the process-wide budget so
    /// a cap larger than the budget can't deny every reservation.
    #[serde_inline_default(32)]
    pub timefusion_write_capture_max_mb: usize,
    /// Process-wide budget (MB) for in-flight write-capture buffers. Each
    /// capturing upload reserves its full per-upload cap up front,
    /// hard-bounding total capture heap regardless of concurrent uploads.
    /// Over budget = capture skipped for that upload (best-effort, upload
    /// unaffected). Also CLAMPS the per-upload cap, so a cap above the
    /// budget degrades capture rather than disabling it. Default 8x the
    /// per-upload cap so a flush wave never starves itself. 0 = unbudgeted.
    #[serde_inline_default(256)]
    pub timefusion_write_capture_budget_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
    /// Scan-resistant admission: a scan reaching further back than this many
    /// hours runs with cache population BYPASSED, so a wide sweep can't
    /// flush the hot tail out of L1/disk. Reads still HIT what's already
    /// cached. 0 disables the bypass.
    #[serde_inline_default(24)]
    pub timefusion_cache_bypass_scan_hours: u64,
}

impl CacheConfig {
    pub fn is_disabled(&self) -> bool {
        self.timefusion_foyer_disabled
    }
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_foyer_ttl_seconds)
    }
    pub fn provider_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_provider_cache_ttl_seconds.max(1))
    }
    pub fn provider_cache_capacity(&self) -> usize {
        self.timefusion_provider_cache_capacity.max(1)
    }
    pub fn stats_enabled(&self) -> bool {
        self.timefusion_foyer_stats.eq_ignore_ascii_case("true")
    }
    pub fn memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_memory_mb * MIB
    }
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map_or(self.timefusion_foyer_disk_gb * GIB, |mb| mb * MIB)
    }
    pub fn file_size_bytes(&self) -> usize {
        self.timefusion_foyer_file_size_mb * MIB
    }
    pub fn metadata_memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_memory_mb * MIB
    }
    pub fn warm_inline_max_bytes(&self) -> usize {
        self.timefusion_warm_inline_max_mb * MIB
    }
    pub fn write_capture_max_bytes(&self) -> usize {
        self.timefusion_write_capture_max_mb * MIB
    }
    pub fn write_capture_budget_bytes(&self) -> usize {
        self.timefusion_write_capture_budget_mb * MIB
    }
    pub fn block_size_bytes(&self) -> usize {
        self.timefusion_foyer_block_size_mb * MIB
    }
    pub fn l1_max_entry_bytes(&self) -> usize {
        self.timefusion_foyer_l1_max_entry_mb * MIB
    }
    /// Scan lookback depth past which cache population is bypassed, in the same
    /// unit the read path measures it. `None` = never bypass.
    pub fn cache_bypass_scan_micros(&self) -> Option<i64> {
        (self.timefusion_cache_bypass_scan_hours > 0).then(|| self.timefusion_cache_bypass_scan_hours as i64 * 3_600 * 1_000_000)
    }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb.map_or(self.timefusion_foyer_metadata_disk_gb * GIB, |mb| mb * MIB)
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde_inline_default(20_000)]
    pub timefusion_page_row_count_limit: usize,
    /// ZSTD level for every WORKING write: flush, hot-tail packing, and dedup
    /// staging. Default 3 — low enough not to charge ingest latency for data
    /// that may be rewritten again, high enough to be a reasonable resting
    /// place if it never is.
    ///
    /// There are exactly TWO levels in the system, and that is deliberate. A
    /// third "intermediate" level of 1 used to sit here, justified by a comment
    /// promising that "recompress still re-tiers it" — but the recompress and
    /// consolidate crons were gated off when the coordinator took over slice
    /// maintenance, so nothing re-tiered anything. Compaction wrote at 9 and
    /// dedup staging then rewrote the SAME data down to 1, permanently, because
    /// staged parquet is the final file. A cheap-and-temporary write became the
    /// cheap-and-permanent one. Do not reintroduce a level whose correctness
    /// depends on a later pass unless that pass provably runs.
    #[serde_inline_default(3)]
    #[serde(alias = "timefusion_zstd_level_hot")]
    #[serde(alias = "timefusion_zstd_level_intermediate")]
    pub timefusion_zstd_compression_level: i32,
    /// ZSTD level for SEALED writes — compaction and consolidation, where the
    /// data is not expected to be rewritten again. Default 9.
    #[serde_inline_default(9)]
    pub timefusion_zstd_level_warm: i32,
    #[serde_inline_default(128 * MIB)]
    pub timefusion_max_row_group_size: usize,
    #[serde_inline_default(10)]
    pub timefusion_checkpoint_interval: u64,
    // 256MB compacted-file target: fewer, larger files cut Delta metadata, S3
    // object count, and the per-commit get_file_uris() walk on the flush append
    // path; sorted + page-indexed files still prune time-range queries within a
    // file, so the query downside is minimal for this (project_id,date)-partitioned
    // workload. Light/today optimize keeps its own 16MB target.
    #[serde_inline_default(256 * MIB as i64)]
    pub timefusion_optimize_target_size: i64,
    // Cold tier: sealed partitions (older than `cold_optimize_after_days`) bin-pack
    // to 512MB. File size grows with partition age — recent days stay at 256MB (less
    // rewrite while the day still fills), sealed days consolidate to 512MB so the
    // Delta checkpoint (≈ live file count) shrinks, the dominant driver of commit
    // latency. Compression is per-row-group, so bigger files don't change bytes
    // stored — the win is fewer files. Re-runs are cheap: Compact skips files
    // already ≥ target. 512MB, not 1GB: a merge holds ~target-sized output buffers
    // per concurrent task and the decompressed working set is ~17x the compressed
    // target, so 1GB made the final consolidation's sort/merge memory-hostile.
    #[serde_inline_default(512 * MIB as i64)]
    pub timefusion_cold_optimize_target_size: i64,
    // 1 day = everything past the current (day-partitioned) partition. Only today
    // still takes writes, so every sealed day consolidates to 512MB. The warm
    // optimize is clamped to dates newer than this boundary (see `optimize_table`)
    // so the 30-min Z-order never fragments these files back to 256MB.
    #[serde_inline_default(1)]
    pub timefusion_cold_optimize_after_days: u64,
    #[serde_inline_default(50)]
    pub timefusion_stats_cache_size: usize,
    #[serde(default)]
    pub timefusion_bloom_filter_disabled: bool,
}

impl ParquetConfig {
    /// Warm/cold boundary in days, floored at 1: the current (day-partitioned)
    /// partition must always stay warm — it's still taking writes — so 0 is
    /// never valid (it would consolidate today to the cold target mid-write).
    pub fn cold_optimize_after_days(&self) -> u64 {
        self.timefusion_cold_optimize_after_days.max(1)
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    // Observability data is high-churn and rarely time-traveled; the only hard
    // floor is that retention must outlive any in-flight query (which holds a Delta
    // snapshot referencing files vacuum would delete). This value also drives
    // `delta.deletedFileRetentionDuration` (set at create + reconciled at load):
    // Remove tombstones stay in every checkpoint for this long, and a shorter
    // default's compaction churn accumulated tombstones replayed on every
    // snapshot refresh.
    //
    // 72h, not 24h: removed-but-unvacuumed files are the ONLY recovery source
    // after a bad rewrite, and the default is what a deploy-wiped env falls back
    // to. A vacuum once fired inside a 24h window right after an env wipe and
    // permanently destroyed millions of rows' recovery source. A 3-day floor
    // keeps one bad daytime incident recoverable across a weekend.
    #[serde_inline_default(72)]
    pub timefusion_vacuum_retention_hours: u64,
    // Delta _delta_log (transaction-log) retention. Keeps the log directory small
    // (~commit-rate × retention files) so every commit's version-discovery LIST
    // stays cheap. Delta's default is 30 DAYS, which let the log grow to tens of
    // thousands of objects and made each commit's version-discovery slow; even a
    // 1-day window regrew under the multi-tenant per-project commit rate, so hold
    // a tighter 6h window. enableExpiredLogCleanup (default true) prunes during
    // checkpoints; cross-project flush coalescing cuts the commit rate driving
    // growth.
    #[serde_inline_default(6)]
    pub timefusion_log_retention_hours: u64,
    #[serde_inline_default(48)]
    pub timefusion_optimize_window_hours: u64,
    /// Target DECODED bytes in one maintenance scan batch.
    ///
    /// A batch is the sort's indivisible admission unit AND the granularity of
    /// every spill write, so the cost that matters is bytes, not rows — and
    /// otel rows differ in width by more than 20x between tenants. The
    /// coordinator rewrite paths pinned 256 ROWS instead, which is right for a
    /// 63 KB whale row and ~30x too small for an ordinary 1 KB one: measured on
    /// prod 2026-08-31, `wave_bin_staged` reported 0.29-1.7 MB/s compressed
    /// across every lane, and got WORSE with bin size (5.6 MB/s at 20 MB in,
    /// 0.29 MB/s at 94 MB in) because spilling at 256-row granularity pays
    /// Arrow IPC framing over 97 columns per record.
    ///
    /// 8 MB keeps a deep merge affordable — peak merge memory is
    /// `fan_in x batch_bytes`, so a 16-way merge costs ~128 MB against a
    /// per-worker coordinator share of ~500 MB — and it is where the measured
    /// win is: on prod's worst repair input (1.148 GB, 6.8 KB/row) the sort ran
    /// 39.4s at 256 rows, 23.4s at 2048 and 20.3s at 8192, against a 7.3s
    /// scan-only floor (`benches/rewrite_throughput.rs`, 2026-08-31).
    #[serde_inline_default(8 * MIB as u64)]
    pub timefusion_maintenance_batch_target_bytes: u64,
    /// Decoded bytes per event-time slice of a REPAIR rewrite. **0 disables
    /// slicing**, which is the default and the only setting that has ever been
    /// measured to work — see `coordinator_slice_target`. Kept as a kill switch,
    /// not a tuning dial: any non-zero value reinstates one full re-read and
    /// re-decode of the input file per slice.
    #[serde_inline_default(0)]
    pub timefusion_repair_slice_decoded_target_bytes: u64,
    /// Use Z-order clustering for the periodic full OPTIMIZE. Default OFF:
    /// Z-order runs a memory-heavy global sort that can exhaust the pool on
    /// large windows, and its space-filling curve loosens timestamp locality.
    /// Plain Compact bin-packs the flush's already time-sorted files instead,
    /// with no global sort. (The cold-tier recompress path keeps Z-order
    /// independently, since it already does a full-file rewrite.) Re-enable
    /// only once Z-order's memory footprint is bounded.
    #[serde(default)]
    pub timefusion_optimize_use_zorder: bool,
    /// Rewrite optimize/compact/recompress output sorted by the schema's
    /// `sorting_columns` with an honest DESC footer, so timestamp-ordering/LIMIT
    /// pushdown keeps firing on rewritten partitions, not just fresh flush
    /// files. Default ON: without it, rewrites concatenate unsorted, strip the
    /// footer, and the all-or-nothing ordering rule disables pushdown for the
    /// whole partition after one compaction cycle.
    ///
    /// Once every file in a partition carries a sorted footer, DataFusion
    /// elides the sort into a streaming `SortPreservingMergeExec` (bounded
    /// memory, ~one batch per file). An OOM was previously traced to `df.sort()`
    /// running a blocking sort over *unsorted* inputs (a heterogeneous-flush-file
    /// bug, since fixed). TRANSITION CAVEAT: the first compaction of a partition
    /// still holding legacy unsorted files is a one-time blocking sort (bounded
    /// by the maintenance pool + spill); after that every later compaction is
    /// the streaming merge. Set `false` (plain Compact) only if a deployment
    /// can't afford even that one-time transition sort.
    #[serde_inline_default(true)]
    pub timefusion_optimize_sort_by: bool,
    /// Budget for an IN-PROCESS Arrow sort on the flush path, in in-memory
    /// bytes. Past it `sort_batches_by_schema` writes unsorted (still correct —
    /// the footer just won't advertise an order) rather than materializing a
    /// giant coalesced backfill with no pool and no spill.
    ///
    /// Measured in IN-MEMORY bytes, not file bytes: at prod's ~17x zstd ratio a
    /// 256 MB file-byte hot-tail bin is ~4.3 GB here. Paths that rewrite whole
    /// bins sort inside a pooled, spillable DataFusion plan instead — see
    /// `stage_hot_bin`.
    // 2 GiB escalation threshold for the flush sort. The flush sort is IN-PROCESS
    // and allocates OUTSIDE the DataFusion pool — raising this ceiling authorised
    // multi-GB untracked allocations on the ingest path and correlated with OOM
    // kills, so it was reverted. Past this threshold the flush instead sorts
    // inside a pooled, disk-spilling DataFusion plan (`sort_flush_group_spilling`)
    // — the footer stays honest and the peak is bounded by the pool. 2 GiB keeps
    // the fast in-process path for everything ordinary; DataFusion's sort also
    // overtakes Arrow lexsort above ~370 MB, so the pooled path is faster there too.
    #[serde_inline_default(2 * GIB)]
    pub timefusion_sort_skip_bytes: usize,
    /// Pool for the flush-path escalation sort, in MB — its own slice so an
    /// ingest-path sort never queues behind a Z-order holding the maintenance
    /// pool. Deliberately smaller than the escalation threshold: exceeding it
    /// spills to disk, the intended degradation.
    #[serde_inline_default(1024)]
    pub timefusion_flush_sort_pool_mb: u64,
    #[serde_inline_default(5)]
    pub timefusion_compact_min_files: usize,

    /// Refuse a packing bin that rewrites more than this many ROWS per file it
    /// eliminates. **0 = off; the refusal is counted either way.**
    ///
    /// A bin's benefit is files removed; its cost is what the rewrite writes.
    /// `min_files` cannot express that — it is tested against the candidate
    /// POOL, so a pool of five still emits a two-file bin once the bytes reach
    /// the target.
    ///
    /// Prod 2026-09-04, quiet 95 min: 2-file merges were **82.5% of packing
    /// write volume** and **9.9% of the file reduction** — **3,129,141 rows per
    /// file eliminated** against **27,098** for 9+-file merges, 115x worse.
    /// Replaying 163 real units, a floor of 1M rows/file refuses **82.4%** of
    /// the write volume for **8.8%** of the benefit, and the population is
    /// bimodal (expensive bins ~3.77M, everything else under 1M) so anything
    /// from ~1M to ~3M gives the same answer.
    ///
    /// **In ROWS, not bytes, and that distinction is the bug this replaces.**
    /// The first version priced it in bytes at 100 MiB, converting the rows
    /// measurement at 104 B/row DECODED — but the packer compares COMPRESSED
    /// `add.size`, 12x smaller, so the floor could never fire. `pack_value_refused`
    /// reading 0 on a live process is what caught it.
    /// **1,000,000 — the measured knee, made safe by the fan-in escape.**
    ///
    /// Without that escape a biting floor WEDGES packing: at 18 MiB arrivals
    /// (~2.18M rows/file) even a 10-file merge is 2.4M rows per file eliminated,
    /// so a 1M floor refused EVERY bin and steady-state amplification fell to
    /// 0.00x — nothing merged at all. The guard therefore never refuses a bin of
    /// `min_files` or more: it can only remove LOW-fan-in bins, which is exactly
    /// the measured problem and never the remedy.
    ///
    /// With the escape, on the real packer over 400 rounds:
    /// **amplification 6.73x -> 2.06x (-69%), live files 31 -> 32.** That is
    /// close to the `log_fanin` floor of ~1.7x for this file size, with no
    /// file-count regression.
    #[serde_inline_default(1_000_000)]
    pub timefusion_pack_max_rows_per_file_eliminated: u64,
    /// Five-minute hot-partition compaction is required to prevent a
    /// small-file backlog. Set false only as an incident kill switch.
    #[serde_inline_default(true)]
    pub timefusion_light_optimize_enabled: bool,
    // 256MB (raised from 32MB): the small-merge-memory rationale for a tiny
    // hot/today target is moot on this box, and 32MB left the hot partition as
    // dozens of tiny files for a high-write project — recent queries were
    // file-open-latency bound. A larger target collapses today's sealed slices
    // into a few large event-time-disjoint runs.
    #[serde_inline_default(256 * MIB as i64)]
    pub timefusion_light_optimize_target_size: i64,

    /// Shrink a maintenance unit's target as its lane's memory pool fills, so a
    /// few large units cannot monopolise it. **Off by default: the reduction is
    /// COMPUTED and COUNTED either way, and only APPLIED when this is true**, so
    /// the decision can be made from `maintenance.pressure_scale_*` rather than
    /// from an argument.
    ///
    /// Borrowed from ClickHouse, whose `ReplacingMergeTree` is the same
    /// keep-greatest dedup we run: `max_bytes_to_merge_at_max_space_in_pool` is
    /// reduced when the background pool is nearly full, "to keep slots available
    /// for smaller, more urgent merges rather than letting a few large merges
    /// monopolize the entire pool". Our budgets are static, and the pathology
    /// that rule prevents is measured here: on 2026-09-04 two Repair units took
    /// **29% of all maintenance worker time** (38.6 of 134 worker-min) against
    /// 203 Pack units, and one 502 s dedup unit was **80% of its lane's** cost
    /// over a quiet hour.
    ///
    /// Taper: full target at or below 50% occupancy, falling linearly to half
    /// the target at 100%. Deliberately gentle — halving is a throughput cost if
    /// the pressure reading is noisy, and nothing has soaked this yet.
    #[serde_inline_default(false)]
    pub timefusion_maintenance_pressure_scaling: bool,
    /// Per-runtime-env spill ceiling in GiB for the maintenance-family
    /// `RuntimeEnv`s (`build_spill_runtime_env`: coordinator, maintenance,
    /// light-optimize, repair spill dirs).
    ///
    /// DataFusion's `DiskManager` defaults to 100 GB, and the default was the
    /// entire reason the repair backlog was FROZEN, not slow: sorting ONE
    /// ~800 MB whale file (~17x decoded) spills past 100 GB, so every attempt
    /// ran ~18 min and died at the cap — `retry_reason=compaction_incomplete`,
    /// 650 of 662 repair WAL records in one night, attempts=1661 on one unit,
    /// zero files repaired in 6 weeks (prod 2026-09-05, project 87576849,
    /// slices Jul 21-28).
    ///
    /// 220 GiB: the prod host has ~390 GB free, the whole-budget clamp means
    /// only ONE repair rewrite runs at a time, and dedup shards are bounded at
    /// 512 MiB decoded — so one repair spill (220) plus incidental spill stays
    /// under free space. Per ENV, not global: two envs spilling 220 GiB
    /// simultaneously would exceed free space, but no two heavy spillers run
    /// concurrently by construction (the clamp) — revisit if that changes.
    #[serde_inline_default(220)]
    pub timefusion_maintenance_spill_max_gb: u64,
    /// Emergency kill switch for the Dedup contiguity rank term (prefer the
    /// slice that EXTENDS a completed run — see `TaskJournal::rank`). ON by
    /// default; set `=false` only to revert the ordering in prod without a
    /// build, the same contract as `timefusion_plan_cache_time_fns`.
    #[serde_inline_default(true)]
    pub timefusion_dedup_contiguity_rank: bool,
    /// Byte ceiling for ONE output file from a rewrite that writes through
    /// `RecordBatchWriter`, which has no target-size support — `flush()` emits
    /// one file per partition regardless of buffer size — so rewrite paths cut
    /// the file themselves once the buffer passes this.
    ///
    /// Unbounded outputs let prod active files grow to 712 MB (median unsorted)
    /// up to 2.34 GB. Beyond wasted read granularity, an oversized file is
    /// effectively unrepairable — re-sorting needs its rows in memory, which at
    /// prod's ~17x zstd ratio is past any sort budget, so once a file lands
    /// without a `sorting_columns` footer it stays that way.
    ///
    /// Cutting is free for correctness: each cut lands on a contiguous slice of
    /// an already-sorted stream, so every piece keeps a sorted footer and stays
    /// event-time disjoint (better pruning).
    #[serde_inline_default(512 * MIB)]
    pub timefusion_writer_max_file_bytes: usize,
    /// Largest file a 5-minute hot tick will rewrite purely to repair its
    /// missing `sorting_columns` footer.
    ///
    /// Deliberately separate from `timefusion_writer_max_file_bytes` even
    /// though they default the same: that one caps what we WRITE, this caps
    /// what we pull into a tick to REPAIR — raising repair reach shouldn't also
    /// start emitting bigger files. Anything above this is left to `timefusion
    /// optimize --recompress`, the only thing that can touch a single-file
    /// partition.
    ///
    /// Raised to 1 GiB on 2026-09-04: a census of the legacy population found
    /// the largest un-repaired files at 889 MB (five of them, 3.74 GB total,
    /// written 2026-08-03 by the pre-cut writer), sitting permanently above the
    /// old 512 MB ceiling with no automatic path back. 1 GiB covers that whole
    /// tail. It does NOT widen what we write — `timefusion_writer_max_file_bytes`
    /// still cuts output at 512 MB, so this can only ever shrink the suspect set.
    #[serde_inline_default(1024 * MIB)]
    pub timefusion_repair_max_file_bytes: usize,
    /// Sealed dates (yesterday backwards) the hot tail also scans for FOOTER
    /// REPAIR — rewriting files with no `sorting_columns` so the reader's
    /// all-or-nothing ordering claim survives. Repair only: sorted files on
    /// those dates are never re-binned.
    ///
    /// Default 31: one file with no `sorting_columns` footer voids the ordering
    /// claim for every query whose window touches its date, so the lookback
    /// must cover the windows users actually query (up to 30d) — too short a
    /// lookback leaves poisoned files that pin wide queries at `full-set` dedup
    /// forever.
    ///
    /// Do NOT set this far past the query window "to be safe": admission offers
    /// every un-verified sealed file, so the lookback IS the suspect-set size,
    /// and too wide a value can spend the whole pass clearing correctly-sorted
    /// files without ever reaching a rewrite. 0 restores today-only repair.
    #[serde_inline_default(31)]
    pub timefusion_light_optimize_repair_days: u64,
    // Concurrent merge tasks per optimize run — formerly
    // `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`. Now `derived.optimize_merge_tasks()`.
    #[serde_inline_default("0 */5 * * * *".to_string())]
    pub timefusion_light_optimize_schedule: String,
    /// Sealed-date FOOTER REPAIR, on its own cron, split out of the hot-tail
    /// tick: a repair unit is one whole-file rewrite of up to ~1 GiB, too big
    /// for a 5-min tick's budget, and an over-budget bin is discarded and
    /// re-selected identically — so repair could never complete while burning
    /// packing ticks.
    ///
    /// Every 3 hours, i.e. a 144-minute budget, sized from measurement: a
    /// contention-free rewrite of prod's worst partition (969 MB, 19.7M rows)
    /// took 43 minutes solo; the same work under five concurrent repair bins
    /// was ~13x slower. Expect roughly 2-3x the solo time under concurrency, so
    /// the budget needs real margin over that.
    ///
    /// Shorter schedules (20-min, then hourly) were both tried and both still
    /// discarded the big file at the deadline every time — an over-budget bin
    /// never completes at any cadence if the budget itself is too small.
    ///
    /// Longer ticks cost nothing in throughput: a wave serves every pending
    /// project (concurrency bounds parallelism, not count), so slot-minutes per
    /// day are identical at any period, and only longer ticks let large units
    /// actually finish.
    #[serde_inline_default("0 30 * * * *".to_string())]
    pub timefusion_footer_repair_schedule: String,
    /// How long ONE repair pass may run, in seconds — deliberately INDEPENDENT
    /// of the schedule above, unlike every other maintenance tick.
    ///
    /// Everywhere else, budget = 80% of the cron period, which forces a single
    /// trade-off: frequent attempts XOR a long run. Repair needs both. Its unit
    /// is a whole 700 MB - 1 GiB file that measured 43 minutes to rewrite
    /// contention-free on prod, so the budget must be hours; but tying cadence
    /// to that means the first attempt after a restart is hours away, and a
    /// process that restarts on every deploy would rarely repair anything.
    ///
    /// `spawn_cron_job` SKIPS overlapping ticks rather than queueing them, so a
    /// short period with a long budget is well-defined: a pass starts soon after
    /// boot, runs as long as it needs, and the ticks it overruns are dropped.
    #[serde_inline_default(8640)]
    pub timefusion_footer_repair_budget_secs: u64,

    /// How many footer-less files ONE repair pass rewrites. Was hard-coded to 1.
    ///
    /// Prod 2026-09-04: **252 files pending** at ~1 unit per 95 minutes is a
    /// ~17-day drain, and until a file is repaired every query whose window
    /// touches it is forced onto the unordered merge-on-read path. monoscope's
    /// log explorer **fails outright past ~2.5 days** as a result. The repair
    /// backlog IS the query wall, so draining it is a user-facing fix, not
    /// housekeeping.
    ///
    /// `timefusion_footer_repair_budget_secs` still bounds the pass, so this
    /// cannot overrun a tick — it stops the pass returning early with time left.
    /// 4 is deliberately modest: a repair unit is a whole-file rewrite (measured
    /// 43 min contention-free on a 1 GiB file), and the pool is shared.
    #[serde_inline_default(4)]
    pub timefusion_footer_repair_files_per_pass: usize,
    /// Dirty-bin dedup of sealed (< today) partitions, on its OWN cron —
    /// decoupled from hot-tail compaction so an old-date dedup backlog can't
    /// starve today's compaction (they touch disjoint partitions). Default 5 min.
    #[serde_inline_default("0 */5 * * * *".to_string())]
    pub timefusion_dedup_schedule: String,
    /// Incident kill switch for physical dirty-bin dedup; read-side dedup
    /// remains the correctness path. Re-enabled by default after prod-shaped
    /// validation: canaried on live traffic, then physically audited committed
    /// bins — distinct dedup keys intact in-bin and across the partition, only
    /// duplicate versions removed.
    #[serde_inline_default(true)]
    pub timefusion_dirty_bin_dedup_enabled: bool,
    #[serde_inline_default("0 */30 * * * *".to_string())]
    pub timefusion_optimize_schedule: String,
    /// Passes of cold consolidation to run on each hot-compaction tick, for
    /// sealed partitions the daily sweep never reached. Small on purpose: each
    /// pass is one ≤target sorted rewrite and its own commit, so a restart
    /// costs at most one pass and the next tick resumes. 0 disables.
    // Every 6h, not daily: tombstones leave the checkpoint once older than the
    // retention window, so vacuum must run often enough to delete files before
    // their tombstones age out (VacuumMode::Full backstops any that slip through).
    #[serde_inline_default("0 15 */6 * * *".to_string())]
    pub timefusion_vacuum_schedule: String,
    /// Out-of-band checkpoint + expired-log-cleanup schedule. See d_checkpoint_schedule.
    // Out-of-band checkpoint + expired-log cleanup, driven here instead of
    // delta-rs's commit-path hook: a hook failure surfaced as a commit error
    // AFTER the commit landed, and the flush path misread that as a failed
    // commit and deleted the committed parquet. Every 2 min, tolerant of R2
    // 500s — faster than the commit cadence so the log stays bounded.
    #[serde_inline_default("0 */2 * * * *".to_string())]
    pub timefusion_checkpoint_schedule: String,
    /// Dangling-Add reconcile schedule. See d_reconcile_schedule.
    // Reconcile active Add entries against object-store truth: HEAD every live
    // file and commit Remove for any that are missing. Repairs dangling Adds left
    // by past commit-path parquet deletions; a nonzero removal count means
    // committed data was destroyed elsewhere.
    #[serde_inline_default("0 0 * * * *".to_string())]
    pub timefusion_reconcile_schedule: String,
    /// Tantivy index reconcile: backfill uncovered live parquet + GC manifest
    /// entries for rewritten-away files, per-uuid manifests included. The
    /// single-process self-management of index consistency — compaction/wave
    /// commits and CLI runs all converge here.
    ///
    /// Hourly, was `0 30 3 * * *` (once a day). Daily could not hold the line:
    /// uncovered files accrue continuously (~85/hr observed) while this box
    /// restarts every few hours for deploys and OOM kills, so a process that
    /// never lived through 03:30 never reconciled AT ALL — which is how prod
    /// reached 5,506 uncovered files with a drain that looked implemented.
    /// Safe to run often only because each pass is now bounded by
    /// `timefusion_tantivy_backfill_max_files_per_pass`; before that bound a
    /// pass attempted every uncovered file at once, which is what forced the
    /// nightly cadence in the first place.
    ///
    /// Every 15 minutes, was `0 20 * * * *` (hourly, at minute 20). The hourly
    /// form fires at ONE INSTANT per hour, and prod restarts more often than
    /// that: measured 2026-08-22, **zero** `tantivy_backfill_started` in six
    /// hours of logs, because no container lived through a `:20` boundary. The
    /// drain was not slow, it was never running — every other coverage finding
    /// that day (ordering, cap, tail reservation) sat downstream of this.
    /// Four chances an hour means a container need only survive 15 minutes to
    /// start draining. Overlapping ticks are already handled: the job logs
    /// "run still in progress" and drops the tick rather than piling up.
    #[serde_inline_default("0 */15 * * * *".to_string())]
    pub timefusion_tantivy_reconcile_schedule: String,
    /// File-level needle pruning: consult per-file bloom sidecars at
    /// file-selection time so point lookups (trace_id/id/span_id…) scan only
    /// files that can contain the needle. Kill switch for the read path; the
    /// sidecar builder cron is keyed off the same flag.
    /// docs/plans/2026-08-22-file-level-needle-pruning.md
    #[serde_inline_default(true)]
    pub timefusion_file_bloom_pruning: bool,
    /// Bloom sidecar reconcile: lift parquet blooms of uncovered live files
    /// into per-(project,date) sidecars, GC retired entries. Each pass is
    /// bounded by `timefusion_bloom_sidecar_files_per_pass`, newest dates
    /// first so hot partitions converge first.
    #[serde_inline_default("0 */5 * * * *".to_string())]
    pub timefusion_bloom_sidecar_schedule: String,
    #[serde_inline_default(512)]
    pub timefusion_bloom_sidecar_files_per_pass: usize,
    /// Resident registry cap; sidecars beyond it are re-fetched on demand
    /// (off the plan path — a miss only skips pruning for that query).
    #[serde_inline_default(256)]
    pub timefusion_bloom_registry_cap_mb: usize,
    /// Re-fetch a resident sidecar this often so entries built since the
    /// last load start pruning without a restart.
    #[serde_inline_default(300)]
    pub timefusion_bloom_registry_refresh_secs: u64,
    /// Proactively warm the Foyer cache for files written by a flush/optimize
    /// commit, so recent partitions dashboards read don't cold-start after
    /// every compaction. Footers are always warmed when enabled.
    #[serde_inline_default(true)]
    pub timefusion_warm_after_compaction: bool,
    /// In addition to footers, warm the full file contents into the main
    /// (full-file) cache. OFF by default. Tried ON to keep the recent-window hot
    /// tail warm, but on the memory-tight prod box continuous full-body warms
    /// (every flush plus a boot burst on the uncompacted busy table) drove RSS
    /// toward the OOM ceiling with no query load. Footers carry most of the
    /// planning-latency win at a fraction of the bytes; only enable full-file
    /// warming where Foyer + memory have real headroom.
    #[serde(default)]
    pub timefusion_warm_full_files: bool,
    /// Only warm files whose `date=` partition is within this many days of
    /// today. Bounds warming to the partitions dashboards actually query.
    /// 0 = no recency limit.
    /// 35 (was 9, was 1): the final-cycle EA (2026-08-22, bench_final_cycle)
    /// attributed the largest per-cell delta to windows beyond the body-warm
    /// depth — P1 B 30d ran 27.2s wall on 0.02s compute over 746 cold file
    /// opens. Full-file warmth must reach the dashboard horizon (30d + ttl
    /// slack); ~35 days fleet-wide is ~40GB against the 600GB cache, and the
    /// paced fetcher (timefusion_warm_body_boot_files_per_sec) bounds the
    /// burst regardless of depth.
    #[serde_inline_default(35)]
    pub timefusion_warm_recency_days: u64,
    /// Paced full-body warm at BOOT for recency-window files, in fetched
    /// files/sec. The unpaced variant was tried and reverted: re-downloading
    /// every recent body at boot saturated object-store bandwidth (13GB
    /// during three 1h queries) and made a recovered deployment slower than
    /// a cold read. Pacing + skip-if-cached removes that failure mode while
    /// closing the measured cold band (2026-08-21: 2-9d-old windows ran
    /// 23-31s cold vs 1.0-1.3s warm on identical scans). At ~0.5MB/file
    /// (fragmented days) 16 files/s ≈ 8MB/s — invisible next to query load;
    /// ~60k fleet files complete in ~1h. 0 = footer-only boot warm (the old
    /// behavior).
    #[serde_inline_default(16)]
    pub timefusion_warm_body_boot_files_per_sec: u32,
    /// Warm parquet footers for EVERY live file (not just recency-window
    /// ones). Footers are tens of KB each, but on tables with thousands of
    /// files the boot-time GET burst may matter on small instances — disable
    /// to fall back to recency-bounded footer warming.
    #[serde_inline_default(true)]
    pub timefusion_warm_all_footers: bool,
    /// Max concurrent warm fetches per commit. Bounds the S3 GET burst a
    /// warm job adds right after a compaction.
    // 16: at concurrency 4 a large boot warm ran >55 min and was cut short by a
    // restart every time; 16 finishes in ~1-3 min. Footer GETs are small
    // suffix-range reads, well within R2/S3 burst limits.
    #[serde_inline_default(16)]
    pub timefusion_warm_concurrency: usize,
    /// How long the maintenance coordinator waits for the boot table REPLAY
    /// before starting anyway.
    ///
    /// The coordinator used to wait for the whole preload, unconditionally, and
    /// preload is only complete when the PACED body warm has finished every
    /// table. Prod 2026-08-23, 45 min over two boots: 26
    /// `bootstrap.phase=table_preload` starts and **zero**
    /// `table_preload_complete`; `tasks_running` sat at 0 against 22,218
    /// pending / 12,329 ELIGIBLE units. That is what made the wait bounded.
    ///
    /// The budget then expired on every boot instead (container `62f2385`:
    /// replay ~4 s, full preload 27 min 21 s), so the coordinator ran beside
    /// the still-running warm for 22 of 27 minutes anyway — the wait bought
    /// nothing and cost a fifth of a ~15-minute container's life. The gate is
    /// now the replay phase, which is what maintenance actually needs; the
    /// warm is paced to be safe beside other work. 0 disables the wait
    /// entirely.
    #[serde_inline_default(300)]
    pub timefusion_coordinator_preload_wait_secs: u64,
    /// After a compaction commit, proactively evict the cached full-file bytes
    /// of the files it tombstoned (no longer in the live set), instead of
    /// waiting for VACUUM / TTL / LRU to reclaim them. Cheap (in-cache only, no
    /// S3) and keeps the cache from filling with dead compaction outputs.
    #[serde_inline_default(true)]
    pub timefusion_evict_after_compaction: bool,
    /// Advance the post-commit snapshot by appending only the files the commit
    /// added, instead of re-materializing the whole active file set (2-8s over
    /// 26k files every flush in prod). Produces an identical file set — a
    /// faster, equivalent replay, safe regardless of writer count. Off reverts
    /// to the full re-materialize per commit.
    #[serde_inline_default(true)]
    pub timefusion_incremental_snapshot: bool,
    /// Belt-and-suspenders for the above: every Nth commit per table, drop the
    /// materialized files and re-materialize from S3 truth, bounding any drift
    /// from an incremental-replay bug. 0 disables reconciliation.
    #[serde_inline_default(500)]
    pub timefusion_snapshot_reconcile_commits: u64,
    /// Commit staged-but-uncommitted footer-repair parquet found at boot,
    /// instead of deleting it and re-doing the 40+ minute rewrite. False
    /// reverts to plain reconcile-and-delete. Only data-preserving
    /// (compaction/repair) bins are eligible — dedup bins drop rows and stay
    /// cleanup-only.
    ///
    /// Defaults ON: a repair bin is a single 40+ minute whole-file rewrite, and
    /// prod was replacing the task every 15-28 minutes (deploys plus
    /// healthcheck replacements), so every pass discarded a complete staged
    /// output and the same file stayed poisoned for days. Resume is what makes
    /// the rewrite survive a restart at all.
    #[serde_inline_default(true)]
    pub timefusion_repair_resume_enabled: bool,

    /// Record a file as verified-sorted when the WRITE that produced it stamped a
    /// `sorting_columns` footer, and sweep the files that predate that. False leaves
    /// `repair_verified_sorted` fed only by the footer probe — the pre-2026-08-28 behaviour.
    ///
    /// Defaults ON. This is a kill switch for a mechanism that decides a file will NEVER be
    /// offered to footer repair, which is the one direction that can hide a genuinely poisoned
    /// file, so it gets an off switch on the same reasoning as
    /// [`Self::timefusion_repair_resume_enabled`].
    ///
    /// **Its limits, stated because a half-working lever is worse than none:** turning this off
    /// stops NEW marks, it does not un-mark anything. The set is persisted, so recovery from a
    /// wrong exoneration is `rm <data_dir>/repair_verified_sorted.txt` and a restart — the flag
    /// alone will not do it. It gates the seeding sweep too; gating only the marking would leave
    /// the sweep re-deriving the same answer from the footers an hour later.
    #[serde_inline_default(true)]
    pub timefusion_repair_mark_sorted_at_write: bool,
    /// Days back (plus today) the dedup sweep scans.
    // 128: a lower bound put a multi-day floor under the backlog, but a much
    // higher one OOM'd the box — RSS climbed independent of query load, tracking
    // pass-scoped state (per-bin provider/session/snapshot) that only frees at
    // pass end. 128 completes a pass in ~10-15min while draining well above the
    // original rate.
    //
    // Separately: how many days back (plus today) the dedup sweep covers. 1 day
    // catches cross-flush dupes from a late replay crossing midnight, but 35 is
    // needed because this sweep is the ONLY caller of `record_certification`,
    // which bounds how far back a partition can be certified duplicate-free —
    // rollup routing needs a contiguous certified prefix across the query window,
    // so a 1-day horizon meant no 7d/14d/30d query could ever route to a rollup.
    //
    // Affordable because the sweep is O(partitions-changed), not O(window): a
    // partition whose `dedup_clean_fp` still matches its live fingerprint is
    // skipped without a probe, and `deadline` + `dedup_sweep_cursor` rotation
    // bound and resume a truncated tick. Do NOT raise this without matching
    // coordinator job concurrency, or the work list backs a queue that commits
    // nothing. 35 covers a 30d query with margin and matches `timefusion_light_optimize_repair_days`.
    #[serde_inline_default(35)]
    pub timefusion_dedup_lookback_days: u64,
    /// Run the legacy partition-wide dedup probe as an audit/fallback. Dirty
    /// sealed bins are the normal maintenance path.
    #[serde(default)]
    pub timefusion_dedup_sweep_fallback: bool,
    // Rollup builds, read routing and the realtime tail were three separate
    // `#[serde(default)]` bools — i.e. OFF unless the environment said otherwise,
    // which meant a fresh deployment ran with rollups silently disabled and every
    // wide query on the raw path. Prod set all three to `true` and had done for
    // months, so the flags encoded no decision anyone was still making; they only
    // created a way for the feature to be off by accident. Deleted rather than
    // defaulted-on, so there is no longer a switch to get wrong.
    /// Optional comma-separated read canary projects.
    #[serde(default)]
    pub timefusion_rollup_read_projects: Option<String>,
    /// Sealed days back the backfill will build rollups for. 0 disables it.
    ///
    /// Without a backfill the only covered dates are today and the dedup
    /// lookback, and routing needs a contiguous certified prefix from the start
    /// of the window — so a 7d/30d query, the only kind worth accelerating, can
    /// never route no matter how long the process runs.
    ///
    /// This was `#[serde(default)]` (0, disabled) while the only implementation
    /// was orphaned by the coordinator redesign and reachable only from tests —
    /// prod confirmed the gap: a rollup table held rows for two dates against
    /// 30+ days of source, and every wide query was refused with `not_built`.
    ///
    /// 35 matches the dedup certification window so the two horizons can't
    /// drift apart. `plan_rollup_backfill` is bounded per pass, so a wide
    /// horizon converges over hours instead of burying the journal in one go.
    /// Set 0 to disable.
    // Must match `timefusion_dedup_lookback_days` — certification and rollup coverage need
    // the same horizon, or a day is certified but never rolled up (or vice versa).
    // 31, matching what prod runs. Still configurable — a shorter horizon is a
    // legitimate choice for a small deployment — but the default is the value
    // that has actually been exercised.
    #[serde_inline_default(31)]
    pub timefusion_rollup_backfill_days: u16,
    #[serde_inline_default("0 */10 * * * *".to_string())]
    pub timefusion_rollup_backfill_schedule: String,
    /// `(project_id, date)` cells a one-shot repair forces a full re-derive of,
    /// as `project:YYYY-MM-DD`. Empty means "use `DAMAGED_CELLS`", which is
    /// itself empty since the 2026-08-25 damage converged.
    ///
    /// Non-empty REPLACES the const rather than extending it — so a stale value
    /// here silently shadows a const someone has just refilled for the next
    /// repair. Malformed entries are warned and dropped, never silently.
    ///
    /// A parameter rather than only a const so the end-to-end cursor guard can
    /// drive a real planner pass with a synthetic list — otherwise the wiring
    /// that the v1 truncation bug lived in is untested whenever the const is
    /// empty, which is exactly when nobody is watching it.
    #[serde_inline_default(Vec::new())]
    pub timefusion_damage_repair_cells: Vec<String>,
    /// Skip the read-side DedupExec (and its key projection) for Delta-only
    /// queries whose every in-window (project, date) partition was verified
    /// duplicate-free by a sweep pass AND whose file set is unchanged since
    /// (fingerprint match). Also restores per-scan LIMIT pushdown.
    ///
    /// Validated by
    /// `dedup_compaction_test::count_is_identical_with_and_without_the_dedup_skip`,
    /// which builds duplicate keys across separate flush-written files, sweeps,
    /// and demands `count(*)` be identical with the skip on and off —
    /// over-counting is the failure mode that matters and that test catches it.
    ///
    /// This validates the mechanism, not prod scale or every column shape. The
    /// runtime guard bounds the rest: the skip cannot fire on a partition the
    /// sweep hasn't certified with a matching file fingerprint, so an unswept
    /// or newly-written partition keeps full dedup. Turn off here if a count is
    /// ever doubted.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_swept: bool,
    /// Per-DATE dedup skip: a window that is only partly certified skips
    /// `DedupExec` over its certified date partitions instead of losing the
    /// skip entirely.
    ///
    /// Why it exists: the all-or-nothing rule never fires in prod. 2026-08-22
    /// measured 97 live certifications across 13 projects with a longest
    /// consecutive run of **5 days**, against the 7 a week needs and the 30 a
    /// month needs — so `dedup_skipped_pct` sat at 0.0 while certification was
    /// working perfectly (docs/plans/2026-08-21-post-hot-tier-speed.md).
    ///
    /// Why it is sound: `date` is derived from `timestamp` and DML re-appends
    /// preserve the original row's timestamp (`write/mod.rs:1104`), so every
    /// version and tombstone of a row shares one date partition. No dedup key
    /// spans dates, so dedup over the union equals dedup applied per date.
    ///
    /// **Default ON since 2026-08-22.** The failure mode is a silent over-count
    /// on every dashboard tile, so it ships on the strength of
    /// `dedup_compaction_test::per_date_dedup_skip_matches_the_all_or_nothing_result`,
    /// which forces PARTIAL certification (sweep, then write to the older date
    /// so its fingerprint moves) and demands the split result equal the
    /// all-or-nothing result — each date yielding its own winning version.
    /// Kill switch: `TIMEFUSION_READ_DEDUP_SKIP_PER_DATE=false` (or
    /// `..._SKIP_SWEPT=false` to remove the skip entirely).
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_per_date: bool,
    /// Per-FILE dedup skip: within an uncertified date, the FILES a sweep proved
    /// clean still skip `DedupExec` when no uncertified file could hold another
    /// version of their rows.
    ///
    /// Why it exists: certification is keyed on a partition's whole file set, so
    /// ANY new file voids it. Recent partitions are rewritten continuously by
    /// ingest, hot-tail compaction and the sealed backlog, so they churn faster
    /// than sweeps can certify them — prod 2026-08-22 measured
    /// `dedup_denied_never_certified` at 100% of eligible scans while the 97 live
    /// certifications all sat on days nobody queries. Per-date skipping cannot
    /// help there: the whole recent band is uncertified. Per FILE can, because a
    /// new file voids only the files it overlaps.
    ///
    /// Soundness: the dedup key is `(timestamp, id)` and merge-on-read re-appends
    /// preserve the original timestamp, so every version of a row carries that
    /// row's timestamp and must land in a file whose span contains it. A
    /// certified file may therefore skip iff no UNCERTIFIED file's span overlaps
    /// its own — see `read::skippable_certified_files`, which fails closed on a
    /// missing-statistics span, an empty certified set, and inclusive-bound
    /// touching.
    ///
    /// **Default ON since 2026-09-01.** It was off while nothing could produce
    /// per-file evidence: `cert_granted_total` had been 0 since 2026-08-20, so
    /// 0 of 9,296 eligible scans skipped `DedupExec` and the flag guarded a code
    /// path that could never fire. Slice certification now produces that
    /// evidence, and `DedupExec` is the largest remaining term in multi-day
    /// query latency (14 d charts measured at 42.8 s WARM vs 1.0 s at 24 h), so
    /// the skip is the point of the whole mechanism rather than an experiment.
    ///
    /// Soundness does not rest on this flag: `read::skippable_certified_files`
    /// fails closed on a missing-statistics span, an empty certified set and
    /// inclusive-bound touching, and the producer only certifies a file whose
    /// whole span lies inside proved-clean intervals. Keep it as an emergency
    /// kill switch — set `=false` if `count(*)` ever disagrees with the raw
    /// answer — the same role `TIMEFUSION_PLAN_CACHE_TIME_FNS` plays.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_per_file: bool,
    /// Dedup-as-you-compact experiment (docs/plans/2026-08-20-dedup-and-sort
    /// strategy §3): the on-demand compaction path (`compact_date`, i.e. pgwire
    /// `OPTIMIZE` and the CLI) upgrades its SortBy rewrite to SortByDedup, so
    /// merging files also collapses superseded merge-on-read versions
    /// (keep-greatest `dedup_tiebreak` per dedup key). Sealed consolidation
    /// already does this unconditionally; this flag extends it. No-op while
    /// `timefusion_optimize_sort_by` is off (dedup needs the sorted stream).
    #[serde(default)]
    pub timefusion_compact_dedup_merge: bool,
    /// Persist sweep certifications to the data dir and reload at boot, so the
    /// read-side dedup skip doesn't restart cold on every deploy.
    ///
    /// `dedup_clean_fp` is process-local and TF deploys several times a day,
    /// the leading suspect for the skip firing on only a tiny fraction of
    /// Delta-reading scans. Persistence is the lever for that.
    ///
    /// It cannot widen certification: a reloaded entry passes the same
    /// fingerprint-equality check against the live file list as an in-memory
    /// one, so a stale/truncated/corrupted store costs a skip rather than
    /// granting a wrong one.
    ///
    /// Kill switches, in order of bluntness: set this false for a cold cache
    /// per process; set `timefusion_read_dedup_skip_swept` false to remove the
    /// skip entirely. Doubt a `count(*)` and reach for the second one.
    #[serde_inline_default(true)]
    pub timefusion_dedup_certification_persist: bool,
    /// Allow `DedupExec` to run in streaming `bounded[timestamp]` mode, which
    /// trusts the scan's declared `output_ordering` (the parquet footer's
    /// `sorting_columns`). A lying footer makes one "run" span many
    /// timestamps — prod once read 132 rows where 1620 existed, surfacing as
    /// multi-minute dashboard holes.
    ///
    /// Defaults ON: that row loss came from bounded mode also dropping the
    /// bound column from the dedup key, which `dedup_key_idxs` no longer does.
    /// This is only an emergency kill switch, and not a cheap one — bounded
    /// mode carries LIMIT early termination, so turning it off makes "top N"
    /// queries scan the whole window.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_bounded: bool,
    /// Branches a wide AGGREGATE window is split into, so each range's
    /// `DedupExec` runs on its own thread. `0`/`1` disables the split.
    ///
    /// `DedupExec` is `SinglePartition`, so one query gets one core: prod
    /// 2026-09-04 measured 2.1 M rows/s on a 14 d window (24.6 s) and could not
    /// finish 30 d inside the 60 s statement timeout — while the same 30 days
    /// as two CONCURRENT 15 d queries took 19.98 s. Splitting is exact because
    /// `timestamp` leads the dedup key, so no row's versions can straddle a
    /// boundary. Applies only under an aggregate; a `ORDER BY … LIMIT` keeps its
    /// streaming TopK. Each branch re-opens the files its range touches, so
    /// raising this trades file opens for parallelism.
    ///
    /// Shipped at 4 on 2026-09-04, turned OFF the same evening, then back on
    /// with the cause fixed. Warm-process measurements, whale project, daily
    /// buckets:
    ///
    /// | window | split off | split on, unfixed |
    /// |--------|-----------|-------------------|
    /// | 7 d    | 5.8 s     | 5.3 s (untouched — under the span threshold) |
    /// | 14 d   | 15.5 s    | 34.2 s |
    /// | 30 d   | TIMEOUT   | **49.5 s** |
    ///
    /// The split is the only thing that has ever completed 30 d, and it did so
    /// while each branch still read the WHOLE window: this rule runs after
    /// `push_down_filter`, so the branch bound never reached
    /// `TableScan.filters` and all four branches pruned to the same files (four
    /// byte-identical pushed predicates; 52 file groups where the unsplit plan
    /// had 22). That 4x over-read is the whole of the 14 d regression.
    /// `narrow_scan_window` now writes the bound into the scan itself, so a
    /// branch costs a QUARTER of the window.
    ///
    /// NOT reproducible locally — reverting that fix leaves the e2e test green,
    /// because DataFusion re-runs pushdown in the local path and prod's pgwire
    /// path does not. Prod is the only instrument, so judge this ONLY on a warm
    /// process: the 2026-09-04 deploy churn made every cold reading look like a
    /// regression and every regression look like a cold reading.
    /// **MEASURED OFF.** Fixing the over-read did NOT help — it made things worse:
    ///
    /// | build | 14 d | 30 d |
    /// |---|---|---|
    /// | no split (baseline, 7 runs) | 14-33 s | TIMEOUT 5/5 |
    /// | split, unfixed 4x over-read | 34.2 s | 49.5 s (1 sample) |
    /// | split, per-branch pruning FIXED | 43.4 / 48.6 / 59.1 s | TIMEOUT 3/3 |
    ///
    /// Pruning verifiably worked in the fixed build (four DISTINCT pushed
    /// predicates; 40 file groups, down from 52) and it was still slower, so the
    /// 4x over-read was NOT the cost. Something about four concurrent branches
    /// dominates — `GatedScanExec` permit contention and per-branch snapshot and
    /// planning work are the open suspects. Do not re-enable without measuring
    /// THAT; the parallelism win seen from two hand-issued 15 d queries does not
    /// survive being folded into one plan.
    #[serde_inline_default(1)]
    pub timefusion_query_range_split_branches: usize,
    /// Answer gate-eligible `SELECT COUNT(*) ... WHERE project_id AND
    /// timestamp range` from Delta add-action stats (zero parquet IO). Only
    /// fires when the window is fully flushed, dedup-provably-clean, and
    /// every overlapping file lies entirely inside the window — otherwise
    /// the normal scan runs. See the `count_pushdown` section of `read/mod.rs`
    /// (the `src/count_pushdown.rs` this used to name no longer exists).
    ///
    /// **DEFAULT false since 2026-09-04: it returns SILENTLY WRONG COUNTS.**
    /// Measured on prod, project 28f62f01, `date=2026-08-21`:
    ///
    /// | query | answer |
    /// |---|---|
    /// | `count(*)` (pushdown fires) | **2,604,236** |
    /// | `sum(1)` (forces the scan) | 3,551,640 |
    /// | `count(*)` + a neutral predicate (gate declines) | 3,551,640 |
    ///
    /// A 27% undercount, reproducible across restarts. It is provably the
    /// PUSHDOWN that is wrong, not the scan: the scan yields 3,551,640 rows that
    /// are pairwise DISTINCT on the dedup key, and each is a real physical row
    /// (5,419,022 physical in that partition), so at least 3,551,640 distinct
    /// keys exist and no correct logical count can be below it.
    ///
    /// For any `tombstones_possible()` table — which is every OTel table — only
    /// the logical-count INDEX path can fire, and `try_logical_count` declines
    /// beyond a 3-day window. So this flag never affected 14/30-day dashboards;
    /// turning it off costs only the narrow-window fast path, which is exactly
    /// where the wrong numbers were being served. Re-enable once the index is
    /// fixed and has a test that pins it against a scan.
    #[serde_inline_default(false)]
    pub timefusion_count_pushdown: bool,
    /// Per-shard COMPRESSED-bytes target for a dedup chunk rewrite (`sum(add.size)`).
    /// The rewrite is split into `ceil(compressed_bytes / this)` hash-bucketed passes
    /// so each pass reads ~this much. 0 disables this ceiling's contribution to the
    /// shard count. See `d_dedup_max_rewrite_bytes`.
    // Byte ceiling on the file set one dedup chunk rewrite may materialize.
    // Over-budget chunks are SKIPPED loudly (metric: timefusion.dedup.chunk_skipped)
    // rather than rewritten — read-side dedup keeps queries correct meanwhile.
    // Guards against e.g. a z-ordered whole-day file dragging the whole day into
    // one rewrite. Kept in step with `d_dedup_max_decoded_bytes` — shard count
    // takes the MAX of both, so leaving this lower would silently cap sharding
    // below the decoded budget.
    #[serde_inline_default(GIB as u64 / 2)]
    pub timefusion_dedup_max_rewrite_bytes: u64,
    /// Per-shard target on the ESTIMATED DECODED (in-memory Arrow) footprint of
    /// a dedup chunk rewrite. Compressed bytes under-count by 5-20x for wide
    /// Variant/JSON columns, and `SELECT * … collect()` Arrow buffers aren't
    /// accounted by DataFusion's memory pool — a compressed-under-budget chunk
    /// once decoded to tens of GB and OOM-killed the process. The rewrite
    /// shards by a hash of the dedup keys into `ceil(est_decoded / this)`
    /// passes so each pass materializes ~this much; a single key group that
    /// alone exceeds this is unshardable and skipped (read-side dedup keeps
    /// queries correct). 0 → one shard for this ceiling. See
    /// `d_dedup_max_decoded_bytes`.
    // 512 MiB estimated decoded footprint, sized so permits x budget stays bounded
    // in flight (see HEAVY_REWRITE_PERMITS). A chunk this large already dwarfs the
    // DataFusion pool; larger chunks skip rather than risk the cgroup.
    //
    // Sized to FUND SHARD CONCURRENCY, not to save memory: `dedup_shard_concurrency`
    // runs `DEDUP_BIN_ARROW_BUDGET / this` shards at once, so a smaller shard buys
    // parallelism at an unchanged peak. It also keeps each unit inside its per-bin
    // deadline — a unit bigger than its budget produces nothing, forever.
    #[serde_inline_default(GIB as u64 / 2)]
    pub timefusion_dedup_max_decoded_bytes: u64,
    /// Compressed→decoded inflation factor used to estimate a dedup chunk's
    /// in-memory footprint when per-file `num_records` stats are unavailable.
    /// See `d_dedup_decode_inflation`.
    // 12x compressed->decoded: zstd on wide Variant/JSON otel rows routinely
    // decodes 10-20x; 12 is a deliberately conservative floor.
    #[serde_inline_default(12)]
    pub timefusion_dedup_decode_inflation: u64,
    /// Estimated decoded Arrow bytes per row, used with per-file `num_records`
    /// to size a dedup chunk's in-memory footprint. otel spans carry wide
    /// Variant/JSON bodies; 4 KiB is a conservative average. See
    /// `d_dedup_bytes_per_row`.
    // 4 KiB/row decoded estimate for otel spans (wide Variant/JSON bodies).
    #[serde_inline_default(4096)]
    pub timefusion_dedup_bytes_per_row: u64,
    // Max concurrent heavy maintenance rewrites (dedup / optimize / recompress)
    // — formerly `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`. Now
    // `derived.rewrite_permits()`.
    //
    // Concurrent hot-tail light-optimize sorts (per-project fan-out) —
    // formerly `TIMEFUSION_LIGHT_OPTIMIZE_CONCURRENCY`. Now
    // `derived.light_optimize_k(hot_project_count)`.
    //
    // Wall-clock budget for one light-optimize tick — formerly
    // `TIMEFUSION_LIGHT_OPTIMIZE_TICK_BUDGET_SECS`. Now
    // `derived.tick_budget(cron_period)`.
    /// Max concurrent user DML MERGE-UPDATEs (hash-enrichment `UPDATE ... FROM`).
    /// Each merge scans the time-windowed target partition for join-key
    /// matches — heavy on a CPU-throttled box. Ungated, bursts of per-project
    /// drains stampede all cores and starve read queries. This caps that so
    /// reads keep CPU; drains queue behind it.
    // Serial: each merge-update decodes + rewrites whole hot partitions with
    // pool-invisible memory; concurrent stacking under a heavy UPDATE-drain
    // storm drove an OOM crash-loop. Results are identical either way — permits
    // only bound peak memory, excess statements queue.
    #[serde_inline_default(1)]
    pub timefusion_dml_merge_concurrency: usize,
    /// Perform UPDATE/DELETE as merge-on-read deletion-vector operations
    /// instead of copy-on-write full-file rewrites. A DV UPDATE appends only
    /// the rewritten matched rows and masks originals with a roaring-bitmap
    /// deletion vector; a DV DELETE just writes the mask — avoids rewriting
    /// whole partitions for a small predicate.
    ///
    /// Requires the `deletionVectors` writer feature, enabled lazily on first
    /// DV write (a one-time protocol upgrade to reader/writer v3/v7). On by
    /// default. NOTE: the upgrade is irreversible and every reader of these
    /// Delta tables must understand DVs (TF's own scan does; external log
    /// readers may not) — set `TIMEFUSION_USE_DELETION_VECTORS=false` to keep
    /// copy-on-write rewrites.
    #[serde_inline_default(true)]
    pub timefusion_use_deletion_vectors: bool,
    /// Commit DV merges append-tolerantly: a concurrent flush commit (AddFile
    /// only) no longer aborts the merge with ConcurrentAppend — the commit
    /// rebases instead of re-running the whole scan+join. Sound because the
    /// mem leg runs before every Delta leg, so rows flushed after the merge's
    /// snapshot already carry post-DML values;
    /// removed-file conflicts (optimize/vacuum) still abort and retry. That
    /// contract is per-process: it relies on the single-writer WAL flock
    /// (`WalDirLock`) — if the table ever gains a second concurrent writer
    /// whose flushes bypass this process's mem leg, set this to false or its
    /// rows can miss enrichment merges. On by default;
    /// `TIMEFUSION_DML_MERGE_APPEND_REBASE=false` restores strict OCC.
    #[serde_inline_default(true)]
    pub timefusion_dml_merge_append_rebase: bool,
    /// Push a `target.key IN (source key values)` filter into the DV merge's
    /// per-file scan so parquet bloom filters prune files/row-groups holding none
    /// of the source keys — turning a whole-window enrichment scan into a few-file
    /// scan. Sound (bloom never false-negatives); on by default. Kill-switch:
    /// `TIMEFUSION_DML_MERGE_KEY_PRUNE=false` reverts to scanning all window files.
    #[serde_inline_default(true)]
    pub timefusion_dml_merge_key_prune: bool,
}

impl MaintenanceConfig {
    fn selected(project_id: &str, projects: Option<&str>) -> bool {
        projects.is_none_or(|projects| projects.trim().is_empty() || projects.split(',').map(str::trim).any(|project| project == project_id))
    }

    /// Reads honour the canary allow-list; BUILDS run for every project,
    /// unconditionally, and there is no longer a switch for either.
    ///
    /// `rollup_build_enabled()` used to live here returning the global flag.
    /// With that flag gone the function could only ever return `true`, and a
    /// predicate that is always true is worse than no predicate — it reads at
    /// the call site like a decision is being made.
    ///
    /// The build side deliberately has no allow-list: a hidden list of project
    /// UUIDs is a debugging trap, because "why has this project no rollup" then
    /// has an answer living in an env var nobody remembers setting, and any
    /// project created after the list was written silently never gets built.
    pub fn rollup_read_enabled_for(&self, project_id: &str) -> bool {
        Self::selected(project_id, self.timefusion_rollup_read_projects.as_deref())
    }

    /// Flush escalation-sort pool in bytes. Floored so a misconfigured 0 can't
    /// build a zero-sized pool that fails every sort.
    pub fn flush_sort_pool_bytes(&self) -> usize {
        (self.timefusion_flush_sort_pool_mb.max(64) as usize).saturating_mul(1 << 20)
    }
}

/// Which DataFusion `MemoryPool` to back the runtime with.
///
/// - `FairSpill` (default): a spillable consumer may hold at most
///   `(pool − unspillable) / num_spill`, and an unspillable one takes from
///   what is left. That is the bound the query pool needs, because the sort
///   machinery's merge halves — `ExternalSorterMerge`, `SortPreservingMerge`,
///   `DedupExec[keep-greatest]` — CANNOT spill. Under `Greedy` a spillable
///   `ExternalSorter` grows instead of spilling until the pool is gone, and
///   the merge that follows it fails: prod 2026-09-02, one 16-partition sort
///   whose partitions held 5.9 GB and 7.3 GB of a 16 GB pool while
///   `ExternalSorterMerge[3]` could not get 331 MB. The process restarted.
///   Under FairSpill each of those sorters is capped at ~1 GB and spills.
/// - `Greedy`: one global cap, first-come first-served. Was the default from
///   `81dcc1cd` (2026-05-28), when FairSpill sliced ~30 concurrent INSERTs
///   into ~76 MB slots and every batch bounced with `Memory limit exceeded`.
///   That reason expired: the write path took its own FairSpill pool in
///   `flush_sort_runtime_env` (2026-08-20), and INSERTs reserve nothing from
///   this pool at all — measured, not inferred, by
///   `tests/suite/query_pool_insert_test.rs`.
///
/// Rollback is `TIMEFUSION_MEMORY_POOL=greedy`; it needs no redeploy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPoolKind {
    Greedy,
    #[default]
    FairSpill,
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    // Formerly `timefusion_memory_limit_gb` — now `derived.memory_limit_bytes()`.
    // Formerly `timefusion_maintenance_pool_gb` — now
    // `derived.maintenance_pool_bytes()` (see DerivedBudget, §4 of the
    // compaction redesign doc). The 07-20/21 starvation this knob fixed
    // (25 GB-box clamp vs a 188 GB box) is what the derivation replaces.
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default)]
    pub timefusion_memory_pool: MemoryPoolKind,
    #[serde_inline_default(true)]
    pub timefusion_tracing_record_metrics: bool,
    /// DataFusion `target_partitions` for query + maintenance sessions. 0 =
    /// auto: `config::apply()` derives it from the container's CPU quota
    /// (num_cpus ignores the CFS quota, oversubscribing throttled containers).
    /// A non-zero env (`TIMEFUSION_QUERY_PARTITIONS`) wins. 0 also when unset in
    /// tests → sessions keep DataFusion's default.
    #[serde_inline_default(0)]
    pub timefusion_query_partitions: usize,
    /// Admission guard for wide-window read scans. A query reaching further
    /// back than `timefusion_wide_scan_lookback_hours` (or with no lower time
    /// bound) opens hundreds of Parquet files whose row groups aren't
    /// page-pruned, so per-file decode buffers are large and untracked by the
    /// DataFusion memory pool — at full concurrency this OOM-restarted prod
    /// from a single 7-day dashboard. Wide scans are gated to
    /// `timefusion_max_concurrent_scan_readers` concurrent batch-decodes across
    /// all queries so they degrade to slower rather than take the process
    /// down; narrow recent-window scans keep full parallelism.
    // Wide-scan admission guard. 16 concurrent Parquet decoders bounds untracked
    // decode heap well under the pool on this box (48-way caused an OOM). Wide
    // observability windows are already latency-bound below this, so gating is
    // near-free. 2h lookback keeps the hot dashboards ungated (they page-prune to
    // tiny per-file bytes) while 3d+/no-time scans queue. Both tunable via
    // TIMEFUSION_{MAX_CONCURRENT_SCAN_READERS,WIDE_SCAN_LOOKBACK_HOURS}.
    #[serde_inline_default(16)]
    pub timefusion_max_concurrent_scan_readers: usize,
    #[serde_inline_default(2)]
    pub timefusion_wide_scan_lookback_hours: u64,
    /// Depth alone badly over-fires the gate above. Lookback is only a PROXY
    /// for decode heap, and once file pruning works the proxy breaks — a query
    /// reading one small file at a long lookback was queued behind a saturated
    /// gate for no reason. So a scan is gated only when it is deep AND actually
    /// selected real work: more than `..._max_files` files or `..._max_mb` of
    /// them, counted from the plan's file groups after pruning. The wide scan
    /// that caused the original OOM still selects hundreds of files and stays
    /// gated; a deep-but-pruned dashboard query no longer waits behind it.
    // Sized against the incident that motivated the gate: a 7-day dashboard
    // opened hundreds of files at ~48-way parallelism.
    //
    // The file-count half assumed a recent-window scan selects a handful of
    // files after pruning — false once partitions fragmented to thousands of
    // small files, at which point the release could never fire and every
    // dashboard past the 2h lookback queued behind the semaphore.
    //
    // BYTES are the honest proxy for decode heap; file COUNT is a proxy for a
    // proxy, and fragmentation is exactly what invalidates it. Median prod file
    // is ~0.1 MB, so 256 files is only ~26 MB — the MB cap does the real
    // bounding, and the 7-day case that motivated the gate blows through it regardless.
    #[serde_inline_default(256)]
    pub timefusion_wide_scan_max_files: usize,
    // Compressed parquet bytes understate transient Arrow decode heap by an
    // order of magnitude on OTel data — a 222 MB file measured ~4 GiB of process
    // growth decoding 48 row groups in parallel, so it must participate in the
    // shared decode gate rather than the small-scan exemption. 64 MB keeps
    // genuinely small, well-pruned history reads ungated.
    #[serde_inline_default(64)]
    pub timefusion_wide_scan_max_mb: u64,
    /// Largest isolated non-conforming Delta leg that `repair_isolated_scan_ordering`
    /// will sort at read time so the conforming majority keeps its `[timestamp DESC]`
    /// claim. **0 disables the repair.**
    ///
    /// A budget, not a heuristic: sorting a WHOLE-window parquet leg is the
    /// 2026-08-02 / 2026-08-07 OOM, and the only thing separating the two cases
    /// is size. Bounding it here makes the bad case structurally unreachable
    /// while the ordinary one (a handful of freshly-concatenated files among
    /// thousands of sorted ones) is repaired.
    ///
    /// COMPRESSED selected bytes, which understate the sort's Arrow heap by ~12x
    /// on OTel data — 64 MB here is ~0.8 GB decoded. Same number and the same
    /// reason as `timefusion_wide_scan_max_mb`; raise it only against a measured
    /// distribution of isolated-leg sizes, and remember the repair runs on EVERY
    /// Delta-reading query, so the ceiling is paid concurrently.
    /// **Raised 64 -> 256 on 2026-09-04, deliberately NOT further.** The doc
    /// above demands a measured distribution before raising this; the
    /// `ordering_repair_declined` warn now emits one. First measurement from
    /// prod: a declined leg of **803,375,479 bytes (766 MiB) against the 64 MiB
    /// budget** — 12x over.
    ///
    /// **That is the case NOT to admit.** At the ~12x compressed-to-Arrow ratio
    /// this field already documents, 766 MiB compressed is ~9 GB decoded, per
    /// query, concurrently — which is how the 16 GiB query pool was exhausted
    /// this morning. Raising the ceiling to cover the worst leg would trade a
    /// failing query for a failing process.
    ///
    /// **1024 MiB, chosen to COVER that leg rather than refuse it.** Refusing it
    /// is not a safe default, it is a broken query: monoscope's log explorer
    /// fails outright past ~2.5 days, and a database whose queries do not run is
    /// not being protected, it is being disabled.
    ///
    /// The reason this is now the right trade, and was not this morning: the
    /// query pool is `FairSpill`. A spillable sort that outgrows its slot
    /// **spills to disk** instead of consuming the pool and starving the
    /// unspillable merge behind it. So admitting a large leg degrades to a SLOW
    /// query, not a dead process — and slow beats failing.
    ///
    /// What it buys directly: with the leg admitted, the repair restores the
    /// union's ordering claim, `DedupExec` runs BOUNDED instead of falling back
    /// to its 2 GiB unordered keep-greatest state, and the query streams. The
    /// 2 GiB ceiling that produces the user-visible error is then never reached
    /// — it is a symptom of the declined repair, not an independent limit.
    ///
    /// Still bounded rather than unlimited: this is per query and paid
    /// concurrently. Draining the unsorted files
    /// (`timefusion_footer_repair_files_per_pass`) remains the real remedy;
    /// this makes the window usable while that happens.
    #[serde_inline_default(1024)]
    pub timefusion_read_sort_unordered_leg_max_mb: u64,
    /// Selected bytes above which a single scan is REFUSED outright, rather than admitted
    /// into the gate above. **0 disables it, and 0 is the default.**
    ///
    /// The gate bounds how many wide scans decode concurrently; it has never bounded how
    /// much any one of them decodes, and `wide_scan_oversize_total` was pure observation.
    /// That gap is a distinct failure mode from a slow query: on 2026-08-18 one scan
    /// selected 514 files / 32.8 GB and, while it ran, NEW CONNECTIONS TIMED OUT — the box
    /// became unreachable, so every other tenant paid for one query. Refusing it instead
    /// costs one client an error naming the limit and what to do about it.
    ///
    /// Cross-connection plan-cache capacity (unique canonical/shape templates).
    /// 256 thrashed in prod (evicting ~half every ~60s); 1024 holds the working
    /// set with room to spare. Each entry is one LogicalPlan (~KBs).
    #[serde_inline_default(2048)]
    pub timefusion_plan_cache_capacity: usize,
    /// Route `now()`/`current_timestamp` SELECTs through the shape cache (time
    /// fn parameterized to a fresh per-query instant) instead of bypassing it.
    /// On by default: prod CPU flamegraphs showed ~25% of CPU in
    /// SessionState::optimize from now()-bearing dashboard cache misses. The
    /// cached artifact is a placeholder plan template; the instant is re-bound
    /// per query, so windows never freeze. Set =false to disable in an emergency.
    #[serde_inline_default(true)]
    pub timefusion_plan_cache_time_fns: bool,
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde_inline_default("http://localhost:4317".to_string())]
    pub otel_exporter_otlp_endpoint: String,
    #[serde_inline_default("timefusion".to_string())]
    pub otel_service_name: String,
    #[serde_inline_default(env!("CARGO_PKG_VERSION").to_string())]
    pub otel_service_version: String,
    #[serde(default)]
    pub log_format: Option<String>,
    /// Standard OTel var; `none` disables span export (logs/metrics unaffected).
    #[serde(default)]
    pub otel_traces_exporter: Option<String>,
}

impl TelemetryConfig {
    pub fn is_json_logging(&self) -> bool {
        self.log_format.as_deref() == Some("json")
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>()).expect("Default config should always succeed with serde defaults")
    }
}

impl AppConfig {
    /// Effective WAL flush thresholds: env override wins, else the derived tree
    /// (this is the wiring that fixes the 6 GB-vs-24 GB threshold drift — the
    /// derived numbers must actually reach the WAL layer, not just the startup log).
    pub fn effective_wal_max_files(&self) -> usize {
        match self.buffer.wal_max_file_count() {
            0 => self.derived.wal_flush_file_threshold(),
            env => env,
        }
    }

    pub fn effective_wal_max_unflushed_bytes(&self) -> u64 {
        self.buffer.wal_max_unflushed_bytes().unwrap_or_else(|| self.derived.wal_flush_byte_threshold())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tantivy read path's tunables are load-bearing and easy to change by
    /// accident, so pin the *deserialized* values — which is what prod runs.
    ///
    /// This also guards a trap that has already cost a debugging cycle:
    /// `TantivyConfig::default()` is the DERIVED `Default`, so it returns zeros
    /// and `false` and bypasses every `#[serde_inline_default]` here. A test
    /// built on `Default::default()` therefore exercises a configuration
    /// production can never have (seeding off, 1-entry reader cache, no
    /// manifest TTL). Deserialize instead — as this test does.
    #[test]
    fn tantivy_defaults_are_the_deserialized_ones_not_the_derived_ones() {
        let cfg: TantivyConfig = serde_json::from_str("{}").expect("every field has a default");
        assert!(cfg.seed_cache_on_publish(), "a published index must be kept locally by default");
        assert_eq!(cfg.timefusion_tantivy_prefetch_days, 3, "the hot window must be warmed by default");
        assert_eq!(cfg.search_concurrency(), 32);
        assert_eq!(cfg.reader_cache_entries().get(), 2048);
        assert_eq!(cfg.manifest_ttl(), Duration::from_secs(300));
        // A COUNT ceiling against a pathological queue of tiny files, NOT the
        // pass bound — `timefusion_tantivy_backfill_max_bytes_per_pass` is. It
        // was 8, sized on `otel_logs_and_spans` where a build costs 4-5 minutes;
        // that is false by ~150x for the rollup tables (a pass logged `built=8`
        // in 14 seconds), and one count cap across populations differing by two
        // orders of magnitude throttled the cheap tables to protect the
        // expensive one. See the field's own comment.
        assert_eq!(cfg.timefusion_tantivy_backfill_max_files_per_pass, 320);

        let derived = TantivyConfig::default();
        assert!(!derived.seed_cache_on_publish(), "derived Default really does diverge — that is why this test exists");
        // The floors are what keep a derived-Default config merely wrong and
        // not deadlocked: zero concurrency would stall the per-index fan-out.
        assert_eq!(derived.search_concurrency(), 1);
        assert_eq!(derived.reader_cache_entries().get(), 1);
    }

    /// The tantivy drain's schedule is a correctness-adjacent default, not a
    /// tuning knob: at `0 20 * * * *` it fires at ONE instant per hour, and a
    /// box that restarts more often than hourly never reaches it. Measured
    /// 2026-08-22 — zero `tantivy_backfill_started` across six hours of prod
    /// logs, with every coverage finding that day sitting downstream of a drain
    /// that never ran.
    #[test]
    fn the_tantivy_drain_gets_more_than_one_chance_an_hour() {
        let cfg: MaintenanceConfig = serde_json::from_str("{}").expect("every field has a default");
        assert_eq!(cfg.timefusion_tantivy_reconcile_schedule, "0 */15 * * * *");
    }

    /// Rollups are ON with no configuration at all, and the read canary is the
    /// only thing left that can narrow them.
    ///
    /// Three `#[serde(default)]` bools used to gate builds, reads and the
    /// realtime tail — OFF unless the environment said otherwise, so a fresh
    /// deployment ran with rollups silently disabled and every wide query on the
    /// raw path. Prod had set all three to `true` for months, so they encoded no
    /// live decision; they only offered a way to be off by accident.
    #[test]
    fn rollups_need_no_configuration_and_only_the_read_canary_narrows_them() {
        let mut config: MaintenanceConfig = serde_json::from_str("{}").expect("every field has a default");
        // A DESERIALIZED default config — what prod actually runs — routes reads
        // for any project without anything being set.
        assert!(config.rollup_read_enabled_for("project-a"), "an unconfigured deployment must still route");
        assert!(config.rollup_read_enabled_for("a-project-created-tomorrow"));
        assert_eq!(config.timefusion_rollup_backfill_days, 31, "the shipped default is the value prod exercises");

        // The canary still narrows the READ side when it is set, and only then.
        config.timefusion_rollup_read_projects = Some("project-b".into());
        assert!(!config.rollup_read_enabled_for("project-a"));
        assert!(config.rollup_read_enabled_for("project-b"));
    }

    /// The tree budgets 100% of whatever limit it is given — correct for a
    /// dedicated box, but oversubscribes the host when TF shares one with other
    /// services entitled to their own share. Pinning the arithmetic so the
    /// "sums to the whole limit" property can't be changed unnoticed.
    #[test]
    fn cli_profile_hands_maintenance_the_cgroup() {
        // The whole point of the profile: an 8 GiB pod must derive multi-GiB
        // sort memory instead of the 1 GiB floor the server shape leaves it.
        let cli = DerivedBudget::from_limits_with_profile(8 * GIB, 4, BudgetProfile::MaintenanceCli);
        assert!(cli.maintenance_pool_bytes >= 6 * GIB, "8 GiB pod must yield >= 6 GiB maintenance pool, got {} GiB", cli.maintenance_pool_bytes / GIB);
        // Engines run one at a time in a CLI: each share claims ~the whole pool.
        assert!(cli.heavy_share_bytes() >= (cli.maintenance_pool_bytes as f64 * 0.8) as usize);
        assert_eq!(cli.heavy_share_bytes(), cli.light_share_bytes());
        assert_eq!(cli.maintenance_batch_size(), "256");
        // The server shape is untouched by the profile's existence.
        let server = DerivedBudget::from_limits(8 * GIB, 4);
        assert!(
            cli.heavy_share_bytes() >= 4 * server.heavy_share_bytes(),
            "the profile's whole purpose: heavy sort memory multiplies ({} GiB -> {} GiB)",
            server.heavy_share_bytes() / GIB,
            cli.heavy_share_bytes() / GIB
        );
        assert_eq!(server.maintenance_batch_size(), "2048");
        // Server: coordinator takes its slice first, then heavy/light divide the rest.
        assert_eq!(server.light_share_bytes(), server.maintenance_pool_bytes - server.coordinator_share_bytes() - server.heavy_share_bytes());
        assert_eq!(cli.coordinator_share_bytes(), 0, "the CLI drives engines directly; no coordinator competes for the pool");
    }

    #[test]
    fn budget_tree_allocates_the_entire_limit() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        let total = b.query_pool_bytes + b.ingest_buffer_bytes + b.foyer_memory_bytes + b.writer_reserve_bytes + b.maintenance_pool_bytes;
        // The tree must NOT hand out every byte: 15% stays unsanctioned for the
        // consumers no pool tracks (decode, parse ASTs, allocator overhead).
        // The old total==limit invariant let every subsystem be "legal" at its
        // cap while the sum still OOMed.
        assert_eq!(total, 120 * GIB - (120.0 * GIB as f64 * 0.15) as usize, "tracked consumers + 15% untracked slack == the limit");
        assert_eq!(b.query_pool_bytes, 24 * GIB);
        assert_eq!(b.ingest_buffer_bytes, 24 * GIB);
        assert_eq!(b.foyer_memory_bytes, 12 * GIB);
        // 120 - (24 query + 24 buffer + 12 foyer + 12 writer reserve + 18 slack).
        // A prior brake-headroom cut freed 6 GiB to the maintenance remainder.
        assert_eq!(b.maintenance_pool_bytes, 30 * GIB, "maintenance takes the remainder AFTER slack");
    }

    /// `TIMEFUSION_MEMORY_BUDGET_GB` exists so a shared host can size TF below
    /// its cgroup without an orchestrator change. It must scale the WHOLE tree
    /// from one input (the per-consumer knobs it replaces drifted out of
    /// proportion, which is why they were deleted) and must never raise the
    /// limit above what the cgroup actually allows.
    #[test]
    fn memory_budget_override_scales_whole_tree_and_only_lowers() {
        let full = DerivedBudget::from_limits(120 * GIB, 48);
        let capped = DerivedBudget::from_limits(80 * GIB, 48);
        assert_eq!(capped.query_pool_bytes, 16 * GIB);
        assert_eq!(capped.ingest_buffer_bytes, 16 * GIB);
        assert_eq!(capped.foyer_memory_bytes, 8 * GIB);
        assert!(capped.maintenance_pool_bytes < full.maintenance_pool_bytes, "maintenance shrinks with the rest, not at its expense");

        // The clamp: an over-large request can never budget past the cgroup.
        assert_eq!(effective_limit(80 * GIB, Some(200 * GIB)), 80 * GIB);
        assert_eq!(effective_limit(80 * GIB, Some(40 * GIB)), 40 * GIB);
        assert_eq!(effective_limit(80 * GIB, None), 80 * GIB);
    }

    // Regression: TIMEFUSION_S3_CONNECT_TIMEOUT=150 (unitless) panicked
    // object_store's Duration parse at boot. Bare numbers must coerce to
    // seconds; values with a unit pass through untouched.
    #[test]
    fn normalize_duration_coerces_bare_numbers_to_seconds() {
        assert_eq!(normalize_duration(Some("150"), "60s"), "150s");
        assert_eq!(normalize_duration(Some("150s"), "60s"), "150s");
        assert_eq!(normalize_duration(Some("3m"), "60s"), "3m");
        assert_eq!(normalize_duration(Some(""), "60s"), "", "an explicitly-empty value is passed through, not defaulted");
        assert_eq!(normalize_duration(None, "60s"), "60s");
        let aws = AwsConfig { timefusion_s3_connect_timeout: Some("150".into()), ..Default::default() };
        assert_eq!(aws.connect_timeout(), "150s");
    }

    /// The commit-log request class must default to a bound that is ORDERS OF
    /// MAGNITUDE under the data bound — that gap is the fix for a prior
    /// commit-lock stall, and a default that drifted up to match
    /// `request_timeout` would silently restore it.
    #[test]
    fn log_request_timeout_defaults_far_below_the_data_bound() {
        let aws = AwsConfig::default();
        assert_eq!(aws.log_request_timeout(), "30s");
        assert_eq!(aws.request_timeout(), "900s");
        let tuned = AwsConfig { timefusion_s3_log_request_timeout: Some("45".into()), ..Default::default() };
        assert_eq!(tuned.log_request_timeout(), "45s", "bare numbers coerce here too, or boot panics");
    }

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.core.pgwire_port, 5432);
        assert_eq!(config.buffer.timefusion_flush_interval_secs, 60);
        assert_eq!(config.buffer.timefusion_bucket_duration_secs, 300);
        // Unset WAL byte-threshold = derive (AppConfig::effective_wal_max_unflushed_bytes).
        assert_eq!(config.buffer.wal_max_unflushed_bytes(), None);
        assert_eq!(config.cache.timefusion_foyer_memory_mb, 1024);
        assert_eq!(config.cache.timefusion_foyer_disk_gb, 500);
        assert_eq!(config.cache.disk_size_bytes(), 500 * GIB);
        assert_eq!(config.cache.timefusion_warm_inline_max_mb, 0);
        assert_eq!(config.cache.timefusion_foyer_block_size_mb, 256);
        assert_eq!(config.cache.block_size_bytes(), 256 * MIB);
        assert_eq!(config.cache.timefusion_foyer_l1_max_entry_mb, 16);
        assert_eq!(config.cache.timefusion_cache_recent_days, 35);
        assert_eq!(config.memory.timefusion_wide_scan_max_mb, 64);
        assert!(config.maintenance.timefusion_warm_after_compaction);
        assert!(config.maintenance.timefusion_evict_after_compaction);
        // Merge-on-read DV is the default write path (and thus what all test
        // harnesses that build from AppConfig::default() exercise).
        assert!(config.maintenance.timefusion_use_deletion_vectors);
        assert!(!config.maintenance.timefusion_warm_full_files);
        assert_eq!(config.maintenance.timefusion_warm_recency_days, 35);
        assert_eq!(config.maintenance.timefusion_warm_concurrency, 16);
        // Durable-by-default WAL: an async fsync default let OOM-kills tear the
        // mmap tail and silently quarantine acked rows. Pin the durable defaults.
        assert_eq!(config.buffer.wal_fsync_mode(), WalFsyncMode::SyncEach);
        assert!(config.buffer.wal_ack_fsync());
        // Compression tiers ascend hot < warm < cold; intermediate (same-day
        // rewrites that nightly consolidate/recompress will rewrite anyway)
        // sits below hot and stays eligible for re-tiering.
        let p = &config.parquet;
        // TWO levels, working < sealed. The old four-level ladder is gone: see
        // the field docs for why an intermediate level that relies on a later
        // re-tier is unsafe when the re-tiering cron is gated off.
        assert_eq!(p.timefusion_zstd_compression_level, 3);
        assert!(p.timefusion_zstd_compression_level < p.timefusion_zstd_level_warm);
    }

    // Prod-shaped box (120 GiB / 48 cores, 11 hot projects): K lands in the
    // 8..=11 range (4 GiB per-sort spill threshold), heavy keeps >= 1/4. The
    // pool sum is pinned by `budget_tree_allocates_the_entire_limit` on the
    // same box.
    #[test]
    fn derived_budget_prod_box_120gib_48cores() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        assert!(b.heavy_share_bytes() as f64 >= b.maintenance_pool_bytes() as f64 * 0.25 - 1.0);
        let k = b.light_optimize_k(11);
        // The band shifted down by exactly one when the repair lane's sort
        // budget was reserved out of it (see `light_optimize_k`) — repair draws
        // on the same coordinator pool and was never counted here.
        assert!((3..=11).contains(&k), "K={k} outside the expected 3..=11 range");
        assert_eq!(
            k + b.repair_pool_holdback_slices(),
            b.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES,
            "exactly the repair lane's holdback is reserved out of light's share"
        );
        // Envelope (permits x per-sort budget) is the invariant, not the raw
        // permit count — a prior fan-in OOM was about that product, and
        // asserting count alone misses permits raised without paying for them.
        // Pinned at 20 GiB (10 x 2), below the level that OOM-killed prod and
        // near the level that ran clean.
        assert_eq!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES,
            20 * GIB,
            "changing the fan-in envelope must be deliberate: state the memory headroom that pays for it"
        );
        assert!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES < b.memory_limit_bytes() / 2,
            "the fan-in envelope must stay well under the cgroup, whatever the permit count"
        );
        // The cgroup is the wrong denominator here: heavy sorts run in
        // `heavy_share_bytes()` (~4.98 GiB on prod), not the cgroup, so 10 x 2 GiB
        // passes this check 2x over while over-committing the real pool 4x.
        //
        // What must actually hold: a FairSpillPool slice clears what a sort
        // cannot avoid allocating — one indivisible batch (2048-wide otel rows
        // ~150 MB) plus `ExternalSorterMerge`'s 32 MB unspillable floor. Below
        // that a unit fails outright instead of spilling. Stated as a per-sort
        // floor so raising permits is only safe if the share grows too.
        const WIDEST_BATCH_BYTES: usize = 150 * 1024 * 1024;
        const UNSPILLABLE_MERGE_FLOOR_BYTES: usize = 32 * 1024 * 1024;
        let per_sort_slice = b.heavy_share_bytes() / b.rewrite_permits();
        assert!(
            per_sort_slice >= WIDEST_BATCH_BYTES + UNSPILLABLE_MERGE_FLOOR_BYTES,
            "each of {} concurrent heavy sorts gets {} MB of the {} MB heavy share, below the {} MB a sort cannot spill below — it will fail rather than spill",
            b.rewrite_permits(),
            per_sort_slice / 1024 / 1024,
            b.heavy_share_bytes() / 1024 / 1024,
            (WIDEST_BATCH_BYTES + UNSPILLABLE_MERGE_FLOOR_BYTES) / 1024 / 1024,
        );
        assert_eq!(b.optimize_merge_tasks(), 2);
    }

    /// REPRODUCES the 2026-09-02 repair starvation: the repair rewrite budget is
    /// smaller than ONE target-sized file, so no repair unit can ever share it.
    ///
    /// A repair unit is exactly one file (`coordinator_compaction_files` returns
    /// `.take(1)` for Repair), so it cannot be split to fit. Its decoded cost is
    /// `file_size * DECODED_BYTES_PER_COMPRESSED`, and compaction deliberately
    /// produces `COORDINATOR_HOT_TARGET_BYTES`-sized files. When the budget is
    /// below that product, every unit's request clamps to the WHOLE semaphore
    /// and repair serializes to one 40-minute rewrite at a time.
    ///
    /// Prod 2026-09-02: `want_mib=1280 budget_mib=1280` logged 243 times in
    /// three hours, with 310 repair units stuck in Retry. 2,188 HAD completed —
    /// those are the files small enough to fit (under ~107 MiB compressed),
    /// which is why the lane looks alive while the target-sized work never runs.
    /// The byte-pricing change that introduced this budget intended "bins below
    /// the budget now share it"; at these constants no correctly-sized bin is
    /// ever below it.
    ///
    /// FAILS at 1,280 MiB against a 3,072 MiB requirement. Raising
    /// `repair_rewrite_budget_bytes` to `N * target * 12` lets N rewrites share.
    #[test]
    fn repair_budget_must_fit_one_target_sized_file() {
        let b = &AppConfig::default().derived;
        const TARGET_FILE_BYTES: usize = 256 * 1024 * 1024; // COORDINATOR_HOT_TARGET_BYTES
        const DECODED_PER_COMPRESSED: usize = 12; // database::maintain::DECODED_BYTES_PER_COMPRESSED
        let one_file_decoded = TARGET_FILE_BYTES * DECODED_PER_COMPRESSED;
        assert!(
            b.repair_rewrite_budget_bytes() >= one_file_decoded,
            "repair budget {} MiB cannot hold ONE target-sized file ({} MiB decoded = {} MiB x {}), \
             so every repair unit clamps to the whole semaphore and repair serializes",
            b.repair_rewrite_budget_bytes() / MIB,
            one_file_decoded / MIB,
            TARGET_FILE_BYTES / MIB,
            DECODED_PER_COMPRESSED,
        );
    }

    /// Certification must reach back far enough to serve a 30d query.
    ///
    /// `dedup_sweep` is the only caller of `record_certification`, scoped to
    /// `today - timefusion_dedup_lookback_days ..= today`, which is therefore a hard
    /// ceiling on the longest window that can ever route to a rollup. At a
    /// former default of 1, nothing past yesterday could certify (30d queries
    /// timed out). Dropping below 30 silently reintroduces that.
    #[test]
    fn certification_window_covers_a_thirty_day_query() {
        assert!(
            AppConfig::default().maintenance.timefusion_dedup_lookback_days >= 30,
            "the dedup sweep is what certifies partitions; below 30d no 30d query can ever route to a rollup"
        );
    }

    /// Maintenance must not be serialized on a box with room to spare.
    ///
    /// A hard-coded 1 here once let the maintenance queue grow unbounded with
    /// zero dedup commits — and thus zero certifications, `DedupExec` in every
    /// plan, and zero rollup hits. A regression silently turns 30d queries back
    /// into timeouts, so assert the prod shape explicitly.
    #[test]
    fn coordinator_jobs_scale_with_the_box() {
        // Only meaningful when the operator has not pinned the override.
        if std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").is_ok() {
            return;
        }
        // Prod: 80 GiB / 48 cores.
        let prod = DerivedBudget::from_limits(80 * GIB, 48);
        assert!(prod.coordinator_jobs() > 1, "prod-shaped box must run maintenance in parallel, got {}", prod.coordinator_jobs());
        // Every admitted unit reserves at most MAX_DECODED_BYTES, so concurrent
        // decode reservation must still fit the maintenance pool.
        assert!(prod.coordinator_jobs() * 512 * 1024 * 1024 <= prod.maintenance_pool_bytes(), "concurrent 512 MiB units must fit the maintenance pool");
        // Small boxes stay modest rather than thrashing. Assert the invariant
        // ("doesn't thrash, still fits its pool"), not a specific job count
        // that would need editing every time the divisor moves.
        let small = DerivedBudget::from_limits(16 * GIB, 4);
        assert!(small.coordinator_jobs() <= 2, "a 4-core box must not run maintenance wide, got {}", small.coordinator_jobs());
        assert!(small.coordinator_jobs() * 512 * 1024 * 1024 <= small.maintenance_pool_bytes(), "concurrent units must fit a small box's pool too");
    }

    /// The coordinator pool must scale with the jobs sharing it, and the three
    /// maintenance shares must still sum to the pool.
    ///
    /// Once pinned at one `MAX_DECODED_BYTES` while jobs went to 16, FairSpill
    /// handed each consumer ~32 MB — right at `ExternalSorterMerge`'s
    /// allocation floor — and units failed outright instead of spilling.
    #[test]
    fn coordinator_pool_scales_with_its_jobs_without_overcommitting() {
        if std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").is_ok() {
            return;
        }
        for (limit_gb, cores) in [(80, 48), (16, 4), (8, 4)] {
            let b = DerivedBudget::from_limits(limit_gb * GIB, cores);
            let per_job = b.coordinator_share_bytes() / b.coordinator_jobs();
            assert!(
                per_job >= 32 * 1024 * 1024,
                "{limit_gb} GiB/{cores}-core: each of {} jobs gets {} MB, below the 32 MB sort floor",
                b.coordinator_jobs(),
                per_job / (1024 * 1024)
            );
            assert_eq!(
                b.coordinator_share_bytes() + b.heavy_share_bytes() + b.light_share_bytes(),
                b.maintenance_pool_bytes(),
                "{limit_gb} GiB/{cores}-core: maintenance shares must partition the pool, not overcommit it"
            );
        }
    }

    /// The hot-packing permit must be priced against the pool its units allocate
    /// from, or it moves whenever an unrelated share does. Prod 2026-09-01:
    /// raising the coordinator's cap shrank the light share, which silently took
    /// K from 3 to 1 and stopped HotPacking being claimed at all.
    #[test]
    fn the_packing_permit_follows_the_coordinator_pool_not_the_light_share() {
        let prod = DerivedBudget::from_limits(80 * GIB, 48);
        assert_eq!(
            prod.light_optimize_k(11),
            prod.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES - prod.repair_pool_holdback_slices(),
            "the memory term is the coordinator's pool, less the pool repair's decoded budget actually needs"
        );
        assert!(prod.light_optimize_k(11) > 1, "one permit shared by HotPacking and SealedConsolidation starves packing");
        // The bench's measured optimum: k hygiene permits + 1 repair = 6
        // concurrent rewrites, one rung below the 8-worker cliff.
        assert_eq!(
            prod.light_optimize_k(11) + prod.repair_pool_holdback_slices(),
            6,
            "the fleet must run at the measured optimum, not one rung either side — the light/repair SPLIT may move, the total may not"
        );
        // THE INVARIANT the decoupling exists to hold: repair's decoded budget
        // must fit the pool its holdback reserves, at the ratio the bench
        // measured. Violating it is what the old hand-set holdback of 2 did —
        // 6,144 MiB decoded against 2,560 MiB of pool is 2.4x, past the 2.39x
        // rung that FAILED.
        let holdback_pool_bytes = prod.repair_pool_holdback_slices() * COORDINATOR_PER_SORT_BUDGET_BYTES;
        assert!(
            prod.repair_rewrite_budget_bytes() as f64 <= holdback_pool_bytes as f64 * SAFE_DECODED_PER_POOL_BYTE,
            "repair may not be admitted more decoded bytes ({} MiB) than its pool holdback ({} MiB at {}x) can carry",
            prod.repair_rewrite_budget_bytes() / MIB,
            holdback_pool_bytes / MIB,
            SAFE_DECODED_PER_POOL_BYTE,
        );
        assert!(prod.light_optimize_k(11) < prod.cores / 4, "and the CPU term is not what binds on a big box");
    }

    // Small box (16 GiB / 4 cores): degrades to K=1, nothing underflows/zeroes.
    #[test]
    fn derived_budget_small_box_degrades_to_k1() {
        let tiny = DerivedBudget::from_limits(8 * GIB, 4);
        let tiny_sum =
            tiny.query_pool_bytes() + tiny.buffer_max_bytes() + tiny.foyer_memory_bytes() + tiny.writer_reserve_bytes() + tiny.maintenance_pool_bytes();
        assert!(tiny_sum <= 8 * GIB, "8 GiB box over-committed: {tiny_sum}");
        let b = DerivedBudget::from_limits(16 * GIB, 4);
        // cores/4 = 1 pins it here whatever the memory term says.
        assert_eq!(b.light_optimize_k(11), 1);
        assert!(b.maintenance_pool_bytes() >= GIB);
        assert!(b.light_share_bytes() > 0);
        assert!(b.heavy_share_bytes() > 0);
        assert!(b.tick_budget(Duration::from_secs(300)) < Duration::from_secs(300));
        assert!(b.memory_brake_limit_bytes() < b.memory_limit_bytes());
    }

    #[test]
    fn tick_budget_is_80pct_of_cron_period() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        assert_eq!(b.tick_budget(Duration::from_secs(300)), Duration::from_secs(240));
    }

    /// The brake must stay well clear of the cgroup the OOM killer watches, and
    /// the budgeted limit is NOT that cgroup — prod budgets 82 GiB inside a
    /// 96 GiB container. Pinned because raising the fraction without
    /// re-checking that gap has produced OOM kills before.
    #[test]
    fn memory_brake_leaves_real_headroom_under_the_cgroup() {
        let b = DerivedBudget::from_limits(100 * GIB, 48);
        assert_eq!(b.memory_brake_limit_bytes(), 80 * GIB);

        // Prod's shape: 82 GiB budgeted in a 96 GiB cgroup.
        let prod = DerivedBudget::from_limits(82 * GIB, 48);
        let cgroup = 96 * GIB;
        assert!(prod.memory_brake_limit_bytes() < cgroup * 7 / 10, "the brake must stay under 70% of the CGROUP, not just of the budget");
        assert!(cgroup - prod.memory_brake_limit_bytes() >= 25 * GIB, "at least 25 GiB must remain between the brake and the OOM killer");
    }

    // cgroup parsers never panic on "max", garbage, or empty content.
    #[test]
    fn cgroup_parsers_handle_max_and_garbage_without_panicking() {
        assert_eq!(parse_cgroup_v2_memory_max("max\n"), None);
        assert_eq!(parse_cgroup_v2_memory_max("134217728\n"), Some(134217728));
        assert_eq!(parse_cgroup_v2_memory_max("not a number"), None);
        assert_eq!(parse_cgroup_v2_memory_max(""), None);

        assert_eq!(parse_cgroup_v1_memory_limit("9223372036854771712\n"), None); // v1 "unlimited" sentinel
        assert_eq!(parse_cgroup_v1_memory_limit("134217728"), Some(134217728));
        assert_eq!(parse_cgroup_v1_memory_limit("garbage"), None);

        assert_eq!(parse_meminfo_total_bytes("MemTotal:       16384000 kB\nMemFree: 100 kB\n"), Some(16384000 * 1024));
        assert_eq!(parse_meminfo_total_bytes("garbage\nmore garbage"), None);
        assert_eq!(parse_meminfo_total_bytes(""), None);

        assert_eq!(parse_cgroup_cpu_max("400000 100000\n"), Some(4));
        assert_eq!(parse_cgroup_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cgroup_cpu_max("50000 100000"), Some(1)); // 0.5 → 1 (rounds up)
        assert_eq!(parse_cgroup_cpu_max("150000 100000"), Some(2)); // 1.5 → 2
        assert_eq!(parse_cgroup_cpu_max("garbage"), None);
        assert_eq!(parse_cgroup_cpu_max(""), None);
    }

    #[test]
    fn test_buffer_min_enforcement() {
        let mut config = AppConfig::default();
        config.buffer.timefusion_buffer_max_memory_mb = 10;
        assert_eq!(config.buffer.max_memory_mb(), 64);
    }

    #[test]
    fn test_cache_size_calculations() {
        let mut config = AppConfig::default();
        config.cache.timefusion_foyer_memory_mb = 256;
        config.cache.timefusion_foyer_disk_mb = Some(1024);
        assert_eq!(config.cache.memory_size_bytes(), 256 * MIB);
        assert_eq!(config.cache.disk_size_bytes(), GIB);
    }

    #[test]
    fn wal_backlog_limit_derives_from_the_tree_and_allows_override() {
        let mut config = AppConfig::default();
        // Unset: the effective threshold comes from the derived tree, and the
        // startup log's number IS the enforced number (a past drift had the
        // thresholds and the tree disagreeing).
        assert_eq!(config.effective_wal_max_unflushed_bytes(), config.derived.wal_flush_byte_threshold());
        assert_eq!(config.effective_wal_max_files(), config.derived.wal_flush_file_threshold());

        // Env override still wins.
        config.buffer.timefusion_wal_max_unflushed_mb = 12_000;
        config.buffer.timefusion_wal_max_file_count = 300;
        assert_eq!(config.effective_wal_max_unflushed_bytes(), 12_000 * MIB as u64);
        assert_eq!(config.effective_wal_max_files(), 300);
    }

    /// The lookback IS the suspect-set size (admission offers every unverified
    /// sealed file), so it is bounded on both sides: too small leaves a hole
    /// where one footer-less file pins every wide query at `full-set` dedup
    /// forever; too large spends the pass clearing correctly-sorted files
    /// instead of rewriting.
    #[test]
    fn repair_lookback_covers_the_query_window_without_flooding() {
        let d = AppConfig::default().maintenance.timefusion_light_optimize_repair_days;
        assert!(d >= 30, "must cover the 30-day window users actually query, got {d}");
        assert!(d <= 45, "must not balloon the suspect set beyond the query window, got {d}");
    }
}

// ===== autotune =====
// Host-aware auto-tuning of memory/disk/parallelism knobs.
//
// Applied in `init_config()` after env-var deserialization but before the
// `OnceLock` is sealed. Each knob is only overridden when the corresponding
// env var is **not** set — explicit user input always wins.
//
// Budget invariant on a fresh host with no overrides:
//     query_pool  ≈ 30% RAM
//     mem_buffer  ≈ 25% RAM
//     foyer_mem   ≈ 15% RAM
//     foyer_meta  ≤ 2% RAM (capped at 512MB)
//     ─────────────────────
//     reserved    ≈ 72% RAM, leaving headroom for Arrow scratch, walrus
//                  mmaps, tantivy, OS page cache.
//
// The remaining ~28% is not spare — it's the only budget parquet decode has
// (explicitly unpooled), plus walrus mmaps and allocator slack. See
// [`budget_audit`], which sums what the process actually commits (including
// the maintenance pool and MemBuffer's 120% admission ceiling) and warns when
// the remainder gets thin.
//
// Disk budget: foyer caches take up to 40% of free space on the data dir,
// capped at 500GB to avoid runaway on very large volumes.
//
// Logged once at startup so ops can see exactly what was chosen.

use sysinfo::Disks;
use tracing::{info, warn};

const MB: usize = 1024 * 1024;
const GB: usize = 1024 * MB;

const RAM_FRACTION_FOYER_META: f64 = 0.02;
const DISK_FRACTION_FOYER: f64 = 0.40;
const DISK_FRACTION_FOYER_META: f64 = 0.02;

/// Warn when the final (post-override) sum of memory reservations exceeds this
/// share of detected RAM — the counterpart of the ≈72% budget above.
///
/// Was 85%, which is *not* the counterpart of a 72% design target: it leaves
/// only 15% for everything no budget tracks. Prod's real commitment audits at
/// 83.5% of its 85 GiB container — passing the old line — while being
/// OOM-killed four times in nine hours, because parquet decode alone
/// (explicitly unpooled) can exceed a 12 GB remainder. 75% keeps a little
/// tolerance over the documented 72% target while still flagging that config.
/// Small hosts can exceed this via the 1 GiB maintenance-pool floor plus the
/// df-metadata cache; that is a truthful warning, and it stays WARN-only.
const OVERSUB_WARN_PCT: usize = 75;

const MIN_BUFFER_MB: usize = 256;
const MIN_FOYER_MEM_MB: usize = 128;
// 16 GiB: the derived tree reserves 10% of the limit for foyer (12 GiB on a
// 120 GiB box); an 8 GiB cap would strand a third of that reservation.
const MAX_FOYER_MEM_MB: usize = 16 * 1024;
const MIN_FOYER_META_MB: usize = 64;
const MAX_FOYER_META_MB: usize = 512;
const MIN_FOYER_DISK_GB: usize = 1;
const MAX_FOYER_DISK_GB: usize = 500;
const MAX_FOYER_META_DISK_GB: usize = 5;

/// Apply host-aware overrides to `config`. Knobs whose env var is set by the
/// user are left untouched.
pub fn apply(config: &mut AppConfig) {
    // ONE memory source: the derived budget tree's cgroup-clamped detection. A
    // second (sysinfo) reading here once let the oversubscription audit warn
    // against a different denominator than the budgets were derived from.
    let total_ram_mb = config.derived.memory_limit_bytes() / MB;

    let cpus = crate::config::detect_cores();

    // Probe free space on the data dir's mount point. Falls back to "unknown"
    // (no disk-derived overrides) if the mount can't be located.
    let data_dir = &config.core.timefusion_data_dir;
    let available_disk_gb = available_disk_for(data_dir);

    info!(
        "Auto-tune host detection: ram={}GB, cpus={}, data_dir={:?}, available_disk={}",
        total_ram_mb / 1024,
        cpus,
        data_dir,
        available_disk_gb.map_or_else(|| "unknown".to_string(), |g| format!("{g}GB"))
    );

    // Imperative by necessity: each knob is a `&mut` into a different config
    // field, so the env-unset-wins / changed / record triad can only be shared
    // via a closure taking the slot by reference.
    let mut applied = Vec::new();
    let mut tune = |name: &'static str, slot: &mut usize, derived: usize, unit: &str| {
        if std::env::var(name).is_err() && *slot != derived {
            *slot = derived;
            applied.push(format!("{name}={derived}{unit}"));
        }
    };

    // MemBuffer and foyer memory come from DerivedBudget — ONE set of RAM
    // fractions (see the config.rs budget tree); autotune only applies them.
    tune(
        "TIMEFUSION_BUFFER_MAX_MEMORY_MB",
        &mut config.buffer.timefusion_buffer_max_memory_mb,
        (config.derived.buffer_max_bytes() / MB).max(MIN_BUFFER_MB),
        "MB",
    );
    tune(
        "TIMEFUSION_FOYER_MEMORY_MB",
        &mut config.cache.timefusion_foyer_memory_mb,
        (config.derived.object_cache_memory_bytes() / MB).clamp(MIN_FOYER_MEM_MB, MAX_FOYER_MEM_MB),
        "MB",
    );
    tune(
        "TIMEFUSION_FOYER_METADATA_MEMORY_MB",
        &mut config.cache.timefusion_foyer_metadata_memory_mb,
        ((total_ram_mb as f64 * RAM_FRACTION_FOYER_META) as usize).clamp(MIN_FOYER_META_MB, MAX_FOYER_META_MB),
        "MB",
    );
    if let Some(avail_gb) = available_disk_gb {
        let disk_share = |fraction: f64, max| ((avail_gb as f64 * fraction) as usize).clamp(MIN_FOYER_DISK_GB, max);
        tune("TIMEFUSION_FOYER_DISK_GB", &mut config.cache.timefusion_foyer_disk_gb, disk_share(DISK_FRACTION_FOYER, MAX_FOYER_DISK_GB), "GB");
        tune(
            "TIMEFUSION_FOYER_METADATA_DISK_GB",
            &mut config.cache.timefusion_foyer_metadata_disk_gb,
            disk_share(DISK_FRACTION_FOYER_META, MAX_FOYER_META_DISK_GB),
            "GB",
        );
    }
    tune("TIMEFUSION_FLUSH_PARALLELISM", &mut config.buffer.timefusion_flush_parallelism, (cpus / 2).max(2), "");
    // Query/maintenance target_partitions. DataFusion defaults to
    // `num_cpus::get()`, which reads `sched_getaffinity` — honors cpuset
    // pinning but not the CFS quota (`docker --cpus`), so a throttled container
    // would oversubscribe by splitting even small files into too many scan
    // groups. `detect_cores` derives from the cgroup quota instead.
    //
    // CAPPED at QUERY_PARTITIONS_MAX, which is a MEMORY bound, not a CPU one.
    // Sort machinery reserves per partition and the merge halves cannot spill,
    // so the peak scales with `target_partitions`, not with how much work there
    // is. On the 48-core prod box that is what killed queries repeatedly:
    //
    //   Resources exhausted: Additional allocation failed for
    //   ExternalSorterMerge[18] … 4 x ExternalSorterMerge (can spill: false)
    //   holding 666.7 MB each … greedy(used: 16.0 GB, pool_size: 16.0 GB)
    //
    // (2026-08-31 ExternalSorterMerge; 2026-08-30 TopK and
    // SortPreservingMergeExec; 2026-08-28 "Not enough memory to continue
    // external sort" — plus the RUM/containers `row_number() OVER (PARTITION
    // BY … ORDER BY timestamp DESC)` queries, which surfaced as
    // HasqlException/XX000 in monoscope.) The window functions and
    // `ORDER BY timestamp LIMIT n` TopKs those pages run are all sort-bound, so
    // their unspillable reservation falls ~3x at 16 partitions while the scan
    // still parallelises 16 ways. `sort_spill_reservation_bytes` only pads
    // ExternalSorter's own merge and does nothing for TopK/SPM, which is why
    // the partition count is the lever and that knob was only a stopgap.
    //
    // Trade-off, stated plainly: this costs large-scan CPU parallelism above 16
    // cores to buy sort-memory headroom. 16 is a starting point, not a law —
    // `TIMEFUSION_QUERY_PARTITIONS` still overrides, so tuning and rollback are
    // env-only and need no redeploy.
    tune("TIMEFUSION_QUERY_PARTITIONS", &mut config.memory.timefusion_query_partitions, cpus.min(QUERY_PARTITIONS_MAX), "");

    if applied.is_empty() {
        info!("Auto-tune: no overrides applied (user has set all knobs explicitly or host signals unavailable)");
    } else {
        info!("Auto-tune applied: {}", applied.join(", "));
    }

    // Coherence guard: user-pinned envs can oversubscribe RAM even though the
    // auto-derived split respects the ≈72% invariant by construction. Check the
    // FINAL post-override sum and warn loudly — operators keep authority, but
    // the failure mode becomes visible instead of an OOM.
    //
    // RECLAIM, don't merely warn. The tree hands maintenance the remainder
    // after `query + ingest + foyer + writer + untracked_slack`, but the audit
    // counts three ceilings `reserved` never included: MemBuffer's 120%
    // admission hard limit (not the nominal ingest fraction), the tantivy
    // writer peak, and the DataFusion metadata cache — silently taken out of
    // maintenance's share. That was harmless only while maintenance was too
    // broken to claim its pool; once it started completing builds again the box
    // OOM-killed repeatedly. A warning nobody is awake to read is not a budget,
    // so take the overage back from the residual claimant that absorbed it.
    let mut audit = budget_audit(config, total_ram_mb);
    if audit.oversubscribed() {
        let overage = audit.committed_mb.saturating_sub(audit.warn_at_mb) * MB;
        let reclaimed = config.derived.reclaim_maintenance_pool(overage) / MB;
        audit = budget_audit(config, total_ram_mb);
        warn!(
            "bootstrap.phase=budget_reclaim reclaimed_mb={reclaimed} from the maintenance pool              (it is the residual claimant, so it is what absorbed the unreserved MemBuffer overshoot,              tantivy peak and metadata cache) — maintenance_pool now {}mb, committed {}mb vs warn_at {}mb",
            audit.maintenance_pool_mb, audit.committed_mb, audit.warn_at_mb
        );
    }
    let _ = BOOT_AUDIT.set(audit);
    // Always emit the breakdown, "we fit" included: the slack figure is the
    // operator's only view of how much room the untracked consumers have.
    let slack_mb = audit.slack_mb();
    let BudgetAudit { committed_mb, warn_at_mb, query_pool_mb, mem_buffer_hard_mb, maintenance_pool_mb, foyer_mb, tantivy_peak_mb, df_metadata_cache_mb } =
        audit;
    let msg = format!(
        "bootstrap.phase=budget_audit committed_mb={committed_mb} warn_at_mb={warn_at_mb} slack_mb={slack_mb} \
         (query_pool={query_pool_mb} mem_buffer_hard={mem_buffer_hard_mb} maintenance_pool={maintenance_pool_mb} foyer={foyer_mb} \
         tantivy_peak={tantivy_peak_mb} df_metadata_cache={df_metadata_cache_mb}) ram_mb={total_ram_mb} — \
         slack absorbs UNTRACKED allocation (parquet decode, walrus mmaps, tantivy, allocator overhead); one wide scan can \
         exceed a small slack, which is how a box gets OOM-killed while every individual budget looks fine"
    );
    if audit.oversubscribed() {
        warn!("{msg} — OVERSUBSCRIBED, expect OOM kills under load; lower one of these knobs");
    } else {
        info!("{msg}");
    }
}

/// Every budget this process commits to, in MB. Sums to what the process can
/// allocate *before* any untracked allocation (parquet decode, walrus mmaps,
/// tantivy, jemalloc slack) — so `committed` well under the limit is the point,
/// not `committed` merely fitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetAudit {
    pub query_pool_mb: usize,
    pub mem_buffer_hard_mb: usize,
    pub maintenance_pool_mb: usize,
    pub foyer_mb: usize,
    pub tantivy_peak_mb: usize,
    pub df_metadata_cache_mb: usize,
    pub committed_mb: usize,
    pub warn_at_mb: usize,
}

impl BudgetAudit {
    pub const fn oversubscribed(&self) -> bool {
        self.committed_mb > self.warn_at_mb
    }

    pub const fn slack_mb(&self) -> usize {
        self.warn_at_mb.saturating_sub(self.committed_mb)
    }
}

/// The audit computed at startup, so `timefusion_stats` can report it without
/// re-detecting RAM — a startup log line rotates away, but an operator
/// diagnosing an OOM needs the committed-vs-slack split on demand.
static BOOT_AUDIT: std::sync::OnceLock<BudgetAudit> = std::sync::OnceLock::new();

pub fn boot_budget_audit() -> Option<&'static BudgetAudit> {
    BOOT_AUDIT.get()
}

/// Sum the committed budgets against RAM.
///
/// A previous version undercounted by ~30GB and passed while the container
/// OOM-killed repeatedly, from four independent gaps:
///
/// 1. **The maintenance pool was omitted entirely.**
/// 2. **MemBuffer was counted at nominal**, not its 120% admission hard
///    ceiling (`HARD_LIMIT_HEADROOM_DIVISOR` in `buffered_write_layer`).
/// 3. **`memory_fraction` was ignored**: the query pool is
///    `limit × fraction` (see `Database::shared_runtime_env`), not `limit`.
/// 4. **The DataFusion metadata cache was omitted.**
///
/// The light-optimize slice is deliberately NOT added: it is carved *out of*
/// `maintenance_pool_bytes()`, so counting it again would double-count.
pub fn budget_audit(config: &AppConfig, total_ram_mb: usize) -> BudgetAudit {
    let foyer_mb = if config.cache.is_disabled() { 0 } else { (config.cache.memory_size_bytes() + config.cache.metadata_memory_size_bytes()) / MB };
    // Peak tantivy writer heap: one writer per in-flight flush.
    let tantivy_peak_mb =
        if config.tantivy.indexed_tables().is_empty() { 0 } else { crate::tantivy::WRITER_HEAP_BYTES * config.buffer.flush_parallelism() / MB };
    // Mirror `BufferedWriteLayer::max_memory_bytes`: the configured knob is
    // reduced by foyer + tantivy (which are counted separately below, so using
    // the raw knob here would double-count them), then admission runs to a 120%
    // hard ceiling (HARD_LIMIT_HEADROOM_DIVISOR).
    let mem_buffer_hard_mb = config.buffer.max_memory_mb().saturating_sub(foyer_mb + tantivy_peak_mb).max(64) * 6 / 5;
    let query_pool_mb = config.derived.query_pool_bytes() / MB;
    let maintenance_pool_mb = config.derived.maintenance_pool_bytes() / MB;
    let df_metadata_cache_mb = config.cache.timefusion_df_metadata_cache_mb;
    BudgetAudit {
        query_pool_mb,
        mem_buffer_hard_mb,
        maintenance_pool_mb,
        foyer_mb,
        tantivy_peak_mb,
        df_metadata_cache_mb,
        committed_mb: query_pool_mb + mem_buffer_hard_mb + maintenance_pool_mb + foyer_mb + tantivy_peak_mb + df_metadata_cache_mb,
        warn_at_mb: total_ram_mb * OVERSUB_WARN_PCT / 100,
    }
}

/// Return free space (GB) on the volume hosting `path`. Returns None if no
/// disk in the sysinfo enumeration covers the path — defensive: we'd rather
/// skip the override than guess wrong.
fn available_disk_for(path: &std::path::Path) -> Option<usize> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    // Pick the disk whose mount_point is the longest prefix of our path.
    Disks::new_with_refreshed_list()
        .iter()
        .filter(|d| canonical.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| (d.available_space() / GB as u64) as usize)
}

#[cfg(test)]
mod autotune_tests {
    use super::*;

    #[test]
    fn apply_is_idempotent_and_respects_overrides() {
        // SAFETY: no #[serial] needed — only reads env, and these vars aren't
        // set in the test process (autotune will fire).
        let mut cfg = AppConfig::default();
        apply(&mut cfg);
        // Only assert non-decrease relative to the floor; on tiny CI runners
        // the floor wins, which is fine.
        assert!(cfg.buffer.timefusion_buffer_max_memory_mb >= MIN_BUFFER_MB);
        let before = cfg.buffer.timefusion_buffer_max_memory_mb;
        apply(&mut cfg);
        assert_eq!(cfg.buffer.timefusion_buffer_max_memory_mb, before);
    }

    /// The prod config that OOM-killed repeatedly must audit as oversubscribed.
    /// A previous check passed it by omitting the maintenance pool, counting
    /// MemBuffer at nominal instead of its 120% admission ceiling, ignoring
    /// `memory_fraction`, and omitting the DataFusion metadata cache. Query +
    /// maintenance pools now come from the derived budget.
    #[test]
    fn budget_audit_flags_an_oversubscribed_config() {
        let mut cfg = AppConfig::default();
        cfg.buffer.timefusion_buffer_max_memory_mb = 24000;
        cfg.cache.timefusion_foyer_memory_mb = 4048;
        cfg.cache.timefusion_foyer_metadata_memory_mb = 512;

        // The container limit, not the 188GB host: this is what the kernel kills on.
        let a = budget_audit(&cfg, 24 * 1024);
        assert_eq!(a.query_pool_mb, cfg.derived.query_pool_bytes() / MB);
        assert_eq!(a.maintenance_pool_mb, cfg.derived.maintenance_pool_bytes() / MB, "was missing entirely");
        assert_eq!(a.foyer_mb, 4560);
        // MemBuffer's ceiling is on its EFFECTIVE budget (knob − foyer −
        // tantivy peak), then x1.2 — not the raw knob, which would count foyer
        // twice since it is summed separately.
        let effective = 24000 - a.foyer_mb - a.tantivy_peak_mb;
        assert_eq!(a.mem_buffer_hard_mb, effective * 6 / 5);
        assert_eq!(a.committed_mb, a.query_pool_mb + a.mem_buffer_hard_mb + a.maintenance_pool_mb + a.foyer_mb + a.tantivy_peak_mb + a.df_metadata_cache_mb);
        assert!(a.oversubscribed(), "a 24GB MemBuffer in a 24GiB container must be flagged: {a:?}");
    }

    /// An oversubscribed budget must be RECLAIMED, not merely warned about.
    ///
    /// The tree hands maintenance the remainder after query + ingest + foyer +
    /// writer + untracked_slack, but the audit counts three ceilings `reserved`
    /// never included: MemBuffer's 120% admission hard limit (not the nominal
    /// ingest fraction), the tantivy writer peak, and the DataFusion metadata
    /// cache — taken silently out of maintenance's share. Harmless only while
    /// maintenance was too broken to claim its pool; once builds started
    /// completing again it OOM-killed repeatedly. A warning nobody is awake to
    /// read is not a budget.
    #[test]
    fn an_oversubscribed_budget_is_reclaimed_from_the_residual_pool() {
        let mut cfg = AppConfig::default();
        cfg.buffer.timefusion_buffer_max_memory_mb = 24000;
        let ram_mb = 24 * 1024;
        let before = budget_audit(&cfg, ram_mb);
        assert!(before.oversubscribed(), "fixture must start oversubscribed: {before:?}");

        let overage = before.committed_mb.saturating_sub(before.warn_at_mb);
        let reclaimed = cfg.derived.reclaim_maintenance_pool(overage * MB) / MB;
        let after = budget_audit(&cfg, ram_mb);

        assert!(reclaimed > 0, "something must actually be surrendered");
        assert_eq!(after.maintenance_pool_mb, before.maintenance_pool_mb - reclaimed, "the reclaim comes out of maintenance");
        assert_eq!(after.committed_mb, before.committed_mb - reclaimed, "and therefore out of committed");
        // Every other budget is untouched — maintenance is the residual
        // claimant, so it is the one that absorbed the unreserved ceilings.
        assert_eq!((after.query_pool_mb, after.mem_buffer_hard_mb, after.foyer_mb), (before.query_pool_mb, before.mem_buffer_hard_mb, before.foyer_mb));

        // A pool that would go below the floor stops there rather than going
        // negative — a 1 GB maintenance pool still sorts and spills.
        let floored = cfg.derived.reclaim_maintenance_pool(usize::MAX);
        assert!(cfg.derived.maintenance_pool_bytes() >= 1024 * 1024 * 1024, "must not reclaim below the floor");
        assert!(floored <= after.maintenance_pool_mb * MB);
    }

    #[test]
    fn budget_audit_passes_a_config_with_real_slack() {
        let mut cfg = AppConfig::default();
        cfg.buffer.timefusion_buffer_max_memory_mb = 4096;
        cfg.cache.timefusion_foyer_memory_mb = 1024;
        cfg.cache.timefusion_foyer_metadata_memory_mb = 256;
        assert!(!budget_audit(&cfg, 256 * 1024).oversubscribed());
        // Unknown RAM (0) must not divide-by-zero; warn_at collapses to 0 so a
        // non-zero commitment is flagged rather than silently passing.
        assert_eq!(budget_audit(&cfg, 0).warn_at_mb, 0);
    }
}

// ===== secret_crypto =====
// AES-256-GCM two-way encryption for at-rest secrets (S3 creds in
// `timefusion_projects`). Key is supplied via the
// `TIMEFUSION_CONFIG_ENCRYPTION_KEY` env var as a base64-encoded 32-byte
// value. Ciphertext is stored as `enc:v1:<base64(nonce||ct||tag)>`.
//
// Plaintext (un-prefixed) rows are still accepted on read so the feature
// can be rolled out without a forced backfill — re-encrypt with
// `timefusion encrypt-secret <value>` and UPDATE the row.

use aes_gcm::{
    AeadCore, Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, OsRng},
};
use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as B64};

pub const ENC_PREFIX: &str = "enc:v1:";
const KEY_ENV: &str = "TIMEFUSION_CONFIG_ENCRYPTION_KEY";
const NONCE_LEN: usize = 12;

static CIPHER: OnceLock<Option<Aes256Gcm>> = OnceLock::new();

fn cipher() -> Option<&'static Aes256Gcm> {
    CIPHER
        .get_or_init(|| {
            let raw = std::env::var(KEY_ENV).ok().filter(|s| !s.is_empty())?;
            B64.decode(raw.trim())
                .map_err(|e| anyhow!("is not valid base64 ({e})"))
                .and_then(|b| <[u8; 32]>::try_from(b).map_err(|_| anyhow!("is not 32 bytes after base64 decode")))
                .map(|b| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&b)))
                .inspect_err(|e| tracing::error!("{KEY_ENV} {e}; encryption disabled"))
                .ok()
        })
        .as_ref()
}

pub fn key_configured() -> bool {
    cipher().is_some()
}

/// Encrypt a plaintext secret. Errors if no key is configured.
pub fn encrypt(plaintext: &str) -> Result<String> {
    let c = cipher().ok_or_else(|| anyhow!("{KEY_ENV} not set — cannot encrypt"))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ct = c.encrypt(&nonce, plaintext.as_bytes()).map_err(|e| anyhow!("AES-GCM encrypt failed: {e}"))?;
    Ok(format!("{ENC_PREFIX}{}", B64.encode([nonce.as_slice(), ct.as_slice()].concat())))
}

/// Decrypt a value loaded from `timefusion_projects`. Pass-through for
/// values without the `enc:v1:` prefix (legacy plaintext rows).
pub fn decrypt_or_passthrough(value: &str) -> Result<String> {
    let Some(rest) = value.strip_prefix(ENC_PREFIX) else {
        return Ok(value.to_string());
    };
    let c = cipher().ok_or_else(|| anyhow!("row is encrypted ({ENC_PREFIX}…) but {KEY_ENV} is not set"))?;
    let bytes = B64.decode(rest).context("encrypted secret is not valid base64")?;
    let (nonce, ct) = bytes.split_at_checked(NONCE_LEN).filter(|(_, ct)| !ct.is_empty()).context("encrypted secret payload too short")?;
    let pt = c.decrypt(Nonce::from_slice(nonce), ct).map_err(|e| anyhow!("AES-GCM decrypt failed (key mismatch or tampered ciphertext): {e}"))?;
    String::from_utf8(pt).context("decrypted secret is not valid UTF-8")
}

/// CLI helper: `timefusion encrypt-secret <plaintext>` — encrypts the
/// argument and prints the `enc:v1:…` string for use in SQL inserts.
pub fn run_cli() -> Result<()> {
    // skip binary + "encrypt-secret"
    let plaintext = std::env::args().nth(2).ok_or_else(|| anyhow!("usage: timefusion encrypt-secret <plaintext>"))?;
    println!("{}", encrypt(&plaintext)?);
    Ok(())
}

#[cfg(test)]
mod secret_crypto_tests {
    use serial_test::serial;

    use super::*;

    // CIPHER is a OnceLock, so the key must be in the env before this
    // process's first cipher() call; #[serial] keeps that ordering race-free
    // against other set_var tests in this binary.
    #[test]
    #[serial]
    fn roundtrip_and_plaintext_passthrough() {
        // SAFETY: #[serial] guarantees no other test in this binary mutates
        // env concurrently.
        unsafe { std::env::set_var(KEY_ENV, B64.encode([7u8; 32])) };
        let ct = encrypt("AKIAEXAMPLE").unwrap();
        assert!(ct.starts_with(ENC_PREFIX));
        assert_eq!(decrypt_or_passthrough(&ct).unwrap(), "AKIAEXAMPLE");
        assert_eq!(decrypt_or_passthrough("plain").unwrap(), "plain");
        // nonce-only payload => no ciphertext left after the split
        assert!(decrypt_or_passthrough(&format!("{ENC_PREFIX}{}", B64.encode([0u8; NONCE_LEN]))).is_err());
    }
}
