use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Bytes per MiB / GiB — used by the `*_bytes()` size accessors below so the
/// `* 1024 * 1024` chains don't repeat (and read as the unit they mean).
const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Self-sizing budget tree (docs/compaction-redesign-2026-07-29.md §4).
// Replaces hand-set memory/concurrency env vars (drift caused the 07-21
// budget-vs-limit crash loop) with derivation from the container's actual
// cgroup limits. Detection is pure-parseable (`parse_*` fns take `&str`,
// doctested) so a misread cgroup file reproduces without a container.
// ---------------------------------------------------------------------------

/// Read+parse a `/proc`/`/sys` file; `None` on failure — every detector below
/// chains these.
fn read_parsed<T>(path: &str, parse: impl FnOnce(&str) -> Option<T>) -> Option<T> {
    std::fs::read_to_string(path).ok().and_then(|s| parse(&s))
}

/// Parse cgroup v2 `memory.max` content. Returns `None` for `"max"`
/// (unlimited, which simply fails to parse) or garbage — caller falls back.
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

/// Parse `/proc/meminfo`'s `MemTotal:` line (kB) into bytes.
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

/// Detect available cores: cgroup v2 `cpu.max` quota/period → OS-reported
/// parallelism → a 4-core floor. Never panics.
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

/// Which reservation shape the budget tree derives. A one-shot maintenance CLI
/// (`optimize` / `redrive-dml` / `migrate-columns`) serves no pgwire queries
/// and starts no ingest, yet under the server shape it still pays those
/// reservations — on a small pod the maintenance pool lands on its floor and
/// sorts die admitting their first batch. The CLI shape hands maintenance
/// nearly the whole cgroup. Selected via `TIMEFUSION_BUDGET_PROFILE=maintenance-cli`,
/// set by `main` for CLI subcommands before config init — never by operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BudgetProfile {
    #[default]
    Server,
    MaintenanceCli,
}

fn profile_from_env() -> BudgetProfile {
    match std::env::var("TIMEFUSION_BUDGET_PROFILE").as_deref() {
        Ok("maintenance-cli") => BudgetProfile::MaintenanceCli,
        _ => BudgetProfile::Server,
    }
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
/// Heavy maintenance's minimum share of the maintenance pool. 0.40, up from
/// 0.25 — a REBALANCE inside the existing pool (total unchanged), following
/// the workload: hot-tail packing (light share) converged once unstarved,
/// while the dedup queue kept growing, so heavy is where the backlog lives.
/// 0.40 specifically because `light_optimize_k` divides light's share by
/// `PER_SORT_BUDGET_BYTES`; going lower still yields the same K for packing
/// while buying heavy little, since heavy's concurrency is bounded by
/// rewrite permits, not bytes.
const HEAVY_MIN_SHARE: f64 = 0.40;

/// Pool slice per in-flight coordinator job. Kept equal to the admission
/// ceiling so a job that is allowed to decode N bytes has N bytes of pool to
/// hold them in before it must spill; see `coordinator_share_bytes`.
const COORDINATOR_JOB_POOL_BYTES: usize = crate::maintenance_coordinator::MAX_DECODED_BYTES as usize;
/// Floor so a tiny box never zeroes the maintenance pool.
const MAINTENANCE_FLOOR_BYTES: usize = GIB;

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
    /// `jobs x MAX_DECODED_BYTES` is the ceiling a fully-rollup-loaded
    /// coordinator could want, not what it typically runs — only rollup units
    /// draw on this pool, DEDUP units sort on the heavy share. Letting the
    /// ceiling take half the pool starved the sorting share instead. A quarter
    /// still leaves each job ~260 MB, above the 32 MB floor, and hands the
    /// difference back to the tiers that were measurably short.
    pub fn coordinator_share_bytes(&self) -> usize {
        match self.profile {
            // The CLI drives engines directly; no coordinator runs.
            BudgetProfile::MaintenanceCli => 0,
            BudgetProfile::Server => (self.coordinator_jobs() * COORDINATOR_JOB_POOL_BYTES).min(self.maintenance_pool_bytes / 4),
        }
    }

    /// What heavy and light divide, once the coordinator has taken its share.
    fn maintenance_split_bytes(&self) -> usize {
        self.maintenance_pool_bytes - self.coordinator_share_bytes()
    }

    /// Heavy maintenance (dedup/optimize/recompress) share — at least
    /// `HEAVY_MIN_SHARE` of the maintenance pool.
    pub fn heavy_share_bytes(&self) -> usize {
        // MaintenanceCli: engines run one command at a time and each engine's
        // pool is a separate FairSpillPool, so both shares may claim ~the whole
        // pool — only the active engine ever allocates.
        match self.profile {
            BudgetProfile::MaintenanceCli => ((self.maintenance_pool_bytes as f64) * 0.85) as usize,
            BudgetProfile::Server => ((self.maintenance_split_bytes() as f64) * HEAVY_MIN_SHARE) as usize,
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
        let mem_bound = self.light_share_bytes() / PER_SORT_BUDGET_BYTES;
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

// serde `default = "..."` needs a fn per default value. The owned-type arms
// convert (`&str` → String/PathBuf); everything else returns the literal.
macro_rules! const_default {
    ($name:ident: String = $val:expr) => {
        fn $name() -> String {
            $val.into()
        }
    };
    ($name:ident: PathBuf = $val:expr) => {
        fn $name() -> PathBuf {
            PathBuf::from($val)
        }
    };
    ($name:ident: $t:ty = $val:expr) => {
        fn $name() -> $t {
            $val
        }
    };
}

const_default!(d_true: bool = true);
const_default!(d_tantivy_build_concurrency: usize = 2);
const_default!(d_s3_endpoint: String = "https://s3.amazonaws.com");
const_default!(d_data_dir: PathBuf = "./data");
const_default!(d_pgwire_port: u16 = 5432);
const_default!(d_table_prefix: String = "timefusion");
const_default!(d_batch_queue_capacity: usize = 100_000_000);
const_default!(d_pgwire_user: String = "postgres");
const_default!(d_pgwire_max_statement_secs: u64 = 60);
// 60s (was 300s): a shorter flush interval bounds how much un-flushed WAL a
// restart must replay — startup/redeploy downtime is dominated by WAL replay,
// which scales ~linearly with this interval. Trade-off: ~5x more Delta
// commits / small files (handled by compaction/OPTIMIZE).
const_default!(d_flush_interval: u64 = 60);
// Flush dwell: a sealed-but-young bucket waits this long from CREATION before
// the periodic flush commits it, unless it is already big. -1 = one
// bucket_duration (the prod default), 0 = off (the test harnesses set this —
// they assert "sealed => next tick flushes"). See flush_completed_buckets.
const_default!(d_flush_dwell_secs: i64 = -1);
const_default!(d_retention_mins: u64 = 70);
// The local hot tier: demoted sealed buckets served as the scan's third leg.
//
// Holds WHATEVER FITS ON DISK — no time retention. Used to keep a fixed
// number of hours, which made the tier's value depend on guessing the right
// number and left disk unused. GC now unlinks oldest-first purely to stay
// under `d_hot_tier_max_disk_gb`, so buying disk buys coverage directly, and
// `skip_for_lookback` reads the tier's MEASURED span rather than a setting.
const_default!(d_hot_tier_enabled: bool = true);
// Files are UNCOMPRESSED (~4x the bytes of the LZ4 era), so this holds
// roughly the coverage the old compressed cap did — bought with disk that was
// sitting idle instead of decompression CPU and anon heap. Raise it to buy
// more history; that is now the only knob that changes how far back the tier
// reaches.
const_default!(d_hot_tier_max_disk_gb: u64 = 600);
const_default!(d_eviction_interval: u64 = 60);
const_default!(d_buffer_max_memory: usize = 4096);
const_default!(d_wal_shards_per_topic: usize = 4);
// Total graceful-shutdown budget shared by ALL serial shutdown phases
// (PGWire drain → buffered-layer flush + cursor snapshot).
// Set to ~80% of the orchestrator's SIGTERM→SIGKILL grace (Docker/CapRover
// `StopGracePeriod`; prod is 90s) so the clean cursor snapshot always lands
// before SIGKILL — the previous per-phase 180s ceilings assumed grace nobody
// configured, and PGWire drain alone could eat the real grace before the
// flush or snapshot ever started. Anything unflushed at the deadline is
// durable in the WAL and replays on next boot.
const_default!(d_stop_grace: u64 = 70);
const_default!(d_wal_corruption_threshold: usize = 10);
// Concurrent staged flush commits. Parquet encode + S3 upload happen outside
// the per-table commit lock (see insert_records_batch staged path), so this scales
// upload throughput directly — the dominant steady-state drain lever under
// backfill. 8 doubles concurrency over the old 4 while bounding in-flight
// encode memory; raise further (env) if CPU/R2 headroom allows.
const_default!(d_flush_parallelism: usize = 8);
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
const_default!(d_flush_coalesce_commits: bool = false);
// Cold-boot Delta cursor reconciliation. R2 happily takes 64+ concurrent
// gets per bucket; the original 8 left ~8× headroom. Depth 8 is half the
// original 16 (the snapshot replaces the bulk of the scan) but keeps a
// safety margin: if a few snapshot writes failed silently before reboot,
// depth-2 could miss the legitimate cursor advance. Tune via env if the
// fallback Delta scan is the bottleneck.
const_default!(d_delta_scan_concurrency: usize = 64);
const_default!(d_delta_scan_depth: usize = 8);
const_default!(d_wal_fsync_ms: u64 = 200);
// MemBuffer bucket window (seconds). Smaller windows free RAM sooner because
// the previous bucket becomes flushable sooner; larger windows amortize into
// fewer/larger Delta commits. 300s, halved from 600s to cut peak MemBuffer
// footprint (the current bucket is excluded from flushing, so this is the
// floor on how long a row accumulates in RAM). Trade-off is ~2× Delta commits
// / small files; high-throughput tenants can go lower (60–120s), memory-relaxed
// deployments can raise it back.
const_default!(d_bucket_duration_secs: u64 = 300);
// Memory pressure threshold (0–100) at which the flush task is woken
// independently of the periodic flush timer. Triggers an early
// `flush_completed_buckets` so MemBuffer drains before reservation reaches
// the hard limit. 0 disables pressure-triggered flushes.
const_default!(d_pressure_flush_pct: u32 = 75);
// Max seconds an insert applies backpressure (synchronously flushing
// MemBuffer → Delta to free RAM) before failing, when the memory hard limit
// is hit. The rows are already durable in the WAL, so this trades a slow
// write for a rejected one — the right call for a TS DB whose producers DLQ
// on rejection. 0 restores the old fail-fast behavior. 60s is long enough to
// ride out a flush cycle / drain a replayed backlog, finite so a genuinely
// down Delta can't pile blocked writers up without bound.
const_default!(d_write_backpressure_secs: u64 = 60);
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
const_default!(d_dml_coalesce_secs: u64 = 3);
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
const_default!(d_flush_bucket_timeout_secs: u64 = 600);
// Durability mode for the WAL. One of:
//   "sync_each" — fsync after every entry (default; zero data-loss window, ~1ms per write)
//   "ms"        — async fsync every `wal_fsync_ms` (~200ms loss window; a torn
//                 mmap tail after OOM/SIGKILL quarantines acked entries)
//   "none"      — never fsync (test/throwaway data only)
const_default!(d_wal_fsync_mode: String = "sync_each");
const_default!(d_wal_ack_fsync: bool = true);
// 0 = unset → derived (DerivedBudget::wal_flush_file_threshold); env-set wins.
const_default!(d_wal_max_files: usize = 0);
const_default!(d_wal_hard_limit_gb: u64 = 192);
const_default!(d_foyer_memory_mb: usize = 1024);
// Local disk is cheap and fast relative to S3 GETs, so default the cache large
// — servers run 500GB–1TB cache volumes. foyer creates the backing file sparse,
// but this is the logical ceiling at which it starts evicting, so it MUST stay
// <= the cache volume's free space or writes hit ENOSPC before eviction kicks
// in. Lower it on smaller disks.
const_default!(d_foyer_disk_gb: usize = 500);
const_default!(d_foyer_ttl: u64 = 604_800); // 7 days
const_default!(d_provider_cache_ttl: u64 = 300); // 5 minutes
const_default!(d_provider_cache_capacity: usize = 4_096);
const_default!(d_foyer_shards: usize = 8);
const_default!(d_foyer_file_size_mb: usize = 32);
const_default!(d_foyer_stats: String = "true");
const_default!(d_metadata_size_hint: usize = MIB);
// DataFusion's in-process decoded-parquet-metadata cache (footer + page index).
// Distinct from the Foyer footer-BYTES cache: this holds the decoded
// ParquetMetaData so repeat scans skip re-parsing. Entries larger than the
// limit are silently dropped, so it must comfortably exceed a single file's
// metadata; the DataFusion default is only 50MB.
const_default!(d_df_metadata_cache_mb: usize = 512);
const_default!(d_metadata_memory_mb: usize = 512);
const_default!(d_metadata_disk_gb: usize = 5);
const_default!(d_metadata_shards: usize = 4);
const_default!(d_warm_inline_max_mb: usize = 0);
const_default!(d_write_capture_max_mb: usize = 32);
const_default!(d_write_capture_budget_mb: usize = 256);
const_default!(d_foyer_block_size_mb: usize = 256);
const_default!(d_l1_max_entry_mb: usize = 16);
const_default!(d_cache_recent_days: usize = 8);
/// Bound on the post-commit cache confirm. It is an optimization, never a
/// durability gate, so a slow warm must not stall the flush loop.
pub const CACHE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrency of the confirm's full-file fetches. Deliberately NOT the 16-way
/// `timefusion_warm_concurrency` (detached, off the flush path): each miss
/// buffers a whole flush-sized parquet body in transient heap no memory pool
/// tracks, ON the flush path — the untracked-consumer shape behind this box's
/// prior OOMs. Peak ≈ this × largest added file.
pub const CACHE_CONFIRM_CONCURRENCY: usize = 4;
const_default!(d_cache_bypass_scan_hours: u64 = 24);
const_default!(d_page_rows: usize = 20_000);
const_default!(d_zstd_level: i32 = 3);
// Tiered compression by partition age. Hot writes prioritize ingest latency;
// older data is rewritten at progressively higher levels by `recompress_tier`.
const_default!(d_zstd_level_intermediate: i32 = 1);
const_default!(d_zstd_level_warm: i32 = 9);
const_default!(d_zstd_level_cold: i32 = 19);
const_default!(d_cold_cutoff_days: u64 = 14);
const_default!(d_recompress_schedule: String = "0 0 3 * * *");
const_default!(d_row_group_size: usize = 128 * MIB);
const_default!(d_checkpoint_interval: u64 = 10);
// 256MB compacted-file target: fewer, larger files cut Delta metadata, S3
// object count, and the per-commit get_file_uris() walk on the flush append
// path; sorted + page-indexed files still prune time-range queries within a
// file, so the query downside is minimal for this (project_id,date)-partitioned
// workload. Light/today optimize keeps its own 16MB target.
const_default!(d_optimize_target: i64 = 256 * MIB as i64);
// Cold tier: sealed partitions (older than `cold_optimize_after_days`) bin-pack
// to 512MB. File size grows with partition age — recent days stay at 256MB (less
// rewrite while the day still fills), sealed days consolidate to 512MB so the
// Delta checkpoint (≈ live file count) shrinks, the dominant driver of commit
// latency. Compression is per-row-group, so bigger files don't change bytes
// stored — the win is fewer files. Re-runs are cheap: Compact skips files
// already ≥ target. 512MB, not 1GB: a merge holds ~target-sized output buffers
// per concurrent task and the decompressed working set is ~17x the compressed
// target, so 1GB made the final consolidation's sort/merge memory-hostile.
const_default!(d_cold_optimize_target: i64 = 512 * MIB as i64);
// 1 day = everything past the current (day-partitioned) partition. Only today
// still takes writes, so every sealed day consolidates to 512MB. The warm
// optimize is clamped to dates newer than this boundary (see `optimize_table`)
// so the 30-min Z-order never fragments these files back to 256MB.
const_default!(d_cold_optimize_after_days: u64 = 1);
const_default!(d_stats_cache_size: usize = 50);
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
const_default!(d_vacuum_retention: u64 = 72);
// Delta _delta_log (transaction-log) retention. Keeps the log directory small
// (~commit-rate × retention files) so every commit's version-discovery LIST
// stays cheap. Delta's default is 30 DAYS, which let the log grow to tens of
// thousands of objects and made each commit's version-discovery slow; even a
// 1-day window regrew under the multi-tenant per-project commit rate, so hold
// a tighter 6h window. enableExpiredLogCleanup (default true) prunes during
// checkpoints; cross-project flush coalescing cuts the commit rate driving
// growth.
const_default!(d_log_retention: u64 = 6);
const_default!(d_optimize_window_hours: u64 = 48);
const_default!(d_compact_min_files: usize = 5);
// 256MB (raised from 32MB): the small-merge-memory rationale for a tiny
// hot/today target is moot on this box, and 32MB left the hot partition as
// dozens of tiny files for a high-write project — recent queries were
// file-open-latency bound. A larger target collapses today's sealed slices
// into a few large event-time-disjoint runs.
const_default!(d_light_optimize_target: i64 = 256 * MIB as i64);
const_default!(d_writer_max_file_bytes: usize = 512 * MIB);
const_default!(d_repair_max_file_bytes: usize = 512 * MIB);
// 2 GiB escalation threshold for the flush sort. The flush sort is IN-PROCESS
// and allocates OUTSIDE the DataFusion pool — raising this ceiling authorised
// multi-GB untracked allocations on the ingest path and correlated with OOM
// kills, so it was reverted. Past this threshold the flush instead sorts
// inside a pooled, disk-spilling DataFusion plan (`sort_flush_group_spilling`)
// — the footer stays honest and the peak is bounded by the pool. 2 GiB keeps
// the fast in-process path for everything ordinary; DataFusion's sort also
// overtakes Arrow lexsort above ~370 MB, so the pooled path is faster there too.
const_default!(d_sort_skip_bytes: usize = 2 * GIB);
const_default!(d_flush_sort_pool_mb: u64 = 1024);
const_default!(d_light_schedule: String = "0 */5 * * * *");
// Must match d_dedup_lookback_days — certification and rollup coverage need
// the same horizon, or a day is certified but never rolled up (or vice versa).
const_default!(d_rollup_backfill_days: u16 = 35);
const_default!(d_rollup_backfill_schedule: String = "0 */10 * * * *");
const_default!(d_footer_repair_schedule: String = "0 30 * * * *");
const_default!(d_footer_repair_budget_secs: u64 = 8640);
const_default!(d_repair_lookback_days: u64 = 31);
const_default!(d_optimize_schedule: String = "0 */30 * * * *");
// Daily cold consolidation sweep (02:30): bin-pack sealed partitions to the 512MB
// cold target. Calendar-age driven; idempotent (skips ≥-target files).
const_default!(d_consolidate_schedule: String = "0 30 2 * * *");
const_default!(d_consolidate_catchup_passes: usize = 4);
// Every 6h, not daily: tombstones leave the checkpoint once older than the
// retention window, so vacuum must run often enough to delete files before
// their tombstones age out (VacuumMode::Full backstops any that slip through).
const_default!(d_vacuum_schedule: String = "0 15 */6 * * *");
// Out-of-band checkpoint + expired-log cleanup, driven here instead of
// delta-rs's commit-path hook: a hook failure surfaced as a commit error
// AFTER the commit landed, and the flush path misread that as a failed
// commit and deleted the committed parquet. Every 2 min, tolerant of R2
// 500s — faster than the commit cadence so the log stays bounded.
const_default!(d_checkpoint_schedule: String = "0 */2 * * * *");
// Reconcile active Add entries against object-store truth: HEAD every live
// file and commit Remove for any that are missing. Repairs dangling Adds left
// by past commit-path parquet deletions; a nonzero removal count means
// committed data was destroyed elsewhere.
const_default!(d_reconcile_schedule: String = "0 0 * * * *");
const_default!(d_tantivy_reconcile_schedule: String = "0 30 3 * * *");
const_default!(d_warm_recency_days: u64 = 1);
// 16: at concurrency 4 a large boot warm ran >55 min and was cut short by a
// restart every time; 16 finishes in ~1-3 min. Footer GETs are small
// suffix-range reads, well within R2/S3 burst limits.
const_default!(d_warm_concurrency: usize = 16);
const_default!(d_snapshot_reconcile: u64 = 500);
// Byte ceiling on the file set one dedup chunk rewrite may materialize.
// Over-budget chunks are SKIPPED loudly (metric: timefusion.dedup.chunk_skipped)
// rather than rewritten — read-side dedup keeps queries correct meanwhile.
// Guards against e.g. a z-ordered whole-day file dragging the whole day into
// one rewrite. Kept in step with `d_dedup_max_decoded_bytes` — shard count
// takes the MAX of both, so leaving this lower would silently cap sharding
// below the decoded budget.
const_default!(d_dedup_max_rewrite_bytes: u64 = GIB as u64 / 2);
// 512 MiB estimated decoded footprint, sized so permits x budget stays bounded
// in flight (see HEAVY_REWRITE_PERMITS). A chunk this large already dwarfs the
// DataFusion pool; larger chunks skip rather than risk the cgroup.
//
// Sized to FUND SHARD CONCURRENCY, not to save memory: `dedup_shard_concurrency`
// runs `DEDUP_BIN_ARROW_BUDGET / this` shards at once, so a smaller shard buys
// parallelism at an unchanged peak. It also keeps each unit inside its per-bin
// deadline — a unit bigger than its budget produces nothing, forever.
const_default!(d_dedup_max_decoded_bytes: u64 = GIB as u64 / 2);
// 12x compressed->decoded: zstd on wide Variant/JSON otel rows routinely
// decodes 10-20x; 12 is a deliberately conservative floor.
const_default!(d_dedup_decode_inflation: u64 = 12);
// 4 KiB/row decoded estimate for otel spans (wide Variant/JSON bodies).
const_default!(d_dedup_bytes_per_row: u64 = 4096);
// Serial: each merge-update decodes + rewrites whole hot partitions with
// pool-invisible memory; concurrent stacking under a heavy UPDATE-drain
// storm drove an OOM crash-loop. Results are identical either way — permits
// only bound peak memory, excess statements queue.
const_default!(d_dml_merge_concurrency: usize = 1);
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
// nothing. 35 covers a 30d query with margin and matches `d_repair_lookback_days`.
const_default!(d_dedup_lookback_days: u64 = 35);
const_default!(d_query_partitions: usize = 0);
// Wide-scan admission guard. 16 concurrent Parquet decoders bounds untracked
// decode heap well under the pool on this box (48-way caused an OOM). Wide
// observability windows are already latency-bound below this, so gating is
// near-free. 2h lookback keeps the hot dashboards ungated (they page-prune to
// tiny per-file bytes) while 3d+/no-time scans queue. Both tunable via
// TIMEFUSION_{MAX_CONCURRENT_SCAN_READERS,WIDE_SCAN_LOOKBACK_HOURS}.
const_default!(d_max_concurrent_scan_readers: usize = 16);
const_default!(d_wide_scan_lookback_hours: u64 = 2);
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
const_default!(d_wide_scan_max_files: usize = 256);
// Compressed parquet bytes understate transient Arrow decode heap by an
// order of magnitude on OTel data — a 222 MB file measured ~4 GiB of process
// growth decoding 48 row groups in parallel, so it must participate in the
// shared decode gate rather than the small-scan exemption. 64 MB keeps
// genuinely small, well-pruned history reads ungated.
const_default!(d_wide_scan_max_mb: u64 = 64);
const_default!(d_plan_cache_capacity: usize = 2048);
const_default!(d_otlp_endpoint: String = "http://localhost:4317");
const_default!(d_service_name: String = "timefusion");
const_default!(d_service_version: String = env!("CARGO_PKG_VERSION"));

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

const_default!(d_tantivy_backfill_max_file_mb: u64 = 4096);
const_default!(d_tantivy_max_index_mb: u64 = 64);
// Sized against a working set, not a wish: the reaper only evicts what no
// query has opened recently, and every eviction re-downloads a blob on the
// next hit. 4 GB (the value this knob carried as dead code) would thrash the
// hot window at prod scale. Measured working set: ~65 GB across ~6500 leaf
// index dirs, so 64 sits right at it — the reaper trims rather than evicting
// the hot window on its first pass.
const_default!(d_tantivy_cache_disk_gb: u64 = 64);
const_default!(d_tantivy_cache_reap_schedule: String = "0 */10 * * * *");
// Level 3: index packing is on the flush hot path; level 19 cost ~88% of a
// CPU window per flush for only 10-15% smaller output.
const_default!(d_tantivy_zstd_level: i32 = 3);
const_default!(d_tantivy_min_files: usize = 2);
const_default!(d_tantivy_prefilter_max_hits: usize = 100_000);
const_default!(d_tantivy_prefilter_min_selectivity_pct: u32 = 50);

/// Tantivy sidecar-index config. Indexing is always-on for any table whose
/// YAML schema declares `tantivy.indexed: true` on at least one field —
/// schema is the single source of truth, no override knob.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde(default = "d_tantivy_max_index_mb")]
    pub timefusion_tantivy_max_index_size_mb: u64,
    /// Byte budget for the local extracted-index cache
    /// (`<timefusion_data_dir>/tantivy_cache`), enforced LRU-first by the
    /// "Tantivy cache reap" cron — the only thing that deletes from that
    /// tree. Shares a volume with the WAL, so a full volume also fails WAL
    /// appends.
    #[serde(default = "d_tantivy_cache_disk_gb")]
    pub timefusion_tantivy_cache_disk_gb: u64,
    /// How often to enforce `timefusion_tantivy_cache_disk_gb`. Each sweep
    /// walks the whole cache tree; empty disables the reap (and the bound).
    #[serde(default = "d_tantivy_cache_reap_schedule")]
    pub timefusion_tantivy_cache_reap_schedule: String,
    #[serde(default = "d_tantivy_zstd_level")]
    pub timefusion_tantivy_compression_level: i32,
    #[serde(default = "d_tantivy_min_files")]
    pub timefusion_tantivy_min_files_for_pushdown: usize,
    /// If a tantivy prefilter would produce more than this many hits, skip
    /// the `id IN (...)` pushdown entirely — the IN-list itself becomes the
    /// bottleneck above this point. Default 100k.
    #[serde(default = "d_tantivy_prefilter_max_hits")]
    pub timefusion_tantivy_prefilter_max_hits: usize,
    /// If a tantivy prefilter selects more than this percentage of the
    /// indexed rows, the pushdown isn't worth the round-trip; skip it and
    /// let Delta scan with the original predicate. Default 50 (%).
    #[serde(default = "d_tantivy_prefilter_min_selectivity_pct")]
    pub timefusion_tantivy_prefilter_min_selectivity_pct: u32,
    /// Route exact `col = 'lit'` on raw-tokenized high-cardinality columns
    /// (trace_id/span_id/id/parent_id) through the tantivy id-prefilter, not
    /// just LIKE (and IN-lists as OR-of-terms). Correctness-safe under OR:
    /// `collect_text_match_tree` only routes a disjunction when every branch
    /// is fully covered by a text_match, and the original predicate always
    /// stays as the post-filter backstop. Targets the trace/span lookup gap
    /// vs the indexed PG path. Default ON; set false to revert to
    /// bloom/stats-only equality pruning.
    #[serde(default = "d_true")]
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
    #[serde(default = "d_tantivy_build_concurrency")]
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
    #[serde(default = "d_tantivy_backfill_max_file_mb")]
    pub timefusion_tantivy_backfill_max_file_mb: u64,
    /// File-level scan pruning: when the prefilter engages, files whose
    /// covering index returned zero hits are excluded from the Delta scan
    /// entirely. Off switch for instant rollback to id-IN-list-only pruning.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_file_pruning: bool,
    /// Warm the local index cache with blobs whose data is at most this many
    /// days old, at startup (0 = off). Turns the cold-window download cliff
    /// into a background cost after restarts.
    #[serde(default)]
    pub timefusion_tantivy_prefetch_days: u32,
    /// Row-selection pushdown: when the prefilter engages, files whose index
    /// was built in parquet row order get a per-file ParquetAccessPlan so the
    /// reader decodes only matching rows. Off switch for instant rollback to
    /// id-IN-list-only filtering inside surviving files.
    #[serde(default = "d_true")]
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
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region: Option<String>,
    #[serde(default = "d_s3_endpoint")]
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

#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde(default = "d_data_dir")]
    pub timefusion_data_dir: PathBuf,
    #[serde(default = "d_pgwire_port")]
    pub pgwire_port: u16,
    #[serde(default = "d_table_prefix")]
    pub timefusion_table_prefix: String,
    #[serde(default)]
    pub timefusion_config_database_url: Option<String>,
    #[serde(default = "d_true")]
    pub enable_batch_queue: bool,
    #[serde(default = "d_batch_queue_capacity")]
    pub timefusion_batch_queue_capacity: usize,
    #[serde(default = "d_pgwire_user")]
    pub pgwire_user: String,
    #[serde(default)]
    pub pgwire_password: Option<String>,
    #[serde(default = "d_pgwire_max_statement_secs")]
    pub timefusion_pgwire_max_statement_secs: u64,
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
    /// Own root for the local hot tier — never share a dir with a generic
    /// recursive deleter (WAL GC once ate the quarantine dir this way).
    pub fn hot_tier_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("hot_tier")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "d_flush_interval")]
    pub timefusion_flush_interval_secs: u64,
    #[serde(default = "d_flush_dwell_secs")]
    pub timefusion_flush_dwell_secs: i64,
    #[serde(default = "d_retention_mins")]
    pub timefusion_buffer_retention_mins: u64,
    #[serde(default = "d_eviction_interval")]
    pub timefusion_eviction_interval_secs: u64,
    /// Local hot tier: instead of dropping a drained bucket, demote it to an
    /// uncompressed Arrow IPC file on local disk and serve recent-window
    /// reads via zero-copy mmap. This is the tier's main switch; **0 turns
    /// demotion off** (GC still sweeps). Past this age a demoted file is
    /// unlinked and its window falls back to Delta.
    #[serde(default = "d_hot_tier_enabled")]
    pub timefusion_hot_tier_enabled: bool,
    /// Hard cap on the tier's directory; over it, GC unlinks oldest-first.
    /// The tier shares the WAL/data volume, which has twice been eaten by an
    /// unbounded consumer, so this one is a real dial.
    #[serde(default = "d_hot_tier_max_disk_gb")]
    pub timefusion_hot_tier_max_disk_gb: u64,
    #[serde(default = "d_buffer_max_memory")]
    pub timefusion_buffer_max_memory_mb: usize,
    #[serde(default = "d_stop_grace")]
    pub timefusion_stop_grace_secs: u64,
    #[serde(default = "d_wal_corruption_threshold")]
    pub timefusion_wal_corruption_threshold: usize,
    #[serde(default = "d_flush_parallelism")]
    pub timefusion_flush_parallelism: usize,
    /// Coalesce one tick's per-project flush commits into one commit per
    /// PHYSICAL Delta table (see `d_flush_coalesce_commits`).
    #[serde(default = "d_flush_coalesce_commits")]
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
    #[serde(default = "d_wal_fsync_ms")]
    pub timefusion_wal_fsync_ms: u64,
    #[serde(default = "d_wal_fsync_mode")]
    pub timefusion_wal_fsync_mode: String,
    /// Fsync the WAL shard before acking DML appends (machine-crash
    /// durability). Batched INSERT appends are always flushed before ack;
    /// only single-entry DML appends defer to the background fsync thread —
    /// this closes that window. Default on: a torn mmap tail after
    /// OOM/SIGKILL can quarantine acked-but-unsynced entries.
    #[serde(default = "d_wal_ack_fsync")]
    pub timefusion_wal_ack_fsync: bool,
    #[serde(default = "d_wal_max_files")]
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
    #[serde(default = "d_wal_hard_limit_gb")]
    pub timefusion_wal_hard_limit_gb: u64,
    #[serde(default = "d_bucket_duration_secs")]
    pub timefusion_bucket_duration_secs: u64,
    #[serde(default = "d_pressure_flush_pct")]
    pub timefusion_pressure_flush_pct: u32,
    #[serde(default = "d_write_backpressure_secs")]
    pub timefusion_write_backpressure_secs: u64,
    /// See `d_dml_coalesce_secs` — drain interval for deferred UPDATE ... FROM
    /// Delta merges; 0 keeps the synchronous per-statement path.
    #[serde(default = "d_dml_coalesce_secs")]
    pub timefusion_dml_coalesce_secs: u64,
    /// Fold same-shape coalesced groups across projects into one MERGE per
    /// unified table per drain (`project_id` becomes a join key + IN-list
    /// partition filter). Eliminates the per-project metadata-scan +
    /// OCC-commit multiplication that starved flush under a heavy merge
    /// storm. Kill switch: `TIMEFUSION_DML_COALESCE_FOLD=false`.
    #[serde(default = "d_true")]
    pub timefusion_dml_coalesce_fold: bool,
    #[serde(default = "d_flush_bucket_timeout_secs")]
    pub timefusion_flush_bucket_timeout_secs: u64,
    /// WAL shards per (project, table) topic. Higher = more append parallelism
    /// at the cost of O(shards) recovery memory and more file handles.
    #[serde(default = "d_wal_shards_per_topic")]
    pub timefusion_wal_shards_per_topic: usize,
    /// Max concurrent S3/R2 reads when reconciling per-table Delta watermarks
    /// at boot. Only used when the cursor snapshot is missing or stale.
    #[serde(default = "d_delta_scan_concurrency")]
    pub timefusion_delta_scan_concurrency: usize,
    /// Per-table Delta commit history depth scanned at boot. The cursor
    /// snapshot covers the bulk of the watermark; this only needs to catch
    /// a writer that committed after the last snapshot was written.
    #[serde(default = "d_delta_scan_depth")]
    pub timefusion_delta_scan_depth: usize,
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
    /// `TIMEFUSION_HOT_TIER_ENABLED=false` is the tier's off switch: no
    /// demotion, no third scan leg, no disk use. No time retention — the
    /// tier holds whatever `hot_tier_limits().max_disk_bytes` affords.
    pub fn hot_tier_enabled(&self) -> bool {
        self.timefusion_hot_tier_enabled
    }
    /// The tier's only ceiling. Per-scan heap needs no knob: `HotTier::scan`
    /// streams its files inside the query's own memory pool.
    pub fn hot_tier_limits(&self) -> crate::hot_tier::HotTierLimits {
        crate::hot_tier::HotTierLimits { max_disk_bytes: self.timefusion_hot_tier_max_disk_gb.saturating_mul(GIB as u64) }
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

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "d_foyer_memory_mb")]
    pub timefusion_foyer_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb: Option<usize>,
    #[serde(default = "d_foyer_disk_gb")]
    pub timefusion_foyer_disk_gb: usize,
    #[serde(default = "d_foyer_ttl")]
    pub timefusion_foyer_ttl_seconds: u64,
    /// Bounded lifetime of resolved Delta providers. A provider is also
    /// invalidated immediately when its Delta snapshot version changes.
    #[serde(default = "d_provider_cache_ttl")]
    pub timefusion_provider_cache_ttl_seconds: u64,
    #[serde(default = "d_provider_cache_capacity")]
    pub timefusion_provider_cache_capacity: usize,
    #[serde(default = "d_foyer_shards")]
    pub timefusion_foyer_shards: usize,
    #[serde(default = "d_foyer_file_size_mb")]
    pub timefusion_foyer_file_size_mb: usize,
    #[serde(default = "d_foyer_stats")]
    pub timefusion_foyer_stats: String,
    #[serde(default = "d_metadata_size_hint")]
    pub timefusion_parquet_metadata_size_hint: usize,
    /// Memory limit (MB) for DataFusion's decoded parquet-metadata cache
    /// (`datafusion.runtime.metadata_cache_limit`). See `d_df_metadata_cache_mb`.
    #[serde(default = "d_df_metadata_cache_mb")]
    pub timefusion_df_metadata_cache_mb: usize,
    #[serde(default = "d_metadata_memory_mb")]
    pub timefusion_foyer_metadata_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb: Option<usize>,
    #[serde(default = "d_metadata_disk_gb")]
    pub timefusion_foyer_metadata_disk_gb: usize,
    #[serde(default = "d_metadata_shards")]
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
    #[serde(default = "d_foyer_block_size_mb")]
    pub timefusion_foyer_block_size_mb: usize,
    /// Entries larger than this (MB) are inserted disk-only so warming a big
    /// compaction output doesn't evict the hot small-entry working set from
    /// L1 memory. 0 = always use L1.
    #[serde(default = "d_l1_max_entry_mb")]
    pub timefusion_foyer_l1_max_entry_mb: usize,
    /// Don't admit writes whose `date=` partition is older than this many
    /// days (e.g. cold-tier recompress rewrites) — recent data stays local,
    /// old data serves from S3. 0 = no age limit. Pairs with the cache TTL.
    #[serde(default = "d_cache_recent_days")]
    pub timefusion_cache_recent_days: usize,
    /// Optional extra cap (MB) on the in-flight buffer used to warm the cache
    /// directly from a multipart write (skip re-downloading what we just
    /// streamed to S3). Always bounded by the disk block size; 0 = bound
    /// only by the block size.
    #[serde(default = "d_warm_inline_max_mb")]
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
    #[serde(default = "d_write_capture_max_mb")]
    pub timefusion_write_capture_max_mb: usize,
    /// Process-wide budget (MB) for in-flight write-capture buffers. Each
    /// capturing upload reserves its full per-upload cap up front,
    /// hard-bounding total capture heap regardless of concurrent uploads.
    /// Over budget = capture skipped for that upload (best-effort, upload
    /// unaffected). Also CLAMPS the per-upload cap, so a cap above the
    /// budget degrades capture rather than disabling it. Default 8x the
    /// per-upload cap so a flush wave never starves itself. 0 = unbudgeted.
    #[serde(default = "d_write_capture_budget_mb")]
    pub timefusion_write_capture_budget_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
    /// Scan-resistant admission: a scan reaching further back than this many
    /// hours runs with cache population BYPASSED, so a wide sweep can't
    /// flush the hot tail out of L1/disk. Reads still HIT what's already
    /// cached. 0 disables the bypass.
    #[serde(default = "d_cache_bypass_scan_hours")]
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

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "d_page_rows")]
    pub timefusion_page_row_count_limit: usize,
    /// ZSTD level for hot writes (flush + today's light optimize). Default 3.
    /// Aliased by the legacy env name; lower = faster ingest.
    #[serde(default = "d_zstd_level", alias = "timefusion_zstd_level_hot")]
    pub timefusion_zstd_compression_level: i32,
    /// ZSTD level for same-day INTERMEDIATE rewrites (hot-tail light optimize,
    /// dedup), later rewritten again by nightly consolidate/recompress. Default
    /// 1: trades transient hot-day file size for less compress CPU and faster
    /// decompress on recent queries. Steady-state size is unaffected — the
    /// tier-1 footer stays below both warm and cold tiers, so recompress still
    /// re-tiers it.
    #[serde(default = "d_zstd_level_intermediate")]
    pub timefusion_zstd_level_intermediate: i32,
    #[serde(default = "d_zstd_level_warm")]
    pub timefusion_zstd_level_warm: i32,
    #[serde(default = "d_zstd_level_cold")]
    pub timefusion_zstd_level_cold: i32,
    #[serde(default = "d_cold_cutoff_days")]
    pub timefusion_cold_cutoff_days: u64,
    #[serde(default = "d_row_group_size")]
    pub timefusion_max_row_group_size: usize,
    #[serde(default = "d_checkpoint_interval")]
    pub timefusion_checkpoint_interval: u64,
    #[serde(default = "d_optimize_target")]
    pub timefusion_optimize_target_size: i64,
    #[serde(default = "d_cold_optimize_target")]
    pub timefusion_cold_optimize_target_size: i64,
    #[serde(default = "d_cold_optimize_after_days")]
    pub timefusion_cold_optimize_after_days: u64,
    #[serde(default = "d_stats_cache_size")]
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

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    #[serde(default = "d_vacuum_retention")]
    pub timefusion_vacuum_retention_hours: u64,
    #[serde(default = "d_log_retention")]
    pub timefusion_log_retention_hours: u64,
    #[serde(default = "d_optimize_window_hours")]
    pub timefusion_optimize_window_hours: u64,
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
    #[serde(default = "d_true")]
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
    #[serde(default = "d_sort_skip_bytes")]
    pub timefusion_sort_skip_bytes: usize,
    /// Pool for the flush-path escalation sort, in MB — its own slice so an
    /// ingest-path sort never queues behind a Z-order holding the maintenance
    /// pool. Deliberately smaller than the escalation threshold: exceeding it
    /// spills to disk, the intended degradation.
    #[serde(default = "d_flush_sort_pool_mb")]
    pub timefusion_flush_sort_pool_mb: u64,
    #[serde(default = "d_compact_min_files")]
    pub timefusion_compact_min_files: usize,
    /// Five-minute hot-partition compaction is required to prevent a
    /// small-file backlog. Set false only as an incident kill switch.
    #[serde(default = "d_true")]
    pub timefusion_light_optimize_enabled: bool,
    #[serde(default = "d_light_optimize_target")]
    pub timefusion_light_optimize_target_size: i64,
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
    #[serde(default = "d_writer_max_file_bytes")]
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
    #[serde(default = "d_repair_max_file_bytes")]
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
    #[serde(default = "d_repair_lookback_days")]
    pub timefusion_light_optimize_repair_days: u64,
    // Concurrent merge tasks per optimize run — formerly
    // `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`. Now `derived.optimize_merge_tasks()`.
    #[serde(default = "d_light_schedule")]
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
    #[serde(default = "d_footer_repair_schedule")]
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
    #[serde(default = "d_footer_repair_budget_secs")]
    pub timefusion_footer_repair_budget_secs: u64,
    /// Dirty-bin dedup of sealed (< today) partitions, on its OWN cron —
    /// decoupled from hot-tail compaction so an old-date dedup backlog can't
    /// starve today's compaction (they touch disjoint partitions). Default 5 min.
    #[serde(default = "d_light_schedule")]
    pub timefusion_dedup_schedule: String,
    /// Incident kill switch for physical dirty-bin dedup; read-side dedup
    /// remains the correctness path. Re-enabled by default after prod-shaped
    /// validation: canaried on live traffic, then physically audited committed
    /// bins — distinct dedup keys intact in-bin and across the partition, only
    /// duplicate versions removed.
    #[serde(default = "d_true")]
    pub timefusion_dirty_bin_dedup_enabled: bool,
    #[serde(default = "d_optimize_schedule")]
    pub timefusion_optimize_schedule: String,
    #[serde(default = "d_consolidate_schedule")]
    pub timefusion_consolidate_schedule: String,
    /// Passes of cold consolidation to run on each hot-compaction tick, for
    /// sealed partitions the daily sweep never reached. Small on purpose: each
    /// pass is one ≤target sorted rewrite and its own commit, so a restart
    /// costs at most one pass and the next tick resumes. 0 disables.
    #[serde(default = "d_consolidate_catchup_passes")]
    pub timefusion_consolidate_catchup_passes: usize,
    #[serde(default = "d_vacuum_schedule")]
    pub timefusion_vacuum_schedule: String,
    #[serde(default = "d_recompress_schedule")]
    pub timefusion_recompress_schedule: String,
    /// Out-of-band checkpoint + expired-log-cleanup schedule. See d_checkpoint_schedule.
    #[serde(default = "d_checkpoint_schedule")]
    pub timefusion_checkpoint_schedule: String,
    /// Dangling-Add reconcile schedule. See d_reconcile_schedule.
    #[serde(default = "d_reconcile_schedule")]
    pub timefusion_reconcile_schedule: String,
    /// Nightly tantivy index reconcile: backfill uncovered live parquet +
    /// GC manifest entries for rewritten-away files, per-uuid manifests
    /// included. The single-process self-management of index consistency —
    /// compaction/wave commits and CLI runs all converge here.
    #[serde(default = "d_tantivy_reconcile_schedule")]
    pub timefusion_tantivy_reconcile_schedule: String,
    /// Proactively warm the Foyer cache for files written by a flush/optimize
    /// commit, so recent partitions dashboards read don't cold-start after
    /// every compaction. Footers are always warmed when enabled.
    #[serde(default = "d_true")]
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
    #[serde(default = "d_warm_recency_days")]
    pub timefusion_warm_recency_days: u64,
    /// Warm parquet footers for EVERY live file (not just recency-window
    /// ones). Footers are tens of KB each, but on tables with thousands of
    /// files the boot-time GET burst may matter on small instances — disable
    /// to fall back to recency-bounded footer warming.
    #[serde(default = "d_true")]
    pub timefusion_warm_all_footers: bool,
    /// Max concurrent warm fetches per commit. Bounds the S3 GET burst a
    /// warm job adds right after a compaction.
    #[serde(default = "d_warm_concurrency")]
    pub timefusion_warm_concurrency: usize,
    /// After a compaction commit, proactively evict the cached full-file bytes
    /// of the files it tombstoned (no longer in the live set), instead of
    /// waiting for VACUUM / TTL / LRU to reclaim them. Cheap (in-cache only, no
    /// S3) and keeps the cache from filling with dead compaction outputs.
    #[serde(default = "d_true")]
    pub timefusion_evict_after_compaction: bool,
    /// Advance the post-commit snapshot by appending only the files the commit
    /// added, instead of re-materializing the whole active file set (2-8s over
    /// 26k files every flush in prod). Produces an identical file set — a
    /// faster, equivalent replay, safe regardless of writer count. Off reverts
    /// to the full re-materialize per commit.
    #[serde(default = "d_true")]
    pub timefusion_incremental_snapshot: bool,
    /// Belt-and-suspenders for the above: every Nth commit per table, drop the
    /// materialized files and re-materialize from S3 truth, bounding any drift
    /// from an incremental-replay bug. 0 disables reconciliation.
    #[serde(default = "d_snapshot_reconcile")]
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
    #[serde(default = "d_true")]
    pub timefusion_repair_resume_enabled: bool,
    /// Days back (plus today) the dedup sweep scans. See `d_dedup_lookback_days`.
    #[serde(default = "d_dedup_lookback_days")]
    pub timefusion_dedup_lookback_days: u64,
    /// Run the legacy partition-wide dedup probe as an audit/fallback. Dirty
    /// sealed bins are the normal maintenance path.
    #[serde(default)]
    pub timefusion_dedup_sweep_fallback: bool,
    /// Master kill switch for rollup builds and reads.
    #[serde(default)]
    pub timefusion_rollup_enabled: bool,
    /// Read-side rollup routing gate. Builds can run while this stays off.
    #[serde(default)]
    pub timefusion_rollup_read_enabled: bool,
    /// Allow raw fringes and a live raw tail around certified rollup windows.
    #[serde(default)]
    pub timefusion_rollup_realtime_tail: bool,
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
    #[serde(default = "d_rollup_backfill_days")]
    pub timefusion_rollup_backfill_days: u16,
    #[serde(default = "d_rollup_backfill_schedule")]
    pub timefusion_rollup_backfill_schedule: String,
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
    #[serde(default = "d_true")]
    pub timefusion_read_dedup_skip_swept: bool,
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
    #[serde(default = "d_true")]
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
    #[serde(default = "d_true")]
    pub timefusion_read_dedup_bounded: bool,
    /// Answer gate-eligible `SELECT COUNT(*) ... WHERE project_id AND
    /// timestamp range` from Delta add-action stats (zero parquet IO). Only
    /// fires when the window is fully flushed, dedup-provably-clean, and
    /// every overlapping file lies entirely inside the window — otherwise
    /// the normal scan runs. See src/count_pushdown.rs.
    #[serde(default = "d_true")]
    pub timefusion_count_pushdown: bool,
    /// Per-shard COMPRESSED-bytes target for a dedup chunk rewrite (`sum(add.size)`).
    /// The rewrite is split into `ceil(compressed_bytes / this)` hash-bucketed passes
    /// so each pass reads ~this much. 0 disables this ceiling's contribution to the
    /// shard count. See `d_dedup_max_rewrite_bytes`.
    #[serde(default = "d_dedup_max_rewrite_bytes")]
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
    #[serde(default = "d_dedup_max_decoded_bytes")]
    pub timefusion_dedup_max_decoded_bytes: u64,
    /// Compressed→decoded inflation factor used to estimate a dedup chunk's
    /// in-memory footprint when per-file `num_records` stats are unavailable.
    /// See `d_dedup_decode_inflation`.
    #[serde(default = "d_dedup_decode_inflation")]
    pub timefusion_dedup_decode_inflation: u64,
    /// Estimated decoded Arrow bytes per row, used with per-file `num_records`
    /// to size a dedup chunk's in-memory footprint. otel spans carry wide
    /// Variant/JSON bodies; 4 KiB is a conservative average. See
    /// `d_dedup_bytes_per_row`.
    #[serde(default = "d_dedup_bytes_per_row")]
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
    #[serde(default = "d_dml_merge_concurrency")]
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
    #[serde(default = "d_true")]
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
    #[serde(default = "d_true")]
    pub timefusion_dml_merge_append_rebase: bool,
    /// Push a `target.key IN (source key values)` filter into the DV merge's
    /// per-file scan so parquet bloom filters prune files/row-groups holding none
    /// of the source keys — turning a whole-window enrichment scan into a few-file
    /// scan. Sound (bloom never false-negatives); on by default. Kill-switch:
    /// `TIMEFUSION_DML_MERGE_KEY_PRUNE=false` reverts to scanning all window files.
    #[serde(default = "d_true")]
    pub timefusion_dml_merge_key_prune: bool,
}

impl MaintenanceConfig {
    fn selected(project_id: &str, projects: Option<&str>) -> bool {
        projects.is_none_or(|projects| projects.trim().is_empty() || projects.split(',').map(str::trim).any(|project| project == project_id))
    }

    /// Builds run for EVERY project once rollups are on.
    ///
    /// There was a per-project canary allow-list here. It was deleted because a
    /// hidden list of project UUIDs is a debugging trap: "why has this project
    /// no rollup" has an answer that lives in an env var nobody remembers
    /// setting, and any project created after the list was written silently
    /// never gets built.
    pub fn rollup_build_enabled(&self) -> bool {
        self.timefusion_rollup_enabled
    }

    pub fn rollup_read_enabled_for(&self, project_id: &str) -> bool {
        self.timefusion_rollup_enabled && self.timefusion_rollup_read_enabled && Self::selected(project_id, self.timefusion_rollup_read_projects.as_deref())
    }

    /// Flush escalation-sort pool in bytes. Floored so a misconfigured 0 can't
    /// build a zero-sized pool that fails every sort.
    pub fn flush_sort_pool_bytes(&self) -> usize {
        (self.timefusion_flush_sort_pool_mb.max(64) as usize).saturating_mul(1 << 20)
    }
}

/// Which DataFusion `MemoryPool` to back the runtime with.
///
/// - `Greedy` (default): all consumers share the full pool; first-come,
///   first-served. Right for write-heavy workloads where INSERTs dominate
///   and per-statement memory needs vary widely (e.g. one batch is 50 MB
///   of Arrow, another is 5 MB). FairSpillPool would slice the pool into
///   per-consumer quotas (`pool / num_consumers`) and reject any consumer
///   whose batch exceeded its slot — bit prod when ~30 concurrent INSERTs
///   each got a ~76 MB slot and every 700-row batch hit `Memory limit
///   exceeded`.
/// - `FairSpill`: slot-per-consumer fairness. Better for ad-hoc query
///   workloads with many concurrent users where one large query
///   shouldn't starve the others. Not the right default for ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPoolKind {
    Greedy,
    FairSpill,
}

fn d_memory_pool() -> MemoryPoolKind {
    MemoryPoolKind::Greedy
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    // Formerly `timefusion_memory_limit_gb` — now `derived.memory_limit_bytes()`.
    // Formerly `timefusion_maintenance_pool_gb` — now
    // `derived.maintenance_pool_bytes()` (see DerivedBudget, §4 of the
    // compaction redesign doc). The 07-20/21 starvation this knob fixed
    // (25 GB-box clamp vs a 188 GB box) is what the derivation replaces.
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default = "d_memory_pool")]
    pub timefusion_memory_pool: MemoryPoolKind,
    #[serde(default = "d_true")]
    pub timefusion_tracing_record_metrics: bool,
    /// DataFusion `target_partitions` for query + maintenance sessions. 0 =
    /// auto: `config::apply()` derives it from the container's CPU quota
    /// (num_cpus ignores the CFS quota, oversubscribing throttled containers).
    /// A non-zero env (`TIMEFUSION_QUERY_PARTITIONS`) wins. 0 also when unset in
    /// tests → sessions keep DataFusion's default.
    #[serde(default = "d_query_partitions")]
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
    #[serde(default = "d_max_concurrent_scan_readers")]
    pub timefusion_max_concurrent_scan_readers: usize,
    #[serde(default = "d_wide_scan_lookback_hours")]
    pub timefusion_wide_scan_lookback_hours: u64,
    /// Depth alone badly over-fires the gate above. Lookback is only a PROXY
    /// for decode heap, and once file pruning works the proxy breaks — a query
    /// reading one small file at a long lookback was queued behind a saturated
    /// gate for no reason. So a scan is gated only when it is deep AND actually
    /// selected real work: more than `..._max_files` files or `..._max_mb` of
    /// them, counted from the plan's file groups after pruning. The wide scan
    /// that caused the original OOM still selects hundreds of files and stays
    /// gated; a deep-but-pruned dashboard query no longer waits behind it.
    #[serde(default = "d_wide_scan_max_files")]
    pub timefusion_wide_scan_max_files: usize,
    #[serde(default = "d_wide_scan_max_mb")]
    pub timefusion_wide_scan_max_mb: u64,
    /// Cross-connection plan-cache capacity (unique canonical/shape templates).
    /// 256 thrashed in prod (evicting ~half every ~60s); 1024 holds the working
    /// set with room to spare. Each entry is one LogicalPlan (~KBs).
    #[serde(default = "d_plan_cache_capacity")]
    pub timefusion_plan_cache_capacity: usize,
    /// Route `now()`/`current_timestamp` SELECTs through the shape cache (time
    /// fn parameterized to a fresh per-query instant) instead of bypassing it.
    /// On by default: prod CPU flamegraphs showed ~25% of CPU in
    /// SessionState::optimize from now()-bearing dashboard cache misses. The
    /// cached artifact is a placeholder plan template; the instant is re-bound
    /// per query, so windows never freeze. Set =false to disable in an emergency.
    #[serde(default = "d_true")]
    pub timefusion_plan_cache_time_fns: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "d_otlp_endpoint")]
    pub otel_exporter_otlp_endpoint: String,
    #[serde(default = "d_service_name")]
    pub otel_service_name: String,
    #[serde(default = "d_service_version")]
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

    /// Builds follow the global switch and NOTHING else. The read side keeps
    /// its canary list; the build side deliberately has none, so a project
    /// missing a rollup is never explained by a forgotten env var.
    #[test]
    fn rollup_builds_follow_the_global_switch_for_every_project() {
        let mut config = AppConfig::default().maintenance;
        assert!(!config.rollup_build_enabled());
        assert!(!config.rollup_read_enabled_for("project-a"));

        config.timefusion_rollup_enabled = true;
        config.timefusion_rollup_read_enabled = true;
        config.timefusion_rollup_read_projects = Some("project-b".into());
        for project in ["project-a", "project-b", "project-c", "a-project-created-tomorrow"] {
            assert!(config.rollup_build_enabled(), "builds must cover every project: {project}");
        }
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
        assert_eq!(config.cache.timefusion_cache_recent_days, 8);
        assert_eq!(config.memory.timefusion_wide_scan_max_mb, 64);
        assert!(config.maintenance.timefusion_warm_after_compaction);
        assert!(config.maintenance.timefusion_evict_after_compaction);
        // Merge-on-read DV is the default write path (and thus what all test
        // harnesses that build from AppConfig::default() exercise).
        assert!(config.maintenance.timefusion_use_deletion_vectors);
        assert!(!config.maintenance.timefusion_warm_full_files);
        assert_eq!(config.maintenance.timefusion_warm_recency_days, 1);
        assert_eq!(config.maintenance.timefusion_warm_concurrency, 16);
        // Durable-by-default WAL: an async fsync default let OOM-kills tear the
        // mmap tail and silently quarantine acked rows. Pin the durable defaults.
        assert_eq!(config.buffer.wal_fsync_mode(), WalFsyncMode::SyncEach);
        assert!(config.buffer.wal_ack_fsync());
        // Compression tiers ascend hot < warm < cold; intermediate (same-day
        // rewrites that nightly consolidate/recompress will rewrite anyway)
        // sits below hot and stays eligible for re-tiering.
        let p = &config.parquet;
        assert_eq!(p.timefusion_zstd_level_intermediate, 1);
        assert!(p.timefusion_zstd_level_intermediate < p.timefusion_zstd_compression_level);
        assert!(p.timefusion_zstd_level_intermediate < p.timefusion_zstd_level_warm);
        assert!(p.timefusion_zstd_level_intermediate < p.timefusion_zstd_level_cold);
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
        // 5 with the 30G post-slack pool: fewer concurrent per-project sorts is
        // the intended trade for not OOMing the box.
        assert!((4..=11).contains(&k), "K={k} outside the expected 4..=11 range");
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

    /// Certification must reach back far enough to serve a 30d query.
    ///
    /// `dedup_sweep` is the only caller of `record_certification`, scoped to
    /// `today - d_dedup_lookback_days ..= today`, which is therefore a hard
    /// ceiling on the longest window that can ever route to a rollup. At a
    /// former default of 1, nothing past yesterday could certify (30d queries
    /// timed out). Dropping below 30 silently reintroduces that.
    #[test]
    fn certification_window_covers_a_thirty_day_query() {
        assert!(super::d_dedup_lookback_days() >= 30, "the dedup sweep is what certifies partitions; below 30d no 30d query can ever route to a rollup");
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

    // Small box (16 GiB / 4 cores): degrades to K=1, nothing underflows/zeroes.
    #[test]
    fn derived_budget_small_box_degrades_to_k1() {
        let tiny = DerivedBudget::from_limits(8 * GIB, 4);
        let tiny_sum =
            tiny.query_pool_bytes() + tiny.buffer_max_bytes() + tiny.foyer_memory_bytes() + tiny.writer_reserve_bytes() + tiny.maintenance_pool_bytes();
        assert!(tiny_sum <= 8 * GIB, "8 GiB box over-committed: {tiny_sum}");
        let b = DerivedBudget::from_limits(16 * GIB, 4);
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
        let d = super::d_repair_lookback_days();
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
    tune("TIMEFUSION_QUERY_PARTITIONS", &mut config.memory.timefusion_query_partitions, cpus, "");

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
