use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Bytes per MiB / GiB — used by the `*_bytes()` size accessors below so the
/// `* 1024 * 1024` chains don't repeat (and read as the unit they mean).
const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Self-sizing budget tree (docs/compaction-redesign-2026-07-29.md §4).
//
// Replaces hand-set memory/concurrency env vars (config drift was the direct
// cause of the 07-21 90GB-budget-vs-85GB-limit crash loop) with a derivation
// from the container's actual cgroup limits. Detection is pure-parseable
// (`parse_*` fns take `&str`, doctested) so a misread `/sys/fs/cgroup` file
// can be reproduced without a container.
// ---------------------------------------------------------------------------

/// Read a `/proc` or `/sys` file and parse it; `None` if either step fails —
/// every detector below is a fallback chain over these.
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
    // `mut` is the iterator's own cursor — two positional fields, no collection.
    let mut parts = content.split_whitespace();
    let (quota, period) = (parts.next()?.parse::<f64>().ok()?, parts.next()?.parse::<f64>().ok()?);
    (quota > 0.0 && period > 0.0).then(|| ((quota / period).ceil() as usize).max(1))
}

/// Detect the effective memory limit in bytes: cgroup v2 → cgroup v1 →
/// `/proc/meminfo` total → a conservative 8 GiB floor. Never panics.
fn detect_memory_limit_bytes() -> usize {
    read_parsed("/sys/fs/cgroup/memory.max", parse_cgroup_v2_memory_max)
        .or_else(|| read_parsed("/sys/fs/cgroup/memory/memory.limit_in_bytes", parse_cgroup_v1_memory_limit))
        // No cgroup limit → not a managed container. An explicit env override is
        // safe to honor HERE and only here (off-box CLI / dev boxes): prod always
        // runs under a cgroup, so the 2026-06-11 misconfigured-knob OOM-loop
        // cannot recur through this path.
        .or_else(|| {
            env_memory_override_bytes().inspect(|v| tracing::warn!("budget tree: no cgroup limit; using TIMEFUSION_MEMORY_LIMIT_GB override ({} GiB)", v / GIB))
        })
        // Shared host: budget HALF the machine, loudly. Sizing from full host
        // RAM inside a container is the 2026-06-11 memcg OOM-loop (16
        // kills/24h), so the fallback stays conservative.
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

/// `TIMEFUSION_MEMORY_BUDGET_GB`: size the whole tree BELOW the cgroup limit.
///
/// Unlike the deleted per-consumer knobs this is a *single* input — every budget
/// still derives from it, so the shares can never drift out of proportion with
/// each other, which is the failure mode that got those knobs removed.
///
/// Why it's needed: the tree budgets 100% of the cgroup. That's right for a
/// dedicated box and wrong for a shared one. Prod runs a 120 GiB TF container on
/// a 188 GB host alongside ~62 GB of other services, so "TF may use its whole
/// cgroup" oversubscribes the HOST — TF grows into its entitlement and the
/// kernel OOM-kills it (repeatedly, 2026-07-31). Lowering the container limit
/// fixes it too but needs an orchestrator change and a redeploy; this lets the
/// process size itself to the share it should actually take.
///
/// Only ever LOWERS the effective limit — budgeting above the cgroup is never
/// valid, so an over-large value is clamped rather than honoured.
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
    // cgroup v2 `cpu.max`, then v1 `cfs_quota_us`/`cfs_period_us`; a quota can
    // exceed host parallelism on misconfigured hosts, so clamp. THE process-wide
    // core detector — `config::apply` reads it too, so
    // partitions and the budget tree can never size from different answers.
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

/// Which reservation shape the budget tree derives. A one-shot maintenance
/// CLI (`optimize` / `redrive-dml` / `migrate-columns`) serves no pgwire
/// queries and starts no ingest, yet under the server shape it still pays
/// those reservations — inside a 7 GiB k8s pod the maintenance pool lands on
/// its 1 GiB floor and whale-bin sorts die admitting their first batch
/// (2026-08-02). The CLI shape hands maintenance nearly the whole cgroup.
/// Selected via `TIMEFUSION_BUDGET_PROFILE=maintenance-cli`, set by `main`
/// for CLI subcommands before config init — never by operators.
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

// 0.20 (24 GiB prod), down from 0.25: sampled pool usage sat at 0 while the
// 70% memory brake — only ~2 GiB above the committed total — chronically
// halted light-compaction waves (prod 2026-08-05: brake fired mid-tick with
// 9 of 9 hot metrics projects still pending). The freed 6 GiB is brake
// headroom; a query past its pool spills (one slower scan), while a stopped
// hot-tail compaction backlogs the whole table.
const QUERY_POOL_FRACTION: f64 = 0.20;
/// Share of the limit reserved for consumers no pool tracks — parquet decode
/// heap, pgwire parse ASTs, allocator overhead (measured ~10-20 GiB on prod,
/// 2026-08-03). Carved out before maintenance takes the remainder so the
/// budget tree can never sanction more than the cgroup holds.
const UNTRACKED_SLACK_FRACTION: f64 = 0.15;
/// Ingest MemBuffer share of the limit. Reproduces today's working ratio
/// (24 GiB of a 120 GiB box).
const INGEST_BUFFER_FRACTION: f64 = 0.20;
/// Foyer read-cache share. Doc: today's 4 GiB of 120 is the most-starved
/// consumer relative to impact (cache hit-rate is what seal-lag/thrash
/// history says query latency lives on) — deliberately larger than the
/// previous ~3.3% so measured headroom goes there first.
const FOYER_MEMORY_FRACTION: f64 = 0.10;
/// Per-(rewrite-permit × merge-task) delta-rs writer buffer. Previously
/// budgeted nowhere — the 06-11 OOM was exactly this gap.
const WRITER_RESERVE_PER_TASK_BYTES: usize = 3 * GIB / 2;
/// delta-rs concurrent merge tasks per optimize run (unchanged default).
const OPTIMIZE_MERGE_TASKS: usize = 2;
/// Cap on files per optimize merge bin. Byte-only bins pack hundreds of
/// tiny files into one rewrite whose merge fan-in (streams × decode batch)
/// or blocking-sort input scales with fragmentation — memory demand peaks
/// exactly when compaction is most needed (SortPreservingMerge exhaustion
/// 2026-07-23, OOM #3 2026-07-27). 32 × ~35MB batches ≈ ~1GB peak per
/// merge; repeated cron passes converge fragmented partitions to target.
const OPTIMIZE_MAX_FILES_PER_BIN: NonZeroUsize = NonZeroUsize::new(32).unwrap();
/// Concurrent heavy maintenance rewrites (dedup/optimize/recompress).
/// Formerly `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`; held at 2 until
/// 2026-08-03 (06-11 OOM was uncapped concurrency, not memory sizing).
/// 3 since 2026-08-03 morning — paired with HALVING `d_dedup_max_decoded_bytes`
/// 4→2 GiB; 4 since 2026-08-03 evening: 4 × 2 GiB lands exactly on the
/// original 8 GiB envelope, and RSS held 30-45 GB all day under
/// `dirty_decay_ms:0` while the queue still net-climbed (enrichment
/// re-dirties today's bins faster than 3 permits clear them).
/// Do NOT bump further without shrinking the per-shard budget again: that
/// is the 2026-07-04-class fan-in OOM.
/// 10 since 2026-08-17, at the full 4 GiB per-sort budget — a deliberate
/// EXPANSION of the envelope (4 x 4 GiB -> 10 x 4 GiB), not a re-balance.
///
/// Measured on prod at 12 coordinator jobs and 4 permits: `permit_wait_ms` p50
/// 118,896 (2.0 min) and max 843,330 (14 min), with 13 unit timeouts. Workers
/// spent most of their wall clock BLOCKED on this semaphore rather than
/// working, which is why the live frontier fell 2.2h behind, rollup coverage
/// froze for two hours, and `tasks_pending` sat flat at ~127k while all 12 jobs
/// read as "running". Raising coordinator jobs cannot help while this is small:
/// 24 jobs was tried and reverted because it only queued deeper here.
///
/// The historical caution ("do not bump without shrinking the per-shard
/// budget", after the 2026-07-04 fan-in OOM) was written for a far tighter box.
/// This one runs at 7-13% of an 85.9 GiB cgroup, and `PER_SORT_BUDGET_BYTES` is
/// a spill THRESHOLD on a FairSpillPool rather than a reservation, so the heavy
/// share bounds real memory while extra permits buy parallelism and spill.
/// The 85% memory brake remains the backstop. Watch `permit_wait_ms`, RSS
/// against that brake, and `occ_conflicts_total`.
const HEAVY_REWRITE_PERMITS: usize = 10;
/// Per-sort budget. 8 GiB (from the legacy 5.8 GiB blocking-sort peak) capped
/// wave concurrency k at 1-2 on prod's 24 GiB pool — hot compact ran serial
/// on a 48-core box while ticks overran 2-3x (2026-07-30). Sorts run on a
/// FairSpillPool with a dedicated spill dir, so this is a spill THRESHOLD,
/// not a hard requirement: exceeding it degrades to bounded disk spill.
/// Staging is R2-latency-dominated, so more spilling-capable parallel sorts
/// beat fewer comfortable ones. 4 GiB doubles k; the memory brake (85% of
/// cgroup) backstops the total.
/// 2 GiB since 2026-08-17, HALVED to pay for `HEAVY_REWRITE_PERMITS` 4 -> 10.
///
/// Leaving it at 4 GiB while permits went to 10 is what the permits comment
/// warns against, and prod proved the warning: an OOM kill at anon-rss
/// 124.9 GB (09:39:02, kernel names `maintenance-wor` as the caller), two
/// minutes after RSS was 11.9 GB. Reverting the sealed share alone did not hold
/// — RSS climbed 7.5 -> 15.2 GB within four minutes of that revert deploying,
/// the same shape as the run-up to the kill.
///
/// A spill THRESHOLD on a FairSpillPool, not a reservation: a sort that exceeds
/// it degrades to bounded disk spill rather than failing, and staging is
/// object-store-latency-dominated, so more spilling-capable parallel sorts beat
/// fewer comfortable ones. Halving keeps the extra concurrency while putting the
/// fan-in envelope back near where it ran clean for 12 hours: 10 x 2 = 20 GiB,
/// against the old 4 x 4 = 16 GiB, rather than 40.
const PER_SORT_BUDGET_BYTES: usize = 2 * GIB;
/// Heavy maintenance keeps at least this share of the maintenance pool.
///
/// 0.40 since 2026-08-13, from 0.25. This is a REBALANCE inside the existing
/// pool, not a bigger pool: the maintenance total is unchanged, so nothing is
/// taken from the untracked slack the 2026-08-02 OOM series was traced to.
///
/// It follows the workload. Hot-tail packing (the light share) converged once
/// it stopped being starved — bins now select 5-6 files and 2-6 projects have
/// work per tick, against 50-65-file partitions before — while the dirty-bin
/// dedup queue was still growing at roughly 13 enqueued per bin drained. Heavy
/// is where the backlog now is.
///
/// 0.40 specifically, because `light_optimize_k` divides the light share by
/// `PER_SORT_BUDGET_BYTES`: at 0.60 light on prod's 20.5 GiB pool that is
/// 12.3 GiB, which still yields K=3. Going further would drop packing to K=2
/// and buy heavy little, since its concurrency is bounded by the rewrite
/// permits rather than by bytes.
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
            // No queries, no ingest: token slices for the scan-side query pool
            // and foyer, the same writer-reserve cap (delta-rs output buffers
            // are real in a CLI), everything else to maintenance. An 8 GiB pod
            // derives ~6 GiB of sort/spill memory instead of the 1 GiB floor.
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
        // Fixed fraction, not the old TIMEFUSION_MEMORY_FRACTION knob: prod set
        // 0.75 calibrated against the hand-set 26 GB limit; applied to the real
        // 120 GiB cgroup it would take 90 GiB and crush maintenance to the
        // floor (K=1) — the drift-class bug this tree exists to kill. 0.20 of
        // the real limit (24 GiB prod) is ~1.25x the old effective pool.
        let query_pool_bytes = (memory_limit_bytes as f64 * QUERY_POOL_FRACTION) as usize;
        let ingest_buffer_bytes = (memory_limit_bytes as f64 * INGEST_BUFFER_FRACTION) as usize;
        let foyer_memory_bytes = (memory_limit_bytes as f64 * FOYER_MEMORY_FRACTION) as usize;
        // Capped at 10% of the limit: the full 6 GiB reserve on an 8 GiB dev
        // box budgeted 142% of the container — the drift class this tree kills.
        let writer_reserve_bytes = (HEAVY_REWRITE_PERMITS * OPTIMIZE_MERGE_TASKS * WRITER_RESERVE_PER_TASK_BYTES).min(memory_limit_bytes / 10);
        // UNTRACKED-CONSUMER SLACK, carved out BEFORE maintenance takes the
        // remainder. Without it the tree hands maintenance everything left and
        // the box budgets itself to `slack_mb=0 oversubscribed=true` — on prod
        // that was query 30G + ingest 24G + foyer 12G + writer 6G + maintenance
        // 48G = 114G of sanctioned allocation on a 120G cgroup, and the
        // consumers no pool tracks (parquet decode ~10-20G measured 2026-08-03,
        // giant-INSERT parse ASTs ~7G live, allocator overhead) pushed it over
        // roughly hourly (70 cgroup kills over 08-01/02). Every subsystem
        // behaved "legally"; the sum was the bug. 15% ≈ 18G on prod covers the
        // measured untracked peak; maintenance shrinks (48G → 30G on prod),
        // which the whale campaign proved harmless (512MB bins sort+spill fine
        // on 6G pools with 256-row admission batches).
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

    /// Formerly `MemoryConfig::maintenance_pool_bytes` (explicit-knob-or-clamped-derive).
    /// Hand `bytes` back from the maintenance pool, never below the floor.
    /// Returns what was actually surrendered.
    ///
    /// Maintenance is the RESIDUAL claimant — the tree gives it
    /// `limit - reserved` — so it is exactly the pool that silently absorbed
    /// every ceiling `reserved` forgot: MemBuffer's 120% admission overshoot,
    /// the tantivy writer peak, and the DataFusion metadata cache. Shrinking it
    /// to match the audit is restoring the intended reservation, not taxing
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
    /// split because the coordinator is now the primary maintenance path.
    ///
    /// Admission lets each job hold `MAX_DECODED_BYTES`, so the pool they share
    /// must be that ceiling times the jobs that can be in flight. It used to be
    /// `MAX_DECODED_BYTES` flat — the admission constant reused as a pool size,
    /// correct only while `coordinator_jobs` was 1. At 16 jobs FairSpill sliced
    /// 512 MB sixteen ways, below `ExternalSorterMerge`'s 32 MB floor, so units
    /// FAILED instead of spilling: prod 2026-08-17 logged 72 `Failed to
    /// allocate additional 32.0 MB ... pool_size: 512.0 MB` in 25 minutes.
    /// Capped at a QUARTER of the maintenance pool, not a half.
    ///
    /// `jobs x MAX_DECODED_BYTES` is the ceiling a fully-rollup-loaded
    /// coordinator could want, but it is not what the coordinator typically
    /// runs: only its rollup units draw on this pool
    /// (`bounded_rollup_maintenance_context`), while its DEDUP units sort on
    /// the heavy share. Letting the ceiling take half the pool starved the
    /// share doing the sorting — prod 2026-08-17, immediately after the
    /// over-commit fix sized heavy honestly at 3.4 GiB:
    ///
    ///   dedup: Not enough memory to continue external sort ... Failed to
    ///   allocate additional 637.9 MB ... pool_size: 3.4 GB
    ///
    /// A quarter still leaves each of 16 jobs ~260 MB — far above
    /// `ExternalSorterMerge`'s 32 MB floor, which is the failure that sizing
    /// this pool by job count was introduced to prevent — and hands the
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
    /// PINNED CONSTANT (not box-derived): concurrency caps guard against the
    /// 06-11 uncapped-rewrite OOM, not against a sizing miss.
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
    /// Held at a hard-coded 1 through 2026-08-16, alongside admission tokens of
    /// `(cpu 1, reads 1, writes 1)`. That serialization is what put the queue
    /// 20.3 TB and 18,397 tasks deep with `decoded_bytes_used` pinned at one
    /// 512 MiB unit against a ~64 GiB decode budget — 99% idle — and it is the
    /// root cause of the dead rollups, not a separate problem: no dedup commits
    /// means `record_certification` never fires, and an uncertified partition
    /// both keeps `DedupExec` in every plan and denies rollup routing its
    /// contiguous certified prefix.
    ///
    /// What made the cap necessary was maintenance sharing Tokio workers with
    /// pgwire and starving health checks. That is fixed properly by the
    /// dedicated maintenance runtime, so bound this by the box instead: each
    /// unit reserves at most `MAX_DECODED_BYTES` (512 MiB), so the memory term
    /// is exact. `TIMEFUSION_COORDINATOR_JOB_WORKERS=1` restores the old
    /// serialized behavior exactly.
    pub fn coordinator_jobs(&self) -> usize {
        std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").ok().and_then(|v| v.parse::<usize>().ok()).filter(|n| *n > 0).unwrap_or_else(|| {
            let mem_bound = self.maintenance_pool_bytes / (512 * 1024 * 1024);
            // cores/8 (cap 6) was the first step off the hard-coded 1, chosen
            // conservatively because nothing had been measured yet. Six hours on
            // prod since then: 6 jobs held ~3 GiB of tracked decode with RSS at
            // 12% of the cgroup, zero OCC conflicts, zero restarts, and the live
            // frontier keeping up (`eligible_watermark_lag_seconds` 0) — while
            // the 8 TB sealed-compaction debt did not drain at all and
            // `oldest_task_age_seconds` kept climbing. The box is not the
            // constraint; the clamp is. cores/4 (cap 12) is 6 GiB of tracked
            // decode against 85.9 GiB, still far below the 48-way exposure that
            // starved health checks on 2026-08-16, and the dedicated maintenance
            // runtime is what protects liveness either way.
            // Raised again 2026-08-17 after measuring the actual constraint.
            // At 12 jobs prod completed ~4,400 tasks/hour (~1.2/s) against ~148k
            // tasks of genuine backfill work (30 days x 11 projects x ~450 tasks
            // per partition) — ~34 hours, with RSS at 8% of the cgroup, OCC
            // conflicts at 0, and 12 of 48 cores in use. Throughput, not the
            // split between frontier and sealed work, is what bounds rollup
            // coverage. 24 jobs is ~12 GiB of tracked decode against 85.9 GiB.
            // Back to cores/4 (cap 12) after measuring 24 on prod. Coordinator
            // jobs are NOT the innermost limit: an admitted unit still waits on
            // the much smaller rewrite/sort permit pools (HEAVY_REWRITE_PERMITS
            // is 4). At 24 jobs those slots filled with units blocked on a
            // permit — permit_wait_ms reached 271,692 (4.5 min), 10 units timed
            // out, and completions collapsed from ~0.6/s to 0.035/s. Wider than
            // the inner pool only converts coordinator slots into queueing.
            // cores/3 (cap 16) since 2026-08-17, once HEAVY_REWRITE_PERMITS went
            // 4 -> 10. Jobs are only useful up to the inner permit pool: at
            // 12 jobs / 4 permits the ratio was 3:1 and `permit_wait_ms` p50 was
            // 2 minutes, which is why 24 jobs had to be reverted. 16 jobs
            // against 10 permits is 1.6:1 — headroom for the operations that do
            // not take a rewrite permit at all, without rebuilding the queue.
            //
            // Memory term stays exact: 16 x MAX_DECODED_BYTES = 8 GiB against a
            // ~16.6 GiB maintenance pool, asserted below.
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
    /// 80% since 2026-08-13, from 70%. The number to reason about is not this
    /// fraction but where it lands against the cgroup the OOM killer watches,
    /// and those differ: `memory_limit_bytes` is the budgeted limit, itself
    /// capped below the cgroup (prod runs `TIMEFUSION_MEMORY_BUDGET_GB=82` in a
    /// 96 GiB container). So 70% was really 60% of the cgroup — 38 GiB of
    /// headroom the brake would never let maintenance use — and 80% is 68% of
    /// it, still leaving ~30 GiB.
    ///
    /// That distinction is why this is not a return to the 85% that failed: in
    /// the 2026-07-30 backlog-drain regime, allocation bursts between wave
    /// boundaries outran jemalloc purge in the 85%→100% window (contained OOM
    /// kills at 08:24/09:19/10:03, each costing a replay cycle). The comment
    /// there asked to revisit upward "once steady-state is the norm"; with
    /// compaction converged and the dirty-bin backlog draining, it is — and the
    /// brake had started firing on ~18% of ticks against transient dedup peaks
    /// while true RSS sat near 23 GiB.
    pub fn memory_brake_limit_bytes(&self) -> usize {
        (self.memory_limit_bytes as f64 * 0.80) as usize
    }

    /// WAL emergency-flush byte threshold, as a fraction of the ingest
    /// buffer rather than a free-standing constant (today's 6 GB threshold
    /// vs a 24 GB buffer had drifted 9× out of proportion — doc §4).
    pub fn wal_flush_byte_threshold(&self) -> u64 {
        // Floor at 4 GiB: the WAL counts PREALLOCATED file bytes (walrus
        // blocks are up to 1 GiB each, MAX_ALLOC), so a threshold below a few
        // blocks trips on preallocation alone — on a small box the derived
        // half-buffer value (~800 MB) fired at "1000 MB" of WAL holding 150 MB
        // of rows, draining open buckets before hard-limit backpressure could
        // engage (the e2e backpressure test caught this, as it did the
        // quarter-buffer default before it). Small boxes are guarded by the
        // file count + memory pressure; the byte ceiling is a prod-scale
        // replay bound, not a small-box valve.
        ((self.ingest_buffer_bytes / 2) as u64).max(4 * GIB as u64)
    }

    /// WAL emergency-flush file-count threshold: the legacy 200 as a FLOOR,
    /// scaled up on boxes with a bigger ingest buffer than the 24 GiB baseline.
    /// Never derived downward: 200 bounds restart REPLAY, not memory, and the
    /// first derivation (max(50, scaled)) tripped at 50 files on small boxes —
    /// the WAL emergency flush then drained the open bucket before the
    /// hard-limit backpressure path could ever engage, which is exactly the
    /// preemption the e2e backpressure test exists to catch (it did).
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
/// `TIMEFUSION_ALLOW_INSECURE_AUTH=true`. Both the pgwire and gRPC auth
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
const_default!(d_grpc_port: u16 = 50051);
const_default!(d_table_prefix: String = "timefusion");
const_default!(d_batch_queue_capacity: usize = 100_000_000);
const_default!(d_pgwire_user: String = "postgres");
const_default!(d_pgwire_max_statement_secs: u64 = 60);
// 60s (was 300s): a shorter flush interval bounds how much un-flushed WAL a
// restart must replay — startup/redeploy downtime is dominated by WAL replay
// (95.6s for 46,692 entries at 300s on 2026-07-13), and it scales ~linearly
// with this interval. Trade-off: ~5x more Delta commits / small files (handled
// by compaction/OPTIMIZE).
const_default!(d_flush_interval: u64 = 60);
// Flush dwell: a sealed-but-young bucket waits this long from CREATION before
// the periodic flush commits it, unless it is already big. -1 = one
// bucket_duration (the prod default), 0 = off (the test harnesses set this —
// they assert "sealed => next tick flushes"). See flush_completed_buckets.
const_default!(d_flush_dwell_secs: i64 = -1);
const_default!(d_retention_mins: u64 = 70);
// The local hot tier: demoted sealed buckets served as the scan's third leg.
//
// It holds WHATEVER FITS ON DISK (2026-08-14) — there is no time retention.
// It used to keep a fixed number of hours (6 -> 24 -> 72), which made the tier's
// value depend on guessing the right number and left disk unused: prod ran a
// 128GB cap on a box with 1.1TB free. GC now unlinks oldest-first purely to stay
// under `d_hot_tier_max_disk_gb`, so buying disk buys coverage directly, and
// `skip_for_lookback` reads the tier's MEASURED span rather than a setting.
const_default!(d_hot_tier_enabled: bool = true);
// 600GB of the box's 1.1TB free. Files are UNCOMPRESSED since 2026-08-14
// (~4x the bytes of the LZ4 era), so this holds roughly the coverage the old
// 128GB compressed cap did — bought with disk that was sitting idle instead of
// with decompression CPU and anon heap. Raise it to buy more history; that is
// now the only knob that changes how far back the tier reaches.
const_default!(d_hot_tier_max_disk_gb: u64 = 600);
const_default!(d_eviction_interval: u64 = 60);
const_default!(d_buffer_max_memory: usize = 4096);
const_default!(d_wal_shards_per_topic: usize = 4);
// Total graceful-shutdown budget shared by ALL serial shutdown phases
// (PGWire drain → gRPC drain → buffered-layer flush + cursor snapshot).
// Set to ~80% of the orchestrator's SIGTERM→SIGKILL grace (Docker/CapRover
// `StopGracePeriod`; prod is 90s) so the clean cursor snapshot always lands
// before SIGKILL — the previous per-phase 180s ceilings assumed 540s of
// grace nobody configured, and PGWire drain alone could eat the real grace
// before the flush or snapshot ever started (2026-06-11 deploy). Anything
// unflushed at the deadline is durable in the WAL and replays on next boot.
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
// N snapshot refreshes and N log-JSON parses where 1 would do (prod: ~1.5s commit
// cadence ≈ 2400 commits/hour). When enabled, one tick produces one commit per
// PHYSICAL table carrying every project's Add actions; parquet writes still fan
// out `flush_parallelism`-wide, only the commit is shared. Custom-storage projects
// have their own `_delta_log` and are never coalesced with default storage.
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
// fewer/larger Delta commits. Default 300s (5 min) — halved from the historical
// 600s to cut peak MemBuffer footprint (the current bucket is excluded from
// flushing, so this is the floor on how long a row accumulates in RAM). The
// trade-off is ~2× Delta commits / small files; high-throughput tenants can go
// lower still (60–120s), memory-relaxed deployments can raise it back.
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
// 3s ON by default since 2026-07-29: the drain hash-update storm (~3
// same-shape UPDATEs/s, each rewriting most of the hot partition) OOM-looped
// prod six times in one afternoon; coalescing collapses a window's statements
// into one rewrite. Set to 0 to restore the synchronous per-statement path.
const_default!(d_dml_coalesce_secs: u64 = 3);
// Watchdog for a single bucket's Delta commit inside `flush_bucket`. A hung S3
// commit / commit-lock wait otherwise pins `flush_lock` forever with no log:
// flushes freeing zero memory while inserts wedge at the hard limit (prod
// 2026-07-01 — 0 flushes, 0 errors, 1300+ rejects). On timeout the flush errors
// (counted in flush_failed + flush_stalled), releasing the lock so relief
// retries; rows stay in MemBuffer + WAL, so it's safe. Must exceed a normal
// backfill commit but stay well under retention.
//
// The CEILING, not the budget: `BufferedWriteLayer::adaptive_flush_timeout`
// contracts it as the ingest buffer fills. Read that function before changing
// this — prod has been wedged from both ends of the fixed-value trade (120s
// aborted legitimate multi-GB drains into a retry loop, 2026-07-02; 600s let a
// hung commit hold the global flush_lock until the ingest buffer filled and
// every tenant's INSERT was rejected, 2026-08-02). 600 stays right for the
// ceiling: with headroom, a slow-but-progressing commit should be allowed to
// finish, because aborting it wastes the work and the next attempt is no
// faster.
const_default!(d_flush_bucket_timeout_secs: u64 = 600);
// Durability mode for the WAL. One of:
//   "sync_each" — fsync after every entry (default; zero data-loss window, ~1ms per write)
//   "ms"        — async fsync every `wal_fsync_ms` (~200ms loss window; a torn
//                 mmap tail after OOM/SIGKILL quarantines acked entries — the
//                 2026-07 silent-loss incidents)
//   "none"      — never fsync (test/throwaway data only)
const_default!(d_wal_fsync_mode: String = "sync_each");
const_default!(d_wal_ack_fsync: bool = true);
// 0 = unset → derived (DerivedBudget::wal_flush_file_threshold); env-set wins.
const_default!(d_wal_max_files: usize = 0);
const_default!(d_wal_hard_limit_gb: u64 = 192);
const_default!(d_foyer_memory_mb: usize = 1024);
// Local disk is cheap and fast relative to S3 GETs, so default the cache large
// — servers run 500GB–1TB cache volumes. foyer creates the backing file sparse
// (no upfront allocation), but this is the logical ceiling at which it starts
// evicting, so it MUST stay <= the cache volume's free space or writes hit
// ENOSPC before eviction kicks in. Lower it on smaller disks.
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
// Delta checkpoint (≈ live file count) shrinks, which is the dominant driver of
// commit latency. Compression is per-row-group, so bigger files don't change bytes
// stored — the win is fewer files (smaller checkpoint, fewer S3 objects, cheaper
// query planning). Re-runs are cheap: Compact skips files already ≥ target.
// 512MB, not 1GB: a merge holds ~target-sized output buffers per concurrent task
// and the decompressed working set is ~17x the compressed target, so 1GB made the
// sort/merge step of the final consolidation memory-hostile on this box.
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
// Remove tombstones stay in every checkpoint for this long, and prod's 5-min
// compaction churn at the 7-day delta default accumulated 38.5k tombstones
// (93% of checkpoint actions) replayed on every snapshot refresh.
//
// 72h, not 24h: removed-but-unvacuumed files are the ONLY recovery source
// after a bad rewrite, and the default is what a deploy-wiped env falls back
// to. 2026-08-05: the 00:15 vacuum fired inside exactly that window (env
// wiped by an auto-deploy, replacement task blocked on the WAL lock) and
// permanently destroyed ~7.3M rows' recovery source at 24h retention. A
// 3-day floor keeps one bad daytime incident recoverable across a weekend.
const_default!(d_vacuum_retention: u64 = 72);
// Delta _delta_log (transaction-log) retention. Keeps the log directory small
// (~commit-rate × retention files) so every commit's version-discovery LIST stays
// cheap. Delta's default is 30 DAYS — which let the log grow to 68k objects and
// made each commit's list take ~35s (2026-06-25 DLQ-drain incident). Even 1 day
// regrew to ~6.7k objects (~3-5s/commit) under the multi-tenant per-project commit
// rate (2026-06-26), so hold a tighter 6h window. enableExpiredLogCleanup (default
// true) prunes during checkpoints; cross-project flush coalescing cuts the commit
// rate that drives growth.
const_default!(d_log_retention: u64 = 6);
const_default!(d_optimize_window_hours: u64 = 48);
const_default!(d_compact_min_files: usize = 5);
// Hot/today target stays small (32MB): the light job runs every 5 min on the
// current partition, which takes constant writes — a large target would rewrite
// the same growing files repeatedly (write amplification) and add in-process
// merge memory on the hot path. Consolidation to 256MB/512MB happens later, once
// the partition is sealed (warm optimize → daily cold consolidate).
// 256MB (was 32MB): on the 188GB/48-core box the small-merge-memory rationale is
// moot, and 32MB left the hot (today) partition as dozens of tiny files for a
// high-write project — recent 1h/3h queries were file-OPEN-latency bound (~62
// opens ≈ 1.2s). A larger target collapses today's sealed slices into a few
// large event-time-disjoint runs so recent queries open a handful of files.
const_default!(d_light_optimize_target: i64 = 256 * MIB as i64);
const_default!(d_writer_max_file_bytes: usize = 512 * MIB);
const_default!(d_repair_max_file_bytes: usize = 512 * MIB);
// 256 MiB. This was briefly raised to 4 GiB (2026-08-01) so that large prod
// flush buckets — ~170 MB compressed, ~2.9 GB in memory at the measured ~17x
// zstd ratio — would be sorted rather than written with no `sorting_columns`
// footer. That reasoning was right about the READ cost and wrong about what it
// would do to the box, so it is reverted:
//
// The flush sort is IN-PROCESS and, unlike compaction's, allocates OUTSIDE the
// DataFusion memory pool. Raising the ceiling authorised multi-GB untracked
// allocations on the INGEST path of a box whose pool hard limit is 23 GB. Both
// images carrying the raise OOM-killed (exit 137); the image before it was
// replaced by a normal deploy. That is a small sample on a box where OOMs are
// chronic, so it is suggestive rather than proven — but a multi-GB unpooled
// allocation on the ingest path is not worth defending on a suggestive prior.
//
// This is now an ESCALATION threshold, not a skip threshold: past it the flush
// sorts inside a DataFusion plan (`sort_flush_group_spilling`) whose pool is
// bounded and spills to disk, so the footer stays honest and the peak is
// bounded by the pool rather than by the bucket. 2 GiB keeps the fast
// in-process path for everything ordinary — the measured crossover where
// DataFusion's sort overtakes Arrow lexsort is ~262k rows / ~370 MB
// (benches/sort_strategy_benchmarks.rs: 0.63x at 372 MB, 0.33x at 1.5 GB), so
// above the threshold the pooled sort is also simply faster.
const_default!(d_sort_skip_bytes: usize = 2 * GIB);
const_default!(d_flush_sort_pool_mb: u64 = 1024);
const_default!(d_light_schedule: String = "0 */5 * * * *");
// Matches d_dedup_lookback_days: certification and rollup coverage must span
// the same horizon, or a day is certified but never rolled up (or vice versa).
const_default!(d_rollup_backfill_days: u16 = 35);
const_default!(d_rollup_backfill_schedule: String = "0 */10 * * * *");
const_default!(d_rollup_backfill_concurrency: usize = 3);
const_default!(d_footer_repair_schedule: String = "0 30 * * * *");
const_default!(d_footer_repair_budget_secs: u64 = 8640);
const_default!(d_repair_lookback_days: u64 = 31);
const_default!(d_optimize_schedule: String = "0 */30 * * * *");
// Daily cold consolidation sweep (02:30): bin-pack sealed partitions to the 512MB
// cold target. Calendar-age driven; idempotent (skips ≥-target files).
const_default!(d_consolidate_schedule: String = "0 30 2 * * *");
const_default!(d_consolidate_catchup_passes: usize = 4);
// Every 6h (not daily): tombstones leave checkpoints once older than the
// retention property, so vacuum must visit often enough to delete the files
// before their tombstones are pruned — a daily cadence against a 24h
// retention orphans most of a day's churn (VacuumMode::Full backstops any
// that slip through).
const_default!(d_vacuum_schedule: String = "0 15 */6 * * *");
// Out-of-band checkpoint + expired-log cleanup. These run in the delta-rs
// post-commit hook by default, but a hook failure (R2 500 on the checkpoint PUT
// or the bulk log ?delete) surfaces as a commit error AFTER the commit landed,
// which the flush path misreads as a failed commit and then deletes the
// committed parquet (2026-07-09 incident). Disabled on the commit path
// (base_commit_properties) and driven here instead, every 2 min, tolerant of
// R2 500s. Faster than the ~1.5s commit cadence so the log stays bounded.
const_default!(d_checkpoint_schedule: String = "0 */2 * * * *");
// Reconcile active Add entries against object-store truth: HEAD every live file
// and commit Remove actions for any that are missing. Repairs dangling Adds
// left by past commit-path parquet deletions (the 2026-07-09 incident left 14);
// a nonzero removal count means committed data was destroyed elsewhere.
const_default!(d_reconcile_schedule: String = "0 0 * * * *");
const_default!(d_tantivy_reconcile_schedule: String = "0 30 3 * * *");
const_default!(d_warm_recency_days: u64 = 1);
// 16: at concurrency 4 prod's 3.1k-file boot warm ran >55 min and was cut
// short by a restart every time; 16 finishes it in ~1-3 min. Footer GETs are
// ~64KB suffix ranges, well within R2/S3 burst limits.
const_default!(d_warm_concurrency: usize = 16);
const_default!(d_snapshot_reconcile: u64 = 500);
// Hard byte ceiling on the file set a single dedup chunk rewrite may
// materialize (sum of target parquet file sizes). Chunks over budget are
// SKIPPED loudly instead of rewritten — read-side dedup keeps queries
// correct, and the skip metric (timefusion.dedup.chunk_skipped) surfaces the
// debt. Guards against e.g. a z-ordered whole-day file (1GB+ on disk, several
// GB decompressed × copies) dragging the whole day into one rewrite.
// Kept in step with `d_dedup_max_decoded_bytes` — `dedup_shard_count` takes the
// MAX of the two shard counts, so leaving this at 2 GiB would let the compressed
// term silently cap sharding below what the decoded budget asked for.
const_default!(d_dedup_max_rewrite_bytes: u64 = GIB as u64 / 2);
// 512 MiB estimated decoded footprint (was 2 GiB; 4 GiB while rewrite permits
// were 2, halved when permits went to 3 so permits x budget stays ≤ 8 GiB in
// flight — see HEAVY_REWRITE_PERMITS). A chunk this large already dwarfs the
// DataFusion pool; larger chunks skip rather than risk the cgroup.
//
// Quartered again on 2026-08-13 to FUND SHARD CONCURRENCY, not to save memory:
// `dedup_shard_concurrency` runs `DEDUP_BIN_ARROW_BUDGET / this` shards at
// once, so a smaller shard buys parallelism at an unchanged peak — 4 x 512 MiB
// in flight is the same 2 GiB one shard used to hold alone. It also makes each
// unit finish well inside the per-bin deadline, which is the whole reason those
// otel_metrics bins were failing: a unit bigger than its budget produces
// nothing, forever.
const_default!(d_dedup_max_decoded_bytes: u64 = GIB as u64 / 2);
// 12× compressed→decoded: zstd on wide Variant/JSON otel rows routinely
// decodes 10-20×; 12 is a deliberately conservative floor.
const_default!(d_dedup_decode_inflation: u64 = 12);
// 4 KiB/row decoded estimate for otel spans (wide Variant/JSON bodies).
const_default!(d_dedup_bytes_per_row: u64 = 4096);
// Serial: each merge-update decodes + rewrites whole hot partitions with
// pool-invisible memory; 4-way stacking under the drain hash-update storm
// (~3 UPDATEs/s) drove the 2026-07-29 OOM crash-loop. Results are identical
// either way — permits only bound peak memory, excess statements queue.
const_default!(d_dml_merge_concurrency: usize = 1);
// 128 since 2026-08-03 (was 512 for a few hours, 32 before): 32/5min put a
// 58h floor under the 22.5k backlog, but 512 turned out to OOM the box —
// prod RSS climbed a constant ~7GB/min while staging ran (independent of
// query load) and the ~55min kill cycle matched the 512-bin pass length
// exactly: something pass-scoped accumulates (per-bin Delta provider /
// session / snapshot state) and frees only at pass end, which a 512-bin
// pass never reached. 128 completes a pass in ~10-15min — the pass-scoped
// growth caps near ~25-30GB — while still draining ~4x the original rate.
// How many days back (in addition to today) the dedup sweep covers. today-only
// left cross-flush dupes that landed in a prior-day partition (a late DLQ replay
// crossing midnight UTC) uncollapsed forever; 1 catches the day-boundary case.
// Arbitrarily-late replays still need read-side dedup — see the parity plan.
//
// 35 since 2026-08-16, because this window is not only about late replays: the
// sweep is the ONLY caller of `record_certification`, so it is also what decides
// how far back a partition can be certified duplicate-free. At 1, certification
// could never reach past yesterday, and since rollup routing needs a contiguous
// certified prefix across the whole query window, no 7d/14d/30d query could ever
// route — measured on prod as `never_certified_pct = 100.0`, `rollup_hits = 0`,
// and shipbubble taking 41s at 1d and timing out past 60s at 7d and beyond.
//
// This is affordable because the sweep is already O(partitions-changed), not
// O(window): a partition whose `dedup_clean_fp` still matches its live file
// fingerprint is skipped without a probe, the work list is already sorted
// newest-date-first, and `deadline` + `dedup_sweep_cursor` rotation bound and
// resume a truncated tick. The one-off cost is the first pass over each day that
// has never been certified, which is precisely the backlog the coordinator's
// job concurrency (2026-08-16, #101) exists to drain — do NOT raise this on a
// build without that change, or the 35x work list lands on a queue that commits
// nothing. 35 covers a 30d query with margin and matches `d_repair_lookback_days`.
const_default!(d_dedup_lookback_days: u64 = 35);
const_default!(d_query_partitions: usize = 0);
// Wide-scan admission guard. 16 concurrent Parquet decoders bounds untracked
// decode heap well under the 25 GB pool on the 188 GB/48-core box (48-way was
// the OOM). Wide observability windows are already latency-bound below this, so
// gating is near-free on speed. 2h lookback keeps the hot 1h/last-few-hours
// dashboards ungated (they page-prune to tiny per-file bytes) while 3d+/no-time
// scans queue. Both tunable via TIMEFUSION_{MAX_CONCURRENT_SCAN_READERS,WIDE_SCAN_LOOKBACK_HOURS}.
const_default!(d_max_concurrent_scan_readers: usize = 16);
const_default!(d_wide_scan_lookback_hours: u64 = 2);
// Sized against the incident the gate exists for: the 7-day dashboard opened
// hundreds of files at ~48-way parallelism.
//
// The FILE-COUNT half assumed "a recent-window scan selects a handful of files
// after pruning". That stopped being true once partitions fragmented to
// thousands of small files: on 2026-08-02 a 1h window on one tenant selected 48
// file groups, so the release could never fire and every dashboard past the 2h
// lookback queued behind the 16-permit semaphore. Measured cliff, same tenant,
// same query: 115min = 9.9s but 125min = TIMEOUT at 130s.
//
// BYTES are the honest proxy for decode heap; file COUNT is a proxy for a proxy,
// and it is the one fragmentation invalidates. Median prod file is ~0.1 MB, so
// 256 files is still only ~26 MB — the MB cap does the real bounding, and the
// 7-day case that motivated the gate blows through the MB cap regardless.
const_default!(d_wide_scan_max_files: usize = 256);
// Compressed parquet bytes understate transient Arrow decode heap by an order
// of magnitude on OTel data. A 222 MB historical file measured ~4 GiB of
// process growth while 48 row groups decoded in parallel (2026-08-04), so it
// must participate in the shared decode gate instead of using the small-scan
// exemption. 64 MB keeps genuinely small, well-pruned history reads ungated.
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
    /// Self-sizing budget tree, derived (not deserialized) once at
    /// construction time from the cgroup limit, optionally lowered by
    /// `TIMEFUSION_MEMORY_BUDGET_GB`. NOT `timefusion_memory_fraction` — that
    /// knob has been dead since the tree landed.
    #[serde(skip)]
    pub derived: DerivedBudget,
}

const_default!(d_tantivy_backfill_max_file_mb: u64 = 4096);
const_default!(d_tantivy_max_index_mb: u64 = 64);
// Sized against a working set, not a wish: the reaper only evicts what no
// query has opened recently, and every eviction costs a blob re-download on
// the next hit. 4 GB — the value this knob carried while it was dead code,
// enforced by nothing — would thrash the hot window at prod scale.
// Measured on prod 2026-08-12: 65 GB across 6499 leaf index dirs (~10 MB each),
// so 64 sits right at the live working set — the reaper trims rather than
// evicting the hot window on its first pass.
const_default!(d_tantivy_cache_disk_gb: u64 = 64);
const_default!(d_tantivy_cache_reap_schedule: String = "0 */10 * * * *");
// Level 3: index packing is on the flush hot path; level 19 cost ~88% of a CPU
// window per flush for ~10-15% smaller output (profiled 2026-07-05).
const_default!(d_tantivy_zstd_level: i32 = 3);
const_default!(d_tantivy_min_files: usize = 2);
const_default!(d_tantivy_prefilter_max_hits: usize = 100_000);
const_default!(d_tantivy_prefilter_min_selectivity_pct: u32 = 50);

/// Tantivy sidecar-index configuration. Indexing is always-on for any
/// table whose YAML schema declares `tantivy.indexed: true` on at least
/// one field. There is no override knob — schema is the single source of
/// truth.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde(default = "d_tantivy_max_index_mb")]
    pub timefusion_tantivy_max_index_size_mb: u64,
    /// Byte budget for the local extracted-index cache
    /// (`<timefusion_data_dir>/tantivy_cache`), enforced LRU-first by the
    /// "Tantivy cache reap" cron. That cron is the ONLY thing that deletes
    /// from that tree, and the tree shares a volume with the WAL — a full
    /// volume means failed WAL appends, not just a cold cache.
    #[serde(default = "d_tantivy_cache_disk_gb")]
    pub timefusion_tantivy_cache_disk_gb: u64,
    /// How often to enforce `timefusion_tantivy_cache_disk_gb`. Each sweep
    /// walks the whole cache tree, so this trades staleness of the bound
    /// against that walk; empty disables the reap (and with it the bound).
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
    /// just LIKE (and `IN`-lists as OR-of-terms). Correctness-safe under OR:
    /// `collect_text_match_tree` only routes a disjunction when every branch
    /// is completely covered by a text_match, and the original predicate is
    /// always preserved as the post-filter backstop. Targets the ~6× trace/
    /// span lookup gap vs the indexed PG path.
    /// Default ON; set false to revert to bloom/stats-only equality pruning.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_route_equality: bool,
    /// Startup backfill: build partition-mirrored indexes for live parquet
    /// files no successful manifest entry covers (pre-tantivy history, failed
    /// builds, files landed while the feature was off). Oldest-first, bounded
    /// concurrency; each covered file widens the windows where the prefilter
    /// can engage. Off by default — reads every uncovered file back from S3.
    #[serde(default)]
    pub timefusion_tantivy_backfill: bool,
    /// Concurrent index builds during backfill/reconcile/post-optimize
    /// reindex. 2 is safe alongside prod query load; the off-box repair CLI
    /// raises it (each 1 GB parquet takes ~2-3 min to index, so the first
    /// full reconcile is throughput-bound on this knob).
    #[serde(default = "d_tantivy_build_concurrency")]
    pub timefusion_tantivy_build_concurrency: usize,
    /// Backfill/reconcile skips parquet files larger than this (MB). 0 = no
    /// limit. Memory-tight runners (8 GB k8s nodes) OOM decoding+indexing
    /// 1 GB files; with a cap they repair everything else and log the skips.
    ///
    /// 4096 since 2026-08-17, from 512. The "~6 GB transient heap each" that
    /// justified 512 was the WHOLE-FILE path (`build_and_pack`, which takes a
    /// materialised `Vec<RecordBatch>`). Backfill and reconcile do not use it:
    /// they call `build_index_for_file` -> `build_parquet_and_pack`, which
    /// STREAMS, and whose peak is fixed by construction —
    /// `PARQUET_INDEX_BATCH_WINDOW` = 2 batches in flight, a
    /// `WRITER_HEAP_BYTES` = 64 MiB writer, and an `MmapDirectory` so the index
    /// is built on DISK. Nothing there scales with parquet size.
    ///
    /// The cap was therefore excluding files for a cost this path stopped
    /// paying, and it is why the reindex could not finish: prod 2026-08-17 held
    /// `tantivy_oversized_skipped` = 917 permanently, with
    /// `tantivy_uncovered_files` still RISING (2,077).
    ///
    /// Not removed outright, because one step does still track file size: the
    /// final `pack_dir` builds the tar.zst blob in memory, sized by the
    /// COMPRESSED INDEX rather than by the parquet. 4096 clears the largest
    /// files this codebase has recorded (~2.34 GB) with margin, while keeping a
    /// bound on that one term.
    #[serde(default = "d_tantivy_backfill_max_file_mb")]
    pub timefusion_tantivy_backfill_max_file_mb: u64,
    /// File-level scan pruning: when the prefilter engages, files whose
    /// covering index returned zero hits are excluded from the Delta scan
    /// entirely (needle queries read only the files that can match). Off
    /// switch for instant rollback to id-IN-list-only pruning.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_file_pruning: bool,
    /// Warm the local index cache with blobs whose data is at most this many
    /// days old, at startup (0 = off). Turns the cold-window download cliff
    /// into a background cost after restarts.
    #[serde(default)]
    pub timefusion_tantivy_prefetch_days: u32,
    /// Row-selection pushdown: when the prefilter engages, files whose index
    /// was built in parquet row order get a per-file ParquetAccessPlan so the
    /// reader decodes only matching row groups/rows. Off switch for instant
    /// rollback to id-IN-list-only filtering inside surviving files.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_row_selection: bool,
}

impl TantivyConfig {
    /// Tables to index: schemas with `tantivy.indexed: true` on any field.
    /// Computed once from the static registry on first access (the registry
    /// is compiled-in YAML, so there's nothing to invalidate).
    /// `BTreeSet` so `indexed_tables` is sorted by construction.
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
    /// Disk budget in bytes. Floored at 1 GB: a zero here would reap the cache
    /// to nothing every 10 minutes and turn every query into a re-download,
    /// which is a worse failure than the leak this bounds.
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
    /// TCP/TLS connection-establishment bound passed to the object_store S3
    /// client (humantime format, e.g. "15s"). `Option` so the derived
    /// `AwsConfig::default()` stays valid (an empty string wouldn't parse);
    /// see `connect_timeout`/`request_timeout` for the effective defaults.
    #[serde(default)]
    pub timefusion_s3_connect_timeout: Option<String>,
    /// Total per-request bound (humantime, e.g. "900s"). Must comfortably
    /// exceed the time to PUT one large multipart part to R2 under load —
    /// the 2026-06-24 compaction failed when 300s + concurrent big PUTs
    /// starved R2 connections. Tunable via TIMEFUSION_S3_REQUEST_TIMEOUT.
    #[serde(default)]
    pub timefusion_s3_request_timeout: Option<String>,
    /// Per-request bound for the COMMIT-LOG request class (`_delta_log/*.json`,
    /// `_last_checkpoint`, log LISTs) — humantime, default "30s". Split from
    /// `timefusion_s3_request_timeout` because the two classes have nothing in
    /// common: a data request is a multi-MB parquet part that legitimately
    /// takes minutes, while a log request is a few-KB control-plane operation
    /// that is sub-second when healthy. Sharing the 900s data bound is what let
    /// one hung R2 commit PUT hold a per-table commit lock for 14 minutes and
    /// stall every committer on that table (prod 2026-07-30 01:20–01:34).
    ///
    /// A timed-out commit PUT is SAFE to bound this tightly: the conditional
    /// (`If-None-Match: *`) PUT that delta-rs uses is NOT marked idempotent in
    /// object_store, so a timeout is never silently re-sent — it surfaces as an
    /// ordinary commit error and TimeFusion's existing landed-probe
    /// (`probe_commit_landed`) decides whether the commit landed. Tunable via
    /// TIMEFUSION_S3_LOG_REQUEST_TIMEOUT.
    #[serde(default)]
    pub timefusion_s3_log_request_timeout: Option<String>,
}

/// Warm-connection pool size per host, shared by both object-store client
/// construction paths. 128 gives headroom above the ~48-way query scan fanout
/// so concurrent GETs reuse sockets instead of re-doing TLS (the connection
/// starvation that failed the 2026-06-24 compaction under load).
pub(crate) const S3_POOL_MAX_IDLE_PER_HOST: usize = 128;

/// Effective timeout for a configured value, coercing a bare number (e.g.
/// `"150"`) to humantime seconds (`"150s"`). object_store's
/// `ClientConfigKey::{ConnectTimeout,Timeout}` parse strictly via humantime and
/// PANIC the process at boot on a unitless value — a prod
/// `TIMEFUSION_S3_CONNECT_TIMEOUT=150` crash-looped TF on 2026-06-24. Treat an
/// all-digit string as seconds; pass anything with a unit through untouched.
fn normalize_duration(configured: Option<&str>, default: &str) -> String {
    let s = configured.unwrap_or(default);
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) { format!("{s}s") } else { s.to_owned() }
}

impl AwsConfig {
    /// Effective connect timeout. R2 establishes healthy connections in <1s;
    /// a generous bound only matters when something is wrong, where it trades
    /// slower failure for surviving transient R2 connection refusals.
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
            // Bound connection establishment + total request time. Kept in sync
            // with create_object_store (which builds its own client) so both the
            // delta-rs register path and the direct AmazonS3Builder agree.
            ("connect_timeout", Some(self.connect_timeout())),
            ("timeout", Some(self.request_timeout())),
            // Keep TLS connections warm for the query read path: a recent-window
            // scan fans out ~target_partitions (48) concurrent GETs; the object_store
            // default idle cap can force TLS re-establishment mid-fanout and starve
            // R2. Matches create_object_store's direct AmazonS3Builder client.
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
    #[serde(default = "d_grpc_port")]
    pub grpc_port: u16,
    #[serde(default)]
    pub grpc_token: Option<String>,
}

impl CoreConfig {
    pub fn wal_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("wal")
    }
    pub fn cache_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("cache")
    }
    /// Own root for the local hot tier — never share a dir with a generic
    /// recursive deleter (ba8820e: WAL GC ate the quarantine dir).
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
    /// uncompressed Arrow IPC file on local disk and serve recent-window reads
    /// from it via zero-copy mmap (third plan leg, between MemBuffer and Delta).
    /// This is the tier's main switch — hours of window; **0 turns demotion
    /// off** (GC still sweeps, so the directory can't strand bytes). Past this
    /// age a demoted file is unlinked and its window falls back to Delta.
    #[serde(default = "d_hot_tier_enabled")]
    pub timefusion_hot_tier_enabled: bool,
    /// Hard cap on the tier's directory; over it, GC unlinks oldest-first. The
    /// tier shares the WAL/data volume, which has twice been eaten by an
    /// unbounded consumer (273GB of orphaned spill dirs, 238GB of tantivy
    /// cache whose size knob was dead code), so this one is a real dial.
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
    /// EXPERIMENTAL (default OFF), parity plan Defect 1: when set, `insert()`
    /// admits over the memory hard limit instead of *rejecting* a write whose
    /// backpressure budget is exhausted — the WAL append is the durability
    /// boundary, so a slow/over-budget write is preferable to a dropped one.
    /// Closes the drop-before-durability loss seam. Requires a soak (watch RSS /
    /// flush throughput) before prod enable — over-budget admission trades a
    /// reject for unbounded growth if Delta flush can't keep up.
    #[serde(default)]
    pub timefusion_wal_admit_decouple: bool,
    #[serde(default = "d_wal_fsync_ms")]
    pub timefusion_wal_fsync_ms: u64,
    #[serde(default = "d_wal_fsync_mode")]
    pub timefusion_wal_fsync_mode: String,
    /// Fsync the WAL shard before acking DML appends (machine-crash
    /// durability). Batched INSERT appends are always flushed before ack by
    /// walrus's `batch_write`; only single-entry DML appends defer to the
    /// background fsync thread — this closes that window. Default on: a torn
    /// mmap tail after OOM/SIGKILL can quarantine acked-but-unsynced entries
    /// (the "only power loss" reasoning was wrong — see WAL quarantine
    /// incidents, 2026-07).
    #[serde(default = "d_wal_ack_fsync")]
    pub timefusion_wal_ack_fsync: bool,
    #[serde(default = "d_wal_max_files")]
    pub timefusion_wal_max_file_count: usize,
    /// Force-flush backstop on total on-disk (unflushed) WAL bytes. Guards the
    /// cursor-lag case the memory-pressure valve misses: a stuck /
    /// persistently-retrying commit pins the WAL GC floor while buffer memory
    /// frees post-commit, so the WAL bloats without tripping memory pressure —
    /// inflating restart replay (issue #83). 0 = derive from the buffer budget.
    #[serde(default)]
    pub timefusion_wal_max_unflushed_mb: usize,
    /// Disk-runaway breaker: HARD cap on total on-disk WAL bytes, past which
    /// INSERTs are rejected (the upstream DLQ absorbs and replays them)
    /// instead of acking writes into unbounded disk growth — during the
    /// 2026-07-26 merge storm the WAL grew to 121GB while the soft thresholds
    /// only WARNed. Total on-disk includes flushed segments the age-gated GC
    /// holds for ~90min and all active per-shard files, so keep this far
    /// above the busy-hour residue (a 2026-07-26 catch-up burst wrote
    /// 4.6GB/min and reached ~99GB of young+flushed segments while flush was
    /// fully healthy — the breaker must not trip on that).
    /// Checked every ~15s by a dedicated WAL-gate task (`run_wal_gate_task` —
    /// deliberately NOT the flush loop, which stalls in exactly the overload
    /// this guards against). DML mem legs are exempt: failing an UPDATE
    /// mid-statement would desync mem vs Delta. 0 disables (see `d_wal_hard_limit_gb`).
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
    /// partition filter). Eliminates the per-project metadata-scan + OCC-commit
    /// multiplication that starved flush during the 2026-07-26 merge storm.
    /// Kill switch: `TIMEFUSION_DML_COALESCE_FOLD=false` restores per-project
    /// merges.
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
    /// demotion, no third scan leg, no disk use. There is no time retention —
    /// the tier holds whatever `hot_tier_limits().max_disk_bytes` affords.
    pub fn hot_tier_enabled(&self) -> bool {
        self.timefusion_hot_tier_enabled
    }
    /// The tier's only ceiling. Per-scan heap needs no knob: `HotTier::scan`
    /// streams its files inside the query's own memory pool.
    pub fn hot_tier_limits(&self) -> crate::hot_tier::HotTierLimits {
        crate::hot_tier::HotTierLimits { max_disk_bytes: self.timefusion_hot_tier_max_disk_gb.saturating_mul(GIB as u64) }
    }

    /// mtime age past which a WAL file is PRESUMED dead weight. This is a
    /// heuristic, not a soundness bound: replay is cursor-bounded (no age
    /// cutoff — see `recover_from_wal`), so GC soundness comes from the
    /// un-flushed floor (`gc_wal_files`'s `unflushed_floor_micros`) and the
    /// drained-gated boot sweep, NEVER from age alone. Do not tighten or
    /// bypass the floor on the strength of this age.
    pub fn wal_gc_max_age(&self) -> Duration {
        // Fixed 30min, decoupled from buffer retention: GC soundness comes
        // from the un-flushed floor (see the doc above), never from age — age
        // only delays reclaiming FLUSHED segments. The old retention+20min
        // (~90min) held ~3x more dead weight on disk, which under catch-up
        // bursts (2026-07-26: 4.6GB/min) pushed total-on-disk into the
        // disk-runaway breaker and flapped ingest for no durability benefit.
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
    /// value overrides the default quarter-buffer ceiling, which bounds the
    /// active bucket's restart replay even when memory pressure has not fired.
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
    /// Per-bucket Delta-commit watchdog inside `flush_bucket`. 0 disables it
    /// (unbounded wait — the pre-2026-07-01 behavior).
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
    /// minimal eviction unit AND its size caps the largest entry that can land
    /// on disk — so it must be >= the largest file we want cached locally. This
    /// acts as a *floor*: `from_app_config` automatically raises the effective
    /// block size to 2x the compaction target size, so the two can't drift out
    /// of sync if an operator bumps the target. Default 256MB.
    ///
    /// Memory note: this also bounds the transient buffer each multipart-write
    /// warm holds in heap (see `timefusion_warm_inline_max_mb`). Up to
    /// `timefusion_warm_concurrency` compactions can run at once, so the worst
    /// case is `block_size_mb * warm_concurrency` of transient heap during a
    /// busy maintenance window (e.g. 256MB x 4 = 1GB). On smaller-memory
    /// instances set `timefusion_warm_inline_max_mb` to cap this independently
    /// of the on-disk block size.
    #[serde(default = "d_foyer_block_size_mb")]
    pub timefusion_foyer_block_size_mb: usize,
    /// Entries larger than this (MB) are inserted disk-only (foyer
    /// `Location::OnDisk`) so warming a big compaction output doesn't evict the
    /// hot small-entry working set from L1 memory. Small entries keep the
    /// default L1+disk placement for fastest repeat reads. 0 = always use L1.
    #[serde(default = "d_l1_max_entry_mb")]
    pub timefusion_foyer_l1_max_entry_mb: usize,
    /// Don't admit writes whose `date=` partition is older than this many days
    /// (e.g. cold-tier recompress rewrites) — recent data stays local, old data
    /// is served from S3. 0 = no age limit. Pairs with the cache TTL, which
    /// governs how long an admitted entry survives before falling back to S3.
    #[serde(default = "d_cache_recent_days")]
    pub timefusion_cache_recent_days: usize,
    /// Optional extra cap (MB) on the in-flight buffer used to warm the cache
    /// directly from a multipart write (so we don't re-download a file we just
    /// streamed to S3). Always bounded by the disk block size; 0 = bound only
    /// by the block size.
    #[serde(default = "d_warm_inline_max_mb")]
    pub timefusion_warm_inline_max_mb: usize,
    /// Per-upload cap (MB) on the heap buffer a multipart write tees into to
    /// warm the cache (see `CachingMultipartUpload`). Uploads that grow past
    /// this abandon capture and stream through untouched — never blocked,
    /// never failed.
    ///
    /// Sized for *flush* outputs, which are the writes that actually benefit:
    /// a flush bucket's parquet is single-digit-to-low-tens of MB and is read
    /// back within seconds by dashboards, so teeing it saves a real S3 GET.
    /// Compaction/optimize outputs are written at
    /// `timefusion_optimize_target_size` (256MB default) and are NOT worth
    /// teeing: they're already warmed post-commit through the read path by
    /// `timefusion_warm_after_compaction` (`warm_cache_for_uris`). 32MB keeps
    /// flush capture intact while letting big compaction outputs skip the RAM
    /// tee. Before this cap, capture was bounded only by the 256MB block size,
    /// which prod heap profiles attributed ~42% of an 85GB heap to. 0 =
    /// bounded only by the block size (pre-cap behaviour), further clamped to
    /// the process-wide budget below so a cap larger than the budget cannot
    /// deny every reservation and disable capture outright.
    #[serde(default = "d_write_capture_max_mb")]
    pub timefusion_write_capture_max_mb: usize,
    /// Process-wide budget (MB) for in-flight write-capture buffers. Each
    /// capturing upload reserves its full per-upload cap up front, so this
    /// hard-bounds total capture heap regardless of how many uploads run
    /// concurrently (flush_parallelism x delta-rs part concurrency, plus
    /// optimize/dedup writes). Over budget = capture skipped for that upload
    /// (best-effort, upload unaffected). It also CLAMPS the per-upload cap, so
    /// a cap above the budget degrades capture rather than disabling it.
    /// Default 8x the per-upload cap — one full reservation per flush writer
    /// (`flush_parallelism`), so a flush wave never starves itself.
    /// 0 = unbudgeted.
    #[serde(default = "d_write_capture_budget_mb")]
    pub timefusion_write_capture_budget_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
    /// Scan-resistant admission: a scan reaching further back than this many
    /// hours runs with cache population BYPASSED, so a 7d/14d sweep can't
    /// flush the hot tail out of L1/disk (ClickHouse big-scan bypass). Reads
    /// still HIT what is already cached. 0 disables the bypass.
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
    /// dedup) whose output is rewritten again the same night by consolidate /
    /// recompress. Default 1: trades ~10-15% transient hot-day file size for
    /// ~2-3x less compress CPU and faster decompress on recent-data queries.
    /// Steady-state on-disk size is unchanged because nightly consolidate
    /// rewrites at the warm tier — a tier-1 footer (`timefusion.compression_tier`)
    /// is below both warm (9) and cold (19), so those partitions stay eligible
    /// for re-tiering by the recompress probe.
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
    /// Use Z-order clustering for the periodic full OPTIMIZE (the 30-min,
    /// window-wide job). Default OFF: Z-order runs a memory-heavy global sort
    /// that exhausts the pool on large windows (prod 2026-07-12), and its
    /// space-filling curve actually loosens timestamp locality. Plain Compact
    /// bin-packs the flush's already-timestamp-sorted, time-bucketed files,
    /// preserving tight per-row-group timestamp ranges — the dominant query
    /// predicate — with no global sort. (The cold-tier recompress path keeps
    /// Z-order independently: it relies on the full-file rewrite to recompress.)
    /// Re-enable only once Z-order's memory footprint is bounded.
    #[serde(default)]
    pub timefusion_optimize_use_zorder: bool,
    /// Rewrite optimize/compact/recompress output sorted by the schema's
    /// `sorting_columns` (delta-rs `OptimizeType::SortBy`) with an honest DESC
    /// footer, so the timestamp-ordering/LIMIT pushdown keeps firing on
    /// optimized/compacted partitions (not just fresh flush files). Default ON:
    /// without it every rewrite path concatenates (`declare_sorted=false`),
    /// strips the footer, and the reader's all-or-nothing ordering rule disables
    /// the top-N pushdown for the whole partition within one compaction cycle.
    ///
    /// Memory: `SortBy` reads each partition through the ordering-advertising
    /// `DeltaScanNext` and runs `df.sort()`. Once every file in the partition
    /// carries an honest sorted footer (guaranteed by the flush path's
    /// `sort_batches_by_schema` + prior SortBy rewrites), DataFusion elides the
    /// blocking sort into a streaming `SortPreservingMergeExec` — a k-way merge
    /// holding ~one batch per file, bounded regardless of partition size. The
    /// 2026-07-14 OOM ("Not enough memory to continue external sort") was
    /// `df.sort()` over *unsorted* inputs (the heterogeneous-flush-file bug,
    /// since fixed) forcing a full-partition blocking sort. TRANSITION CAVEAT:
    /// the first compaction of a partition still holding legacy unsorted files
    /// is a one-time blocking sort (bounded by the maintenance FairSpillPool +
    /// 64 MB reservation + disk spill, partitions capped at 4); after it the
    /// partition is sorted and every later compaction is the streaming merge.
    /// Set to `false` (plain Compact) only if a deployment can't afford even
    /// that one-time transition sort.
    #[serde(default = "d_true")]
    pub timefusion_optimize_sort_by: bool,
    /// Budget for an IN-PROCESS Arrow sort on the flush path, in in-memory
    /// bytes. Past it `sort_batches_by_schema` writes unsorted (correctness-safe
    /// — the footer just doesn't advertise an order) rather than materialising a
    /// giant coalesced backfill 2-3x with no pool and no spill.
    ///
    /// It is in IN-MEMORY bytes, which is NOT what a compaction target is
    /// measured in: prod's zstd ratio on otel data is ~17x, so a 256 MB
    /// FILE-byte hot-tail bin is ~4.3 GB here. Paths that rewrite whole bins
    /// sort inside a DataFusion plan (pooled, spillable, streaming) instead of
    /// consulting this at all — see `stage_hot_bin`.
    #[serde(default = "d_sort_skip_bytes")]
    pub timefusion_sort_skip_bytes: usize,
    /// Pool for the flush-path escalation sort, in MB. Its own slice so an
    /// ingest-path sort never queues behind a Z-order holding the maintenance
    /// pool. Deliberately smaller than the escalation threshold: exceeding it
    /// spills to disk, which is the intended degradation.
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
    /// `RecordBatchWriter`. That writer has no target-size support of its own —
    /// `flush()` emits one file per partition no matter how much was buffered —
    /// so the rewrite paths cut the file themselves once the buffer passes this.
    ///
    /// Unbounded outputs are how prod grew active files of 712 MB (median
    /// unsorted) up to 2.34 GB. Beyond the wasted read granularity, an oversized
    /// file is effectively unrepairable: re-sorting it needs its rows in memory,
    /// which at prod's ~17x zstd ratio is far past any sort budget, so once one
    /// lands without a `sorting_columns` footer it stays that way.
    ///
    /// Cutting is free for correctness here: each cut lands on a contiguous
    /// slice of an already-sorted stream, so every piece keeps an honest sorted
    /// footer AND the pieces are event-time disjoint (better file pruning).
    #[serde(default = "d_writer_max_file_bytes")]
    pub timefusion_writer_max_file_bytes: usize,
    /// Largest file a 5-minute hot tick will rewrite purely to repair its
    /// missing `sorting_columns` footer.
    ///
    /// Deliberately SEPARATE from `timefusion_writer_max_file_bytes` even though
    /// they default the same: that one caps what we WRITE, this caps what we
    /// pull into a tick to REPAIR. Raising the repair reach to cover legacy
    /// files should not also start emitting bigger files. Anything above this
    /// is left to `timefusion optimize --recompress`, which force-rewrites a
    /// whole date and is the only thing that can touch a single-file partition.
    #[serde(default = "d_repair_max_file_bytes")]
    pub timefusion_repair_max_file_bytes: usize,
    /// Sealed dates (yesterday backwards) the hot tail also scans for FOOTER
    /// REPAIR — rewriting files that carry no `sorting_columns` so the reader's
    /// all-or-nothing ordering claim survives. Repair only: sorted files on
    /// those dates are never re-binned, so settled history isn't rewritten.
    ///
    /// Default 31 — one file with no `sorting_columns` footer voids the ordering
    /// claim for EVERY query whose window touches its date, so the lookback has
    /// to cover the windows users actually ask for. Users query 30 days; a
    /// 2-day default (the old value, sized for the yesterday-crossing 24h
    /// dashboard) leaves a 28-day hole in which one poisoned file pins every
    /// wide query at `full-set` dedup forever. Prod 2026-08-08: exactly that,
    /// on 2026-07-30 for two tenants.
    ///
    /// Do NOT set this far past the query window "to be safe". Admission offers
    /// every un-verified sealed file, so the lookback IS the suspect-set size:
    /// prod ran 75 and the pass spent itself clearing correctly-sorted files at
    /// ~1.24/min without ever reaching a rewrite. 0 restores the old today-only
    /// behaviour, under which an unsorted file that survived midnight was never
    /// repaired at all (prod 2026-08-06: 130s vs 0.81s).
    #[serde(default = "d_repair_lookback_days")]
    pub timefusion_light_optimize_repair_days: u64,
    // Concurrent merge tasks per optimize run — formerly
    // `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`. Now `derived.optimize_merge_tasks()`.
    #[serde(default = "d_light_schedule")]
    pub timefusion_light_optimize_schedule: String,
    /// Sealed-date FOOTER REPAIR, on its own cron. Split out of the hot-tail
    /// tick 2026-08-08: a repair unit is one 700 MB - 1 GiB whole-file rewrite,
    /// which does not fit the 240s budget a 5-min schedule derives, and an
    /// over-budget bin is discarded and re-selected identically — so repair
    /// could never complete while burning every third packing tick.
    ///
    /// EVERY 3 HOURS, i.e. a 144-minute budget — sized from a measurement, not
    /// a guess. On prod 2026-08-08 a contention-free rewrite of the partition
    /// that walls both named tenants (`date=2026-07-30`, 969 MB, 19.7M rows,
    /// 96 columns) took **43 minutes** of streaming output; the same work under
    /// five concurrent repair bins was ~13x slower and produced one part in 48
    /// minutes. At `concurrency = k/2` expect roughly 2-3x the solo time, so
    /// ~90-130 minutes, and the budget needs margin over that.
    ///
    /// A 20-minute schedule (16-minute budget) and then an hourly one (48) were
    /// both tried on prod the same morning: each committed real work (18 and 46
    /// repair bins respectively) and each still discarded the big file at the
    /// deadline. An over-budget bin is DISCARDED and re-selected, so a unit that
    /// does not fit never completes AT ANY CADENCE.
    ///
    /// Longer ticks cost nothing in throughput: a wave serves every pending
    /// project (concurrency bounds parallelism, not the count), so bins per tick
    /// are bounded by wall clock, and slot-minutes per day are identical at any
    /// period. Fewer, longer ticks therefore strictly dominate — same drain
    /// rate, plus the large units can actually finish.
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
    /// Dirty-bin dedup of sealed (< today) partitions. Runs on its OWN cron,
    /// decoupled from hot-tail compaction: dedup churning an old-date backlog
    /// must not starve today's compaction (they touch disjoint partitions —
    /// dedup skips today, compaction only touches today). Default 5 min.
    #[serde(default = "d_light_schedule")]
    pub timefusion_dedup_schedule: String,
    /// Incident kill switch for physical dirty-bin dedup (2026-08-03 row-loss);
    /// read-side dedup remains the correctness path. Re-enabled by default
    /// 2026-08-05 after prod-shaped validation: canaried on live traffic, then
    /// physically audited committed bins against the pre-rewrite files —
    /// distinct dedup keys intact in-bin AND across the whole partition
    /// (adjacent-bin carry-through), only duplicate versions removed.
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
    /// (full-file) cache. OFF by default. Tried ON (2026-07-19) to keep the
    /// recent-window hot tail warm, but on the memory-tight prod box the
    /// continuous full-body warms (every flush + an 8801-file boot burst on the
    /// uncompacted busy table) drove RSS up ~2GB/min toward the OOM ceiling with
    /// no query load — same "good in theory, OOM on this box" failure mode as the
    /// reverted compaction. Footers carry most of the planning-latency win at a
    /// fraction of the bytes; only enable full-file warming where Foyer + memory
    /// have real headroom.
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
    /// instead of deleting it and re-doing the 40+ minute rewrite. Setting it
    /// false reverts to plain reconcile-and-delete. Only data-preserving
    /// (compaction/repair) bins are eligible — dedup bins drop rows and stay
    /// cleanup-only.
    ///
    /// Shipped OFF for one deploy so the widened manifest was written and
    /// exercised before anything committed from it; 7f572e7 and 5ec0097 both ran
    /// in prod, so that condition is met and the default is now ON. It has to
    /// be: a repair bin is a single 40+ minute whole-file rewrite, and prod was
    /// replacing the task every 15-28 minutes (deploys plus healthcheck
    /// replacements), so every pass discarded a complete staged output and the
    /// same file stayed poisoned for days. Resume is what makes the rewrite
    /// survive a restart at all.
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
    /// Without a backfill the ONLY covered dates are today and the dedup
    /// lookback, and routing needs a contiguous certified prefix from the start
    /// of the window — so a 7d or 30d query, the only kind expensive enough to
    /// be worth accelerating, can never route no matter how long the process
    /// runs.
    ///
    /// That paragraph was written as a warning and then left true: this was
    /// `#[serde(default)]` — 0, disabled — while the only implementation
    /// (`rollup_backfill_tick`) had been orphaned by the coordinator redesign
    /// and was reachable solely from tests. Prod 2026-08-17 confirmed it exactly:
    /// `otel_logs_and_spans_rollup_dashboard_1m_v3` held rows for two dates
    /// against 30+ days of source data, and every 7d/14d/30d query was refused
    /// with `not_built`.
    ///
    /// 35 matches the dedup certification window so the two horizons cannot
    /// drift apart. `plan_rollup_backfill` consumes this and is bounded per
    /// pass, so a wide horizon converges over hours instead of burying the
    /// journal in one go. Set 0 to disable.
    #[serde(default = "d_rollup_backfill_days")]
    pub timefusion_rollup_backfill_days: u16,
    #[serde(default = "d_rollup_backfill_schedule")]
    pub timefusion_rollup_backfill_schedule: String,
    /// Backfill units in flight at once.
    ///
    /// Serial units were the binding constraint on convergence: prod 2026-08-15
    /// logged `attempted=2 built=1`, `attempted=3 built=2` and `attempted=1
    /// built=0` against `queued=355`, i.e. ~3 partitions an hour — five days to
    /// cover a pool that is invalidated faster than that. One unit that runs to
    /// its 24-minute deadline and commits nothing consumed the entire tick.
    ///
    /// The heavy work stays bounded by `maintenance_rewrite_sem` regardless, so
    /// this widens how many units can be certifying or waiting, not how much
    /// Arrow is resident at once. Above the rewrite pool it buys progressively
    /// less; 3 against the default pool of 2 keeps one unit certifying while two
    /// rewrite.
    #[serde(default = "d_rollup_backfill_concurrency")]
    pub timefusion_rollup_backfill_concurrency: usize,
    /// Skip the read-side DedupExec (and, since 4ecca05, its key projection) for
    /// Delta-only queries whose every in-window (project, date) partition was
    /// verified duplicate-free by a sweep pass AND whose file set is unchanged
    /// since (fingerprint match). Also restores per-scan LIMIT pushdown.
    ///
    /// This shipped off "until COUNT parity is validated on prod-shaped data".
    /// That gate is now met by
    /// `dedup_compaction_test::count_is_identical_with_and_without_the_dedup_skip`,
    /// which builds the shape that actually produces duplicates here — 250 keys
    /// written twice across separate flushes so the copies land in different
    /// Delta files, plus 120 written once — sweeps, and demands `count(*)` be
    /// identical with the skip on and off. Over-counting is the failure mode
    /// that matters (it would inflate every dashboard) and that test fails on it.
    ///
    /// What is validated: the MECHANISM, at 370 keys. What is not: prod scale or
    /// every column shape. The runtime guard is what bounds the rest — the skip
    /// cannot fire on a partition the sweep has not certified with a matching
    /// file fingerprint, so an unswept or newly-written partition keeps full
    /// dedup. Turn it off here if a count is ever doubted.
    #[serde(default = "d_true")]
    pub timefusion_read_dedup_skip_swept: bool,
    /// Persist sweep certifications to the data dir and reload them at boot, so
    /// the read-side dedup skip does not restart from cold on every deploy.
    ///
    /// `dedup_clean_fp` is process-local and TF deploys several times a day,
    /// which is the leading suspect for the skip firing on only 0.2–0.5% of
    /// Delta-reading scans. Persistence is the lever for that.
    ///
    /// It cannot widen certification. A reloaded entry passes through the same
    /// fingerprint-equality check against the live file list as an in-memory one,
    /// so a stale, truncated or corrupted store costs a skip rather than granting
    /// a wrong one. What it DOES do is let the skip actually fire, where before it
    /// almost never got the chance — so this raises exposure to
    /// `timefusion_read_dedup_skip_swept`'s certification rule rather than adding
    /// a new mechanism of its own.
    ///
    /// Kill switches, in order of bluntness: set this false to go back to a cold
    /// cache per process; set `timefusion_read_dedup_skip_swept` false to remove
    /// the skip entirely. Doubt a `count(*)` and reach for the second one.
    ///
    /// Enabling this also makes Phase 0
    /// (`docs/plans/2026-08-11-certification-survival.md`) measure the end state
    /// directly — `dedup_denied_never_certified` now reflects what a WARM cache
    /// leaves on the table, instead of predicting it from a cold one.
    #[serde(default = "d_true")]
    pub timefusion_dedup_certification_persist: bool,
    /// Allow `DedupExec` to run in streaming `bounded[timestamp]` mode, which
    /// trusts the scan's DECLARED `output_ordering` — i.e. the parquet footer's
    /// `sorting_columns`. A footer that lies makes one "run" span many
    /// timestamps; prod 2026-08-07 read 132 rows where 1620 existed, surfacing
    /// as multi-minute holes in customer dashboards.
    ///
    /// Defaults ON: the row loss came from bounded mode ALSO dropping the bound
    /// column from the dedup key, which `dedup_key_idxs` no longer does. This is
    /// only an emergency kill switch, and not a cheap one — bounded mode carries
    /// LIMIT early termination, so turning it off makes "top N" queries scan the
    /// whole window.
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
    /// Per-shard target on the ESTIMATED DECODED (in-memory Arrow) footprint of a
    /// dedup chunk rewrite. Compressed bytes under-count by 5-20× for wide
    /// Variant/JSON columns, and the `SELECT * … collect()` Arrow buffers are NOT
    /// accounted by DataFusion's memory pool — so a compressed-under-budget chunk
    /// once decoded to tens of GB and OOM-killed the process (prod 2026-07-04, 89GB
    /// cgroup kill). The rewrite now SHARDS by a hash of the dedup keys into
    /// `ceil(est_decoded / this)` passes so each pass materializes ~this much; a
    /// single key group that alone exceeds this is unshardable and skipped (read-side
    /// dedup keeps queries correct). 0 → one shard for this ceiling. See
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
    /// Each merge scans the time-windowed target partition to locate join-key
    /// matches — heavy on a CPU-throttled box. Ungated, bursts of per-project
    /// drains stampede all cores and starve read queries (prod 2026-07-19). This
    /// caps that so reads keep CPU; drains queue behind it.
    #[serde(default = "d_dml_merge_concurrency")]
    pub timefusion_dml_merge_concurrency: usize,
    /// Perform UPDATE/DELETE as merge-on-read deletion-vector operations instead
    /// of copy-on-write full-file rewrites. A DV UPDATE appends only the rewritten
    /// matched rows and masks the originals with a roaring-bitmap deletion vector;
    /// a DV DELETE just writes the mask. This avoids rewriting whole partitions for
    /// a small predicate (the hash-enrichment UPDATE / retention DELETE hotspots).
    ///
    /// Requires the `deletionVectors` writer feature on the table — enabled lazily
    /// on first DV write (a one-time protocol upgrade to reader/writer v3/v7). On
    /// by default. NOTE: the upgrade is irreversible and every reader of these Delta
    /// tables must understand DVs (TF's own scan does; external log readers may not) —
    /// set `TIMEFUSION_USE_DELETION_VECTORS=false` to keep copy-on-write rewrites.
    #[serde(default = "d_true")]
    pub timefusion_use_deletion_vectors: bool,
    /// Commit DV merges append-tolerantly: a concurrent flush commit (AddFile
    /// only) no longer aborts the merge with ConcurrentAppend — the commit
    /// rebases instead of re-running the whole scan+join (the 2026-07-26 OCC
    /// retry storm). Sound because the mem leg runs before every Delta leg, so
    /// rows flushed after the merge's snapshot already carry post-DML values;
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
///   whose batch exceeded its slot — bit us in prod on 2026-05-28 when
///   ~30 concurrent INSERTs each got a ~76 MB slot and every 700-row
///   batch hit `Memory limit exceeded`.
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
    /// Admission guard for wide-window read scans. A query reaching further back
    /// than `timefusion_wide_scan_lookback_hours` (or with no lower time bound)
    /// opens hundreds of Parquet files whose row groups aren't page-pruned (a
    /// one-sided `timestamp >= cutoff` can't prune files past the date cut), so
    /// its per-file decode buffers are large and unbounded by the DataFusion
    /// memory pool (Parquet decode is untracked). At `target_partitions` (~48)
    /// concurrent decoders this OOM-restarted prod from a single 7-day dashboard
    /// (2026-07-20). Wide scans are gated to `timefusion_max_concurrent_scan_readers`
    /// concurrent batch-decodes across ALL queries so they degrade to slower
    /// rather than take the process down; narrow recent-window scans (the hot
    /// 1h dashboard path) are never gated and keep full parallelism.
    #[serde(default = "d_max_concurrent_scan_readers")]
    pub timefusion_max_concurrent_scan_readers: usize,
    #[serde(default = "d_wide_scan_lookback_hours")]
    pub timefusion_wide_scan_lookback_hours: u64,
    /// Depth alone badly over-fires the gate above. Lookback is only a PROXY for
    /// decode heap, and once file pruning works the proxy breaks: prod
    /// 2026-08-01, the same `ORDER BY timestamp DESC LIMIT 50` took 255ms at a
    /// 115-minute lookback and 40-57s at 125 minutes, and `EXPLAIN ANALYZE`
    /// showed the slow plan reading ONE file and 8.24 KB with every other metric
    /// in microseconds — it was simply queued behind a saturated gate
    /// (`permits=0`). So a scan is gated only when it is deep AND actually
    /// selected real work: more than `..._max_files` files or `..._max_mb` of
    /// them, counted from the plan's file groups AFTER pruning. The 7-day scan
    /// that caused the original OOM selects hundreds of files and is still
    /// gated; the deep-but-pruned dashboard query no longer waits behind it.
    #[serde(default = "d_wide_scan_max_files")]
    pub timefusion_wide_scan_max_files: usize,
    #[serde(default = "d_wide_scan_max_mb")]
    pub timefusion_wide_scan_max_mb: u64,
    /// Cross-connection plan-cache capacity (unique canonical/shape templates).
    /// 256 thrashed in prod (evicting ~half every ~60s); 1024 holds the working
    /// set with room to spare. Each entry is one LogicalPlan (~KBs).
    #[serde(default = "d_plan_cache_capacity")]
    pub timefusion_plan_cache_capacity: usize,
    /// Route `now()`/`current_timestamp` SELECTs through the shape cache (time fn
    /// parameterized to a fresh per-query instant) instead of bypassing it.
    /// On by default: 2026-07-19 prod CPU flamegraphs showed ~25% of CPU in
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
    /// dedicated box, and the reason prod OOM-looped on 2026-07-31: a 120 GiB TF
    /// container on a 188 GB host shared with ~62 GB of other services means TF's
    /// entitlement alone oversubscribes the HOST. Pinning the arithmetic so the
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
        // The old total==limit invariant was the 70-kills-in-3-days bug: every
        // subsystem at its cap was "legal" and the sum was the OOM.
        assert_eq!(total, 120 * GIB - (120.0 * GIB as f64 * 0.15) as usize, "tracked consumers + 15% untracked slack == the limit");
        assert_eq!(b.query_pool_bytes, 24 * GIB);
        assert_eq!(b.ingest_buffer_bytes, 24 * GIB);
        assert_eq!(b.foyer_memory_bytes, 12 * GIB);
        // 120 - (24 query + 24 buffer + 12 foyer + 12 writer reserve + 18 slack).
        // Was 24 GiB at query=30; the 2026-08-05 brake-headroom cut hands the
        // freed 6 GiB to the maintenance remainder.
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

    // Regression for the 2026-06-24 crash loop: TIMEFUSION_S3_CONNECT_TIMEOUT=150
    // (unitless) panicked object_store's Duration parse at boot. Bare numbers must
    // coerce to seconds; values with a unit pass through untouched.
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
    /// MAGNITUDE under the data bound — that gap is the whole fix for the
    /// 2026-07-30 commit-lock stall, and a default that drifted up to match
    /// `request_timeout` would silently restore the incident.
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
        // Durable-by-default WAL (2026-07 quarantine incidents): an async
        // fsync default let OOM-kills tear the mmap tail and silently
        // quarantine acked rows. Pin the durable defaults.
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
    // 8..=11 range (4 GiB per-sort spill threshold, 2026-07-30 — was 4..=6 at
    // 8 GiB), heavy keeps >= 1/4. The pool sum is pinned by
    // `budget_tree_allocates_the_entire_limit` on the same box.
    #[test]
    fn derived_budget_prod_box_120gib_48cores() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        assert!(b.heavy_share_bytes() as f64 >= b.maintenance_pool_bytes() as f64 * 0.25 - 1.0);
        let k = b.light_optimize_k(11);
        // 5 with the 30G post-slack pool (was 8..=11 at 48G): fewer concurrent
        // per-project sorts is the intended trade for not OOMing the box.
        assert!((4..=11).contains(&k), "K={k} outside the expected 4..=11 range");
        // The ENVELOPE is the invariant, not the permit count: permits x
        // per-sort budget must stay put, because that product is what the
        // 2026-07-04 fan-in OOM was about. 4 x 4 GiB became 8 x 2 GiB on
        // 2026-08-17 — same 16 GiB, twice the concurrency, each sort spilling
        // sooner. Asserting the count alone would have to be edited every time
        // the pairing moves, and would not catch the case that actually hurts:
        // permits raised WITHOUT paying for them.
        // permits x per-sort budget is the fan-in envelope (2026-07-04 OOM).
        // Briefly 40 GiB on 2026-08-17 and it OOM-killed prod at anon-rss
        // 124.9 GB; now 20 GiB (10 x 2), near the 16 GiB that ran clean for 12
        // hours. Pinned so a change is always a conscious edit with a memory
        // argument behind it, never a drift.
        assert_eq!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES,
            20 * GIB,
            "changing the fan-in envelope must be deliberate: state the memory headroom that pays for it"
        );
        assert!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES < b.memory_limit_bytes() / 2,
            "the fan-in envelope must stay well under the cgroup, whatever the permit count"
        );
        // The cgroup is the wrong denominator for the assertion above, and it is
        // why the envelope could go to 20 GiB unnoticed: heavy sorts do not run
        // in the cgroup, they run in `heavy_share_bytes()`, which on prod is
        // ~4.98 GiB (16.6 GiB pool, less a 4.15 GiB coordinator quarter, x 0.40).
        // 10 x 2 GiB against 4.98 GiB passes the cgroup test by 2x while
        // over-committing the real pool by 4x.
        //
        // What actually has to hold is that a FairSpillPool slice still clears
        // the allocations a sort cannot avoid: one indivisible `batch_size`
        // batch (2048 wide otel rows reach ~150 MB) plus `ExternalSorterMerge`'s
        // 32 MB floor, which cannot spill. Below that a unit FAILS instead of
        // spilling — prod 2026-08-17 logged exactly that at a 512 MB
        // coordinator pool, and again at 3.4 GiB heavy.
        //
        // Stated as a floor per concurrent sort rather than as an envelope, so
        // raising permits is only safe if the share grows with them.
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
    /// `dedup_sweep` is the only caller of `record_certification`, and its scope
    /// is `today - d_dedup_lookback_days ..= today`. Rollup routing needs a
    /// contiguous certified prefix across the whole query window, so this value
    /// is a hard ceiling on the longest window that can ever route. At the
    /// former default of 1 nothing past yesterday could be certified, which is
    /// why prod measured `never_certified_pct = 100.0` and `rollup_hits = 0`
    /// while 30d queries timed out. Dropping it back below 30 silently
    /// reintroduces that, with no failing behavior anywhere nearer the change.
    #[test]
    fn certification_window_covers_a_thirty_day_query() {
        assert!(super::d_dedup_lookback_days() >= 30, "the dedup sweep is what certifies partitions; below 30d no 30d query can ever route to a rollup");
    }

    /// Maintenance must not be serialized on a box with room to spare.
    ///
    /// Through 2026-08-16 this was a hard-coded 1, and one 512 MiB unit at a
    /// time against a ~64 GiB decode budget is what let the queue reach 20.3 TB
    /// and 18,397 tasks with zero dedup commits — and therefore zero
    /// certifications, `DedupExec` in every plan, and zero rollup hits. A
    /// regression here is not a throughput nit; it silently turns 30d queries
    /// back into timeouts, so assert the prod shape explicitly.
    #[test]
    fn coordinator_jobs_scale_with_the_box() {
        // Only meaningful when the operator has not pinned the override.
        if std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").is_ok() {
            return;
        }
        // Prod: 80 GiB / 48 cores.
        let prod = DerivedBudget::from_limits(80 * GIB, 48);
        assert!(prod.coordinator_jobs() > 1, "prod-shaped box must run maintenance in parallel, got {}", prod.coordinator_jobs());
        // Every admitted unit reserves at most MAX_DECODED_BYTES, so the
        // concurrent decode reservation must still fit the maintenance pool.
        assert!(prod.coordinator_jobs() * 512 * 1024 * 1024 <= prod.maintenance_pool_bytes(), "concurrent 512 MiB units must fit the maintenance pool");
        // Small boxes stay modest rather than thrashing. This pinned exactly 1
        // while the divisor was cores/8; at cores/2 a 4-core box gets 2, which
        // is still only 1 GiB of concurrent decode. The invariant worth holding
        // is "does not thrash a small box and still fits its pool", not a
        // specific number that has to be edited every time the divisor moves.
        let small = DerivedBudget::from_limits(16 * GIB, 4);
        assert!(small.coordinator_jobs() <= 2, "a 4-core box must not run maintenance wide, got {}", small.coordinator_jobs());
        assert!(small.coordinator_jobs() * 512 * 1024 * 1024 <= small.maintenance_pool_bytes(), "concurrent units must fit a small box's pool too");
    }

    /// The coordinator pool must scale with the jobs sharing it, and the three
    /// maintenance shares must still sum to the pool.
    ///
    /// Prod 2026-08-17: the pool was pinned at one `MAX_DECODED_BYTES` while
    /// jobs went to 16, so FairSpill handed each consumer ~32 MB — right at
    /// `ExternalSorterMerge`'s allocation floor — and 72 units failed outright
    /// in 25 minutes instead of spilling.
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
    /// 96 GiB container, so the fraction here lands lower against the real one.
    /// Pinned because raising the fraction without re-checking that gap is
    /// exactly how 85% produced contained OOM kills on 2026-07-30.
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
        // startup log's number IS the enforced number (the 6 GB-vs-24 GB drift
        // was the thresholds and the tree disagreeing).
        assert_eq!(config.effective_wal_max_unflushed_bytes(), config.derived.wal_flush_byte_threshold());
        assert_eq!(config.effective_wal_max_files(), config.derived.wal_flush_file_threshold());

        // Env override still wins.
        config.buffer.timefusion_wal_max_unflushed_mb = 12_000;
        config.buffer.timefusion_wal_max_file_count = 300;
        assert_eq!(config.effective_wal_max_unflushed_bytes(), 12_000 * MIB as u64);
        assert_eq!(config.effective_wal_max_files(), 300);
    }

    /// The lookback IS the suspect-set size (admission offers every unverified
    /// sealed file), so it is bounded on BOTH sides: too small leaves a hole
    /// where one footer-less file pins every wide query at `full-set` dedup
    /// forever (prod 2026-08-08, 2026-07-30, two tenants, a 2-day default); too
    /// large spends the pass clearing correctly-sorted files instead of
    /// rewriting (prod ran 75 at ~1.24 clears/min with zero rewrites).
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
// Budget invariant we try to respect on a fresh host with no overrides:
//     query_pool  ≈ 30% RAM
//     mem_buffer  ≈ 25% RAM
//     foyer_mem   ≈ 15% RAM
//     foyer_meta  ≤ 2% RAM (capped at 512MB)
//     ─────────────────────
//     reserved    ≈ 72% RAM, leaving headroom for Arrow scratch, walrus
//                  mmaps, tantivy, OS page cache.
//
// That remaining ~28% is not spare — it is the only budget parquet decode has
// (explicitly unpooled), alongside walrus mmaps and allocator slack. See
// [`budget_audit`], which sums what the process actually commits (including
// the maintenance pool and MemBuffer's 120% admission ceiling, both of which
// the previous check omitted) and warns when the remainder gets thin.
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
// 16 GiB: the derived tree reserves 10% of the limit for foyer (12 GiB on the
// 120 GiB box); an 8 GiB cap stranded a third of that reservation.
const MAX_FOYER_MEM_MB: usize = 16 * 1024;
const MIN_FOYER_META_MB: usize = 64;
const MAX_FOYER_META_MB: usize = 512;
const MIN_FOYER_DISK_GB: usize = 1;
const MAX_FOYER_DISK_GB: usize = 500;
const MAX_FOYER_META_DISK_GB: usize = 5;

/// Apply host-aware overrides to `config`. Knobs whose env var is set by the
/// user are left untouched.
pub fn apply(config: &mut AppConfig) {
    // ONE memory source: the derived budget tree's cgroup-clamped detection.
    // A second (sysinfo) reading here once let the oversubscription audit warn
    // against a different denominator than the budgets were derived from —
    // exactly the drift class the tree exists to kill.
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

    // Imperative by necessity: each knob is a `&mut` into a *different* config
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
    // Query/maintenance target_partitions. DataFusion defaults it to
    // `num_cpus::get()`, which on Linux reads `sched_getaffinity` — that honors
    // cpuset pinning but NOT the CFS quota (`docker --cpus`). In a CFS-throttled
    // container TF would see the host's core count, split even a single small
    // parquet file into that many scan groups (each re-opening the file's
    // metadata), and oversubscribe the CPU it actually has. `detect_cores`
    // derives from the cgroup quota instead.
    tune("TIMEFUSION_QUERY_PARTITIONS", &mut config.memory.timefusion_query_partitions, cpus, "");

    if applied.is_empty() {
        info!("Auto-tune: no overrides applied (user has set all knobs explicitly or host signals unavailable)");
    } else {
        info!("Auto-tune applied: {}", applied.join(", "));
    }

    // Coherence guard: user-pinned envs can oversubscribe RAM (prod ran a
    // hand-set combination for months before the 2026-07-08 OOM loop). The
    // auto-derived split respects the ≈72% invariant by construction; this
    // checks the FINAL post-override sum and warns loudly — operators keep
    // authority, but the failure mode becomes visible instead of an OOM.
    // RECLAIM, don't merely warn. The tree reserves `query + ingest + foyer +
    // writer + untracked_slack` and hands maintenance the remainder — but the
    // audit counts three ceilings that `reserved` never included: MemBuffer at
    // its 120% admission hard limit rather than the nominal ingest fraction,
    // the tantivy writer peak, and the DataFusion metadata cache. Roughly 5.8 GB
    // on prod, silently taken out of maintenance's share.
    //
    // That was harmless only while maintenance was too broken to claim its pool.
    // When the rollup backfill started completing builds again on 2026-08-13 the
    // box went straight to `committed_mb=64956` against `warn_at_mb=61440` and
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
/// The previous version undercounted by ~30GB on the prod box and so passed
/// while the container was OOM-killed four times in nine hours. Four terms were
/// wrong, each independently enough to hide a kill:
///
/// 1. **The maintenance pool was omitted entirely** — 24GB in prod.
/// 2. **MemBuffer was counted at nominal**, but admission runs to a 120% hard
///    ceiling (`HARD_LIMIT_HEADROOM_DIVISOR` in `buffered_write_layer`), so the
///    largest consumer was understated by a fifth.
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
        // SAFETY: this test runs without #[serial], but only reads env. The
        // values come from the test process's env which doesn't have these
        // vars set (autotune will fire).
        let mut cfg = AppConfig::default();
        apply(&mut cfg);
        // On any modern dev host, MemBuffer should now reflect RAM-based sizing.
        // We only assert non-decrease relative to the 256MB floor; on tiny CI
        // runners the floor wins, which is fine.
        assert!(cfg.buffer.timefusion_buffer_max_memory_mb >= MIN_BUFFER_MB);
        // Reapplying must not change anything (idempotent).
        let before = cfg.buffer.timefusion_buffer_max_memory_mb;
        apply(&mut cfg);
        assert_eq!(cfg.buffer.timefusion_buffer_max_memory_mb, before);
    }

    /// The prod config that was OOM-killed four times in nine hours on
    /// 2026-07-27 must audit as oversubscribed. The previous check passed it,
    /// because it omitted the maintenance pool, counted MemBuffer at nominal
    /// instead of its 120% admission ceiling, ignored `memory_fraction`, and
    /// left out the DataFusion metadata cache. Query + maintenance pools now
    /// come from the derived budget (the knobs they used to read were deleted
    /// with the 2026-07-29 budget tree), so the audit sums those.
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
    /// The tree reserves query + ingest + foyer + writer + untracked_slack and
    /// hands maintenance the remainder, but the audit counts three ceilings
    /// `reserved` never included: MemBuffer at its 120% admission hard limit
    /// (not the nominal ingest fraction), the tantivy writer peak, and the
    /// DataFusion metadata cache — ~5.8 GB on prod, taken silently out of
    /// maintenance's share. Harmless only while maintenance was too broken to
    /// claim its pool; the day the rollup backfill started completing builds
    /// again (2026-08-13) prod sat at committed 64956 / warn_at 61440 and
    /// OOM-killed repeatedly. A warning nobody is awake to read is not a budget.
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
    // nth(2): skips binary + "encrypt-secret"
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
