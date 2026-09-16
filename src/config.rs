use std::{collections::HashMap, num::NonZeroUsize, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * 1024 * 1024;

/// Field-forwarding accessors. `name: Type = (field <tail>);` expands to
/// `pub fn name(&self) -> Type { self.field <tail> }`; `<tail>` is any suffix
/// expression (`.max(1)`, `* MIB`, `.join("wal")`).
/// The `@const` arm forwards a constant instead: `@const name: Type = CONST;`.
macro_rules! getters {
    (@const $($(#[$m:meta])* $name:ident: $ty:ty = $val:expr;)*) => {
        $($(#[$m])* pub fn $name(&self) -> $ty { $val })*
    };
    ($($(#[$m:meta])* $name:ident: $ty:ty = ($field:ident $($tail:tt)*);)*) => {
        $($(#[$m])* pub fn $name(&self) -> $ty { self.$field $($tail)* })*
    };
}

fn read_parsed<T>(path: &str, parse: impl FnOnce(&str) -> Option<T>) -> Option<T> {
    std::fs::read_to_string(path).ok().and_then(|s| parse(&s))
}

/// `None` for unset or unparseable — every env knob here treats both the same.
fn env_parse<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name).ok()?.parse().ok()
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

/// Whole cores a CFS quota/period pair allows, rounded up. `None` for unlimited.
fn cores_from_quota(quota: f64, period: f64) -> Option<usize> {
    (quota > 0.0 && period > 0.0).then(|| ((quota / period).ceil() as usize).max(1))
}

/// Parse cgroup v2 `cpu.max` content (`"<quota> <period>"` or `"max
/// <period>"`) into a whole-core count, rounded up. `None` for unlimited.
fn parse_cgroup_cpu_max(content: &str) -> Option<usize> {
    let mut parts = content.split_whitespace();
    cores_from_quota(parts.next()?.parse().ok()?, parts.next()?.parse().ok()?)
}

/// Detect the effective memory limit in bytes: cgroup v2 → cgroup v1 →
/// `/proc/meminfo` total → a conservative 8 GiB floor. Never panics.
fn detect_memory_limit_bytes() -> usize {
    read_parsed("/sys/fs/cgroup/memory.max", parse_cgroup_v2_memory_max)
        .or_else(|| read_parsed("/sys/fs/cgroup/memory/memory.limit_in_bytes", parse_cgroup_v1_memory_limit))
        // No cgroup limit → unmanaged box: the env override applies HERE only.
        .or_else(|| {
            env_memory_override_bytes().inspect(|v| tracing::warn!("budget tree: no cgroup limit; using TIMEFUSION_MEMORY_LIMIT_GB override ({} GiB)", v / GIB))
        })
        // Shared host: budget HALF the machine — sizing from full host RAM
        // inside a container risks a memcg OOM-loop.
        .or_else(|| {
            read_parsed("/proc/meminfo", parse_meminfo_total_bytes)
                .map(|v| v / 2)
                .inspect(|v| tracing::warn!("budget tree: no cgroup memory limit; deriving from HALF of host RAM ({} GiB)", v / GIB))
        })
        // macOS (dev / off-box CLI): same half-the-machine rule.
        .or_else(host_half_bytes)
        .unwrap_or_else(|| {
            tracing::warn!("budget tree: could not detect memory limit from cgroup or /proc/meminfo; falling back to 8 GiB");
            8 * GIB
        })
}

/// Half of host RAM, where the platform exposes it outside `/proc/meminfo`.
#[cfg(target_os = "macos")]
fn host_half_bytes() -> Option<usize> {
    Some(sysinfo::System::new_with_specifics(sysinfo::RefreshKind::new().with_memory(sysinfo::MemoryRefreshKind::everything())).total_memory() as usize / 2)
        .filter(|half| *half > 0)
        .inspect(|half| tracing::warn!("budget tree: no cgroup; deriving from HALF of host RAM ({} GiB)", half / GIB))
}

#[cfg(not(target_os = "macos"))]
fn host_half_bytes() -> Option<usize> {
    None
}

/// `TIMEFUSION_MEMORY_LIMIT_GB`, parsed. Consulted ONLY when no cgroup limit
/// exists — a containerized deployment can never be resized by env var.
fn env_memory_override_bytes() -> Option<usize> {
    env_parse::<usize>("TIMEFUSION_MEMORY_LIMIT_GB").filter(|gb| *gb > 0).map(|gb| gb * GIB)
}

/// `TIMEFUSION_MEMORY_BUDGET_GB`: sizes the whole tree BELOW the cgroup limit.
/// Only ever LOWERS the effective limit — an over-large value is clamped.
fn env_memory_budget_bytes() -> Option<usize> {
    env_parse::<f64>("TIMEFUSION_MEMORY_BUDGET_GB").filter(|gb| *gb > 0.0).map(|gb| (gb * GIB as f64) as usize)
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
    // parallelism, so clamp. THE process-wide core detector: every sizing
    // decision must read it so they cannot disagree.
    read_parsed("/sys/fs/cgroup/cpu.max", parse_cgroup_cpu_max)
        .or_else(|| cores_from_quota(read_i64("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")? as f64, read_i64("/sys/fs/cgroup/cpu/cpu.cfs_period_us")? as f64))
        .map_or(host, |c| c.clamp(1, host))
}

/// Self-sizing memory/concurrency budget derived once at startup from the
/// container's cgroup limits. The fractions are pinned in code, not overridable.
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

const QUERY_POOL_FRACTION: f64 = 0.20;
/// Share reserved for consumers no pool tracks (parquet decode heap, parse ASTs,
/// allocator overhead), carved out before maintenance takes the remainder.
const UNTRACKED_SLACK_FRACTION: f64 = 0.15;
/// Ingest MemBuffer share of the limit.
const INGEST_BUFFER_FRACTION: f64 = 0.20;
/// Foyer read-cache share.
const FOYER_MEMORY_FRACTION: f64 = 0.10;
/// Per-(rewrite-permit × merge-task) delta-rs writer buffer.
const WRITER_RESERVE_PER_TASK_BYTES: usize = 3 * GIB / 2;
/// delta-rs concurrent merge tasks per optimize run.
const OPTIMIZE_MERGE_TASKS: usize = 2;
/// Cap on files per optimize merge bin: merge fan-in memory scales with
/// fragmentation, so a byte-only bin would peak exactly when compaction is most
/// needed. 32 bounds peak per-merge memory at roughly 1 GB.
const OPTIMIZE_MAX_FILES_PER_BIN: NonZeroUsize = NonZeroUsize::new(32).unwrap();
/// Concurrent heavy maintenance rewrites (dedup/optimize/recompress).
const HEAVY_REWRITE_PERMITS: usize = 10;
/// Per-sort budget: a spill THRESHOLD on a FairSpillPool, not a hard
/// reservation — exceeding it degrades to bounded disk spill. The product with
/// `HEAVY_REWRITE_PERMITS` is the fan-in envelope, so raising one without
/// lowering the other risks an OOM.
const PER_SORT_BUDGET_BYTES: usize = 2 * GIB;
/// Per-sort budget for the COORDINATOR rewrite path: the measured per-rewrite
/// footprint, one rung below the measured concurrency failure cliff.
const COORDINATOR_PER_SORT_BUDGET_BYTES: usize = 5 * GIB / 4;

/// The largest COMPRESSED bin one coordinator sort can decode inside its budget.
///
/// Packing targets are expressed in compressed bytes and sort budgets in
/// decoded ones; this is the conversion between them. Without it a bin can be
/// sealed at a size no sort can ever finish decoding.
pub fn coordinator_bin_compressed_cap_bytes() -> i64 {
    (COORDINATOR_PER_SORT_BUDGET_BYTES / crate::database::DECODED_BYTES_PER_COMPRESSED as usize) as i64
}

/// The cap a PACKER may use: the decode cap with a margin, but never so small
/// that two target-sized files cannot pair.
///
/// The margin exists because `DECODED_BYTES_PER_COMPRESSED` is an optimistic
/// estimate, so a bin priced at exactly one sort budget still spills into an
/// unspillable merge. The floor is mandatory: a cap that cannot admit the two
/// smallest files admits no pair at all, so the cell re-enqueues forever and the
/// lane retires nothing.
pub fn coordinator_packing_cap_bytes(smallest_pair_bytes: i64) -> i64 {
    let margin = coordinator_bin_compressed_cap_bytes() * 3 / 5;
    // The floor is the PAIR itself, not a per-file size doubled: doubling loses a
    // byte whenever the pair sums odd, and prod 2026-09-13 hit exactly that —
    // `target=82703666 smallest_pair_bytes=82703667 smallest_pair_fits=false`.
    // One byte short is as unpackable as a hundred megabytes short.
    margin.max(smallest_pair_bytes)
}
/// Concurrent target-sized repair rewrites the repair budget must hold. A repair
/// unit is exactly ONE file and cannot be split, so the budget is a multiple of
/// the target file size, never a free-standing byte count.
const REPAIR_REWRITE_TARGET_FILES: usize = 2;
/// Decoded bytes one byte of sort pool can carry before rewrites start failing —
/// the conversion between the repair semaphore (DECODED bytes) and the pool it
/// draws from (POOL bytes). Highest ratio measured passing.
const SAFE_DECODED_PER_POOL_BYTE: f64 = 1.79;
/// Per-sort slices the hygiene lane keeps whatever repair would reserve. Two,
/// not one: HotPacking and SealedConsolidation SHARE the permit, so at one slice
/// a single stalled unit takes the whole lane with it.
const LIGHT_MIN_SLICES: usize = 2;

/// Heavy maintenance's slice of the whole maintenance pool — of the WHOLE pool,
/// not of the coordinator's residual; see `heavy_share_bytes`.
const HEAVY_POOL_SHARE: f64 = 0.30;

/// Pool slice per in-flight coordinator job. Kept equal to the admission
/// ceiling so a job that is allowed to decode N bytes has N bytes of pool to
/// hold them in before it must spill; see `coordinator_share_bytes`.
const COORDINATOR_JOB_POOL_BYTES: usize = crate::maintenance_coordinator::MAX_DECODED_BYTES as usize;
/// Floor so a tiny box never zeroes the maintenance pool.
const MAINTENANCE_FLOOR_BYTES: usize = GIB;

/// Ceiling on the query session's `target_partitions`. It bounds the sort
/// machinery's per-partition, NON-SPILLABLE reservations, which is what
/// exhausts the query pool on a wide box: raising it buys scan concurrency and
/// sort reservation in equal measure. `TIMEFUSION_QUERY_PARTITIONS` overrides it.
const QUERY_PARTITIONS_MAX: usize = 24;

/// Concurrent sort-bearing queries the query pool must survive.
///
/// monoscope opens **TWO** pools against this server, both from `tfParams` in
/// its `src/System/Config.hs`, and the budget must cover their SUM. Counting
/// only the first is how prod kept failing dashboard reads with `Resources
/// exhausted`: at 8 the clamp never binds (22 GB / (24 x 8) = 114 MB, above the
/// 64 MB default), so nothing limited the real load, and a single observed
/// failure had the 22 GB pool down to 78.1 MB free behind roughly 344
/// non-spillable 64 MB merge reservations — about fourteen concurrent sorts,
/// well past the eight budgeted for.
///
/// Keep these two in step with monoscope, or the arithmetic silently protects a
/// load that does not exist. monoscope's own comment still says "this pool size
/// IS our concurrency limit against TimeFusion" three lines above the second
/// pool being opened.
const CLIENT_PGWIRE_POOL: usize = 8;
const CLIENT_HASQL_POOL: usize = 30;
const CONCURRENT_SORT_QUERIES: usize = CLIENT_PGWIRE_POOL + CLIENT_HASQL_POOL;

/// The query pool's concurrency: every client connection that may plan a sort.
pub const fn client_sort_concurrency() -> usize {
    CONCURRENT_SORT_QUERIES
}

const DEFAULT_SORT_SPILL_RESERVATION_BYTES: usize = 64 * MIB;
/// Floor, so a small box (or a large `target_partitions`) cannot clamp the
/// reservation to nothing and push sorts back into dying mid-merge.
const MIN_SORT_SPILL_RESERVATION_BYTES: usize = 8 * MIB;

/// Effective `sort_spill_reservation_bytes`: the requested value, LOWERED so
/// that `CONCURRENT_SORT_QUERIES` sorts of `partitions` partitions still fit
/// `pool_bytes`.
///
/// `ExternalSorter` takes this reservation per partition, up front, and its
/// merge half cannot spill, so every sort pays it whether or not it sorts
/// anything — hence the clamp rather than honouring an over-large request.
/// The share of the pool reservations may consume. The rest is what the sorts
/// actually sort INTO.
///
/// Without this the cap is `pool / (partitions x concurrency)` — the value at
/// which reservations exactly fill the pool and leave **nothing** for data, which
/// is not a budget at all. Prod 2026-09-14 ran at 8 partitions x 64 MB x 38
/// possible clients = 19.5 GB of a 22 GB pool locked up before a single row is
/// sorted, and failed dashboard reads with 78.1 MB free.
const RESERVATION_POOL_SHARE: usize = 2;

/// `concurrency` is how many sorts may share `pool_bytes` AT ONCE, and it differs
/// per pool. The query pool faces monoscope's client connections
/// (`CONCURRENT_SORT_QUERIES`); the maintenance pool faces its own bounded worker
/// count and never sees a client at all.
///
/// Passing the client number for the maintenance pool over-divides and starves the
/// reservation — which is not a harmless under-estimate, because this value is the
/// memory held BACK so a spilling operator can finish its merge. Prod 2026-09-14:
/// #288 took the maintenance reservation from 64 MB to 33 MB and the 5 GB pool
/// began failing rollup aggregations with "Failed to reserve memory for sort
/// during spill" — the exact failure the reservation exists to prevent, four times
/// in the first four minutes against zero in the preceding hour.
pub fn sort_spill_reservation_bytes(requested: Option<usize>, partitions: usize, pool_bytes: usize, concurrency: usize) -> usize {
    let cap = pool_bytes / (partitions.max(1) * concurrency.max(1) * RESERVATION_POOL_SHARE);
    requested.unwrap_or(DEFAULT_SORT_SPILL_RESERVATION_BYTES).min(cap).max(MIN_SORT_SPILL_RESERVATION_BYTES)
}

/// How many heavy (spilling-sort) queries may execute against the query pool at
/// once, independent of how many connections exist. Connections are cheap and
/// TimeFusion caps none of them; a concurrent unbounded sort is ~partitions x a
/// healthy reservation of NON-spillable pool memory, so N of them physically
/// cannot share a fixed pool. Accept every connection, admit K heavy queries,
/// queue the rest — the warehouse-standard split (Redshift WLM, Snowflake).
///
/// K is derived from the pool geometry, NEVER from the client connection count:
/// `pool_share / (partitions x reservation)`, so raising monoscope's pool cannot
/// silently shrink each sort's merge reservation toward the mid-merge-death floor
/// (the trap `sort_spill_reservation_bytes`'s client-count divisor set). The
/// reservation stays a fixed healthy value; only the ADMISSION count flexes.
pub fn max_concurrent_heavy_sorts(partitions: usize, pool_bytes: usize) -> usize {
    let per_sort = DEFAULT_SORT_SPILL_RESERVATION_BYTES * partitions.max(1);
    (pool_bytes / (per_sort * RESERVATION_POOL_SHARE)).max(MIN_CONCURRENT_HEAVY_SORTS)
}

/// A floor so a small box still admits a useful degree of concurrency rather than
/// serializing every heavy query.
const MIN_CONCURRENT_HEAVY_SORTS: usize = 4;

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
    /// Pure derivation over an already-detected limit/core count; touches no
    /// filesystem.
    fn from_limits(memory_limit_bytes: usize, cores: usize) -> Self {
        Self::from_limits_with_profile(memory_limit_bytes, cores, BudgetProfile::Server)
    }

    fn from_limits_with_profile(memory_limit_bytes: usize, cores: usize, profile: BudgetProfile) -> Self {
        let share = |fraction: f64| (memory_limit_bytes as f64 * fraction) as usize;
        // The last term is untracked-consumer slack, carved out BEFORE
        // maintenance (the residual claimant) takes the remainder.
        // MaintenanceCli has no queries/ingest: token slices for query pool and
        // foyer, no slack, rest to maintenance.
        let (query_pool_bytes, ingest_buffer_bytes, foyer_memory_bytes, untracked_slack_bytes) = match profile {
            BudgetProfile::MaintenanceCli => (share(0.08), share(0.02), share(0.02), 0),
            BudgetProfile::Server => (share(QUERY_POOL_FRACTION), share(INGEST_BUFFER_FRACTION), share(FOYER_MEMORY_FRACTION), share(UNTRACKED_SLACK_FRACTION)),
        };
        // Capped at 10% of the limit so the full reserve cannot budget more
        // than the container holds on a small box.
        let writer_reserve_bytes = (HEAVY_REWRITE_PERMITS * OPTIMIZE_MERGE_TASKS * WRITER_RESERVE_PER_TASK_BYTES).min(memory_limit_bytes / 10);
        let reserved = query_pool_bytes + ingest_buffer_bytes + foyer_memory_bytes + writer_reserve_bytes + untracked_slack_bytes;
        let maintenance_pool_bytes = memory_limit_bytes.saturating_sub(reserved).max(MAINTENANCE_FLOOR_BYTES);
        Self { memory_limit_bytes, cores, query_pool_bytes, ingest_buffer_bytes, foyer_memory_bytes, writer_reserve_bytes, maintenance_pool_bytes, profile }
    }

    /// Detect the real container limits and derive the tree. The only env input
    /// is `TIMEFUSION_MEMORY_BUDGET_GB`, which can lower (never raise) the limit.
    pub fn compute() -> Self {
        // Anything unrecognised (including unset) is the server profile.
        let profile = env_parse("TIMEFUSION_BUDGET_PROFILE").unwrap_or_default();
        Self::from_limits_with_profile(effective_limit(detect_memory_limit_clamped(), env_memory_budget_bytes()), detect_cores(), profile)
    }

    getters! {
        query_pool_bytes: usize = (query_pool_bytes);
        cores: usize = (cores);
        buffer_max_bytes: usize = (ingest_buffer_bytes);
        foyer_memory_bytes: usize = (foyer_memory_bytes);
        /// Raw-object-bytes half of the cache reservation; the other half is the
        /// logical-count index. Both stay inside the one reservation.
        object_cache_memory_bytes: usize = (foyer_memory_bytes / 2);
        /// The other half — exactly what `object_cache_memory_bytes` leaves.
        logical_count_memory_bytes: usize = (foyer_memory_bytes.div_ceil(2));
        writer_reserve_bytes: usize = (writer_reserve_bytes);
        memory_limit_bytes: usize = (memory_limit_bytes);
        maintenance_pool_bytes: usize = (maintenance_pool_bytes);
    }

    /// Hands `bytes` back from the maintenance pool (never below the floor);
    /// returns what was actually surrendered.
    pub fn reclaim_maintenance_pool(&mut self, bytes: usize) -> usize {
        let before = self.maintenance_pool_bytes;
        self.maintenance_pool_bytes = before.saturating_sub(bytes).max(MAINTENANCE_FLOOR_BYTES);
        before - self.maintenance_pool_bytes
    }

    /// The durable coordinator's own pool, carved off before the heavy/light
    /// split. Sized as `jobs x MAX_DECODED_BYTES`, capped at three fifths of the
    /// maintenance pool: the share divides by `jobs` through the `FairSpillPool`,
    /// so too small a share drops each rewrite below `ExternalSorterMerge`'s
    /// floor and units fail instead of spilling.
    pub fn coordinator_share_bytes(&self) -> usize {
        match self.profile {
            // The CLI drives engines directly; no coordinator runs.
            BudgetProfile::MaintenanceCli => 0,
            BudgetProfile::Server => (self.coordinator_jobs() * COORDINATOR_JOB_POOL_BYTES).min(self.maintenance_pool_bytes * 3 / 5),
        }
    }

    /// The decoded-bytes budget shared by concurrent repair rewrites. Priced in
    /// BYTES, not permits: a bin larger than the budget takes all of it and runs
    /// alone, while small bins share. Derived from the compaction target file
    /// size so a change there cannot silently make repair unrunnable.
    pub fn repair_rewrite_budget_bytes(&self) -> usize {
        REPAIR_REWRITE_TARGET_FILES * (crate::database::COORDINATOR_HOT_TARGET_BYTES as usize) * (crate::database::DECODED_BYTES_PER_COMPRESSED as usize)
    }

    /// The same budget in whole MiB, the unit the repair semaphore counts in:
    /// one permit per MiB stays under `Semaphore`'s permit ceiling.
    pub fn repair_rewrite_budget_mib(&self) -> usize {
        (self.repair_rewrite_budget_bytes() / MIB).max(1)
    }

    /// Per-sort budgets `light_optimize_k` must hold back so the repair lane's
    /// DECODED budget has enough POOL behind it.
    pub fn repair_pool_holdback_slices(&self) -> usize {
        (self.repair_rewrite_budget_bytes() as f64 / SAFE_DECODED_PER_POOL_BYTE / COORDINATOR_PER_SORT_BUDGET_BYTES as f64).ceil() as usize
    }

    /// What heavy and light divide, once the coordinator has taken its share.
    fn maintenance_split_bytes(&self) -> usize {
        self.maintenance_pool_bytes - self.coordinator_share_bytes()
    }

    /// Heavy maintenance (dedup/optimize/recompress) share: a fraction of the
    /// WHOLE maintenance pool, deliberately NOT of the coordinator's residual.
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

    getters! { @const
        /// Concurrent heavy maintenance rewrites. Pinned, not box-derived: the cap
        /// guards against an uncapped-rewrite OOM.
        rewrite_permits: usize = HEAVY_REWRITE_PERMITS;
        /// delta-rs concurrent merge tasks per optimize run. Pinned.
        optimize_merge_tasks: usize = OPTIMIZE_MERGE_TASKS;
        /// Files-per-bin cap for every optimize rewrite; bounds per-merge memory
        /// regardless of box size.
        optimize_max_files_per_bin: NonZeroUsize = OPTIMIZE_MAX_FILES_PER_BIN;
        /// Pinned, not box-derived: the empirical sort peak.
        per_sort_budget_bytes: usize = PER_SORT_BUDGET_BYTES;
    }

    /// Scan batch size for maintenance sessions. A batch is indivisible — it
    /// must be admitted to the memory pool before it can ever spill — so the
    /// CLI profile's small cgroups need a smaller unit to make progress.
    pub fn maintenance_batch_size(&self) -> &'static str {
        match self.profile {
            BudgetProfile::MaintenanceCli => "256",
            BudgetProfile::Server => "2048",
        }
    }

    /// Concurrent hot-tail light-optimize sorts: memory-bound by the
    /// coordinator share, CPU-bound to a quarter of cores, and never more than
    /// there are hot projects to compact.
    pub fn light_optimize_k(&self, hot_project_count: usize) -> usize {
        // Priced against the COORDINATOR pool, which is what these units
        // actually allocate from — not the light share. Repair's holdback comes
        // off the top but yields before the hygiene lane is zeroed
        // (`LIGHT_MIN_SLICES`): one permit shared by HotPacking and
        // SealedConsolidation means a single long unit stops the lane dead.
        let slices = self.light_pool_slices();
        let mem_bound = slices.saturating_sub(self.repair_pool_holdback_slices().min(slices.saturating_sub(LIGHT_MIN_SLICES)));
        mem_bound.min(self.cores / 4).min(hot_project_count).max(1)
    }

    /// Per-sort slices the coordinator share holds — the pool term both light-K variants start from.
    fn light_pool_slices(&self) -> usize {
        self.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES
    }

    /// Concurrently admitted maintenance coordinator units. Bounded by the box:
    /// each unit reserves at most `MAX_DECODED_BYTES`, making the memory term
    /// exact. `TIMEFUSION_COORDINATOR_JOB_WORKERS=1` serializes maintenance.
    pub fn coordinator_jobs(&self) -> usize {
        env_parse::<usize>("TIMEFUSION_COORDINATOR_JOB_WORKERS").filter(|n| *n > 0).unwrap_or_else(|| {
            // Jobs are only useful up to the inner rewrite/sort permit pool
            // (`HEAVY_REWRITE_PERMITS`); wider just converts slots into queueing.
            (self.maintenance_pool_bytes / (512 * MIB)).min(self.cores / 3).clamp(1, 16)
        })
    }

    /// K with the project-count term removed (memory × CPU only) — sizes the
    /// light pool slice, which cannot depend on the tick's plan.
    pub fn max_light_optimize_k(&self) -> usize {
        self.light_optimize_k(usize::MAX)
    }

    /// The light permits the repair holdback is reserving, which repair can only
    /// use if it HAS work.
    ///
    /// `light_optimize_k` subtracts `repair_pool_holdback_slices` so a repair
    /// rewrite always has budget. Prod 2026-09-13: `pending_repair` was ZERO all
    /// day while that reservation pinned the hygiene lane at K=2, and the sealed
    /// backlog is what the box is behind on. At 32 cores the share holds 4
    /// slices and the holdback takes 2 of them.
    ///
    /// This is a RESERVATION, not a memory ceiling: `slices` is already
    /// `coordinator_share / COORDINATOR_PER_SORT_BUDGET`, so lending these out
    /// uses exactly the share the budget tree computed and no more. That is why
    /// it is safe to lend, and why it must be RETURNED the moment repair has
    /// work — the pool cannot hold both at once.
    pub fn repair_holdback_permits(&self) -> usize {
        self.max_light_optimize_k_ignoring_holdback().saturating_sub(self.max_light_optimize_k())
    }

    /// `max_light_optimize_k` as if repair reserved nothing. Same floors — the
    /// CPU term and the pool's slice count still bind.
    fn max_light_optimize_k_ignoring_holdback(&self) -> usize {
        self.light_pool_slices().min(self.cores / 4).max(1)
    }

    pub fn tick_budget(&self, cron_period: Duration) -> Duration {
        cron_period.mul_f64(0.8)
    }

    /// Wave-boundary memory brake, as a fraction of the BUDGETED limit. A
    /// one-way safety valve — never used to size K. Above ~85%, allocation
    /// bursts between wave boundaries outrun jemalloc purge.
    pub fn memory_brake_limit_bytes(&self) -> usize {
        (self.memory_limit_bytes as f64 * 0.80) as usize
    }

    /// WAL emergency-flush byte threshold, as a fraction of the ingest buffer
    /// so the two cannot drift out of proportion.
    pub fn wal_flush_byte_threshold(&self) -> u64 {
        // Floor at 4 GiB: the WAL counts PREALLOCATED file bytes (walrus blocks
        // are up to 1 GiB each), so a lower threshold trips on preallocation
        // alone and drains open buckets before backpressure can engage.
        ((self.ingest_buffer_bytes / 2) as u64).max(4 * GIB as u64)
    }

    /// WAL emergency-flush file-count threshold: a FLOOR of 200, scaled up on
    /// boxes with a bigger ingest buffer. Never derived downward — 200 bounds
    /// restart REPLAY, not memory.
    pub fn wal_flush_file_threshold(&self) -> usize {
        const BASELINE_BUFFER_BYTES: usize = 24 * GIB;
        const BASELINE_FILES: f64 = 200.0;
        (BASELINE_FILES * (self.ingest_buffer_bytes as f64 / BASELINE_BUFFER_BYTES as f64)).round().max(BASELINE_FILES) as usize
    }

    /// Startup log of the whole derived tree. K is logged at an illustrative
    /// 11 hot projects.
    pub fn log(&self) {
        tracing::info!(
            profile = ?self.profile,
            detected_limit_gb = detect_memory_limit_clamped() / GIB,
            effective_limit_gb = self.memory_limit_bytes / GIB,
            cores = self.cores,
            query_pool_gb = self.query_pool_bytes() / GIB,
            ingest_buffer_gb = self.buffer_max_bytes() / GIB,
            cache_memory_gb = self.foyer_memory_bytes() / GIB,
            foyer_memory_gb = self.object_cache_memory_bytes() / GIB,
            logical_count_memory_gb = self.logical_count_memory_bytes() / GIB,
            writer_reserve_gb = self.writer_reserve_bytes() / GIB,
            maintenance_pool_gb = self.maintenance_pool_bytes() / GIB,
            coordinator_share_gb = self.coordinator_share_bytes() / GIB,
            heavy_share_gb = self.heavy_share_bytes() / GIB,
            light_share_gb = self.light_share_bytes() / GIB,
            rewrite_permits = self.rewrite_permits(),
            optimize_merge_tasks = self.optimize_merge_tasks(),
            light_optimize_k_at_11_hot_projects = self.light_optimize_k(11),
            memory_brake_limit_gb = self.memory_brake_limit_bytes() / GIB,
            wal_flush_byte_threshold_gb = self.wal_flush_byte_threshold() / GIB as u64,
            wal_flush_file_threshold = self.wal_flush_file_threshold(),
            "self-sizing budget tree derived at startup"
        );
    }
}

/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Each sub-config is loaded separately: envy does not handle
    // `#[serde(flatten)]`.
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
    let mut cfg = load_config_from_env()?;
    apply(&mut cfg);
    let _ = CONFIG.set(cfg);
    Ok(config())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}

/// Global config if initialized, else `None`, for construction paths that may
/// run before `init_config()` and want defaults rather than a panic.
pub fn try_config() -> Option<&'static AppConfig> {
    CONFIG.get()
}

/// Test-only: seed the global config so `try_config()` callers see it. No-op if
/// already set.
#[doc(hidden)]
pub fn set_config_for_test(cfg: AppConfig) {
    let _ = CONFIG.set(cfg);
}

/// Whether the operator opted into open auth for local dev via
/// `TIMEFUSION_ALLOW_INSECURE_AUTH=true`; auth paths gate fail-secure defaults
/// on this flag.
pub fn is_insecure_auth_allowed() -> bool {
    std::env::var("TIMEFUSION_ALLOW_INSECURE_AUTH").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

/// Bound on the post-commit cache confirm. It is an optimization, never a
/// durability gate, so a slow warm must not stall the flush loop.
pub const CACHE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrency of the confirm's full-file fetches. Each miss buffers a whole
/// flush-sized parquet body in untracked heap ON the flush path, so peak is
/// roughly this times the largest added file.
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
    /// Self-sizing budget tree, derived (not deserialized) at construction from
    /// the cgroup limit, optionally lowered by `TIMEFUSION_MEMORY_BUDGET_GB`.
    #[serde(skip)]
    pub derived: DerivedBudget,
}

/// Tantivy sidecar-index config. Indexing is on for any table whose YAML schema
/// declares `tantivy.indexed: true` on at least one field; there is no override.
#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde_inline_default(64)]
    pub timefusion_tantivy_max_index_size_mb: u64,
    /// Byte budget for the local extracted-index cache
    /// (`<timefusion_data_dir>/tantivy_cache`), enforced LRU-first by the
    /// "Tantivy cache reap" cron — the only thing that deletes from that tree.
    /// Shares a volume with the WAL. Must exceed the query working set with
    /// headroom, or the reaper evicts what the prefetch cron re-downloads.
    #[serde_inline_default(200)]
    pub timefusion_tantivy_cache_disk_gb: u64,
    /// How often to enforce `timefusion_tantivy_cache_disk_gb`. Each sweep
    /// walks the whole cache tree; empty disables the reap (and the bound).
    #[serde_inline_default("0 */10 * * * *".to_string())]
    pub timefusion_tantivy_cache_reap_schedule: String,
    // Low: index packing is on the flush hot path, where high zstd levels cost
    // far more CPU than the output size they save.
    #[serde_inline_default(3)]
    pub timefusion_tantivy_compression_level: i32,
    #[serde_inline_default(2)]
    pub timefusion_tantivy_min_files_for_pushdown: usize,
    /// If a tantivy prefilter would produce more than this many hits, skip
    /// the `id IN (...)` pushdown entirely — planning cost grows with the
    /// literal count, so above this the pushdown is slower than the scan it
    /// replaces.
    #[serde_inline_default(2_000)]
    pub timefusion_tantivy_prefilter_max_hits: usize,
    /// If a tantivy prefilter selects more than this percentage of the
    /// indexed rows, the pushdown isn't worth the round-trip; skip it and
    /// let Delta scan with the original predicate. Default 50 (%).
    #[serde_inline_default(50)]
    pub timefusion_tantivy_prefilter_min_selectivity_pct: u32,
    /// Route exact `col = 'lit'` on raw-tokenized high-cardinality columns
    /// (trace_id/span_id/id/parent_id) through the tantivy id-prefilter, not
    /// just LIKE (and IN-lists as OR-of-terms). Safe under OR: a disjunction is
    /// routed only when every branch is covered, and the original predicate
    /// always stays as the post-filter backstop.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_route_equality: bool,
    /// Concurrent index builds during backfill/reconcile/post-optimize reindex.
    /// Low by default so it runs alongside live query load.
    #[serde_inline_default(2)]
    pub timefusion_tantivy_build_concurrency: usize,
    /// Backfill/reconcile skips parquet files larger than this (MB); 0 = no
    /// limit. The real memory bound is `pack_dir`, which builds the
    /// compressed-index tar in memory; the parquet read itself is streaming.
    #[serde_inline_default(4096)]
    pub timefusion_tantivy_backfill_max_file_mb: u64,
    /// File-level scan pruning: when the prefilter engages, files whose
    /// covering index returned zero hits are excluded from the Delta scan
    /// entirely.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_file_pruning: bool,
    /// Warm the local index cache with blobs whose data is at most this many
    /// days old, at startup AND on `timefusion_tantivy_prefetch_schedule`
    /// (0 = off). Keep it small: extraction inflates blobs several-fold against
    /// `cache_disk_gb`.
    #[serde_inline_default(3)]
    pub timefusion_tantivy_prefetch_days: u32,
    /// Re-warm cadence for `timefusion_tantivy_prefetch_days`. Also re-stamps
    /// `last_used` on hot dirs, which is what keeps them at the young end of
    /// the reaper's LRU order. Empty disables the periodic pass (startup
    /// warming still runs).
    #[serde_inline_default("0 */15 * * * *".to_string())]
    pub timefusion_tantivy_prefetch_schedule: String,
    /// Seed the local extracted-index cache at publish time so a freshly built
    /// index is not re-downloaded to answer the first query. The upload still
    /// happens either way; S3 remains the source of truth.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_seed_cache_on_publish: bool,
    /// Open-index (mmap + reader) LRU capacity. Must exceed a single query's
    /// working set — a wide window opens hundreds of indexes. Costs fds (~28 per
    /// open index) and file-backed mmap pages, not anon memory.
    #[serde_inline_default(2048)]
    pub timefusion_tantivy_reader_cache_entries: usize,
    /// Concurrent per-index download+open+search tasks within one query. These
    /// are IO-bound (object-store GET or page-cache read), so the useful
    /// ceiling is high.
    #[serde_inline_default(32)]
    pub timefusion_tantivy_search_concurrency: usize,
    /// TTL for the parsed-manifest cache, which sits on the planning path. Safe
    /// to lengthen: this process invalidates its own entry on publish and GC,
    /// and a stale entry only costs a wasted lookup.
    #[serde_inline_default(300)]
    pub timefusion_tantivy_manifest_ttl_secs: u64,
    /// Files a single backfill pass will attempt — a COUNT ceiling only, against
    /// a pathological queue of tiny files. The real bound is
    /// `timefusion_tantivy_backfill_max_bytes_per_pass_mb`; set too low, this
    /// binds instead and starves the cheap tables.
    #[serde_inline_default(320)]
    pub timefusion_tantivy_backfill_max_files_per_pass: usize,
    /// The real per-pass bound: total INPUT bytes a backfill pass will read.
    /// At least one file is always attempted, so an over-budget file still
    /// makes progress instead of wedging the queue.
    #[serde_inline_default(2048)]
    pub timefusion_tantivy_backfill_max_bytes_per_pass_mb: u64,
    /// Percentage of each backfill pass reserved for the OLDEST uncovered files,
    /// carved OUT of the cap (never added to it) so pass cost is unchanged.
    /// 0 = newest-first. Raise it if the oldest files stay frozen while newer
    /// ones converge.
    #[serde_inline_default(0)]
    pub timefusion_tantivy_backfill_tail_share_pct: u8,
    /// Skip TODAY's date partition in the backfill queue: the hot partition is
    /// rewritten continuously, so an index built for one of its files is usually
    /// GC'd before it is consulted.
    ///
    /// KNOWN GAP: `light_optimize_tail` has no tantivy hook, so a hot-tail merge
    /// drops its inputs' coverage and the output stays uncovered until the date
    /// rolls over. Correctness is unaffected; the hottest window loses its
    /// prefilter.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_backfill_skip_today: bool,
    /// Row-selection pushdown: when the prefilter engages, files whose index
    /// was built in parquet row order get a per-file ParquetAccessPlan so the
    /// reader decodes only matching rows.
    #[serde_inline_default(true)]
    pub timefusion_tantivy_row_selection: bool,
}

impl TantivyConfig {
    /// Tables to index: schemas with `tantivy.indexed: true` on any field,
    /// computed once. `BTreeSet` so `indexed_tables` is sorted by construction.
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
    getters! {
        compression_level: i32 = (timefusion_tantivy_compression_level);
        prefilter_max_hits: usize = (timefusion_tantivy_prefilter_max_hits.max(1));
        prefilter_min_selectivity_pct: u32 = (timefusion_tantivy_prefilter_min_selectivity_pct.min(100));
        route_equality: bool = (timefusion_tantivy_route_equality);
        /// Disk budget in bytes. Floored at 1 GB — zero would reap the cache to
        /// nothing every 10 minutes, turning every query into a re-download.
        cache_disk_bytes: u64 = (timefusion_tantivy_cache_disk_gb.max(1) * GIB as u64);
        /// Floored at 1: zero concurrency would deadlock the per-index fan-out.
        search_concurrency: usize = (timefusion_tantivy_search_concurrency.max(1));
        seed_cache_on_publish: bool = (timefusion_tantivy_seed_cache_on_publish);
    }
    /// Floored at 1: a zero-capacity LRU would make every open a cold open.
    pub fn reader_cache_entries(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.timefusion_tantivy_reader_cache_entries).unwrap_or(NonZeroUsize::MIN)
    }
    pub fn manifest_ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_tantivy_manifest_ttl_secs)
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
    /// (humantime, e.g. "15s"). See `connect_timeout` for the effective default.
    #[serde(default)]
    pub timefusion_s3_connect_timeout: Option<String>,
    /// Total per-request bound (humantime, e.g. "900s"). Must comfortably
    /// exceed the time to PUT one large multipart part under load — too
    /// short and concurrent big PUTs starve connections.
    #[serde(default)]
    pub timefusion_s3_request_timeout: Option<String>,
    /// Per-request bound for the COMMIT-LOG request class (`_delta_log/*.json`,
    /// `_last_checkpoint`, log LISTs) — humantime, default "30s". Separate from
    /// `timefusion_s3_request_timeout`: a hung commit PUT holds a table's commit
    /// lock and stalls every committer on it. Safe to bound tightly — the
    /// conditional commit PUT is not idempotent in object_store, so a timeout is
    /// never silently re-sent; `probe_commit_landed` decides the outcome.
    #[serde(default)]
    pub timefusion_s3_log_request_timeout: Option<String>,
}

/// Warm-connection pool size per host, shared by both object-store client
/// construction paths. Must stay above the query scan fanout so concurrent
/// GETs reuse sockets instead of re-doing TLS mid-fanout.
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
    /// Effective connect timeout. Generous on purpose: it trades slower failure
    /// for surviving transient connection refusals.
    pub fn connect_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_connect_timeout.as_deref(), "60s")
    }

    pub fn request_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_request_timeout.as_deref(), "900s")
    }

    /// Effective per-request bound for the commit-log request class.
    /// Deliberately not clamped against `request_timeout`; the two classes are
    /// independent.
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
            // Kept in sync with create_object_store so both paths agree.
            ("connect_timeout", Some(self.connect_timeout())),
            ("timeout", Some(self.request_timeout())),
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
    /// Interactive statement cap, in seconds.
    #[serde_inline_default(90)]
    pub timefusion_pgwire_max_statement_secs: u64,
    /// How far a session may RAISE its statement timeout via
    /// `SET statement_timeout`, in seconds. 0 = it cannot.
    #[serde_inline_default(0)]
    pub timefusion_pgwire_batch_statement_secs: u64,
    #[serde(default)]
    pub timefusion_otel_scan_guard: OtelScanGuard,
}

impl CoreConfig {
    getters! {
        wal_dir: PathBuf = (timefusion_data_dir.join("wal"));
        cache_dir: PathBuf = (timefusion_data_dir.join("cache"));
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    // Bounds how much un-flushed WAL a restart must replay; trades ~5x more Delta
    // commits / small files for shorter startup.
    #[serde_inline_default(60)]
    pub timefusion_flush_interval_secs: u64,
    // Flush dwell: a sealed-but-young bucket waits this long from CREATION before
    // the periodic flush commits it, unless it is already big. -1 = one
    // bucket_duration, 0 = off. See flush_completed_buckets.
    #[serde_inline_default(-1)]
    pub timefusion_flush_dwell_secs: i64,
    #[serde_inline_default(70)]
    pub timefusion_buffer_retention_mins: u64,
    #[serde_inline_default(60)]
    pub timefusion_eviction_interval_secs: u64,
    #[serde_inline_default(4096)]
    pub timefusion_buffer_max_memory_mb: usize,
    // Total graceful-shutdown budget shared by ALL serial shutdown phases
    // (PGWire drain → buffered-layer flush + cursor snapshot). Keep at ~80% of the
    // orchestrator's SIGTERM→SIGKILL grace so the cursor snapshot lands in time.
    #[serde_inline_default(70)]
    pub timefusion_stop_grace_secs: u64,
    #[serde_inline_default(10)]
    pub timefusion_wal_corruption_threshold: usize,
    // Concurrent staged flush commits. Parquet encode + S3 upload happen outside
    // the per-table commit lock, so this scales upload throughput directly while
    // bounding in-flight encode memory.
    #[serde_inline_default(8)]
    pub timefusion_flush_parallelism: usize,
    /// Coalesce one tick's per-project flush commits into one commit per PHYSICAL
    /// Delta table. Parquet writes still fan out `flush_parallelism`-wide; only the
    /// commit is shared. Custom-storage projects have their own `_delta_log` and are
    /// never coalesced with default storage.
    #[serde_inline_default(false)]
    pub timefusion_flush_coalesce_commits: bool,
    #[serde(default)]
    pub timefusion_flush_immediately: bool,
    /// `insert()` admits over the memory hard limit instead of rejecting a write
    /// whose backpressure budget is exhausted — trades a reject for unbounded
    /// growth if flush can't keep up.
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
    /// Fsync the WAL shard before acking DML appends. Batched INSERT appends are
    /// always flushed before ack; only single-entry DML appends would otherwise
    /// defer to the background fsync thread.
    #[serde_inline_default(true)]
    pub timefusion_wal_ack_fsync: bool,
    // 0 = unset → derived (DerivedBudget::wal_flush_file_threshold); env-set wins.
    #[serde_inline_default(0)]
    pub timefusion_wal_max_file_count: usize,
    /// Force-flush backstop on total on-disk (unflushed) WAL bytes, for the case
    /// the memory-pressure valve misses. 0 = derive from the buffer budget.
    #[serde(default)]
    pub timefusion_wal_max_unflushed_mb: usize,
    /// Disk-runaway breaker: HARD cap on total on-disk WAL bytes, past which
    /// INSERTs are rejected. Total on-disk includes flushed segments the
    /// age-gated GC still holds, so keep this well above busy-hour residue.
    /// Checked by a dedicated WAL-gate task, never the flush loop (which stalls
    /// in exactly the overload this guards against). DML mem legs are exempt:
    /// failing an UPDATE mid-statement would desync mem vs Delta. 0 disables.
    #[serde_inline_default(192)]
    pub timefusion_wal_hard_limit_gb: u64,
    // MemBuffer bucket window (seconds). The current bucket is excluded from
    // flushing, so this is the floor on how long a row accumulates in RAM.
    // Smaller = less peak memory but more Delta commits / small files.
    #[serde_inline_default(300)]
    pub timefusion_bucket_duration_secs: u64,
    // Memory pressure threshold (0–100) at which the flush task is woken
    // independently of the periodic timer, so MemBuffer drains before
    // reservation reaches the hard limit. 0 disables pressure-triggered flushes.
    #[serde_inline_default(75)]
    pub timefusion_pressure_flush_pct: u32,
    // Max seconds an insert applies backpressure (synchronously flushing
    // MemBuffer → Delta to free RAM) before failing at the memory hard limit.
    // Finite so a down Delta can't pile up blocked writers. 0 = fail fast.
    #[serde_inline_default(60)]
    pub timefusion_write_backpressure_secs: u64,
    /// Drain interval for deferred `UPDATE ... FROM` Delta merges; 0 keeps the
    /// synchronous per-statement path. The in-memory leg still applies
    /// synchronously, so reads stay read-your-writes.
    ///
    /// CONTRACT: statements must be idempotent under re-application (e.g. guard
    /// appends with `NOT (col @> val)`) — a row flushed between the mem leg and
    /// the drain sees the assignment applied twice, and a failed drain retries
    /// whole groups. Timestamp-range conjuncts are widened to the union across
    /// coalesced statements.
    #[serde_inline_default(3)]
    pub timefusion_dml_coalesce_secs: u64,
    /// Fold same-shape coalesced groups across projects into one MERGE per
    /// unified table per drain (`project_id` becomes a join key + IN-list
    /// partition filter).
    #[serde_inline_default(true)]
    pub timefusion_dml_coalesce_fold: bool,
    // Watchdog for a single bucket's Delta commit inside `flush_bucket`; without it
    // a hung S3 commit pins `flush_lock` forever. Must exceed a normal backfill
    // commit but stay well under retention. This is the CEILING, not the budget:
    // `BufferedWriteLayer::adaptive_flush_timeout` contracts it as the buffer fills.
    #[serde_inline_default(600)]
    pub timefusion_flush_bucket_timeout_secs: u64,
    /// WAL shards per (project, table) topic. Higher = more append parallelism
    /// at the cost of O(shards) recovery memory and more file handles.
    #[serde_inline_default(4)]
    pub timefusion_wal_shards_per_topic: usize,
    /// Max concurrent S3/R2 reads when reconciling per-table Delta watermarks
    /// at boot. Only used when the cursor snapshot is missing or stale.
    #[serde_inline_default(64)]
    pub timefusion_delta_scan_concurrency: usize,
    /// Per-table Delta commit history depth scanned at boot; only needs to catch
    /// writers that committed after the last cursor snapshot.
    #[serde_inline_default(8)]
    pub timefusion_delta_scan_depth: usize,
    /// Reject a compaction candidate whose merged output's UNION SPAN would
    /// exceed this many dedup bins; 0 disables. A wide output is re-read once per
    /// bin it touches, forever, and the other budgets (bytes, rows) do not
    /// correlate with span. Off by default because any bound that bites also
    /// rejects most sealed consolidation. Candidates with no event range are no
    /// objection.
    #[serde_inline_default(0)]
    pub timefusion_compaction_span_budget_bins: i64,
    /// Width of a dedup bin, in minutes. A dedup unit rewrites every file
    /// overlapping its bin whole, so a file straddling N bins is read N times;
    /// wider bins trade larger units for far less total read.
    ///
    /// Changing this RE-KEYS the dirty-bin queue. Persisted bins carry the width
    /// they were recorded at and are remapped on load, so a change costs an
    /// over-approximation, never a lost bin. **Rollback hazard:** a binary
    /// predating that field reads a post-flip sidecar at its own width — clear
    /// `dedup_dirty_bins.json` when rolling back across a flip.
    #[serde_inline_default(10)]
    pub timefusion_dedup_bin_minutes: i64,
    /// Decline a flush whose batch set provably already committed — the
    /// duplicates WAL replay re-inserts after an unclean exit. Only ever ACTIVE
    /// on a DIRTY boot, so `wal.landed_skips` reading 0 is dormancy, not failure.
    #[serde_inline_default(true)]
    pub timefusion_landed_skip_enabled: bool,
    /// After a merge-on-read UPDATE appends its new row versions, eagerly drop
    /// the superseded older versions still sitting in the MemBuffer (exactly
    /// what read-side dedup would drop), so the flush writes one copy instead
    /// of two. Fail-safe: any row this misses just flushes both copies — the
    /// status quo. Watch `dml.mor_versions_retracted_total`.
    #[serde_inline_default(false)]
    pub timefusion_mor_eager_retract: bool,
}

/// WAL durability mode. See `d_wal_fsync_mode` for the env-var encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalFsyncMode {
    Milliseconds(u64),
    SyncEach,
    None,
}

impl BufferConfig {
    getters! {
        flush_interval_secs: u64 = (timefusion_flush_interval_secs.max(1));
        retention_mins: u64 = (timefusion_buffer_retention_mins.max(1));
        eviction_interval_secs: u64 = (timefusion_eviction_interval_secs.max(1));
        max_memory_mb: usize = (timefusion_buffer_max_memory_mb.max(64));
        wal_shards_per_topic: usize = (timefusion_wal_shards_per_topic.max(1));
        wal_corruption_threshold: usize = (timefusion_wal_corruption_threshold);
        flush_parallelism: usize = (timefusion_flush_parallelism.max(1));
        flush_coalesce_commits: bool = (timefusion_flush_coalesce_commits);
        dml_coalesce_secs: u64 = (timefusion_dml_coalesce_secs);
        dml_coalesce_fold: bool = (timefusion_dml_coalesce_fold);
        delta_scan_concurrency: usize = (timefusion_delta_scan_concurrency.max(1));
        landed_skip_enabled: bool = (timefusion_landed_skip_enabled);
        delta_scan_depth: usize = (timefusion_delta_scan_depth.max(1));
        flush_immediately: bool = (timefusion_flush_immediately);
        wal_admit_decouple: bool = (timefusion_wal_admit_decouple);
        wal_fsync_ms: u64 = (timefusion_wal_fsync_ms.max(1));
        wal_ack_fsync: bool = (timefusion_wal_ack_fsync);
        wal_max_file_count: usize = (timefusion_wal_max_file_count);
        bucket_duration_secs: u64 = (timefusion_bucket_duration_secs.max(1));
        pressure_flush_pct: u32 = (timefusion_pressure_flush_pct.min(100));
    }

    /// mtime age past which a WAL file is PRESUMED dead weight. A heuristic, not
    /// a soundness bound: GC soundness comes from the un-flushed floor and the
    /// drained-gated boot sweep, never from age. Do not bypass the floor on the
    /// strength of this age.
    pub fn wal_gc_max_age(&self) -> Duration {
        Duration::from_secs(30 * 60)
    }
    pub fn wal_fsync_mode(&self) -> WalFsyncMode {
        match self.timefusion_wal_fsync_mode.to_ascii_lowercase().as_str() {
            "sync_each" | "synceach" | "each" => WalFsyncMode::SyncEach,
            "none" | "off" | "disabled" => WalFsyncMode::None,
            _ => WalFsyncMode::Milliseconds(self.wal_fsync_ms()),
        }
    }
    /// Byte ceiling for the unflushed-WAL force-flush backstop. Env-set bytes
    /// only; `None` = derive (`AppConfig::effective_wal_max_unflushed_bytes`).
    pub fn wal_max_unflushed_bytes(&self) -> Option<u64> {
        (self.timefusion_wal_max_unflushed_mb > 0).then(|| (self.timefusion_wal_max_unflushed_mb as u64).saturating_mul(MIB as u64))
    }
    pub fn wal_hard_limit_bytes(&self) -> Option<u64> {
        (self.timefusion_wal_hard_limit_gb > 0).then(|| self.timefusion_wal_hard_limit_gb.saturating_mul(GIB as u64))
    }
    /// The flush dwell in micros: -1 = one bucket_duration, 0 = gate off.
    pub fn flush_dwell_micros(&self) -> i64 {
        let secs = self.timefusion_flush_dwell_secs;
        (if secs < 0 { self.bucket_duration_secs() as i64 } else { secs }) * 1_000_000
    }
    pub fn write_backpressure_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_write_backpressure_secs)
    }
    /// Per-bucket Delta-commit watchdog inside `flush_bucket`. 0 disables it (unbounded wait).
    pub fn flush_bucket_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_flush_bucket_timeout_secs)
    }

    /// Total graceful-shutdown budget.
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
    // foyer creates the backing file sparse, but this is the logical ceiling at
    // which it starts evicting, so it MUST stay <= the cache volume's free space
    // or writes hit ENOSPC before eviction kicks in.
    #[serde_inline_default(500)]
    pub timefusion_foyer_disk_gb: usize,
    // ~35 days: foyer IS the local tier, so its horizon must cover the query mix
    // (30d dashboards), not the flush cadence. Disk stays the real bound.
    #[serde_inline_default(3_024_000)]
    pub timefusion_foyer_ttl_seconds: u64,
    /// Bounded lifetime of resolved Delta providers, in seconds. A provider is also
    /// invalidated immediately when its Delta snapshot version changes.
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
    /// (`datafusion.runtime.metadata_cache_limit`). Entries larger than the limit
    /// are silently dropped, so it must exceed a single file's metadata.
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
    /// Disk block size (MB) for the main data cache: foyer's minimal eviction
    /// unit AND the cap on the largest entry that can land on disk, so it must be
    /// >= the largest file to cache locally. `from_app_config` auto-raises the
    /// effective size to 2x the compaction target. Also bounds the transient heap
    /// each multipart-write warm holds (`block_size_mb * warm_concurrency`).
    #[serde_inline_default(256)]
    pub timefusion_foyer_block_size_mb: usize,
    /// Entries larger than this (MB) are inserted disk-only so warming a big
    /// compaction output doesn't evict the hot small-entry working set from
    /// L1 memory. 0 = always use L1.
    #[serde_inline_default(16)]
    pub timefusion_foyer_l1_max_entry_mb: usize,
    /// Don't admit writes whose `date=` partition is older than this many days;
    /// 0 = no age limit. Must cover the dashboard query horizon (30d) or its tail
    /// is permanently cold.
    #[serde_inline_default(35)]
    pub timefusion_cache_recent_days: usize,
    /// Optional extra cap (MB) on the in-flight buffer used to warm the cache
    /// directly from a multipart write. Always bounded by the disk block size;
    /// 0 = bound only by the block size.
    #[serde_inline_default(0)]
    pub timefusion_warm_inline_max_mb: usize,
    /// Per-upload cap (MB) on the heap buffer a multipart write tees into to warm
    /// the cache. Uploads past this abandon capture and stream through untouched
    /// — never blocked, never failed. Sized for flush outputs; compaction outputs
    /// are warmed post-commit instead. 0 = bounded only by the block size,
    /// clamped to the process-wide budget.
    #[serde_inline_default(32)]
    pub timefusion_write_capture_max_mb: usize,
    /// Process-wide budget (MB) for in-flight write-capture buffers. Each
    /// capturing upload reserves its full per-upload cap up front; over budget,
    /// capture is skipped (the upload is unaffected). Also CLAMPS the per-upload
    /// cap. 0 = unbudgeted.
    #[serde_inline_default(256)]
    pub timefusion_write_capture_budget_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
    /// Scan-resistant admission: a scan reaching further back than this many
    /// hours runs with cache population BYPASSED, so a wide sweep can't flush the
    /// hot tail out. Reads still HIT what's already cached. 0 disables.
    #[serde_inline_default(24)]
    pub timefusion_cache_bypass_scan_hours: u64,
}

impl CacheConfig {
    getters! {
        is_disabled: bool = (timefusion_foyer_disabled);
        provider_cache_capacity: usize = (timefusion_provider_cache_capacity.max(1));
        stats_enabled: bool = (timefusion_foyer_stats.eq_ignore_ascii_case("true"));
        memory_size_bytes: usize = (timefusion_foyer_memory_mb * MIB);
        file_size_bytes: usize = (timefusion_foyer_file_size_mb * MIB);
        metadata_memory_size_bytes: usize = (timefusion_foyer_metadata_memory_mb * MIB);
        warm_inline_max_bytes: usize = (timefusion_warm_inline_max_mb * MIB);
        write_capture_max_bytes: usize = (timefusion_write_capture_max_mb * MIB);
        write_capture_budget_bytes: usize = (timefusion_write_capture_budget_mb * MIB);
        block_size_bytes: usize = (timefusion_foyer_block_size_mb * MIB);
        l1_max_entry_bytes: usize = (timefusion_foyer_l1_max_entry_mb * MIB);
    }

    // `self` in a macro tail is E0424 (hygiene), so these stay hand-written.
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map_or(self.timefusion_foyer_disk_gb * GIB, |mb| mb * MIB)
    }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb.map_or(self.timefusion_foyer_metadata_disk_gb * GIB, |mb| mb * MIB)
    }
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_foyer_ttl_seconds)
    }
    pub fn provider_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_provider_cache_ttl_seconds.max(1))
    }
    /// Scan lookback depth past which cache population is bypassed, in micros.
    /// `None` = never bypass.
    pub fn cache_bypass_scan_micros(&self) -> Option<i64> {
        (self.timefusion_cache_bypass_scan_hours > 0).then(|| self.timefusion_cache_bypass_scan_hours as i64 * 3_600 * 1_000_000)
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde_inline_default(20_000)]
    pub timefusion_page_row_count_limit: usize,
    /// ZSTD level for every WORKING write: flush, hot-tail packing, dedup staging.
    /// There are exactly two levels in the system (this and `..._zstd_level_warm`)
    /// — staged parquet is final, so a cheaper level needs a re-tiering pass that
    /// provably runs.
    #[serde_inline_default(3)]
    #[serde(alias = "timefusion_zstd_level_hot")]
    #[serde(alias = "timefusion_zstd_level_intermediate")]
    pub timefusion_zstd_compression_level: i32,
    /// ZSTD level for SEALED writes — compaction and consolidation, where the
    /// data is not expected to be rewritten again.
    #[serde_inline_default(9)]
    pub timefusion_zstd_level_warm: i32,
    #[serde_inline_default(128 * MIB)]
    pub timefusion_max_row_group_size: usize,
    #[serde_inline_default(10)]
    pub timefusion_checkpoint_interval: u64,
    // Compacted-file target for the warm tier.
    #[serde_inline_default(256 * MIB as i64)]
    pub timefusion_optimize_target_size: i64,
    // Cold tier target for sealed partitions. Not 1GB: the decompressed working
    // set is ~17x the compressed target, which makes consolidation memory-hostile.
    #[serde_inline_default(512 * MIB as i64)]
    pub timefusion_cold_optimize_target_size: i64,
    // Warm/cold boundary. The warm optimize is clamped to dates newer than this
    // so it never fragments cold files back to the warm target.
    #[serde_inline_default(1)]
    pub timefusion_cold_optimize_after_days: u64,
    #[serde_inline_default(50)]
    pub timefusion_stats_cache_size: usize,
    #[serde(default)]
    pub timefusion_bloom_filter_disabled: bool,
}

impl ParquetConfig {
    /// Warm/cold boundary in days, floored at 1: today's partition is still
    /// taking writes and must never be consolidated to the cold target.
    pub fn cold_optimize_after_days(&self) -> u64 {
        self.timefusion_cold_optimize_after_days.max(1)
    }
}

#[serde_inline_default::serde_inline_default]
#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    // Must outlive any in-flight query (which holds a Delta snapshot referencing
    // files vacuum would delete). Also drives `delta.deletedFileRetentionDuration`.
    // Removed-but-unvacuumed files are the only recovery source after a bad
    // rewrite — keep a multi-day floor.
    #[serde_inline_default(72)]
    pub timefusion_vacuum_retention_hours: u64,
    // Delta _delta_log retention; keeps each commit's version-discovery LIST cheap.
    #[serde_inline_default(6)]
    pub timefusion_log_retention_hours: u64,
    #[serde_inline_default(48)]
    pub timefusion_optimize_window_hours: u64,
    /// Target DECODED bytes in one maintenance scan batch. A batch is the sort's
    /// indivisible admission unit and the spill granularity; peak merge memory is
    /// `fan_in x batch_bytes`, so keep it small enough that a deep merge fits a
    /// per-worker pool share.
    #[serde_inline_default(8 * MIB as u64)]
    pub timefusion_maintenance_batch_target_bytes: u64,
    /// Decoded bytes per event-time slice of a REPAIR rewrite. 0 (default)
    /// disables slicing; any non-zero value costs one full re-read and re-decode
    /// of the input file per slice.
    #[serde_inline_default(0)]
    pub timefusion_repair_slice_decoded_target_bytes: u64,
    /// Write optimize/compact/recompress output sorted by the schema's
    /// `sorting_columns` with an honest DESC footer, so timestamp-ordering/LIMIT
    /// pushdown keeps firing on rewritten partitions. Off, one compaction cycle
    /// strips the footer and the all-or-nothing ordering rule disables pushdown
    /// for the whole partition.
    #[serde_inline_default(true)]
    pub timefusion_optimize_sort_by: bool,
    /// Budget for the IN-PROCESS Arrow sort on the flush path, in IN-MEMORY bytes
    /// (not file bytes — zstd on otel data decodes ~12-17x). This sort allocates
    /// OUTSIDE the DataFusion pool, so raising the ceiling authorises untracked
    /// multi-GB allocations on the ingest path. Past it the flush escalates to a
    /// pooled, disk-spilling plan (`sort_flush_group_spilling`).
    #[serde_inline_default(2 * GIB)]
    pub timefusion_sort_skip_bytes: usize,
    /// Pool for the flush-path escalation sort, in MB — its own slice so an
    /// ingest-path sort never queues behind maintenance. Deliberately smaller
    /// than the escalation threshold: exceeding it spills to disk.
    #[serde_inline_default(1024)]
    pub timefusion_flush_sort_pool_mb: u64,
    #[serde_inline_default(5)]
    pub timefusion_compact_min_files: usize,

    /// Refuse a packing bin that rewrites more than this many ROWS per file it
    /// eliminates. 0 = off; the refusal is counted either way. In rows, not
    /// bytes, because the packer compares COMPRESSED `add.size`.
    ///
    /// Load-bearing escape: the guard never refuses a bin of `min_files` or more,
    /// or a biting floor refuses every bin and packing stops entirely.
    #[serde_inline_default(1_000_000)]
    pub timefusion_pack_max_rows_per_file_eliminated: u64,
    /// Five-minute hot-partition compaction is required to prevent a
    /// small-file backlog. Set false only as an incident kill switch.
    #[serde_inline_default(true)]
    pub timefusion_light_optimize_enabled: bool,
    // Hot/today compaction target. A tiny target leaves the hot partition as
    // dozens of files and makes recent queries file-open-latency bound.
    #[serde_inline_default(256 * MIB as i64)]
    pub timefusion_light_optimize_target_size: i64,

    /// Shrink a maintenance unit's target as its lane's memory pool fills, so a
    /// few large units cannot monopolise it. Off by default; the reduction is
    /// computed and counted either way (`maintenance.pressure_scale_*`) and only
    /// applied when true. Taper: full target at <=50% occupancy, half at 100%.
    #[serde_inline_default(false)]
    pub timefusion_maintenance_pressure_scaling: bool,
    /// Per-runtime-env spill ceiling in GiB for the maintenance-family
    /// `RuntimeEnv`s (`build_spill_runtime_env`). Must exceed the largest single
    /// rewrite's spill or the unit can never complete. PER ENV, not global —
    /// sound only because no two heavy spillers run concurrently by construction.
    #[serde_inline_default(220)]
    pub timefusion_maintenance_spill_max_gb: u64,
    /// Cap for QUERY spill under `<data_dir>/query_spill`. Sized to coexist with
    /// `timefusion_maintenance_spill_max_gb` on the same volume: a query needing
    /// more should fail rather than fill the volume the WAL lives on.
    #[serde_inline_default(64)]
    pub timefusion_query_spill_max_gb: u64,
    /// Kill switch for the Dedup contiguity rank term (prefer the slice that
    /// EXTENDS a completed run — see `TaskJournal::rank`).
    #[serde_inline_default(true)]
    pub timefusion_dedup_contiguity_rank: bool,
    /// Refuse to admit a file into a packing bin more than this many times the
    /// size of the bin's smallest member (the similar-size rule). 0 = off.
    ///
    /// OFF, because it WEDGES when composed with the value floor: refusal returns
    /// empty instead of resuming past the refused bin. Make selection resume past
    /// a floor refusal before enabling this.
    #[serde_inline_default(0)]
    pub timefusion_pack_max_size_ratio: i64,
    /// Byte ceiling for ONE output file from a rewrite. `RecordBatchWriter` has
    /// no target-size support (`flush()` emits one file per partition), so
    /// rewrite paths cut the file themselves once the buffer passes this. An
    /// oversized file is effectively unrepairable — re-sorting it exceeds any
    /// sort budget. Cutting is correctness-free: each cut is a contiguous slice
    /// of an already-sorted stream, so every piece keeps a sorted footer.
    #[serde_inline_default(512 * MIB)]
    pub timefusion_writer_max_file_bytes: usize,
    /// Largest file a hot tick will rewrite purely to repair a missing
    /// `sorting_columns` footer. Separate from `timefusion_writer_max_file_bytes`
    /// on purpose: that caps what we WRITE, this caps what we pull in to REPAIR.
    /// Anything above is left to `timefusion optimize --recompress`.
    #[serde_inline_default(1024 * MIB)]
    pub timefusion_repair_max_file_bytes: usize,
    /// Sealed dates (yesterday backwards) the hot tail also scans for FOOTER
    /// REPAIR. Repair only: sorted files on those dates are never re-binned.
    /// Bounded on both sides — one unsorted file voids the ordering claim for
    /// every query touching its date, but the lookback IS the suspect-set size,
    /// so too wide a value spends the pass clearing already-sorted files.
    /// 0 restores today-only repair.
    #[serde_inline_default(31)]
    pub timefusion_light_optimize_repair_days: u64,
    #[serde_inline_default("0 */5 * * * *".to_string())]
    pub timefusion_light_optimize_schedule: String,
    /// Sealed-date FOOTER REPAIR, on its own cron: a repair unit is one whole-file
    /// rewrite of up to ~1 GiB, too big for a hot-tail tick's budget.
    #[serde_inline_default("0 30 * * * *".to_string())]
    pub timefusion_footer_repair_schedule: String,
    /// How long ONE repair pass may run, in seconds — deliberately INDEPENDENT of
    /// the schedule above (everywhere else budget = 80% of the cron period).
    /// Repair needs frequent attempts AND a long run; `spawn_cron_job` skips
    /// overlapping ticks rather than queueing them, so that combination is safe.
    #[serde_inline_default(8640)]
    pub timefusion_footer_repair_budget_secs: u64,

    /// How many footer-less files ONE repair pass rewrites. Keep modest: a repair
    /// unit is a whole-file rewrite and the pool is shared.
    #[serde_inline_default(4)]
    pub timefusion_footer_repair_files_per_pass: usize,
    /// Dirty-bin dedup of sealed (< today) partitions, on its OWN cron so an
    /// old-date dedup backlog can't starve today's compaction.
    #[serde_inline_default("0 */5 * * * *".to_string())]
    pub timefusion_dedup_schedule: String,
    /// Kill switch for physical dirty-bin dedup; read-side dedup remains the
    /// correctness path.
    #[serde_inline_default(true)]
    pub timefusion_dirty_bin_dedup_enabled: bool,
    /// VACUUM schedule. Must run often enough to delete files before their
    /// tombstones age out of the checkpoint (`VacuumMode::Full` backstops the rest).
    #[serde_inline_default("0 15 */6 * * *".to_string())]
    pub timefusion_vacuum_schedule: String,
    /// Out-of-band checkpoint + expired-log cleanup, driven here rather than from
    /// delta-rs's commit-path hook (a hook failure surfaces as a commit error AFTER
    /// the commit landed). Must be faster than the commit cadence.
    #[serde_inline_default("0 */2 * * * *".to_string())]
    pub timefusion_checkpoint_schedule: String,
    /// Dangling-Add reconcile: HEAD every live file and commit Remove for any that
    /// are missing. A nonzero removal count means committed data was destroyed
    /// elsewhere.
    #[serde_inline_default("0 0 * * * *".to_string())]
    pub timefusion_reconcile_schedule: String,
    /// Tantivy index reconcile: backfill uncovered live parquet + GC manifest
    /// entries for rewritten-away files. Keep the period SHORT relative to how
    /// often the process restarts — an hourly cron never fires at all on a box
    /// that restarts more often. Safe to run often: each pass is bounded by
    /// `timefusion_tantivy_backfill_max_files_per_pass`.
    #[serde_inline_default("0 */15 * * * *".to_string())]
    pub timefusion_tantivy_reconcile_schedule: String,
    /// File-level needle pruning: consult per-file bloom sidecars at
    /// file-selection time so point lookups scan only files that can contain the
    /// needle. The sidecar builder cron is keyed off the same flag.
    #[serde_inline_default(true)]
    pub timefusion_file_bloom_pruning: bool,
    /// Bloom sidecar reconcile: lift parquet blooms of uncovered live files into
    /// per-(project,date) sidecars, GC retired entries. Newest dates first.
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
    /// Warm the Foyer cache for files written by a flush/optimize commit.
    /// Footers are always warmed when enabled.
    #[serde_inline_default(true)]
    pub timefusion_warm_after_compaction: bool,
    /// In addition to footers, warm full file contents. OFF by default:
    /// continuous full-body warms drive RSS toward the OOM ceiling with no query
    /// load, and footers carry most of the planning-latency win.
    #[serde(default)]
    pub timefusion_warm_full_files: bool,
    /// Only warm files whose `date=` partition is within this many days of today.
    /// Must reach the dashboard query horizon or its tail is permanently cold.
    /// 0 = no recency limit.
    #[serde_inline_default(35)]
    pub timefusion_warm_recency_days: u64,
    /// Paced full-body warm at BOOT for recency-window files, in fetched
    /// files/sec. Pacing is required: an unpaced boot warm saturates object-store
    /// bandwidth. 0 = footer-only boot warm.
    #[serde_inline_default(16)]
    pub timefusion_warm_body_boot_files_per_sec: u32,
    /// Warm parquet footers for EVERY live file, not just recency-window ones.
    /// Disable to fall back to recency-bounded footer warming when the boot-time
    /// GET burst matters.
    #[serde_inline_default(true)]
    pub timefusion_warm_all_footers: bool,
    /// Max concurrent warm fetches per commit; bounds the post-compaction GET burst.
    #[serde_inline_default(16)]
    pub timefusion_warm_concurrency: usize,
    /// How long the maintenance coordinator waits for the boot table REPLAY
    /// before starting anyway. The gate is the REPLAY phase only — the paced body
    /// warm can safely run beside maintenance. 0 disables the wait.
    #[serde_inline_default(300)]
    pub timefusion_coordinator_preload_wait_secs: u64,
    /// After a compaction commit, evict the cached bytes of the files it
    /// tombstoned instead of waiting for VACUUM / TTL / LRU.
    #[serde_inline_default(true)]
    pub timefusion_evict_after_compaction: bool,
    /// Advance the post-commit snapshot by appending only the files the commit
    /// added, instead of re-materializing the whole active file set.
    #[serde_inline_default(true)]
    pub timefusion_incremental_snapshot: bool,
    /// Every Nth commit per table, re-materialize from S3 truth to bound drift
    /// from an incremental-replay bug. 0 disables reconciliation.
    #[serde_inline_default(500)]
    pub timefusion_snapshot_reconcile_commits: u64,
    /// Commit staged-but-uncommitted footer-repair parquet found at boot instead
    /// of deleting it and redoing the rewrite. Only data-preserving
    /// (compaction/repair) bins are eligible — dedup bins drop rows and stay
    /// cleanup-only.
    #[serde_inline_default(true)]
    pub timefusion_repair_resume_enabled: bool,

    /// Complete a rollup unit without rebuilding when its input file set —
    /// deletion vectors included — is unchanged since the live slice coverage
    /// was published, and that coverage's generation is still current.
    /// Kill switch: the failure it could cause is silent — a cell that should
    /// have rebuilt and did not.
    #[serde_inline_default(true)]
    pub timefusion_rollup_noop_skip_enabled: bool,

    /// Record a file as verified-sorted when the WRITE that produced it stamped a
    /// `sorting_columns` footer. A marked file is NEVER offered to footer repair.
    ///
    /// Turning this off stops NEW marks; it does not un-mark anything. The set is
    /// persisted, so undoing a wrong exoneration needs
    /// `rm <data_dir>/repair_verified_sorted.txt` and a restart.
    #[serde_inline_default(true)]
    pub timefusion_repair_mark_sorted_at_write: bool,
    /// Days back (plus today) the dedup sweep scans.
    //
    // This sweep is the ONLY caller of `record_certification`, and rollup routing
    // needs a contiguous certified prefix across the query window — so the horizon
    // must cover the widest query or no wide query can route to a rollup. Do NOT
    // raise it without matching coordinator job concurrency. Keep in step with
    // `timefusion_light_optimize_repair_days`.
    #[serde_inline_default(35)]
    pub timefusion_dedup_lookback_days: u64,
    /// Run the legacy partition-wide dedup probe as an audit/fallback. Dirty
    /// sealed bins are the normal maintenance path.
    #[serde(default)]
    pub timefusion_dedup_sweep_fallback: bool,
    /// Optional comma-separated read canary projects.
    #[serde(default)]
    pub timefusion_rollup_read_projects: Option<String>,
    /// Sealed days back the backfill will build rollups for. 0 disables it.
    /// Keep in step with `timefusion_dedup_lookback_days`: certification and
    /// rollup coverage need the same horizon, or a day is certified but never
    /// rolled up (or vice versa).
    #[serde_inline_default(31)]
    pub timefusion_rollup_backfill_days: u16,
    /// `(project_id, date)` cells a one-shot repair forces a full re-derive of,
    /// as `project:YYYY-MM-DD`. Empty means "use `DAMAGED_CELLS`". Non-empty
    /// REPLACES that const rather than extending it. Malformed entries are dropped
    /// with a warning.
    #[serde_inline_default(Vec::new())]
    pub timefusion_damage_repair_cells: Vec<String>,
    /// Skip the read-side DedupExec (and its key projection) for Delta-only
    /// queries whose every in-window (project, date) partition was verified
    /// duplicate-free by a sweep pass AND whose file set is unchanged since
    /// (fingerprint match). Also restores per-scan LIMIT pushdown. An unswept or
    /// newly-written partition keeps full dedup. Kill switch if a count is ever
    /// doubted.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_swept: bool,
    /// Per-DATE dedup skip: a window that is only partly certified skips
    /// `DedupExec` over its certified date partitions instead of losing the
    /// skip entirely.
    ///
    /// SOUNDNESS: `date` is derived from `timestamp` and DML re-appends preserve
    /// the original row's timestamp, so every version and tombstone of a row
    /// shares one date partition. No dedup key spans dates, so dedup over the
    /// union equals dedup applied per date.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_per_date: bool,
    /// Per-FILE dedup skip: within an uncertified date, the FILES a sweep proved
    /// clean still skip `DedupExec` when no uncertified file could hold another
    /// version of their rows. Partition-level certification is voided by ANY new
    /// file; per-file, a new file voids only the files it overlaps.
    ///
    /// SOUNDNESS: the dedup key is `(timestamp, id)` and merge-on-read re-appends
    /// preserve the original timestamp, so every version of a row must land in a
    /// file whose span contains it. A certified file may therefore skip iff no
    /// UNCERTIFIED file's span overlaps its own — see
    /// `read::skippable_certified_files`, which fails closed on a
    /// missing-statistics span, an empty certified set and inclusive-bound
    /// touching.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_skip_per_file: bool,
    /// Dedup-as-you-compact: the on-demand compaction path (`compact_date`)
    /// upgrades its SortBy rewrite to SortByDedup, collapsing superseded
    /// merge-on-read versions. No-op while `timefusion_optimize_sort_by` is off
    /// (dedup needs the sorted stream).
    #[serde(default)]
    pub timefusion_compact_dedup_merge: bool,
    /// Persist sweep certifications to the data dir and reload at boot, so the
    /// read-side dedup skip doesn't restart cold. It cannot widen certification:
    /// a reloaded entry faces the same fingerprint-equality check, so a stale
    /// store costs a skip rather than granting a wrong one.
    #[serde_inline_default(true)]
    pub timefusion_dedup_certification_persist: bool,
    /// Keep span-disjoint clean coverage across a partition fingerprint move
    /// instead of discarding the day. Kill switch for the read-side dedup skip:
    /// a defect here is wrong ROWS, not slow ones, so it reverts in one env var.
    #[serde_inline_default(true)]
    pub timefusion_dedup_coverage_retention: bool,
    /// Re-prove a stale-looking rollup slice against its bounded witness
    /// (`TAG_SOURCE_ROWS_BELOW`) before refusing it. The rescue that stops a
    /// tail append from staling the morning's slices; a defect serves stale
    /// AGGREGATES, so it reverts in one env var.
    #[serde_inline_default(true)]
    pub timefusion_rollup_bounded_witness: bool,

    /// Let a whole-day certification survive a fingerprint move for windows the
    /// newly-added files cannot have touched. Read-side dedup skip: a defect is
    /// wrong ROWS, so it reverts in one env var.
    #[serde_inline_default(true)]
    pub timefusion_dedup_window_scoped_certification: bool,
    /// Allow `DedupExec` to run in streaming `bounded[timestamp]` mode, which
    /// trusts the scan's declared `output_ordering` (the parquet footer's
    /// `sorting_columns`). A lying footer makes one "run" span many timestamps
    /// and can drop rows. Turning it off is not cheap: bounded mode carries LIMIT
    /// early termination, so "top N" queries then scan the whole window.
    #[serde_inline_default(true)]
    pub timefusion_read_dedup_bounded: bool,
    /// Branches a wide AGGREGATE window is split into, so each range's
    /// `DedupExec` runs on its own thread. `0`/`1` disables the split.
    ///
    /// `DedupExec` is `SinglePartition`, so one query otherwise gets one core.
    /// Splitting is exact because `timestamp` leads the dedup key, so no row's
    /// versions can straddle a boundary. Applies only under an aggregate; an
    /// `ORDER BY … LIMIT` keeps its streaming TopK. Each branch re-opens the files
    /// its range touches, so raising this trades file opens for parallelism.
    ///
    /// DEFAULT OFF: measured slower than no split. Do not re-enable without
    /// measuring against a real deployment — the local `ctx.sql` path re-runs
    /// filter pushdown that the pgwire path does not.
    #[serde_inline_default(1)]
    pub timefusion_query_range_split_branches: usize,
    /// Answer gate-eligible `SELECT COUNT(*) ... WHERE project_id AND
    /// timestamp range` from Delta add-action stats (zero parquet IO). Only
    /// fires when the window is fully flushed, dedup-provably-clean, and
    /// every overlapping file lies entirely inside the window — otherwise
    /// the normal scan runs. See the `count_pushdown` section of `read/mod.rs`.
    ///
    /// DEFAULT false: it returns SILENTLY WRONG COUNTS (undercounts against the
    /// scan on a `tombstones_possible()` table). Re-enable only once the
    /// logical-count index is fixed and pinned against a scan by a test.
    #[serde_inline_default(false)]
    pub timefusion_count_pushdown: bool,
    /// Per-shard COMPRESSED-bytes target for a dedup chunk rewrite (`sum(add.size)`).
    /// The rewrite is split into `ceil(compressed_bytes / this)` hash-bucketed passes
    /// so each pass reads ~this much. 0 disables this ceiling's contribution to the
    /// shard count. Over-budget chunks are skipped rather than rewritten;
    /// read-side dedup keeps queries correct meanwhile. Keep in step with
    /// `timefusion_dedup_max_decoded_bytes` — shard count takes the MAX of both,
    /// so leaving this lower silently caps sharding below the decoded budget.
    #[serde_inline_default(GIB as u64 / 2)]
    pub timefusion_dedup_max_rewrite_bytes: u64,
    /// Per-shard target on the ESTIMATED DECODED (in-memory Arrow) footprint of
    /// a dedup chunk rewrite; these buffers are not accounted by DataFusion's
    /// memory pool. The rewrite shards by a hash of the dedup keys into
    /// `ceil(est_decoded / this)` passes; a single key group that alone exceeds
    /// this is unshardable and skipped. 0 → one shard for this ceiling.
    ///
    /// Sized to FUND SHARD CONCURRENCY, not to save memory:
    /// `dedup_shard_concurrency` runs `DEDUP_BIN_ARROW_BUDGET / this` shards at
    /// once, so a smaller shard buys parallelism at an unchanged peak.
    #[serde_inline_default(GIB as u64 / 2)]
    pub timefusion_dedup_max_decoded_bytes: u64,
    /// Compressed→decoded inflation factor used to estimate a dedup chunk's
    /// in-memory footprint when per-file `num_records` stats are unavailable.
    #[serde_inline_default(12)]
    pub timefusion_dedup_decode_inflation: u64,
    /// Estimated decoded Arrow bytes per row, used with per-file `num_records`
    /// to size a dedup chunk's in-memory footprint.
    #[serde_inline_default(4096)]
    pub timefusion_dedup_bytes_per_row: u64,
    /// Max concurrent user DML MERGE-UPDATEs. Each merge rewrites whole hot
    /// partitions with pool-invisible memory, so stacking risks OOM. Results are
    /// identical at any value — excess statements queue.
    #[serde_inline_default(1)]
    pub timefusion_dml_merge_concurrency: usize,
    /// Perform UPDATE/DELETE as merge-on-read deletion-vector operations instead
    /// of copy-on-write full-file rewrites.
    ///
    /// Requires the `deletionVectors` writer feature, enabled lazily on first DV
    /// write (protocol upgrade to reader/writer v3/v7). That upgrade is
    /// IRREVERSIBLE and every reader of these Delta tables must then understand
    /// DVs — set false to keep copy-on-write rewrites.
    #[serde_inline_default(true)]
    pub timefusion_use_deletion_vectors: bool,
    /// Commit DV merges append-tolerantly: a concurrent AddFile-only flush commit
    /// rebases instead of aborting with ConcurrentAppend. Removed-file conflicts
    /// (optimize/vacuum) still abort and retry.
    ///
    /// Sound only because the mem leg runs before every Delta leg, which relies on
    /// the single-writer WAL flock (`WalDirLock`). If the table ever gains a second
    /// concurrent writer whose flushes bypass this process's mem leg, set false
    /// (strict OCC) or its rows can miss enrichment merges.
    #[serde_inline_default(true)]
    pub timefusion_dml_merge_append_rebase: bool,
    /// Push a `target.key IN (source key values)` filter into the DV merge's
    /// per-file scan so parquet bloom filters prune files/row-groups holding none
    /// of the source keys. Sound: a bloom filter never false-negatives.
    #[serde_inline_default(true)]
    pub timefusion_dml_merge_key_prune: bool,
}

impl MaintenanceConfig {
    /// Reads honour the canary allow-list (empty/unset = every project);
    /// rollup BUILDS are unconditional and have no allow-list.
    pub fn rollup_read_enabled_for(&self, project_id: &str) -> bool {
        self.timefusion_rollup_read_projects.as_deref().is_none_or(|ps| ps.trim().is_empty() || ps.split(',').map(str::trim).any(|p| p == project_id))
    }

    /// Flush escalation-sort pool in bytes. Floored so a misconfigured 0 cannot
    /// build a zero-sized pool that fails every sort.
    pub fn flush_sort_pool_bytes(&self) -> usize {
        (self.timefusion_flush_sort_pool_mb.max(64) as usize).saturating_mul(1 << 20)
    }
}

/// Which DataFusion `MemoryPool` to back the runtime with.
///
/// `FairSpill` (default) bounds each spillable consumer, which the query pool
/// needs because the merge halves (`ExternalSorterMerge`, `SortPreservingMerge`,
/// `DedupExec[keep-greatest]`) cannot spill: under `Greedy` a sorter grows until
/// the pool is gone and the merge behind it fails.
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
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default)]
    pub timefusion_memory_pool: MemoryPoolKind,
    #[serde_inline_default(true)]
    pub timefusion_tracing_record_metrics: bool,
    /// Rows per decode batch for pgwire QUERY sessions on the wide OTel schema.
    ///
    /// Default 2048 = `WIDE_ROW_DECODE_BATCH_SIZE`, unchanged behaviour. The
    /// parquet decode buffer is NOT pool-accounted, so this bounds ~64-73 KB
    /// per row of untracked memory per active scan stream — that is why the
    /// default is a quarter of DataFusion's 8192. The cost of the small value
    /// is per-batch fixed work: prod profiling (2026-09-15) found no hot
    /// kernel, just per-batch overhead spread across every operator, which
    /// this knob multiplies by 4x. Canary upward (4096, then 8192) while
    /// watching RSS and query-pool `Resources exhausted`; maintenance
    /// sessions keep their own 2048 regardless.
    #[serde_inline_default(2048)]
    pub timefusion_query_batch_size: usize,
    /// DataFusion `target_partitions` for query + maintenance sessions. 0 = auto:
    /// `config::apply()` derives it from the container's CPU quota (num_cpus
    /// ignores the CFS quota and oversubscribes throttled containers).
    #[serde_inline_default(0)]
    pub timefusion_query_partitions: usize,
    /// Admit at most `max_concurrent_heavy_sorts` spilling-sort queries against the
    /// query pool at once; queue the rest with a bounded wait rather than letting
    /// them race into `Resources exhausted`. Off by default — enabling it changes
    /// the concurrency behaviour of every heavy pgwire query, so it ships dark and
    /// is turned on only after the concurrent-connection e2e proves it.
    #[serde(default)]
    pub timefusion_heavy_query_admission: bool,
    /// Admission guard for wide-window read scans: a query reaching further back
    /// than `timefusion_wide_scan_lookback_hours` is limited to this many
    /// concurrent Parquet batch-decodes across all queries, bounding decode heap
    /// that the DataFusion memory pool does not track.
    #[serde_inline_default(16)]
    pub timefusion_max_concurrent_scan_readers: usize,
    #[serde_inline_default(2)]
    pub timefusion_wide_scan_lookback_hours: u64,
    /// A scan is gated only when it is deep AND selected real work after pruning:
    /// more than `..._max_files` files or `..._max_mb` of them. Both are needed —
    /// file COUNT alone breaks once partitions fragment into small files.
    #[serde_inline_default(256)]
    pub timefusion_wide_scan_max_files: usize,
    // Compressed parquet bytes understate transient Arrow decode heap by ~an order
    // of magnitude on OTel data.
    #[serde_inline_default(64)]
    pub timefusion_wide_scan_max_mb: u64,
    /// Largest isolated non-conforming Delta leg that `repair_isolated_scan_ordering`
    /// will sort at read time so the conforming majority keeps its `[timestamp DESC]`
    /// claim. 0 disables the repair.
    ///
    /// COMPRESSED selected bytes, which understate the sort's Arrow heap by ~12x
    /// on OTel data. The repair runs on EVERY Delta-reading query, so this
    /// ceiling is paid per query, concurrently.
    #[serde_inline_default(1024)]
    pub timefusion_read_sort_unordered_leg_max_mb: u64,
    /// Cross-connection plan-cache capacity (unique canonical/shape templates).
    /// Each entry is one LogicalPlan (~KBs).
    #[serde_inline_default(2048)]
    pub timefusion_plan_cache_capacity: usize,
    /// Route `now()`/`current_timestamp` SELECTs through the shape cache instead
    /// of bypassing it. The cached artifact is a placeholder plan template and the
    /// instant is re-bound per query, so windows never freeze.
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
    getters! {
        is_json_logging: bool = (log_format.as_deref() == Some("json"));
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>()).expect("Default config should always succeed with serde defaults")
    }
}

impl AppConfig {
    /// Effective WAL flush thresholds: env override wins, else the derived tree.
    pub fn effective_wal_max_files(&self) -> usize {
        NonZeroUsize::new(self.buffer.wal_max_file_count()).map_or_else(|| self.derived.wal_flush_file_threshold(), NonZeroUsize::get)
    }

    pub fn effective_wal_max_unflushed_bytes(&self) -> u64 {
        self.buffer.wal_max_unflushed_bytes().unwrap_or_else(|| self.derived.wal_flush_byte_threshold())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins the DESERIALIZED tantivy defaults. Trap: `TantivyConfig::default()`
    /// is the derived `Default`, which bypasses every `#[serde_inline_default]`
    /// and yields a configuration that can never be deployed.
    #[test]
    fn tantivy_defaults_are_the_deserialized_ones_not_the_derived_ones() {
        let cfg: TantivyConfig = serde_json::from_str("{}").expect("every field has a default");
        assert!(cfg.seed_cache_on_publish(), "a published index must be kept locally by default");
        assert_eq!(cfg.timefusion_tantivy_prefetch_days, 3, "the hot window must be warmed by default");
        assert_eq!(cfg.search_concurrency(), 32);
        assert_eq!(cfg.reader_cache_entries().get(), 2048);
        assert_eq!(cfg.manifest_ttl(), Duration::from_secs(300));
        // A COUNT ceiling against a pathological queue of tiny files, NOT the
        // pass bound — `timefusion_tantivy_backfill_max_bytes_per_pass` is.
        assert_eq!(cfg.timefusion_tantivy_backfill_max_files_per_pass, 320);

        // The backfill order policy, as a pair: recent data first, today
        // excluded. Changing either default alone starves the lane.
        assert_eq!(cfg.timefusion_tantivy_backfill_tail_share_pct, 0);
        assert!(cfg.timefusion_tantivy_backfill_skip_today);

        let derived = TantivyConfig::default();
        assert!(!derived.seed_cache_on_publish(), "derived Default really does diverge — that is why this test exists");
        // Floors keep a derived-Default config merely wrong, not deadlocked:
        // zero concurrency would stall the per-index fan-out.
        assert_eq!(derived.search_concurrency(), 1);
        assert_eq!(derived.reader_cache_entries().get(), 1);
    }

    /// A once-hourly drain schedule is never reached by a box that restarts more
    /// often than hourly, so the schedule must fire several times an hour.
    #[test]
    fn the_tantivy_drain_gets_more_than_one_chance_an_hour() {
        let cfg: MaintenanceConfig = serde_json::from_str("{}").expect("every field has a default");
        assert_eq!(cfg.timefusion_tantivy_reconcile_schedule, "0 */15 * * * *");
    }

    /// Rollups are ON with no configuration at all, and the read canary is the
    /// only thing that can narrow them.
    #[test]
    fn rollups_need_no_configuration_and_only_the_read_canary_narrows_them() {
        let mut config: MaintenanceConfig = serde_json::from_str("{}").expect("every field has a default");
        assert!(config.rollup_read_enabled_for("project-a"), "an unconfigured deployment must still route");
        assert!(config.rollup_read_enabled_for("a-project-created-tomorrow"));
        assert_eq!(config.timefusion_rollup_backfill_days, 31, "the shipped default is the value prod exercises");

        // The canary still narrows the READ side when it is set, and only then.
        config.timefusion_rollup_read_projects = Some("project-b".into());
        assert!(!config.rollup_read_enabled_for("project-a"));
        assert!(config.rollup_read_enabled_for("project-b"));
    }

    /// The CLI budget profile: an 8 GiB pod derives multi-GiB sort memory
    /// instead of the 1 GiB floor the server shape leaves it.
    #[test]
    fn cli_profile_hands_maintenance_the_cgroup() {
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
        assert_eq!(server.light_share_bytes(), server.maintenance_pool_bytes - server.coordinator_share_bytes() - server.heavy_share_bytes());
        assert_eq!(cli.coordinator_share_bytes(), 0, "the CLI drives engines directly; no coordinator competes for the pool");
    }

    /// `TIMEFUSION_MEMORY_BUDGET_GB` must scale the WHOLE tree from one input and
    /// must never raise the limit above what the cgroup allows.
    #[test]
    fn budget_tree_allocates_the_entire_limit_and_the_override_only_lowers() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        let total = b.query_pool_bytes + b.ingest_buffer_bytes + b.foyer_memory_bytes + b.writer_reserve_bytes + b.maintenance_pool_bytes;
        // The tree must NOT hand out every byte: 15% stays unsanctioned for the
        // consumers no pool tracks (decode, parse ASTs, allocator overhead).
        assert_eq!(total, 120 * GIB - (120.0 * GIB as f64 * 0.15) as usize, "tracked consumers + 15% untracked slack == the limit");
        assert_eq!(b.query_pool_bytes, 24 * GIB);
        assert_eq!(b.ingest_buffer_bytes, 24 * GIB);
        assert_eq!(b.foyer_memory_bytes, 12 * GIB);
        // 120 - (24 query + 24 buffer + 12 foyer + 12 writer reserve + 18 slack).
        assert_eq!(b.maintenance_pool_bytes, 30 * GIB, "maintenance takes the remainder AFTER slack");
        assert_eq!(b.tick_budget(Duration::from_secs(300)), Duration::from_secs(240), "a tick budget is 80% of the cron period");

        let capped = DerivedBudget::from_limits(80 * GIB, 48);
        assert_eq!(capped.query_pool_bytes, 16 * GIB);
        assert_eq!(capped.ingest_buffer_bytes, 16 * GIB);
        assert_eq!(capped.foyer_memory_bytes, 8 * GIB);
        assert!(capped.maintenance_pool_bytes < b.maintenance_pool_bytes, "maintenance shrinks with the rest, not at its expense");

        // The clamp: an over-large request can never budget past the cgroup.
        assert_eq!(effective_limit(80 * GIB, Some(200 * GIB)), 80 * GIB);
        assert_eq!(effective_limit(80 * GIB, Some(40 * GIB)), 40 * GIB);
        assert_eq!(effective_limit(80 * GIB, None), 80 * GIB);
    }

    // Bare numbers must coerce to seconds (a unitless value panics
    // object_store's Duration parse at boot); values with a unit pass through.
    #[test_case::test_case(Some("150"), "60s" => "150s" ; "a bare number coerces to seconds")]
    #[test_case::test_case(Some("150s"), "60s" => "150s" ; "an explicit seconds unit passes through")]
    #[test_case::test_case(Some("3m"), "60s" => "3m" ; "a non-seconds unit passes through")]
    #[test_case::test_case(Some(""), "60s" => "" ; "an explicitly-empty value is passed through, not defaulted")]
    #[test_case::test_case(None, "60s" => "60s" ; "unset takes the default")]
    fn normalize_duration_coerces_bare_numbers_to_seconds(configured: Option<&str>, default: &str) -> String {
        normalize_duration(configured, default)
    }

    /// The commit-log request class must stay ORDERS OF MAGNITUDE under the data
    /// bound; a default that drifted up to `request_timeout` stalls the commit lock.
    #[test]
    fn log_request_timeout_defaults_far_below_the_data_bound() {
        let aws = AwsConfig::default();
        assert_eq!(aws.log_request_timeout(), "30s");
        assert_eq!(aws.request_timeout(), "900s");
        let tuned = AwsConfig { timefusion_s3_log_request_timeout: Some("45".into()), ..Default::default() };
        assert_eq!(tuned.log_request_timeout(), "45s", "bare numbers coerce here too, or boot panics");
        let connect = AwsConfig { timefusion_s3_connect_timeout: Some("150".into()), ..Default::default() };
        assert_eq!(connect.connect_timeout(), "150s", "the connect timeout coerces the same way");
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
        // Merge-on-read DV is the default write path.
        assert!(config.maintenance.timefusion_use_deletion_vectors);
        assert!(!config.maintenance.timefusion_warm_full_files);
        assert_eq!(config.maintenance.timefusion_warm_recency_days, 35);
        assert_eq!(config.maintenance.timefusion_warm_concurrency, 16);
        // Durable-by-default WAL: an async fsync default lets an OOM-kill tear
        // the mmap tail and silently quarantine acked rows.
        assert_eq!(config.buffer.wal_fsync_mode(), WalFsyncMode::SyncEach);
        assert!(config.buffer.wal_ack_fsync());
        // Compression: TWO levels, working < sealed.
        let p = &config.parquet;
        assert_eq!(p.timefusion_zstd_compression_level, 3);
        assert!(p.timefusion_zstd_compression_level < p.timefusion_zstd_level_warm);
    }

    /// Whatever an operator asks for, the pool must still hold
    /// `CONCURRENT_SORT_QUERIES` sorts of it.
    #[test_case::test_case(Some(128 * MIB), 24, 16 * GIB, CONCURRENT_SORT_QUERIES ; "prod: an over-large request is clamped")]
    #[test_case::test_case(None, 24, 16 * GIB, CONCURRENT_SORT_QUERIES ; "prod: the default already fits")]
    #[test_case::test_case(Some(usize::MAX), 48, 16 * GIB, CONCURRENT_SORT_QUERIES ; "an absurd request cannot escape the pool")]
    #[test_case::test_case(None, 2, 8 * GIB, 10 ; "maintenance scan: few partitions, keeps the default")]
    // Prod's ACTUAL shape. The old table stopped at a 16 GiB pool and never
    // asked what happens at the size prod really runs, which is how a constant
    // describing the client could drift from the client unnoticed.
    #[test_case::test_case(None, 24, 22 * GIB, CONCURRENT_SORT_QUERIES ; "prod today: 24 partitions against the 22 GB pool")]
    // Prod's REAL partition count, read from information_schema.df_settings on
    // 2026-09-14: target_partitions is 8, not the 24 an earlier fix assumed.
    #[test_case::test_case(None, 8, 22 * GIB, CONCURRENT_SORT_QUERIES ; "prod today: the 8 partitions prod actually runs")]
    // THE MAINTENANCE POOL, which is shared by maintenance workers and never sees a
    // client. Feeding it the client count over-divides and starves the reservation:
    // #288 did exactly that, taking it 64 MB -> 33 MB, and prod began failing rollup
    // aggregations with "Failed to reserve memory for sort during spill" — six in
    // seven minutes against zero in the hour before.
    #[test_case::test_case(None, 2, 5_000_000_000, 10 ; "maintenance pool: its own worker count, not the client count")]
    fn sort_reservation_always_fits_the_pool(requested: Option<usize>, partitions: usize, pool: usize, concurrency: usize) {
        let got = sort_spill_reservation_bytes(requested, partitions, pool, concurrency);
        // Divided, not multiplied: an unclamped `usize::MAX` request would
        // overflow the product and fail as a panic rather than as this claim.
        // Reservations must leave room for the DATA. Asserting only that they fit
        // the pool passes at the exact point where they fill it and starve every
        // sort of anywhere to sort into — which is how a clamp that "fits" still
        // produced `Resources exhausted` with 78.1 MB free.
        let total = got * partitions * concurrency;
        assert!(
            total <= pool / RESERVATION_POOL_SHARE || got == MIN_SORT_SPILL_RESERVATION_BYTES,
            "{got} x {partitions} x {concurrency} = {total} takes more than 1/{RESERVATION_POOL_SHARE} of the {pool}-byte pool"
        );
        assert!(got >= MIN_SORT_SPILL_RESERVATION_BYTES, "clamped below the merge floor: {got}");
        // Never RAISES a request — this is a ceiling, not a target.
        assert!(got <= requested.unwrap_or(DEFAULT_SORT_SPILL_RESERVATION_BYTES));
    }

    /// The maintenance pool must not clamp at all. Its ~5 GB is shared by ~10
    /// coordinator workers at `MAINTENANCE_MAX_PARTITIONS` = 2, which is 125 MB
    /// each — comfortably above the 64 MB default, so the ceiling never binds.
    ///
    /// #288 passed the CLIENT connection count (38) here instead of the worker
    /// count, cutting the reservation to 33 MB, and prod began failing rollup
    /// aggregations with "Failed to reserve memory for sort during spill" — six in
    /// seven minutes against zero in the hour before. The reservation is the memory
    /// held BACK so a spilling operator can finish; starving it causes precisely
    /// the failure it exists to prevent.
    #[test]
    fn the_maintenance_pool_never_clamps_its_spill_reservation() {
        const MAINTENANCE_PARTITIONS: usize = 2;
        const MAINTENANCE_WORKERS: usize = 10;
        const POOL: usize = 5_000_000_000;
        assert_eq!(
            sort_spill_reservation_bytes(None, MAINTENANCE_PARTITIONS, POOL, MAINTENANCE_WORKERS),
            DEFAULT_SORT_SPILL_RESERVATION_BYTES,
            "the maintenance pool is generous enough that the ceiling must not bind"
        );
        // And with the client count it WOULD bind — the regression, pinned so the
        // two numbers can never be confused for each other again.
        assert!(
            sort_spill_reservation_bytes(None, MAINTENANCE_PARTITIONS, POOL, CONCURRENT_SORT_QUERIES) < DEFAULT_SORT_SPILL_RESERVATION_BYTES,
            "using the client count here is what starved the spill reservation"
        );
    }

    /// K is derived from pool geometry, NOT from any client count — the invariant
    /// that stops a monoscope pool bump from silently starving each sort's merge
    /// reservation. On prod's 8 partitions x 22 GB pool it admits ~20 concurrent
    /// heavy sorts, and NEVER fewer than the floor.
    #[test]
    fn heavy_sort_admission_is_derived_from_the_pool_not_the_clients() {
        assert_eq!(max_concurrent_heavy_sorts(8, 22 * GIB), 22 * GIB / (DEFAULT_SORT_SPILL_RESERVATION_BYTES * 8 * RESERVATION_POOL_SHARE));
        assert!(max_concurrent_heavy_sorts(8, 22 * GIB) >= 20, "prod geometry admits a useful degree of concurrency");
        // A tiny pool still admits the floor rather than serializing everything.
        assert_eq!(max_concurrent_heavy_sorts(48, 512 * MIB), MIN_CONCURRENT_HEAVY_SORTS);
    }

    // Prod-shaped box (120 GiB / 48 cores, 11 hot projects).
    #[test]
    fn derived_budget_prod_box_120gib_48cores() {
        let b = DerivedBudget::from_limits(120 * GIB, 48);
        assert!(b.heavy_share_bytes() as f64 >= b.maintenance_pool_bytes() as f64 * 0.25 - 1.0);
        let k = b.light_optimize_k(11);
        assert!((3..=11).contains(&k), "K={k} outside the expected 3..=11 range");
        assert_eq!(
            k + b.repair_pool_holdback_slices(),
            b.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES,
            "exactly the repair lane's holdback is reserved out of light's share"
        );
        // The envelope (permits x per-sort budget) is the invariant, not the raw
        // permit count.
        assert_eq!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES,
            20 * GIB,
            "changing the fan-in envelope must be deliberate: state the memory headroom that pays for it"
        );
        assert!(
            b.rewrite_permits() * PER_SORT_BUDGET_BYTES < b.memory_limit_bytes() / 2,
            "the fan-in envelope must stay well under the cgroup, whatever the permit count"
        );
        // A FairSpillPool slice must clear what a sort cannot avoid allocating:
        // one indivisible batch plus `ExternalSorterMerge`'s unspillable floor.
        // Below that a unit fails outright instead of spilling.
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

    /// A repair unit is exactly one file and cannot be split, so the repair
    /// rewrite budget must hold one target-sized file decoded; below that every
    /// unit clamps to the whole semaphore and repair serializes.
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

    /// Both maintenance lookbacks must reach far enough back to serve a 30d query.
    /// The repair lookback IS the suspect-set size, so it is bounded on both sides.
    #[test]
    fn lookback_windows_cover_a_thirty_day_query_without_flooding() {
        let m = &AppConfig::default().maintenance;
        assert!(m.timefusion_dedup_lookback_days >= 30, "the dedup sweep is what certifies partitions; below 30d no 30d query can ever route to a rollup");
        let d = m.timefusion_light_optimize_repair_days;
        assert!(d >= 30, "must cover the 30-day window users actually query, got {d}");
        assert!(d <= 45, "must not balloon the suspect set beyond the query window, got {d}");
    }

    /// Maintenance must not be serialized on a box with room to spare, the
    /// coordinator pool must scale with the jobs sharing it, and the three
    /// maintenance shares must still sum to the pool. Each job's slice must also
    /// clear `ExternalSorterMerge`'s 32 MB floor, or units fail instead of spilling.
    #[test_case::test_case(80, 48, 2..=16 ; "prod-shaped box must run maintenance in parallel")]
    #[test_case::test_case(16, 4, 1..=2 ; "a 4-core box must not run maintenance wide")]
    #[test_case::test_case(8, 4, 1..=2 ; "a tiny box stays modest rather than thrashing")]
    fn coordinator_jobs_and_pool_scale_with_the_box(limit_gb: usize, cores: usize, jobs: std::ops::RangeInclusive<usize>) {
        let b = DerivedBudget::from_limits(limit_gb * GIB, cores);
        let committed = b.query_pool_bytes() + b.buffer_max_bytes() + b.foyer_memory_bytes() + b.writer_reserve_bytes() + b.maintenance_pool_bytes();
        assert!(committed <= limit_gb * GIB, "{limit_gb} GiB box over-committed: {committed}");
        assert_eq!(
            b.coordinator_share_bytes() + b.heavy_share_bytes() + b.light_share_bytes(),
            b.maintenance_pool_bytes(),
            "{limit_gb} GiB/{cores}-core: maintenance shares must partition the pool, not overcommit it"
        );
        // Only meaningful when the operator has not pinned the override.
        if std::env::var("TIMEFUSION_COORDINATOR_JOB_WORKERS").is_ok() {
            return;
        }
        assert!(jobs.contains(&b.coordinator_jobs()), "{limit_gb} GiB/{cores}-core: expected {jobs:?} jobs, got {}", b.coordinator_jobs());
        // Every admitted unit reserves at most MAX_DECODED_BYTES, so concurrent
        // decode reservation must still fit the maintenance pool.
        assert!(
            b.coordinator_jobs() * 512 * MIB <= b.maintenance_pool_bytes(),
            "{limit_gb} GiB/{cores}-core: concurrent 512 MiB units must fit the maintenance pool"
        );
        let per_job = b.coordinator_share_bytes() / b.coordinator_jobs();
        assert!(
            per_job >= 32 * MIB,
            "{limit_gb} GiB/{cores}-core: each of {} jobs gets {} MB, below the 32 MB sort floor",
            b.coordinator_jobs(),
            per_job / MIB
        );
    }

    /// The hot-packing permit must be priced against the pool its units allocate
    /// from, or it moves whenever an unrelated share does.
    #[test]
    fn the_packing_permit_follows_the_coordinator_pool_not_the_light_share() {
        let prod = DerivedBudget::from_limits(80 * GIB, 48);
        assert_eq!(
            prod.light_optimize_k(11),
            prod.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES - prod.repair_pool_holdback_slices(),
            "the memory term is the coordinator's pool, less the pool repair's decoded budget actually needs"
        );
        assert!(prod.light_optimize_k(11) > 1, "one permit shared by HotPacking and SealedConsolidation starves packing");
        // The measured optimum: 6 concurrent rewrites.
        assert_eq!(
            prod.light_optimize_k(11) + prod.repair_pool_holdback_slices(),
            6,
            "the fleet must run at the measured optimum, not one rung either side — the light/repair SPLIT may move, the total may not"
        );
        // Repair's decoded budget must fit the pool its holdback reserves.
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
        let b = DerivedBudget::from_limits(16 * GIB, 4);
        // cores/4 = 1 pins it here whatever the memory term says.
        assert_eq!(b.light_optimize_k(11), 1);
        assert!(b.maintenance_pool_bytes() >= GIB);
        assert!(b.light_share_bytes() > 0);
        assert!(b.heavy_share_bytes() > 0);
        assert!(b.tick_budget(Duration::from_secs(300)) < Duration::from_secs(300));
        assert!(b.memory_brake_limit_bytes() < b.memory_limit_bytes());
    }

    /// The brake must stay well clear of the cgroup the OOM killer watches, and
    /// the budgeted limit is NOT that cgroup (a deployment may budget 82 GiB
    /// inside a 96 GiB container).
    #[test]
    fn memory_brake_leaves_real_headroom_under_the_cgroup() {
        let b = DerivedBudget::from_limits(100 * GIB, 48);
        assert_eq!(b.memory_brake_limit_bytes(), 80 * GIB);

        // 82 GiB budgeted inside a 96 GiB cgroup.
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

    /// The clamps and unit conversions applied on top of a configured value:
    /// a below-floor buffer request is raised to the floor, and the cache's MB
    /// knobs convert to bytes.
    #[test]
    fn buffer_floor_is_enforced_and_cache_sizes_convert() {
        let mut config = AppConfig::default();
        config.buffer.timefusion_buffer_max_memory_mb = 10;
        config.cache.timefusion_foyer_memory_mb = 256;
        config.cache.timefusion_foyer_disk_mb = Some(1024);
        assert_eq!(config.buffer.max_memory_mb(), 64, "a below-floor buffer request is clamped up to the floor");
        assert_eq!(config.cache.memory_size_bytes(), 256 * MIB);
        assert_eq!(config.cache.disk_size_bytes(), GIB);
    }

    #[test]
    fn wal_backlog_limit_derives_from_the_tree_and_allows_override() {
        let mut config = AppConfig::default();
        // Unset: the effective threshold comes from the derived tree.
        assert_eq!(config.effective_wal_max_unflushed_bytes(), config.derived.wal_flush_byte_threshold());
        assert_eq!(config.effective_wal_max_files(), config.derived.wal_flush_file_threshold());

        // Env override still wins.
        config.buffer.timefusion_wal_max_unflushed_mb = 12_000;
        config.buffer.timefusion_wal_max_file_count = 300;
        assert_eq!(config.effective_wal_max_unflushed_bytes(), 12_000 * MIB as u64);
        assert_eq!(config.effective_wal_max_files(), 300);
    }
}

// ===== autotune =====
// Host-aware auto-tuning of memory/disk/parallelism knobs, applied in
// `init_config()` after env deserialization and before the `OnceLock` is sealed.
// A knob is only overridden when its env var is NOT set — user input wins.
//
// Budget invariant on a fresh host: query_pool ~30% RAM, mem_buffer ~25%,
// foyer_mem ~15%, foyer_meta <=2% (capped at 512MB) => ~72% reserved. The
// remaining ~28% is not spare: it is the only budget parquet decode has
// (explicitly unpooled), plus walrus mmaps and allocator slack. `budget_audit`
// sums what the process actually commits and warns when it gets thin.

use sysinfo::Disks;
use tracing::{info, warn};

const RAM_FRACTION_FOYER_META: f64 = 0.02;
const DISK_FRACTION_FOYER: f64 = 0.40;
const DISK_FRACTION_FOYER_META: f64 = 0.02;

/// Warn when the final (post-override) sum of memory reservations exceeds this
/// share of detected RAM — a little tolerance over the ~72% budget above.
/// Anything higher leaves too little for the untracked consumers (parquet decode
/// alone can exceed a 12 GB remainder). Small hosts can exceed this via the
/// 1 GiB maintenance-pool floor; that is truthful, and it stays WARN-only.
const OVERSUB_WARN_PCT: usize = 75;

const MIN_BUFFER_MB: usize = 256;
const MIN_FOYER_MEM_MB: usize = 128;
// 16 GiB: the derived tree reserves 10% of the limit for foyer, so a smaller
// cap would strand part of that reservation on a large box.
const MAX_FOYER_MEM_MB: usize = 16 * 1024;
const MIN_FOYER_META_MB: usize = 64;
const MAX_FOYER_META_MB: usize = 512;
const MIN_FOYER_DISK_GB: usize = 1;
const MAX_FOYER_DISK_GB: usize = 500;
const MAX_FOYER_META_DISK_GB: usize = 5;

/// Apply host-aware overrides to `config`. Knobs whose env var is set by the
/// user are left untouched.
pub fn apply(config: &mut AppConfig) {
    // ONE memory source: the derived budget tree's cgroup-clamped detection, so
    // the audit warns against the same denominator the budgets were derived from.
    let total_ram_mb = config.derived.memory_limit_bytes() / MIB;

    let cpus = detect_cores();

    let data_dir = &config.core.timefusion_data_dir;
    let available_disk_gb = available_disk_for(data_dir);

    info!(
        "Auto-tune host detection: ram={}GB, cpus={}, data_dir={:?}, available_disk={}",
        total_ram_mb / 1024,
        cpus,
        data_dir,
        available_disk_gb.map_or_else(|| "unknown".to_string(), |g| format!("{g}GB"))
    );

    // A `None` derived value (the disk probe found no mount) leaves the knob alone.
    let mut applied = Vec::new();
    let mut tune = |name: &'static str, slot: &mut usize, derived: Option<usize>, unit: &str| {
        if let Some(d) = derived.filter(|d| *d != *slot && std::env::var(name).is_err()) {
            *slot = d;
            applied.push(format!("{name}={d}{unit}"));
        }
    };
    let disk_share = |fraction: f64, max| available_disk_gb.map(|gb| ((gb as f64 * fraction) as usize).clamp(MIN_FOYER_DISK_GB, max));

    // MemBuffer and foyer memory come from DerivedBudget — ONE set of RAM
    // fractions; autotune only applies them.
    tune(
        "TIMEFUSION_BUFFER_MAX_MEMORY_MB",
        &mut config.buffer.timefusion_buffer_max_memory_mb,
        Some((config.derived.buffer_max_bytes() / MIB).max(MIN_BUFFER_MB)),
        "MB",
    );
    tune(
        "TIMEFUSION_FOYER_MEMORY_MB",
        &mut config.cache.timefusion_foyer_memory_mb,
        Some((config.derived.object_cache_memory_bytes() / MIB).clamp(MIN_FOYER_MEM_MB, MAX_FOYER_MEM_MB)),
        "MB",
    );
    tune(
        "TIMEFUSION_FOYER_METADATA_MEMORY_MB",
        &mut config.cache.timefusion_foyer_metadata_memory_mb,
        Some(((total_ram_mb as f64 * RAM_FRACTION_FOYER_META) as usize).clamp(MIN_FOYER_META_MB, MAX_FOYER_META_MB)),
        "MB",
    );
    tune("TIMEFUSION_FOYER_DISK_GB", &mut config.cache.timefusion_foyer_disk_gb, disk_share(DISK_FRACTION_FOYER, MAX_FOYER_DISK_GB), "GB");
    tune(
        "TIMEFUSION_FOYER_METADATA_DISK_GB",
        &mut config.cache.timefusion_foyer_metadata_disk_gb,
        disk_share(DISK_FRACTION_FOYER_META, MAX_FOYER_META_DISK_GB),
        "GB",
    );
    tune("TIMEFUSION_FLUSH_PARALLELISM", &mut config.buffer.timefusion_flush_parallelism, Some((cpus / 2).max(2)), "");
    // Query/maintenance target_partitions. `detect_cores` derives from the cgroup
    // quota; DataFusion's own default (`num_cpus::get()`) honors cpuset pinning
    // but not the CFS quota, so it oversubscribes throttled containers.
    //
    // CAPPED at QUERY_PARTITIONS_MAX, which is a MEMORY bound, not a CPU one: the
    // sort machinery reserves per partition and the merge halves (TopK,
    // SortPreservingMerge, ExternalSorterMerge) cannot spill, so the unspillable
    // peak scales with `target_partitions` rather than with how much work there
    // is. The cap costs large-scan CPU parallelism to buy that headroom;
    // `TIMEFUSION_QUERY_PARTITIONS` overrides it without a redeploy.
    tune("TIMEFUSION_QUERY_PARTITIONS", &mut config.memory.timefusion_query_partitions, Some(cpus.min(QUERY_PARTITIONS_MAX)), "");

    if applied.is_empty() {
        info!("Auto-tune: no overrides applied (user has set all knobs explicitly or host signals unavailable)");
    } else {
        info!("Auto-tune applied: {}", applied.join(", "));
    }

    // Coherence guard: user-pinned envs can oversubscribe RAM even though the
    // auto-derived split respects the ~72% invariant by construction. RECLAIM,
    // don't merely warn — the audit counts three ceilings the tree never
    // reserved (MemBuffer's 120% admission hard limit, the tantivy writer peak,
    // the DataFusion metadata cache), and maintenance is the residual claimant
    // that silently absorbed them, so the overage comes back out of its pool.
    let mut audit = budget_audit(config, total_ram_mb);
    if audit.oversubscribed() {
        let overage = audit.committed_mb.saturating_sub(audit.warn_at_mb) * MIB;
        let reclaimed = config.derived.reclaim_maintenance_pool(overage) / MIB;
        audit = budget_audit(config, total_ram_mb);
        warn!(
            "bootstrap.phase=budget_reclaim reclaimed_mb={reclaimed} from the maintenance pool              (it is the residual claimant, so it is what absorbed the unreserved MemBuffer overshoot,              tantivy peak and metadata cache) — maintenance_pool now {}mb, committed {}mb vs warn_at {}mb",
            audit.maintenance_pool_mb, audit.committed_mb, audit.warn_at_mb
        );
    }
    let _ = BOOT_AUDIT.set(audit);
    // Always emit the breakdown: the slack figure is the operator's only view of
    // how much room the untracked consumers have.
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

/// The audit computed at startup, so `timefusion_stats` can report the
/// committed-vs-slack split on demand without re-detecting RAM.
static BOOT_AUDIT: std::sync::OnceLock<BudgetAudit> = std::sync::OnceLock::new();

pub fn boot_budget_audit() -> Option<&'static BudgetAudit> {
    BOOT_AUDIT.get()
}

/// Sum the committed budgets against RAM.
///
/// Every term matters: the maintenance pool, MemBuffer at its 120% admission
/// hard ceiling (not nominal), the query pool at `limit × memory_fraction`, and
/// the DataFusion metadata cache. The light-optimize slice is deliberately NOT
/// added — it is carved out of `maintenance_pool_bytes()`.
pub fn budget_audit(config: &AppConfig, total_ram_mb: usize) -> BudgetAudit {
    let foyer_mb = if config.cache.is_disabled() { 0 } else { (config.cache.memory_size_bytes() + config.cache.metadata_memory_size_bytes()) / MIB };
    // Peak tantivy writer heap: one writer per in-flight flush.
    let tantivy_peak_mb =
        if config.tantivy.indexed_tables().is_empty() { 0 } else { crate::tantivy::WRITER_HEAP_BYTES * config.buffer.flush_parallelism() / MIB };
    // Mirror `BufferedWriteLayer::max_memory_bytes`: the configured knob is
    // reduced by foyer + tantivy (which are counted separately below, so using
    // the raw knob here would double-count them), then admission runs to a 120%
    // hard ceiling (HARD_LIMIT_HEADROOM_DIVISOR).
    let mem_buffer_hard_mb = config.buffer.max_memory_mb().saturating_sub(foyer_mb + tantivy_peak_mb).max(64) * 6 / 5;
    let query_pool_mb = config.derived.query_pool_bytes() / MIB;
    let maintenance_pool_mb = config.derived.maintenance_pool_bytes() / MIB;
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

/// Return free space (GB) on the volume hosting `path`, or None if no disk in
/// the sysinfo enumeration covers it (callers then skip the disk overrides).
fn available_disk_for(path: &std::path::Path) -> Option<usize> {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    // Pick the disk whose mount_point is the longest prefix of our path.
    Disks::new_with_refreshed_list()
        .iter()
        .filter(|d| canonical.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| (d.available_space() / GIB as u64) as usize)
}

#[cfg(test)]
mod autotune_tests {
    use test_case::test_case;

    use super::*;

    fn cfg_mb(buffer: usize, foyer: usize, foyer_meta: usize) -> AppConfig {
        let mut cfg = AppConfig::default();
        cfg.buffer.timefusion_buffer_max_memory_mb = buffer;
        cfg.cache.timefusion_foyer_memory_mb = foyer;
        cfg.cache.timefusion_foyer_metadata_memory_mb = foyer_meta;
        cfg
    }

    /// Audit `cfg` and assert the accounting identities that must hold for ANY
    /// config: both pools come from the derived budget, and MemBuffer's 120%
    /// ceiling applies to its EFFECTIVE budget (knob − foyer − tantivy peak),
    /// not the raw knob, which would count foyer twice.
    fn audited(cfg: &AppConfig, ram_mb: usize) -> BudgetAudit {
        let a = budget_audit(cfg, ram_mb);
        assert_eq!(a.query_pool_mb, cfg.derived.query_pool_bytes() / MIB);
        assert_eq!(a.maintenance_pool_mb, cfg.derived.maintenance_pool_bytes() / MIB, "was missing entirely");
        let effective = cfg.buffer.timefusion_buffer_max_memory_mb - a.foyer_mb - a.tantivy_peak_mb;
        assert_eq!(a.mem_buffer_hard_mb, effective * 6 / 5);
        assert_eq!(a.committed_mb, a.query_pool_mb + a.mem_buffer_hard_mb + a.maintenance_pool_mb + a.foyer_mb + a.tantivy_peak_mb + a.df_metadata_cache_mb);
        a
    }

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

    /// An over-committed config must audit as oversubscribed; the identities
    /// behind the verdict live in `audited`. RAM here is the container limit,
    /// not the host — that is what the kernel kills on.
    #[test_case(24000, 4048, 512, 24 * 1024 => (4560, 18432, true) ; "24GB MemBuffer in a 24GiB container is flagged")]
    #[test_case(4096, 1024, 256, 256 * 1024 => (1280, 196608, false) ; "real slack passes")]
    // Unknown RAM (0) must not divide-by-zero; warn_at collapses to 0 so a
    // non-zero commitment is flagged rather than silently passing.
    #[test_case(4096, 1024, 256, 0 => (1280, 0, true) ; "unknown ram flags rather than passes")]
    fn budget_audit_flags_oversubscription(buffer: usize, foyer: usize, foyer_meta: usize, ram_mb: usize) -> (usize, usize, bool) {
        let a = audited(&cfg_mb(buffer, foyer, foyer_meta), ram_mb);
        (a.foyer_mb, a.warn_at_mb, a.oversubscribed())
    }

    /// An oversubscribed budget must be RECLAIMED, not merely warned about, and
    /// the reclaim comes out of maintenance — the residual claimant that
    /// absorbed the ceilings the tree never reserved.
    #[test]
    fn an_oversubscribed_budget_is_reclaimed_from_the_residual_pool() {
        let mut cfg = AppConfig::default();
        cfg.buffer.timefusion_buffer_max_memory_mb = 24000;
        let ram_mb = 24 * 1024;
        let before = audited(&cfg, ram_mb);
        assert!(before.oversubscribed(), "fixture must start oversubscribed: {before:?}");

        let overage = before.committed_mb.saturating_sub(before.warn_at_mb);
        let reclaimed = cfg.derived.reclaim_maintenance_pool(overage * MIB) / MIB;
        let after = audited(&cfg, ram_mb);

        assert!(reclaimed > 0, "something must actually be surrendered");
        assert_eq!(after.maintenance_pool_mb, before.maintenance_pool_mb - reclaimed, "the reclaim comes out of maintenance");
        assert_eq!(after.committed_mb, before.committed_mb - reclaimed, "and therefore out of committed");
        // Every other budget is untouched.
        assert_eq!((after.query_pool_mb, after.mem_buffer_hard_mb, after.foyer_mb), (before.query_pool_mb, before.mem_buffer_hard_mb, before.foyer_mb));

        // A pool that would go below the floor stops there rather than going negative.
        let floored = cfg.derived.reclaim_maintenance_pool(usize::MAX);
        assert!(cfg.derived.maintenance_pool_bytes() >= 1024 * 1024 * 1024, "must not reclaim below the floor");
        assert!(floored <= after.maintenance_pool_mb * MIB);
    }
}

// ===== secret_crypto =====
// AES-256-GCM two-way encryption for at-rest secrets (S3 creds in
// `timefusion_projects`). Key comes from `TIMEFUSION_CONFIG_ENCRYPTION_KEY` as a
// base64-encoded 32-byte value; ciphertext is stored as
// `enc:v1:<base64(nonce||ct||tag)>`. Un-prefixed plaintext rows are still
// accepted on read, so the feature needs no forced backfill.

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

#[cfg(test)]
mod light_permit_floor_tests {
    use super::*;

    /// `light_optimize_k` may never return 1 on a box whose coordinator share
    /// can hold two sorts. The permit is taken BEFORE the claim and shared by
    /// HotPacking and SealedConsolidation, so at K=1 the two cannot run
    /// concurrently and any stalled unit stops the lane entirely.
    #[test_case::test_case(28, 1; "prod: 28-core cgroup cap, the box this was found on")]
    #[test_case::test_case(30, 1; "just past the jobs boundary and still starved before the fix")]
    #[test_case::test_case(16, 1; "a small box")]
    #[test_case::test_case(48, 3; "the box the formula was calibrated for")]
    fn the_hygiene_lane_never_collapses_to_one_permit(cores: usize, slices_before_fix: usize) {
        let budget = DerivedBudget::from_limits(120 * GIB, cores);
        let k = budget.max_light_optimize_k();
        let share_slices = budget.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES;
        assert!(
            k >= LIGHT_MIN_SLICES.min(share_slices),
            "cores={cores}: share holds {share_slices} sorts but K={k} (was {slices_before_fix} before the holdback cap)"
        );
        // Repair keeps its full holdback wherever the share can pay for it.
        if cores == 48 {
            assert_eq!(k, 3, "the 48-core derivation must not move");
        }
    }

    /// A box too small to hold two sorts is left alone rather than
    /// over-committed — the floor is a floor, not a demand.
    #[test]
    fn a_tiny_share_is_not_over_committed() {
        let budget = DerivedBudget::from_limits(2 * GIB, 2);
        let share_slices = budget.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES;
        assert!(share_slices < LIGHT_MIN_SLICES, "precondition: this box cannot hold two sorts");
        assert_eq!(budget.max_light_optimize_k(), 1, "and must not be pushed to two");
    }
}

#[cfg(test)]
mod bin_decode_budget_tests {
    use super::*;

    /// A packing bin must fit ONE SORT once decoded. Packing targets are
    /// COMPRESSED bytes and sort budgets are DECODED bytes; nothing else converts
    /// between them, so an uncapped bin stages forever without finishing.
    #[test]
    fn a_bin_capped_for_packing_still_fits_one_sort_decoded() {
        let cap = coordinator_bin_compressed_cap_bytes();
        let decoded = cap as usize * crate::database::DECODED_BYTES_PER_COMPRESSED as usize;
        assert!(
            decoded <= COORDINATOR_PER_SORT_BUDGET_BYTES,
            "a full bin decodes to {decoded} bytes against a {COORDINATOR_PER_SORT_BUDGET_BYTES}-byte sort budget"
        );
        // And the cap is what actually binds, i.e. it is below the output target
        // we would otherwise pack to — otherwise this guard is decorative.
        assert!(cap < crate::database::COORDINATOR_HOT_TARGET_BYTES, "the decode cap ({cap}) must bind below the packing target, or it changes nothing");
        // The boundary, stated so it is not re-derived by hand.
        let ratio = |mb: i64| (mb * 1024 * 1024 * crate::database::DECODED_BYTES_PER_COMPRESSED) as f64 / COORDINATOR_PER_SORT_BUDGET_BYTES as f64;
        assert!(ratio(255) > 2.0, "the stalled prod bins were well past one sort budget");
        assert!(ratio(cap / (1024 * 1024)) <= 1.0, "a bin at the new cap is not");
    }

    /// The repair holdback must be LENDABLE, or the hygiene lane stays at K=2
    /// while repair sits idle.
    ///
    /// Prod 2026-09-13: `pending_repair` was ZERO all day while the reservation
    /// pinned the sealed lane — the lane holding a backlog the box was behind
    /// on — at two concurrent sorts. At 32 cores the share holds 4 slices and
    /// the holdback took 2 of them.
    ///
    /// Also pins the safety property: lending never exceeds what the budget tree
    /// computed, because `slices` IS `coordinator_share / per_sort_budget`. The
    /// holdback is a reservation, not a memory ceiling.
    #[test]
    fn the_repair_holdback_is_lendable_and_bounded_by_the_share() {
        // Both shapes: the 48-core box this tree was calibrated for, and the
        // 32 cores the container is actually capped at.
        for (limit_gib, cores) in [(80usize, 48usize), (120, 32)] {
            let prod = DerivedBudget::from_limits(limit_gib * GIB, cores);
            let lendable = prod.repair_holdback_permits();
            assert!(lendable > 0, "with a holdback in force there must be something to lend, or the hygiene lane can never reach its share");
            // Lending must land exactly on the share's slice count -- never past it.
            let share_slices = prod.coordinator_share_bytes() / COORDINATOR_PER_SORT_BUDGET_BYTES;
            let lent_total = prod.max_light_optimize_k() + lendable;
            assert!(
                lent_total <= share_slices.max(1),
                "lending {lendable} on top of K={} would exceed the {share_slices} slices the coordinator share holds",
                prod.max_light_optimize_k()
            );
            assert!(lent_total <= prod.cores / 4, "and must still respect the CPU term");
        }
    }

    /// A packing cap must ALWAYS admit two target-sized files, margin or not:
    /// below 2x the target file size no pair fits, the packer returns empty, and
    /// the cell re-enqueues forever. A compaction cap is a MULTIPLE of the target
    /// file size (RocksDB uses 25x, IOx 3x).
    #[test]
    fn a_packing_cap_always_admits_two_target_sized_files() {
        let metrics_file = 36 * 1024 * 1024;
        let cap = coordinator_packing_cap_bytes(metrics_file * 2);
        assert!(cap >= 72_140_172, "the two smallest prod otel_metrics files ({}) must pair under the cap ({cap})", 72_140_172_i64);
        assert!(cap >= metrics_file * 2, "a cap below 2x the target file size can never merge anything");
        // Where the floor does not bind, the margin still governs.
        let small = 4 * 1024 * 1024;
        let margin_cap = coordinator_packing_cap_bytes(small * 2);
        // An ODD pair must still fit: halving then doubling loses a byte, which
        // prod hit as `target=82703666` against `smallest_pair_bytes=82703667`.
        let odd = 82_703_667;
        assert!(coordinator_packing_cap_bytes(odd) >= odd, "an odd-summed pair must fit the cap it was measured against");
        let ratio = margin_cap as f64 * crate::database::DECODED_BYTES_PER_COMPRESSED as f64 / COORDINATOR_PER_SORT_BUDGET_BYTES as f64;
        assert!(ratio <= 0.65, "where the floor does not bind, a full bin is {ratio:.2} of a sort budget; 1.00x measured 28.5 minutes");
        assert!(margin_cap < coordinator_bin_compressed_cap_bytes(), "the margin must still bind below the raw decode cap");
    }
}
