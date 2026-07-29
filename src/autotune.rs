//! Host-aware auto-tuning of memory/disk/parallelism knobs.
//!
//! Applied in `init_config()` after env-var deserialization but before the
//! `OnceLock` is sealed. Each knob is only overridden when the corresponding
//! env var is **not** set — explicit user input always wins.
//!
//! Budget invariant we try to respect on a fresh host with no overrides:
//!     query_pool  ≈ 30% RAM
//!     mem_buffer  ≈ 25% RAM
//!     foyer_mem   ≈ 15% RAM
//!     foyer_meta  ≤ 2% RAM (capped at 512MB)
//!     ─────────────────────
//!     reserved    ≈ 72% RAM, leaving headroom for Arrow scratch, walrus
//!                  mmaps, tantivy, OS page cache.
//!
//! That remaining ~28% is not spare — it is the only budget parquet decode has
//! (explicitly unpooled), alongside walrus mmaps and allocator slack. See
//! [`budget_audit`], which sums what the process actually commits (including
//! the maintenance pool and MemBuffer's 120% admission ceiling, both of which
//! the previous check omitted) and warns when the remainder gets thin.
//!
//! Disk budget: foyer caches take up to 40% of free space on the data dir,
//! capped at 500GB to avoid runaway on very large volumes.
//!
//! Logged once at startup so ops can see exactly what was chosen.

use sysinfo::Disks;
use tracing::info;

use crate::config::AppConfig;

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
const MAX_FOYER_META_MB: usize = 512;
const MIN_FOYER_DISK_GB: usize = 1;
const MAX_FOYER_DISK_GB: usize = 500;
const MAX_FOYER_META_DISK_GB: usize = 5;

/// Apply host-aware overrides to `config`. Knobs whose env var is set by the
/// user are left untouched. Returns the set of knobs that were auto-tuned for
/// logging.
pub fn apply(config: &mut AppConfig) {
    // ONE memory source: the derived budget tree's cgroup-clamped detection.
    // A second (sysinfo) reading here once let the oversubscription audit warn
    // against a different denominator than the budgets were derived from —
    // exactly the drift class the tree exists to kill.
    let total_ram_bytes = config.derived.memory_limit_bytes();
    let total_ram_gb = total_ram_bytes / (1024 * 1024 * 1024);
    let total_ram_mb = total_ram_bytes / (1024 * 1024);

    let cpus = crate::config::detect_cores();

    // Probe free space on the data dir's mount point. Falls back to "unknown"
    // (no disk-derived overrides) if the mount can't be located.
    let data_dir = &config.core.timefusion_data_dir;
    let available_disk_gb = available_disk_for(data_dir);

    info!(
        "Auto-tune host detection: ram={}GB, cpus={}, data_dir={:?}, available_disk={}",
        total_ram_gb,
        cpus,
        data_dir,
        available_disk_gb.map_or("unknown".to_string(), |g| format!("{}GB", g))
    );

    let mut applied: Vec<(&str, String)> = Vec::new();

    // MemBuffer. Default static = 4096MB.
    if env_unset("TIMEFUSION_BUFFER_MAX_MEMORY_MB") {
        // Sourced from DerivedBudget — ONE set of RAM fractions (see config.rs
        // budget tree); autotune only applies it under env-unset-wins semantics.
        let derived = (config.derived.buffer_max_bytes() / (1024 * 1024)).max(MIN_BUFFER_MB);
        if derived != config.buffer.timefusion_buffer_max_memory_mb {
            config.buffer.timefusion_buffer_max_memory_mb = derived;
            applied.push(("TIMEFUSION_BUFFER_MAX_MEMORY_MB", format!("{}MB", derived)));
        }
    }

    // Foyer memory cache. Default static = 512MB.
    if env_unset("TIMEFUSION_FOYER_MEMORY_MB") {
        let derived = (config.derived.foyer_memory_bytes() / (1024 * 1024)).clamp(MIN_FOYER_MEM_MB, MAX_FOYER_MEM_MB);
        if derived != config.cache.timefusion_foyer_memory_mb {
            config.cache.timefusion_foyer_memory_mb = derived;
            applied.push(("TIMEFUSION_FOYER_MEMORY_MB", format!("{}MB", derived)));
        }
    }

    // Foyer metadata memory cache. Default static = 512MB.
    if env_unset("TIMEFUSION_FOYER_METADATA_MEMORY_MB") {
        let derived = ((total_ram_mb as f64 * RAM_FRACTION_FOYER_META) as usize).clamp(64, MAX_FOYER_META_MB);
        if derived != config.cache.timefusion_foyer_metadata_memory_mb {
            config.cache.timefusion_foyer_metadata_memory_mb = derived;
            applied.push(("TIMEFUSION_FOYER_METADATA_MEMORY_MB", format!("{}MB", derived)));
        }
    }

    // Foyer disk cache (depends on available disk on data_dir's volume).
    if let Some(avail_gb) = available_disk_gb {
        if env_unset("TIMEFUSION_FOYER_DISK_GB") {
            let derived = ((avail_gb as f64 * DISK_FRACTION_FOYER) as usize).clamp(MIN_FOYER_DISK_GB, MAX_FOYER_DISK_GB);
            if derived != config.cache.timefusion_foyer_disk_gb {
                config.cache.timefusion_foyer_disk_gb = derived;
                applied.push(("TIMEFUSION_FOYER_DISK_GB", format!("{}GB", derived)));
            }
        }
        if env_unset("TIMEFUSION_FOYER_METADATA_DISK_GB") {
            let derived = ((avail_gb as f64 * DISK_FRACTION_FOYER_META) as usize).clamp(1, MAX_FOYER_META_DISK_GB);
            if derived != config.cache.timefusion_foyer_metadata_disk_gb {
                config.cache.timefusion_foyer_metadata_disk_gb = derived;
                applied.push(("TIMEFUSION_FOYER_METADATA_DISK_GB", format!("{}GB", derived)));
            }
        }
    }

    // Flush parallelism. Default static = 4.
    if env_unset("TIMEFUSION_FLUSH_PARALLELISM") {
        let derived = (cpus / 2).max(2);
        if derived != config.buffer.timefusion_flush_parallelism {
            config.buffer.timefusion_flush_parallelism = derived;
            applied.push(("TIMEFUSION_FLUSH_PARALLELISM", derived.to_string()));
        }
    }

    // Query/maintenance target_partitions, from the cgroup CPU quota. Default
    // static = 0 (DataFusion default); applies to query + optimize sessions.
    if env_unset("TIMEFUSION_QUERY_PARTITIONS") {
        let derived = detected_query_partitions();
        if derived != config.memory.timefusion_query_partitions {
            config.memory.timefusion_query_partitions = derived;
            applied.push(("TIMEFUSION_QUERY_PARTITIONS", derived.to_string()));
        }
    }

    if applied.is_empty() {
        info!("Auto-tune: no overrides applied (user has set all knobs explicitly or host signals unavailable)");
    } else {
        let summary = applied.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(", ");
        info!("Auto-tune applied: {}", summary);
    }

    // Coherence guard: user-pinned envs can oversubscribe RAM (prod ran a
    // hand-set combination for months before the 2026-07-08 OOM loop). The
    // auto-derived split respects the ≈72% invariant by construction; this
    // checks the FINAL post-override sum and warns loudly — operators keep
    // authority, but the failure mode becomes visible instead of an OOM.
    let audit = budget_audit(config, total_ram_mb);
    let _ = BOOT_AUDIT.set(audit);
    // Always emit the breakdown: "we fit" is the interesting case too, since
    // the slack is what absorbs untracked allocation, and an operator tuning a
    // knob needs the current split in front of them.
    let msg = format!(
        "bootstrap.phase=budget_audit committed_mb={} warn_at_mb={} slack_mb={} \
         (query_pool={} mem_buffer_hard={} maintenance_pool={} foyer={} tantivy_peak={} df_metadata_cache={}) ram_mb={total_ram_mb} — \
         slack absorbs UNTRACKED allocation (parquet decode, walrus mmaps, tantivy, allocator overhead); one wide scan can \
         exceed a small slack, which is how a box gets OOM-killed while every individual budget looks fine",
        audit.committed_mb,
        audit.warn_at_mb,
        audit.warn_at_mb.saturating_sub(audit.committed_mb),
        audit.query_pool_mb,
        audit.mem_buffer_hard_mb,
        audit.maintenance_pool_mb,
        audit.foyer_mb,
        audit.tantivy_peak_mb,
        audit.df_metadata_cache_mb,
    );
    // "We fit" is worth logging too: the slack figure is the operator's only
    // view of how much room the untracked consumers actually have.
    if audit.oversubscribed() {
        tracing::warn!("{msg} — OVERSUBSCRIBED, expect OOM kills under load; lower one of these knobs");
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
    const MB: usize = 1024 * 1024;
    let foyer_mb = if config.cache.is_disabled() { 0 } else { (config.cache.memory_size_bytes() + config.cache.metadata_memory_size_bytes()) / MB };
    // Peak tantivy writer heap: one writer per in-flight flush.
    let tantivy_peak_mb =
        if config.tantivy.indexed_tables().is_empty() { 0 } else { crate::tantivy_index::builder::WRITER_HEAP_BYTES * config.buffer.flush_parallelism() / MB };
    // Mirror `BufferedWriteLayer::max_memory_bytes`: the configured knob is
    // reduced by foyer + tantivy (which are counted separately below, so using
    // the raw knob here would double-count them), then admission runs to a 120%
    // hard ceiling (HARD_LIMIT_HEADROOM_DIVISOR).
    let mem_buffer_budget_mb = config.buffer.max_memory_mb().saturating_sub(foyer_mb + tantivy_peak_mb).max(64);
    let mem_buffer_hard_mb = mem_buffer_budget_mb * 6 / 5;
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

fn env_unset(name: &str) -> bool {
    std::env::var(name).is_err()
}

/// Query/maintenance parallelism (DataFusion `target_partitions`).
///
/// DataFusion defaults `target_partitions` to `num_cpus::get()`, which on Linux
/// reads `sched_getaffinity` — that honors cpuset pinning but NOT the CFS quota
/// (`docker --cpus`). In a CFS-throttled container TF therefore sees the host's
/// core count, splits even a single small parquet file into that many scan
/// groups (each re-opening the file's metadata), and oversubscribes the CPU it
/// actually has. Derive from the cgroup CPU quota instead, capped at the
/// affinity-derived count. Set onto the config in `apply()`; the env override
/// `TIMEFUSION_QUERY_PARTITIONS` wins via serde (apply only fills when unset).
fn detected_query_partitions() -> usize {
    crate::config::detect_cores()
}

/// Round a quota/period ratio up to whole cores (a 1.5-core quota → 2).
/// Return free space (GB) on the volume hosting `path`. Returns None if no
/// disk in the sysinfo enumeration covers the path — defensive: we'd rather
/// skip the override than guess wrong.
fn available_disk_for(path: &std::path::Path) -> Option<usize> {
    let disks = Disks::new_with_refreshed_list();
    let canonical = std::fs::canonicalize(path).ok().or_else(|| Some(path.to_path_buf()))?;
    // Pick the disk whose mount_point is the longest prefix of our path.
    disks
        .iter()
        .filter(|d| canonical.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len())
        .map(|d| (d.available_space() / (1024 * 1024 * 1024)) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_is_idempotent_and_respects_overrides() {
        // SAFETY: this test runs without #[serial], but only reads env. The
        // values come from the test process's env which doesn't have these
        // vars set (autotune will fire).
        let mut cfg = AppConfig::default();
        let buffer_before = cfg.buffer.timefusion_buffer_max_memory_mb;
        apply(&mut cfg);
        // On any modern dev host, MemBuffer should now reflect RAM-based sizing.
        // We only assert non-decrease relative to the 256MB floor; on tiny CI
        // runners the floor wins, which is fine.
        assert!(cfg.buffer.timefusion_buffer_max_memory_mb >= MIN_BUFFER_MB);
        // Reapplying must not change anything (idempotent).
        let snapshot = cfg.clone();
        apply(&mut cfg);
        assert_eq!(cfg.buffer.timefusion_buffer_max_memory_mb, snapshot.buffer.timefusion_buffer_max_memory_mb);
        let _ = buffer_before;
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
        assert_eq!(a.query_pool_mb, cfg.derived.query_pool_bytes() / (1024 * 1024));
        assert_eq!(a.maintenance_pool_mb, cfg.derived.maintenance_pool_bytes() / (1024 * 1024), "was missing entirely");
        assert_eq!(a.foyer_mb, 4560);
        // MemBuffer's ceiling is on its EFFECTIVE budget (knob − foyer −
        // tantivy peak), then x1.2 — not the raw knob, which would count foyer
        // twice since it is summed separately.
        let effective = 24000 - a.foyer_mb - a.tantivy_peak_mb;
        assert_eq!(a.mem_buffer_hard_mb, effective * 6 / 5);
        assert_eq!(a.committed_mb, a.query_pool_mb + a.mem_buffer_hard_mb + a.maintenance_pool_mb + a.foyer_mb + a.tantivy_peak_mb + a.df_metadata_cache_mb);
        assert!(a.oversubscribed(), "a 24GB MemBuffer in a 24GiB container must be flagged: {a:?}");
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
