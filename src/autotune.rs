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
use tracing::{info, warn};

use crate::config::AppConfig;

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
        if config.tantivy.indexed_tables().is_empty() { 0 } else { crate::tantivy_index::builder::WRITER_HEAP_BYTES * config.buffer.flush_parallelism() / MB };
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
mod tests {
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
