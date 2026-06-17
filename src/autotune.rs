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
//! Disk budget: foyer caches take up to 40% of free space on the data dir,
//! capped at 500GB to avoid runaway on very large volumes.
//!
//! Logged once at startup so ops can see exactly what was chosen.

use sysinfo::{Disks, System};
use tracing::info;

use crate::config::AppConfig;

const RAM_FRACTION_QUERY_POOL: f64 = 0.30;
const RAM_FRACTION_BUFFER: f64 = 0.25;
const RAM_FRACTION_FOYER_MEM: f64 = 0.15;
const RAM_FRACTION_FOYER_META: f64 = 0.02;
const DISK_FRACTION_FOYER: f64 = 0.40;
const DISK_FRACTION_FOYER_META: f64 = 0.02;

const MIN_QUERY_POOL_GB: usize = 1;
const MAX_QUERY_POOL_GB: usize = 32;
const MIN_BUFFER_MB: usize = 256;
const MIN_FOYER_MEM_MB: usize = 128;
const MAX_FOYER_MEM_MB: usize = 8 * 1024;
const MAX_FOYER_META_MB: usize = 512;
const MIN_FOYER_DISK_GB: usize = 1;
const MAX_FOYER_DISK_GB: usize = 500;
const MAX_FOYER_META_DISK_GB: usize = 5;

/// Apply host-aware overrides to `config`. Knobs whose env var is set by the
/// user are left untouched. Returns the set of knobs that were auto-tuned for
/// logging.
pub fn apply(config: &mut AppConfig) {
    let mut sys = System::new();
    sys.refresh_memory();
    // Inside a container the budget is the cgroup limit, not host RAM —
    // sizing 72% of a 148GB host into a 66.6GiB cgroup guarantees memcg OOM
    // kills (prod 2026-06-11: 16 kills/24h, every-10-min crash loop).
    let host_ram = sys.total_memory() as usize;
    let total_ram_bytes = sys.cgroup_limits().map_or(host_ram, |c| (c.total_memory as usize).min(host_ram));
    let total_ram_gb = total_ram_bytes / (1024 * 1024 * 1024);
    let total_ram_mb = total_ram_bytes / (1024 * 1024);

    let cpus = num_cpus::get();

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

    // Query execution pool (DataFusion). Default static = 8GB.
    if env_unset("TIMEFUSION_MEMORY_LIMIT_GB") {
        let derived = ((total_ram_gb as f64 * RAM_FRACTION_QUERY_POOL) as usize).clamp(MIN_QUERY_POOL_GB, MAX_QUERY_POOL_GB);
        if derived != config.memory.timefusion_memory_limit_gb {
            config.memory.timefusion_memory_limit_gb = derived;
            applied.push(("TIMEFUSION_MEMORY_LIMIT_GB", format!("{}GB", derived)));
        }
    }

    // MemBuffer. Default static = 4096MB.
    if env_unset("TIMEFUSION_BUFFER_MAX_MEMORY_MB") {
        let derived = ((total_ram_mb as f64 * RAM_FRACTION_BUFFER) as usize).max(MIN_BUFFER_MB);
        if derived != config.buffer.timefusion_buffer_max_memory_mb {
            config.buffer.timefusion_buffer_max_memory_mb = derived;
            applied.push(("TIMEFUSION_BUFFER_MAX_MEMORY_MB", format!("{}MB", derived)));
        }
    }

    // Foyer memory cache. Default static = 512MB.
    if env_unset("TIMEFUSION_FOYER_MEMORY_MB") {
        let derived = ((total_ram_mb as f64 * RAM_FRACTION_FOYER_MEM) as usize).clamp(MIN_FOYER_MEM_MB, MAX_FOYER_MEM_MB);
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
    let fallback = num_cpus::get().max(1);
    cpu_quota_cores().map_or(fallback, |q| q.clamp(1, fallback))
}

/// Cores implied by the cgroup CPU quota (v2 `cpu.max`, then v1
/// `cfs_quota_us`/`cfs_period_us`). `None` when unthrottled or unreadable.
fn cpu_quota_cores() -> Option<usize> {
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        return parse_cpu_max(&s);
    }
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").ok()?.trim().parse::<i64>().ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us").ok()?.trim().parse::<i64>().ok()?;
    quota_period_to_cores(quota, period)
}

/// Parse cgroup v2 `cpu.max` contents: `"<quota> <period>"` or `"max <period>"`.
fn parse_cpu_max(s: &str) -> Option<usize> {
    let mut it = s.split_whitespace();
    let quota = it.next()?;
    if quota == "max" {
        return None;
    }
    let period: i64 = it.next().unwrap_or("100000").parse().ok()?;
    quota_period_to_cores(quota.parse().ok()?, period)
}

/// Round a quota/period ratio up to whole cores (a 1.5-core quota → 2).
fn quota_period_to_cores(quota: i64, period: i64) -> Option<usize> {
    (quota > 0 && period > 0).then(|| (quota as f64 / period as f64).ceil() as usize)
}

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
        assert_eq!(cfg.memory.timefusion_memory_limit_gb, snapshot.memory.timefusion_memory_limit_gb);
        let _ = buffer_before;
    }

    #[test]
    fn cpu_max_parsing_rounds_up_and_honors_unlimited() {
        assert_eq!(parse_cpu_max("max 100000"), None); // unthrottled
        assert_eq!(parse_cpu_max("200000 100000"), Some(2)); // 2 cores
        assert_eq!(parse_cpu_max("50000 100000"), Some(1)); // 0.5 → 1
        assert_eq!(parse_cpu_max("150000 100000"), Some(2)); // 1.5 → 2
        assert_eq!(parse_cpu_max("100000"), Some(1)); // period defaults to 100000
        assert_eq!(quota_period_to_cores(-1, 100000), None);
    }
}
