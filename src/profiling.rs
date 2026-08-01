//! Production heap + CPU profiling, compiled only under `--features profiling`
//! (Linux-only deps; a default or macOS build sees an empty module).
//!
//! Why baked-in rather than attached at runtime: the CapRover host is
//! strictly read-only for us (no `perf`, no `MALLOC_CONF` env change, no
//! `exec`/signal into the container). So the binary self-instruments and
//! writes artifacts into the data-dir volume, which we CAN read off the host
//! (`/var/lib/docker/volumes/…/_data/timefusion/profiles`).
//!
//! Heap: jemalloc's own profiler (`prof:true`), configured via the baked
//! `malloc_conf` symbol in `main.rs`, auto-dumps a `.heap` every
//! `lg_prof_interval` bytes allocated — so as RSS climbs toward the 89GB
//! cgroup kill, the last dumps before each OOM show the allocation call
//! stacks. Analyze off-host with `jeprof --svg <binary> jeprof.*.heap`.
//!
//! CPU: a `pprof` sampling profiler writes a rolling flamegraph SVG every
//! `interval`, capturing what's hot while memory grows.

#[cfg(all(feature = "profiling", target_os = "linux"))]
mod imp {
    use std::{path::PathBuf, time::Duration};

    use tracing::{info, warn};

    /// Start background profiling. Heap profiling is already active via the
    /// baked `malloc_conf`; here we (1) ensure the artifact dir exists and
    /// (2) spawn the rolling CPU flamegraph sampler. Safe to call once at boot.
    pub fn start(data_dir: PathBuf) {
        // MUST equal the parent of the baked jemalloc `prof_prefix` in main.rs
        // so the heap dumps land in a dir we create — jemalloc does NOT mkdir
        // its prefix, and the earlier doubled `timefusion/timefusion/profiles`
        // meant the prefix dir never existed and every .heap silently failed.
        let dir = data_dir.join("profiles");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("profiling: cannot create {dir:?}: {e} — CPU flamegraphs disabled, heap dumps still land at malloc_conf prof_prefix");
        }
        info!("profiling: enabled (jemalloc heap auto-dump + rolling CPU flamegraph) → {dir:?}");
        spawn_cpu_sampler(dir);
    }

    /// One CPU profile window at a time on a dedicated OS thread: build a
    /// guard, sample for `WINDOW`, write a flamegraph, drop, repeat. A fresh
    /// guard per window keeps each SVG scoped to a recent interval (so the
    /// window overlapping an OOM isn't diluted by minutes of prior samples).
    fn spawn_cpu_sampler(dir: PathBuf) {
        const HZ: i32 = 99; // 99Hz: cheap, avoids lock-step with periodic timers
        const WINDOW: Duration = Duration::from_secs(60);
        const KEEP_CPU: usize = 10;
        const KEEP_HEAP: usize = 50;
        std::thread::Builder::new()
            .name("cpu-profiler".into())
            .spawn(move || {
                let mut seq: u64 = 0; // `mut`: advances per completed window only, so it can't be an iterator counter
                loop {
                    let Ok(guard) = pprof::ProfilerGuardBuilder::default()
                        .frequency(HZ)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .inspect_err(|e| warn!("profiling: cpu guard build failed: {e} — retrying in {WINDOW:?}"))
                    else {
                        std::thread::sleep(WINDOW);
                        continue;
                    };
                    std::thread::sleep(WINDOW);
                    let Ok(report) = guard.report().build().inspect_err(|e| warn!("profiling: cpu report build failed: {e}")) else {
                        continue;
                    };
                    let path = dir.join(format!("cpu-{seq:06}.svg"));
                    if let Err(e) = write_flamegraph(&path, &report) {
                        warn!("profiling: writing cpu flamegraph {path:?} failed: {e}");
                    }
                    // Rolling windows: keep the ones straddling an OOM without
                    // unbounded growth. jemalloc auto-dumps a .heap every ~8GiB
                    // allocated (lg_prof_interval:33) and never prunes them —
                    // left alone they grow unbounded (95GB / 42k files in prod).
                    prune_old(&dir, "cpu-", KEEP_CPU);
                    prune_old(&dir, "jeprof", KEEP_HEAP);
                    seq += 1;
                }
            })
            .expect("spawn cpu-profiler thread");
    }

    fn write_flamegraph(path: &std::path::Path, report: &pprof::Report) -> anyhow::Result<()> {
        report.flamegraph(std::fs::File::create(path)?)?;
        Ok(())
    }

    /// Keep only the newest `keep` files whose name starts with `prefix`,
    /// ordered by mtime — NOT filename. The CPU seq counter resets to 0 on every
    /// process restart, so a dead process's high-seq files (`cpu-000902`) would
    /// outsort a live process's fresh low-seq files (`cpu-000003`) by name and
    /// survive pruning forever, leaving us blind to the current run's CPU. mtime
    /// is monotonic across restarts, so newest-by-mtime always keeps the live
    /// process's files and evicts the stale ones.
    fn prune_old(dir: &std::path::Path, prefix: &str, keep: usize) {
        let mut files: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(prefix)))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        files.sort_unstable_by_key(|(mtime, _)| std::cmp::Reverse(*mtime)); // newest first; `mut`: std sorts in place
        files.into_iter().skip(keep).for_each(|(_, old)| {
            let _ = std::fs::remove_file(old);
        });
    }
}

#[cfg(all(feature = "profiling", target_os = "linux"))]
pub use imp::start;

/// No-op without the `profiling` feature (Linux) — callers wire it unconditionally at boot.
#[cfg(not(all(feature = "profiling", target_os = "linux")))]
pub fn start(_data_dir: std::path::PathBuf) {}
