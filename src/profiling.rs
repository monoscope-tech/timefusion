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

    /// Directory for profiling artifacts. MUST equal the parent of the baked
    /// jemalloc `prof_prefix` in main.rs (`<data_dir>/profiles`) so the heap
    /// dumps land in a dir we create — jemalloc does NOT mkdir its prefix, and
    /// the earlier doubled `timefusion/timefusion/profiles` meant the prefix
    /// dir never existed and every .heap silently failed.
    fn profiles_dir(data_dir: &std::path::Path) -> PathBuf {
        data_dir.join("profiles")
    }

    /// Start background profiling. Heap profiling is already active via the
    /// baked `malloc_conf`; here we (1) ensure the artifact dir exists and
    /// (2) spawn the rolling CPU flamegraph sampler. Safe to call once at boot.
    pub fn start(data_dir: PathBuf) {
        let dir = profiles_dir(&data_dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("profiling: cannot create {:?}: {} — CPU flamegraphs disabled, heap dumps still land at malloc_conf prof_prefix", dir, e);
        }
        info!("profiling: enabled (jemalloc heap auto-dump + rolling CPU flamegraph) → {:?}", dir);
        spawn_cpu_sampler(dir);
    }

    /// One CPU profile window at a time on a dedicated OS thread: build a
    /// guard, sample for `WINDOW`, write a flamegraph, drop, repeat. A fresh
    /// guard per window keeps each SVG scoped to a recent interval (so the
    /// window overlapping an OOM isn't diluted by minutes of prior samples).
    fn spawn_cpu_sampler(dir: PathBuf) {
        const HZ: i32 = 99; // 99Hz: cheap, avoids lock-step with periodic timers
        const WINDOW: Duration = Duration::from_secs(60);
        std::thread::Builder::new()
            .name("cpu-profiler".into())
            .spawn(move || {
                let mut seq: u64 = 0;
                loop {
                    let guard = match pprof::ProfilerGuardBuilder::default().frequency(HZ).blocklist(&["libc", "libgcc", "pthread", "vdso"]).build() {
                        Ok(g) => g,
                        Err(e) => {
                            warn!("profiling: cpu guard build failed: {} — retrying in {:?}", e, WINDOW);
                            std::thread::sleep(WINDOW);
                            continue;
                        }
                    };
                    std::thread::sleep(WINDOW);
                    match guard.report().build() {
                        Ok(report) => {
                            // Rolling window of the last few files (timestamped
                            // via seq) so we keep the ones straddling an OOM
                            // without unbounded growth — prune to the newest 10.
                            let path = dir.join(format!("cpu-{:06}.svg", seq));
                            match std::fs::File::create(&path).map_err(|e| e.to_string()).and_then(|f| report.flamegraph(f).map_err(|e| e.to_string())) {
                                Ok(()) => {}
                                Err(e) => warn!("profiling: writing cpu flamegraph {:?} failed: {}", path, e),
                            }
                            prune_old(&dir, "cpu-", 10);
                            // jemalloc auto-dumps a .heap every ~8GiB allocated
                            // (lg_prof_interval:33) and never prunes them — left
                            // alone they grow unbounded (95GB / 42k files seen in
                            // prod). Cap to the newest 50 here on the same cadence.
                            prune_old(&dir, "jeprof", 50);
                            seq += 1;
                        }
                        Err(e) => warn!("profiling: cpu report build failed: {}", e),
                    }
                }
            })
            .expect("spawn cpu-profiler thread");
    }

    /// Keep only the newest `keep` files whose name starts with `prefix`,
    /// ordered by mtime — NOT filename. The CPU seq counter resets to 0 on every
    /// process restart, so a dead process's high-seq files (`cpu-000902`) would
    /// outsort a live process's fresh low-seq files (`cpu-000003`) by name and
    /// survive pruning forever, leaving us blind to the current run's CPU. mtime
    /// is monotonic across restarts, so newest-by-mtime always keeps the live
    /// process's files and evicts the stale ones.
    fn prune_old(dir: &std::path::Path, prefix: &str, keep: usize) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        let mut files: Vec<(std::time::SystemTime, PathBuf)> = rd
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(prefix)))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        if files.len() <= keep {
            return;
        }
        files.sort_by_key(|(mtime, _)| *mtime);
        for (_, old) in &files[..files.len() - keep] {
            let _ = std::fs::remove_file(old);
        }
    }
}

/// Start profiling if the `profiling` feature is on (Linux). No-op otherwise —
/// callers wire it unconditionally at boot.
#[cfg(all(feature = "profiling", target_os = "linux"))]
pub fn start(data_dir: std::path::PathBuf) {
    imp::start(data_dir);
}

#[cfg(not(all(feature = "profiling", target_os = "linux")))]
pub fn start(_data_dir: std::path::PathBuf) {}
