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

    /// Directory (under the data dir) for profiling artifacts. Created on init.
    fn profiles_dir(data_dir: &std::path::Path) -> PathBuf {
        data_dir.join("timefusion").join("profiles")
    }

    /// Start background profiling. Heap profiling is already active via the
    /// baked `malloc_conf`; here we (1) ensure the artifact dir exists and
    /// (2) spawn the rolling CPU flamegraph sampler. Safe to call once at boot.
    pub fn start(data_dir: PathBuf) {
        let dir = profiles_dir(&data_dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(
                "profiling: cannot create {:?}: {} — CPU flamegraphs disabled, heap dumps still land at malloc_conf prof_prefix",
                dir, e
            );
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
                            seq += 1;
                        }
                        Err(e) => warn!("profiling: cpu report build failed: {}", e),
                    }
                }
            })
            .expect("spawn cpu-profiler thread");
    }

    /// Keep only the newest `keep` files whose name starts with `prefix`.
    fn prune_old(dir: &std::path::Path, prefix: &str, keep: usize) {
        let Ok(rd) = std::fs::read_dir(dir) else { return };
        let mut files: Vec<PathBuf> = rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(prefix)))
            .collect();
        if files.len() <= keep {
            return;
        }
        files.sort();
        for old in &files[..files.len() - keep] {
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
