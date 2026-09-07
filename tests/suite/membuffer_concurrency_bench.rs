//! Microbenchmark: concurrent insert+query against MemBuffer in-process.
//!
//! Bypasses TF startup, pgwire, MinIO. Iterates an order of magnitude faster
//! than `bench/concurrent_load.py`. Use to find the contention point in
//! MemBuffer itself before paying release-build time.
//!
//! Run: `cargo test --release --test membuffer_concurrency_bench -- --nocapture`
//! Or for fast iteration: `cargo test --test membuffer_concurrency_bench -- --nocapture`

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arrow::{
    array::{Int64Array, StringViewArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use timefusion::write::mem_buffer::MemBuffer;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8View, false),
    ]))
}

fn batch(schema: Arc<Schema>, base_ts: i64, n: usize) -> RecordBatch {
    let ts: Vec<i64> = (0..n as i64).map(|i| base_ts + i).collect();
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<String> = (0..n).map(|i| format!("row-{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")), Arc::new(Int64Array::from(ids)), Arc::new(StringViewArray::from(name_refs))],
    )
    .unwrap()
}

/// Ingest-dedup overhead bench: dedup-on vs dedup-off arms, ALTERNATED
/// (A/B/A/B…, so both see the same machine state), against an index PRELOADED
/// with ~20M entries — an empty DashMap is artificially fast. The on-arm runs
/// the exact production probe (`ingest_dedup_filter_batch`) on realistic otel
/// batches before each MemBuffer insert; incoming keys are fresh, so it
/// exercises the steady-state stage-1 path (~0% key hits), like prod. Also
/// prices the key-hit (stage-2) path and the flush-time populate separately.
///
/// Run: `BENCH_DURATION=3 cargo nextest run --release ingest_dedup_insert_overhead_bench --run-ignored all --no-capture`
#[test]
#[ignore = "microbench: opt-in via --run-ignored; use a release build for real numbers"]
fn ingest_dedup_insert_overhead_bench() {
    use timefusion::write::{IngestDedupIndex, ingest_dedup_filter_batch, ingest_identity_idxs, per_row_identities};
    let preload: usize = std::env::var("BENCH_PRELOAD").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000_000);
    let writers: usize = std::env::var("BENCH_WRITERS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let slice_s: u64 = std::env::var("BENCH_DURATION").ok().and_then(|s| s.parse().ok()).unwrap_or(3);
    let slices: usize = std::env::var("BENCH_SLICES").ok().and_then(|s| s.parse().ok()).unwrap_or(6); // 3 per arm, alternated
    let batch_rows: usize = 128;
    let table = "otel_logs_and_spans";

    // Realistic otel batches (the real ~90-column schema), distinct ids per
    // batch so probe keys are fresh — the steady state.
    let now_micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
    let mk_batch = |tag: &str, b: usize| {
        timefusion::write::mem_buffer::compact_batch(
            timefusion::support::test_helpers::json_to_batch(
                (0..batch_rows)
                    .map(|r| {
                        timefusion::support::test_helpers::test_span_ts(
                            &format!("{tag}-{b}-{r}"),
                            "bench-span",
                            "bench-proj",
                            now_micros + (b * batch_rows + r) as i64,
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        )
    };
    let pool: Vec<_> = (0..32).map(|b| mk_batch("fresh", b)).collect();
    let (key_idxs, content_idxs) = ingest_identity_idxs(table, &pool[0].schema()).expect("otel schema has dedup keys");

    let idx = Arc::new(IngestDedupIndex::new(usize::MAX / 4, i64::MAX / 4, 0)); // never rotate: hold the full preload
    let t0 = Instant::now();
    (0..preload).for_each(|i| idx.populate((i as u128) << 64 | 0xfeed, i as u128));
    println!("\n=== ingest-dedup overhead bench ===");
    println!("preloaded {preload} index entries in {:?}; writers={writers} slices={slices}x{slice_s}s batch={batch_rows} rows", t0.elapsed());

    // Alternated arms over a fresh MemBuffer per slice (so neither arm inherits
    // the other's bucket growth). Writers record PER-INSERT latency.
    let mut arm_lat: [Vec<u64>; 2] = [Vec::new(), Vec::new()];
    let mut arm_inserts: [u64; 2] = [0, 0];
    let mut arm_secs: [f64; 2] = [0.0, 0.0];
    for slice in 0..slices {
        let on = slice % 2 == 1; // A=off, B=on
        let buf = Arc::new(MemBuffer::new());
        let stop = Arc::new(AtomicBool::new(false));
        let handles: Vec<_> = (0..writers)
            .map(|w| {
                let (buf, stop, idx, pool) = (buf.clone(), stop.clone(), idx.clone(), pool.clone());
                std::thread::spawn(move || {
                    let mut lat = Vec::with_capacity(200_000);
                    let mut i = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let b = pool[(w + i) % pool.len()].clone();
                        let ts = now_micros + i as i64;
                        let t = Instant::now();
                        let b = if on {
                            let (kept, _, _) = ingest_dedup_filter_batch(&idx, "otel_logs_and_spans", b);
                            kept.expect("fresh keys are never dropped")
                        } else {
                            b
                        };
                        buf.insert(&format!("proj-{w:02}"), "otel_logs_and_spans", b, ts).unwrap();
                        lat.push(t.elapsed().as_nanos() as u64);
                        i += 1;
                    }
                    lat
                })
            })
            .collect();
        let ts = Instant::now();
        std::thread::sleep(Duration::from_secs(slice_s));
        stop.store(true, Ordering::Relaxed);
        let secs = ts.elapsed().as_secs_f64();
        for h in handles {
            let lat = h.join().unwrap();
            arm_inserts[on as usize] += lat.len() as u64;
            arm_lat[on as usize].extend(lat);
        }
        arm_secs[on as usize] += secs;
    }

    let stats = |lat: &mut Vec<u64>| {
        lat.sort_unstable();
        (percentile(lat, 0.50), percentile(lat, 0.95), percentile(lat, 0.99))
    };
    let (off_p50, off_p95, off_p99) = stats(&mut arm_lat[0]);
    let (on_p50, on_p95, on_p99) = stats(&mut arm_lat[1]);
    let thr = |i: usize| arm_inserts[i] as f64 * batch_rows as f64 / arm_secs[i];
    println!("OFF: p50={off_p50}ns p95={off_p95}ns p99={off_p99}ns  rows/s={:.0}", thr(0));
    println!("ON:  p50={on_p50}ns p95={on_p95}ns p99={on_p99}ns  rows/s={:.0}", thr(1));
    let pct = |on: u64, off: u64| (on as f64 - off as f64) / off as f64 * 100.0;
    println!(
        "delta: p50={:+.1}% p95={:+.1}% p99={:+.1}% throughput={:+.1}%  probe={:.0}ns/row at p50",
        pct(on_p50, off_p50),
        pct(on_p95, off_p95),
        pct(on_p99, off_p99),
        (thr(1) - thr(0)) / thr(0) * 100.0,
        (on_p50.saturating_sub(off_p50)) as f64 / batch_rows as f64
    );

    // Stage-2 (key-hit) path: batches whose keys ARE in the index with
    // different content — worst-case version traffic (100% key hits).
    let hit_pool: Vec<_> = (0..8).map(|b| mk_batch("hit", b)).collect();
    for b in &hit_pool {
        for (k, _) in per_row_identities(b, &key_idxs, &content_idxs).unwrap() {
            idx.populate(k, 0xdead); // key present, content differs => stage 2 runs, nothing drops
        }
    }
    let t = Instant::now();
    let reps = 200;
    for i in 0..reps {
        let (kept, hits, dropped) = ingest_dedup_filter_batch(&idx, table, hit_pool[i % hit_pool.len()].clone());
        assert_eq!((kept.unwrap().num_rows(), hits as usize, dropped), (batch_rows, batch_rows, 0));
    }
    println!("stage-2 path (100% key hits, worst case): {:.0}ns/row", t.elapsed().as_nanos() as f64 / (reps * batch_rows) as f64);

    // Flush-time populate cost (off the ack path): full two-hash identity.
    let t = Instant::now();
    for i in 0..reps {
        let _ = per_row_identities(&pool[i % pool.len()], &key_idxs, &content_idxs).unwrap();
    }
    println!("populate identity (flush-time, off ack path): {:.0}ns/row", t.elapsed().as_nanos() as f64 / (reps * batch_rows) as f64);

    // REAL-PATH context: the production insert includes a WAL append with
    // fsync-per-append (`sync_each`, prod default) — the microbench above
    // deliberately excludes it. Measure the full `layer.insert` per-row cost
    // (probe included; it is always-on) so the probe's share of the real ack
    // path can be stated, not guessed.
    let cfg = timefusion::support::test_helpers::TestConfigBuilder::new("dedup_bench").build();
    let layer = Arc::new(timefusion::support::test_helpers::test_layer(cfg).unwrap());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut real = Vec::with_capacity(reps);
    rt.block_on(async {
        for i in 0..reps {
            let b = pool[i % pool.len()].clone();
            let t = Instant::now();
            layer.insert("bench-proj", table, vec![b]).await.unwrap();
            real.push(t.elapsed().as_nanos() as u64);
        }
    });
    real.sort_unstable();
    let (rp50, rp99) = (percentile(&real, 0.50), percentile(&real, 0.99));
    println!(
        "real path (WAL fsync incl, probe always-on): p50={:.0}µs p99={:.0}µs per {batch_rows}-row batch => probe share of p50 ≈ {:.2}%",
        rp50 as f64 / 1000.0,
        rp99 as f64 / 1000.0,
        (on_p50.saturating_sub(off_p50)) as f64 / rp50 as f64 * 100.0
    );
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p).min((sorted.len() - 1) as f64) as usize;
    sorted[idx]
}

#[test]
#[ignore = "long-running microbench: default BENCH_DURATION=20s × 300 writers / 75 readers — opt-in via `cargo test -- --ignored concurrent_insert_query_bench` or override env (BENCH_DURATION=5)"]
fn concurrent_insert_query_bench() {
    let projects: usize = std::env::var("BENCH_PROJECTS").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let readers: usize = std::env::var("BENCH_READERS").ok().and_then(|s| s.parse().ok()).unwrap_or(75);
    let duration_s: u64 = std::env::var("BENCH_DURATION").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let batch_size: usize = std::env::var("BENCH_BATCH").ok().and_then(|s| s.parse().ok()).unwrap_or(30);
    let writer_rate: f64 = std::env::var("BENCH_WRITER_RATE").ok().and_then(|s| s.parse().ok()).unwrap_or(5.0);

    println!("\n=== membuffer microbench ===");
    println!("projects={projects} readers={readers} duration={duration_s}s batch={batch_size} writer_rate={writer_rate}/s");

    let buf = Arc::new(MemBuffer::new());
    let schema = schema();
    let stop = Arc::new(AtomicBool::new(false));
    let now_micros = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;

    let inserts = Arc::new(AtomicU64::new(0));
    let mut writer_handles = vec![];
    for p in 0..projects {
        let buf = buf.clone();
        let stop = stop.clone();
        let schema = schema.clone();
        let inserts = inserts.clone();
        let pid = format!("proj-{p:04}");
        let per_batch_sleep = if writer_rate > 0.0 { Duration::from_secs_f64(batch_size as f64 / writer_rate) } else { Duration::ZERO };
        writer_handles.push(std::thread::spawn(move || {
            let mut next = Instant::now();
            let mut i: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let ts = now_micros + i * 1_000;
                let b = batch(schema.clone(), ts, batch_size);
                buf.insert(&pid, "otel", b, ts).unwrap();
                inserts.fetch_add(batch_size as u64, Ordering::Relaxed);
                i += 1;
                if per_batch_sleep > Duration::ZERO {
                    next += per_batch_sleep;
                    let now = Instant::now();
                    if next > now {
                        std::thread::sleep(next - now);
                    } else {
                        next = now;
                    }
                }
            }
        }));
    }

    let lat = Arc::new(parking_lot::Mutex::new(Vec::<u64>::with_capacity(1_000_000)));
    let mut reader_handles = vec![];
    for r in 0..readers {
        let buf = buf.clone();
        let stop = stop.clone();
        let lat = lat.clone();
        reader_handles.push(std::thread::spawn(move || {
            let mut local: Vec<u64> = Vec::with_capacity(50_000);
            let mut rng_state: u64 = (r as u64).wrapping_mul(0x9E3779B97F4A7C15);
            while !stop.load(Ordering::Relaxed) {
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                let pid_idx = (rng_state as usize) % projects;
                let pid = format!("proj-{pid_idx:04}");
                let t0 = Instant::now();
                let _ = buf.query(&pid, "otel", &[]).unwrap();
                local.push(t0.elapsed().as_micros() as u64);
            }
            lat.lock().extend(local);
        }));
    }

    std::thread::sleep(Duration::from_secs(duration_s));
    stop.store(true, Ordering::Relaxed);
    for h in writer_handles {
        h.join().unwrap();
    }
    for h in reader_handles {
        h.join().unwrap();
    }

    let mut latencies = lat.lock().clone();
    latencies.sort_unstable();
    let n = latencies.len();
    let p50 = percentile(&latencies, 0.50) as f64 / 1000.0;
    let p95 = percentile(&latencies, 0.95) as f64 / 1000.0;
    let p99 = percentile(&latencies, 0.99) as f64 / 1000.0;
    let max = latencies.last().copied().unwrap_or(0) as f64 / 1000.0;
    let ins = inserts.load(Ordering::Relaxed);
    println!("inserts={ins} ({:.0}/s)  reads={n} ({:.0}/s)", ins as f64 / duration_s as f64, n as f64 / duration_s as f64);
    println!("read lat: p50={p50:.2}ms  p95={p95:.2}ms  p99={p99:.2}ms  max={max:.2}ms");
}
