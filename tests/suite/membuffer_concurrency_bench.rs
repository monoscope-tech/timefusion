//! Microbenchmarks: concurrent insert+query against MemBuffer in-process,
//! bypassing TF startup, pgwire and MinIO. Use a release build for real numbers.

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

/// Reads a `BENCH_*` knob from the environment, falling back to `default`.
fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64
}

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
    let projects: usize = env_or("BENCH_PROJECTS", 300);
    let readers: usize = env_or("BENCH_READERS", 75);
    let duration_s: u64 = env_or("BENCH_DURATION", 20);
    let batch_size: usize = env_or("BENCH_BATCH", 30);
    let writer_rate: f64 = env_or("BENCH_WRITER_RATE", 5.0);

    println!("\n=== membuffer microbench ===");
    println!("projects={projects} readers={readers} duration={duration_s}s batch={batch_size} writer_rate={writer_rate}/s");

    let buf = Arc::new(MemBuffer::new());
    let schema = schema();
    let stop = Arc::new(AtomicBool::new(false));
    let now_micros = now_micros();

    let inserts = Arc::new(AtomicU64::new(0));
    let per_batch_sleep = if writer_rate > 0.0 { Duration::from_secs_f64(batch_size as f64 / writer_rate) } else { Duration::ZERO };
    let writer_handles: Vec<_> = (0..projects)
        .map(|p| {
            let (buf, stop, schema, inserts) = (buf.clone(), stop.clone(), schema.clone(), inserts.clone());
            let pid = format!("proj-{p:04}");
            std::thread::spawn(move || {
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
            })
        })
        .collect();

    let lat = Arc::new(parking_lot::Mutex::new(Vec::<u64>::with_capacity(1_000_000)));
    let reader_handles: Vec<_> = (0..readers)
        .map(|r| {
            let (buf, stop, lat) = (buf.clone(), stop.clone(), lat.clone());
            std::thread::spawn(move || {
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
            })
        })
        .collect();

    std::thread::sleep(Duration::from_secs(duration_s));
    stop.store(true, Ordering::Relaxed);
    for h in writer_handles.into_iter().chain(reader_handles) {
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
