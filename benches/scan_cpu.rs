//! CPU-per-row of the wide-OTel scan path, swept over `batch_size`.
//!
//! Prod profiling (2026-09-15, perf on the live process) showed no hot kernel:
//! the top symbol was 3%, with the mass spread across Arrow array
//! construction, allocation, hashing and per-batch stream overhead — the
//! signature of per-batch fixed costs. Prod runs `batch_size = 2048`
//! (`WIDE_ROW_DECODE_BATCH_SIZE`), a quarter of DataFusion's default, chosen
//! because the parquet decode buffer is not pool-accounted. This bench prices
//! the CPU side of that trade so the knob can be judged with numbers on both
//! sides; the counting allocator reports the memory side.
//!
//!   cargo bench --bench scan_cpu

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::prelude::{SessionConfig, SessionContext};
use serde_json::json;
use timefusion::support::test_helpers::json_to_batch_for;

struct CountingAlloc;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Relaxed) + layout.size();
            PEAK.fetch_max(live, Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static A: CountingAlloc = CountingAlloc;

const TABLE: &str = "otel_logs_and_spans";
const ROWS: usize = 131_072;
const GEN_CHUNK: usize = 8_192;

/// Write `ROWS` of realistic wide-schema data to one zstd parquet file
/// (matching prod's on-disk compression), return its path.
fn build_parquet(dir: &std::path::Path) -> std::path::PathBuf {
    use datafusion::parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let names = ["GET /api/v1/users", "POST /api/v1/events", "worker.tick", "db.query", "cache.get"];
    let path = dir.join("scan_cpu.parquet");
    let mut writer: Option<ArrowWriter<std::fs::File>> = None;
    let mut ts = 1_757_900_000_000_000i64;
    for chunk_start in (0..ROWS).step_by(GEN_CHUNK) {
        let records: Vec<serde_json::Value> = (chunk_start..(chunk_start + GEN_CHUNK).min(ROWS))
            .map(|i| {
                ts += 3_000; // ~3ms apart
                json!({
                    "project_id": "bench-project",
                    "id": format!("{i:032x}"),
                    "timestamp": ts,
                    "observed_timestamp": ts,
                    "name": names[i % names.len()],
                    "level": levels[i % levels.len()],
                    "status_code": if i % 17 == 0 { "ERROR" } else { "OK" },
                    "duration": (i % 5_000) as i64 * 1_000,
                    "hashes": [format!("h{:04}", i % 512)],
                    "summary": [format!("span {} finished with {} attributes in partition {}", i, i % 40, i % 8)],
                    "date": "2026-09-15",
                })
            })
            .collect();
        let batch = json_to_batch_for(TABLE, records).expect("batch");
        let w = writer.get_or_insert_with(|| {
            let props = WriterProperties::builder().set_compression(Compression::ZSTD(Default::default())).build();
            ArrowWriter::try_new(std::fs::File::create(&path).expect("create"), batch.schema(), Some(props)).expect("writer")
        });
        w.write(&batch).expect("write");
    }
    writer.expect("rows written").close().expect("close");
    path
}

fn ctx_with_batch_size(batch_size: usize) -> SessionContext {
    // The prod execution knobs that shape per-batch work; everything else default.
    let cfg = SessionConfig::new()
        .set_str("datafusion.execution.batch_size", &batch_size.to_string())
        .set_str("datafusion.execution.coalesce_batches", "true")
        .set_str("datafusion.execution.target_partitions", "8")
        .set_str("datafusion.execution.parquet.pushdown_filters", "true")
        .set_str("datafusion.execution.parquet.reorder_filters", "true")
        .set_str("datafusion.execution.parquet.enable_page_index", "true");
    SessionContext::new_with_config(cfg)
}

fn bench_scans(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(8).enable_all().build().unwrap();
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = build_parquet(dir.path());
    let url = parquet.to_string_lossy().to_string();

    // (label, sql) — the three shapes the prod profile is made of.
    let queries: [(&str, &str); 3] = [
        ("filter_count", "SELECT count(*) FROM t WHERE level = 'ERROR' AND duration > 2000000"),
        ("dashboard_topk", "SELECT id, name, level, status_code, duration FROM t WHERE project_id = 'bench-project' ORDER BY timestamp DESC LIMIT 1000"),
        ("group_agg", "SELECT level, count(*), sum(duration), max(duration) FROM t GROUP BY level"),
    ];

    for (label, sql) in queries {
        let mut g = c.benchmark_group(format!("scan/{label}"));
        g.throughput(Throughput::Elements(ROWS as u64));
        g.sample_size(20);
        for batch_size in [2048usize, 4096, 8192, 16384] {
            g.bench_with_input(BenchmarkId::from_parameter(batch_size), &batch_size, |b, &bs| {
                let ctx = ctx_with_batch_size(bs);
                rt.block_on(ctx.register_parquet("t", &url, Default::default())).expect("register");
                // One warm run, then reset the peak so it reflects steady state.
                rt.block_on(async { ctx.sql(sql).await.unwrap().collect().await.unwrap() });
                PEAK.store(LIVE.load(Relaxed), Relaxed);
                b.iter(|| rt.block_on(async { ctx.sql(sql).await.unwrap().collect().await.unwrap() }));
                println!("  [mem] {label}/{bs}: peak_over_live = {:.1} MB", (PEAK.load(Relaxed) - LIVE.load(Relaxed)) as f64 / 1e6);
            });
        }
        g.finish();
    }
}

criterion_group!(benches, bench_scans);
criterion_main!(benches);
