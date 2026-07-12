//! DedupExec read-side dedup benchmark (parity plan Point 3).
//!
//! Drives `DedupExec` end-to-end over an in-memory input (no S3): a
//! `MemorySourceConfig` feeds synthetic `(id, timestamp, payload)` batches
//! through the operator and we drain the resulting stream. Sweeps the axes the
//! optimization plan calls out — input size, duplicate ratio, sorted vs
//! shuffled — so every change to `read_dedup.rs` can be gated on real numbers.
//!
//! Throughput (rows/s) comes from criterion. Peak seen-set bytes and allocation
//! count come from a counting global allocator (below); run with
//! `DEDUP_MEM_REPORT=1` to print that table. The 49M-row axis is heavy (multi-GB
//! input) so it is gated behind `DEDUP_BIG=1`; the default max is 10M.
//!
//!   cargo bench --bench dedup_benchmarks
//!   DEDUP_MEM_REPORT=1 DEDUP_BIG=1 cargo bench --bench dedup_benchmarks

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    execution::TaskContext,
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use futures::StreamExt;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use timefusion::read_dedup::DedupExec;

// ── Counting allocator: peak live bytes + total alloc count ──────────────────
struct CountingAlloc;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            ALLOCS.fetch_add(1, Relaxed);
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

/// Reset counters, run `f`, return (peak_live_bytes, alloc_count) attributable
/// to it. `LIVE` isn't reset (other live allocs exist) — we track the delta in
/// PEAK by zeroing it and letting `fetch_max` climb.
fn measure<R>(f: impl FnOnce() -> R) -> (R, usize, usize) {
    let live0 = LIVE.load(Relaxed);
    PEAK.store(live0, Relaxed);
    let a0 = ALLOCS.load(Relaxed);
    let r = f();
    (r, PEAK.load(Relaxed).saturating_sub(live0), ALLOCS.load(Relaxed) - a0)
}

// ── Synthetic input ──────────────────────────────────────────────────────────
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("payload", DataType::Utf8, false), // fat body DedupExec must never touch
    ]))
}

const BATCH: usize = 8192;
const PAYLOAD: &str = "payloadpayloadpayloadpayloadpayloadpayloadpayload__"; // ~50B

/// `total` rows drawn from `distinct` unique `(id, timestamp)` keys (dup ratio =
/// 1 - distinct/total). A duplicate shares BOTH id and ts, so when `sorted` the
/// copies cluster in one timestamp run — the case the bounded-window mode
/// exploits. Chunked into `BATCH`-row batches to model streaming.
fn make_batches(total: usize, distinct: usize, sorted: bool) -> Vec<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(0xDED0_9000);
    // key i → (id_i, ts_i); ts strictly increasing in i so sort-by-ts == sort-by-key-index.
    let mut idx: Vec<usize> = (0..total).map(|r| r % distinct).collect();
    if sorted {
        idx.sort_unstable(); // group identical keys; ts increasing in key index
    } else {
        idx.shuffle(&mut rng);
    }
    let s = schema();
    idx.chunks(BATCH)
        .map(|chunk| {
            let ids: Vec<String> = chunk.iter().map(|&k| format!("id-{k:016x}")).collect();
            let ts: Vec<i64> = chunk.iter().map(|&k| 1_700_000_000_000_000 + k as i64).collect();
            let cols: Vec<ArrayRef> =
                vec![Arc::new(StringArray::from(ids)), Arc::new(TimestampMicrosecondArray::from(ts)), Arc::new(StringArray::from(vec![PAYLOAD; chunk.len()]))];
            RecordBatch::try_new(s.clone(), cols).unwrap()
        })
        .collect()
}

fn dedup_plan(batches: Vec<RecordBatch>, sorted: bool) -> Arc<dyn ExecutionPlan> {
    let s = schema();
    let src = MemorySourceConfig::try_new(&[batches], s.clone(), None).unwrap();
    // Declare the timestamp ordering so DedupExec's Tier-2 bounded-window gate
    // can fire; shuffled input declares nothing (full-set fallback).
    let src = if sorted {
        let ord = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("timestamp", 1)), Default::default())]).unwrap();
        src.try_with_sort_information(vec![ord]).unwrap()
    } else {
        src
    };
    let input: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(src)));
    Arc::new(DedupExec::new(input, vec!["id".into(), "timestamp".into()], None).unwrap())
}

/// Drain the dedup stream, return surviving row count.
async fn drain(plan: Arc<dyn ExecutionPlan>) -> usize {
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let mut rows = 0;
    while let Some(b) = stream.next().await {
        rows += b.unwrap().num_rows();
    }
    rows
}

fn sizes() -> Vec<usize> {
    if std::env::var("DEDUP_BIG").is_ok() { vec![1_000_000, 10_000_000, 49_000_000] } else { vec![1_000_000, 10_000_000] }
}
const DUP_RATIOS: [u32; 4] = [0, 50, 90, 99];

fn bench_dedup(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap();
    let mut group = c.benchmark_group("dedup");
    group.sample_size(10);

    for &total in &sizes() {
        for &ratio in &DUP_RATIOS {
            let distinct = (total * (100 - ratio) as usize / 100).max(1);
            for &sorted in &[false, true] {
                let batches = make_batches(total, distinct, sorted);
                let tag = format!("n{}m/dup{}/{}", total / 1_000_000, ratio, if sorted { "sorted" } else { "shuffled" });
                group.throughput(Throughput::Elements(total as u64));
                group.bench_with_input(BenchmarkId::from_parameter(&tag), &batches, |b, batches| {
                    b.to_async(&rt).iter(|| {
                        let plan = dedup_plan(batches.clone(), sorted);
                        async move { drain(plan).await }
                    });
                });
            }
        }
    }
    group.finish();
}

/// One-shot memory/alloc report (peak seen-set bytes, alloc count) — the metric
/// criterion can't give. Gated so `cargo bench` stays throughput-only.
fn mem_report(rt: &tokio::runtime::Runtime) {
    println!("\n=== dedup memory report (peak live bytes / alloc count) ===");
    println!("{:<28} {:>14} {:>14} {:>10}", "config", "peak_bytes", "allocs", "kept");
    for &total in &sizes() {
        for &ratio in &DUP_RATIOS {
            let distinct = (total * (100 - ratio) as usize / 100).max(1);
            for &sorted in &[false, true] {
                let batches = make_batches(total, distinct, sorted);
                let (kept, peak, allocs) = measure(|| rt.block_on(drain(dedup_plan(batches, sorted))));
                let tag = format!("n{}m/dup{}/{}", total / 1_000_000, ratio, if sorted { "s" } else { "x" });
                println!("{tag:<28} {peak:>14} {allocs:>14} {kept:>10}");
            }
        }
    }
}

fn maybe_report(c: &mut Criterion) {
    if std::env::var("DEDUP_MEM_REPORT").is_ok() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap();
        mem_report(&rt);
    }
    bench_dedup(c);
}

criterion_group!(benches, maybe_report);
criterion_main!(benches);
