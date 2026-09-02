//! `DedupExec` read-side dedup benchmark.
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
        array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    },
    execution::TaskContext,
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::ExecutionPlan,
};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use futures::StreamExt;
use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};
use timefusion::read::DedupExec;

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

/// `LIVE` includes unrelated allocations, so measure peak growth from its baseline.
fn measure<R>(f: impl FnOnce() -> R) -> (R, usize, usize) {
    let live0 = LIVE.load(Relaxed);
    PEAK.store(live0, Relaxed);
    let a0 = ALLOCS.load(Relaxed);
    let r = f();
    (r, PEAK.load(Relaxed).saturating_sub(live0), ALLOCS.load(Relaxed) - a0)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("version", DataType::Int64, false),
        // Keep payloads large to expose accidental reads by `DedupExec`.
        Field::new("payload", DataType::Utf8, false),
    ]))
}

const BATCH: usize = 8192;
const PAYLOAD: &str = "payloadpayloadpayloadpayloadpayloadpayloadpayload__";

fn make_batches(total: usize, distinct: usize, sorted: bool) -> Vec<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(0xDED0_9000);
    let mut idx: Vec<usize> = (0..total).map(|r| r % distinct).collect();
    if sorted {
        // Sorted duplicates exercise the bounded-window path.
        idx.sort_unstable();
    } else {
        idx.shuffle(&mut rng);
    }
    let s = schema();
    idx.chunks(BATCH)
        .map(|chunk| {
            let ids: Vec<String> = chunk.iter().map(|&k| format!("id-{k:016x}")).collect();
            let ts: Vec<i64> = chunk.iter().map(|&k| 1_700_000_000_000_000 + k as i64).collect();
            let versions: Vec<i64> = chunk.iter().enumerate().map(|(row, &k)| (row as i64) ^ (k as i64)).collect();
            let cols: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(TimestampMicrosecondArray::from(ts)),
                Arc::new(Int64Array::from(versions)),
                Arc::new(StringArray::from(vec![PAYLOAD; chunk.len()])),
            ];
            RecordBatch::try_new(s.clone(), cols).unwrap()
        })
        .collect()
}

fn dedup_plan(batches: Vec<RecordBatch>, sorted: bool, keep_greatest: bool) -> Arc<dyn ExecutionPlan> {
    let src = MemorySourceConfig::try_new(&[batches], schema(), None).unwrap();
    let src = if sorted {
        // Ordered input enables DedupExec's bounded-window path.
        let ord = LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("timestamp", 1)), Default::default())]).unwrap();
        src.try_with_sort_information(vec![ord]).unwrap()
    } else {
        src
    };
    Arc::new(
        DedupExec::with_tiebreak(
            Arc::new(DataSourceExec::new(Arc::new(src))),
            vec!["id".into(), "timestamp".into()],
            keep_greatest.then_some("version".into()),
            None,
        )
        .unwrap(),
    )
}

async fn drain(plan: Arc<dyn ExecutionPlan>) -> usize {
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let mut rows = 0;
    while let Some(b) = stream.next().await {
        rows += b.unwrap().num_rows();
    }
    rows
}

fn sizes() -> &'static [usize] {
    if std::env::var("DEDUP_BIG").is_ok() { &[1_000_000, 10_000_000, 49_000_000] } else { &[1_000_000, 10_000_000] }
}
const DUP_RATIOS: [u32; 4] = [0, 50, 90, 99];

fn bench_dedup(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap();
    let mut group = c.benchmark_group("dedup");
    group.sample_size(10);

    for &total in sizes() {
        for &ratio in &DUP_RATIOS {
            let distinct = (total * (100 - ratio) as usize / 100).max(1);
            for &keep_greatest in &[false, true] {
                for &sorted in &[false, true] {
                    let batches = make_batches(total, distinct, sorted);
                    let tag = format!(
                        "n{}m/dup{}/{}/{}",
                        total / 1_000_000,
                        ratio,
                        if sorted { "sorted" } else { "shuffled" },
                        if keep_greatest { "greatest" } else { "first" },
                    );
                    group.throughput(Throughput::Elements(total as u64));
                    group.bench_with_input(BenchmarkId::from_parameter(&tag), &batches, |b, batches| {
                        b.to_async(&rt).iter(|| {
                            let plan = dedup_plan(batches.clone(), sorted, keep_greatest);
                            async move { drain(plan).await }
                        });
                    });
                }
            }
        }
    }
    group.finish();
}

fn mem_report(rt: &tokio::runtime::Runtime) {
    println!("\n=== dedup memory report (peak live bytes / alloc count) ===");
    println!("{:<28} {:>14} {:>14} {:>10}", "config", "peak_bytes", "allocs", "kept");
    for &total in sizes() {
        for &ratio in &DUP_RATIOS {
            let distinct = (total * (100 - ratio) as usize / 100).max(1);
            for &keep_greatest in &[false, true] {
                for &sorted in &[false, true] {
                    let batches = make_batches(total, distinct, sorted);
                    let (kept, peak, allocs) = measure(|| rt.block_on(drain(dedup_plan(batches, sorted, keep_greatest))));
                    let tag = format!(
                        "n{}m/dup{}/{}/{}",
                        total / 1_000_000,
                        ratio,
                        if sorted { "s" } else { "x" },
                        if keep_greatest { "greatest" } else { "first" },
                    );
                    println!("{tag:<28} {peak:>14} {allocs:>14} {kept:>10}");
                }
            }
        }
    }
}

fn bench_flush_dedup(c: &mut Criterion) {
    let keys = vec!["id".to_string(), "timestamp".to_string()];
    let mut group = c.benchmark_group("flush_dedup");
    for &(total, distinct) in &[(200_000usize, 200_000usize), (200_000, 100_000), (200_000, 20_000)] {
        // Flushes receive batches in insertion order.
        let batches = make_batches(total, distinct, false);
        group.throughput(Throughput::Elements(total as u64));
        group.bench_function(format!("{}k_rows_{}pct_dup", total / 1000, 100 - distinct * 100 / total), |b| {
            b.iter(|| {
                let out = timefusion::write::mem_buffer::dedup_batches(batches.clone(), &keys, Some("version"), None).unwrap();
                std::hint::black_box(out.iter().map(|x| x.num_rows()).sum::<usize>())
            })
        });
    }
    group.finish();
}

/// Cost of the landed-batch identity that lets a flush decline rows Delta
/// already holds (`docs/plans/2026-09-02-stop-manufacturing-duplicates.md`),
/// against the parquet encode it guards. Exists because the first estimate was
/// taken in a DEBUG build, where SHA-256 runs ~30x under its release speed and
/// the digest looked 5x more expensive than the encode. The flag's cost is
/// this ratio; measure it here, not in a test binary.
fn ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size() + 1024);
    {
        let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut buf, batch.schema_ref()).unwrap();
        w.write(batch).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn bench_landed_digest(c: &mut Criterion) {
    let mut group = c.benchmark_group("landed_digest");
    for &rows in &[20_000usize, 200_000] {
        let batches = make_batches(rows, rows, false);
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        group.throughput(Throughput::Bytes(bytes as u64));
        group.bench_function(BenchmarkId::new("digest", rows), |b| b.iter(|| std::hint::black_box(timefusion::write::landed_digest(&batches))));
        // The FLOOR: the Arrow round-trip alone, no hash. Tells us how much of
        // the digest any hash choice can possibly remove.
        group.bench_function(BenchmarkId::new("canonicalize_only", rows), |b| {
            b.iter(|| {
                let mut total = 0usize;
                for batch in &batches {
                    let once = ipc_bytes(batch);
                    let mut r = datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(&once), None).unwrap();
                    let back = r.next().unwrap().unwrap();
                    total += ipc_bytes(&back).len();
                }
                std::hint::black_box(total)
            })
        });
        // Hash-only, over the canonical bytes: what the hash choice actually buys.
        let canonical: Vec<Vec<u8>> = batches.iter().map(ipc_bytes).collect();
        group.bench_function(BenchmarkId::new("hash_xxh3_128", rows), |b| {
            b.iter(|| std::hint::black_box(canonical.iter().map(|c| twox_hash::XxHash3_128::oneshot(c) as usize).sum::<usize>()))
        });
        group.bench_function(BenchmarkId::new("parquet_encode", rows), |b| {
            b.iter(|| {
                let mut sink = Vec::new();
                let mut w = datafusion::parquet::arrow::ArrowWriter::try_new(&mut sink, batches[0].schema(), None).unwrap();
                for batch in &batches {
                    w.write(batch).unwrap();
                }
                w.close().unwrap();
                std::hint::black_box(sink.len())
            })
        });
    }
    group.finish();
}

fn maybe_report(c: &mut Criterion) {
    if std::env::var("DEDUP_MEM_REPORT").is_ok() {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).build().unwrap();
        mem_report(&rt);
    }
    bench_dedup(c);
    bench_flush_dedup(c);
    bench_landed_digest(c);
}

criterion_group!(benches, maybe_report);
criterion_main!(benches);
