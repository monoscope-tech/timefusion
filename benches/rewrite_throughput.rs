//! Prices the maintenance rewrite shapes (scan / sort / dedup) against a real
//! parquet file, varying batch size, partitions, slicing, pool size and
//! concurrency.
//!
//! ```bash
//! TF_BENCH_PARQUET=/path/to/part-....parquet cargo bench --bench rewrite_throughput
//! ```
//!
//! The session config MIRRORS `build_optimize_session_state_tuned`; it is
//! duplicated rather than imported because that function is `pub(crate)` and
//! the crate's lib-test target cannot build in release
//! (`datafusion_postgres::testing` is gated on debug assertions).

use std::{sync::Arc, time::Instant};

use datafusion::{
    execution::{
        SessionStateBuilder,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{FairSpillPool, TrackConsumersPool},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use futures::StreamExt;

/// `schemas/otel_logs_and_spans.yaml`'s `sorting_columns`, as the rewrite spells them.
const ORDER_BY: &str = " ORDER BY \"timestamp\" DESC NULLS FIRST, \"resource___service___name\" ASC NULLS LAST, \"id\" ASC NULLS LAST, \"level\" ASC NULLS LAST, \"status_code\" ASC NULLS LAST";

/// The three rewrite shapes this bench prices.
#[derive(Clone, Copy)]
enum Shape {
    Scan,
    Sort,
    Window,
}

impl Shape {
    fn sql(self, filter: &str) -> String {
        match self {
            Self::Scan => format!("SELECT * FROM bin{filter}"),
            Self::Sort => format!("SELECT * FROM bin{filter}{ORDER_BY}"),
            Self::Window => format!(
                "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY \"timestamp\", \"resource___service___name\", \"id\" ORDER BY \"updated_at\" DESC NULLS LAST) AS __tf_rn FROM bin{filter}) WHERE __tf_rn = 1{ORDER_BY}"
            ),
        }
    }
}

fn runtime(pool_bytes: usize, spill: &std::path::Path) -> Arc<RuntimeEnv> {
    let top = std::num::NonZeroUsize::new(5).expect("5 is non-zero");
    let pool = Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_bytes), top));
    Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![spill.to_path_buf()])))
            .build()
            .expect("runtime env"),
    )
}

fn session(batch: &str, partitions: usize) -> SessionConfig {
    let mut cfg = SessionConfig::new().set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    for (key, value) in [
        ("datafusion.execution.batch_size", batch),
        ("datafusion.execution.sort_spill_reservation_bytes", "33554432"),
        ("datafusion.execution.skip_physical_aggregate_schema_check", "true"),
    ] {
        let _ = cfg.options_mut().set(key, value);
    }
    cfg.with_target_partitions(partitions)
}

/// A context over the bin file, registered as `bin`. The runtime is a parameter
/// so callers choose whether workers get their own pool or share one.
async fn parquet_ctx(path: &str, batch: &str, partitions: usize, runtime: Arc<RuntimeEnv>) -> Result<SessionContext, String> {
    let state = SessionStateBuilder::new().with_config(session(batch, partitions)).with_runtime_env(runtime).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_parquet("bin", path, ParquetReadOptions::default()).await.map_err(|e| e.to_string())?;
    Ok(ctx)
}

/// Event-time bounds of the registered file.
async fn bounds(ctx: &SessionContext) -> Result<(i64, i64), String> {
    use arrow::array::Array;
    let batches = ctx.sql("SELECT min(timestamp), max(timestamp) FROM bin").await.map_err(|e| e.to_string())?.collect().await.map_err(|e| e.to_string())?;
    let at = |index: usize| {
        batches[0].column(index).as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().map(|array| array.value(0)).unwrap_or_default()
    };
    Ok((at(0), at(1)))
}

/// One event-time slice, spelled as the rewrite spells it.
fn window(lo: i64, hi: i64) -> String {
    format!(
        " WHERE timestamp >= arrow_cast({lo}, 'Timestamp(Microsecond, Some(\"UTC\"))') AND timestamp < arrow_cast({hi}, 'Timestamp(Microsecond, Some(\"UTC\"))')"
    )
}

/// Streams a statement to completion, returning the rows it consumed.
async fn drain(ctx: &SessionContext, sql: &str) -> Result<u64, String> {
    let mut stream = ctx.sql(sql).await.map_err(|e| e.to_string())?.execute_stream().await.map_err(|e| e.to_string())?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        rows += batch.map_err(|e| e.to_string())?.num_rows() as u64;
    }
    Ok(rows)
}

/// Runs `count` workers concurrently, returning how many failed and the first
/// failure. A join failure (panic, cancellation) counts as a worker failure.
async fn race<Fut>(count: usize, make: impl Fn(usize) -> Fut) -> (usize, Option<String>)
where
    Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    let mut set = tokio::task::JoinSet::new();
    for worker in 0..count {
        set.spawn(make(worker));
    }
    let (mut failed, mut first) = (0usize, None);
    while let Some(result) = set.join_next().await {
        if let Err(error) = result.map_err(|e| e.to_string()).and_then(|worker| worker) {
            failed += 1;
            first.get_or_insert(error);
        }
    }
    (failed, first)
}

/// Prints and flushes immediately, so a later hang or OOM cannot lose rows
/// already measured.
fn emit(line: String) {
    use std::io::Write;
    println!("{line}");
    let _ = std::io::stdout().flush();
}

/// One measured rewrite. `slices > 1` reproduces `repair_bin_sliced`: N
/// event-time windows, each a separate full pass over the same file.
async fn pass(
    path: &str, batch: &str, partitions: usize, slices: usize, shape: Shape, pool_bytes: usize, spill: &std::path::Path,
) -> Result<(f64, u64), String> {
    let ctx = parquet_ctx(path, batch, partitions, runtime(pool_bytes, spill)).await?;
    let (min, max) = bounds(&ctx).await?;
    let width = (max - min + 1).max(1) / slices as i64 + 1;
    let started = Instant::now();
    let mut rows = 0u64;
    for slice in 0..slices {
        let filter = if slices == 1 { String::new() } else { window(min + width * slice as i64, min + width * (slice as i64 + 1)) };
        rows += drain(&ctx, &shape.sql(&filter)).await?;
    }
    Ok((started.elapsed().as_secs_f64(), rows))
}

#[tokio::main]
async fn main() {
    let Ok(path) = std::env::var("TF_BENCH_PARQUET") else {
        eprintln!("set TF_BENCH_PARQUET to a parquet file");
        return;
    };
    let pool_mb: usize = std::env::var("TF_BENCH_POOL_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(4096);
    let spill = tempfile::tempdir().expect("spill dir");
    let bytes = std::fs::metadata(&path).expect("stat").len();
    println!("\nfile {} ({:.1} MB compressed), pool {pool_mb} MB", path, bytes as f64 / 1e6);
    println!("{:<26} {:>8} {:>11} {:>10}", "variant", "secs", "rows", "MB/s in");

    let run = async |label: String, batch: &str, partitions: usize, slices: usize, shape: Shape| {
        emit(match pass(&path, batch, partitions, slices, shape, pool_mb * 1024 * 1024, spill.path()).await {
            Ok((secs, rows)) => format!("{label:<26} {secs:>8.1} {rows:>11} {:>10.2}", bytes as f64 / 1e6 / secs),
            Err(error) => format!("{label:<26} {:>8} {error}", "FAILED"),
        });
    };

    if std::env::var("TF_BENCH_FLEET").is_ok() {
        return fleet(&path, pool_mb, bytes).await;
    }
    if std::env::var("TF_BENCH_PROBE").is_ok() {
        return probe_shards(&path, pool_mb, bytes, spill.path()).await;
    }
    if std::env::var("TF_BENCH_SLICE").is_ok() {
        return slice_floor(&path, bytes, spill.path()).await;
    }
    if std::env::var("TF_BENCH_PRODSHAPE").is_ok() {
        return prod_shape(&path, pool_mb, bytes, spill.path()).await;
    }

    run("scan only".to_owned(), "8192", 1, 1, Shape::Scan).await;
    for batch in ["256", "2048", "8192"] {
        for partitions in [1usize, 8] {
            run(format!("sort b{batch} p{partitions}"), batch, partitions, 1, Shape::Sort).await;
        }
    }
    run("PROD: b256 p1 x13 slices".to_owned(), "256", 1, 13, Shape::Sort).await;
    // The dedup rewrite's two shapes: `Window` plans two full external sorts
    // (the window normalizes its partition ordering to ASC); `Sort` is the
    // one-sort + RunCollapse replacement.
    for (batch, partitions) in [("256", 1usize), ("2048", 1), ("2048", 8)] {
        run(format!("dedup WINDOW b{batch} p{partitions}"), batch, partitions, 1, Shape::Window).await;
        run(format!("dedup COLLAPSE b{batch} p{partitions}"), batch, partitions, 1, Shape::Sort).await;
    }
}

/// `workers` concurrent sorts, each over ONE `1/WINDOWS` time-window of the
/// file, sharing one pool — the shape a real coordinator runs, where each job
/// is admitted for at most `MAX_DECODED_BYTES`. Pass criterion is `failed == 0`.
/// (`fleet` instead hands every worker the WHOLE file, a much heavier load per
/// byte of pool, so its rungs do not speak to this question.)
async fn prod_shape(path: &str, pool_mb: usize, bytes: u64, spill: &std::path::Path) {
    /// Windows the file is cut into; each worker takes one. Five puts a worker's
    /// share near the 512 MiB admission ceiling.
    const WINDOWS: i64 = 5;
    let workers: usize = std::env::var("TF_BENCH_WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let shared = runtime(pool_mb * 1024 * 1024, spill);
    let (min, max) = bounds(&parquet_ctx(path, "2048", 1, Arc::clone(&shared)).await.expect("register")).await.expect("bounds");
    let width = (max - min + 1).max(1) / WINDOWS + 1;
    let per_worker_mb = bytes as f64 / 1e6 * 12.0 / WINDOWS as f64;
    println!("\n{workers} workers x ~{per_worker_mb:.0} MB decoded each = {:.1} GB through a {pool_mb} MB pool", per_worker_mb * workers as f64 / 1000.0);
    let started = Instant::now();
    let (failed, first) = race(workers, |worker| {
        let (path, runtime) = (path.to_owned(), Arc::clone(&shared));
        let (lo, hi) = (min + width * (worker as i64 % WINDOWS), min + width * (worker as i64 % WINDOWS + 1));
        async move { one_window(&path, runtime, lo, hi).await }
    })
    .await;
    println!("secs {:.1}   failed {failed} of {workers}", started.elapsed().as_secs_f64());
    match first {
        Some(error) => println!("VERDICT: FAIL — {}", error.lines().next().unwrap_or("")),
        None => println!("VERDICT: PASS — prod's job count survives its own admitted load"),
    }
}

/// One worker's window. Same shape as `one_rewrite`, bounded to a time range.
async fn one_window(path: &str, runtime: Arc<RuntimeEnv>, lo: i64, hi: i64) -> Result<(), String> {
    let ctx = parquet_ctx(path, "2048", 1, runtime).await?;
    drain(&ctx, &Shape::Sort.sql(&window(lo, hi))).await.map(drop)
}

/// The smallest per-job pool slice a dedup rewrite actually completes in —
/// the input for sizing `COORDINATOR_JOB_POOL_BYTES`.
///
/// Prints the RATIO: minimum viable pool over the decoded bytes the budget
/// prices the same work at (`compressed x DECODED_BYTES_PER_COMPRESSED`). A
/// ratio above 1 means the per-job slice must exceed the admission ceiling.
///
/// ```bash
/// TF_BENCH_SLICE=1 TF_BENCH_PARQUET=… cargo bench --bench rewrite_throughput
/// ```
async fn slice_floor(path: &str, bytes: u64, spill: &std::path::Path) {
    /// The ratio every sort budget in the crate is denominated by
    /// (`database::maintain::DECODED_BYTES_PER_COMPRESSED`).
    const DECODED_PER_COMPRESSED: f64 = 12.0;
    let decoded_mb = bytes as f64 / 1e6 * DECODED_PER_COMPRESSED;
    println!("\ndecoded ~{decoded_mb:.0} MB at {DECODED_PER_COMPRESSED:.0}x — the size every budget prices this work at");
    println!("{:<12} {:>8} {:>11} {:>9}  outcome", "pool MB", "secs", "rows", "pool/dec");
    // Descending, and a failed rung does NOT end the sweep: a pool can fail for
    // reasons other than size, which must be visible rather than inferred.
    for pool_mb in [4096usize, 3072, 2048, 1536, 1024, 768, 512, 384, 256] {
        let ratio = pool_mb as f64 / decoded_mb;
        // `Sort` is the dedup rewrite's shape; batch 2048 is what
        // `maintenance_batch_size` sets for the Server profile.
        emit(match pass(path, "2048", 1, 1, Shape::Sort, pool_mb * 1024 * 1024, spill).await {
            Ok((secs, rows)) => format!("{pool_mb:<12} {secs:>8.1} {rows:>11} {ratio:>9.2}  ok"),
            Err(error) => format!("{pool_mb:<12} {:>8} {:>11} {ratio:>9.2}  {}", "FAIL", "-", error.lines().next().unwrap_or("")),
        });
    }
}

/// Aggregate throughput of N concurrent whole-file rewrites sharing one pool.
///
/// ```bash
/// TF_BENCH_FLEET=1 TF_BENCH_PARQUET=… TF_BENCH_POOL_MB=8192 cargo bench --bench rewrite_throughput
/// ```
async fn fleet(path: &str, pool_mb: usize, bytes: u64) {
    println!(
        "
{:<22} {:>8} {:>12} {:>12} {:>9}",
        "concurrency", "secs", "MB/s total", "MB/s each", "failed"
    );
    // `pool / jobs` is NOT the share a worker gets, so only the ladder answers
    // what a job count costs. `TF_BENCH_WORKERS=1,2,4,…` overrides the rungs;
    // rerun the same rungs at several pool sizes to see whether a cliff is
    // pool-priced. Rungs far past a small pool's cliff only generate spill,
    // which on a full disk fails for the wrong reason.
    let rungs: Vec<usize> = std::env::var("TF_BENCH_WORKERS")
        .ok()
        .map(|list| list.split(',').filter_map(|n| n.trim().parse().ok()).collect::<Vec<_>>())
        .filter(|rungs: &Vec<usize>| !rungs.is_empty())
        .unwrap_or_else(|| vec![1, 2, 4, 5, 6, 8, 10, 12, 16]);
    for workers in rungs {
        // A FRESH spill dir per rung: a shared one accumulates the whole
        // ladder's spill and later rungs then fail on ENOSPC, which reads as a
        // memory cliff.
        let rung_spill = tempfile::tempdir().expect("spill dir");
        // ONE pool shared by all workers in the rung, as the coordinator does.
        let shared = runtime(pool_mb * 1024 * 1024, rung_spill.path());
        let started = Instant::now();
        let (failed, _) = race(workers, |_| {
            let (path, runtime) = (path.to_owned(), Arc::clone(&shared));
            async move { one_rewrite(&path, runtime).await }
        })
        .await;
        let secs = started.elapsed().as_secs_f64();
        let moved = bytes as f64 / 1e6 * (workers - failed) as f64;
        println!("{:<22} {secs:>8.1} {:>12.2} {:>12.2} {failed:>9}", format!("{workers} workers"), moved / secs, moved / secs / workers as f64);
    }
}

/// One unit's worth of work: the same scan+sort+consume the staging loop drives.
async fn one_rewrite(path: &str, runtime: Arc<RuntimeEnv>) -> Result<(), String> {
    // 2048 rows is what `batch_rows_for` picks for an ordinary otel row at the
    // 8 MB target.
    let ctx = parquet_ctx(path, "2048", 1, runtime).await?;
    let sql = Shape::Sort.sql("");
    // THE SORT MUST ACTUALLY RUN. A fixture written by our own rewrite carries
    // footer `sorting_columns` equal to this ORDER BY, so DataFusion declares
    // the scan already ordered and the ladder silently degenerates into a scan
    // benchmark with zero failures at every rung. Fail loudly instead.
    let plan = ctx.sql(&sql).await.map_err(|e| e.to_string())?.create_physical_plan().await.map_err(|e| e.to_string())?;
    let rendered = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
    if !rendered.contains("SortExec") {
        return Err(format!("the fixture is ALREADY SORTED, so no sort ran and this measures a scan:\n{rendered}"));
    }
    drain(&ctx, &sql).await.map(drop)
}

/// What the dedup probe's hash SHARDS cost: wall time for N passes vs one, at a
/// given pool.
///
/// `stage_dedup_partition_range` runs the duplicate probe once per shard and
/// each pass re-reads every selected file (the shard predicate is a hash over
/// the dedup keys, so nothing prunes). Sharding trades IO for memory: one pass
/// must hold the partition's whole dedup-key cardinality, N passes hold 1/N.
///
/// ```bash
/// TF_BENCH_PROBE=1 TF_BENCH_PARQUET=… TF_BENCH_POOL_MB=1024 cargo bench --bench rewrite_throughput
/// ```
async fn probe_shards(path: &str, pool_mb: usize, bytes: u64, spill: &std::path::Path) {
    println!("\n{:<20} {:>8} {:>12} {:>10}", "probe variant", "secs", "MB/s in", "result");
    for shards in [1usize, 2, 4, 6] {
        let Ok(ctx) = parquet_ctx(path, "8192", 1, runtime(pool_mb * 1024 * 1024, spill)).await else {
            println!("{:<20} {:>8}", format!("{shards} shard(s)"), "REGISTER-FAILED");
            continue;
        };
        let started = Instant::now();
        let mut failed = None;
        for shard in 0..shards {
            // A non-pruning predicate, like the real hash-bucket one: every
            // pass still decodes every row.
            let filter = match shards {
                1 => String::new(),
                _ => format!(" WHERE abs(length(CAST(\"id\" AS VARCHAR))) % {shards} = {shard}"),
            };
            let sql = format!("SELECT count(*) FROM (SELECT \"timestamp\", count(*) AS c FROM bin{filter} GROUP BY \"timestamp\", \"id\") AS g WHERE c > 1");
            if let Err(error) = async { ctx.sql(&sql).await?.collect().await }.await {
                failed = Some(error.to_string());
                break;
            }
        }
        let secs = started.elapsed().as_secs_f64();
        let outcome = failed.map_or_else(|| "ok".to_owned(), |error| error.chars().take(46).collect());
        emit(format!("{:<20} {secs:>8.1} {:>12.2} {outcome:>10}", format!("{shards} shard(s)"), bytes as f64 / 1e6 / secs));
    }
}
