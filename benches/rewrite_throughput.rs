//! Where does a maintenance rewrite's time actually go?
//!
//! Every coordinator rewrite — Repair, HotPacking, SealedConsolidation and the
//! limited Dedup path — sorts with `batch_size = 256` and
//! `target_partitions = 1` (`maintain.rs:6025`, `compact.rs:998`), and Repair
//! additionally cuts a bin into N event-time slices, each its own full SQL pass
//! over the same file. Prod staged 93.8 MB in 322 s that way. This measures
//! each of those decisions against a REAL prod parquet file, because the cost
//! is dominated by row shape and row-group layout, which a generator does not
//! reproduce.
//!
//! ```bash
//! TF_BENCH_PARQUET=/path/to/part-....parquet cargo bench --bench rewrite_throughput
//! TF_BENCH_POOL_MB=256 ...   # reproduce prod's per-worker share (4.2 GB / 16 jobs)
//! ```
//!
//! The session config here MIRRORS `build_optimize_session_state_tuned`; it is
//! duplicated rather than imported because that function is `pub(crate)` and
//! the crate's lib-test target cannot build in release
//! (`datafusion_postgres::testing` is gated on debug assertions).

use std::{sync::Arc, time::Instant};

use datafusion::{
    execution::{
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

/// One measured rewrite. `slices > 1` reproduces `repair_bin_sliced`: N
/// event-time windows, each a separate full pass over the same file.
async fn pass(
    path: &str, batch: &str, partitions: usize, slices: usize, shape: Shape, pool_bytes: usize, spill: &std::path::Path,
) -> Result<(f64, u64), String> {
    use datafusion::execution::SessionStateBuilder;
    let state = SessionStateBuilder::new().with_config(session(batch, partitions)).with_runtime_env(runtime(pool_bytes, spill)).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_parquet("bin", path, ParquetReadOptions::default()).await.map_err(|e| e.to_string())?;
    let (min, max) = {
        use arrow::array::Array;
        let batches = ctx.sql("SELECT min(timestamp), max(timestamp) FROM bin").await.map_err(|e| e.to_string())?.collect().await.map_err(|e| e.to_string())?;
        let at = |index: usize| {
            batches[0].column(index).as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().map(|array| array.value(0)).unwrap_or_default()
        };
        (at(0), at(1))
    };
    let width = (max - min + 1).max(1) / slices as i64 + 1;
    let started = Instant::now();
    let mut rows = 0u64;
    for slice in 0..slices {
        let filter = if slices == 1 {
            String::new()
        } else {
            let (lo, hi) = (min + width * slice as i64, min + width * (slice as i64 + 1));
            format!(
                " WHERE timestamp >= arrow_cast({lo}, 'Timestamp(Microsecond, Some(\"UTC\"))') AND timestamp < arrow_cast({hi}, 'Timestamp(Microsecond, Some(\"UTC\"))')"
            )
        };
        let sql = shape.sql(&filter);
        let mut stream = ctx.sql(&sql).await.map_err(|e| e.to_string())?.execute_stream().await.map_err(|e| e.to_string())?;
        while let Some(batch) = stream.next().await {
            rows += batch.map_err(|e| e.to_string())?.num_rows() as u64;
        }
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

    // Each row is flushed as it completes: a variant that hangs or OOMs must not
    // take the rows already measured with it.
    let run = async |label: String, batch: &str, partitions: usize, slices: usize, shape: Shape| {
        use std::io::Write;
        let line = match pass(&path, batch, partitions, slices, shape, pool_mb * 1024 * 1024, spill.path()).await {
            Ok((secs, rows)) => format!("{label:<26} {secs:>8.1} {rows:>11} {:>10.2}", bytes as f64 / 1e6 / secs),
            Err(error) => format!("{label:<26} {:>8} {error}", "FAILED"),
        };
        println!("{line}");
        let _ = std::io::stdout().flush();
    };

    if std::env::var("TF_BENCH_FLEET").is_ok() {
        fleet(&path, pool_mb, bytes, spill.path()).await;
        return;
    }

    if std::env::var("TF_BENCH_PROBE").is_ok() {
        probe_shards(&path, pool_mb, bytes, spill.path()).await;
        return;
    }

    if std::env::var("TF_BENCH_SLICE").is_ok() {
        slice_floor(&path, bytes, spill.path()).await;
        return;
    }

    run("scan only".to_owned(), "8192", 1, 1, Shape::Scan).await;
    for batch in ["256", "2048", "8192"] {
        for partitions in [1usize, 8] {
            run(format!("sort b{batch} p{partitions}"), batch, partitions, 1, Shape::Sort).await;
        }
    }
    run("PROD: b256 p1 x13 slices".to_owned(), "256", 1, 13, Shape::Sort).await;
    // The dedup rewrite's own two shapes. `Window` is what shipped until
    // 2026-09-02: `ROW_NUMBER() OVER (PARTITION BY dedup_keys)` plus the output
    // `ORDER BY`, which plans TWO full external sorts because the window
    // normalizes its partition ordering to ASC. `Sort` is the replacement —
    // one sort in schema order, with the keep-greatest done as a one-pass
    // collapse of adjacent runs (`RunCollapse`), which the widened dedup key
    // makes valid. The gap between these two rows is the change.
    for (batch, partitions) in [("256", 1usize), ("2048", 1), ("2048", 8)] {
        run(format!("dedup WINDOW b{batch} p{partitions}"), batch, partitions, 1, Shape::Window).await;
        run(format!("dedup COLLAPSE b{batch} p{partitions}"), batch, partitions, 1, Shape::Sort).await;
    }
}

/// Does the rewrite fleet SCALE with concurrency, and what pool does 10x demand?
///
/// The per-unit numbers above say what one rewrite costs. They do not say what
/// the fleet can sustain, which is the question 10x actually asks: concurrency
/// is capped by `light_optimize_k = coordinator_share / PER_SORT_BUDGET_BYTES`,
/// so the thing to measure is aggregate throughput against pool size and
/// concurrency — including where it stops scaling and where it starts failing.
///
/// Each worker gets its own `SessionContext` over the same file and its own
/// slice of one SHARED pool, which is how prod is arranged: N coordinator units
/// on one `FairSpillPool`.
///
/// ```bash
/// The smallest per-job pool slice a dedup rewrite actually completes in.
///
/// Sizes `COORDINATOR_JOB_POOL_BYTES`, which prod currently derives as
/// `coordinator_share / jobs` = 8 GiB / 16 = **512 MiB — exactly the decoded
/// bytes a unit is admitted for**, leaving nothing for merge buffers or the
/// spill reservation. The journal shows the consequence directly: dedup units
/// retrying with `Not enough memory to continue external sort ... Additional
/// allocation failed for ExternalSorter[0]`.
///
/// The number this prints is the RATIO — minimum viable pool divided by the
/// decoded bytes the budget prices the same work at
/// (`compressed x DECODED_BYTES_PER_COMPRESSED`). A ratio above 1 means the
/// per-job slice must exceed the admission ceiling, and by how much. That is
/// the second half of the admission/sort-slice pair; widening admission without
/// it converts a starving queue into a failing one.
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
    println!("{:<12} {:>8} {:>11} {:>9}  {}", "pool MB", "secs", "rows", "pool/dec", "outcome");
    // Descending: the first failure is the floor, and everything below it is
    // known-bad, so a failed rung does not end the sweep — a pool can fail for
    // reasons other than size and that must be visible, not inferred.
    for pool_mb in [4096usize, 3072, 2048, 1536, 1024, 768, 512, 384, 256] {
        use std::io::Write;
        let ratio = pool_mb as f64 / decoded_mb;
        // `Sort` is the dedup rewrite's shape since 2026-09-02 (one sort in
        // schema order + RunCollapse), batch 2048 as `maintenance_batch_size`
        // sets for the Server profile.
        let line = match pass(path, "2048", 1, 1, Shape::Sort, pool_mb * 1024 * 1024, spill).await {
            Ok((secs, rows)) => format!("{pool_mb:<12} {secs:>8.1} {rows:>11} {ratio:>9.2}  ok"),
            Err(error) => format!("{pool_mb:<12} {:>8} {:>11} {ratio:>9.2}  {}", "FAIL", "-", error.lines().next().unwrap_or("")),
        };
        println!("{line}");
        let _ = std::io::stdout().flush();
    }
}

/// TF_BENCH_FLEET=1 TF_BENCH_PARQUET=… TF_BENCH_POOL_MB=8192 cargo bench --bench rewrite_throughput
/// ```
async fn fleet(path: &str, pool_mb: usize, bytes: u64, spill: &std::path::Path) {
    println!(
        "
{:<22} {:>8} {:>12} {:>12} {:>9}",
        "concurrency", "secs", "MB/s total", "MB/s each", "failed"
    );
    // 3, 5, 6 included deliberately: prod runs 4 and the 8-worker rung FAILS at
    // an 8 GB pool, so the usable ceiling is somewhere in between and that is the
    // number a permit change has to be justified against.
    for workers in [1usize, 2, 4, 5, 6, 8] {
        // ONE pool for all of them, sized as prod sizes the coordinator's.
        let shared = runtime(pool_mb * 1024 * 1024, spill);
        let started = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..workers {
            let (path, runtime) = (path.to_owned(), Arc::clone(&shared));
            set.spawn(async move { one_rewrite(&path, runtime).await });
        }
        let mut failed = 0usize;
        while let Some(result) = set.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                failed += 1;
            }
        }
        let secs = started.elapsed().as_secs_f64();
        let moved = bytes as f64 / 1e6 * (workers - failed) as f64;
        println!("{:<22} {secs:>8.1} {:>12.2} {:>12.2} {failed:>9}", format!("{workers} workers"), moved / secs, moved / secs / workers as f64);
    }
}

/// One unit's worth of work: the same scan+sort+consume the staging loop drives.
async fn one_rewrite(path: &str, runtime: Arc<RuntimeEnv>) -> Result<(), String> {
    use datafusion::execution::SessionStateBuilder;
    // 2048 rows is what `batch_rows_for` picks for an ordinary otel row at the
    // 8 MB target; the whale's wide rows land lower, which is the point of it.
    let state = SessionStateBuilder::new().with_config(session("2048", 1)).with_runtime_env(runtime).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_parquet("bin", path, ParquetReadOptions::default()).await.map_err(|e| e.to_string())?;
    let mut stream = ctx.sql(&format!("SELECT * FROM bin{ORDER_BY}")).await.map_err(|e| e.to_string())?.execute_stream().await.map_err(|e| e.to_string())?;
    while let Some(batch) = stream.next().await {
        batch.map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// What do the dedup probe's hash SHARDS cost?
///
/// `stage_dedup_partition_range` runs the duplicate probe once per shard, and
/// each pass re-reads the selected files — the shard predicate is a hash over
/// the dedup keys, so nothing prunes. Prod 2026-09-01 shows the consequence
/// directly in `maintenance_scan_pruning`: the same 3,433.7 MB scanned SIX
/// times (90-118 s each, ~640 s total) and the same 9,408 MB scanned twice at
/// ~1,450 s. Across 45 warm minutes, 462 scans read 58.88 GB in 11,821 s.
///
/// Sharding is a deliberate memory-for-IO trade, made when the coordinator pool
/// was 4.2 GB: one pass must hold the partition's whole dedup-key cardinality,
/// N passes hold 1/N of it. The pool is now ~10 GB and DataFusion spills
/// grouped aggregates, so the trade is worth re-deriving rather than assuming —
/// which is what this measures: wall time for N passes vs one, at a given pool.
///
/// ```bash
/// TF_BENCH_PROBE=1 TF_BENCH_PARQUET=… TF_BENCH_POOL_MB=1024 cargo bench --bench rewrite_throughput
/// ```
async fn probe_shards(path: &str, pool_mb: usize, bytes: u64, spill: &std::path::Path) {
    use std::io::Write;
    println!("\n{:<20} {:>8} {:>12} {:>10}", "probe variant", "secs", "MB/s in", "result");
    for shards in [1usize, 2, 4, 6] {
        let runtime = runtime(pool_mb * 1024 * 1024, spill);
        let state = datafusion::execution::SessionStateBuilder::new().with_config(session("8192", 1)).with_runtime_env(runtime).with_default_features().build();
        let ctx = SessionContext::new_with_state(state);
        if ctx.register_parquet("bin", path, ParquetReadOptions::default()).await.is_err() {
            println!("{:<20} {:>8}", format!("{shards} shard(s)"), "REGISTER-FAILED");
            continue;
        }
        let started = Instant::now();
        let mut failed = None;
        for shard in 0..shards {
            // A non-pruning predicate, exactly like the real hash-bucket one:
            // every pass still decodes every row. The hash itself is a crate UDF
            // and irrelevant to the IO this measures.
            let filter = match shards {
                1 => String::new(),
                _ => format!(" WHERE abs(length(CAST(\"id\" AS VARCHAR))) % {shards} = {shard}"),
            };
            let sql = format!("SELECT count(*) FROM (SELECT \"timestamp\", count(*) AS c FROM bin{filter} GROUP BY \"timestamp\", \"id\") AS g WHERE c > 1");
            match ctx.sql(&sql).await {
                Ok(df) => {
                    if let Err(error) = df.collect().await {
                        failed = Some(error.to_string());
                        break;
                    }
                }
                Err(error) => {
                    failed = Some(error.to_string());
                    break;
                }
            }
        }
        let secs = started.elapsed().as_secs_f64();
        let outcome = failed.map_or_else(|| "ok".to_owned(), |error| error.chars().take(46).collect());
        println!("{:<20} {secs:>8.1} {:>12.2} {outcome:>10}", format!("{shards} shard(s)"), bytes as f64 / 1e6 / secs);
        let _ = std::io::stdout().flush();
    }
}
