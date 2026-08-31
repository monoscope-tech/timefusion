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
    path: &str, batch: &str, partitions: usize, slices: usize, sorted: bool, pool_bytes: usize, spill: &std::path::Path,
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
        let sql = format!("SELECT * FROM bin{filter}{}", if sorted { ORDER_BY } else { "" });
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
    let run = async |label: String, batch: &str, partitions: usize, slices: usize, sorted: bool| {
        use std::io::Write;
        let line = match pass(&path, batch, partitions, slices, sorted, pool_mb * 1024 * 1024, spill.path()).await {
            Ok((secs, rows)) => format!("{label:<26} {secs:>8.1} {rows:>11} {:>10.2}", bytes as f64 / 1e6 / secs),
            Err(error) => format!("{label:<26} {:>8} {error}", "FAILED"),
        };
        println!("{line}");
        let _ = std::io::stdout().flush();
    };

    run("scan only".to_owned(), "8192", 1, 1, false).await;
    for batch in ["256", "2048", "8192"] {
        for partitions in [1usize, 8] {
            run(format!("sort b{batch} p{partitions}"), batch, partitions, 1, true).await;
        }
    }
    run("PROD: b256 p1 x13 slices".to_owned(), "256", 1, 13, true).await;
}
