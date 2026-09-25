//! Local mechanism experiment over the real rollup coordinator, in ABBA/BAAB order.
//! `cargo bench --bench rollup_work -- 32768 certified` compares winner selection.
//! Replace `certified` with `batches` to compare fixed and byte-aware batches independently.
//! Requires local MinIO's timefusion-tests bucket.
//! Each arm receives the same logical records in a separate test prefix. File IDs and
//! write stamps differ: this is not an identical-snapshot or production acceptance test.
//! Reports process CPU (all threads), elapsed time, endpoint RSS, and existing work counters.
//! RSS is not a phase peak. MinIO CPU and I/O are outside this process measurement.
//! Synthetic MinIO prefixes remain available for inspection; temporary local state is removed.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};

use anyhow::{Result, ensure};
use arrow::{array::AsArray, datatypes::Int64Type};
use serde::Serialize;
use timefusion::{
    database::Database,
    observability::{maintenance_stats, process_rss_bytes},
    support::{
        advance_micros,
        test_helpers::{json_to_batch, minio_test_config, process_cpu, test_span_ts},
    },
};
use tracing::{
    Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const SOURCE: &str = "otel_logs_and_spans";
const TIER: &str = "otel_logs_and_spans_rollup_dashboard_1m_v3";
const PROJECT: &str = "rollup-work-benchmark";
const COUNTERS: [&str; 5] =
    ["rollup_scan_cohorts_total", "rollup_scan_estimated_bytes_total", "rollup_output_rows_total", "rollup_output_files_total", "rollup_commit_actions_total"];

#[derive(Clone, Copy, Serialize, strum::EnumString)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
enum Experiment {
    Certified,
    Batches,
}

#[derive(Default)]
struct PublicationCounts {
    certified: AtomicU64,
    larger_batches: AtomicU64,
}

#[derive(Clone, Default)]
struct Publications(Arc<PublicationCounts>);

impl<S: Subscriber> Layer<S> for Publications {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        #[derive(Default)]
        struct Build {
            clean: bool,
            larger_batch: bool,
        }
        impl Visit for Build {
            fn record_bool(&mut self, field: &Field, value: bool) {
                if field.name() == "certified_clean" {
                    self.clean = value;
                }
            }
            fn record_u64(&mut self, field: &Field, value: u64) {
                if field.name() == "batch_rows" {
                    self.larger_batch = value > 256;
                }
            }
            fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
        }
        if event.metadata().target() == "timefusion::database::maintain" {
            let mut build = Build::default();
            event.record(&mut build);
            self.0.certified.fetch_add(u64::from(build.clean), Relaxed);
            self.0.larger_batches.fetch_add(u64::from(build.larger_batch), Relaxed);
        }
    }
}

fn counters() -> BTreeMap<&'static str, u64> {
    maintenance_stats().stats_rows().into_iter().filter(|(_, name, _)| COUNTERS.contains(name)).map(|(_, name, value)| (name, value)).collect()
}

#[derive(Serialize)]
struct Measurement {
    cpu_seconds: f64,
    elapsed_seconds: f64,
    rss_before_bytes: Option<usize>,
    rss_after_bytes: Option<usize>,
}

async fn measured<T>(work: impl Future<Output = Result<T>>) -> Result<(T, Measurement)> {
    let rss_before_bytes = process_rss_bytes();
    let cpu = process_cpu()?;
    let started = Instant::now();
    let value = work.await?;
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let cpu_seconds = process_cpu()?.checked_sub(cpu).ok_or_else(|| anyhow::anyhow!("process CPU clock moved backward"))?.as_secs_f64();
    Ok((value, Measurement { cpu_seconds, elapsed_seconds, rss_before_bytes, rss_after_bytes: process_rss_bytes() }))
}

#[derive(Serialize)]
struct Sample {
    repetition: usize,
    experiment: Experiment,
    candidate: bool,
    debug_assertions: bool,
    input_rows: usize,
    live_rows: i64,
    fixture_prefix: String,
    preparation: Measurement,
    build: Measurement,
    claimed_units: usize,
    certified_publications: u64,
    larger_batch_publications: u64,
    work: BTreeMap<&'static str, u64>,
}

async fn sample(repetition: usize, candidate: bool, rows: usize, experiment: Experiment, publications: &Publications) -> Result<Sample> {
    let dir = tempfile::tempdir()?;
    let id = format!("rollup-work-{}", uuid::Uuid::new_v4());
    let mut config = (*minio_test_config(&id, dir.path().to_str().ok_or_else(|| anyhow::anyhow!("non-UTF8 temporary path"))?)).clone();
    let certified = candidate && matches!(experiment, Experiment::Certified);
    let adaptive = candidate && matches!(experiment, Experiment::Batches);
    config.maintenance.timefusion_rollup_certified_clean = certified;
    config.maintenance.timefusion_rollup_adaptive_batches = adaptive;
    config.maintenance.timefusion_rollup_backfill_days = 7;
    config.maintenance.timefusion_dedup_lookback_days = 7;
    let fixture_prefix = config.core.timefusion_table_prefix.clone();
    let db = Database::with_config(Arc::new(config)).await?;
    let date = chrono::Utc::now().date_naive() - chrono::Duration::days(3);
    let start = date.and_hms_opt(12, 0, 0).ok_or_else(|| anyhow::anyhow!("invalid fixture date"))?.and_utc().timestamp_micros();
    // Chunking bounds fixture allocation independently of the requested population.
    for begin in (0..rows).step_by(8192) {
        let records = (begin..(begin + 8192).min(rows))
            .map(|row| {
                let mut record = test_span_ts(&format!("row-{row}"), "op", PROJECT, start + i64::try_from(row % 60).expect("modulo 60") * 1_000_000);
                record["deleted"] = serde_json::json!(row % 17 == 0);
                record["duration"] = serde_json::json!(row % 10_000);
                record
            })
            .collect();
        db.insert_records_batch(PROJECT, SOURCE, vec![json_to_batch(records)?], true, None).await?;
    }
    let table = db.resolve_table(PROJECT, SOURCE).await?;
    let (_, preparation) = measured(db.dedup_today_partitions(&table, SOURCE, SOURCE)).await?;
    let before = counters();
    let clean_before = publications.0.certified.load(Relaxed);
    let batches_before = publications.0.larger_batches.load(Relaxed);
    let (claimed_units, build) = measured(async {
        db.plan_rollup_backfill().await?;
        advance_micros(16 * 60 * 1_000_000);
        db.drain_coordinator_rollups(256).await
    })
    .await?;
    let work = counters().into_iter().map(|(key, value)| (key, value - before[key])).collect::<BTreeMap<_, _>>();
    let certified_publications = publications.0.certified.load(Relaxed) - clean_before;
    let larger_batch_publications = publications.0.larger_batches.load(Relaxed) - batches_before;
    ensure!(claimed_units > 0 && work.get("rollup_scan_cohorts_total").is_some_and(|count| *count > 0), "the benchmark performed no source aggregation");
    ensure!((certified_publications > 0) == certified, "the requested certified path was not exercised as expected");
    ensure!((larger_batch_publications > 0) == adaptive, "the requested batch-size path was not exercised as expected");
    let actual = db.query_delta_only(&format!("SELECT CAST(SUM(request_count) AS BIGINT) FROM {TIER} WHERE project_id = '{PROJECT}'")).await?;
    let live_rows = actual[0].column(0).as_primitive::<Int64Type>().value(0);
    ensure!(live_rows == i64::try_from(rows - rows.div_ceil(17))?, "rollup result differs from the deterministic input oracle");
    Ok(Sample {
        repetition,
        experiment,
        candidate,
        debug_assertions: cfg!(debug_assertions),
        input_rows: rows,
        live_rows,
        fixture_prefix,
        preparation,
        build,
        claimed_units,
        certified_publications,
        larger_batch_publications,
        work,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1).filter(|arg| arg != "--bench");
    let rows = args.next().map(|value| value.parse::<usize>()).transpose()?.unwrap_or(32_768);
    let experiment = args.next().map(|value| value.parse::<Experiment>()).transpose()?.unwrap_or(Experiment::Certified);
    ensure!(args.next().is_none(), "usage: rollup_work [rows] [certified|batches]");
    ensure!((1..=1_000_000).contains(&rows), "rows must be between 1 and 1000000");
    let publications = Publications::default();
    let filter = tracing_subscriber::filter::Targets::new().with_target("timefusion::database::maintain", tracing::Level::INFO);
    tracing_subscriber::registry().with(publications.clone().with_filter(filter)).try_init()?;
    for (repetition, candidate) in [false, true, true, false, true, false, false, true].into_iter().enumerate() {
        println!("{}", serde_json::to_string(&sample(repetition, candidate, rows, experiment, &publications).await?)?);
    }
    Ok(())
}
