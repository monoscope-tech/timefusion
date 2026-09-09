//! Full SQL/Delta/Tantivy benchmark against local MinIO, with exact bucket checks.
//! Run: cargo bench --bench hash_histogram_sql -- 10000 > histogram.json
//! The argument is rows per day across 30 days. This excludes PostgreSQL wire
//! overhead and production payload widths. Data remains under a unique local
//! MinIO prefix reported in the output so subsequent investigations can reuse it.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use arrow::array::{Int64Array, TimestampMicrosecondArray};
use datafusion::prelude::SessionContext;
use serde::Serialize;
use serde_json::json;
use timefusion::{
    config::AppConfig,
    database::Database,
    support::test_helpers::{json_to_batch_for, minio_test_config},
    tantivy::search::{SearchStats, TantivyIndexService, TantivySearchService, parquet_rel_of_uri},
};

const TABLE: &str = "mor_versioned";
const DAY: i64 = 86_400_000_000;
const HOUR: i64 = DAY / 24;

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Predicate {
    Rare,
    Medium,
    Common,
    Overlap,
}

impl Predicate {
    fn sql(self) -> &'static str {
        match self {
            Self::Rare => "hashes @> ARRAY['rare']",
            Self::Medium => "hashes @> ARRAY['medium']",
            Self::Common => "hashes @> ARRAY['common']",
            Self::Overlap => "hashes && ARRAY['medium', 'overlap']",
        }
    }

    fn matches(self, row: i64) -> bool {
        match self {
            Self::Rare => row % 1000 == 0,
            Self::Medium | Self::Overlap => row % 100 == 0,
            Self::Common => row % 10 < 9,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Coverage {
    Complete,
    Partial,
}

#[derive(Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Route {
    Ordinary,
    Histogram,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReaderState {
    Warm,
    Cold,
}

#[derive(Serialize)]
struct Sample {
    reader_state: ReaderState,
    coverage: Coverage,
    days: i64,
    predicate: Predicate,
    route: Route,
    repetition: usize,
    sql: String,
    #[serde(flatten)]
    measurement: Measurement,
}

#[derive(Serialize)]
struct Measurement {
    elapsed_ms: f64,
    index_io: IndexIo,
    histogram_partitions: u64,
    unique_partitions: u64,
    delta_cache_hits: u64,
    parquet_prepares: u64,
    buckets: BTreeMap<i64, i64>,
}

#[derive(Serialize)]
struct IndexIo {
    blob_fetches: u64,
    blob_fetch_us: u64,
    index_opens: u64,
    index_open_us: u64,
    manifest_load_us: u64,
}

impl IndexIo {
    fn read(stats: &SearchStats) -> Self {
        Self {
            blob_fetches: stats.blob_fetches.load(Ordering::Relaxed),
            blob_fetch_us: stats.blob_fetch_us.load(Ordering::Relaxed),
            index_opens: stats.index_opens.load(Ordering::Relaxed),
            index_open_us: stats.index_open_us.load(Ordering::Relaxed),
            manifest_load_us: stats.manifest_load_us.load(Ordering::Relaxed),
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            blob_fetches: self.blob_fetches - before.blob_fetches,
            blob_fetch_us: self.blob_fetch_us - before.blob_fetch_us,
            index_opens: self.index_opens - before.index_opens,
            index_open_us: self.index_open_us - before.index_open_us,
            manifest_load_us: self.manifest_load_us - before.manifest_load_us,
        }
    }
}

fn record(project: &str, start: i64, spacing: i64, day: i64, row: i64, coverage: Coverage) -> Result<serde_json::Value> {
    let timestamp = start + day * DAY + row * spacing;
    let hashes = if coverage == Coverage::Partial {
        vec!["replacement"]
    } else {
        [("common", row % 10 != 9), ("medium", row % 100 == 0), ("overlap", row % 200 == 0), ("rare", row % 1000 == 0)]
            .into_iter()
            .filter_map(|(tag, present)| present.then_some(tag))
            .chain((row % 1000 == 0).then_some("rare"))
            .collect()
    };
    Ok(
        json!({"project_id": project, "timestamp": timestamp, "date": chrono::DateTime::from_timestamp_micros(timestamp).context("invalid date")?.date_naive().to_string(), "id": format!("{day}-{row}"), "hashes": hashes}),
    )
}

fn expected(start: i64, rows: i64, days: i64, predicate: Predicate, coverage: Coverage) -> BTreeMap<i64, i64> {
    let mut counts = BTreeMap::new();
    for day in 30 - days..30 {
        for row in 0..rows {
            if predicate.matches(row) && !(coverage == Coverage::Partial && row == 0) {
                *counts.entry((start + day * DAY + row * (DAY / rows)).div_euclid(HOUR) * HOUR).or_default() += 1;
            }
        }
    }
    counts
}

fn sql(project: &str, lo: i64, hi: i64, predicate: Predicate, route: Route) -> Result<String> {
    let lo = chrono::DateTime::from_timestamp_micros(lo).context("invalid start")?.format("%Y-%m-%d %H:%M:%S%.6f");
    let hi = chrono::DateTime::from_timestamp_micros(hi).context("invalid end")?.format("%Y-%m-%d %H:%M:%S%.6f");
    let count = if route == Route::Ordinary { "count(timestamp)" } else { "count(*)" };
    Ok(format!(
        "SELECT time_bucket('1 hour', timestamp), {count} FROM {TABLE} WHERE project_id='{project}' AND timestamp >= TIMESTAMP '{lo}' AND timestamp < TIMESTAMP '{hi}' AND {} GROUP BY 1 ORDER BY 1",
        predicate.sql()
    ))
}

async fn query(ctx: &SessionContext, search: &TantivySearchService, sql: &str) -> Result<Measurement> {
    let counters = || (search.stats.histogram_snapshots.load(Ordering::Relaxed), search.stats.histogram_unique_partitions.load(Ordering::Relaxed));
    let before = counters();
    let before_io = IndexIo::read(&search.stats);
    let before_cache = search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed);
    let before_prepares = search.stats.histogram_parquet_prepares.load(Ordering::Relaxed);
    let started = Instant::now();
    let batches = tokio::time::timeout(Duration::from_secs(30), async { ctx.sql(sql).await?.collect().await })
        .await
        .with_context(|| format!("query timed out: {sql}"))?
        .with_context(|| format!("query failed: {sql}"))?;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let after = counters();
    let index_io = IndexIo::read(&search.stats).since(before_io);
    let mut counts = BTreeMap::new();
    for batch in batches {
        let buckets = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().context("bucket is not a timestamp")?;
        let values = batch.column(1).as_any().downcast_ref::<Int64Array>().context("count is not Int64")?;
        for row in 0..batch.num_rows() {
            ensure!(counts.insert(buckets.value(row), values.value(row)).is_none(), "SQL returned a duplicate bucket");
        }
    }
    Ok(Measurement {
        buckets: counts,
        elapsed_ms,
        index_io,
        histogram_partitions: after.0 - before.0,
        unique_partitions: after.1 - before.1,
        delta_cache_hits: search.stats.histogram_delta_cache_hits.load(Ordering::Relaxed) - before_cache,
        parquet_prepares: search.stats.histogram_parquet_prepares.load(Ordering::Relaxed) - before_prepares,
    })
}

async fn database(config: Arc<AppConfig>, cache: &Path) -> Result<(Arc<Database>, Arc<TantivySearchService>)> {
    let db = Database::with_config(config.clone()).await?;
    let storage = format!("s3://timefusion-tests/{}/tantivy", config.core.timefusion_table_prefix);
    let store = db.create_object_store(&storage, &config.aws.build_storage_options(None)).await?;
    let search = Arc::new(TantivySearchService::new(store.clone(), cache.join("indexes"), Arc::new(config.tantivy.clone())));
    let indexer = Arc::new(TantivyIndexService::new(store, Arc::new(config.tantivy.clone())));
    indexer.with_reader(&search);
    Ok((Arc::new(db.with_tantivy_search(search.clone()).with_tantivy_indexer(indexer)), search))
}

// Fresh local metadata and reader caches; the local MinIO server and OS page
// cache remain warm. A cold query must recover the persisted count proof.
async fn cold_query(config: &AppConfig, sql: &str) -> Result<Measurement> {
    let dir = tempfile::tempdir()?;
    let mut config = config.clone();
    config.core.timefusion_data_dir = dir.path().into();
    let (db, search) = database(Arc::new(config), dir.path()).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let result = query(&ctx, &search, sql).await;
    db.shutdown().await?;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("timefusion::dml=warn,timefusion::database::histogram=warn").with_writer(std::io::stderr).init();
    let rows: i64 = std::env::args().skip(1).find(|arg| arg != "--bench").map_or(Ok(10_000), |arg| arg.parse()).context("rows per day must be an integer")?;
    ensure!((1000..=1_000_000).contains(&rows), "rows per day must be between 1,000 and 1,000,000");
    let started = Instant::now();
    let dir = tempfile::tempdir()?;
    let project = uuid::Uuid::new_v4().to_string();
    let config = minio_test_config(&project, dir.path().to_str().context("non-UTF8 cache path")?);
    let (db, search) = database(config.clone(), dir.path()).await?;
    let indexer = db.tantivy_indexer().context("benchmark indexer is missing")?;
    let prefix = &config.core.timefusion_table_prefix;
    let start = chrono::DateTime::parse_from_rfc3339("2026-08-01T00:00:00Z")?.timestamp_micros();
    for day in 0..30 {
        let records = (0..rows).map(|row| record(&project, start, DAY / rows, day, row, Coverage::Complete)).collect::<Result<Vec<_>>>()?;
        db.insert_records_batch(&project, TABLE, vec![json_to_batch_for(TABLE, records)?], true, None).await?;
    }
    let table = db.resolve_table(&project, TABLE).await?;
    let store = table.read().await.log_store().object_store(None);
    for uri in db.list_file_uris(&project, TABLE).await? {
        indexer.build_index_for_file(TABLE, &project, parquet_rel_of_uri(&uri).context("missing Parquet path")?, &uri, store.clone()).await?;
    }
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let setup_ms = started.elapsed().as_secs_f64() * 1000.0;
    let warming = Instant::now();
    for day in 0..30 {
        let sql = sql(&project, start + day * DAY, start + (day + 1) * DAY, Predicate::Common, Route::Histogram)?;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let measurement = query(&ctx, &search, &sql).await?;
            ensure!(measurement.histogram_partitions == 1, "warmup did not reach the histogram path");
            if measurement.unique_partitions == 1 {
                break;
            }
            ensure!(Instant::now() < deadline, "daily uniqueness proof did not become ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let warmup_ms = warming.elapsed().as_secs_f64() * 1000.0;
    let mut samples = Vec::new();
    for coverage in [Coverage::Complete, Coverage::Partial] {
        if coverage == Coverage::Partial {
            let records = (0..30).map(|day| record(&project, start, DAY / rows, day, 0, coverage)).collect::<Result<Vec<_>>>()?;
            db.insert_records_batch(&project, TABLE, vec![json_to_batch_for(TABLE, records)?], true, None).await?;
        }
        for days in [3, 7, 30] {
            for predicate in [Predicate::Rare, Predicate::Medium, Predicate::Common, Predicate::Overlap] {
                let expected = expected(start, rows, days, predicate, coverage);
                for (repetition, route, reader_state) in (0..4)
                    .flat_map(|rep| {
                        let routes = if rep % 2 == 0 { [Route::Ordinary, Route::Histogram] } else { [Route::Histogram, Route::Ordinary] };
                        routes.map(move |route| (rep, route, ReaderState::Warm))
                    })
                    .chain([Route::Ordinary, Route::Histogram].map(|route| (0, route, ReaderState::Cold)))
                {
                    let sql = sql(&project, start + (30 - days) * DAY, start + 30 * DAY, predicate, route)?;
                    let measurement = match reader_state {
                        ReaderState::Warm => query(&ctx, &search, &sql).await?,
                        ReaderState::Cold => cold_query(&config, &sql).await?,
                    };
                    ensure!(measurement.buckets == expected, "bucket mismatch for {sql}");
                    let expected_partitions = if route == Route::Histogram { u64::try_from(days)? } else { 0 };
                    if measurement.histogram_partitions != expected_partitions {
                        let state = ctx.state();
                        let plan = state.create_logical_plan(&sql).await?;
                        anyhow::bail!(
                            "unexpected SQL execution route ({coverage:?}, repetition {repetition}): {} histogram partitions, expected {expected_partitions}; {sql}\n{}",
                            measurement.histogram_partitions,
                            state.optimize(&plan)?.display_indent()
                        );
                    }
                    ensure!(
                        measurement.unique_partitions == if coverage == Coverage::Complete { expected_partitions } else { 0 },
                        "unexpected uniqueness route"
                    );
                    if coverage == Coverage::Partial && route == Route::Histogram && matches!(reader_state, ReaderState::Warm) && repetition > 0 {
                        ensure!(measurement.delta_cache_hits == expected_partitions, "warm partial SQL must reuse every day's Delta visibility");
                        ensure!(measurement.parquet_prepares == expected_partitions, "warm partial SQL must only prepare its unindexed replacements");
                    }
                    samples.push(Sample { reader_state, coverage, days, predicate, route, repetition, sql, measurement });
                }
            }
        }
    }
    db.shutdown().await?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"table": TABLE, "rows_per_day": rows, "project": project, "local_minio_prefix": prefix, "profile": if cfg!(debug_assertions) { "debug" } else { "optimized" }, "os": std::env::consts::OS, "arch": std::env::consts::ARCH, "foyer_disabled": config.cache.timefusion_foyer_disabled, "setup_ms": setup_ms, "warmup_ms": warmup_ms, "samples": samples})
        )?
    );
    Ok(())
}
