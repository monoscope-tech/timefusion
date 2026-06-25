//! Sort-layout micro-benchmark.
//!
//! Writes the same synthetic dataset to Parquet under three candidate sort
//! layouts, then times representative queries against each via DataFusion
//! (which honours row-group min/max stats and Parquet bloom filters for
//! pruning).
//!
//! Layouts:
//!   A — sort by (timestamp, id)                          — 2-col PK
//!   B — sort by (timestamp, service_name, id)            — 3-col PK
//!   C — sort by (level, status_code, service_name, ts)   — pre-change baseline
//!
//! Queries:
//!   Q1 — point lookup    `timestamp = T AND id = X`
//!   Q2 — service in time `timestamp BETWEEN .. AND service_name = X`
//!   Q3 — time range only `timestamp BETWEEN ..`
//!   Q4 — service only    `service_name = X`
//!
//! Reports wall time, file size, and (row groups read / total).

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    compute::{SortColumn, SortOptions, lexsort_to_indices, take},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use datafusion::{execution::context::SessionContext, prelude::ParquetReadOptions};
use deltalake::datafusion::parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::{
        properties::{EnabledStatistics, WriterProperties},
        reader::{FileReader, SerializedFileReader},
    },
};

const N_ROWS: usize = 200_000;
const N_SERVICES: usize = 20;
const TS_SPAN_SECS: i64 = 3600; // 1 hour
const ROW_GROUP_SIZE: usize = 8_000;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("resource___service___name", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("status_code", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int32, true),
    ]))
}

fn generate_batch(seed_offset: usize) -> RecordBatch {
    let base_ts: i64 = 1_700_000_000_000_000; // 2023-11-14
    let mut ts = Vec::with_capacity(N_ROWS);
    let mut id = Vec::with_capacity(N_ROWS);
    let mut svc = Vec::with_capacity(N_ROWS);
    let mut level = Vec::with_capacity(N_ROWS);
    let mut status = Vec::with_capacity(N_ROWS);
    let mut name = Vec::with_capacity(N_ROWS);
    let mut sev = Vec::with_capacity(N_ROWS);
    for i in 0..N_ROWS {
        // Spread timestamps evenly across the span, with µs precision.
        let t = base_ts + ((i as i64) * (TS_SPAN_SECS * 1_000_000) / N_ROWS as i64);
        ts.push(t);
        id.push(format!("id_{:08x}", i + seed_offset));
        svc.push(format!("svc_{:02}", (i + seed_offset) % N_SERVICES));
        level.push(["INFO", "WARN", "ERROR", "DEBUG"][i % 4].to_string());
        status.push(["OK", "ERROR", "UNSET"][i % 3].to_string());
        name.push(format!("op_{}", i % 50));
        sev.push(((i % 100) as i32) + 1);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")) as ArrayRef,
            Arc::new(StringArray::from(id)),
            Arc::new(StringArray::from(svc)),
            Arc::new(StringArray::from(level)),
            Arc::new(StringArray::from(status)),
            Arc::new(StringArray::from(name)),
            Arc::new(Int32Array::from(sev)),
        ],
    )
    .unwrap()
}

/// Sort a batch by the supplied (column-name, descending) pairs, returning a new batch.
fn sort_batch(batch: &RecordBatch, by: &[&str]) -> RecordBatch {
    let cols: Vec<SortColumn> = by
        .iter()
        .map(|name| SortColumn {
            values:  batch.column(batch.schema().index_of(name).unwrap()).clone(),
            options: Some(SortOptions {
                descending:  false,
                nulls_first: false,
            }),
        })
        .collect();
    let indices = lexsort_to_indices(&cols, None).unwrap();
    let sorted_cols: Vec<ArrayRef> = batch.columns().iter().map(|c| take(c.as_ref(), &indices, None).unwrap()).collect();
    RecordBatch::try_new(batch.schema(), sorted_cols).unwrap()
}

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(0.01)
        .set_bloom_filter_ndv(100_000)
        .build()
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_props())).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn row_group_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    SerializedFileReader::new(file).unwrap().metadata().num_row_groups()
}

async fn time_query(ctx: &SessionContext, sql: &str, iters: u32) -> (f64, usize) {
    // Warm-up
    let df = ctx.sql(sql).await.unwrap();
    let rows: usize = df.collect().await.unwrap().iter().map(|b| b.num_rows()).sum();
    let start = Instant::now();
    for _ in 0..iters {
        let df = ctx.sql(sql).await.unwrap();
        let _ = df.collect().await.unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
    (elapsed, rows)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().to_path_buf();
    println!("Generating {} rows...", N_ROWS);
    let raw = generate_batch(0);

    let layouts: &[(&str, &[&str])] = &[
        ("A_ts_id", &["timestamp", "id"]),
        ("B_ts_svc_id", &["timestamp", "resource___service___name", "id"]),
        ("C_level_status_svc_ts", &["level", "status_code", "resource___service___name", "timestamp"]),
    ];

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    for (name, sort_by) in layouts {
        let sorted = sort_batch(&raw, sort_by);
        let path = base.join(format!("{name}.parquet"));
        write_parquet(&path, &sorted);
        let rg = row_group_count(&path);
        println!("Layout {:<22} {:>8} bytes  {:>3} row groups", name, file_size(&path), rg);
        files.push((name.to_string(), path));
    }

    // Pick a target row from the middle of the dataset.
    let target_idx = N_ROWS / 2;
    let ts_array = raw.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let id_array = raw.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let target_ts = ts_array.value(target_idx);
    let target_id = id_array.value(target_idx).to_string();
    let target_svc = format!("svc_{:02}", target_idx % N_SERVICES);

    // Time window covering a small fraction (~6 minutes = 0.1 hour) of the dataset.
    let win_start = target_ts - 3 * 60 * 1_000_000;
    let win_end = target_ts + 3 * 60 * 1_000_000;
    let ts_lit = |t: i64| format!("TIMESTAMP '1970-01-01 00:00:00 UTC' + INTERVAL '{} microseconds'", t);

    let queries: Vec<(&str, String)> = vec![
        (
            "Q1_point_lookup",
            format!("SELECT id FROM t WHERE timestamp = {} AND id = '{}'", ts_lit(target_ts), target_id),
        ),
        (
            "Q2_service_in_time",
            format!(
                "SELECT count(*) FROM t WHERE timestamp >= {} AND timestamp <= {} AND resource___service___name = '{}'",
                ts_lit(win_start),
                ts_lit(win_end),
                target_svc
            ),
        ),
        (
            "Q3_time_range",
            format!(
                "SELECT count(*) FROM t WHERE timestamp >= {} AND timestamp <= {}",
                ts_lit(win_start),
                ts_lit(win_end)
            ),
        ),
        (
            "Q4_service_only",
            format!("SELECT count(*) FROM t WHERE resource___service___name = '{}'", target_svc),
        ),
    ];

    println!("\nTimings (ms, mean over 30 iters; rows = result row count):");
    println!("{:<24} {:>14} {:>14} {:>14}", "query", "A_ts_id", "B_ts_svc_id", "C_orig");

    for (qname, sql) in &queries {
        let mut row = format!("{:<24}", qname);
        for (lname, path) in &files {
            let ctx = SessionContext::new();
            ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
            // Toggle pushdown + bloom-filter pruning so layouts compete fairly.
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.pushdown_filters = true;
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.reorder_filters = true;
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.bloom_filter_on_read = true;
            let (ms, rows) = time_query(&ctx, sql, 30).await;
            row.push_str(&format!(" {:>10.3}ms({})", ms, rows));
            let _ = lname;
        }
        println!("{}", row);
    }
}
