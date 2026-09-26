//! Sort-layout micro-benchmark: writes one synthetic dataset to Parquet under
//! three candidate sort layouts and times representative queries against each,
//! reporting wall time, file size and row-group counts.

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
const BASE_TIMESTAMP: i64 = 1_700_000_000_000_000;
const TS_SPAN_SECS: i64 = 3600;
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
    let rows = 0..N_ROWS;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(
            TimestampMicrosecondArray::from_iter_values(rows.clone().map(|i| BASE_TIMESTAMP + ((i as i64) * (TS_SPAN_SECS * 1_000_000) / N_ROWS as i64)))
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter_values(rows.clone().map(|i| format!("id_{:08x}", i + seed_offset)))),
        Arc::new(StringArray::from_iter_values(rows.clone().map(|i| format!("svc_{:02}", (i + seed_offset) % N_SERVICES)))),
        Arc::new(StringArray::from_iter_values(rows.clone().map(|i| ["INFO", "WARN", "ERROR", "DEBUG"][i % 4]))),
        Arc::new(StringArray::from_iter_values(rows.clone().map(|i| ["OK", "ERROR", "UNSET"][i % 3]))),
        Arc::new(StringArray::from_iter_values(rows.clone().map(|i| format!("op_{}", i % 50)))),
        Arc::new(Int32Array::from_iter_values(rows.map(|i| ((i % 100) as i32) + 1))),
    ];
    RecordBatch::try_new(schema(), columns).unwrap()
}

fn sort_batch(batch: &RecordBatch, by: &[&str]) -> RecordBatch {
    let cols: Vec<SortColumn> = by
        .iter()
        .map(|name| SortColumn {
            values: batch.column(batch.schema().index_of(name).unwrap()).clone(),
            options: Some(SortOptions { descending: false, nulls_first: false }),
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
        .set_bloom_filter_max_ndv(100_000)
        .build()
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(writer_props())).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |metadata| metadata.len())
}

fn row_group_count(path: &Path) -> usize {
    let file = File::open(path).unwrap();
    SerializedFileReader::new(file).unwrap().metadata().num_row_groups()
}

async fn time_query(ctx: &SessionContext, sql: &str, iters: u32) -> (f64, usize) {
    let rows: usize = ctx.sql(sql).await.unwrap().collect().await.unwrap().iter().map(|b| b.num_rows()).sum();
    let start = Instant::now();
    for _ in 0..iters {
        let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let elapsed = start.elapsed().as_secs_f64() / iters as f64 * 1000.0;
    (elapsed, rows)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let tmp = tempfile::tempdir().unwrap();
    println!("Generating {} rows...", N_ROWS);
    let raw = generate_batch(0);

    let layouts: [(&str, &[&str]); 3] = [
        ("A_ts_id", &["timestamp", "id"]),
        ("B_ts_svc_id", &["timestamp", "resource___service___name", "id"]),
        ("C_level_status_svc_ts", &["level", "status_code", "resource___service___name", "timestamp"]),
    ];

    let files: Vec<(&str, PathBuf)> = layouts
        .iter()
        .map(|(name, sort_by)| {
            let path = tmp.path().join(format!("{name}.parquet"));
            write_parquet(&path, &sort_batch(&raw, sort_by));
            println!("Layout {:<22} {:>8} bytes  {:>3} row groups", name, file_size(&path), row_group_count(&path));
            (*name, path)
        })
        .collect();

    let target_idx = N_ROWS / 2;
    let ts_array = raw.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let id_array = raw.column(1).as_any().downcast_ref::<StringArray>().unwrap();
    let target_ts = ts_array.value(target_idx);
    let target_id = id_array.value(target_idx).to_string();
    let target_svc = format!("svc_{:02}", target_idx % N_SERVICES);

    // ~6-minute window, a small fraction of the dataset's 1-hour span.
    let win_start = target_ts - 3 * 60 * 1_000_000;
    let win_end = target_ts + 3 * 60 * 1_000_000;
    let ts_lit = |t: i64| format!("TIMESTAMP '1970-01-01 00:00:00 UTC' + INTERVAL '{} microseconds'", t);

    let queries = [
        ("Q1_point_lookup", format!("SELECT id FROM t WHERE timestamp = {} AND id = '{}'", ts_lit(target_ts), target_id)),
        (
            "Q2_service_in_time",
            format!(
                "SELECT count(*) FROM t WHERE timestamp >= {} AND timestamp <= {} AND resource___service___name = '{}'",
                ts_lit(win_start),
                ts_lit(win_end),
                target_svc
            ),
        ),
        ("Q3_time_range", format!("SELECT count(*) FROM t WHERE timestamp >= {} AND timestamp <= {}", ts_lit(win_start), ts_lit(win_end))),
        ("Q4_service_only", format!("SELECT count(*) FROM t WHERE resource___service___name = '{}'", target_svc)),
    ];

    println!("\nTimings (ms, mean over 30 iters; rows = result row count):");
    println!("{:<24} {:>14} {:>14} {:>14}", "query", "A_ts_id", "B_ts_svc_id", "C_orig");

    for (qname, sql) in &queries {
        let mut row = format!("{:<24}", qname);
        for (_, path) in &files {
            let ctx = SessionContext::new();
            ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
            // Toggle pushdown + bloom-filter pruning so layouts compete fairly.
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.pushdown_filters = true;
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.reorder_filters = true;
            ctx.state_ref().write().config_mut().options_mut().execution.parquet.bloom_filter_on_read = true;
            let (ms, rows) = time_query(&ctx, sql, 30).await;
            row.push_str(&format!(" {:>10.3}ms({})", ms, rows));
        }
        println!("{}", row);
    }
}
