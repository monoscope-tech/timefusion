//! Compare in-process Arrow and spillable DataFusion sorts at flush-bucket sizes.
//!
//! An unsorted flush file disables ordering for every scan of its partition;
//! this benchmark identifies when a DataFusion-sort escalation is worthwhile.

use std::{sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray},
    compute::{SortColumn, SortOptions, lexsort_to_indices, take},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    datasource::MemTable,
    prelude::{SessionConfig, SessionContext},
};

/// Wide-ish rows: the cost that matters is per-ROW comparison plus the `take`
/// of every payload column, and otel rows carry fat string payloads.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(
        [
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("level", DataType::Int32, true),
        ]
        .into_iter()
        .chain((0..20).map(|i| Field::new(format!("body_{i}"), DataType::Utf8, true)))
        .collect::<Vec<_>>(),
    ))
}

fn batch(rows: usize, seed: u64) -> RecordBatch {
    let mut ts = Vec::with_capacity(rows);
    let mut ids = Vec::with_capacity(rows);
    let mut svc = Vec::with_capacity(rows);
    let mut lvl = Vec::with_capacity(rows);
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    for i in 0..rows {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ts.push(1_700_000_000_000_000i64 + ((x >> 17) % 600_000_000) as i64);
        ids.push(format!("{:032x}", x ^ (i as u64)));
        svc.push(format!("svc-{}", x % 24));
        lvl.push((x % 5) as i32);
    }
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
        Arc::new(StringArray::from(ids)),
        Arc::new(StringArray::from(svc)),
        Arc::new(Int32Array::from(lvl)),
    ];
    cols.extend(
        (0..20).map(|i| Arc::new(StringArray::from((0..rows).map(|r| format!("payload-{i}-{r:06}-xxxxxxxxxxxxxxxxxxxx")).collect::<Vec<_>>())) as ArrayRef),
    );
    RecordBatch::try_new(schema(), cols).unwrap()
}

/// What the flush path does today: concat + lexsort + take, all in process.
fn arrow_sort(batches: &[RecordBatch]) -> usize {
    let one = arrow::compute::concat_batches(&schema(), batches).unwrap();
    let opts = SortOptions { descending: true, nulls_first: true };
    let keys = vec![
        SortColumn { values: one.column(0).clone(), options: Some(opts) },
        SortColumn { values: one.column(1).clone(), options: Some(SortOptions { descending: false, nulls_first: false }) },
    ];
    let idx = lexsort_to_indices(&keys, None).unwrap();
    let out: Vec<ArrayRef> = one.columns().iter().map(|c| take(c, &idx, None).unwrap()).collect();
    RecordBatch::try_new(schema(), out).unwrap().num_rows()
}

/// What compaction now does: sort inside a DataFusion plan and stream it.
async fn datafusion_sort(batches: Vec<RecordBatch>, ctx: &SessionContext) -> usize {
    let mem = MemTable::try_new(schema(), vec![batches]).unwrap();
    let name = format!("s{}", uuid::Uuid::new_v4().simple());
    ctx.register_table(&name, Arc::new(mem)).unwrap();
    let df = ctx.sql(&format!("SELECT * FROM {name} ORDER BY \"timestamp\" DESC NULLS FIRST, \"id\" ASC NULLS LAST")).await.unwrap();
    let rows = df.collect().await.unwrap().iter().map(RecordBatch::num_rows).sum();
    let _ = ctx.deregister_table(&name);
    rows
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    const BATCH_ROWS: usize = 8192;
    let sizes = [8_192usize, 65_536, 262_144, 1_048_576];
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));

    println!("{:>10} {:>10} {:>12} {:>12} {:>10}", "rows", "MB(arrow)", "arrow_ms", "datafusion_ms", "ratio");
    for rows in sizes {
        let batches: Vec<RecordBatch> = (0..rows.div_ceil(BATCH_ROWS)).map(|i| batch(BATCH_ROWS.min(rows), i as u64)).collect();
        let mb = batches.iter().map(|b| b.get_array_memory_size()).sum::<usize>() as f64 / 1e6;

        let _ = arrow_sort(&batches);
        let t = Instant::now();
        let arrow_rows = arrow_sort(&batches);
        let arrow_ms = t.elapsed().as_secs_f64() * 1e3;

        let _ = datafusion_sort(batches.clone(), &ctx).await;
        let t = Instant::now();
        let datafusion_rows = datafusion_sort(batches.clone(), &ctx).await;
        let df_ms = t.elapsed().as_secs_f64() * 1e3;

        assert_eq!(arrow_rows, datafusion_rows, "both strategies must emit the same row count");
        println!("{rows:>10} {mb:>10.1} {arrow_ms:>12.1} {df_ms:>13.1} {:>10.2}", df_ms / arrow_ms);
    }
    println!("\nratio < 1 => DataFusion is faster at that size.");
    println!("The flush threshold should sit where the ratio crosses 1 (below it keep the");
    println!("in-process sort for ingest latency; above it ESCALATE rather than skip).");
}
