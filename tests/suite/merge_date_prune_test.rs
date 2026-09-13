//! A raw `timestamp` predicate cannot prune the `date` partition of a
//! `[project_id, date]`-partitioned table; `with_date_partition_filters`
//! derives the `date` bounds so a MERGE scans only the touched partition.

use std::sync::Arc;

use anyhow::Result;
use datafusion::{
    arrow::{
        array::{Date32Array, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray},
        buffer::OffsetBuffer,
        datatypes::{DataType, Field, Schema, TimeUnit},
    },
    logical_expr::{BinaryExpr, Expr, Operator},
    prelude::{SessionContext, col, lit},
    scalar::ScalarValue,
};
use deltalake::DeltaTable;
use timefusion::read::optimizers::time_range_partition_pruner::with_date_partition_filters;

const PROJECT: &str = "prune-proj";
const BASE_DAY: i32 = 19_723; // 2024-01-01, days since epoch
const NUM_DAYS: i32 = 5;
const MID_DAY: i32 = BASE_DAY + 2; // the one date the update targets

/// Noon UTC of `day`, in microseconds.
fn day_to_micros(day: i32) -> i64 {
    (day as i64 * 86_400 + 43_200) * 1_000_000
}

fn schema() -> Arc<Schema> {
    let hashes = Field::new_list("hashes", Field::new("item", DataType::Utf8, true), true);
    Arc::new(Schema::new(vec![
        Field::new("project_id", DataType::Utf8, false),
        Field::new("context___span_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        hashes,
        Field::new("date", DataType::Date32, false),
    ]))
}

const SPAN_ID: &str = "span-mid";

/// One file's worth of rows for the `date=day` partition. The `timestamp` and
/// `context___span_id` values are deliberately identical across every file, so
/// column stats prune nothing and only the `date` partition value can.
fn batch_for_day(day: i32) -> Result<RecordBatch> {
    let n = 2;
    let empty_hashes = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(vec![0i32; n + 1].into()),
        Arc::new(StringArray::from(Vec::<&str>::new())),
        None,
    );
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from(vec![PROJECT; n])),
            Arc::new(StringArray::from(vec![SPAN_ID; n])),
            Arc::new(TimestampMicrosecondArray::from(vec![day_to_micros(MID_DAY); n]).with_timezone("UTC")),
            Arc::new(empty_hashes),
            Arc::new(Date32Array::from(vec![day; n])),
        ],
    )?)
}

/// Create a `[project_id, date]`-partitioned table with one file per day.
/// The first `write` auto-creates it — partition columns are honored only on
/// creation.
async fn build_table(uri: &str) -> Result<()> {
    let mut table = DeltaTable::try_from_url(url::Url::parse(uri)?).await?;
    for day in BASE_DAY..BASE_DAY + NUM_DAYS {
        table = table.write(vec![batch_for_day(day)?]).with_partition_columns(["project_id", "date"]).await?;
    }
    Ok(())
}

/// `project_id = P AND timestamp >= lo AND timestamp < hi` — the user's WHERE
/// predicate for the mid-day update, minus the join key.
fn user_predicate() -> Expr {
    let ts = || col("target.timestamp");
    let lit_ts = |m: i64| Expr::Literal(ScalarValue::TimestampMicrosecond(Some(m), Some("UTC".into())), None);
    col("target.project_id")
        .eq(lit(PROJECT))
        // 1 h window entirely within MID_DAY, so both bounds map to one `date`.
        .and(Expr::BinaryExpr(BinaryExpr::new(Box::new(ts()), Operator::GtEq, Box::new(lit_ts(day_to_micros(MID_DAY) - 3_600_000_000)))))
        .and(Expr::BinaryExpr(BinaryExpr::new(Box::new(ts()), Operator::Lt, Box::new(lit_ts(day_to_micros(MID_DAY) + 3_600_000_000)))))
}

/// Run the merge for the mid-day span, return files scanned during the merge.
async fn merge_files_scanned(table: DeltaTable, predicate: Expr) -> Result<usize> {
    let src_schema = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false)]));
    let src = RecordBatch::try_new(src_schema, vec![Arc::new(StringArray::from(vec![SPAN_ID]))])?;
    let source_df = SessionContext::new().read_batch(src)?;

    let join_pred = col("target.context___span_id").eq(col("source.span_id")).and(predicate);

    let (_t, metrics) = table
        .merge(source_df, join_pred)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(SessionContext::new().state()) as Arc<dyn datafusion::catalog::Session>)
        .when_matched_update(|u| u.update("timestamp", col("target.timestamp")))? // no-op set; we only measure the scan
        .await?;
    Ok(metrics.num_target_files_scanned)
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_prunes_to_single_date_partition() -> Result<()> {
    let dir = tempfile::tempdir()?;
    // Two identical tables: a merge commits and rewrites, so each arm needs its
    // own pristine baseline.
    let uri = |name: &str| format!("file://{}/{name}", dir.path().display());
    for name in ["raw", "pruned"] {
        std::fs::create_dir_all(dir.path().join(name))?;
        build_table(&uri(name)).await?;
    }
    let (raw_uri, pruned_uri) = (uri("raw"), uri("pruned"));

    // Only `timestamp` bounds: delta cannot prune `date`, so all files scan.
    let raw_scanned = merge_files_scanned(DeltaTable::try_from_url(url::Url::parse(&raw_uri)?).await?, user_predicate()).await?;

    let pruned_pred = with_date_partition_filters(user_predicate(), "timestamp");
    let pruned_scanned = merge_files_scanned(DeltaTable::try_from_url(url::Url::parse(&pruned_uri)?).await?, pruned_pred).await?;

    assert_eq!(raw_scanned, NUM_DAYS as usize, "raw timestamp predicate should scan every date partition (reproduces the OOM)");
    assert_eq!(pruned_scanned, 1, "date-augmented predicate should prune to the single touched partition");
    Ok(())
}
