//! `RangeParallelDedup` splits a wide aggregate window into disjoint timestamp
//! ranges so each range's `DedupExec` runs on its own thread. The split is only
//! sound because `timestamp` LEADS the dedup key, so a row's versions can never
//! fall either side of a boundary.
//!
//! These tests exist because the failure mode is silent: a gap between branches
//! drops rows and an overlap double-counts them, and either way the query still
//! returns a plausible number.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
use serial_test::serial;
use timefusion::{
    database::Database,
    support::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span_ts},
};

const DAYS: i64 = 30;
const BRANCHES: usize = 4;
const DAY_MICROS: i64 = 24 * 3_600 * 1_000_000;

/// One row per day for 30 days, inserted TWICE as two independent Delta commits
/// so every logical row exists as two physical versions in different files —
/// the shape read-side dedup exists to collapse. Returns (db, project, window).
async fn seeded(label: &str) -> Result<(Arc<Database>, String, i64, i64)> {
    timefusion::read::optimizers::set_range_split_branches(BRANCHES);
    let cfg = TestConfigBuilder::new(label).with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Anchor on a whole day so branch boundaries land on round numbers, then
    // put a row at each day AND at each branch boundary — the boundary rows are
    // the ones a half-open/closed mistake loses or counts twice.
    let end = chrono::Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    let start = end - DAYS * DAY_MICROS;
    let step = (end - start) / BRANCHES as i64;
    let boundaries: Vec<i64> = (0..BRANCHES as i64).map(|i| start + step * i).collect();
    let stamps: Vec<i64> = (0..DAYS).map(|d| start + d * DAY_MICROS + 3_600 * 1_000_000).chain(boundaries).collect();

    let rows = |name: &str| -> Result<_> {
        json_to_batch(stamps.iter().enumerate().map(|(i, ts)| test_span_ts(&format!("row_{i}"), name, &project_id, *ts)).collect())
    };
    // Two commits => two files per partition holding the same logical rows.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![rows("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![rows("second")?], true, None).await?;
    Ok((db, project_id, start, end))
}

async fn scalar(db: &Arc<Database>, sql: &str) -> Result<i64> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx.sql(sql).await?.collect().await?[0].column(0).as_primitive::<Int64Type>().value(0))
}

/// The whole point: a wide window must split, and still count every logical row
/// exactly once. Asserting the count alone would pass vacuously if the rule
/// silently declined, so the plan is checked too.
#[serial]
#[tokio::test]
async fn wide_window_splits_and_still_counts_each_row_once() -> Result<()> {
    let (db, project_id, start, end) = seeded("range_split_counts").await?;
    let expected = DAYS + BRANCHES as i64; // one row per day, plus the boundary rows

    let window = format!(
        "project_id = '{project_id}' AND timestamp >= '{}'::timestamp AND timestamp <= '{}'::timestamp",
        chrono::DateTime::from_timestamp_micros(start).unwrap().to_rfc3339(),
        chrono::DateTime::from_timestamp_micros(end).unwrap().to_rfc3339(),
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let plan = ctx.sql(&format!("EXPLAIN SELECT count(*) FROM otel_logs_and_spans WHERE {window}")).await?.collect().await?;
    let rendered: String = plan
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|r| timefusion::support::test_helpers::array_get_str(b.column(1).as_ref(), r)).collect::<Vec<_>>())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("Union"), "a {DAYS}-day aggregate must split into branches:\n{rendered}");

    let counted = scalar(&db, &format!("SELECT count(*) FROM otel_logs_and_spans WHERE {window}")).await?;
    assert_eq!(counted, expected, "split count must equal the logical row count (duplicates collapsed exactly once)");
    Ok(())
}

/// A narrow window has nothing to gain and must keep the un-split plan, so the
/// hot dashboard path is untouched by this rule.
#[serial]
#[tokio::test]
async fn narrow_window_is_left_alone() -> Result<()> {
    let (db, project_id, _, end) = seeded("range_split_narrow").await?;
    let window = format!(
        "project_id = '{project_id}' AND timestamp >= '{}'::timestamp AND timestamp <= '{}'::timestamp",
        chrono::DateTime::from_timestamp_micros(end - 2 * DAY_MICROS).unwrap().to_rfc3339(),
        chrono::DateTime::from_timestamp_micros(end).unwrap().to_rfc3339(),
    );
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let plan = ctx.sql(&format!("EXPLAIN SELECT count(*) FROM otel_logs_and_spans WHERE {window}")).await?.collect().await?;
    let rendered: String = plan
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|r| timefusion::support::test_helpers::array_get_str(b.column(1).as_ref(), r)).collect::<Vec<_>>())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!rendered.contains("Union\n"), "a 2-day window must not split:\n{rendered}");
    Ok(())
}
