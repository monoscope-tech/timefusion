//! Regression: copy A of a row flushes to Delta, then a retry (copy B) lands
//! in a different Delta file in the same `(project_id, date)` partition.
//! Flush-time dedup runs per-bucket and cannot see across files, so without
//! a Delta-vs-Delta compaction pass the duplicate persists forever.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{Array, Int64Array};
use deltalake::DeltaTable;
use serial_test::serial;
use timefusion::{
    database::Database,
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch, test_span_ts},
};

/// Sum `stats.numRecords` across all Add files in the (project_id, date)
/// partition. Bypasses the read-side dedup wrapper so we can assert on the
/// raw on-disk row set.
async fn rows_in_partition(table: &DeltaTable, project_id: &str, date: &str) -> Result<i64> {
    let snap = table.snapshot()?;
    let actions = snap.add_actions_table(true)?;
    let n = actions.num_rows();
    let proj_col = actions.column_by_name("partition.project_id");
    let date_col = actions.column_by_name("partition.date");
    let nrec = actions
        .column_by_name("num_records")
        .or_else(|| actions.column_by_name("stats.numRecords"))
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| anyhow::anyhow!("num_records column missing or wrong type"))?;
    let mut sum = 0i64;
    for i in 0..n {
        let proj_ok =
            proj_col.is_none() || proj_col.map(|c| arrow::util::display::array_value_to_string(c, i).ok()) == Some(Some(project_id.into()));
        let date_ok = date_col.map(|c| arrow::util::display::array_value_to_string(c, i).ok()) == Some(Some(date.into()));
        if proj_ok && date_ok && !nrec.is_null(i) {
            sum += nrec.value(i);
        }
    }
    Ok(sum)
}

#[serial]
#[tokio::test]
async fn dedup_compaction_collapses_cross_flush_duplicates() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_compaction").with_buffer_mode(BufferMode::Enabled).build();
    // SAFETY: walrus-rust reads WALRUS_DATA_DIR from process env. `#[serial]`
    // serializes the suite; this set is racy in principle but safe here.
    unsafe { std::env::set_var("WALRUS_DATA_DIR", &cfg.core.timefusion_data_dir) };
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Pick a fixed timestamp so both inserts share (id, timestamp) and date.
    let ts = chrono::Utc::now().timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };

    // Two skip_queue=true inserts → two independent Delta commits, two files
    // in the same (project_id, date) partition. This is the cross-flush
    // scenario in production: bucket A flushes, then a client retry arrives
    // in a fresh bucket B and flushes separately.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Verify the duplicate really landed in Delta as two separate file rows.
    // We assert on raw Delta stats (not SQL COUNT) because Phase 2's read
    // wrapper now collapses cross-file dupes on every query — the bug we
    // care about here is the on-disk state, which only Phase 1's compaction
    // sweep fixes.
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let date_str = date.to_string();
    let part_marker = format!("project_id={}/date={}", project_id, date_str);
    let file_count_before = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&part_marker)).count();
    assert!(file_count_before >= 2, "expected >=2 files in partition before dedup, got {}", file_count_before);

    let pre_rows = rows_in_partition(&*table_ref.read().await, &project_id, &date_str).await?;
    assert_eq!(pre_rows, 2, "pre-dedup: on-disk partition should hold the duplicate as 2 rows");

    // Run the new dedup compaction on the partition.
    let dropped = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!(dropped, 1, "expected exactly one duplicate row dropped");

    // After dedup the on-disk row count for this partition is 1.
    let post_rows = rows_in_partition(&*table_ref.read().await, &project_id, &date_str).await?;
    assert_eq!(post_rows, 1, "post-dedup: partition should hold a single row on disk");

    Ok(())
}
