//! Regression guard for the "recent 1h window is slow" bug (2026-07-20).
//!
//! A compacted hot partition is one big timestamp-DESC-sorted file (in prod a
//! single 300k-row row group spanning ~4h). A `timestamp > now()-1h` query
//! should read only the newest rows via parquet page-index + row pushdown — the
//! file is sorted and carries a per-page timestamp ColumnIndex. But with the
//! Deletion-Vectors table feature enabled (prod default), the delta-rs scan
//! DROPPED the parquet predicate entirely (`process_filters` gated all parquet
//! pushdown on `!DeletionVectors`), so the scan read every row and filtered in a
//! FilterExec above it — a ~13× over-read. The per-file `has_selection_vectors`
//! guard already skips pushdown for files that actually carry a DV, so the
//! blanket feature-level gate was unnecessary and is what this test locks down.

use std::time::Duration;

use super::harness::{E2eEnv, FROZEN_START_MICROS, insert_at};

/// Parse a scalar DataSourceExec metric `name=N` (first digit run after `name`).
fn scan_metric(plan: &str, name: &str) -> Option<i64> {
    let i = plan.rfind(name)?;
    plan[i + name.len()..].split(|c: char| !c.is_ascii_digit()).find(|s| !s.is_empty())?.parse().ok()
}

async fn explain_analyze(client: &tokio_postgres::Client, sql: &str) -> anyhow::Result<String> {
    Ok(client
        .query(&format!("EXPLAIN ANALYZE {sql}"), &[])
        .await?
        .iter()
        .map(|r| (0..r.len()).map(|c| r.try_get::<_, String>(c).unwrap_or_default()).collect::<Vec<_>>().join(" | "))
        .collect::<Vec<_>>()
        .join("\n"))
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread")]
async fn recent_window_prunes_within_compacted_file() -> anyhow::Result<()> {
    let bucket_secs = 60u64;
    // Small pages (50 rows) so ~600 rows → ~12 pages in one row group, mirroring
    // prod's single-row-group compacted file but with fine page granularity.
    // Deletion Vectors stay ON (harness default = prod config) — the bug.
    let env = E2eEnv::builder()
        .with_bucket_duration(Duration::from_secs(bucket_secs))
        .with_retention(Duration::from_secs(60 * 60))
        .with_optimize_sort_by()
        .with_page_row_count_limit(50)
        .start()
        .await?;
    let client = env.pg_client().await?;

    // 600 rows spanning ~10 minutes (1s apart), flushed in chunks so several
    // Delta files exist for the compaction to merge into one sorted file.
    let sec = 1_000_000i64;
    let total_rows = 600i64;
    for chunk in 0..6i64 {
        for i in 0..100i64 {
            let idx = chunk * 100 + i;
            insert_at(&client, &format!("r-{idx:04}"), FROZEN_START_MICROS + idx * sec).await?;
        }
        env.advance(Duration::from_secs(bucket_secs * 2));
        env.force_flush().await?;
    }
    // Drain MemBuffer so the query hits Delta only (no unordered mem branch).
    env.advance(Duration::from_secs(60 * 61));
    env.force_evict().await?;

    // Compact → one timestamp-DESC-sorted file, one row group, ~12 pages.
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(FROZEN_START_MICROS).unwrap().date_naive();
    let table_ref = env.db().resolve_table("e2e_project", "otel_logs_and_spans").await?;
    let (removed, added) = env.db().compact_date(&table_ref, "otel_logs_and_spans", date).await?;
    assert!(removed >= 2 && added >= 1, "compaction should merge files (removed={removed}, added={added})");

    // Narrow trailing window: newest ~50 rows (last ~50s of the 600s span).
    let cutoff = FROZEN_START_MICROS + (total_rows - 50) * sec;
    let cutoff_ts = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(cutoff).unwrap().format("%Y-%m-%d %H:%M:%S%.f");
    let sql = format!("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = 'e2e_project' AND timestamp > '{cutoff_ts}'");

    // Sanity: the window really is ~50 rows out of 600.
    let matched: i64 = client.query_one(&sql, &[]).await?.get(0);
    assert_eq!(matched, 49, "window should select 49 rows (> cutoff), got {matched}");

    let plan = explain_analyze(&client, &sql).await?;
    // The definitive signal is rows actually read from parquet: with the
    // timestamp predicate pushed into the scan, page-index + row pushdown skip
    // all but the newest rows. Without pushdown (the bug) the scan reads every
    // row and filters above it.
    let scanned = scan_metric(&plan, "output_rows=").unwrap_or(total_rows);
    let pushdown_pruned = scan_metric(&plan, "pushdown_rows_pruned=").unwrap_or(0);

    assert!(
        pushdown_pruned > 0,
        "predicate was not pushed into the parquet scan (pushdown_rows_pruned=0); \
         the Deletion-Vectors feature gate disabled parquet pushdown.\nplan:\n{plan}"
    );
    assert!(scanned < total_rows / 2, "scan read {scanned}/{total_rows} rows for a 49-row window — pruning not effective.\nplan:\n{plan}");

    Ok(())
}
