//! Regression: copy A of a row flushes to Delta, then a retry (copy B) lands
//! in a different Delta file in the same `(project_id, date)` partition.
//! Flush-time dedup runs per-bucket and cannot see across files, so without
//! a Delta-vs-Delta compaction pass the duplicate persists forever.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
use serial_test::serial;
use timefusion::{
    database::Database,
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, delta_physical_row_count, json_to_batch, test_span_ts, walrus_env_guard},
};

#[serial]
#[tokio::test]
async fn dedup_compaction_collapses_cross_flush_duplicates() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_compaction").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Pick a fixed timestamp so both inserts share (id, timestamp) and date.
    // 3h back: dedup only rewrites hour chunks sealed for 2h+ (late data may
    // still flush into newer hours). The partition date is derived from ts
    // below, so a midnight-UTC crossing stays consistent.
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };

    // Two skip_queue=true inserts → two independent Delta commits, two files
    // in the same (project_id, date) partition. This is the cross-flush
    // scenario in production: bucket A flushes, then a client retry arrives
    // in a fresh bucket B and flushes separately.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Sanity: the duplicate really did land physically in Delta. Measured via
    // the Delta log stats (`delta_physical_row_count`), NOT a routed query —
    // the read-side DedupExec would otherwise mask the on-disk duplicate.
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        2,
        "pre-dedup: cross-flush duplicate should exist as 2 physical rows in Delta"
    );

    // Verify there are at least two parquet files in the partition (proves the
    // two commits did not coalesce by accident).
    let date_str = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let part_marker = format!("project_id={}/date={}", project_id, date_str);
    let file_count_before = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&part_marker)).count();
    assert!(
        file_count_before >= 2,
        "expected >=2 files in partition before dedup, got {}",
        file_count_before
    );

    // Run the new dedup compaction on the partition.
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let dropped = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!(dropped, 1, "expected exactly one duplicate row dropped");

    // After the sweep, the duplicate is physically gone (1 row on disk).
    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        1,
        "post-dedup: duplicate should be physically collapsed to a single row"
    );

    Ok(())
}

/// Read-side dedup (parity plan Defect 2 #1): a cross-flush physical duplicate
/// that the background sweep has NOT yet collapsed must still read as a single
/// row through the normal routed scan path (`ProjectRoutingTable`, MemBuffer ∪
/// Delta), so `COUNT(*)` is correct regardless of physical dupes. Without the
/// in-scan `DedupExec` this query returns 2. Also covers the dedup-keys-projected-
/// away case (`SELECT name`, `COUNT(*)`), which exercises projection augmentation.
#[serial]
#[tokio::test]
async fn dup_across_flush_is_deduped_on_read() -> Result<()> {
    let cfg = TestConfigBuilder::new("read_side_dedup").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };
    // Two independent Delta commits → physical duplicate in one partition. No sweep run.
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Routed scan (NOT query_delta_only): read-side dedup must collapse to 1.
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;

    let count_sql = format!(
        "SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'",
        project_id
    );
    let res = ctx.sql(&count_sql).await?.collect().await?;
    assert_eq!(
        res[0].column(0).as_primitive::<Int64Type>().value(0),
        1,
        "read-side dedup must collapse the cross-flush duplicate to a single row (COUNT(*) projects keys away)"
    );

    // Non-empty projection that omits the dedup keys: augmentation must still dedup.
    let name_sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let rows: usize = ctx.sql(&name_sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "read-side dedup must still collapse when dedup keys are projected away (`SELECT name`)"
    );

    Ok(())
}

/// Regression for the LIMIT-pushdown-undercount bug found in code review: a pushed
/// `LIMIT N` must not be forwarded into the underlying Delta scan, because that
/// truncates to N rows *before* DedupExec drops duplicates — so the deduped union
/// can yield < N distinct rows even when more exist below the truncation point, and
/// the top-level limit can't recover them. With many physical copies of one id plus
/// one other id, `LIMIT 2` must still return 2 distinct rows.
#[serial]
#[tokio::test]
async fn limit_query_not_truncated_below_read_dedup() -> Result<()> {
    let cfg = TestConfigBuilder::new("read_dedup_limit").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    // 3 physical copies of "a" (one (id,timestamp) key) plus one "b": 4 physical
    // rows in Delta, 2 distinct. Pushing fetch=2 into the scan truncates to 2
    // physical rows that often are both "a", collapsing to a single deduped row;
    // the fix suppresses the scan limit so all 4 are read and dedup yields {a,b}.
    for _ in 0..3 {
        db.insert_records_batch(
            &project_id,
            "otel_logs_and_spans",
            vec![json_to_batch(vec![test_span_ts("a", "a", &project_id, ts)])?],
            true,
            None,
        )
        .await?;
    }
    db.insert_records_batch(
        &project_id,
        "otel_logs_and_spans",
        vec![json_to_batch(vec![test_span_ts("b", "b", &project_id, ts)])?],
        true,
        None,
    )
    .await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' LIMIT 2", project_id);
    let rows: usize = ctx.sql(&sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2, "LIMIT 2 must return both distinct ids, not a duplicate-truncated single row");

    Ok(())
}

/// Regression: the dedup *sweep* (`dedup_today_partitions`) was scoped to
/// `today` only, so a cross-flush duplicate that landed in a prior-day
/// partition — e.g. a late DLQ `WriteTfOnly` replay crossing midnight UTC —
/// was never collapsed (observed in prod: a 4-day-old window still 2×). The
/// sweep must cover a recent-day lookback window, not just today.
#[serial]
#[tokio::test]
async fn dedup_sweep_collapses_prior_day_partition() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_sweep_lookback").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Yesterday at noon UTC: always a prior-day `date=` partition and always
    // >2h sealed (≥12h ago regardless of wall-clock), so dedup will rewrite it.
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1))
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Physical row count (Delta log stats), so the read-side DedupExec doesn't
    // mask whether the *sweep* actually rewrote the on-disk duplicate.
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        2,
        "pre-sweep: prior-day cross-flush duplicate should exist as 2 physical rows"
    );

    // The production entry point the scheduler calls. With today-only scope this
    // is a no-op for a yesterday partition; with the lookback window it collapses it.
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    assert_eq!(
        delta_physical_row_count(&table_ref).await?,
        1,
        "post-sweep: prior-day duplicate must be physically collapsed to a single row"
    );
    Ok(())
}

/// Regression for the 2026-06-11 prod OOM/restart loop: dedup's replace_where
/// commit carries a bare-string timestamp predicate that delta-kernel's OCC
/// checker cannot evaluate ("arrow_cast should have been simplified"), so any
/// append landing between dedup's snapshot and commit aborted the sweep —
/// every attempt, every 5 minutes, materializing and abandoning chunk writes
/// (observed climbing to the 70GB memcg ceiling). The in-process
/// `delta_commit_lock` serializes commits so the rebase sees no newer
/// versions and the checker never runs: dedup must succeed under append fire.
#[serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dedup_commits_despite_concurrent_appends() -> Result<()> {
    use std::sync::atomic::Ordering::{Acquire, Release};
    let cfg = TestConfigBuilder::new("dedup_occ_race").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Duplicate pair in a sealed (3h-old) bin — the chunk dedup will rewrite.
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    for name in ["first", "second"] {
        let batch = json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    }

    // Append fire: fresh-timestamp rows (same partition date space, distinct
    // ids) committing continuously while dedup rewrites the sealed chunk.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let committed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let appender = {
        let (db, project_id, stop, committed) = (Arc::clone(&db), project_id.clone(), Arc::clone(&stop), Arc::clone(&committed));
        tokio::spawn(async move {
            let mut i = 0u64;
            while !stop.load(Acquire) {
                let now = chrono::Utc::now().timestamp_micros();
                let batch = json_to_batch(vec![test_span_ts(&format!("live_{i}"), "live", &project_id, now)]).unwrap();
                db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await.unwrap();
                i += 1;
                committed.store(i, Release);
            }
            i
        })
    };

    // Gate dedup on the appender's first committed row so the race is guaranteed,
    // not a scheduling artifact: on a loaded CI runner dedup could otherwise finish
    // before the spawned task is scheduled, failing the `appended > 0` assertion.
    while committed.load(Acquire) == 0 {
        tokio::task::yield_now().await;
    }

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let dropped = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    stop.store(true, Release);
    let appended = appender.await?;
    assert!(appended > 0, "appender must have raced at least one commit");
    assert_eq!(dropped, 1, "dedup must collapse the duplicate despite concurrent appends");

    let count_sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let post = db.query_delta_only(&count_sql).await?;
    assert_eq!(
        post[0].column(0).as_primitive::<Int64Type>().value(0),
        1,
        "post-dedup: dup_id row should be collapsed to 1"
    );
    Ok(())
}

/// Regression: light OPTIMIZE (bin-pack compact) must preserve ALL partition
/// values on rewritten files. The kernel narrows `partitionValues_parsed` to
/// the predicate-referenced subset (data skipping), and optimize used that
/// narrowed map for grouping/output — so a `date = today` filter rewrote
/// files as partitionValues={date}, silently NULLing project_id and hiding
/// every compacted row from project-scoped queries.
#[serial]
#[tokio::test]
async fn optimize_preserves_all_partition_values() -> Result<()> {
    let cfg = TestConfigBuilder::new("optimize_partition_preserve").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = chrono::Utc::now().timestamp_micros();
    // Distinct ids → no dedup interplay; 6 separate commits → 6 small files
    // (>= timefusion_compact_min_files=5 so the optimize commit isn't skipped).
    for i in 0..6 {
        let batch = json_to_batch(vec![test_span_ts(&format!("opt_id_{i}"), "row", &project_id, ts + i)])?;
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
    }

    let count_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
    let pre = db.query_delta_only(&count_sql).await?;
    assert_eq!(pre[0].column(0).as_primitive::<Int64Type>().value(0), 6, "pre-optimize row count");

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    db.optimize_table_light(&table_ref, "otel_logs_and_spans").await?;

    // Compacted files must keep the full (project_id, date) partition path…
    let date_str = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let bad: Vec<String> = table_ref
        .read()
        .await
        .get_file_uris()?
        .filter(|u| u.contains(&format!("/date={date_str}")) && !u.contains("project_id="))
        .collect();
    assert!(bad.is_empty(), "optimize dropped project_id partition from: {bad:?}");

    // …and project-scoped queries must still see every row.
    let post = db.query_delta_only(&count_sql).await?;
    assert_eq!(
        post[0].column(0).as_primitive::<Int64Type>().value(0),
        6,
        "post-optimize: project-scoped count must be unchanged"
    );
    Ok(())
}
