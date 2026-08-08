//! Regression: copy A of a row flushes to Delta, then a retry (copy B) lands
//! in a different Delta file in the same `(project_id, date)` partition.
//! Flush-time dedup runs per-bucket and cannot see across files, so without
//! a Delta-vs-Delta compaction pass the duplicate persists forever.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::{
    array::{Array, AsArray},
    datatypes::Int64Type,
};
use serial_test::serial;
use timefusion::{
    database::Database,
    test_utils::test_helpers::{BufferMode, TestConfigBuilder, delta_physical_row_count, json_to_batch, test_span_ts},
};

#[serial]
#[tokio::test]
async fn dedup_compaction_collapses_cross_flush_duplicates() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_compaction").with_buffer_mode(BufferMode::Enabled).build();
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
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-dedup: cross-flush duplicate should exist as 2 physical rows in Delta");

    // Verify there are at least two parquet files in the partition (proves the
    // two commits did not coalesce by accident).
    let date_str = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let part_marker = format!("project_id={}/date={}", project_id, date_str);
    let file_count_before = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&part_marker)).count();
    assert!(file_count_before >= 2, "expected >=2 files in partition before dedup, got {}", file_count_before);

    // Run the new dedup compaction on the partition.
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let (dropped, complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!((dropped, complete), (1, true), "expected exactly one duplicate row dropped in a complete pass");

    // After the sweep, the duplicate is physically gone (1 row on disk).
    assert_eq!(delta_physical_row_count(&table_ref).await?, 1, "post-dedup: duplicate should be physically collapsed to a single row");

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

    let count_sql = format!("SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let res = ctx.sql(&count_sql).await?.collect().await?;
    assert_eq!(
        res[0].column(0).as_primitive::<Int64Type>().value(0),
        1,
        "read-side dedup must collapse the cross-flush duplicate to a single row (COUNT(*) projects keys away)"
    );

    // Non-empty projection that omits the dedup keys: augmentation must still dedup.
    let name_sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let rows: usize = ctx.sql(&name_sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "read-side dedup must still collapse when dedup keys are projected away (`SELECT name`)");

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
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    // 3 physical copies of "a" (one (id,timestamp) key) plus one "b": 4 physical
    // rows in Delta, 2 distinct. Pushing fetch=2 into the scan truncates to 2
    // physical rows that often are both "a", collapsing to a single deduped row;
    // the fix suppresses the scan limit so all 4 are read and dedup yields {a,b}.
    for _ in 0..3 {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("a", "a", &project_id, ts)])?], true, None).await?;
    }
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("b", "b", &project_id, ts)])?], true, None).await?;

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
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Yesterday at noon UTC: always a prior-day `date=` partition and always
    // >2h sealed (≥12h ago regardless of wall-clock), so dedup will rewrite it.
    let ts = (chrono::Utc::now().date_naive() - chrono::Duration::days(1)).and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    // Physical row count (Delta log stats), so the read-side DedupExec doesn't
    // mask whether the *sweep* actually rewrote the on-disk duplicate.
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "pre-sweep: prior-day cross-flush duplicate should exist as 2 physical rows");

    // The production entry point the scheduler calls. With today-only scope this
    // is a no-op for a yesterday partition; with the lookback window it collapses it.
    db.dedup_today_partitions(&table_ref, "otel_logs_and_spans", "otel_logs_and_spans").await?;

    assert_eq!(delta_physical_row_count(&table_ref).await?, 1, "post-sweep: prior-day duplicate must be physically collapsed to a single row");
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
    let (dropped, _complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    stop.store(true, Release);
    let appended = appender.await?;
    assert!(appended > 0, "appender must have raced at least one commit");
    assert_eq!(dropped, 1, "dedup must collapse the duplicate despite concurrent appends");

    let count_sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'dup_id'", project_id);
    let post = db.query_delta_only(&count_sql).await?;
    assert_eq!(post[0].column(0).as_primitive::<Int64Type>().value(0), 1, "post-dedup: dup_id row should be collapsed to 1");
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
    db.optimize_table_light(&table_ref, "otel_logs_and_spans", timefusion::database::TailPass::Pack).await?;

    // Compacted files must keep the full (project_id, date) partition path…
    let date_str = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let bad: Vec<String> = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&format!("/date={date_str}")) && !u.contains("project_id=")).collect();
    assert!(bad.is_empty(), "optimize dropped project_id partition from: {bad:?}");

    // …and project-scoped queries must still see every row.
    let post = db.query_delta_only(&count_sql).await?;
    assert_eq!(post[0].column(0).as_primitive::<Int64Type>().value(0), 6, "post-optimize: project-scoped count must be unchanged");
    Ok(())
}

/// The dedup rewrite is a TARGETED file transaction (remove+add of exactly
/// the files holding the duplicate chunk's rows) — a bystander file in the
/// same partition but outside the duplicate's 10-minute window must survive
/// byte-identical (same path, never rewritten), while the duplicate-bearing
/// files are replaced. Pins the 2026-07-04 fix: the old replace_where's
/// bare-string predicate planned against the whole table.
#[serial]
#[tokio::test]
async fn dedup_rewrite_targets_only_duplicate_files() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_targeted").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Duplicate pair 3h back (sealed); bystander 20 minutes earlier — a
    // different 10-minute chunk, usually the same date partition (if the test
    // straddles midnight UTC the bystander lands in a different partition,
    // which only makes the untouched assertion trivially true).
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let ts_bystander = ts - chrono::Duration::minutes(20).num_microseconds().unwrap();
    let dup = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };

    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![dup("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![dup("second")?], true, None).await?;
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    let files_before_bystander: std::collections::HashSet<String> = table_ref.read().await.get_file_uris()?.collect();

    db.insert_records_batch(
        &project_id,
        "otel_logs_and_spans",
        vec![json_to_batch(vec![test_span_ts("bystander", "witness", &project_id, ts_bystander)])?],
        true,
        None,
    )
    .await?;
    let bystander_files: Vec<String> = {
        let now: std::collections::HashSet<String> = table_ref.read().await.get_file_uris()?.collect();
        now.difference(&files_before_bystander).cloned().collect()
    };
    assert!(!bystander_files.is_empty(), "bystander insert must add a file");

    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let (dropped, _complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!(dropped, 1, "expected exactly the duplicate row dropped");

    let files_after: std::collections::HashSet<String> = table_ref.read().await.get_file_uris()?.collect();
    for f in &bystander_files {
        assert!(files_after.contains(f), "bystander file must be untouched by the targeted rewrite: {f}");
    }
    for f in files_before_bystander {
        assert!(!files_after.contains(&f), "duplicate-bearing file must have been replaced: {f}");
    }
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "post-dedup: 1 deduped row + 1 bystander");
    Ok(())
}

/// Fix 3 (2026-07-09): a chunk whose estimated decoded footprint exceeds the
/// budget must NOT be skipped (leaving the dupe forever — prod project dcad860a's
/// 743 MB single-file chunks skipped every 5-min sweep). Instead the rewrite
/// SHARDS by an md5 hash of the dedup keys so each pass stays under the budget,
/// the duplicate is collapsed, AND every distinct row survives — no row lost to a
/// shard, none duplicated across shards. Pre-fix this SKIPPED (dropped=0, all rows
/// persist); the fix collapses exactly the duplicate.
///
/// `bytes_per_row`/`inflation` are pinned so the estimate is deterministic: 5 rows
/// × 1 MB × 2 = 10 MB est over a 5 MB budget ⇒ 2 shards; the largest key group
/// (a×2 = 4 MB) stays under budget so it is shardable, not skipped.
#[serial]
#[tokio::test]
async fn dedup_shards_over_budget_and_preserves_rows() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_shard_preserve").with_buffer_mode(BufferMode::Enabled).build();
    let mut cfg = Arc::try_unwrap(cfg).expect("fresh config Arc");
    cfg.maintenance.timefusion_dedup_bytes_per_row = 1_000_000;
    cfg.maintenance.timefusion_dedup_decode_inflation = 1;
    cfg.maintenance.timefusion_dedup_max_decoded_bytes = 5_000_000;
    let cfg = Arc::new(cfg);

    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Base timestamp 3h back (sealed). Distinct (id, ts) tuples hash to spread
    // buckets; "a" is inserted twice at the SAME (id, ts) → same bucket → same shard.
    let base = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let ins = |id: &str, ts: i64| -> Result<_> { json_to_batch(vec![test_span_ts(id, id, &project_id, ts)]) };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![ins("a", base)?], true, None).await?; // dup 1
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![ins("a", base)?], true, None).await?; // dup 2
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![ins("b", base + 1)?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![ins("c", base + 2)?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![ins("d", base + 3)?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 5, "pre-dedup: 5 physical rows (a×2, b, c, d)");

    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(base).unwrap().date_naive();
    let (dropped, complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!((dropped, complete), (1, true), "over budget, the sharded rewrite collapses exactly the one 'a' duplicate (not skip)");
    assert_eq!(delta_physical_row_count(&table_ref).await?, 4, "4 distinct rows survive across shards");

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let ids = ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id")).await?.collect().await?;
    let got: Vec<String> = ids
        .iter()
        .flat_map(|b| {
            let col = b.column(0);
            (0..b.num_rows()).map(|i| timefusion::test_utils::test_helpers::array_get_str(col.as_ref(), i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(got, vec!["a", "b", "c", "d"], "every distinct id preserved exactly once");
    Ok(())
}

/// Skew safety valve: sharding can't split a single key group (all copies hash to
/// one bucket). If that one group alone exceeds the decoded budget, no shard count
/// helps, so the chunk is SKIPPED (0, false) rather than materialized into an OOM —
/// preserving the pre-fix safety. Here one key has 3 copies (3 × 1 MB × 2 = 6 MB)
/// over a 4 MB budget, so it must skip and leave the rows physically intact.
#[serial]
#[tokio::test]
async fn dedup_skips_single_hot_key_over_budget() -> Result<()> {
    let cfg = TestConfigBuilder::new("dedup_hot_key").with_buffer_mode(BufferMode::Enabled).build();
    let mut cfg = Arc::try_unwrap(cfg).expect("fresh config Arc");
    cfg.maintenance.timefusion_dedup_bytes_per_row = 1_000_000;
    cfg.maintenance.timefusion_dedup_decode_inflation = 1;
    cfg.maintenance.timefusion_dedup_max_decoded_bytes = 4_000_000;
    let cfg = Arc::new(cfg);

    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let row = || -> Result<_> { json_to_batch(vec![test_span_ts("hot", "hot", &project_id, ts)]) };
    for _ in 0..3 {
        db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row()?], true, None).await?;
    }
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 3, "pre-dedup: 3 copies of one key");

    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    let (dropped, complete) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project_id, date).await?;
    assert_eq!((dropped, complete), (0, false), "an unshardable single key group over budget must skip, not certify clean");
    assert_eq!(delta_physical_row_count(&table_ref).await?, 3, "skipped chunk left physically intact (no rewrite, no OOM)");
    Ok(())
}

/// Characterization (refutes the target-dup hypothesis): a duplicated *target*
/// row does NOT break `UPDATE ... FROM`. The cardinality violation that aborted
/// prod's MERGE is source-side (see the next test), not a consequence of the
/// over-budget dedup skip leaving target duplicates.
///
/// The reported count differs by table kind, and BOTH are correct:
/// - in-place (delta-rs MERGE): two physical rows are two rows to rewrite ⇒ 2.
/// - merge-on-read: the two physical rows share `(id, timestamp)`, so they are
///   ONE logical row. The scan feeding the append is post-`DedupExec`, and the
///   single appended version supersedes both copies ⇒ 1.
/// `otel_logs_and_spans` is merge-on-read, so 1 is the answer here; asserting 2
/// would be asserting that the append duplicated the row.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_from_on_duplicated_target_updates_all_copies() -> Result<()> {
    let cfg = TestConfigBuilder::new("update_dup_target").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Two skip_queue inserts of the SAME (id, timestamp) → 2 physical target rows.
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let row = |name: &str| -> Result<_> { json_to_batch(vec![test_span_ts("dup_id", name, &project_id, ts)]) };
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![row("second")?], true, None).await?;

    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 2, "precondition: duplicate exists physically in Delta");

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let sql = format!(
        "UPDATE otel_logs_and_spans SET name = u.name \
         FROM (VALUES ('dup_id', 'enriched')) AS u(id, name) \
         WHERE project_id = '{project_id}' AND otel_logs_and_spans.id = u.id"
    );
    let updated = ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().value(0);
    assert_eq!(updated, 1, "the duplicate copies are ONE logical row to merge-on-read: one version appended, superseding both");
    // The point of the test: the MERGE did not abort, and the duplicate is gone
    // from the logical table rather than half-updated.
    let mut ctx2 = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx2)?;
    let rows = ctx2.sql(&format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'")).await?.collect().await?;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "both copies resolve to one current version");
    assert_eq!(rows[0].column(0).as_string_view().value(0), "enriched", "and it is the updated one");
    Ok(())
}

/// Regression for the prod 2026-07-09 `MERGE matched a target row with multiple
/// source rows` cardinality abort, and its 2026-07-19 fix (`split_source_rounds`,
/// commit 9314499). The violation is SOURCE-side: two source rows sharing a join
/// key both match one target row, which delta-rs would abort. TF now splits such
/// a source into successive single-key rounds and applies them last-write-wins
/// instead of failing — so same-key multi-tag hash enrichment neither errors nor
/// silently drops tags. The UPDATE must therefore SUCCEED, apply every round
/// (count = rounds), and leave the target holding the last source row's value.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_from_duplicate_source_keys_applies_last_write_wins() -> Result<()> {
    let cfg = TestConfigBuilder::new("update_dup_source").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // Single, clean target row for 'dup_id'.
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("dup_id", "orig", &project_id, ts)])?], true, None)
        .await?;
    let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
    assert_eq!(delta_physical_row_count(&table_ref).await?, 1, "precondition: exactly one target row");

    // Source with TWO rows for the same id → split into two rounds ('a' then 'b').
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let sql = format!(
        "UPDATE otel_logs_and_spans SET name = u.name \
         FROM (VALUES ('dup_id', 'a'), ('dup_id', 'b')) AS u(id, name) \
         WHERE project_id = '{project_id}' AND otel_logs_and_spans.id = u.id"
    );
    let updated = ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<datafusion::arrow::datatypes::UInt64Type>().value(0);
    assert_eq!(updated, 2, "both rounds applied to the single target row (last-write-wins), not aborted");

    // Last source row wins; the target stays a single logical row.
    let rows = ctx.sql(&format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND id = 'dup_id'")).await?.collect().await?;
    let total: usize = rows.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "target remains a single logical row");
    assert_eq!(rows[0].column(0).as_string_view().value(0), "b", "last source row wins");
    Ok(())
}

/// Regression for the 2026-07-21 wide-dashboard latency incident: the cold
/// consolidate sweep must produce event-time DISJOINT sorted runs. The old
/// whole-partition optimize binned files in snapshot (arrival) order, so a day
/// whose files interleave event times (dedup rewrites, DV merges, backfill)
/// merged into runs that ALL overlapped the full day — a recent-window or
/// ORDER-BY-timestamp-DESC-LIMIT query had to open every file. Event-time
/// binned selection makes successive runs cover strictly later slices.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn cold_consolidate_produces_event_time_disjoint_runs() -> Result<()> {
    use timefusion::test_utils::test_helpers::minio_test_config;
    let id = format!("cold-consol-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let dir = format!("/tmp/timefusion-{id}");
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);

    // A sealed, cold date (3 days back). Arrival order interleaves event
    // times: hours [0, 6, 1, 7, 2, 8] — snapshot-order binning mixes early and
    // late hours in every bin; event-time binning separates them.
    let base = (chrono::Utc::now() - chrono::Duration::days(3)).date_naive().and_hms_opt(0, 0, 0).unwrap().and_utc();
    let date = base.date_naive();
    let hours = [0i64, 6, 1, 7, 2, 8];

    // Event-time range from the raw Add stats JSON (timestamps are RFC3339).
    fn ts_range(stats: &str) -> Option<(i64, i64)> {
        let v: serde_json::Value = serde_json::from_str(stats).ok()?;
        let get = |key: &str| v[key]["timestamp"].as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_micros());
        Some((get("minValues")?, get("maxValues")?))
    }

    // The flush path (not the skip_queue direct commit) is what writes
    // min/max timestamp stats into the Add actions — the same files the prod
    // selection sees; flush_immediately lands each insert as its own commit.
    let cfg = {
        let mut c = (*minio_test_config(&id, &dir)).clone();
        c.buffer.timefusion_flush_immediately = true;
        Arc::new(c)
    };
    let sizes: Vec<i64> = {
        let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
        for (i, h) in hours.iter().enumerate() {
            let ts = (base + chrono::Duration::hours(*h)).timestamp_micros();
            let mut row = test_span_ts(&format!("id-{i}"), &format!("span-{i}"), &project_id, ts);
            // Data-dominated file sizes (incompressible ~140KB payload): run
            // sizes must scale with their member rows for the size-based
            // convergence assertions below; 1-row files are otherwise pure
            // footer overhead and merging wouldn't grow them.
            let blob: String = (0..4000).map(|_| uuid::Uuid::new_v4().to_string()).collect();
            row["summary"] = serde_json::json!([blob]);
            let batch = json_to_batch(vec![row])?;
            // flush_immediately → each insert flushes as its own Delta commit.
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
        }
        let table_ref = db.unified_tables().read().await.get("otel_logs_and_spans").expect("table created").clone();
        let guard = table_ref.read().await;
        let marker = format!("project_id={project_id}/date={date}");
        let batch = guard.snapshot()?.add_actions_table(true)?;
        let paths = batch.column_by_name("path").unwrap();
        let file_sizes = batch.column_by_name("size_bytes").unwrap().as_primitive::<Int64Type>();
        (0..file_sizes.len())
            .filter(|&i| timefusion::test_utils::test_helpers::array_get_str(paths.as_ref(), i).contains(&marker))
            .map(|i| file_sizes.value(i))
            .collect()
    };
    assert_eq!(sizes.len(), 6, "each skip_queue insert must land as its own file");

    // Cold target sized so ~2 files fit per run but 3 don't → forces >1 run.
    let median = {
        let mut s = sizes.clone();
        s.sort();
        s[s.len() / 2]
    };
    let mut cfg2 = (*cfg).clone();
    cfg2.parquet.timefusion_cold_optimize_target_size = median * 5 / 2;
    let db = Arc::new(Database::with_config(Arc::new(cfg2)).await?);
    let table_ref = db.get_or_create_unified_table("otel_logs_and_spans").await?;
    db.consolidate_sealed_partitions(&table_ref, "otel_logs_and_spans").await?;

    // Collect (min,max) event-time ranges of the partition's live files.
    let filters = vec![
        deltalake::PartitionFilter::try_from(("project_id", "=", project_id.as_str()))?,
        deltalake::PartitionFilter::try_from(("date", "=", date.to_string().as_str()))?,
    ];
    let ranges: Vec<(i64, i64)> = {
        use futures::TryStreamExt;
        let guard = table_ref.read().await;
        let adds: Vec<_> = guard.get_active_add_actions_by_partitions(&filters).try_collect().await?;
        // A run without readable timestamp stats overlaps everything —
        // equally fatal for pruning, so it must fail the disjointness assert.
        adds.iter().map(|a| a.stats().and_then(|s| ts_range(&s)).unwrap_or((i64::MIN, i64::MAX))).collect()
    };
    assert!(ranges.len() < 6, "consolidation must merge files (got {} of 6)", ranges.len());
    assert!(ranges.len() >= 2, "target must split the day into multiple runs (got {})", ranges.len());
    let mut sorted = ranges.clone();
    sorted.sort();
    for w in sorted.windows(2) {
        assert!(w[0].1 < w[1].0, "consolidated runs must be event-time disjoint, got overlapping ranges {:?} and {:?} (all: {:?})", w[0], w[1], sorted);
    }
    assert_eq!(delta_physical_row_count(&table_ref).await?, 6, "consolidation must not lose rows");

    // Idempotence: a second sweep must not rewrite converged runs.
    let before: Vec<(i64, i64)> = sorted.clone();
    db.consolidate_sealed_partitions(&table_ref, "otel_logs_and_spans").await?;
    let after: Vec<(i64, i64)> = {
        use futures::TryStreamExt;
        let guard = table_ref.read().await;
        let adds: Vec<_> = guard.get_active_add_actions_by_partitions(&filters).try_collect().await?;
        let mut r: Vec<_> = adds.iter().map(|a| a.stats().and_then(|s| ts_range(&s)).unwrap_or((i64::MIN, i64::MAX))).collect();
        r.sort();
        r
    };
    assert_eq!(after, before, "second sweep must be a no-op on converged runs");
    Ok(())
}

// ---------------------------------------------------------------------------
// Merge-on-read read path (docs/plans/2026-08-01-merge-on-read-dml.md §3).
//
// Keep-greatest only engages while `DedupExec`'s input still DECLARES an
// ordering on the leading dedup key — that is what makes every version of a key
// arrive in one contiguous run. `DedupExec` requires `SinglePartition`, which
// otherwise gets an ordering-erasing `CoalescePartitionsExec`, and the
// mem ∪ hot ∪ delta union is unordered while the MemBuffer branch declares
// nothing. `scan` therefore sorts the (in-memory, cheap) mem/hot legs up to the
// Delta leg's declared footer ordering; the union then advertises it and
// `EnforceDistribution` picks a `SortPreservingMergeExec` instead.
//
// The shape under test is the production one: the base row is already flushed to
// Delta (one file → the fork's footer pushdown declares `timestamp DESC`) and
// the new version is still in MemBuffer.
// ---------------------------------------------------------------------------

/// A `Database` with a real buffered layer, so writes can land in MemBuffer.
async fn buffered_db(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let layer = Arc::new(timefusion::test_utils::test_helpers::test_layer(Arc::clone(&cfg))?);
    let db = Arc::new(Database::with_config(cfg).await?.with_buffered_layer(layer));
    Ok((db, format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8])))
}

/// `to_delta = true` commits straight to Delta (the already-flushed base row);
/// `false` goes through the buffered layer into MemBuffer (the new version).
async fn write(db: &Arc<Database>, project_id: &str, rows: Vec<serde_json::Value>, to_delta: bool) -> Result<()> {
    write_to(db, "otel_logs_and_spans", project_id, rows, to_delta).await
}

async fn write_to(db: &Arc<Database>, table: &str, project_id: &str, rows: Vec<serde_json::Value>, to_delta: bool) -> Result<()> {
    let batch = timefusion::test_utils::test_helpers::json_to_batch_for(table, rows)?;
    db.insert_records_batch(project_id, table, vec![batch], to_delta, None).await?;
    Ok(())
}

/// A `mor_versioned` row — the fixture table that ships `version_append: true`,
/// so the merge-on-read read path is actually live on it.
fn mor_row(id: &str, name: &str, project_id: &str, ts: i64, deleted: Option<bool>) -> serde_json::Value {
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    serde_json::json!({ "timestamp": ts, "id": id, "name": name, "project_id": project_id, "date": date, "deleted": deleted })
}

/// Run the dedup sweep until the window's partitions carry a clean fingerprint —
/// the precondition both the read-side dedup skip and `count_pushdown` gate on.
/// Only a 0-drop pass over an UNCHANGED live file set certifies a partition, so
/// a first pass that rewrote anything marks nothing — the second pass over the
/// settled set is what certifies. Two passes; the second is an early-return
/// no-op when the first already left the table's version untouched.
async fn sweep_clean(db: &Arc<Database>, table: &str) -> Result<()> {
    let table_ref = db.unified_tables().read().await.get(table).expect("table created").clone();
    for _ in 0..2 {
        db.dedup_today_partitions(&table_ref, table, table).await?;
    }
    Ok(())
}

async fn physical_plan(db: &Arc<Database>, sql: &str) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx.sql(sql).await?.create_physical_plan().await?)
}

fn rendered(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> String {
    datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string()
}

/// Depth-first search for the first node named `name`.
fn find_node(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if plan.name() == name {
        return Some(plan.clone());
    }
    plan.children().into_iter().find_map(|c| find_node(c, name))
}

async fn column_strings(db: &Arc<Database>, sql: &str) -> Result<Vec<String>> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    Ok(ctx
        .sql(sql)
        .await?
        .collect()
        .await?
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(|i| timefusion::test_utils::test_helpers::array_get_str(b.column(0).as_ref(), i)).collect::<Vec<_>>())
        .collect())
}

/// A plain merge-on-read `SELECT` must never sort the DELTA leg — that
/// blocking whole-window SortExec exhausted the query pool on prod
/// 2026-08-02. In-memory legs MAY be sorted (bounded, already materialized):
/// when the Delta leg declares its footer ordering, the cheap mem-leg sort
/// plus a `SortPreservingMergeExec` is the intended shape, because it gives
/// `DedupExec` a bounded keep-greatest instead of a full-set buffer.
#[serial]
#[tokio::test]
async fn plain_select_dedups_without_sorting_under_mor() -> Result<()> {
    let (db, project_id) = buffered_db("mor_plan_shape").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    let rows = (0..8).map(|i| mor_row(&format!("k{i}"), "v", &project_id, ts - i * 1000, None)).collect();
    write_to(&db, "mor_versioned", &project_id, rows, true).await?;
    write_to(&db, "mor_versioned", &project_id, vec![mor_row("k0", "v2", &project_id, ts, None)], false).await?;

    let plan = physical_plan(&db, &format!("SELECT name FROM mor_versioned WHERE project_id = '{project_id}'")).await?;
    let text = rendered(&plan);
    find_node(&plan, "DedupExec").unwrap_or_else(|| panic!("no DedupExec in plan:\n{text}"));
    // The one invariant: no SortExec whose child is the Delta scan.
    fn delta_leg_sorted(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> bool {
        (plan.name() == "SortExec" && find_node(&plan.children()[0].clone(), "DeltaScanExec").is_some())
            || plan.children().iter().any(|c| delta_leg_sorted(&(*c).clone()))
    }
    assert!(!delta_leg_sorted(&plan), "the Delta leg must never be sorted under MOR:\n{text}");
    Ok(())
}

/// The other half of the gate, and the one that ships: while `version_append` is
/// OFF, the ordering machinery must be completely absent from the scan. No
/// version can exist, so the blocking `SortExec` over the mem/hot legs and the
/// k-way `SortPreservingMergeExec` (one in-flight batch per Delta partition —
/// the 2026-07-20 OOM shape) would be pure cost. `DedupExec` keeps its
/// keep-first behaviour behind the pre-existing `CoalescePartitionsExec`.
#[serial]
#[tokio::test]
async fn dormant_version_append_table_keeps_coalesce_and_no_injected_sort() -> Result<()> {
    // `mor_dormant`, not otel: otel itself played this role until it flipped
    // `version_append: true`, at which point asserting the dormant shape on it
    // asserted the opposite of what ships.
    let (db, project_id) = buffered_db("mor_plan_shape_dormant").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "mor_dormant", &project_id, (0..8).map(|i| mor_row(&format!("k{i}"), "v", &project_id, ts - i * 1000, None)).collect(), true).await?;
    write_to(&db, "mor_dormant", &project_id, vec![mor_row("k0", "v2", &project_id, ts, None)], false).await?;

    let plan = physical_plan(&db, &format!("SELECT name FROM mor_dormant WHERE project_id = '{project_id}'")).await?;
    let text = rendered(&plan);
    let dedup = find_node(&plan, "DedupExec").unwrap_or_else(|| panic!("no DedupExec in plan:\n{text}"));
    assert_eq!(dedup.children()[0].name(), "CoalescePartitionsExec", "a dormant version_append table must keep the pre-merge-on-read plan shape:\n{text}");
    assert!(!text.contains("SortExec"), "no sort may be injected over the mem/hot legs while version_append is off:\n{text}");
    assert!(!text.contains("SortPreservingMergeExec"), "no k-way merge may be injected while version_append is off:\n{text}");
    Ok(())
}

/// A table declaring no `dedup_tiebreak` (and no dedup keys / tombstone) must
/// plan exactly as it did before merge-on-read: no injected sort, no merge, no
/// dedup, no tombstone filter.
#[serial]
#[tokio::test]
async fn no_tiebreak_table_plan_is_unchanged() -> Result<()> {
    let (db, _project_id) = buffered_db("mor_no_tiebreak").await?;
    let text = rendered(&physical_plan(&db, "SELECT id FROM variant_bench WHERE project_id = 'p'").await?);
    for op in ["DedupExec", "SortPreservingMergeExec", "SortExec", "IS DISTINCT FROM true"] {
        assert!(!text.contains(op), "variant_bench declares no dedup_tiebreak/tombstone — `{op}` must not appear:\n{text}");
    }
    Ok(())
}

/// The behavioural contract of merge-on-read: two versions of one
/// `(timestamp, id)` that differ only in their TF-stamped `updated_at` must both
/// read back as the NEWER version — through a plain `SELECT` and through an
/// aggregation. Under keep-first (arrival order) this returns "v1".
#[serial]
#[tokio::test]
async fn keep_greatest_returns_newest_version() -> Result<()> {
    let (db, project_id) = buffered_db("mor_keep_greatest").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "mor_versioned", &project_id, vec![mor_row("k", "v1", &project_id, ts, None)], true).await?;
    write_to(&db, "mor_versioned", &project_id, vec![mor_row("k", "v2", &project_id, ts, None)], false).await?;

    let where_ = format!("WHERE project_id = '{project_id}' AND id = 'k'");
    assert_eq!(column_strings(&db, &format!("SELECT name FROM mor_versioned {where_}")).await?, vec!["v2"], "plain SELECT must resolve to the newest version");
    assert_eq!(
        column_strings(&db, &format!("SELECT max(name) FROM mor_versioned {where_}")).await?,
        vec!["v2"],
        "an aggregation must see the newest version too — one surviving row"
    );
    Ok(())
}

/// Merge-on-read `DELETE`: the tombstone version must first BEAT the older live
/// version on `updated_at` (so keep-greatest picks it), and only then remove the
/// row. Filtering below the dedup would drop the tombstone and resurrect the
/// stale live row. It must vanish from `SELECT` and from `COUNT(*)` alike — the
/// latter also pins that the stats-based `count_pushdown` declines on a
/// tombstone table, where `numRecords` counts versions it never materializes.
#[serial]
#[tokio::test]
async fn tombstoned_row_hidden_from_select_and_count() -> Result<()> {
    let (db, project_id) = buffered_db("mor_tombstone").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    let row = |id: &str, deleted: Option<bool>| mor_row(id, id, &project_id, ts, deleted);
    write_to(&db, "mor_versioned", &project_id, vec![row("gone", None), row("live", None)], true).await?;
    write_to(&db, "mor_versioned", &project_id, vec![row("gone", Some(true))], false).await?;

    let where_ = format!("WHERE project_id = '{project_id}'");
    assert_eq!(
        column_strings(&db, &format!("SELECT id FROM mor_versioned {where_}")).await?,
        vec!["live"],
        "a key whose winning version is a tombstone must not appear in SELECT"
    );

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let cnt = ctx.sql(&format!("SELECT COUNT(*) FROM mor_versioned {where_}")).await?.collect().await?;
    assert_eq!(cnt[0].column(0).as_primitive::<Int64Type>().value(0), 1, "COUNT(*) must not count the tombstoned row");
    Ok(())
}

/// `COUNT(*)` over a Delta-only, fully-flushed, timestamp-bounded window — the
/// exact shape `count_pushdown` answers from add-action `numRecords`, and one
/// `dedup_window_clean` will happily certify. Those stats count a tombstone as a
/// row and the live version it retires as another, so the pushdown must decline
/// wherever tombstones can exist; the answer here is 0, not 2.
#[serial]
#[tokio::test]
async fn count_pushdown_declines_where_tombstones_are_possible() -> Result<()> {
    let (db, project_id) = buffered_db("mor_count_pushdown").await?;
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let iso = |t: i64| chrono::DateTime::<chrono::Utc>::from_timestamp_micros(t).unwrap().to_rfc3339();
    // Both versions in ONE Delta file (so the footer ordering is declared and
    // keep-greatest engages), nothing in MemBuffer. `numRecords` says 2.
    let rows = vec![mor_row("k", "v", &project_id, ts, None), mor_row("k", "v", &project_id, ts, Some(true))];
    write_to(&db, "mor_versioned", &project_id, rows, true).await?;
    sweep_clean(&db, "mor_versioned").await?;

    let sql = format!(
        "SELECT COUNT(*) FROM mor_versioned WHERE project_id = '{project_id}' AND timestamp >= '{}'::timestamptz AND timestamp < '{}'::timestamptz",
        iso(ts - 60_000_000),
        iso(ts + 60_000_000)
    );
    // A successful pushdown replaces the whole plan with a one-row in-memory
    // exec; declining leaves the real scan (dedup + tombstone filter) standing.
    let text = rendered(&physical_plan(&db, &sql).await?);
    assert!(text.contains("DedupExec"), "count_pushdown must decline where tombstones can exist — it answered from add-action stats:\n{text}");
    assert!(text.contains("IS DISTINCT FROM true"), "the tombstone filter must be part of the counted plan:\n{text}");

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let cnt = ctx.sql(&sql).await?.collect().await?;
    assert_eq!(cnt[0].column(0).as_primitive::<Int64Type>().value(0), 0, "the tombstone wins its key and removes the row — stats would have said 2");
    Ok(())
}

/// The tombstone column is live (all NULL) before any tombstone is ever
/// written: NULL must read as LIVE, so the filter is a provable no-op on
/// pre-existing data — which is what lets a table declare the column at birth.
#[serial]
#[tokio::test]
async fn null_tombstone_is_live() -> Result<()> {
    let (db, project_id) = buffered_db("mor_tombstone_null").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    let rows = (0..5).map(|i| mor_row(&format!("k{i}"), "v", &project_id, ts - i * 1000, None)).collect();
    write_to(&db, "mor_versioned", &project_id, rows, true).await?;
    assert_eq!(column_strings(&db, &format!("SELECT name FROM mor_versioned WHERE project_id = '{project_id}'")).await?.len(), 5);
    Ok(())
}

/// A predicate on the tombstone marker must NEVER be pushed into a scan leg:
/// applied at the source it drops the tombstone row before the dedup, so the
/// older live version wins and a deleted row silently resurrects. It is reported
/// `Unsupported` (DataFusion keeps its own `FilterExec` above the whole scan)
/// and stripped again inside `scan`.
#[serial]
#[tokio::test]
async fn tombstone_predicate_is_not_pushed_into_the_scan() -> Result<()> {
    use datafusion::logical_expr::{TableProviderFilterPushDown, col, lit};
    let (db, project_id) = buffered_db("mor_tombstone_pushdown").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write_to(&db, "mor_versioned", &project_id, vec![mor_row("k", "v1", &project_id, ts, None)], true).await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let provider = ctx.table_provider("mor_versioned").await?;
    let pred = col("deleted").eq(lit(true));
    assert!(
        matches!(provider.supports_filters_pushdown(&[&pred])?[0], TableProviderFilterPushDown::Unsupported),
        "a tombstone-column predicate must never be pushed to the scan legs"
    );

    // End to end: the dedup still runs under a user predicate on the marker —
    // the predicate is applied above the whole scan, not inside a leg.
    let text = rendered(&physical_plan(&db, &format!("SELECT name FROM mor_versioned WHERE project_id = '{project_id}' AND deleted")).await?);
    assert!(text.contains("DedupExec"), "the dedup must still run under a tombstone predicate:\n{text}");
    Ok(())
}

/// The existing top-K path must still early-terminate: `ORDER BY timestamp DESC
/// LIMIT n` must not regrow the blocking whole-window `SortExec` that
/// `ordered_union_for_topk` exists to remove — every surviving sort carries a
/// fetch (a TopK).
#[serial]
#[tokio::test]
async fn topk_path_still_streams() -> Result<()> {
    let (db, project_id) = buffered_db("mor_topk").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write(&db, &project_id, (0..8).map(|i| test_span_ts(&format!("t{i}"), "n", &project_id, ts - i * 1000)).collect(), true).await?;
    write(&db, &project_id, vec![test_span_ts("t9", "n", &project_id, ts + 1000)], false).await?;

    let sql = format!("SELECT name FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY timestamp DESC LIMIT 2");
    let text = rendered(&physical_plan(&db, &sql).await?);
    for line in text.lines().filter(|l| l.contains("SortExec")) {
        assert!(line.contains("fetch="), "a blocking whole-window SortExec regrew in the top-K plan: {line}\n{text}");
    }
    assert_eq!(column_strings(&db, &sql).await?.len(), 2);
    Ok(())
}

/// The URI + storage options `get_or_create_unified_table` will resolve for
/// `table` under `cfg` — so a test can reach the SAME Delta table out-of-band
/// and manufacture states TF's own write path would never produce.
fn unified_table_location(cfg: &timefusion::config::AppConfig, table: &str) -> (String, std::collections::HashMap<String, String>) {
    let endpoint = cfg.aws.aws_s3_endpoint.clone();
    let uri = format!("s3://{}/{}/{}/?endpoint={}", cfg.aws.aws_s3_bucket.as_ref().unwrap(), cfg.core.timefusion_table_prefix, table, endpoint);
    let opts = [
        ("AWS_ACCESS_KEY_ID", cfg.aws.aws_access_key_id.clone().unwrap()),
        ("AWS_SECRET_ACCESS_KEY", cfg.aws.aws_secret_access_key.clone().unwrap()),
        ("AWS_REGION", cfg.aws.aws_default_region.clone().unwrap()),
        ("AWS_ENDPOINT_URL", endpoint),
        ("AWS_ALLOW_HTTP", "true".into()),
        ("AWS_S3_ALLOW_UNSAFE_RENAME", "true".into()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    (uri, opts)
}

/// REGRESSION GUARD for the 7d68f01 prod outage (2026-07-31): adding a column to
/// a table that ALREADY HAS live Delta data.
///
/// 7d68f01 appended two nullable columns (`updated_at`, `deleted`) to
/// `otel_logs_and_spans`. Every local suite passed. Prod took 268 flush failures
/// and rejected pgwire INSERTs within minutes:
///
///     Arrow error: Invalid argument error: number of columns(94) must match
///     number of fields(92)
///
/// The suites passed because a test creates its Delta table FROM the YAML, so
/// the YAML schema and the stored Delta schema always agree. Prod's tables were
/// created months earlier and still declare the OLD column set. No amount of
/// from-scratch testing can see that skew — this test manufactures it.
///
/// It creates the Delta table at a TRIMMED column set (the YAML minus its two
/// trailing nullable columns — the same two columns, appended in the same place,
/// as the change that broke prod), then drives a normal write carrying the FULL
/// YAML column set through `Database::insert_records_batch`.
///
/// COVERS: the unified-table write path against a narrower-on-disk Delta schema
/// — `prepare_staged_write`'s `evolves` detection, the staged `RecordBatchWriter`
/// fast path, and the locked `WriteBuilder` + `SchemaMode::Merge` fallback — for
/// a nullable column appended at the END, which is the only shape anyone has
/// proposed adding. Both `skip_queue` variants (direct Delta commit and the
/// buffered/flush path that produced the 268 failures) are exercised.
///
/// DOES NOT COVER: custom (BYO-bucket) project tables, which take a different
/// `get_or_create_*` branch; non-nullable or mid-schema column insertions; type
/// CHANGES to an existing column; the reverse skew (Delta wider than the YAML,
/// i.e. a rollback); the tantivy sidecar and the dedup/optimize rewrite paths,
/// which read the stored schema separately. A green run here is evidence that
/// the plain write path tolerates the skew, NOT that a column addition is safe
/// to deploy.
#[serial]
#[tokio::test]
async fn adding_a_column_to_an_existing_table_is_caught() -> Result<()> {
    use deltalake::operations::create::CreateBuilder;

    const TABLE: &str = "mor_versioned";
    let cfg = TestConfigBuilder::new("schema_skew").with_buffer_mode(BufferMode::Enabled).build();

    // Pre-create the unified Delta table at the OLD column set, at exactly the
    // URI `get_or_create_unified_table` will later resolve, so TF LOADS this
    // table instead of creating one from the YAML.
    let schema = timefusion::schema_loader::get_schema(TABLE).expect("fixture registered");
    let added = ["updated_at", "deleted"];
    let old_columns: Vec<_> = schema.columns()?.into_iter().filter(|c| !added.contains(&c.name().as_str())).collect();
    assert_eq!(old_columns.len(), schema.columns()?.len() - added.len(), "the fixture must still declare the columns this test removes");

    let (storage_uri, storage_options) = unified_table_location(&cfg, TABLE);
    CreateBuilder::new()
        .with_location(&storage_uri)
        .with_columns(old_columns)
        .with_partition_columns(schema.partitions.clone())
        .with_storage_options(storage_options.clone())
        .await?;

    // Wire the Delta write callback exactly as `bootstrap` does — without it a
    // flush drains the MemBuffer and never reaches Delta, and this test's whole
    // point is the Delta commit.
    let db_inner = Database::with_config(Arc::clone(&cfg)).await?;
    let db_for_cb = db_inner.clone();
    let cb: timefusion::buffered_write_layer::DeltaWriteCallback = Arc::new(move |project, table, batches, wm| {
        let db = db_for_cb.clone();
        Box::pin(async move { db.insert_records_batch(&project, &table, batches, true, Some(&wm)).await })
    });
    let layer = Arc::new(timefusion::test_utils::test_helpers::test_layer(Arc::clone(&cfg))?.with_delta_writer(cb));
    let db = Arc::new(db_inner.with_buffered_layer(Arc::clone(&layer)));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = chrono::Utc::now().timestamp_micros();

    // The write path now builds batches at the FULL (wider) YAML column set
    // while the loaded Delta table declares the narrower one. This is the exact
    // skew that failed every prod flush.
    for (i, skip_queue) in [(0, true), (1, false)] {
        let rows = vec![mor_row(&format!("k{i}"), "v", &project_id, ts - i * 1000, None)];
        let batch = timefusion::test_utils::test_helpers::json_to_batch_for(TABLE, rows)?;
        db.insert_records_batch(&project_id, TABLE, vec![batch], skip_queue, None).await.map_err(|e| {
            anyhow::anyhow!(
                "writing the YAML's column set into a Delta table created at an OLDER one failed (skip_queue={skip_queue}): {e}\n\
                 This is the 7d68f01 prod failure. Either the write path must evolve the stored Delta schema, \
                 or the column addition needs an explicit migration before deploy — see the note above `TableSchema`."
            )
        })?;
    }

    // Drain the buffered row through the FLUSH path — the leg that actually
    // produced the 268 prod failures. Without this the MemBuffer row never
    // reaches a Delta commit and the test would pass on the fast path alone.
    // Pre-flush the buffered row is served from the MemBuffer leg; both rows
    // must be visible through the routed scan.
    let where_ = format!("WHERE project_id = '{project_id}'");
    let mut ids = column_strings(&db, &format!("SELECT id FROM {TABLE} {where_}")).await?;
    ids.sort();
    assert_eq!(ids, ["k0", "k1"], "both rows must be readable across the MemBuffer ∪ Delta union under the schema skew");
    assert_eq!(
        column_strings(&db, &format!("SELECT CAST(updated_at AS VARCHAR) FROM {TABLE} {where_}")).await?.iter().filter(|s| !s.is_empty()).count(),
        2,
        "the added column must carry values — silently dropping it is the quieter form of this bug"
    );

    // Now the FLUSH leg — the one that produced the 268 prod failures.
    // `flush_all_now` swallows per-bucket errors into `buckets_failed`; that
    // counter IS the 268, so it is the assertion that matters.
    let stats = layer.flush_all_now().await.map_err(|e| {
        anyhow::anyhow!(
            "flushing a buffered write into a Delta table created at an OLDER column set failed: {e}\n\
             This is the 7d68f01 prod failure — see the note above `TableSchema`."
        )
    })?;
    assert_eq!(stats.buckets_failed, 0, "a bucket failed to flush against the older Delta schema: {stats:?}");
    assert!(stats.total_rows > 0, "the flush must have moved the buffered row to Delta, got {stats:?}");

    // Both rows are now physically in Delta. Read the log FRESH (not through
    // TF's cached snapshot, which lags a flush) so this asserts what durably
    // landed rather than what a cache remembers.
    let table = deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?;
    let snapshot = table.snapshot()?;
    let adds = snapshot.add_actions_table(true)?;
    let nr = adds.column_by_name("num_records").expect("num_records").as_primitive::<Int64Type>();
    let physical: i64 = (0..nr.len()).filter(|&i| !nr.is_null(i)).map(|i| nr.value(i)).sum();
    assert_eq!(physical, 2, "both rows must be durable in Delta after the flush");
    // ...and the evolved stored schema really did gain the columns.
    let stored: Vec<String> = snapshot.schema().fields().map(|f| f.name().to_string()).collect();
    for c in added {
        assert!(stored.contains(&c.to_string()), "the write path did not evolve the stored Delta schema — `{c}` is still missing: {stored:?}");
    }
    Ok(())
}

/// REGRESSION GUARD for the 2026-07-31 dashboard outage — the SECOND failure
/// from 7d68f01, and the one that survived the rollback.
///
/// While 7d68f01 was live TF wrote parquet into the LIVE `otel_logs_and_spans`
/// table with `timestamp` NULLABLE, though the YAML declares it NOT NULL. Those
/// files are permanent table content: no `metaData` action was ever committed
/// (the Delta schema never changed), so rolling the binary back fixed nothing.
/// Every logical plan still types `timestamp` NOT NULL from the YAML, and
/// DataFusion rejected the resulting aggregate:
///
///     Internal error: Physical input schema should be the same as the one
///     converted from logical input schema. Differences: field nullability at
///     index 0 [timestamp]: (physical) true vs (logical) false.
///
/// which killed `GROUP BY time_bucket(timestamp)` on every project. `GROUP BY
/// status_code` (a nullable-declared column) kept working — that asymmetry is
/// the fingerprint, so this test asserts both.
///
/// Reproduces it through the SAME mechanism prod hit: a `SchemaMode::Merge`
/// write. delta-rs merges nullability by UNION (`left.is_nullable() ||
/// right.is_nullable()`, `kernel/schema/cast/merge_schema.rs:149`), so a
/// nullable incoming batch permanently widens the physical parquet — while the
/// committed Delta schema, which only changes when the STRUCTURAL schema does,
/// stays NOT NULL. That merge path is exactly what `prepare_staged_write` falls
/// back to when a batch carries a column the table lacks, which is why the
/// column addition and the nullability widening were one event.
///
/// Note TF's normal (staged) write path casts to the table's arrow schema and
/// does NOT widen — so a test that merely hands `insert_records_batch` a
/// nullable batch passes with or without the flag and proves nothing. The
/// out-of-band merge write below is what makes this a real guard.
///
/// Guards `datafusion.execution.skip_physical_aggregate_schema_check`.
///
/// HONESTY NOTE: with the state above reproduced exactly, this query passes on
/// THIS branch whether or not the flag is set — the failure was observed on
/// 5692555, and something in the scan path here already normalizes the schema.
/// So it pins prod's on-disk state and the dashboards' query shape, but it does
/// NOT by itself prove the flag is what keeps them working. Do not delete the
/// flag on the strength of this test being green.
#[serial]
#[tokio::test]
async fn aggregate_groups_on_a_nullability_widened_column() -> Result<()> {
    use datafusion::arrow::{datatypes::Schema, record_batch::RecordBatch};
    use deltalake::operations::write::SchemaMode;

    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("nullability_widened").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // A normal write first, so TF creates the table with `timestamp` NOT NULL.
    let rows: Vec<_> = (0..2).map(|i| test_span_ts(&format!("n{i}"), "v", &project_id, ts + i * 1000)).collect();
    db.insert_records_batch(&project_id, TABLE, vec![json_to_batch(rows)?], true, None).await?;

    // Now widen `timestamp` + `id` to nullable and merge-write them in, exactly
    // as the 7d68f01 binary did. Values untouched — only the field flags move.
    let rows: Vec<_> = (2..4).map(|i| test_span_ts(&format!("n{i}"), "v", &project_id, ts + i * 1000)).collect();
    let batch = json_to_batch(rows)?;
    let widened: Vec<_> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| if matches!(f.name().as_str(), "timestamp" | "id") { Arc::new(f.as_ref().clone().with_nullable(true)) } else { f.clone() })
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new_with_metadata(widened, batch.schema().metadata().clone())), batch.columns().to_vec())?;

    let (storage_uri, storage_options) = unified_table_location(&cfg, TABLE);
    let table = deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?;
    table.write(vec![batch]).with_schema_mode(SchemaMode::Merge).await?;

    // Precondition: the file set really is nullability-widened. If this ever
    // stops holding, the test below is green for the wrong reason.
    let (storage_uri, storage_options) = unified_table_location(&cfg, TABLE);
    let table = deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?;
    let ts_field = table.snapshot()?.schema().field("timestamp").expect("timestamp declared").clone();
    // The COMMITTED schema is untouched (no `metaData` action) while the file
    // just written has `timestamp nullable = true` in its parquet footer —
    // verified out-of-band against MinIO, and the exact state prod is in.
    assert!(!ts_field.is_nullable(), "the COMMITTED Delta schema must still say NOT NULL — that is the whole mismatch");

    // THE production query shape. Without the skip flag this is an
    // `Internal error: Physical input schema should be the same ...`.
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let sql = format!("SELECT time_bucket('1 hour', timestamp) AS b, COUNT(*) AS c FROM {TABLE} WHERE project_id = '{project_id}' GROUP BY b");
    let got = ctx.sql(&sql).await?.collect().await.map_err(|e| {
        anyhow::anyhow!(
            "GROUP BY on a NOT NULL-declared column failed over nullability-widened files: {e}\n\
             Set datafusion.execution.skip_physical_aggregate_schema_check (2026-07-31 dashboard outage)."
        )
    })?;
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 1, "all four rows fall in one hour bucket");

    // The asymmetry that identified the bug in prod: grouping on a column the
    // YAML already declares nullable was never affected, so a green result there
    // alone would NOT have proved anything.
    let sql = format!("SELECT status_code, COUNT(*) FROM {TABLE} WHERE project_id = '{project_id}' GROUP BY status_code");
    assert_eq!(ctx.sql(&sql).await?.collect().await?.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    Ok(())
}

/// A predicate on a column an UPDATE can change must never reach a scan leg.
///
/// Applied at the source it selects rows by a value belonging to a SUPERSEDED
/// version AND removes the newer version from `DedupExec`'s input, so
/// keep-greatest returns the stale row and it passes the same filter above. The
/// row then reads back — and for an UPDATE, gets a new version written — under a
/// value it no longer has. Found 2026-08-02 when `integration::test_update_
/// operations` reported 3 rows matching `status_code = 'OK'` where 2 were
/// correct, one of them already updated to 'ERROR'.
///
/// `Inexact` pushdown cannot fix this: re-applying the filter above the scan
/// cannot recover a version the source already dropped.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_on_an_updated_column_never_matches_the_superseded_version() -> Result<()> {
    let (db, project_id) = buffered_db("mor_mutable_filter").await?;
    let ts = chrono::Utc::now().timestamp_micros();
    write(&db, &project_id, vec![test_span_ts("row", "before", &project_id, ts)], true).await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    ctx.sql(&format!("UPDATE otel_logs_and_spans SET name = 'after' WHERE project_id = '{project_id}' AND id = 'row'")).await?.collect().await?;

    let count = |ctx: datafusion::prelude::SessionContext, pid: String, name: &'static str| async move {
        let rows = ctx.sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}' AND name = '{name}'")).await?.collect().await?;
        anyhow::Ok(rows[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0))
    };
    assert_eq!(count(ctx.clone(), project_id.clone(), "after").await?, 1, "the current version must match its own value");
    assert_eq!(
        count(ctx, project_id, "before").await?,
        0,
        "the superseded value must match NOTHING — a non-zero count means the filter reached a scan leg and resurrected the old version"
    );
    Ok(())
}

/// THE state prod is in on the first deploy after the merge-on-read flip: every
/// row already in Delta was written before `updated_at` existed, so its stamp is
/// NULL, while every new version carries a real one.
///
/// If keep-greatest let NULL win, every UPDATE after the deploy would silently
/// do nothing — the row would keep reading back at its pre-update value with no
/// error anywhere. The `mor_versioned` fixture cannot catch this: TF stamps
/// everything it writes, so a legacy NULL-stamped row has to be manufactured
/// out-of-band, exactly as the nullability-widening regression above does.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_null_stamped_row_loses_to_a_stamped_version() -> Result<()> {
    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("mor_null_stamp").with_buffer_mode(BufferMode::Enabled).build();
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?);
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // A normal write, to create the table.
    db.insert_records_batch(&project_id, TABLE, vec![json_to_batch(vec![test_span_ts("other", "v", &project_id, ts)])?], true, None).await?;

    // Now the legacy row: written straight to Delta so nothing stamps it, which
    // leaves `updated_at` NULL just like every pre-migration row in prod.
    let batch = json_to_batch(vec![test_span_ts("legacy", "before", &project_id, ts)])?;
    assert!(
        batch.column_by_name("updated_at").is_none_or(|c| c.null_count() == c.len()),
        "precondition: the out-of-band row must carry no stamp, or this test proves nothing"
    );
    let (storage_uri, storage_options) = unified_table_location(&cfg, TABLE);
    let table = deltalake::DeltaTableBuilder::from_url(url::Url::parse(&storage_uri)?)?.with_storage_options(storage_options).load().await?;
    table.write(vec![batch]).with_schema_mode(deltalake::operations::write::SchemaMode::Merge).await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let read = |ctx: datafusion::prelude::SessionContext, pid: String| async move {
        let rows = ctx.sql(&format!("SELECT name FROM {TABLE} WHERE project_id = '{pid}' AND id = 'legacy'")).await?.collect().await?;
        anyhow::Ok(
            rows.iter().flat_map(|b| (0..b.num_rows()).map(|i| b.column(0).as_string_view().value(i).to_string()).collect::<Vec<_>>()).collect::<Vec<_>>(),
        )
    };
    assert_eq!(read(ctx.clone(), project_id.clone()).await?, vec!["before"], "precondition: the legacy row reads back");

    ctx.sql(&format!("UPDATE {TABLE} SET name = 'after' WHERE project_id = '{project_id}' AND id = 'legacy'")).await?.collect().await?;

    // The appended version carries a real stamp; the legacy row's is NULL. The
    // stamped version MUST win, and there must be exactly one logical row left.
    assert_eq!(
        read(ctx, project_id).await?,
        vec!["after"],
        "a stamped version must beat a legacy NULL stamp — otherwise UPDATE is a silent no-op after the flip"
    );
    Ok(())
}

/// The migration that must run BEFORE a shipped table's YAML may declare new
/// columns.
///
/// `otel_logs_and_spans` and `otel_metrics` cannot turn on `version_append`
/// without two new columns: a TF-owned tiebreak (the stamp OVERWRITES whatever
/// the client sent, so it must not reuse a client field like
/// `observed_timestamp`) and a nullable Boolean tombstone. Widening the YAML
/// alone is what broke prod in 7d68f01. `migrate_add_columns` widens STORAGE
/// first, so the YAML change that follows is a no-op for the stored schema.
///
/// The fixture creates the table at the OLD column set — the state every live
/// prod table is in — because a table built from the current YAML already has
/// the columns and would prove nothing.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
async fn migrate_add_columns_widens_the_stored_schema_and_is_idempotent() -> Result<()> {
    use deltalake::operations::create::CreateBuilder;

    const TABLE: &str = "mor_versioned";
    let cfg = TestConfigBuilder::new("migrate_cols").with_buffer_mode(BufferMode::Enabled).build();

    let schema = timefusion::schema_loader::get_schema(TABLE).expect("fixture registered");
    let added = ["updated_at", "deleted"];
    let old_columns: Vec<_> = schema.columns()?.into_iter().filter(|c| !added.contains(&c.name().as_str())).collect();
    let (storage_uri, storage_options) = unified_table_location(&cfg, TABLE);
    CreateBuilder::new()
        .with_location(&storage_uri)
        .with_columns(old_columns)
        .with_partition_columns(schema.partitions.clone())
        .with_storage_options(storage_options)
        .await?;

    let db = Database::with_config(Arc::clone(&cfg)).await?;
    let adds = vec![("updated_at".to_string(), "timestamp".to_string()), ("deleted".to_string(), "boolean".to_string())];

    // Dry run must report the work without committing any of it.
    let dry = db.migrate_add_columns(TABLE, &adds, true).await?;
    assert_eq!(dry.added.len(), 2, "dry run must report both missing columns");
    assert_eq!(dry.stored_after, dry.stored_before, "dry run must not change the stored schema");

    let first = db.migrate_add_columns(TABLE, &adds, false).await?;
    assert_eq!(first.added.len(), 2, "both columns must be added to the STORED schema, got {:?}", first.added);
    assert_eq!(first.stored_after, first.stored_before + 2, "stored column count must grow by exactly the two added");

    // Idempotent: re-running after a crash or a partial rollout must be a no-op,
    // not a second commit that re-adds the columns.
    let second = db.migrate_add_columns(TABLE, &adds, false).await?;
    assert!(second.added.is_empty(), "re-running the migration must add nothing, got {:?}", second.added);
    assert_eq!(second.stored_before, first.stored_after, "the second run must observe the widened schema");

    Ok(())
}

/// Regression (2026-08-02): the optimize CLI built its `Database` without a
/// tantivy service, so `tantivy_indexer()` was `None` and every off-box
/// compaction silently skipped both the output reindex and the input GC —
/// orphaned manifest entries accumulated and compacted files stayed
/// unindexed until a server-boot backfill. `tantivy_reconcile_table` is the
/// CLI's post-run repair; it must also visit the per-uuid manifests that the
/// in-server hook's fixed "default"+customs GC list never reaches.
#[serial]
#[tokio::test]
async fn tantivy_reconcile_backfills_new_files_and_gcs_orphans() -> Result<()> {
    use timefusion::tantivy_index::{manifest, service::TantivyIndexService};
    const TABLE: &str = "otel_logs_and_spans";
    let cfg = TestConfigBuilder::new("tantivy_reconcile").with_buffer_mode(BufferMode::Enabled).build();
    let tantivy_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let svc = Arc::new(TantivyIndexService::new(tantivy_store.clone(), Arc::new(cfg.tantivy.clone())));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_tantivy_indexer(svc));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros();

    // Two direct commits → two live parquet files, neither indexed (the
    // direct insert path has no flush-time index callback).
    let row = |id: &str| -> Result<_> { json_to_batch(vec![test_span_ts(id, "n", &project_id, ts)]) };
    db.insert_records_batch(&project_id, TABLE, vec![row("id_a")?], true, None).await?;
    db.insert_records_batch(&project_id, TABLE, vec![row("id_b")?], true, None).await?;

    // Reconcile #1 = backfill: both uncovered live files get indexes under
    // the project-uuid manifest; nothing is stale yet.
    let (built, removed, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert!(built >= 2, "expected both uncovered live files indexed, built={built}");
    assert_eq!(removed, 0, "nothing to GC before compaction");
    let m = manifest::load(tantivy_store.as_ref(), TABLE, &project_id).await?;
    assert_eq!(m.entries.len(), 2, "per-uuid manifest covers both files");

    // Compact 2 files → 1: the inputs' entries are now stale.
    let table_ref = db.unified_tables().read().await.get(TABLE).expect("table created").clone();
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive();
    db.compact_date_concurrent(&table_ref, TABLE, date, Some(&project_id), None).await?;

    // Reconcile #2: stale entries GC'd (via manifest enumeration — the fixed
    // "default" list would miss this manifest) and the output file covered.
    let (_, removed2, _) = db.tantivy_reconcile_table(TABLE).await?;
    assert!(removed2 >= 2, "pre-compaction entries must be GC'd, removed={removed2}");
    let m = manifest::load(tantivy_store.as_ref(), TABLE, &project_id).await?;
    let live: Vec<String> = table_ref.read().await.get_file_uris()?.filter(|u| u.contains(&project_id)).collect();
    assert!(!live.is_empty());
    let covered: Vec<&String> = m.entries.values().filter(|e| e.error.is_none()).flat_map(|e| e.covered_files.iter()).collect();
    for u in &live {
        assert!(covered.contains(&u), "live file {u} must be index-covered after reconcile");
    }
    assert!(m.entries.values().all(|e| e.covered_files.iter().all(|u| live.contains(u))), "no entry may cover a dead file");
    Ok(())
}
