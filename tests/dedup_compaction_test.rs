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
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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

    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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

    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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
/// row does NOT break `UPDATE ... FROM`. One source row matching multiple target
/// rows is allowed by delta-rs — it updates all of them (count=2 here). So the
/// prod MERGE failure is NOT caused by the over-budget dedup skip leaving target
/// duplicates; the cardinality violation is source-side (see the next test).
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn update_from_on_duplicated_target_updates_all_copies() -> Result<()> {
    let cfg = TestConfigBuilder::new("update_dup_target").with_buffer_mode(BufferMode::Enabled).build();
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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
    assert_eq!(updated, 2, "one source row must update BOTH duplicate target rows — target dups do not fail the MERGE");
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
    let _env = walrus_env_guard(&cfg.core.timefusion_data_dir);
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
    let _env = walrus_env_guard(std::path::Path::new(&dir));
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
